"""
Destination Selector / Placement service — Phase 5.

Implements FR-07: filter nodes to find feasible migration destinations.

A candidate node must satisfy ALL of the following (PRD Section 7 + FR-07):
  1. Not the source node.
  2. Status is 'healthy' (not degraded, failed, or fenced).
  3. Reachable via the storage adapter (not fenced, not unreachable).
  4. risk_score <= max_destination_risk.
  5. load_metric <= max_destination_load.
  6. free_bytes - reserve_margin >= object_size_bytes.
  7. Does not already hold a verified valid replica of the object (spread policy).

All logic is pure / deterministic given the same inputs (no I/O).
The adapter connectivity check is the only side-effectful operation and is
called by the controller, not here.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence


def _existing_replica_node_ids(replicas: List[Dict[str, Any]]) -> set:
    """Return the set of node_ids that already hold a verified valid replica."""
    return {
        r["node_id"]
        for r in replicas
        if r.get("verified") and r.get("status") == "valid"
    }


def select_destinations(
    *,
    source_node_id: str,
    object_size_bytes: int,
    existing_replicas: List[Dict[str, Any]],
    candidate_nodes: List[Dict[str, Any]],
    max_destination_risk: float,
    max_destination_load: float,
    reserve_margin_bytes: int,
    needed: int = 1,
) -> List[Dict[str, Any]]:
    """Return up to *needed* feasible destination nodes, sorted by descending free_bytes.

    Parameters
    ----------
    source_node_id:
        Node that currently holds the authoritative replica (excluded from
        destinations).
    object_size_bytes:
        Size of the object to be placed.
    existing_replicas:
        Current replica records for the object (from DB). Nodes already holding
        a valid verified replica are excluded (spread policy).
    candidate_nodes:
        Sequence of node info dicts (as returned by get_nodes_with_capacity).
        Each dict must have: node_id, status, risk_score, load_metric,
        free_bytes, virtual_capacity_bytes.
    max_destination_risk:
        Upper bound on node risk_score (inclusive).
    max_destination_load:
        Upper bound on node load_metric (inclusive).
    reserve_margin_bytes:
        Bytes to keep free after placement (PRD Section 7.4 / Section 24).
    needed:
        How many destinations to return (default 1).

    Returns
    -------
    List of node dicts that pass all feasibility constraints, up to *needed*,
    sorted by descending free_bytes then ascending node_id for determinism.
    """
    already_held = _existing_replica_node_ids(existing_replicas)
    feasible = []

    for node in candidate_nodes:
        nid = node["node_id"]

        # 1. Exclude source
        if nid == source_node_id:
            continue

        # 2. Status must be healthy
        if node.get("status", "healthy") != "healthy":
            continue

        # 3. Exclude if already holds valid verified replica (spread)
        if nid in already_held:
            continue

        # 4. Risk bound
        if float(node.get("risk_score", 0.0)) > max_destination_risk:
            continue

        # 5. Load bound
        if float(node.get("load_metric", 0.0)) > max_destination_load:
            continue

        # 6. Capacity check: free_bytes - reserve_margin >= object_size_bytes
        free = int(node.get("free_bytes", 0))
        if free - reserve_margin_bytes < object_size_bytes:
            continue

        feasible.append(node)

    # Sort descending free_bytes, then ascending node_id for determinism
    feasible.sort(key=lambda n: (-int(n.get("free_bytes", 0)), n["node_id"]))

    return feasible[:needed]


def is_feasible(
    *,
    source_node_id: str,
    object_size_bytes: int,
    existing_replicas: List[Dict[str, Any]],
    candidate_nodes: List[Dict[str, Any]],
    max_destination_risk: float,
    max_destination_load: float,
    reserve_margin_bytes: int,
) -> bool:
    """Return True if at least one feasible destination exists."""
    return bool(
        select_destinations(
            source_node_id=source_node_id,
            object_size_bytes=object_size_bytes,
            existing_replicas=existing_replicas,
            candidate_nodes=candidate_nodes,
            max_destination_risk=max_destination_risk,
            max_destination_load=max_destination_load,
            reserve_margin_bytes=reserve_margin_bytes,
            needed=1,
        )
    )
