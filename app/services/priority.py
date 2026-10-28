"""
Priority Engine — Phase 5.

Pure, deterministic, side-effect-free functions that implement the priority
scoring specification from PRD Section 9 and FR-06.

Formula
-------
    C(d)  = (criticality - 1) / 4                          normalised criticality  [0,1]
    A(d)  = log(1 + access_count) / log(1 + max_count)     normalised access freq  [0,1]
            (0 when max_count == 0)
    Pf    = node.risk_score                                 node failure risk       [0,1]
    R(d)  = 1 - min(h, r) / r                              redundancy vulnerability [0,1]
            where h = healthy verified replicas, r = required_replica_count
            objects with r == 0 are excluded

    Priority(d) = w1*C + w2*A + w3*Pf + w4*R

Size-aware variant (Mode C+):
    PriorityRate(d) = Priority(d) / max(size_bytes, 1) ** gamma
    gamma == 0  =>  PriorityRate == Priority  (PRD requirement)

Tie-breaking (PRD Section 9):
    Sort descending by priority, then ascending by healthy verified replicas,
    then ascending by object_id lexicographically.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence


# ---------------------------------------------------------------------------
# Individual normalised term helpers
# ---------------------------------------------------------------------------

def _criticality_term(criticality: int) -> float:
    """C(d) = (criticality - 1) / 4.  Input must be in [1, 5]."""
    return (criticality - 1) / 4.0


def _access_frequency_term(access_count: int, max_count: int) -> float:
    """A(d) = log(1 + count) / log(1 + max_count), or 0 when max_count == 0."""
    if max_count == 0:
        return 0.0
    return math.log(1.0 + access_count) / math.log(1.0 + max_count)


def _redundancy_vulnerability(
    healthy_verified_replicas: int,
    required_replica_count: int,
) -> Optional[float]:
    """R(d) = 1 - min(h, r) / r.

    Returns None if r == 0 (object excluded from ranking).
    """
    if required_replica_count == 0:
        return None  # exclude
    h = healthy_verified_replicas
    r = required_replica_count
    return 1.0 - min(h, r) / r


# ---------------------------------------------------------------------------
# Healthy replica counter
# ---------------------------------------------------------------------------

def count_healthy_verified_replicas(
    replicas: List[Dict[str, Any]],
    healthy_node_ids: Sequence[str],
) -> int:
    """Count replicas that are verified AND on a healthy node.

    A healthy node is one whose status is not degraded/failed/fenced (PRD Section 7.3
    defines healthy verified replica as verified AND on a reachable, not-degraded
    node).  The caller must pass the set of currently-healthy node IDs.
    """
    healthy_set = set(healthy_node_ids)
    return sum(
        1
        for r in replicas
        if r.get("verified") and r.get("status") == "valid" and r.get("node_id") in healthy_set
    )


# ---------------------------------------------------------------------------
# Single-object scorer
# ---------------------------------------------------------------------------

def score_object(
    *,
    criticality: int,
    access_count: int,
    max_access_count: int,
    node_risk_score: float,
    healthy_verified_replicas: int,
    required_replica_count: int,
    size_bytes: int,
    w1: float,
    w2: float,
    w3: float,
    w4: float,
    gamma: float = 0.0,
) -> Optional[Dict[str, Any]]:
    """Compute Priority and PriorityRate for one object.

    Returns None when required_replica_count == 0 (object excluded per PRD).

    Returns a dict with keys:
        priority          - base score
        priority_rate     - size-aware score (== priority when gamma == 0)
        c_term            - normalised criticality component
        a_term            - normalised access frequency component
        pf_term           - node risk component (= node_risk_score)
        r_term            - redundancy vulnerability component
        healthy_replicas  - h value used in R computation
        gamma             - gamma used for size-aware scoring
    """
    r_term = _redundancy_vulnerability(healthy_verified_replicas, required_replica_count)
    if r_term is None:
        return None  # excluded

    c_term = _criticality_term(criticality)
    a_term = _access_frequency_term(access_count, max_access_count)
    pf_term = float(node_risk_score)

    priority = w1 * c_term + w2 * a_term + w3 * pf_term + w4 * r_term

    # Size-aware variant: divide by max(size_bytes, 1)^gamma
    if gamma == 0.0:
        priority_rate = priority
    else:
        priority_rate = priority / (max(size_bytes, 1) ** gamma)

    return {
        "priority": priority,
        "priority_rate": priority_rate,
        "c_term": c_term,
        "a_term": a_term,
        "pf_term": pf_term,
        "r_term": r_term,
        "healthy_replicas": healthy_verified_replicas,
        "gamma": gamma,
    }


# ---------------------------------------------------------------------------
# Multi-object ranker
# ---------------------------------------------------------------------------

def rank_objects(
    objects: List[Dict[str, Any]],
    *,
    healthy_node_ids: Sequence[str],
    w1: float,
    w2: float,
    w3: float,
    w4: float,
    gamma: float = 0.0,
    use_rate: bool = False,
) -> List[Dict[str, Any]]:
    """Rank a list of object dicts by descending priority.

    Each object dict must contain:
        object_id, criticality, access_count, size_bytes,
        required_replica_count, replicas (list of replica dicts),
        and optionally _node_risk_score (float, default 0.0).

    Returns a new list of dicts, each with original object fields plus:
        priority, priority_rate, c_term, a_term, pf_term, r_term,
        healthy_replicas, rank (1-based).

    Objects excluded (required_replica_count == 0) are omitted.
    """
    # Compute max access_count across all objects for normalisation
    max_count = max((obj.get("access_count", 0) for obj in objects), default=0)

    scored: List[Dict[str, Any]] = []
    for obj in objects:
        replicas = obj.get("replicas", [])
        h = count_healthy_verified_replicas(replicas, healthy_node_ids)
        r = obj.get("required_replica_count", 0)

        # Caller may attach _node_risk_score directly on the object dict
        node_risk = float(obj.get("_node_risk_score", 0.0))

        result = score_object(
            criticality=obj.get("criticality", 1),
            access_count=obj.get("access_count", 0),
            max_access_count=max_count,
            node_risk_score=node_risk,
            healthy_verified_replicas=h,
            required_replica_count=r,
            size_bytes=obj.get("size_bytes", 1),
            w1=w1,
            w2=w2,
            w3=w3,
            w4=w4,
            gamma=gamma,
        )
        if result is None:
            continue  # excluded

        entry = {**obj, **result}
        scored.append(entry)

    # Sort: descending priority_rate (or priority), then ascending healthy_replicas,
    # then ascending object_id (deterministic tie-break per PRD Section 9)
    sort_key = "priority_rate" if use_rate else "priority"
    scored.sort(
        key=lambda x: (-x[sort_key], x["healthy_replicas"], x["object_id"])
    )

    for rank, entry in enumerate(scored, start=1):
        entry["rank"] = rank

    return scored
