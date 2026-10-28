"""scripts/run_experiments.py

Automated experiment harness for Phase 8:
- Executes recovery modes:
  * Mode A: Reactive FIFO (re-replicate after hard failure)
  * Mode B: Predictive FIFO (evacuate in FIFO order)
  * Mode C: Predictive Priority (evacuate in descending Priority score order)
  * Mode C+: Predictive Size-Aware (evacuate in descending PriorityRate order, gamma in {0.5, 1.0})
- Evaluates across prediction outcomes: true_alarm, false_alarm, missed_prediction.
- Executes lead-time sweeps (10s, 30s, 120s, 600s).
- Collects metrics: MTTR, critical-data recovery time, lost objects/bytes,
  data-at-risk window, migration overhead (wasted bytes in false alarms),
  and integrity success rate.
- Exports results to data/experiments_summary.csv and data/experiments_summary.json.
"""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
import random
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.config import AppConfig, AppInfo, NodeConfig, PolicyConfig
from app.db.connection import create_connection
from app.db.jobs_repo import create_job, update_job
from app.db.repository import (
    create_object_and_replica,
    get_nodes_with_capacity,
    list_objects,
)
from app.db.schema import run_migrations
from app.jobs.evacuation import _set_object_status
from app.jobs.rereplication import (
    fallback_evacuation_jobs,
    find_surviving_replicas,
)
from app.jobs.worker import process_next_job
from app.services.priority import rank_objects
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import InMemoryStorageAdapter

logger = logging.getLogger("self_healing_storage.experiments")


def make_test_db() -> sqlite3.Connection:
    """Creates a fresh in-memory database with all migrations applied."""
    conn = create_connection(":memory:")
    run_migrations(conn)
    return conn


def make_test_adapter(*node_ids: str) -> InMemoryStorageAdapter:
    """Creates an InMemoryStorageAdapter for test nodes."""
    configs = [
        NodeConfig(
            id=nid,
            name=f"Node {nid}",
            endpoint_url=f"http://fake-{nid}:9000",
            virtual_capacity_bytes=10 * 1024 * 1024 * 1024,
        )
        for nid in node_ids
    ]
    return InMemoryStorageAdapter(configs)


def make_test_config(
    bandwidth_bytes_per_s: int = 50_000,
    gamma: float = 0.0,
) -> AppConfig:
    """Builds AppConfig configured for simulation."""
    policy = PolicyConfig(
        bandwidth_bytes_per_s=bandwidth_bytes_per_s,
        lease_ttl_s=300,
        reserve_margin_bytes=0,
        max_destination_risk=1.0,
        max_destination_load=1.0,
        gamma=gamma,
    )
    info = AppInfo(worker_enabled=False)
    return AppConfig(app=info, policy=policy)


def seed_experiment_dataset(
    conn: sqlite3.Connection,
    adapter: InMemoryStorageAdapter,
    n_objects: int = 15,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """Seeds a controlled, reproducible dataset on node-1, node-2, node-3.

    Node-2 is the source/fault node.
    Mixed replica policy per PRD Section 24:
      - 30% single replica (r=1, stored only on node-2)
      - 50% two replicas (r=2, stored on node-2 and node-1)
      - 20% three replicas (r=3, stored on node-2, node-1, and node-3)
    Mixed criticalities (1 to 5) and sizes (20KB, 100KB, 500KB).
    """
    rng = random.Random(seed)

    # Ensure 3 nodes exist in DB
    for nid in ("node-1", "node-2", "node-3"):
        conn.execute(
            """
            INSERT OR REPLACE INTO nodes (node_id, name, endpoint, status, virtual_capacity_bytes)
            VALUES (?, ?, ?, 'healthy', ?)
            """,
            (nid, nid, f"http://{nid}", 10 * 1024 * 1024 * 1024),
        )
    conn.commit()

    created_objects = []
    # Sizes chosen to show size-aware prioritization benefits
    size_classes = [20_480, 102_400, 512_000]  # 20KB, 100KB, 500KB

    for i in range(n_objects):
        oid = f"obj-{i+1:03d}"
        size_bytes = size_classes[i % len(size_classes)]
        data = (f"payload-for-{oid}-" * (size_bytes // 20 + 1)).encode("utf-8")[:size_bytes]

        # Mixed replica policy
        if i < int(n_objects * 0.3):
            req_replicas = 1
            node_placement = ["node-2"]
        elif i < int(n_objects * 0.8):
            req_replicas = 2
            node_placement = ["node-2", "node-1"]
        else:
            req_replicas = 3
            node_placement = ["node-2", "node-1", "node-3"]

        # Criticality: distribute 1..5 with some critical (>= 4)
        crit = (i % 5) + 1
        access_cnt = rng.randint(0, 100)

        # Upload to adapter
        for nid in node_placement:
            adapter.put_object(nid, oid, data)

        now_iso = "2026-10-28T10:00:00+00:00"
        checksum = adapter.stream_sha256("node-2", oid)
        create_object_and_replica(
            conn=conn,
            object_data={
                "object_id": oid,
                "object_name": f"file_{oid}.dat",
                "size_bytes": size_bytes,
                "criticality": crit,
                "access_count": access_cnt,
                "access_frequency_normalized": 0.0,
                "origin_node_id": "node-2",
                "checksum_sha256": checksum,
                "required_replica_count": req_replicas,
                "status": "ok",
                "created_at": now_iso,
                "updated_at": now_iso,
                "last_access_at": None,
            },
            replica_data={
                "replica_id": str(uuid.uuid4()),
                "object_id": oid,
                "node_id": "node-2",
                "checksum_sha256": checksum,
                "verified": 1,
                "is_authoritative": 1,
                "status": "valid",
                "created_at": now_iso,
                "verified_at": now_iso,
            },
        )

        # Additional replicas if needed
        for nid in node_placement[1:]:
            rep_id = str(uuid.uuid4())
            now_iso = "2026-10-28T10:00:00+00:00"
            conn.execute(
                """
                INSERT INTO replicas (
                    replica_id, object_id, node_id, checksum_sha256,
                    verified, is_authoritative, status, created_at, verified_at
                ) VALUES (?, ?, ?, ?, 1, 0, 'valid', ?, ?)
                """,
                (rep_id, oid, nid, adapter.stream_sha256(nid, oid), now_iso, now_iso),
            )
        conn.commit()

        created_objects.append({
            "object_id": oid,
            "size_bytes": size_bytes,
            "criticality": crit,
            "required_replica_count": req_replicas,
            "placement": node_placement,
        })

    return created_objects


def run_single_experiment(
    mode: str,
    outcome: str,
    lead_time_s: float = 30.0,
    seed: int = 42,
    bandwidth_bytes_per_s: int = 50_000,
    source_node: str = "node-2",
) -> Dict[str, Any]:
    """Executes a single end-to-end recovery experiment under exact controls."""
    db = make_test_db()
    adapter = make_test_adapter("node-1", "node-2", "node-3")
    clock = ScenarioClock(tick_size_s=1.0)

    gamma = 0.5 if "0.5" in mode else (1.0 if "1.0" in mode else 0.0)
    use_rate = "c_plus" in mode
    config = make_test_config(bandwidth_bytes_per_s=bandwidth_bytes_per_s, gamma=gamma)

    objects = seed_experiment_dataset(db, adapter, n_objects=15, seed=seed)
    all_items, _ = list_objects(db, limit=1000)
    source_items = [
        o for o in all_items
        if any(r["node_id"] == source_node for r in o.get("replicas", []))
    ]

    healthy_node_ids = ["node-1", "node-3"]
    w = config.weights

    # -------------------------------------------------------------------------
    # 1. SCENARIO OUTCOME HANDLING
    # -------------------------------------------------------------------------
    if outcome == "missed_prediction":
        # Node hard-fails immediately at t=0 with zero warning
        adapter.fence_node(source_node)
        db.execute("UPDATE nodes SET status = 'failed' WHERE node_id = ?", (source_node,))
        db.commit()

        # All modes degrade to reactive re-replication
        for obj in source_items:
            oid = obj["object_id"]
            surviving = find_surviving_replicas(db, adapter, oid, exclude_node_ids={source_node})
            if not surviving:
                _set_object_status(db, oid, "lost")
            else:
                create_job(
                    db,
                    job_type="re_replicate",
                    object_id=oid,
                    source_node_id=surviving[0]["node_id"],
                    target_node_id="",
                    priority_score=0.0,
                    now=clock.now_iso(),
                )

        while process_next_job(db, adapter, clock, config, mode="fifo") is not None:
            pass

    elif outcome == "false_alarm":
        # Risk crosses threshold, node degraded, but NEVER hard-fails
        db.execute("UPDATE nodes SET status = 'degraded', risk_score = 0.85 WHERE node_id = ?", (source_node,))
        db.commit()

        if mode == "mode_a":
            # Mode A is reactive to hard-failure only; no jobs created
            pass
        else:
            # Modes B, C, C+ create evacuation jobs
            if mode == "mode_b":
                ordered = source_items
            else:
                for obj in source_items:
                    obj["_node_risk_score"] = 0.85
                ordered = rank_objects(
                    source_items,
                    healthy_node_ids=healthy_node_ids,
                    w1=w.w1_criticality,
                    w2=w.w2_access_frequency,
                    w3=w.w3_node_risk,
                    w4=w.w4_redundancy,
                    gamma=gamma,
                    use_rate=use_rate,
                )

            for obj in ordered:
                score = float(obj.get("priority_rate" if use_rate else "priority", 0.0)) if mode != "mode_b" else 0.0
                create_job(
                    db,
                    job_type="evacuate",
                    object_id=obj["object_id"],
                    source_node_id=source_node,
                    target_node_id="",
                    priority_score=score,
                    now=clock.now_iso(),
                )

            job_mode = "fifo" if mode == "mode_b" else "priority"
            while process_next_job(db, adapter, clock, config, mode=job_mode) is not None:
                pass

    elif outcome == "true_alarm":
        # Risk crosses threshold at t=0, node hard-fails at t=lead_time_s
        db.execute("UPDATE nodes SET status = 'degraded', risk_score = 0.85 WHERE node_id = ?", (source_node,))
        db.commit()

        if mode == "mode_a":
            # Waits until hard failure at t=lead_time_s
            clock.advance(lead_time_s)
            adapter.fence_node(source_node)
            db.execute("UPDATE nodes SET status = 'failed' WHERE node_id = ?", (source_node,))
            db.commit()

            for obj in source_items:
                oid = obj["object_id"]
                surviving = find_surviving_replicas(db, adapter, oid, exclude_node_ids={source_node})
                if not surviving:
                    _set_object_status(db, oid, "lost")
                else:
                    create_job(
                        db,
                        job_type="re_replicate",
                        object_id=oid,
                        source_node_id=surviving[0]["node_id"],
                        target_node_id="",
                        priority_score=0.0,
                        now=clock.now_iso(),
                    )

            while process_next_job(db, adapter, clock, config, mode="fifo") is not None:
                pass

        else:
            # Modes B, C, C+: Evacuate in mode-specific order
            if mode == "mode_b":
                ordered = source_items
            else:
                for obj in source_items:
                    obj["_node_risk_score"] = 0.85
                ordered = rank_objects(
                    source_items,
                    healthy_node_ids=healthy_node_ids,
                    w1=w.w1_criticality,
                    w2=w.w2_access_frequency,
                    w3=w.w3_node_risk,
                    w4=w.w4_redundancy,
                    gamma=gamma,
                    use_rate=use_rate,
                )

            for obj in ordered:
                score = float(obj.get("priority_rate" if use_rate else "priority", 0.0)) if mode != "mode_b" else 0.0
                create_job(
                    db,
                    job_type="evacuate",
                    object_id=obj["object_id"],
                    source_node_id=source_node,
                    target_node_id="",
                    priority_score=score,
                    now=clock.now_iso(),
                )

            job_mode = "fifo" if mode == "mode_b" else "priority"
            node_fenced = False

            while True:
                # Check if lead time expired
                if clock.current_time_s >= lead_time_s and not node_fenced:
                    node_fenced = True
                    adapter.fence_node(source_node)
                    db.execute("UPDATE nodes SET status = 'failed' WHERE node_id = ?", (source_node,))
                    db.commit()
                    # Fallback active/queued jobs for failed node
                    fallback_evacuation_jobs(db, adapter, source_node, clock, config)

                job_id = process_next_job(db, adapter, clock, config, mode=job_mode)
                if job_id is None:
                    break

                # Check if lead time expired after job completion
                if clock.current_time_s >= lead_time_s and not node_fenced:
                    node_fenced = True
                    adapter.fence_node(source_node)
                    db.execute("UPDATE nodes SET status = 'failed' WHERE node_id = ?", (source_node,))
                    db.commit()
                    fallback_evacuation_jobs(db, adapter, source_node, clock, config)

    # -------------------------------------------------------------------------
    # 2. METRICS COLLECTION
    # -------------------------------------------------------------------------
    final_items, _ = list_objects(db, limit=1000)
    lost_objs = [o for o in final_items if o.get("status") == "lost"]
    lost_critical_objs = [o for o in lost_objs if o.get("criticality", 1) >= 4]

    lost_count = len(lost_objs)
    lost_bytes = sum(o["size_bytes"] for o in lost_objs)
    lost_critical_count = len(lost_critical_objs)
    lost_critical_bytes = sum(o["size_bytes"] for o in lost_critical_objs)

    jobs_cur = db.execute("SELECT * FROM migration_jobs")
    all_jobs = [dict(r) for r in jobs_cur.fetchall()]
    succeeded_jobs = [j for j in all_jobs if j["status"] == "succeeded"]
    failed_jobs = [j for j in all_jobs if j["status"] == "failed"]

    bytes_moved = sum(j.get("bytes_transferred", 0) for j in succeeded_jobs)
    wasted_bytes = bytes_moved if outcome == "false_alarm" else 0

    verified_jobs = [j for j in all_jobs if j.get("verification_result") is not None]
    if verified_jobs:
        integrity_success_rate = round(
            sum(1 for j in verified_jobs if j["verification_result"] in ("match", "pass")) / len(verified_jobs), 4
        )
    else:
        integrity_success_rate = 1.0

    mttr_s = round(clock.current_time_s, 2)
    critical_recovery_time_s = mttr_s if any(o.get("criticality", 1) >= 4 for o in source_items) else 0.0
    data_at_risk_window_s = mttr_s

    run_id = f"run_{mode}_{outcome}_{int(lead_time_s)}_{seed}"
    scenario_id = f"scenario_{outcome}_{int(lead_time_s)}"

    return {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "recovery_mode": mode,
        "outcome": outcome,
        "prediction_outcome": outcome,
        "lead_time_s": lead_time_s,
        "seed": seed,
        "gamma": gamma,
        "mttr_s": mttr_s,
        "critical_recovery_time_s": critical_recovery_time_s,
        "data_at_risk_window_s": data_at_risk_window_s,
        "lost_objects_count": lost_count,
        "lost_bytes": lost_bytes,
        "lost_critical_objects_count": lost_critical_count,
        "lost_critical_bytes": lost_critical_bytes,
        "bytes_moved": bytes_moved,
        "wasted_bytes": wasted_bytes,
        "integrity_success_rate": integrity_success_rate,
        "jobs_total": len(all_jobs),
        "jobs_succeeded": len(succeeded_jobs),
        "jobs_failed": len(failed_jobs),
    }


def run_full_experiment_matrix(
    output_dir: str = "data",
    lead_times: Optional[List[float]] = None,
    seed: int = 42,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Runs recovery modes across outcomes and lead times, writing CSV and JSON."""
    modes = ["mode_a", "mode_b", "mode_c", "mode_c_plus_0.5", "mode_c_plus_1.0"]
    lead_times_sweep = lead_times or [10.0, 30.0, 120.0, 600.0]

    runs: List[Dict[str, Any]] = []

    # 1. True Alarm Sweep across all modes and lead times
    for lt in lead_times_sweep:
        for m in modes:
            res = run_single_experiment(mode=m, outcome="true_alarm", lead_time_s=lt, seed=seed)
            runs.append(res)

    # 2. False Alarm Comparison across all modes
    for m in modes:
        res = run_single_experiment(mode=m, outcome="false_alarm", lead_time_s=0.0, seed=seed)
        runs.append(res)

    # 3. Missed Prediction Comparison across all modes
    for m in modes:
        res = run_single_experiment(mode=m, outcome="missed_prediction", lead_time_s=0.0, seed=seed)
        runs.append(res)

    # Summary aggregations
    summary = {
        "total_runs": len(runs),
        "by_mode": {},
        "by_outcome": {},
    }
    for m in modes:
        mode_runs = [r for r in runs if r["recovery_mode"] == m]
        summary["by_mode"][m] = {
            "runs": len(mode_runs),
            "avg_mttr_s": round(sum(r["mttr_s"] for r in mode_runs) / len(mode_runs), 2),
            "avg_lost_objects": round(sum(r["lost_objects_count"] for r in mode_runs) / len(mode_runs), 2),
            "total_wasted_bytes": sum(r["wasted_bytes"] for r in mode_runs),
        }

    for oc in ("true_alarm", "false_alarm", "missed_prediction"):
        oc_runs = [r for r in runs if r["outcome"] == oc]
        summary["by_outcome"][oc] = {
            "runs": len(oc_runs),
            "avg_lost_objects": round(sum(r["lost_objects_count"] for r in oc_runs) / len(oc_runs), 2),
            "total_bytes_moved": sum(r["bytes_moved"] for r in oc_runs),
        }

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Export CSV
    csv_file = out_path / "experiments_summary.csv"
    if runs:
        fieldnames = list(runs[0].keys())
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(runs)

    # Export JSON
    json_file = out_path / "experiments_summary.json"
    full_output = {
        "total_runs": len(runs),
        "runs": runs,
        "summary": summary,
    }
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(full_output, f, indent=2)

    return runs, summary


if __name__ == "__main__":
    runs, summary = run_full_experiment_matrix()
    print(f"Executed {len(runs)} experiment runs across modes and lead times.")
    print("Summary by mode:")
    for m, stats in summary["by_mode"].items():
        print(f"  {m:18s} -> avg_lost={stats['avg_lost_objects']}, wasted_bytes={stats['total_wasted_bytes']}")
