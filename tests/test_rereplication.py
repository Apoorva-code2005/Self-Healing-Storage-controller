"""Tests for Phase 6b: Hard-Failure Re-replication, Fallback & Controller-Restart Reconciliation."""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from typing import List

import pytest
from fastapi.testclient import TestClient

from app.config import AppConfig, AppInfo, NodeConfig, PolicyConfig
from app.db.connection import create_connection
from app.db.jobs_repo import acquire_lease, create_job, get_job, update_job
from app.db.repository import get_object
from app.db.schema import run_migrations
from app.jobs.evacuation import process_evacuation
from app.jobs.reconciliation import reconcile_on_startup
from app.jobs.rereplication import (
    fallback_evacuation_jobs,
    find_surviving_replicas,
    is_node_hard_failed,
    process_rereplication,
)
from app.jobs.worker import process_next_job
from app.main import create_app, get_adapter, get_db_session
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import InMemoryStorageAdapter

DATA_A = b"payload-alpha" * 150
DATA_B = b"payload-beta" * 150
T0 = "2026-10-28T10:00:00+00:00"


def make_db() -> sqlite3.Connection:
    conn = create_connection(":memory:")
    run_migrations(conn)
    return conn


def make_adapter(*node_ids: str) -> InMemoryStorageAdapter:
    configs = [
        NodeConfig(
            id=nid,
            name=nid,
            endpoint_url=f"http://fake-{nid}:9000",
            virtual_capacity_bytes=10 * 1024 * 1024 * 1024,
        )
        for nid in node_ids
    ]
    return InMemoryStorageAdapter(configs)


def make_cfg(worker_enabled: bool = False) -> AppConfig:
    policy = PolicyConfig(
        bandwidth_bytes_per_s=1024 * 1024,
        lease_ttl_s=300,
        reserve_margin_bytes=0,
        max_destination_risk=1.0,
        max_destination_load=1.0,
    )
    info = AppInfo(worker_enabled=worker_enabled)
    return AppConfig(app=info, policy=policy)


def seed_node(conn: sqlite3.Connection, node_id: str, status: str = "healthy") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO nodes (node_id, name, endpoint, status, virtual_capacity_bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        (node_id, node_id, f"http://{node_id}", status, 10 * 1024 * 1024 * 1024),
    )
    conn.commit()


def seed_object_with_replicas(
    conn: sqlite3.Connection,
    data: bytes,
    replica_nodes: List[str],
    criticality: int = 3,
    required: int = 2,
) -> str:
    now = T0
    object_id = str(uuid.uuid4())
    checksum = hashlib.sha256(data).hexdigest()
    conn.execute(
        "INSERT INTO objects (object_id, object_name, size_bytes, criticality, "
        "checksum_sha256, required_replica_count, status, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'ok', ?, ?)",
        (object_id, f"{object_id}.bin", len(data), criticality, checksum, required, now, now),
    )
    for i, nid in enumerate(replica_nodes):
        conn.execute(
            "INSERT INTO replicas (replica_id, object_id, node_id, checksum_sha256, "
            "verified, is_authoritative, status, created_at) VALUES (?, ?, ?, ?, 1, ?, 'valid', ?)",
            (str(uuid.uuid4()), object_id, nid, checksum, 1 if i == 0 else 0, now),
        )
    conn.commit()
    return object_id


def make_client(
    db: sqlite3.Connection,
    adapter: InMemoryStorageAdapter,
    cfg: AppConfig | None = None,
) -> TestClient:
    _cfg = cfg or make_cfg()
    app = create_app(cfg=_cfg, adapter=adapter, db_conn=db)
    app.dependency_overrides[get_db_session] = lambda: db
    app.dependency_overrides[get_adapter] = lambda: adapter
    return TestClient(app)


# ---------------------------------------------------------------------------
# 1. Re-replication Logic
# ---------------------------------------------------------------------------

def test_rereplication_from_surviving_replica_never_reads_failed_node():
    """When a node hard-fails, re-replication reads from a surviving replica, never the failed node."""
    db = make_db()
    adapter = make_adapter("n1", "n2", "n3")
    seed_node(db, "n1", status="healthy")
    seed_node(db, "n2", status="healthy")
    seed_node(db, "n3", status="healthy")

    oid = seed_object_with_replicas(db, DATA_A, ["n1", "n2"], required=2)
    adapter.put_object("n1", oid, DATA_A)
    adapter.put_object("n2", oid, DATA_A)

    # Hard-fail / fence n1
    adapter.fence_node("n1")
    assert adapter.is_fenced("n1")

    client = make_client(db, adapter)
    resp = client.post("/recovery/n1")
    assert resp.status_code == 202
    body = resp.json()
    jobs = body["jobs"]
    assert len(jobs) == 1
    job_info = jobs[0]
    job_id = job_info["job_id"]

    # In DB, the job type must be re_replicate and source node must be n2, not n1!
    job_row = get_job(db, job_id)
    assert job_row["job_type"] == "re_replicate"
    assert job_row["source_node_id"] == "n2"

    # Process next job directly
    clock = ScenarioClock()
    cfg = make_cfg()
    processed_id = process_next_job(db, adapter, clock, cfg)
    assert processed_id == job_id

    # Verify target received verified replica on n3
    target_data = adapter.get_object("n3", oid)
    assert target_data == DATA_A
    assert adapter.stream_sha256("n3", oid) == hashlib.sha256(DATA_A).hexdigest()

    # Check status endpoint
    status_resp = client.get("/recovery/status")
    assert status_resp.status_code == 200
    st_body = status_resp.json()
    j_entry = next(j for j in st_body["jobs"] if j["job_id"] == job_id)
    assert j_entry["status"] == "succeeded"
    assert j_entry["target_node_id"] == "n3"


# ---------------------------------------------------------------------------
# 2. Lost-Object Handling
# ---------------------------------------------------------------------------

def test_lost_object_handling_zero_surviving_replicas():
    """If an object on a failed node has zero surviving replicas, mark lost and do not create job."""
    db = make_db()
    adapter = make_adapter("n1", "n2")
    seed_node(db, "n1", status="healthy")
    seed_node(db, "n2", status="healthy")

    # Single-replica object solely on n1
    oid = seed_object_with_replicas(db, DATA_A, ["n1"], required=1)
    adapter.put_object("n1", oid, DATA_A)

    # Fence n1
    adapter.fence_node("n1")

    client = make_client(db, adapter)
    resp = client.post("/recovery/n1")
    assert resp.status_code == 202
    body = resp.json()
    # Zero recovery jobs created because object is lost
    assert body["jobs"] == []

    # Object status updated to 'lost'
    obj = get_object(db, oid)
    assert obj["status"] == "lost"

    # Status endpoint reflects lost object
    st_resp = client.get("/recovery/status")
    st_body = st_resp.json()
    assert oid in st_body.get("lost_objects", [])


# ---------------------------------------------------------------------------
# 3. Fallback Mechanism (Queued and Mid-Flight)
# ---------------------------------------------------------------------------

def test_fallback_when_evacuation_queued_and_node_hard_fails():
    """If a node hard-fails while evacuation is queued, worker falls back to re-replication."""
    db = make_db()
    adapter = make_adapter("n1", "n2", "n3")
    seed_node(db, "n1", status="degraded")
    seed_node(db, "n2", status="healthy")
    seed_node(db, "n3", status="healthy")

    oid = seed_object_with_replicas(db, DATA_B, ["n1", "n2"], required=2)
    adapter.put_object("n1", oid, DATA_B)
    adapter.put_object("n2", oid, DATA_B)

    # Create evacuation job initially while n1 was reachable (degraded)
    client = make_client(db, adapter)
    resp = client.post("/recovery/n1")
    assert resp.status_code == 202
    job_id = resp.json()["jobs"][0]["job_id"]
    assert get_job(db, job_id)["job_type"] == "evacuate"

    # Now n1 hard-fails (fenced) while job was queued!
    adapter.fence_node("n1")

    clock = ScenarioClock()
    cfg = make_cfg()
    processed_id = process_next_job(db, adapter, clock, cfg)
    assert processed_id == job_id

    # The job must have completed as re_replicate from n2, not failed!
    job_row = get_job(db, job_id)
    assert job_row["status"] == "succeeded"
    assert job_row["job_type"] == "re_replicate"
    assert job_row["source_node_id"] == "n2"
    assert adapter.get_object("n3", oid) == DATA_B


def test_fallback_when_evacuation_fails_mid_flight_and_no_surviving_replica():
    """If evacuation source fails mid-flight and object has no surviving replica, mark lost."""
    db = make_db()
    adapter = make_adapter("n1", "n2")
    seed_node(db, "n1", status="degraded")
    seed_node(db, "n2", status="healthy")

    oid = seed_object_with_replicas(db, DATA_A, ["n1"], required=1)
    adapter.put_object("n1", oid, DATA_A)

    job_id = create_job(
        db,
        job_type="evacuate",
        object_id=oid,
        source_node_id="n1",
        target_node_id="n2",
        priority_score=0.5,
        now=T0,
    )

    # Fence n1 before/during execution
    adapter.fence_node("n1")

    clock = ScenarioClock()
    cfg = make_cfg()
    process_evacuation(job_id, db, adapter, clock, cfg)

    job_row = get_job(db, job_id)
    assert job_row["status"] == "failed"
    obj = get_object(db, oid)
    assert obj["status"] == "lost"


# ---------------------------------------------------------------------------
# 4. Controller Restart Reconciliation
# ---------------------------------------------------------------------------

def test_restart_reconciliation_reclaims_expired_leases():
    """Expired leases are reclaimed during startup reconciliation."""
    db = make_db()
    adapter = make_adapter("n1")
    clock = ScenarioClock()
    cfg = make_cfg()

    # Insert an expired lease (expires in past)
    db.execute(
        "INSERT INTO object_leases (object_id, job_id, owner, acquired_at, expires_at) "
        "VALUES ('obj-1', 'job-old', 'worker', '2026-10-28T09:00:00+00:00', '2026-10-28T09:05:00+00:00')"
    )
    # Insert an unexpired lease
    acquire_lease(db, object_id="obj-2", job_id="job-new", owner="worker", now=clock.now_iso(), ttl_s=600)
    db.commit()

    summary = reconcile_on_startup(db, adapter, clock, cfg)
    assert summary["reclaimed_leases"] >= 1

    # obj-1 lease should be deleted
    cur = db.execute("SELECT * FROM object_leases WHERE object_id = 'obj-1'")
    assert cur.fetchone() is None


def test_restart_reconciliation_marks_succeeded_if_target_has_valid_data():
    """If a stale running job already completed on target before restart, mark succeeded."""
    db = make_db()
    adapter = make_adapter("n1", "n2")
    seed_node(db, "n1")
    seed_node(db, "n2")
    clock = ScenarioClock()
    cfg = make_cfg()

    oid = seed_object_with_replicas(db, DATA_A, ["n1"], required=2)
    adapter.put_object("n1", oid, DATA_A)
    # Target already has the valid data
    adapter.put_object("n2", oid, DATA_A)

    job_id = create_job(
        db,
        job_type="evacuate",
        object_id=oid,
        source_node_id="n1",
        target_node_id="n2",
        priority_score=0.8,
        now=T0,
    )
    update_job(db, job_id, status="running")

    reconcile_on_startup(db, adapter, clock, cfg)

    job_row = get_job(db, job_id)
    assert job_row["status"] == "succeeded"
    assert job_row["verification_result"] == "match"

    # Replica registered on n2
    replicas = get_object(db, oid)["replicas"]
    assert any(r["node_id"] == "n2" and r["verified"] for r in replicas)


def test_restart_reconciliation_resets_interrupted_job_to_queued():
    """If a stale running job was interrupted and target has no data, reset to queued."""
    db = make_db()
    adapter = make_adapter("n1", "n2")
    seed_node(db, "n1", status="healthy")
    seed_node(db, "n2", status="healthy")
    clock = ScenarioClock()
    cfg = make_cfg()

    oid = seed_object_with_replicas(db, DATA_B, ["n1"], required=2)
    adapter.put_object("n1", oid, DATA_B)
    # Target n2 does NOT have data

    job_id = create_job(
        db,
        job_type="evacuate",
        object_id=oid,
        source_node_id="n1",
        target_node_id="n2",
        priority_score=0.4,
        now=T0,
    )
    update_job(db, job_id, status="running")

    reconcile_on_startup(db, adapter, clock, cfg)

    job_row = get_job(db, job_id)
    assert job_row["status"] == "queued"
