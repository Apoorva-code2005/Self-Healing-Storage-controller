"""Tests for POST /recovery/{node_id} and GET /recovery/status (Phase 6a step 5)."""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import List

import pytest
from fastapi.testclient import TestClient

from app.config import AppConfig, AppInfo, NodeConfig, PolicyConfig
from app.db.connection import create_connection
from app.db.schema import run_migrations
from app.db.jobs_repo import create_job
from app.jobs.evacuation import process_evacuation
from app.jobs.worker import process_next_job
from app.main import create_app, get_adapter, get_db_session
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import InMemoryStorageAdapter

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

DATA_A = b"alpha-data" * 200   # criticality 5
DATA_B = b"beta-data" * 200    # criticality 1
T0 = "2026-10-28T10:00:00+00:00"


def make_db() -> sqlite3.Connection:
    conn = create_connection(":memory:")
    run_migrations(conn)
    return conn



def make_adapter(*node_ids: str) -> InMemoryStorageAdapter:
    configs = [
        NodeConfig(id=nid, name=nid, endpoint_url=f"http://fake-{nid}:9000",
                   virtual_capacity_bytes=10 * 1024 * 1024 * 1024)
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


def seed_node(conn: sqlite3.Connection, node_id: str, status: str = "degraded") -> None:
    conn.execute(
        "INSERT OR REPLACE INTO nodes (node_id, name, endpoint, status, virtual_capacity_bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        (node_id, node_id, f"http://{node_id}", status, 10 * 1024 * 1024 * 1024),
    )
    conn.commit()


def seed_object(conn: sqlite3.Connection, data: bytes, source_node: str,
                criticality: int = 3, required: int = 1) -> str:
    now = T0
    object_id = str(uuid.uuid4())
    checksum = hashlib.sha256(data).hexdigest()
    conn.execute(
        "INSERT INTO objects (object_id, object_name, size_bytes, criticality, "
        "checksum_sha256, required_replica_count, status, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'ok', ?, ?)",
        (object_id, f"{object_id}.bin", len(data), criticality, checksum, required, now, now),
    )
    conn.execute(
        "INSERT INTO replicas (replica_id, object_id, node_id, checksum_sha256, "
        "verified, is_authoritative, status, created_at) VALUES (?, ?, ?, ?, 1, 1, 'valid', ?)",
        (str(uuid.uuid4()), object_id, source_node, checksum, now),
    )
    conn.commit()
    return object_id


def make_client(db: sqlite3.Connection, adapter: InMemoryStorageAdapter,
                cfg: AppConfig | None = None) -> TestClient:
    _cfg = cfg or make_cfg()
    app = create_app(cfg=_cfg, adapter=adapter, db_conn=db)
    app.dependency_overrides[get_db_session] = lambda: db
    app.dependency_overrides[get_adapter] = lambda: adapter
    return TestClient(app)


# ---------------------------------------------------------------------------
# POST /recovery/{node_id} tests
# ---------------------------------------------------------------------------

def test_unknown_node_gives_404():
    db = make_db()
    adapter = make_adapter("n1")
    seed_node(db, "n1")
    client = make_client(db, adapter)
    resp = client.post("/recovery/no-such-node")
    assert resp.status_code == 404


def test_invalid_mode_gives_400():
    db = make_db()
    adapter = make_adapter("src")
    seed_node(db, "src")
    client = make_client(db, adapter)
    resp = client.post("/recovery/src?mode=turbo")
    assert resp.status_code == 400


def test_empty_node_gives_202_with_empty_jobs():
    db = make_db()
    adapter = make_adapter("src")
    seed_node(db, "src")
    client = make_client(db, adapter)
    resp = client.post("/recovery/src")
    assert resp.status_code == 202
    body = resp.json()
    assert body["jobs"] == []


def test_priority_mode_orders_by_criticality():
    """Objects with higher criticality must appear first in priority mode."""
    db = make_db()
    adapter = make_adapter("src", "tgt")
    seed_node(db, "src", status="degraded")
    seed_node(db, "tgt", status="healthy")
    adapter.put_object("src", "dummy", b"x")  # adapter init

    # low criticality first (older created_at)
    oid_low = seed_object(db, DATA_B, "src", criticality=1)
    oid_high = seed_object(db, DATA_A, "src", criticality=5)

    for oid in (oid_low, oid_high):
        adapter.put_object("src", oid, DATA_A if oid == oid_high else DATA_B)

    client = make_client(db, adapter)
    resp = client.post("/recovery/src?mode=priority")
    assert resp.status_code == 202
    jobs = resp.json()["jobs"]
    assert len(jobs) == 2
    # High criticality object should be first
    assert jobs[0]["object_id"] == oid_high


def test_fifo_mode_orders_by_creation():
    """FIFO mode must list oldest object first regardless of criticality."""
    db = make_db()
    adapter = make_adapter("src", "tgt")
    seed_node(db, "src", status="degraded")
    seed_node(db, "tgt", status="healthy")

    oid_first = seed_object(db, DATA_A, "src", criticality=1)
    oid_second = seed_object(db, DATA_B, "src", criticality=5)
    for oid, data in [(oid_first, DATA_A), (oid_second, DATA_B)]:
        adapter.put_object("src", oid, data)

    client = make_client(db, adapter)
    resp = client.post("/recovery/src?mode=fifo")
    assert resp.status_code == 202
    jobs = resp.json()["jobs"]
    assert len(jobs) == 2
    assert jobs[0]["object_id"] == oid_first


def test_no_duplicate_jobs_for_active_object():
    """A second POST must not create a duplicate job for an object with an active job."""
    db = make_db()
    adapter = make_adapter("src", "tgt")
    seed_node(db, "src", status="degraded")
    seed_node(db, "tgt", status="healthy")

    oid = seed_object(db, DATA_A, "src", criticality=3)
    adapter.put_object("src", oid, DATA_A)

    client = make_client(db, adapter)
    resp1 = client.post("/recovery/src")
    assert resp1.status_code == 202
    assert len(resp1.json()["jobs"]) == 1

    resp2 = client.post("/recovery/src")
    assert resp2.status_code == 202
    assert len(resp2.json()["jobs"]) == 0  # already has active job


# ---------------------------------------------------------------------------
# GET /recovery/status tests
# ---------------------------------------------------------------------------

def test_status_shows_succeeded_with_verified_replica():
    db = make_db()
    adapter = make_adapter("src", "tgt")
    seed_node(db, "src", status="degraded")
    seed_node(db, "tgt", status="healthy")

    oid = seed_object(db, DATA_A, "src", criticality=3)
    adapter.put_object("src", oid, DATA_A)

    client = make_client(db, adapter)
    resp = client.post("/recovery/src")
    assert resp.status_code == 202
    job_id = resp.json()["jobs"][0]["job_id"]

    # Run the job directly (worker_enabled=False)
    clock = ScenarioClock()
    cfg = make_cfg()
    process_next_job(db, adapter, clock, cfg)

    status_resp = client.get("/recovery/status")
    assert status_resp.status_code == 200
    body = status_resp.json()
    job_entry = next(j for j in body["jobs"] if j["job_id"] == job_id)
    assert job_entry["status"] == "succeeded"
    assert job_entry["redundancy_status"] in ("redundancy_restored", "evacuation_complete")
    assert body["summary"]["succeeded"] >= 1


def test_blocked_job_shows_blocked_never_succeeded():
    db = make_db()
    # Only source node — no healthy target available
    adapter = make_adapter("src")
    seed_node(db, "src", status="degraded")

    oid = seed_object(db, DATA_A, "src", criticality=3)
    adapter.put_object("src", oid, DATA_A)

    # Create job manually with empty target (no placement possible)
    now = T0
    job_id = create_job(db, job_type="evacuate", object_id=oid,
                        source_node_id="src", target_node_id="",
                        priority_score=0.5, now=now)

    clock = ScenarioClock()
    cfg = make_cfg()
    process_next_job(db, adapter, clock, cfg)

    client = make_client(db, adapter)
    status_resp = client.get("/recovery/status")
    body = status_resp.json()
    job_entry = next(j for j in body["jobs"] if j["job_id"] == job_id)
    # blocked because target is empty/missing; never succeeded
    assert job_entry["status"] in ("blocked", "failed")
    assert "succeeded" not in body["summary"] or body["summary"].get("succeeded", 0) == 0


def test_status_summary_counts_correct():
    db = make_db()
    adapter = make_adapter("src", "tgt")
    seed_node(db, "src", status="degraded")
    seed_node(db, "tgt", status="healthy")

    oid1 = seed_object(db, DATA_A, "src", criticality=5)
    oid2 = seed_object(db, DATA_B, "src", criticality=1)
    for oid, data in [(oid1, DATA_A), (oid2, DATA_B)]:
        adapter.put_object("src", oid, data)

    client = make_client(db, adapter)
    client.post("/recovery/src")

    clock = ScenarioClock()
    cfg = make_cfg()
    # Process both jobs
    process_next_job(db, adapter, clock, cfg)
    process_next_job(db, adapter, clock, cfg)

    status_resp = client.get("/recovery/status")
    body = status_resp.json()
    total = sum(body["summary"].values())
    assert total == 2
