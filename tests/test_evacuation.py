"""Tests for process_evacuation and redundancy_status (Phase 6a step 4).

Uses the in-memory adapter and ScenarioClock — no Docker, no wall-clock.
"""
from __future__ import annotations

import hashlib
import sqlite3
import uuid

import pytest

from app.config import AppConfig, NodeConfig, PolicyConfig
from app.db.jobs_repo import acquire_lease, create_job, get_job
from app.db.repository import get_object
from app.db.schema import run_migrations
from app.jobs.evacuation import process_evacuation, redundancy_status
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import InMemoryStorageAdapter

CHUNK = 1024 * 1024
T0 = "2026-10-28T10:00:00+00:00"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def make_config(**policy_overrides) -> AppConfig:
    defaults = dict(
        bandwidth_bytes_per_s=1024 * 1024,  # 1 MiB/s for fast clock tests
        lease_ttl_s=300,
        reserve_margin_bytes=0,
        max_destination_risk=1.0,
        max_destination_load=1.0,
    )
    defaults.update(policy_overrides)
    policy = PolicyConfig(**defaults)
    return AppConfig(policy=policy)


def make_adapter(*node_ids: str) -> InMemoryStorageAdapter:
    configs = [
        NodeConfig(id=nid, name=nid, endpoint_url=f"http://fake-{nid}:9000",
                   virtual_capacity_bytes=10 * 1024 * 1024 * 1024)
        for nid in node_ids
    ]
    return InMemoryStorageAdapter(configs)


def seed_db(conn, *, node_ids, object_data: bytes, source_node: str,
            criticality: int = 3, required: int = 1) -> tuple[str, str]:
    """Insert nodes, an object, and an authoritative replica. Returns (object_id, replica_id)."""
    now = T0
    for nid in node_ids:
        status = "degraded" if nid == source_node else "healthy"
        conn.execute(
            "INSERT INTO nodes (node_id, name, endpoint, status, virtual_capacity_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            (nid, nid, f"http://{nid}", status, 10 * 1024 * 1024 * 1024),
        )
    object_id = str(uuid.uuid4())
    checksum = hashlib.sha256(object_data).hexdigest()
    conn.execute(
        "INSERT INTO objects (object_id, object_name, size_bytes, criticality, "
        "checksum_sha256, required_replica_count, status, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 'ok', ?, ?)",
        (object_id, "test.bin", len(object_data), criticality, checksum, required, now, now),
    )
    replica_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO replicas (replica_id, object_id, node_id, checksum_sha256, "
        "verified, is_authoritative, status, created_at) VALUES (?, ?, ?, ?, 1, 1, 'valid', ?)",
        (replica_id, object_id, source_node, checksum, now),
    )
    conn.commit()
    return object_id, replica_id


def make_job(conn, object_id, source, target, priority=0.5) -> str:
    return create_job(
        conn,
        job_type="evacuate",
        object_id=object_id,
        source_node_id=source,
        target_node_id=target,
        priority_score=priority,
        now=T0,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

DATA = b"hello evacuation " * 100  # ~1.7 KB


def test_success_path_creates_verified_replica():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    adapter.put_object("src", "OBJID", DATA)

    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=DATA,
                           source_node="src")
    # key == object_id by convention in evacuation.py
    # re-put with the real object_id as key
    adapter.put_object("src", object_id, DATA)

    job_id = make_job(conn, object_id, "src", "tgt")
    clock = ScenarioClock()
    result = process_evacuation(job_id, conn, adapter, clock, make_config())

    assert result == "success"
    job = get_job(conn, job_id)
    assert job["status"] == "succeeded"
    assert job["verification_result"] == "match"

    obj = get_object(conn, object_id)
    replicas = obj["replicas"]
    assert any(r["node_id"] == "tgt" and r["verified"] and r["status"] == "valid"
               for r in replicas), "Expected verified replica on target"


def test_source_unchanged_after_success():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=DATA, source_node="src")
    adapter.put_object("src", object_id, DATA)
    job_id = make_job(conn, object_id, "src", "tgt")
    clock = ScenarioClock()
    process_evacuation(job_id, conn, adapter, clock, make_config())
    assert adapter.get_object("src", object_id) == DATA


def test_checksum_mismatch_not_success():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=DATA, source_node="src")
    # Put corrupted data on source so hashes won't match stored checksum
    adapter.put_object("src", object_id, b"corrupted data")
    job_id = make_job(conn, object_id, "src", "tgt")
    clock = ScenarioClock()
    result = process_evacuation(job_id, conn, adapter, clock, make_config())
    assert result == "failed"
    job = get_job(conn, job_id)
    assert job["status"] == "failed"
    assert job["verification_result"] == "mismatch"
    # Source bytes must be untouched (still "corrupted data" as we put it)
    assert adapter.get_object("src", object_id) == b"corrupted data"


def test_no_valid_destination_gives_blocked_and_at_risk():
    conn = make_conn()
    adapter = make_adapter("src")  # only one node — no valid target
    object_id, _ = seed_db(conn, node_ids=["src"], object_data=DATA, source_node="src")
    adapter.put_object("src", object_id, DATA)
    job_id = make_job(conn, object_id, "src", "nowhere")
    clock = ScenarioClock()
    result = process_evacuation(job_id, conn, adapter, clock, make_config())
    assert result == "blocked"
    job = get_job(conn, job_id)
    assert job["status"] == "blocked"
    obj = get_object(conn, object_id)
    assert obj["status"] == "at_risk"


def test_lease_busy_returns_lease_busy():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=DATA, source_node="src")
    adapter.put_object("src", object_id, DATA)

    # Acquire a lease with another job — use the same base time as T0
    from datetime import datetime, timezone
    t0_dt = datetime.fromisoformat(T0)
    other_job_id = "other-job-123"
    acquire_lease(conn, object_id=object_id, job_id=other_job_id, owner="blocker",
                  now=T0, ttl_s=300)

    job_id = make_job(conn, object_id, "src", "tgt")
    # Start clock at T0 so the lease is still within its TTL window
    clock = ScenarioClock(start_time_utc=t0_dt)
    result = process_evacuation(job_id, conn, adapter, clock, make_config())
    assert result == "lease_busy"


def test_interrupted_copy_retries_and_fails():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    data = b"X" * (3 * CHUNK)  # 3 MiB
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=data, source_node="src")
    adapter.put_object("src", object_id, data)
    # Fail after 1 MiB every time
    adapter.set_fail_after_bytes("tgt", CHUNK)

    job_id = make_job(conn, object_id, "src", "tgt")
    clock = ScenarioClock()
    result = process_evacuation(job_id, conn, adapter, clock, make_config())

    assert result == "failed"
    job = get_job(conn, job_id)
    assert job["status"] == "failed"
    assert job["retries"] == 3
    import json
    backoff = json.loads(job["backoff_log_json"])
    assert backoff == [1.0, 2.0, 4.0]


def test_backoff_advances_clock():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    data = b"Y" * (2 * CHUNK)
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=data, source_node="src")
    adapter.put_object("src", object_id, data)
    adapter.set_fail_after_bytes("tgt", CHUNK)

    clock = ScenarioClock(tick_size_s=1.0)
    t_before = clock.current_time_s
    job_id = make_job(conn, object_id, "src", "tgt")
    process_evacuation(job_id, conn, adapter, clock, make_config())
    # Backoff sums to 1+2+4=7 s, plus any copy advances
    assert clock.current_time_s > t_before + 6.9


def test_clock_advances_by_bytes_over_bandwidth():
    conn = make_conn()
    data = b"Z" * (2 * 1024 * 1024)  # 2 MiB
    adapter = make_adapter("src", "tgt")
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=data, source_node="src")
    adapter.put_object("src", object_id, data)

    bw = 1024 * 1024  # 1 MiB/s
    clock = ScenarioClock()
    t_before = clock.current_time_s
    job_id = make_job(conn, object_id, "src", "tgt")
    process_evacuation(job_id, conn, adapter, clock, make_config(bandwidth_bytes_per_s=bw))

    expected_duration = len(data) / bw
    actual_duration = clock.current_time_s - t_before
    assert abs(actual_duration - expected_duration) < 0.01


def test_idempotent_rerun_does_not_recopy():
    """A second copy attempt to a node already holding the correct data is a no-op."""
    conn = make_conn()
    # Use three nodes: src (degraded), tgt (first copy), tgt2 (second job target)
    adapter = make_adapter("src", "tgt", "tgt2")
    object_id, _ = seed_db(conn, node_ids=["src", "tgt", "tgt2"], object_data=DATA, source_node="src")
    adapter.put_object("src", object_id, DATA)

    job_id = make_job(conn, object_id, "src", "tgt")
    clock = ScenarioClock()
    cfg = make_config()

    first = process_evacuation(job_id, conn, adapter, clock, cfg)
    assert first == "success"

    # Put correct data on tgt2 manually to simulate prior partial run
    adapter.put_object("tgt2", object_id, DATA)
    job_id2 = make_job(conn, object_id, "src", "tgt2")
    result2 = process_evacuation(job_id2, conn, adapter, clock, cfg)
    assert result2 in ("success", "idempotent_skip")


def test_metadata_unchanged_until_verification():
    """Replica count must not increase until verification passes."""
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=DATA, source_node="src")
    # Put corrupted data so verification will fail
    adapter.put_object("src", object_id, b"bad")
    before = len(get_object(conn, object_id)["replicas"])
    job_id = make_job(conn, object_id, "src", "tgt")
    process_evacuation(job_id, conn, adapter, clock=ScenarioClock(), config=make_config())
    after = len(get_object(conn, object_id)["replicas"])
    assert after == before


def test_redundancy_status_redundancy_restored():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=DATA,
                           source_node="src", required=1)
    adapter.put_object("src", object_id, DATA)
    job_id = make_job(conn, object_id, "src", "tgt")
    process_evacuation(job_id, conn, adapter, ScenarioClock(), make_config())
    status = redundancy_status(object_id, conn)
    assert status == "redundancy_restored"


def test_redundancy_status_lost():
    conn = make_conn()
    adapter = make_adapter("src")
    # seed with no replicas
    object_id = str(uuid.uuid4())
    now = T0
    conn.execute(
        "INSERT INTO nodes (node_id, name, endpoint, status, virtual_capacity_bytes) "
        "VALUES ('src', 'src', 'http://src', 'failed', 10737418240)",
    )
    checksum = hashlib.sha256(DATA).hexdigest()
    conn.execute(
        "INSERT INTO objects (object_id, object_name, size_bytes, criticality, "
        "checksum_sha256, required_replica_count, status, created_at, updated_at) "
        "VALUES (?, ?, ?, 3, ?, 1, 'ok', ?, ?)",
        (object_id, "lost.bin", len(DATA), checksum, now, now),
    )
    conn.commit()
    assert redundancy_status(object_id, conn) == "lost"


def test_redundancy_status_evacuation_complete():
    conn = make_conn()
    adapter = make_adapter("src", "tgt")
    # required=2 but we only copy once -> evacuation_complete not redundancy_restored
    object_id, _ = seed_db(conn, node_ids=["src", "tgt"], object_data=DATA,
                           source_node="src", required=2)
    # src is degraded so healthy replica count = 0 before copy
    adapter.put_object("src", object_id, DATA)
    job_id = make_job(conn, object_id, "src", "tgt")
    process_evacuation(job_id, conn, adapter, ScenarioClock(), make_config())
    # After copy: 1 healthy replica on tgt (src is degraded). required=2 -> evacuation_complete
    status = redundancy_status(object_id, conn)
    assert status == "evacuation_complete"
