"""Unit tests for job and lease repository functions (Phase 6a step 2)."""
from __future__ import annotations

import sqlite3
import pytest

from app.db.schema import run_migrations
from app.db.jobs_repo import (
    create_job,
    update_job,
    get_job,
    list_recent_jobs,
    acquire_lease,
    release_lease,
    reclaim_expired_leases,
)


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


T0 = "2026-10-28T10:00:00+00:00"
T1 = "2026-10-28T10:05:00+00:00"   # +5 min (after 300 s TTL)
T_MID = "2026-10-28T10:02:00+00:00"  # inside TTL window


# ---------------------------------------------------------------------------
# Job tests
# ---------------------------------------------------------------------------

def test_create_and_get_job():
    conn = make_conn()
    jid = create_job(
        conn,
        job_type="evacuate",
        object_id="obj-1",
        source_node_id="n1",
        target_node_id="n2",
        priority_score=0.9,
        now=T0,
    )
    job = get_job(conn, jid)
    assert job is not None
    assert job["job_id"] == jid
    assert job["status"] == "queued"
    assert job["retries"] == 0
    assert job["bytes_transferred"] == 0


def test_update_job_status():
    conn = make_conn()
    jid = create_job(conn, job_type="evacuate", object_id="o", source_node_id="n1",
                     target_node_id="n2", priority_score=0.5, now=T0)
    update_job(conn, jid, status="running", started_at=T0)
    job = get_job(conn, jid)
    assert job["status"] == "running"
    assert job["started_at"] == T0


def test_update_job_invalid_status():
    conn = make_conn()
    jid = create_job(conn, job_type="evacuate", object_id="o", source_node_id="n1",
                     target_node_id="n2", priority_score=0.5, now=T0)
    with pytest.raises(ValueError, match="Invalid job status"):
        update_job(conn, jid, status="nonsense")


def test_list_recent_jobs_ordering():
    conn = make_conn()
    j1 = create_job(conn, job_type="evacuate", object_id="a", source_node_id="n1",
                    target_node_id="n2", priority_score=0.1, now=T0)
    j2 = create_job(conn, job_type="evacuate", object_id="b", source_node_id="n1",
                    target_node_id="n2", priority_score=0.2, now=T0)
    jobs = list_recent_jobs(conn, limit=10)
    ids = [j["job_id"] for j in jobs]
    # most recent first
    assert ids.index(j2) < ids.index(j1)


# ---------------------------------------------------------------------------
# Lease tests
# ---------------------------------------------------------------------------

def test_acquire_lease_succeeds():
    conn = make_conn()
    ok = acquire_lease(conn, object_id="obj-1", job_id="j1", owner="worker",
                       now=T0, ttl_s=300)
    assert ok is True


def test_second_acquire_refused():
    conn = make_conn()
    acquire_lease(conn, object_id="obj-1", job_id="j1", owner="worker",
                  now=T0, ttl_s=300)
    # Second job tries to acquire the same object
    ok = acquire_lease(conn, object_id="obj-1", job_id="j2", owner="worker",
                       now=T_MID, ttl_s=300)
    assert ok is False


def test_acquire_after_expiry_succeeds():
    conn = make_conn()
    acquire_lease(conn, object_id="obj-1", job_id="j1", owner="worker",
                  now=T0, ttl_s=300)
    # T1 is 5 minutes later — lease has expired
    ok = acquire_lease(conn, object_id="obj-1", job_id="j2", owner="worker",
                       now=T1, ttl_s=300)
    assert ok is True


def test_release_by_holder():
    conn = make_conn()
    acquire_lease(conn, object_id="obj-1", job_id="j1", owner="worker",
                  now=T0, ttl_s=300)
    released = release_lease(conn, "obj-1", "j1")
    assert released is True
    # Now another job can acquire
    ok = acquire_lease(conn, object_id="obj-1", job_id="j2", owner="worker",
                       now=T_MID, ttl_s=300)
    assert ok is True


def test_release_by_wrong_job_does_nothing():
    conn = make_conn()
    acquire_lease(conn, object_id="obj-1", job_id="j1", owner="worker",
                  now=T0, ttl_s=300)
    released = release_lease(conn, "obj-1", "j-other")
    assert released is False
    # Original lease still blocks
    ok = acquire_lease(conn, object_id="obj-1", job_id="j2", owner="worker",
                       now=T_MID, ttl_s=300)
    assert ok is False


def test_reclaim_expired_leases():
    conn = make_conn()
    # Two leases that expire at T0+300s = 10:05:00
    acquire_lease(conn, object_id="obj-1", job_id="j1", owner="w", now=T0, ttl_s=300)
    acquire_lease(conn, object_id="obj-2", job_id="j2", owner="w", now=T0, ttl_s=300)
    # One unexpired lease (TTL = 600 s, expires at 10:10:00)
    acquire_lease(conn, object_id="obj-3", job_id="j3", owner="w", now=T0, ttl_s=600)

    # At T1 (10:05:00) the first two are exactly at expiry — reclaim them
    count = reclaim_expired_leases(conn, T1)
    assert count == 2

    # obj-3 still held
    ok = acquire_lease(conn, object_id="obj-3", job_id="j4", owner="w",
                       now=T1, ttl_s=300)
    assert ok is False
