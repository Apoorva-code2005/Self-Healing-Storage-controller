"""app/jobs/evacuation.py

Synchronous, single-job evacuation processor (Phase 6a step 4).

process_evacuation(job_id, db, adapter, clock, config) runs ONE queued
evacuate job to completion and returns a status string.  It is a plain
function with no asyncio dependencies so tests can call it directly.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.config import AppConfig
from app.db.jobs_repo import (
    acquire_lease,
    get_job,
    release_lease,
    update_job,
)
from app.db.repository import (
    get_nodes_with_capacity,
    get_object,
)
from app.services.placement import select_destinations
from app.services.priority import count_healthy_verified_replicas
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import StorageAdapter

log = logging.getLogger(__name__)

MAX_RETRIES = 3


def _now_iso(clock: ScenarioClock) -> str:
    return clock.now_iso()


def _insert_replica(
    conn: sqlite3.Connection,
    *,
    object_id: str,
    node_id: str,
    checksum_sha256: str,
    now: str,
) -> None:
    """Register a new verified valid non-authoritative replica."""
    replica_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO replicas (
            replica_id, object_id, node_id, checksum_sha256,
            verified, is_authoritative, status, created_at, verified_at
        ) VALUES (?, ?, ?, ?, 1, 0, 'valid', ?, ?)
        """,
        (replica_id, object_id, node_id, checksum_sha256, now, now),
    )
    conn.commit()


def _set_object_status(conn: sqlite3.Connection, object_id: str, status: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE objects SET status = ?, updated_at = ? WHERE object_id = ?",
        (status, now, object_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Public helper: redundancy_status
# ---------------------------------------------------------------------------

def redundancy_status(object_id: str, db: sqlite3.Connection) -> str:
    """Classify current redundancy for object_id.

    Returns one of:
      "redundancy_restored"   — healthy verified replicas >= required_replica_count
      "evacuation_complete"   — at least one healthy verified replica off the source
      "lost"                  — zero verified replicas on any reachable node
    """
    obj = get_object(db, object_id)
    if obj is None or obj.get("status") == "lost":
        return "lost"

    nodes = get_nodes_with_capacity(db)
    healthy_node_ids = [
        nid for nid, n in nodes.items() if n["status"] == "healthy"
    ]

    replicas: List[Dict[str, Any]] = obj.get("replicas", [])
    h = count_healthy_verified_replicas(replicas, healthy_node_ids)

    if h == 0:
        return "lost"
    required = int(obj.get("required_replica_count", 1))
    if h >= required:
        return "redundancy_restored"
    return "evacuation_complete"


# ---------------------------------------------------------------------------
# Main processor
# ---------------------------------------------------------------------------

def process_evacuation(
    job_id: str,
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    clock: ScenarioClock,
    config: AppConfig,
) -> str:
    """Run one queued evacuate job to completion.

    Returns one of: "success", "lease_busy", "blocked", "failed", "idempotent_skip".
    Never deletes or modifies the source object or its replica.
    """
    policy = config.policy
    job = get_job(db, job_id)
    if job is None:
        raise ValueError(f"Job {job_id} not found")

    object_id: str = job["object_id"]
    source_node_id: str = job["source_node_id"]

    # ------------------------------------------------------------------ 1. Lease
    now = _now_iso(clock)
    acquired = acquire_lease(
        db,
        object_id=object_id,
        job_id=job_id,
        owner="evacuation-worker",
        now=now,
        ttl_s=policy.lease_ttl_s,
    )
    if not acquired:
        log.info("Job %s: lease busy for object %s", job_id, object_id)
        return "lease_busy"

    try:
        return _run_job(job, db, adapter, clock, config, source_node_id, object_id)
    finally:
        release_lease(db, object_id, job_id)


def _run_job(
    job: Dict[str, Any],
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    clock: ScenarioClock,
    config: AppConfig,
    source_node_id: str,
    object_id: str,
) -> str:
    from app.jobs.rereplication import _run_rereplication, is_node_hard_failed

    policy = config.policy
    job_id: str = job["job_id"]

    # Mark running
    update_job(db, job_id, status="running", started_at=_now_iso(clock))

    obj = get_object(db, object_id)
    if obj is None:
        update_job(db, job_id, status="failed", error_message="Object not found",
                   finished_at=_now_iso(clock))
        return "failed"

    # Check if source node hard-failed before starting copy
    if is_node_hard_failed(adapter, source_node_id, db):
        log.warning("Job %s: source %s hard-failed before copy; falling back to re-replication", job_id, source_node_id)
        update_job(db, job_id, job_type="re_replicate")
        return _run_rereplication(job, db, adapter, clock, config, source_node_id, object_id)

    stored_checksum: str = obj["checksum_sha256"]
    size_bytes: int = int(obj["size_bytes"])
    replicas: List[Dict[str, Any]] = obj.get("replicas", [])
    key: str = object_id  # object key == object_id by convention

    # ------------------------------------------------------------------ 2. Destination
    nodes_map = get_nodes_with_capacity(db)
    candidate_nodes = list(nodes_map.values())

    destinations = select_destinations(
        source_node_id=source_node_id,
        object_size_bytes=size_bytes,
        existing_replicas=replicas,
        candidate_nodes=candidate_nodes,
        max_destination_risk=policy.max_destination_risk,
        max_destination_load=policy.max_destination_load,
        reserve_margin_bytes=policy.reserve_margin_bytes,
        needed=1,
    )

    if not destinations:
        msg = (
            f"No feasible destination for object {object_id} from {source_node_id}. "
            f"Checked {len(candidate_nodes)} nodes."
        )
        log.warning("Job %s: %s", job_id, msg)
        update_job(db, job_id, status="blocked", error_message=msg,
                   finished_at=_now_iso(clock))
        _set_object_status(db, object_id, "at_risk")
        return "blocked"

    target_node_id: str = destinations[0]["node_id"]

    # ------------------------------------------------------------------ 3. Idempotency check
    try:
        existing_hash = adapter.stream_sha256(target_node_id, key)
        if existing_hash == stored_checksum:
            log.info("Job %s: idempotent skip – target already holds correct data", job_id)
            _register_success(db, job_id, object_id, target_node_id, stored_checksum,
                               stored_checksum, 0, clock)
            return "idempotent_skip"
    except (KeyError, ConnectionError):
        pass  # object not on target yet — proceed normally

    # ------------------------------------------------------------------ 4. Copy with retries
    bytes_copied = 0
    backoff_log: List[float] = []
    retries = 0
    copy_error: Optional[Exception] = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            bytes_copied = adapter.copy_object(source_node_id, target_node_id, key)
            # Advance clock for simulated transfer time
            duration_s = bytes_copied / max(policy.bandwidth_bytes_per_s, 1)
            clock.advance(duration_s)
            update_job(db, job_id, bytes_transferred=bytes_copied)
            copy_error = None
            break
        except (ConnectionError, Exception) as exc:
            copy_error = exc
            if is_node_hard_failed(adapter, source_node_id, db):
                log.warning("Job %s: source %s hard-failed during copy; falling back to re-replication", job_id, source_node_id)
                update_job(db, job_id, job_type="re_replicate")
                return _run_rereplication(job, db, adapter, clock, config, source_node_id, object_id)
            if attempt < MAX_RETRIES:
                backoff_s = policy.evacuation_backoff_base_s * (2 ** attempt)
                backoff_log.append(backoff_s)
                retries += 1
                clock.advance(backoff_s)
                update_job(db, job_id, retries=retries,
                           backoff_log_json=json.dumps(backoff_log))
                log.warning("Job %s attempt %d failed: %s – retrying in %.1fs",
                            job_id, attempt + 1, exc, backoff_s)
            else:
                break

    if copy_error is not None:
        if is_node_hard_failed(adapter, source_node_id, db):
            log.warning("Job %s: source %s hard-failed during copy; falling back to re-replication", job_id, source_node_id)
            update_job(db, job_id, job_type="re_replicate")
            return _run_rereplication(job, db, adapter, clock, config, source_node_id, object_id)
        msg = f"Copy failed after {retries} retries: {copy_error}"
        log.error("Job %s: %s", job_id, msg)
        update_job(db, job_id, status="failed", error_message=msg,
                   finished_at=_now_iso(clock), retries=retries,
                   backoff_log_json=json.dumps(backoff_log))
        return "failed"

    # ------------------------------------------------------------------ 5. Verify
    update_job(db, job_id, status="verifying")
    try:
        src_checksum = adapter.stream_sha256(source_node_id, key)
        tgt_checksum = adapter.stream_sha256(target_node_id, key)
    except Exception as exc:
        update_job(db, job_id, status="failed",
                   error_message=f"Checksum read error: {exc}",
                   finished_at=_now_iso(clock))
        return "failed"

    if src_checksum == stored_checksum and tgt_checksum == stored_checksum:
        _register_success(db, job_id, object_id, target_node_id,
                          src_checksum, tgt_checksum, bytes_copied, clock)
        return "success"

    # ------------------------------------------------------------------ 6. Mismatch
    log.error(
        "Job %s: CHECKSUM MISMATCH object=%s src=%s tgt=%s stored=%s",
        job_id, object_id, src_checksum, tgt_checksum, stored_checksum,
    )
    update_job(
        db, job_id,
        status="failed",
        source_checksum=src_checksum,
        target_checksum=tgt_checksum,
        verification_result="mismatch",
        error_message="Checksum mismatch after copy",
        finished_at=_now_iso(clock),
    )
    return "failed"


def _register_success(
    db: sqlite3.Connection,
    job_id: str,
    object_id: str,
    target_node_id: str,
    src_checksum: str,
    tgt_checksum: str,
    bytes_copied: int,
    clock: ScenarioClock,
) -> None:
    now = _now_iso(clock)
    _insert_replica(
        db,
        object_id=object_id,
        node_id=target_node_id,
        checksum_sha256=tgt_checksum,
        now=now,
    )
    update_job(
        db, job_id,
        status="succeeded",
        source_checksum=src_checksum,
        target_checksum=tgt_checksum,
        verification_result="match",
        bytes_transferred=bytes_copied,
        finished_at=now,
    )
