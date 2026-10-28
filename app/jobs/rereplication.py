"""app/jobs/rereplication.py

Hard-failure re-replication and fallback logic (Phase 6b).
Re-replicates objects from surviving healthy verified replicas when a node
has hard-failed or is fenced. Never reads from a hard-failed/fenced node.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Set

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
from app.jobs.evacuation import _insert_replica, _register_success, _set_object_status
from app.services.placement import select_destinations
from app.services.priority import count_healthy_verified_replicas
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import StorageAdapter

log = logging.getLogger(__name__)

MAX_RETRIES = 3


def is_node_hard_failed(
    adapter: StorageAdapter,
    node_id: str,
    db: Optional[sqlite3.Connection] = None,
) -> bool:
    """Checks whether a node is hard-failed (fenced, unreachable, or DB status failed)."""
    if hasattr(adapter, "is_fenced") and adapter.is_fenced(node_id):
        return True
    try:
        reach = adapter.check_node_connectivity(node_id)
        if not reach.reachable or reach.fenced:
            return True
    except Exception:
        return True
    if db is not None:
        try:
            cursor = db.execute("SELECT status FROM nodes WHERE node_id = ?", (node_id,))
            row = cursor.fetchone()
            if row and row[0] in ("failed", "fenced"):
                return True
        except Exception:
            pass
    return False


def find_surviving_replicas(
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    object_id: str,
    exclude_node_ids: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Returns valid verified replicas on reachable, non-fenced nodes."""
    exclude = exclude_node_ids or set()
    cursor = db.execute(
        """
        SELECT replica_id, object_id, node_id, checksum_sha256, verified, is_authoritative, status
        FROM replicas
        WHERE object_id = ? AND verified = 1 AND status = 'valid'
        """,
        (object_id,),
    )
    surviving = []
    for row in cursor.fetchall():
        nid = row["node_id"]
        if nid in exclude:
            continue
        if is_node_hard_failed(adapter, nid, db):
            continue
        surviving.append(dict(row))
    return surviving


def process_rereplication(
    job_id: str,
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    clock: ScenarioClock,
    config: AppConfig,
) -> str:
    """Run one queued re_replicate job to completion.

    Never reads from a hard-failed/fenced node.
    Returns: "success", "lease_busy", "blocked", "failed", "lost", "idempotent_skip".
    """
    policy = config.policy
    job = get_job(db, job_id)
    if job is None:
        raise ValueError(f"Job {job_id} not found")

    object_id: str = job["object_id"]
    failed_node_id: str = job.get("source_node_id", "")

    now = clock.now_iso()
    acquired = acquire_lease(
        db,
        object_id=object_id,
        job_id=job_id,
        owner="rereplication-worker",
        now=now,
        ttl_s=policy.lease_ttl_s,
    )
    if not acquired:
        log.info("Job %s: lease busy for object %s", job_id, object_id)
        return "lease_busy"

    try:
        return _run_rereplication(job, db, adapter, clock, config, failed_node_id, object_id)
    finally:
        release_lease(db, object_id, job_id)


def _run_rereplication(
    job: Dict[str, Any],
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    clock: ScenarioClock,
    config: AppConfig,
    failed_node_id: str,
    object_id: str,
) -> str:
    policy = config.policy
    job_id: str = job["job_id"]
    now_iso = clock.now_iso()

    update_job(db, job_id, status="running", started_at=now_iso)

    obj = get_object(db, object_id)
    if obj is None:
        update_job(db, job_id, status="failed", error_message="Object not found", finished_at=now_iso)
        return "failed"

    # Find surviving healthy verified replicas on reachable, non-fenced nodes
    surviving = find_surviving_replicas(db, adapter, object_id)
    if not surviving:
        msg = f"Object {object_id} is lost: zero surviving verified replicas on reachable nodes."
        log.error("ALERT: %s", msg)
        _set_object_status(db, object_id, "lost")
        update_job(db, job_id, status="failed", error_message=msg, finished_at=clock.now_iso())
        return "lost"

    # Pick read source from surviving replicas (never a failed/fenced node)
    surviving_nids = {s["node_id"] for s in surviving}
    read_source_node_id = job.get("source_node_id")
    if not read_source_node_id or read_source_node_id not in surviving_nids:
        read_source_node_id = surviving[0]["node_id"]
        update_job(db, job_id, source_node_id=read_source_node_id)

    stored_checksum: str = obj["checksum_sha256"]
    size_bytes: int = int(obj["size_bytes"])
    replicas: List[Dict[str, Any]] = obj.get("replicas", [])
    key: str = object_id

    # Destination selection
    nodes_map = get_nodes_with_capacity(db)
    candidate_nodes = [
        n for n in nodes_map.values()
        if not is_node_hard_failed(adapter, n["node_id"], db)
    ]

    destinations = select_destinations(
        source_node_id=read_source_node_id,
        object_size_bytes=size_bytes,
        existing_replicas=replicas,
        candidate_nodes=candidate_nodes,
        max_destination_risk=policy.max_destination_risk,
        max_destination_load=policy.max_destination_load,
        reserve_margin_bytes=policy.reserve_margin_bytes,
        needed=1,
    )

    if not destinations:
        msg = f"No feasible destination for re-replicating object {object_id}."
        log.warning("Job %s: %s", job_id, msg)
        update_job(db, job_id, status="blocked", error_message=msg, finished_at=clock.now_iso())
        _set_object_status(db, object_id, "at_risk")
        return "blocked"

    target_node_id: str = destinations[0]["node_id"]
    update_job(db, job_id, target_node_id=target_node_id)

    # Idempotency check
    try:
        existing_hash = adapter.stream_sha256(target_node_id, key)
        if existing_hash == stored_checksum:
            log.info("Job %s: idempotent skip – target already holds correct data", job_id)
            _register_success(db, job_id, object_id, target_node_id, stored_checksum, stored_checksum, 0, clock)
            return "idempotent_skip"
    except (KeyError, ConnectionError):
        pass

    # Copy with retries
    bytes_copied = 0
    backoff_log: List[float] = []
    retries = 0
    copy_error: Optional[Exception] = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            bytes_copied = adapter.copy_object(read_source_node_id, target_node_id, key)
            duration_s = bytes_copied / max(policy.bandwidth_bytes_per_s, 1)
            clock.advance(duration_s)
            update_job(db, job_id, bytes_transferred=bytes_copied)
            copy_error = None
            break
        except (ConnectionError, Exception) as exc:
            copy_error = exc
            if attempt < MAX_RETRIES:
                backoff_s = policy.evacuation_backoff_base_s * (2 ** attempt)
                backoff_log.append(backoff_s)
                retries += 1
                clock.advance(backoff_s)
                update_job(db, job_id, retries=retries, backoff_log_json=json.dumps(backoff_log))
                log.warning("Job %s re-replicate attempt %d failed: %s – retrying in %.1fs",
                            job_id, attempt + 1, exc, backoff_s)
            else:
                break

    if copy_error is not None:
        msg = f"Re-replication copy failed after {retries} retries: {copy_error}"
        log.error("Job %s: %s", job_id, msg)
        update_job(db, job_id, status="failed", error_message=msg,
                   finished_at=clock.now_iso(), retries=retries,
                   backoff_log_json=json.dumps(backoff_log))
        return "failed"

    # Verify
    update_job(db, job_id, status="verifying")
    try:
        src_checksum = adapter.stream_sha256(read_source_node_id, key)
        tgt_checksum = adapter.stream_sha256(target_node_id, key)
    except Exception as exc:
        update_job(db, job_id, status="failed", error_message=f"Checksum read error: {exc}",
                   finished_at=clock.now_iso())
        return "failed"

    if src_checksum == stored_checksum and tgt_checksum == stored_checksum:
        _register_success(db, job_id, object_id, target_node_id,
                          src_checksum, tgt_checksum, bytes_copied, clock)
        # Update object status
        nodes = get_nodes_with_capacity(db)
        healthy_nodes = [nid for nid, n in nodes.items() if n["status"] == "healthy" and not is_node_hard_failed(adapter, nid, db)]
        updated_obj = get_object(db, object_id)
        if updated_obj:
            h = count_healthy_verified_replicas(updated_obj.get("replicas", []), healthy_nodes)
            req = int(updated_obj.get("required_replica_count", 1))
            new_status = "ok" if h >= req else "at_risk"
            _set_object_status(db, object_id, new_status)
        return "success"

    # Checksum mismatch
    log.error("Job %s: CHECKSUM MISMATCH object=%s src=%s tgt=%s stored=%s",
              job_id, object_id, src_checksum, tgt_checksum, stored_checksum)
    update_job(
        db, job_id,
        status="failed",
        source_checksum=src_checksum,
        target_checksum=tgt_checksum,
        verification_result="mismatch",
        error_message="Checksum mismatch after re-replication copy",
        finished_at=clock.now_iso(),
    )
    return "failed"


def fallback_evacuation_jobs(
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    failed_node_id: str,
    clock: ScenarioClock,
    config: AppConfig,
) -> int:
    """Gracefully falls back active evacuation jobs for a failed node to re-replication."""
    cursor = db.execute(
        """
        SELECT * FROM migration_jobs
        WHERE source_node_id = ? AND job_type = 'evacuate' AND status IN ('queued', 'running', 'blocked')
        """,
        (failed_node_id,),
    )
    jobs = [dict(r) for r in cursor.fetchall()]
    count = 0
    for j in jobs:
        oid = j["object_id"]
        jid = j["job_id"]
        surviving = find_surviving_replicas(db, adapter, oid, exclude_node_ids={failed_node_id})
        if surviving:
            new_source = surviving[0]["node_id"]
            update_job(
                db, jid,
                job_type="re_replicate",
                source_node_id=new_source,
                status="queued",
                error_message=f"Fell back to re-replication from {new_source} after {failed_node_id} failure",
            )
            count += 1
        else:
            _set_object_status(db, oid, "lost")
            log.error("ALERT: Object %s is lost: source %s failed and zero surviving replicas exist", oid, failed_node_id)
            update_job(
                db, jid,
                status="failed",
                error_message=f"Object lost: source {failed_node_id} failed with no surviving replicas",
                finished_at=clock.now_iso(),
            )
    return count
