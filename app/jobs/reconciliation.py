"""app/jobs/reconciliation.py

Restart reconciliation logic (Phase 6b).
Scans for expired leases and stale running/verifying jobs on controller startup,
reclaims leases, and reconciles job states against persistent metadata and storage state.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict

from app.config import AppConfig
from app.db.jobs_repo import (
    reclaim_expired_leases,
    release_lease,
    update_job,
)
from app.db.repository import get_object
from app.jobs.evacuation import _register_success, _set_object_status
from app.jobs.rereplication import find_surviving_replicas, is_node_hard_failed
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import StorageAdapter

log = logging.getLogger(__name__)


def reconcile_on_startup(
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    clock: ScenarioClock,
    config: AppConfig,
) -> Dict[str, Any]:
    """Scans for expired leases and stale running/verifying jobs, reclaims leases,
    and reconciles job states against persistent metadata and actual object-store state.
    """
    now = clock.now_iso()
    reclaimed_leases = reclaim_expired_leases(db, now)

    cursor = db.execute(
        "SELECT * FROM migration_jobs WHERE status IN ('running', 'verifying')"
    )
    stale_jobs = [dict(r) for r in cursor.fetchall()]
    reconciled_jobs = 0

    for job in stale_jobs:
        job_id = job["job_id"]
        object_id = job["object_id"]
        target_node_id = job.get("target_node_id") or ""
        source_node_id = job.get("source_node_id") or ""

        # Release any in-flight lease for this object
        release_lease(db, object_id, job_id)

        obj = get_object(db, object_id)
        stored_checksum = obj.get("checksum_sha256") if obj else None

        # Check whether the copy already completed before restart
        copy_succeeded = False
        if target_node_id and stored_checksum:
            try:
                target_hash = adapter.stream_sha256(target_node_id, object_id)
                if target_hash == stored_checksum:
                    copy_succeeded = True
            except Exception:
                copy_succeeded = False

        if copy_succeeded:
            log.info("Reconciliation: job %s succeeded before restart; registering replica", job_id)
            bytes_transferred = job.get("bytes_transferred") or (int(obj["size_bytes"]) if obj else 0)
            _register_success(
                db, job_id, object_id, target_node_id,
                stored_checksum, stored_checksum,
                bytes_transferred, clock,
            )
        else:
            # Copy was not complete. Check source node health
            if is_node_hard_failed(adapter, source_node_id, db):
                surviving = find_surviving_replicas(db, adapter, object_id, exclude_node_ids={source_node_id})
                if surviving:
                    new_source = surviving[0]["node_id"]
                    log.warning("Reconciliation: job %s source %s failed; falling back to re-replication from %s",
                                job_id, source_node_id, new_source)
                    update_job(
                        db, job_id,
                        status="queued",
                        job_type="re_replicate",
                        source_node_id=new_source,
                        error_message="Reconciled on restart: source failed, fell back to re-replication",
                    )
                else:
                    log.error("ALERT: Reconciliation: object %s lost; source %s failed with zero surviving replicas",
                              object_id, source_node_id)
                    _set_object_status(db, object_id, "lost")
                    update_job(
                        db, job_id,
                        status="failed",
                        error_message="Reconciled on restart: object lost, source failed with no surviving replicas",
                        finished_at=now,
                    )
            else:
                log.info("Reconciliation: job %s interrupted mid-flight; resetting to queued", job_id)
                update_job(
                    db, job_id,
                    status="queued",
                    error_message="Reconciled on restart: reset to queued",
                )
        reconciled_jobs += 1

    return {
        "reclaimed_leases": reclaimed_leases,
        "reconciled_jobs": reconciled_jobs,
    }
