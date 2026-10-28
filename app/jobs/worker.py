"""app/jobs/worker.py

process_next_job: synchronous, picks the oldest/highest-priority queued job
and calls process_evacuation on it.

run_worker: async loop that calls process_next_job, sleeping 0.2 s only when
the queue is empty. Concurrency = 1.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Any, Dict, Optional

from app.config import AppConfig
from app.jobs.evacuation import process_evacuation
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import StorageAdapter

log = logging.getLogger(__name__)


def _pick_next_job(
    db: sqlite3.Connection,
    mode: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Select the next queued job deterministically.

    In fifo mode: ordered by created order (rowid ASC).
    In priority mode (or default): ordered by priority_score DESC, then rowid ASC.
    """
    if mode == "fifo":
        cursor = db.execute(
            """
            SELECT * FROM migration_jobs
            WHERE status = 'queued'
            ORDER BY rowid ASC
            LIMIT 1
            """
        )
    else:
        cursor = db.execute(
            """
            SELECT * FROM migration_jobs
            WHERE status = 'queued'
            ORDER BY priority_score DESC, rowid ASC
            LIMIT 1
            """
        )
    row = cursor.fetchone()
    return dict(row) if row else None


def process_next_job(
    db: sqlite3.Connection,
    adapter: StorageAdapter,
    clock: ScenarioClock,
    config: AppConfig,
    mode: Optional[str] = None,
) -> Optional[str]:
    """Pick and process the next queued evacuation or re-replication job.

    Returns the job_id that was processed, or None if the queue was empty.
    """
    job = _pick_next_job(db, mode=mode)
    if job is None:
        return None
    job_id: str = job["job_id"]
    job_type: str = job.get("job_type", "evacuate")
    log.info("Worker: processing %s job %s (object=%s)", job_type, job_id, job["object_id"])
    try:
        if job_type == "re_replicate":
            from app.jobs.rereplication import process_rereplication
            process_rereplication(job_id, db, adapter, clock, config)
        else:
            from app.jobs.rereplication import is_node_hard_failed, process_rereplication
            source_node_id = job.get("source_node_id", "")
            if is_node_hard_failed(adapter, source_node_id, db):
                log.warning("Worker: source %s hard-failed for job %s; falling back to re-replication",
                            source_node_id, job_id)
                from app.db.jobs_repo import update_job
                update_job(db, job_id, job_type="re_replicate")
                process_rereplication(job_id, db, adapter, clock, config)
            else:
                process_evacuation(job_id, db, adapter, clock, config)
    except Exception as exc:
        log.exception("Worker: unhandled error in job %s: %s", job_id, exc)
    return job_id


async def run_worker(
    app_state: Any,
    adapter: Optional[StorageAdapter] = None,
    clock: Optional[ScenarioClock] = None,
    config: Optional[AppConfig] = None,
) -> None:
    """Async worker loop. Concurrency = 1.

    Repeatedly calls process_next_job and yields with asyncio.sleep(0.2)
    only when the queue is empty.
    """
    if adapter is not None:
        db = app_state
    else:
        db = getattr(app_state, "db", None) or (app_state.get("db") if isinstance(app_state, dict) else None)
        adapter = getattr(app_state, "adapter", None) or (app_state.get("adapter") if isinstance(app_state, dict) else None)
        clock = getattr(app_state, "clock", None) or (app_state.get("clock") if isinstance(app_state, dict) else None)
        config = getattr(app_state, "config", None) or (app_state.get("config") if isinstance(app_state, dict) else None)

    log.info("Evacuation worker started")
    while True:
        try:
            job_id = process_next_job(db, adapter, clock, config)
            if job_id is None:
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            log.info("Evacuation worker stopped")
            return
        except Exception as exc:
            log.exception("Worker loop error: %s", exc)
            await asyncio.sleep(0.2)

