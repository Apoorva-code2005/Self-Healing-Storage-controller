"""Repository functions for migration jobs and object leases (V3 schema).

All time-sensitive functions accept a `now` ISO-string argument from the
scenario clock so that no wall-clock reads happen inside this module.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any, Dict, List, Optional

VALID_JOB_STATES = {"queued", "running", "verifying", "succeeded", "failed", "blocked"}


# ---------------------------------------------------------------------------
# Migration jobs
# ---------------------------------------------------------------------------

def create_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    object_id: str,
    source_node_id: str,
    target_node_id: str,
    priority_score: float,
    now: str,
) -> str:
    """Inserts a new migration job with status='queued'. Returns job_id."""
    job_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO migration_jobs (
            job_id, job_type, object_id, source_node_id, target_node_id,
            priority_score, status, bytes_transferred, retries, backoff_log_json
        ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, 0, '[]')
        """,
        (job_id, job_type, object_id, source_node_id, target_node_id, priority_score),
    )
    conn.commit()
    return job_id


def update_job(conn: sqlite3.Connection, job_id: str, **fields: Any) -> None:
    """Updates arbitrary fields on a migration job. Validates status if provided."""
    if not fields:
        return
    status = fields.get("status")
    if status is not None and status not in VALID_JOB_STATES:
        raise ValueError(f"Invalid job status '{status}'. Allowed: {sorted(VALID_JOB_STATES)}")

    # backoff_log_json may be passed as a list; serialise it
    if "backoff_log_json" in fields and not isinstance(fields["backoff_log_json"], str):
        fields["backoff_log_json"] = json.dumps(fields["backoff_log_json"])

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    conn.execute(f"UPDATE migration_jobs SET {set_clause} WHERE job_id = ?", values)
    conn.commit()


def get_job(conn: sqlite3.Connection, job_id: str) -> Optional[Dict[str, Any]]:
    """Fetches a single migration job by job_id."""
    cursor = conn.execute("SELECT * FROM migration_jobs WHERE job_id = ?", (job_id,))
    row = cursor.fetchone()
    return dict(row) if row else None


def list_recent_jobs(
    conn: sqlite3.Connection, limit: int = 50
) -> List[Dict[str, Any]]:
    """Returns the most recent migration jobs ordered by rowid descending."""
    cursor = conn.execute(
        "SELECT * FROM migration_jobs ORDER BY rowid DESC LIMIT ?", (limit,)
    )
    return [dict(r) for r in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Object leases
# ---------------------------------------------------------------------------

def acquire_lease(
    conn: sqlite3.Connection,
    *,
    object_id: str,
    job_id: str,
    owner: str,
    now: str,
    ttl_s: int,
) -> bool:
    """Attempts to acquire a lease on object_id for job_id.

    Returns True if the lease was granted, False if an unexpired lease
    for this object already exists.
    The expiry is computed as: now + ttl_s seconds (ISO string arithmetic
    is avoided; we store the expiry as a plain ISO string derived from
    the caller's now + timedelta, but here we accept expiry as a computed
    string via the scenario clock convention: caller passes now and ttl_s,
    and we compute expiry ourselves using only string-safe ISO arithmetic).
    """
    from datetime import datetime, timedelta, timezone  # local import keeps module lightweight

    now_dt = datetime.fromisoformat(now)
    expires_dt = now_dt + timedelta(seconds=ttl_s)
    expires_at = expires_dt.isoformat()

    # Check for any unexpired lease
    cursor = conn.execute(
        "SELECT expires_at FROM object_leases WHERE object_id = ?", (object_id,)
    )
    row = cursor.fetchone()
    if row:
        existing_expires = datetime.fromisoformat(row["expires_at"])
        if existing_expires > now_dt:
            # Active unexpired lease exists — refuse
            return False
        # Expired lease — delete it first
        conn.execute("DELETE FROM object_leases WHERE object_id = ?", (object_id,))

    conn.execute(
        """
        INSERT INTO object_leases (object_id, job_id, owner, acquired_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (object_id, job_id, owner, now, expires_at),
    )
    conn.commit()
    return True


def release_lease(conn: sqlite3.Connection, object_id: str, job_id: str) -> bool:
    """Releases the lease only if job_id is the current holder. Returns True if released."""
    cursor = conn.execute(
        "SELECT job_id FROM object_leases WHERE object_id = ?", (object_id,)
    )
    row = cursor.fetchone()
    if not row or row["job_id"] != job_id:
        return False
    conn.execute("DELETE FROM object_leases WHERE object_id = ?", (object_id,))
    conn.commit()
    return True


def reclaim_expired_leases(conn: sqlite3.Connection, now: str) -> int:
    """Deletes all leases whose expires_at <= now. Returns the count reclaimed."""
    cursor = conn.execute(
        "DELETE FROM object_leases WHERE expires_at <= ?", (now,)
    )
    conn.commit()
    return cursor.rowcount
