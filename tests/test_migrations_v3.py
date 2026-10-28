"""Tests: V3 schema migrations on fresh and already-migrated databases."""
from __future__ import annotations

import sqlite3

from app.db.schema import run_migrations, MIGRATIONS


def make_fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_migrations_fresh_db():
    """All three migrations apply cleanly to a fresh in-memory database."""
    conn = make_fresh_conn()
    run_migrations(conn)

    # Check schema_migrations table has all 3 versions
    cursor = conn.cursor()
    cursor.execute("SELECT version FROM schema_migrations ORDER BY version")
    versions = [r[0] for r in cursor.fetchall()]
    assert versions == [1, 2, 3], f"Expected [1,2,3], got {versions}"

    # Verify V3 tables exist by inserting and selecting
    cursor.execute(
        "INSERT INTO migration_jobs "
        "(job_id, job_type, object_id, source_node_id, target_node_id, priority_score, status) "
        "VALUES ('j1','evacuate','o1','n1','n2',0.9,'queued')"
    )
    cursor.execute(
        "INSERT INTO object_leases (object_id, job_id, owner, acquired_at, expires_at) "
        "VALUES ('o1','j1','worker','2026-01-01T00:00:00+00:00','2026-01-01T00:05:00+00:00')"
    )
    conn.commit()

    cursor.execute("SELECT job_id FROM migration_jobs")
    assert cursor.fetchone()[0] == "j1"

    cursor.execute("SELECT object_id FROM object_leases")
    assert cursor.fetchone()[0] == "o1"


def test_migrations_idempotent_already_migrated():
    """Re-running migrations on an already-migrated DB is safe and preserves rows."""
    conn = make_fresh_conn()
    run_migrations(conn)

    # Insert a row before re-run
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO migration_jobs "
        "(job_id, job_type, object_id, source_node_id, target_node_id, priority_score, status) "
        "VALUES ('j2','evacuate','o2','n1','n2',0.5,'succeeded')"
    )
    conn.commit()

    # Re-run migrations — must not raise and must not wipe the row
    run_migrations(conn)

    cursor.execute("SELECT job_id FROM migration_jobs WHERE job_id='j2'")
    row = cursor.fetchone()
    assert row is not None, "Existing row was lost after re-migration"
    assert row[0] == "j2"

    # schema_migrations should still have exactly 3 rows (no duplicates)
    cursor.execute("SELECT COUNT(*) FROM schema_migrations")
    count = cursor.fetchone()[0]
    assert count == 3, f"Expected 3 migration rows, got {count}"
