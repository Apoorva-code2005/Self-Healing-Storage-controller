from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
from typing import Dict

MIGRATION_V1 = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'healthy',
    virtual_capacity_bytes INTEGER NOT NULL,
    load_metric REAL NOT NULL DEFAULT 0.0,
    risk_score REAL NOT NULL DEFAULT 0.0,
    risk_updated_at TEXT,
    telemetry_source TEXT NOT NULL DEFAULT 'observed',
    last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS objects (
    object_id TEXT PRIMARY KEY,
    object_name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    criticality INTEGER NOT NULL CHECK (criticality >= 1 AND criticality <= 5),
    access_count INTEGER NOT NULL DEFAULT 0,
    access_frequency_normalized REAL NOT NULL DEFAULT 0.0,
    origin_node_id TEXT,
    checksum_sha256 TEXT NOT NULL,
    required_replica_count INTEGER NOT NULL CHECK (required_replica_count >= 1 AND required_replica_count <= 3),
    status TEXT NOT NULL DEFAULT 'ok',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_access_at TEXT
);

CREATE TABLE IF NOT EXISTS replicas (
    replica_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL REFERENCES nodes(node_id),
    checksum_sha256 TEXT NOT NULL,
    verified INTEGER NOT NULL DEFAULT 1,
    is_authoritative INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'valid',
    created_at TEXT NOT NULL,
    verified_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_replicas_object_id ON replicas(object_id);
CREATE INDEX IF NOT EXISTS idx_replicas_node_id ON replicas(node_id);
"""

MIGRATION_V2 = """
CREATE TABLE IF NOT EXISTS telemetry (
    telemetry_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(node_id),
    timestamp_utc TEXT NOT NULL,
    latency_ms REAL,
    error_rate REAL NOT NULL DEFAULT 0.0,
    temperature_c REAL,
    capacity_free_bytes INTEGER,
    load_metric REAL NOT NULL DEFAULT 0.0,
    reachable INTEGER NOT NULL DEFAULT 1,
    source_type TEXT NOT NULL DEFAULT 'observed',
    replay_drive_serial TEXT,
    replay_source_date TEXT
);

CREATE INDEX IF NOT EXISTS idx_telemetry_node_ts ON telemetry(node_id, timestamp_utc);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(node_id),
    timestamp_utc TEXT NOT NULL,
    risk_score REAL NOT NULL,
    model_name TEXT NOT NULL DEFAULT 'baseline_threshold',
    model_version TEXT NOT NULL DEFAULT '1.0.0',
    target_horizon TEXT,
    anomaly_flag INTEGER NOT NULL DEFAULT 0,
    explanation TEXT
);

CREATE INDEX IF NOT EXISTS idx_predictions_node_ts ON predictions(node_id, timestamp_utc);

CREATE TABLE IF NOT EXISTS scenarios (
    scenario_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    scenario_type TEXT NOT NULL DEFAULT 'synthetic',
    prediction_outcome TEXT NOT NULL,
    lead_time_s INTEGER,
    seed INTEGER NOT NULL,
    node_faults_json TEXT NOT NULL,
    replay_drive_map_json TEXT,
    initial_dataset_json TEXT,
    created_at TEXT NOT NULL
);
"""

MIGRATION_V3 = """
CREATE TABLE IF NOT EXISTS migration_jobs (
    job_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    source_node_id TEXT NOT NULL,
    target_node_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued','running','verifying','succeeded','failed','blocked')),
    bytes_transferred INTEGER NOT NULL DEFAULT 0,
    retries INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS object_leases (
    object_id TEXT PRIMARY KEY,
    owner_job_id TEXT NOT NULL,
    expiry TEXT NOT NULL
);
"""

MIGRATIONS: Dict[int, str] = {
    1: MIGRATION_V1,
    2: MIGRATION_V2,
    3: MIGRATION_V3,
}

MIGRATIONS: Dict[int, str] = {
    1: MIGRATION_V1,
    2: MIGRATION_V2,
}


def run_migrations(conn: sqlite3.Connection) -> None:
    """Executes unapplied versioned schema migrations sequentially."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
    """)
    conn.commit()

    cursor.execute("SELECT version FROM schema_migrations")
    applied = {row[0] for row in cursor.fetchall()}

    for version in sorted(MIGRATIONS.keys()):
        if version not in applied:
            cursor.executescript(MIGRATIONS[version])
            now_iso = datetime.now(timezone.utc).isoformat()
            cursor.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, now_iso),
            )
            conn.commit()
