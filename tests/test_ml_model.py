"""tests/test_ml_model.py

Tests Phase 4: Backblaze ML Model & Telemetry Replay.
Verifies data preparation, drive split isolation, Random Forest training/scoring,
and replayed telemetry emission into the database.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import pytest

from app.config import AppConfig, ThresholdsConfig
from app.db.schema import run_migrations
from app.services.predictor import SmartPredictor, score_smart_telemetry
from app.services.telemetry import emit_replayed_telemetry, ingest_telemetry_record, load_replay_pool
from scripts.prepare_backblaze import (
    SMART_FEATURE_COLS,
    construct_failure_targets,
    drive_aware_split,
    generate_sample_smart_data,
)
from scripts.build_replay_pool import build_replay_pool_dict
from scripts.train_model import train_and_evaluate


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)

    # Insert a test node
    conn.execute(
        """
        INSERT INTO nodes (node_id, name, endpoint, status, virtual_capacity_bytes)
        VALUES ('node-1', 'Storage Node 1', 'http://127.0.0.1:9001', 'healthy', 100000000)
        """
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def sample_config():
    return AppConfig()


# =====================================================================
# 1. Data Preparation & Target Horizon Tests
# =====================================================================

def test_generate_sample_smart_data():
    df = generate_sample_smart_data(n_healthy_drives=5, n_failing_drives=3, n_days=10, seed=123)
    assert not df.empty
    assert "serial_number" in df.columns
    assert "failure" in df.columns
    for col in SMART_FEATURE_COLS:
        assert col in df.columns

    # Verify healthy drives never fail
    healthy_serials = [s for s in df["serial_number"].unique() if s.startswith("HEALTHY")]
    assert len(healthy_serials) == 5
    assert df[df["serial_number"].isin(healthy_serials)]["failure"].sum() == 0

    # Verify failing drives have exactly one failure day
    failing_serials = [s for s in df["serial_number"].unique() if s.startswith("FAILING")]
    assert len(failing_serials) == 3
    for s in failing_serials:
        assert df[df["serial_number"] == s]["failure"].sum() == 1


def test_construct_failure_targets():
    df = generate_sample_smart_data(n_healthy_drives=2, n_failing_drives=2, n_days=20, seed=42)
    labeled = construct_failure_targets(df, horizon_days=7)

    assert "target" in labeled.columns
    # Healthy drives should have 0 targets
    healthy = labeled[labeled["serial_number"].str.startswith("HEALTHY")]
    assert (healthy["target"] == 0).all()

    # Failing drives should have target=1 in the 7 days prior to failure
    failing = labeled[labeled["serial_number"].str.startswith("FAILING")]
    assert (failing["target"] == 1).any()


# =====================================================================
# 2. Drive-Aware Split Isolation (Leakage Prevention)
# =====================================================================

def test_drive_aware_split_no_leakage():
    df = generate_sample_smart_data(n_healthy_drives=10, n_failing_drives=5, n_days=15, seed=42)
    labeled = construct_failure_targets(df, horizon_days=7)

    train_df, test_df, replay_df = drive_aware_split(labeled, test_ratio=0.2, replay_ratio=0.2, seed=42)

    train_drives = set(train_df["serial_number"].unique())
    test_drives = set(test_df["serial_number"].unique())
    replay_drives = set(replay_df["serial_number"].unique())

    # Ensure strictly zero overlap across splits
    assert len(train_drives.intersection(test_drives)) == 0
    assert len(train_drives.intersection(replay_drives)) == 0
    assert len(test_drives.intersection(replay_drives)) == 0

    # Ensure all drives are partitioned
    all_drives = set(df["serial_number"].unique())
    assert (train_drives | test_drives | replay_drives) == all_drives


# =====================================================================
# 3. Replay Pool Construction Tests
# =====================================================================

def test_build_replay_pool():
    df = generate_sample_smart_data(n_healthy_drives=3, n_failing_drives=2, n_days=10, seed=42)
    pool = build_replay_pool_dict(df)

    assert len(pool) == 5
    for serial, drive_info in pool.items():
        assert "serial_number" in drive_info
        assert "is_failing" in drive_info
        assert "records" in drive_info
        assert len(drive_info["records"]) > 0

        first_rec = drive_info["records"][0]
        assert "smart_5_raw" in first_rec
        assert "smart_187_raw" in first_rec


def test_load_replay_pool_file():
    pool = load_replay_pool()
    if pool:
        assert isinstance(pool, dict)
        drive_ids = list(pool.keys())
        assert len(drive_ids) > 0


# =====================================================================
# 4. Model Training & Evaluation Metrics Tests
# =====================================================================

def test_train_and_evaluate_model():
    df = generate_sample_smart_data(n_healthy_drives=15, n_failing_drives=8, n_days=20, seed=42)
    labeled = construct_failure_targets(df, horizon_days=7)
    train_df, test_df, _ = drive_aware_split(labeled, seed=42)

    clf, metrics = train_and_evaluate(train_df, test_df, n_estimators=20, max_depth=5, seed=42)

    assert clf is not None
    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert "false_positive_rate" in metrics
    assert metrics["precision"] >= 0.0
    assert metrics["recall"] >= 0.0


def test_saved_model_artifact_loading():
    model_path = Path("models/random_forest_v1.joblib")
    metrics_path = Path("models/metrics.json")

    assert model_path.is_file(), "models/random_forest_v1.joblib must exist"
    assert metrics_path.is_file(), "models/metrics.json must exist"

    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    assert metrics["model_name"] == "random_forest"
    assert metrics["model_version"] == "1.0.0"
    assert "confusion_matrix" in metrics


# =====================================================================
# 5. Smart Predictor Tests
# =====================================================================

def test_smart_predictor_nominal_telemetry():
    predictor = SmartPredictor()
    nominal_record = {
        "smart_5_raw": 0,
        "smart_9_raw": 10000,
        "smart_187_raw": 0,
        "smart_194_raw": 28,
        "smart_197_raw": 0,
        "smart_198_raw": 0,
    }
    risk, anomaly, explanation = predictor.predict_risk(nominal_record, ThresholdsConfig(risk_threshold=0.7))
    assert risk < 0.7
    assert anomaly is False
    assert "nominal" in explanation.lower() or "risk" in explanation.lower()


def test_smart_predictor_degraded_telemetry():
    predictor = SmartPredictor()
    degraded_record = {
        "smart_5_raw": 80,
        "smart_9_raw": 35000,
        "smart_187_raw": 15,
        "smart_194_raw": 42,
        "smart_197_raw": 12,
        "smart_198_raw": 6,
    }
    risk, anomaly, explanation = predictor.predict_risk(degraded_record, ThresholdsConfig(risk_threshold=0.7))
    assert risk >= 0.7
    assert anomaly is True
    assert "elevated" in explanation.lower()
    assert "smart_5_raw" in explanation


def test_smart_predictor_fallback_without_model(tmp_path):
    # Predictor initialized with non-existent path should fallback safely
    fake_path = tmp_path / "non_existent.joblib"
    predictor = SmartPredictor(model_path=fake_path)
    assert predictor.model is None

    degraded_record = {"smart_5_raw": 25}
    risk, anomaly, explanation = predictor.predict_risk(degraded_record)
    assert risk >= 0.7
    assert anomaly is True
    assert "Fallback heuristic" in explanation


# =====================================================================
# 6. Replayed Telemetry Emission Tests
# =====================================================================

def test_emit_replayed_telemetry(in_memory_db, sample_config):
    drive_record = {
        "serial_number": "REPLAY_DRIVE_001",
        "date": "2026-01-15",
        "smart_5_raw": 55,
        "smart_9_raw": 24000,
        "smart_187_raw": 8,
        "smart_194_raw": 38,
        "smart_197_raw": 5,
        "smart_198_raw": 3,
    }

    result = emit_replayed_telemetry(
        conn=in_memory_db,
        node_id="node-1",
        drive_record=drive_record,
        config=sample_config,
    )

    assert result["node_status"] == "degraded"
    assert result["prediction"]["model_name"] == "random_forest"
    assert result["prediction"]["target_horizon"] == "7d"
    assert result["prediction"]["risk_score"] >= 0.7
    assert result["prediction"]["anomaly_flag"] is True

    # Verify written to telemetry table
    cur = in_memory_db.cursor()
    cur.execute("SELECT * FROM telemetry WHERE node_id = 'node-1'")
    telem_row = cur.fetchone()
    assert telem_row is not None
    assert telem_row["source_type"] == "replayed"
    assert telem_row["replay_drive_serial"] == "REPLAY_DRIVE_001"
    assert telem_row["replay_source_date"] == "2026-01-15"

    # Verify node status updated in nodes table
    cur.execute("SELECT status, risk_score FROM nodes WHERE node_id = 'node-1'")
    node_row = cur.fetchone()
    assert node_row["status"] == "degraded"
    assert node_row["risk_score"] >= 0.7


def test_emit_replayed_telemetry_nominal(in_memory_db, sample_config):
    drive_record = {
        "serial_number": "REPLAY_HEALTHY_002",
        "date": "2026-01-15",
        "smart_5_raw": 0,
        "smart_9_raw": 12000,
        "smart_187_raw": 0,
        "smart_194_raw": 27,
        "smart_197_raw": 0,
        "smart_198_raw": 0,
    }

    result = emit_replayed_telemetry(
        conn=in_memory_db,
        node_id="node-1",
        drive_record=drive_record,
        config=sample_config,
    )

    assert result["node_status"] == "healthy"
    assert result["prediction"]["model_name"] == "random_forest"
    assert result["prediction"]["anomaly_flag"] is False

    cur = in_memory_db.cursor()
    cur.execute("SELECT status FROM nodes WHERE node_id = 'node-1'")
    node_row = cur.fetchone()
    assert node_row["status"] == "healthy"
