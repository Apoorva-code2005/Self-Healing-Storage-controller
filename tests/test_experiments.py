"""tests/test_experiments.py

Tests for Phase 8: Experiments Framework, Recovery Mode Comparison & Lead-Time Sweep.
Verifies experiment harness, metric calculations, mode comparison dynamics,
CSV/JSON export, and the GET /experiments API endpoint.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from scripts.run_experiments import (
    make_test_adapter,
    make_test_config,
    make_test_db,
    run_full_experiment_matrix,
    run_single_experiment,
    seed_experiment_dataset,
)


@pytest.fixture
def client():
    app = create_app()
    return TestClient(app)


# =============================================================================
# 1. Dataset Seeding & Setup Tests
# =============================================================================

def test_seed_experiment_dataset():
    db = make_test_db()
    adapter = make_test_adapter("node-1", "node-2", "node-3")
    dataset = seed_experiment_dataset(db, adapter, n_objects=15, seed=42)

    assert len(dataset) == 15
    # Verify mixed replica policies
    r1_count = sum(1 for o in dataset if o["required_replica_count"] == 1)
    r2_count = sum(1 for o in dataset if o["required_replica_count"] == 2)
    r3_count = sum(1 for o in dataset if o["required_replica_count"] == 3)

    assert r1_count > 0
    assert r2_count > 0
    assert r3_count > 0

    # Verify critical objects exist
    crit_count = sum(1 for o in dataset if o["criticality"] >= 4)
    assert crit_count > 0


# =============================================================================
# 2. Harness Execution & Mode Comparisons
# =============================================================================

def test_run_single_experiment_mode_a_reactive():
    # In true alarm with 30s lead time, Mode A waits for hard failure, losing r=1 objects
    res = run_single_experiment(mode="mode_a", outcome="true_alarm", lead_time_s=30.0, seed=42)

    assert res["recovery_mode"] == "mode_a"
    assert res["outcome"] == "true_alarm"
    assert res["lead_time_s"] == 30.0
    assert res["lost_objects_count"] > 0, "Mode A should lose single-replica objects on failed node"
    assert res["wasted_bytes"] == 0, "No wasted bytes in true alarm"
    assert res["integrity_success_rate"] == 1.0


def test_run_single_experiment_mode_c_priority():
    # In true alarm with 120s lead time, Mode C evacuates before failure
    res = run_single_experiment(mode="mode_c", outcome="true_alarm", lead_time_s=120.0, seed=42)

    assert res["recovery_mode"] == "mode_c"
    assert res["outcome"] == "true_alarm"
    assert res["lost_critical_objects_count"] == 0, "Mode C prioritizes and saves critical objects"
    assert res["bytes_moved"] > 0
    assert res["integrity_success_rate"] == 1.0


def test_run_single_experiment_mode_c_plus_size_aware():
    # Mode C+ with gamma=1.0 size-aware prioritization
    res = run_single_experiment(
        mode="mode_c_plus_1.0",
        outcome="true_alarm",
        lead_time_s=10.0,
        seed=42,
    )

    assert res["recovery_mode"] == "mode_c_plus_1.0"
    assert res["gamma"] == 1.0
    assert res["lost_critical_objects_count"] == 0, "Size-aware prioritization protects critical objects"


def test_mode_comparison_false_alarm():
    # In false alarm:
    # Mode A creates 0 jobs and moves 0 bytes.
    # Predictive modes move bytes, and all are classified as wasted_bytes.
    res_a = run_single_experiment(mode="mode_a", outcome="false_alarm", lead_time_s=0.0, seed=42)
    res_c = run_single_experiment(mode="mode_c", outcome="false_alarm", lead_time_s=0.0, seed=42)

    assert res_a["wasted_bytes"] == 0
    assert res_a["bytes_moved"] == 0
    assert res_c["wasted_bytes"] > 0
    assert res_c["wasted_bytes"] == res_c["bytes_moved"]
    assert res_c["lost_objects_count"] == 0


def test_mode_comparison_missed_prediction():
    # In missed prediction: all modes should degrade gracefully to reactive re-replication
    res_a = run_single_experiment(mode="mode_a", outcome="missed_prediction", lead_time_s=0.0, seed=42)
    res_b = run_single_experiment(mode="mode_b", outcome="missed_prediction", lead_time_s=0.0, seed=42)

    assert res_a["lost_objects_count"] > 0
    assert res_b["lost_objects_count"] > 0
    # Both should have identical lost counts because predictive modes degrade to reactive
    assert res_a["lost_objects_count"] == res_b["lost_objects_count"]


# =============================================================================
# 3. Full Matrix Execution & Data Export (CSV/JSON)
# =============================================================================

def test_run_full_experiment_matrix(tmp_path):
    lead_times = [10.0, 30.0]
    runs, summary = run_full_experiment_matrix(output_dir=str(tmp_path), lead_times=lead_times, seed=42)

    # 5 modes * 2 lead_times (true_alarm) + 5 modes (false_alarm) + 5 modes (missed_prediction) = 20 runs
    assert len(runs) == 20
    assert summary["total_runs"] == 20
    assert "by_mode" in summary
    assert "by_outcome" in summary

    # Verify CSV file
    csv_path = tmp_path / "experiments_summary.csv"
    assert csv_path.is_file()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        csv_rows = list(reader)
    assert len(csv_rows) == 20
    assert "mttr_s" in csv_rows[0]
    assert "lost_critical_objects_count" in csv_rows[0]
    assert "wasted_bytes" in csv_rows[0]

    # Verify JSON file
    json_path = tmp_path / "experiments_summary.json"
    assert json_path.is_file()
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["total_runs"] == 20
    assert len(data["runs"]) == 20


# =============================================================================
# 4. REST API Endpoint: GET /experiments
# =============================================================================

def test_api_get_experiments(client):
    response = client.get("/experiments")
    assert response.status_code == 200
    data = response.json()

    assert "total" in data
    assert "runs" in data
    assert "summary" in data
    assert data["total"] > 0


def test_api_get_experiments_filters(client):
    # Filter by recovery_mode
    response = client.get("/experiments?recovery_mode=mode_a")
    assert response.status_code == 200
    data = response.json()
    assert all(r["recovery_mode"] == "mode_a" for r in data["runs"])

    # Filter by outcome
    response = client.get("/experiments?outcome=false_alarm")
    assert response.status_code == 200
    data = response.json()
    assert all(r["outcome"] == "false_alarm" for r in data["runs"])
