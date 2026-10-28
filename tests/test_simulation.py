from datetime import datetime, timezone
import json
import pytest
from fastapi.testclient import TestClient

from app.config import AppConfig, NodeConfig
from app.db import create_connection, run_migrations, sync_nodes
from app.main import create_app, get_adapter, get_db_session
from app.services.failure_injection import execute_scenario, generate_synthetic_telemetry, load_scenario
from app.services.scenario_clock import ScenarioClock
from app.services.telemetry import evaluate_baseline_risk
from app.storage.adapter import InMemoryStorageAdapter


@pytest.fixture
def test_sim_setup(tmp_path):
    """Sets up isolated db, clock, and InMemoryStorageAdapter for testing."""
    nodes = [
        NodeConfig(
            id="node-1",
            name="Node 1",
            endpoint_url="http://127.0.0.1:9001",
            virtual_capacity_bytes=10000,
        ),
        NodeConfig(
            id="node-2",
            name="Node 2",
            endpoint_url="http://127.0.0.1:9002",
            virtual_capacity_bytes=10000,
        ),
        NodeConfig(
            id="node-3",
            name="Node 3",
            endpoint_url="http://127.0.0.1:9003",
            virtual_capacity_bytes=10000,
        ),
    ]
    cfg = AppConfig(nodes=nodes)
    db_file = tmp_path / "test_sim.db"
    conn = create_connection(db_file)
    run_migrations(conn)
    sync_nodes(conn, nodes)

    adapter = InMemoryStorageAdapter(nodes)
    app = create_app(cfg=cfg, adapter=adapter, db_conn=conn)

    def override_get_db():
        yield conn

    def override_get_adapter():
        return adapter

    app.dependency_overrides[get_db_session] = override_get_db
    app.dependency_overrides[get_adapter] = override_get_adapter

    client = TestClient(app)
    return {
        "client": client,
        "conn": conn,
        "adapter": adapter,
        "cfg": cfg,
        "nodes": nodes,
    }


def test_clock_behavior():
    """Validates deterministic clock advancing and resetting without real sleeping."""
    start = datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)
    clock = ScenarioClock(tick_size_s=1.0, start_time_utc=start)

    assert clock.current_time_s == 0.0
    assert clock.current_tick == 0
    assert clock.now_utc() == start
    assert "2026-10-28T12:00:00" in clock.now_iso()

    # Advance 1 tick
    t1 = clock.tick()
    assert t1 == 1.0
    assert clock.current_tick == 1
    assert clock.current_time_s == 1.0

    # Advance 4 more ticks
    clock.tick(4)
    assert clock.current_tick == 5
    assert clock.current_time_s == 5.0

    # Advance arbitrary seconds
    clock.advance(2.5)
    assert clock.current_time_s == 7.5

    # Reset
    clock.reset()
    assert clock.current_time_s == 0.0
    assert clock.current_tick == 0


def test_same_seed_determinism(test_sim_setup):
    """Verifies that running synthetic telemetry with the same seed produces identical outputs."""
    seed = 42
    target_node = "node-2"
    ts = "2026-10-28T12:00:00+00:00"

    # Run 1
    run1 = [
        generate_synthetic_telemetry(
            node_id=target_node,
            tick=t,
            seed=seed,
            active_faults={},
            is_fenced=False,
            timestamp_utc=ts,
        )
        for t in range(10)
    ]

    # Run 2 with identical seed
    run2 = [
        generate_synthetic_telemetry(
            node_id=target_node,
            tick=t,
            seed=seed,
            active_faults={},
            is_fenced=False,
            timestamp_utc=ts,
        )
        for t in range(10)
    ]

    assert run1 == run2

    # Different seed must yield different outputs
    run_diff_seed = [
        generate_synthetic_telemetry(
            node_id=target_node,
            tick=t,
            seed=999,
            active_faults={},
            is_fenced=False,
            timestamp_utc=ts,
        )
        for t in range(10)
    ]
    assert run1 != run_diff_seed


def test_detector_flags_latency_spike(test_sim_setup):
    """Validates baseline detector identifies latency anomalies above threshold."""
    cfg = test_sim_setup["cfg"]

    # Normal latency (5ms <= 50ms)
    nominal_record = {
        "node_id": "node-1",
        "latency_ms": 5.0,
        "error_rate": 0.0,
        "reachable": True,
    }
    risk, anomaly, exp = evaluate_baseline_risk(nominal_record, cfg.thresholds)
    assert anomaly is False
    assert risk < cfg.thresholds.risk_threshold
    assert "Nominal" in exp

    # Latency spike (120ms >= 50ms)
    spike_record = {
        "node_id": "node-1",
        "latency_ms": 120.0,
        "error_rate": 0.0,
        "reachable": True,
    }
    risk, anomaly, exp = evaluate_baseline_risk(spike_record, cfg.thresholds)
    assert anomaly is True
    assert risk >= cfg.thresholds.risk_threshold
    assert "latency_ms=120.00" in exp


def test_true_alarm_scenario(test_sim_setup):
    """Tests true_alarm: risk crosses threshold before failing at lead_time_s."""
    client = test_sim_setup["client"]
    adapter = test_sim_setup["adapter"]

    response = client.post("/simulate-failure", json={"scenario_name": "true_alarm"})
    assert response.status_code == 200, response.text
    data = response.json()

    assert data["prediction_outcome"] == "true_alarm"
    assert data["alarm_tick"] == 2
    assert data["failure_tick"] == 7
    assert data["measured_lead_time_s"] == 5.0
    assert data["node_statuses"]["node-2"] == "fenced"
    assert adapter.is_fenced("node-2") is True

    # GET /nodes must report node-2 as fenced with risk_score = 1.0
    nodes = {n["node_id"]: n for n in client.get("/nodes").json()}
    assert nodes["node-2"]["status"] == "fenced"
    assert nodes["node-2"]["risk_score"] == 1.0


def test_false_alarm_scenario(test_sim_setup):
    """Tests false_alarm: risk crosses threshold but node never hard-fails."""
    client = test_sim_setup["client"]
    adapter = test_sim_setup["adapter"]

    response = client.post("/simulate-failure", json={"scenario_name": "false_alarm"})
    assert response.status_code == 200, response.text
    data = response.json()

    assert data["prediction_outcome"] == "false_alarm"
    assert data["alarm_tick"] == 2
    assert data["failure_tick"] is None  # Node never failed
    assert adapter.is_fenced("node-2") is False

    # Node is degraded due to alarm, but reachable and not fenced
    nodes = {n["node_id"]: n for n in client.get("/nodes").json()}
    assert nodes["node-2"]["status"] == "degraded"
    assert nodes["node-2"]["risk_score"] >= 0.7
    assert nodes["node-2"]["reachable"] is True
    assert nodes["node-2"]["fenced"] is False


def test_missed_prediction_scenario(test_sim_setup):
    """Tests missed prediction: node hard-fails without a prior threshold crossing."""
    client = test_sim_setup["client"]
    adapter = test_sim_setup["adapter"]

    response = client.post("/simulate-failure", json={"scenario_name": "missed_prediction"})
    assert response.status_code == 200, response.text
    data = response.json()

    assert data["prediction_outcome"] == "missed"
    # Never crossed threshold prior to hard failure
    assert data["alarm_tick"] is None
    assert data["failure_tick"] == 4
    assert adapter.is_fenced("node-2") is True


def test_simulate_failure_refused_outside_development(test_sim_setup, monkeypatch):
    """Verifies that failure injection is rejected with HTTP 403 when ENV != development."""
    client = test_sim_setup["client"]
    cfg = test_sim_setup["cfg"]

    # Set environment to production
    cfg.app.env = "production"
    monkeypatch.setenv("ENV", "production")

    response = client.post("/simulate-failure", json={"scenario_name": "true_alarm"})
    assert response.status_code == 403
    assert "development mode" in response.json()["detail"]


def test_get_predictions_endpoint(test_sim_setup):
    """Validates GET /predictions returns latest predictions with anomaly flags and explanation."""
    client = test_sim_setup["client"]

    # Run false_alarm scenario to generate predictions
    client.post("/simulate-failure", json={"scenario_name": "false_alarm"})

    resp = client.get("/predictions?limit=10")
    assert resp.status_code == 200
    preds = resp.json()
    assert len(preds) > 0

    first = preds[0]
    assert "prediction_id" in first
    assert "node_id" in first
    assert "risk_score" in first
    assert "anomaly_flag" in first
    assert "explanation" in first
