"""Tests for Phase 7: Streamlit Dashboard and Observability."""
from __future__ import annotations

import unittest.mock as mock
import pytest

from dashboard.api_client import (
    ControllerClient,
    format_bytes,
    format_risk_level,
)


def test_format_bytes():
    assert format_bytes(0) == "0 B"
    assert format_bytes(500) == "500 B"
    assert format_bytes(1024) == "1.0 KiB"
    assert format_bytes(1024 * 1024) == "1.0 MiB"
    assert format_bytes(1024 * 1024 * 1024 * 5) == "5.0 GiB"
    assert format_bytes(None) == "0 B"


def test_format_risk_level():
    assert format_risk_level(0.0) == "LOW"
    assert format_risk_level(0.15) == "LOW"
    assert format_risk_level(0.3) == "MEDIUM"
    assert format_risk_level(0.65) == "HIGH"
    assert format_risk_level(0.85) == "CRITICAL"
    assert format_risk_level(None) == "UNKNOWN"


def test_client_get_health():
    client = ControllerClient(base_url="http://test-server")
    mock_resp = mock.MagicMock()
    mock_resp.json.return_value = {"status": "ok", "app": "self-healing-storage", "version": "0.1.0"}

    with mock.patch("requests.get", return_value=mock_resp) as mock_get:
        res = client.get_health()
        mock_get.assert_called_once_with("http://test-server/health", timeout=5.0)
        assert res["status"] == "ok"


def test_client_get_nodes():
    client = ControllerClient(base_url="http://test-server")
    mock_resp = mock.MagicMock()
    mock_resp.json.return_value = [
        {"node_id": "n1", "reachable": True, "status": "healthy", "virtual_capacity_bytes": 1000, "free_bytes": 800}
    ]

    with mock.patch("requests.get", return_value=mock_resp) as mock_get:
        nodes = client.get_nodes()
        mock_get.assert_called_once_with("http://test-server/nodes", timeout=5.0)
        assert len(nodes) == 1
        assert nodes[0]["node_id"] == "n1"


def test_client_get_predictions():
    client = ControllerClient(base_url="http://test-server")
    mock_resp = mock.MagicMock()
    mock_resp.json.return_value = [
        {"node_id": "n1", "risk_score": 0.85, "anomaly_flag": 1, "explanation": "high latency"}
    ]

    with mock.patch("requests.get", return_value=mock_resp) as mock_get:
        preds = client.get_predictions(limit=10, node_id="n1")
        mock_get.assert_called_once_with(
            "http://test-server/predictions", params={"limit": 10, "node_id": "n1"}, timeout=5.0
        )
        assert len(preds) == 1
        assert preds[0]["anomaly_flag"] == 1


def test_client_get_priority_objects():
    client = ControllerClient(base_url="http://test-server")
    mock_resp = mock.MagicMock()
    mock_resp.json.return_value = {
        "total": 1,
        "items": [{"object_id": "obj-1", "priority": 0.95}],
        "weights": {"w1_criticality": 0.4},
    }

    with mock.patch("requests.get", return_value=mock_resp) as mock_get:
        res = client.get_priority_objects(node_id="n1", gamma=0.5, use_rate=True, limit=20)
        mock_get.assert_called_once_with(
            "http://test-server/priority",
            params={"gamma": 0.5, "use_rate": True, "limit": 20, "node_id": "n1"},
            timeout=5.0,
        )
        assert res["total"] == 1
        assert res["items"][0]["priority"] == 0.95


def test_client_get_recovery_status():
    client = ControllerClient(base_url="http://test-server")
    mock_resp = mock.MagicMock()
    mock_resp.json.return_value = {
        "jobs": [{"job_id": "j-1", "status": "succeeded"}],
        "summary": {"succeeded": 1},
        "lost_objects": ["obj-lost"],
    }

    with mock.patch("requests.get", return_value=mock_resp) as mock_get:
        res = client.get_recovery_status(limit=50)
        mock_get.assert_called_once_with(
            "http://test-server/recovery/status", params={"limit": 50}, timeout=5.0
        )
        assert len(res["jobs"]) == 1
        assert res["lost_objects"] == ["obj-lost"]


def test_client_trigger_recovery_and_simulate_failure():
    client = ControllerClient(base_url="http://test-server")

    # Recovery
    mock_resp_rec = mock.MagicMock()
    mock_resp_rec.json.return_value = {"node_id": "n1", "mode": "priority", "jobs": []}
    with mock.patch("requests.post", return_value=mock_resp_rec) as mock_post:
        res_rec = client.trigger_recovery(node_id="n1", mode="priority")
        mock_post.assert_called_once_with(
            "http://test-server/recovery/n1", params={"mode": "priority"}, timeout=5.0
        )
        assert res_rec["node_id"] == "n1"

    # Simulate failure
    mock_resp_sim = mock.MagicMock()
    mock_resp_sim.json.return_value = {"scenario_name": "true_alarm", "ticks_advanced": 5}
    with mock.patch("requests.post", return_value=mock_resp_sim) as mock_post:
        res_sim = client.simulate_failure(scenario_name="true_alarm", advance_ticks=5, seed=42)
        mock_post.assert_called_once_with(
            "http://test-server/simulate-failure",
            json={"scenario_name": "true_alarm", "advance_ticks": 5, "seed": 42},
            timeout=5.0,
        )
        assert res_sim["ticks_advanced"] == 5
