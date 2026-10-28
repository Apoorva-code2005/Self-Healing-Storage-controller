from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient

from app.config import NodeConfig
from app.main import app, get_adapter
from app.storage.adapter import NodeReachability, StorageAdapter


@pytest.fixture
def client():
    return TestClient(app)


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["app"] == "self-healing-storage"
    assert data["version"] == "0.1.0"
    assert data["env"] == "development"


def test_nodes_endpoint_all_reachable(client):
    mock_adapter = MagicMock()
    mock_adapter.check_all_nodes.return_value = [
        NodeReachability(
            node_id="node-1",
            endpoint_url="http://127.0.0.1:9001",
            reachable=True,
            latency_ms=1.45,
        ),
        NodeReachability(
            node_id="node-2",
            endpoint_url="http://127.0.0.1:9002",
            reachable=True,
            latency_ms=1.82,
        ),
        NodeReachability(
            node_id="node-3",
            endpoint_url="http://127.0.0.1:9003",
            reachable=True,
            latency_ms=1.21,
        ),
    ]

    app.dependency_overrides[get_adapter] = lambda: mock_adapter
    try:
        response = client.get("/nodes")
        assert response.status_code == 200
        nodes = response.json()
        assert len(nodes) == 3
        assert nodes[0]["node_id"] == "node-1"
        assert nodes[0]["reachable"] is True
        assert nodes[0]["latency_ms"] == 1.45
        assert nodes[1]["reachable"] is True
        assert nodes[2]["reachable"] is True
    finally:
        app.dependency_overrides.clear()


def test_nodes_endpoint_with_unreachable_and_fenced_nodes(client):
    mock_adapter = MagicMock()
    mock_adapter.check_all_nodes.return_value = [
        NodeReachability(
            node_id="node-1",
            endpoint_url="http://127.0.0.1:9001",
            reachable=True,
            latency_ms=1.1,
        ),
        NodeReachability(
            node_id="node-2",
            endpoint_url="http://127.0.0.1:9002",
            reachable=False,
            fenced=False,
            error="Connection refused",
        ),
        NodeReachability(
            node_id="node-3",
            endpoint_url="http://127.0.0.1:9003",
            reachable=False,
            fenced=True,
            error="Node is fenced (simulated failure)",
        ),
    ]

    app.dependency_overrides[get_adapter] = lambda: mock_adapter
    try:
        response = client.get("/nodes")
        assert response.status_code == 200
        nodes = response.json()
        assert len(nodes) == 3

        # node-1 healthy
        assert nodes[0]["reachable"] is True

        # node-2 unreachable
        assert nodes[1]["node_id"] == "node-2"
        assert nodes[1]["reachable"] is False
        assert nodes[1]["error"] == "Connection refused"

        # node-3 fenced
        assert nodes[2]["node_id"] == "node-3"
        assert nodes[2]["reachable"] is False
        assert nodes[2]["fenced"] is True
        assert "fenced" in nodes[2]["error"]
    finally:
        app.dependency_overrides.clear()


def test_storage_adapter_connectivity_and_fencing(monkeypatch):
    node_cfgs = [
        NodeConfig(
            id="node-test",
            name="Test Node",
            endpoint_url="http://127.0.0.1:9999",
            access_key="user",
            secret_key="pass",
        )
    ]
    adapter = StorageAdapter(node_cfgs)

    # Mock boto3 client list_buckets to succeed
    mock_boto_client = MagicMock()
    mock_boto_client.list_buckets.return_value = {"Buckets": []}
    adapter._clients["node-test"] = mock_boto_client

    # Check healthy reachability
    reachability = adapter.check_node_connectivity("node-test")
    assert reachability.reachable is True
    assert reachability.fenced is False
    assert reachability.latency_ms is not None
    assert reachability.error is None

    # Fence the node (simulated failure per Section 7.1)
    adapter.fence_node("node-test")
    fenced_reachability = adapter.check_node_connectivity("node-test")
    assert fenced_reachability.reachable is False
    assert fenced_reachability.fenced is True
    assert "fenced" in fenced_reachability.error

    # Unfence the node
    adapter.unfence_node("node-test")
    unfenced_reachability = adapter.check_node_connectivity("node-test")
    assert unfenced_reachability.reachable is True
    assert unfenced_reachability.fenced is False
