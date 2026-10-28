from pathlib import Path
import pytest
from app.config import AppConfig, find_config_path, load_config


def test_default_config_loading():
    config = load_config()
    assert isinstance(config, AppConfig)
    assert config.app.name == "self-healing-storage"
    assert config.app.env == "development"

    # Verify 3 nodes per Section 6.1 / FR-01
    assert len(config.nodes) == 3
    node_ids = [n.id for n in config.nodes]
    assert node_ids == ["node-1", "node-2", "node-3"]

    # Verify 127.0.0.1 bindings and ports
    assert config.nodes[0].endpoint_url == "http://127.0.0.1:9001"
    assert config.nodes[1].endpoint_url == "http://127.0.0.1:9002"
    assert config.nodes[2].endpoint_url == "http://127.0.0.1:9003"

    # Verify virtual capacity quotas per Section 7.4
    for node in config.nodes:
        assert node.virtual_capacity_bytes == 10 * 1024 * 1024 * 1024


def test_policy_and_threshold_config():
    config = load_config()
    assert config.policy.default_replica_count == 2
    assert config.policy.reserve_margin_bytes == 1073741824
    assert config.policy.reserve_margin_ratio == 0.10
    assert config.policy.max_destination_risk == 0.5
    assert config.policy.max_destination_load == 0.8
    assert config.policy.gamma == 0.0
    assert config.thresholds.risk_threshold == 0.7
    assert config.weights.w1_criticality == 0.40
    assert config.weights.w2_access_frequency == 0.20
    assert config.weights.w3_node_risk == 0.25
    assert config.weights.w4_redundancy == 0.15


def test_missing_config_raises():
    with pytest.raises(FileNotFoundError):
        load_config(config_path="/nonexistent/path/config.yaml")


def test_env_override(monkeypatch):
    monkeypatch.setenv("MINIO_ROOT_USER", "custom_admin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "custom_secret")
    config = load_config()
    for node in config.nodes:
        assert node.access_key == "custom_admin"
        assert node.secret_key == "custom_secret"
