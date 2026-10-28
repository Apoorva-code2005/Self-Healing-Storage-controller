from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional
import yaml
from pydantic import BaseModel, Field, model_validator


class NodeConfig(BaseModel):
    id: str
    name: str
    endpoint_url: str
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    region: str = "us-east-1"
    virtual_capacity_bytes: int = Field(default=10 * 1024 * 1024 * 1024)  # 10 GiB virtual quota


class PolicyConfig(BaseModel):
    default_replica_count: int = 2
    reserve_margin_ratio: float = 0.10  # 10% of node virtual capacity per PRD Section 24
    reserve_margin_bytes: int = 1073741824  # 1 GiB (10% of 10 GiB node virtual quota)
    max_destination_risk: float = 0.5
    max_destination_load: float = 0.8
    gamma: float = 0.0
    lease_ttl_s: int = 300  # scenario-clock seconds; per PRD Section 7.5
    bandwidth_bytes_per_s: int = 10 * 1024 * 1024  # 10 MiB/s simulated
    evacuation_backoff_base_s: float = 1.0  # exponential back-off base (1, 2, 4 s)


class WeightsConfig(BaseModel):
    w1_criticality: float = 0.40
    w2_access_frequency: float = 0.20
    w3_node_risk: float = 0.25
    w4_redundancy: float = 0.15

    @model_validator(mode="after")
    def validate_non_negative(self) -> WeightsConfig:
        for name, val in [
            ("w1_criticality", self.w1_criticality),
            ("w2_access_frequency", self.w2_access_frequency),
            ("w3_node_risk", self.w3_node_risk),
            ("w4_redundancy", self.w4_redundancy),
        ]:
            if val < 0.0:
                raise ValueError(f"Weight '{name}' must be non-negative, got {val}")
        return self


class ThresholdsConfig(BaseModel):
    risk_threshold: float = 0.7
    latency_threshold_ms: float = 50.0
    error_rate_threshold: float = 0.05


class AppInfo(BaseModel):
    name: str = "self-healing-storage"
    version: str = "0.1.0"
    env: str = "development"


class AppConfig(BaseModel):
    app: AppInfo = Field(default_factory=AppInfo)
    nodes: List[NodeConfig] = Field(default_factory=list)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    weights: WeightsConfig = Field(default_factory=WeightsConfig)
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)


def find_config_path(explicit_path: Optional[str | Path] = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if path.is_file():
            return path
        raise FileNotFoundError(f"Config file not found at: {explicit_path}")

    env_path = os.getenv("CONFIG_PATH")
    if env_path:
        path = Path(env_path)
        if path.is_file():
            return path
        raise FileNotFoundError(f"Config file specified in CONFIG_PATH not found at: {env_path}")

    # Fallback search locations
    candidates = [
        Path("config/default.yaml"),
        Path("../config/default.yaml"),
        Path(__file__).resolve().parent.parent / "config" / "default.yaml",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate

    raise FileNotFoundError("Default configuration file config/default.yaml not found.")


def load_config(config_path: Optional[str | Path] = None) -> AppConfig:
    resolved_path = find_config_path(config_path)
    with open(resolved_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # Allow overriding root user and password from environment
    default_user = os.getenv("MINIO_ROOT_USER")
    default_password = os.getenv("MINIO_ROOT_PASSWORD")

    if "nodes" in data and isinstance(data["nodes"], list):
        for node in data["nodes"]:
            if default_user and node.get("access_key") == "minioadmin":
                node["access_key"] = default_user
            if default_password and node.get("secret_key") == "minioadmin":
                node["secret_key"] = default_password

    return AppConfig(**data)
