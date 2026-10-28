"""dashboard/api_client.py

Client functions and data formatting utilities for the Streamlit dashboard.
Interacts with the FastAPI controller REST endpoints.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import requests


def format_bytes(b: Optional[int | float]) -> str:
    """Formats raw bytes into a human-readable string (KiB, MiB, GiB)."""
    if b is None or b < 0:
        return "0 B"
    val = float(b)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if val < 1024.0 or unit == "TiB":
            return f"{val:.1f} {unit}" if unit != "B" else f"{int(val)} B"
        val /= 1024.0
    return f"{b} B"


def format_risk_level(score: Optional[float]) -> str:
    """Classifies numerical risk into descriptive categories."""
    if score is None:
        return "UNKNOWN"
    if score >= 0.8:
        return "CRITICAL"
    if score >= 0.5:
        return "HIGH"
    if score >= 0.2:
        return "MEDIUM"
    return "LOW"


class ControllerClient:
    """HTTP client for interacting with the Self-Healing Storage Controller."""

    def __init__(self, base_url: str = "http://127.0.0.1:8000", timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_health(self) -> Dict[str, Any]:
        resp = requests.get(f"{self.base_url}/health", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_nodes(self) -> List[Dict[str, Any]]:
        resp = requests.get(f"{self.base_url}/nodes", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_predictions(
        self, limit: int = 50, node_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": limit}
        if node_id:
            params["node_id"] = node_id
        resp = requests.get(f"{self.base_url}/predictions", params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_priority_objects(
        self,
        node_id: Optional[str] = None,
        gamma: float = 0.0,
        use_rate: bool = False,
        limit: int = 50,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "gamma": gamma,
            "use_rate": use_rate,
            "limit": limit,
        }
        if node_id:
            params["node_id"] = node_id
        resp = requests.get(f"{self.base_url}/priority", params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_recovery_status(self, limit: int = 50) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.base_url}/recovery/status", params={"limit": limit}, timeout=self.timeout
        )
        resp.raise_for_status()
        return resp.json()

    def trigger_recovery(self, node_id: str, mode: str = "priority") -> Dict[str, Any]:
        resp = requests.post(
            f"{self.base_url}/recovery/{node_id}",
            params={"mode": mode},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def simulate_failure(
        self, scenario_name: str, advance_ticks: int = 5, seed: int = 42
    ) -> Dict[str, Any]:
        payload = {
            "scenario_name": scenario_name,
            "advance_ticks": advance_ticks,
            "seed": seed,
        }
        resp = requests.post(
            f"{self.base_url}/simulate-failure",
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()
