from __future__ import annotations

import json
from pathlib import Path
import random
from typing import Any, Dict, List, Optional
import yaml

from app.config import AppConfig
from app.db.repository import save_scenario
from app.services.scenario_clock import ScenarioClock
from app.services.telemetry import ingest_telemetry_record, log_structured_event
from app.storage.adapter import StorageAdapter


def find_scenario_file(scenario_name: str) -> Path:
    """Locates a scenario YAML or JSON file in config/scenarios/."""
    candidates = [
        Path(f"config/scenarios/{scenario_name}.yaml"),
        Path(f"config/scenarios/{scenario_name}.yml"),
        Path(f"config/scenarios/{scenario_name}.json"),
        Path(f"config/scenarios/{scenario_name}"),
        Path(__file__).resolve().parent.parent.parent / "config" / "scenarios" / f"{scenario_name}.yaml",
        Path(__file__).resolve().parent.parent.parent / "config" / "scenarios" / f"{scenario_name}.json",
    ]
    for c in candidates:
        if c.is_file():
            return c
    raise FileNotFoundError(f"Scenario '{scenario_name}' not found in config/scenarios/")


def load_scenario(scenario_name: str) -> Dict[str, Any]:
    """Loads and validates a scenario definition."""
    path = find_scenario_file(scenario_name)
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data


def generate_synthetic_telemetry(
    node_id: str,
    tick: int,
    seed: int,
    active_faults: Dict[str, Any],
    is_fenced: bool,
    timestamp_utc: str,
    virtual_capacity_bytes: int = 10737418240,
) -> Dict[str, Any]:
    """Generates deterministic synthetic node telemetry using PRNG seeded with (seed, tick, node_id)."""
    # Deterministic seed per tick and node
    rng = random.Random(seed * 10007 + tick * 31 + abs(hash(node_id)) % 1000)

    base_latency = round(5.0 + rng.uniform(-0.5, 0.5), 2)
    base_error = 0.0
    base_load = round(0.1 + rng.uniform(-0.02, 0.02), 2)

    if is_fenced:
        return {
            "node_id": node_id,
            "timestamp_utc": timestamp_utc,
            "latency_ms": None,
            "error_rate": 1.0,
            "temperature_c": None,
            "capacity_free_bytes": 0,
            "load_metric": 0.0,
            "reachable": False,
            "source_type": "simulated",
            "replay_drive_serial": None,
            "replay_source_date": None,
        }

    lat = active_faults.get("latency_ms", base_latency)
    err = active_faults.get("error_rate", base_error)
    load = active_faults.get("load_metric", base_load)
    cap = active_faults.get("capacity_free_bytes", virtual_capacity_bytes)

    return {
        "node_id": node_id,
        "timestamp_utc": timestamp_utc,
        "latency_ms": float(lat),
        "error_rate": float(err),
        "temperature_c": None,
        "capacity_free_bytes": int(cap),
        "load_metric": float(load),
        "reachable": True,
        "source_type": "simulated",
        "replay_drive_serial": None,
        "replay_source_date": None,
    }


def execute_scenario(
    scenario_data: Dict[str, Any],
    adapter: StorageAdapter,
    db_conn: Any,
    config: AppConfig,
    clock: Optional[ScenarioClock] = None,
    max_ticks: Optional[int] = None,
    override_seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Executes a scenario using the ScenarioClock, StorageAdapter fencing, and baseline telemetry ingestion."""
    clock = clock or ScenarioClock()
    clock.reset()

    scenario_id = scenario_data.get("scenario_id", "unnamed_scenario")
    name = scenario_data.get("name", scenario_id)
    scenario_type = scenario_data.get("scenario_type", "synthetic")
    prediction_outcome = scenario_data.get("prediction_outcome", "unknown")
    lead_time_s = scenario_data.get("lead_time_s")
    seed = override_seed if override_seed is not None else scenario_data.get("seed", 42)
    target_node_id = scenario_data.get("target_node_id")

    faults: List[Dict[str, Any]] = scenario_data.get("faults", [])

    # Persist scenario definition into SQLite
    save_scenario(
        db_conn,
        {
            "scenario_id": scenario_id,
            "name": name,
            "scenario_type": scenario_type,
            "prediction_outcome": prediction_outcome,
            "lead_time_s": lead_time_s,
            "seed": seed,
            "node_faults_json": json.dumps(faults),
            "replay_drive_map_json": None,
            "initial_dataset_json": None,
        },
    )

    # Determine simulation length
    fault_ticks = [f.get("tick", 0) for f in faults]
    calculated_max = (max(fault_ticks) + 2) if fault_ticks else 10
    total_ticks = max_ticks if max_ticks is not None else calculated_max

    active_faults_by_node: Dict[str, Dict[str, Any]] = {n.id: {} for n in config.nodes}

    alarm_tick: Optional[int] = None
    failure_tick: Optional[int] = None
    final_node_statuses: Dict[str, str] = {}

    for tick_idx in range(total_ticks):
        current_time_iso = clock.now_iso()

        # Apply scheduled faults for this tick
        for fault in faults:
            if fault.get("tick") == tick_idx:
                f_type = fault.get("type")
                f_node = fault.get("node_id")

                log_structured_event(
                    "failure_injection",
                    {
                        "scenario_id": scenario_id,
                        "tick": tick_idx,
                        "timestamp": current_time_iso,
                        "fault_type": f_type,
                        "node_id": f_node,
                    },
                )

                if f_type == "node_unavailable":
                    adapter.fence_node(f_node)
                    if f_node == target_node_id and failure_tick is None:
                        failure_tick = tick_idx

                elif f_type == "latency_spike":
                    active_faults_by_node.setdefault(f_node, {})["latency_ms"] = fault.get(
                        "latency_ms", 120.0
                    )

                elif f_type == "error_rate_increase":
                    active_faults_by_node.setdefault(f_node, {})["error_rate"] = fault.get(
                        "error_rate", 0.1
                    )

                elif f_type == "low_capacity":
                    active_faults_by_node.setdefault(f_node, {})["capacity_free_bytes"] = fault.get(
                        "capacity_free_bytes", 1024
                    )

        # Ingest telemetry for all configured nodes at this tick
        for node in config.nodes:
            is_fenced = adapter.is_fenced(node.id)
            node_faults = active_faults_by_node.get(node.id, {})
            telem = generate_synthetic_telemetry(
                node_id=node.id,
                tick=tick_idx,
                seed=seed,
                active_faults=node_faults,
                is_fenced=is_fenced,
                timestamp_utc=current_time_iso,
                virtual_capacity_bytes=node.virtual_capacity_bytes,
            )

            result = ingest_telemetry_record(
                conn=db_conn,
                record=telem,
                config=config,
                is_fenced=is_fenced,
            )

            pred = result["prediction"]
            final_node_statuses[node.id] = result["node_status"]

            # Check if target node crossed risk threshold
            if (
                node.id == target_node_id
                and pred.get("anomaly_flag")
                and alarm_tick is None
                and not is_fenced
            ):
                alarm_tick = tick_idx

        # Advance scenario clock by 1 tick (1 second)
        clock.tick()

    measured_lead_time_s = None
    if alarm_tick is not None and failure_tick is not None:
        measured_lead_time_s = float((failure_tick - alarm_tick) * clock.tick_size_s)

    return {
        "scenario_id": scenario_id,
        "name": name,
        "prediction_outcome": prediction_outcome,
        "seed": seed,
        "lead_time_s": lead_time_s,
        "ticks_simulated": total_ticks,
        "alarm_tick": alarm_tick,
        "failure_tick": failure_tick,
        "measured_lead_time_s": measured_lead_time_s,
        "node_statuses": final_node_statuses,
    }
