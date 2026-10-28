from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.config import AppConfig, ThresholdsConfig
from app.db.repository import record_prediction, record_telemetry
from app.storage.adapter import StorageAdapter

logger = logging.getLogger("self_healing_storage.telemetry")


def log_structured_event(event_type: str, data: Dict[str, Any]) -> None:
    """Logs structured JSON event omitting sensitive credentials/secrets."""
    sanitized = {
        "event": event_type,
        **{
            k: v
            for k, v in data.items()
            if not any(s in k.lower() for s in ("secret", "password", "token", "key"))
        },
    }
    logger.info(json.dumps(sanitized))


def evaluate_baseline_risk(
    telemetry_record: Dict[str, Any],
    thresholds: ThresholdsConfig,
) -> Tuple[float, bool, str]:
    """Baseline non-ML detector using threshold rules on latency and error rate.

    Returns:
        (risk_score in [0, 1], anomaly_flag: bool, explanation: str)
    """
    reachable = bool(telemetry_record.get("reachable", True))
    if not reachable:
        return 1.0, True, "Node is unreachable or connection failed"

    lat = float(telemetry_record.get("latency_ms") or 0.0)
    err = float(telemetry_record.get("error_rate") or 0.0)

    lat_thresh = thresholds.latency_threshold_ms
    err_thresh = thresholds.error_rate_threshold
    risk_thresh = thresholds.risk_threshold

    signals = []
    if lat >= lat_thresh:
        signals.append(f"latency_ms={lat:.2f} >= {lat_thresh:.2f}")
    if err >= err_thresh:
        signals.append(f"error_rate={err:.4f} >= {err_thresh:.4f}")

    if signals:
        # Anomaly flagged: risk score scaled into [risk_thresh, 1.0]
        lat_ratio = (lat - lat_thresh) / lat_thresh if lat_thresh > 0 else 0.0
        err_ratio = (err - err_thresh) / err_thresh if err_thresh > 0 else 0.0
        severity = min(1.0, max(lat_ratio, err_ratio) * 0.4)
        risk_score = min(1.0, round(risk_thresh + (1.0 - risk_thresh) * severity, 4))
        explanation = "Threshold exceeded: " + "; ".join(signals)
        return risk_score, True, explanation
    else:
        # Nominal operation: risk score strictly below risk_thresh
        lat_ratio = min(1.0, lat / lat_thresh) if lat_thresh > 0 else 0.0
        err_ratio = min(1.0, err / err_thresh) if err_thresh > 0 else 0.0
        risk_score = min(round(risk_thresh - 0.01, 4), round(max(lat_ratio, err_ratio) * 0.35, 4))
        return risk_score, False, "Nominal operation: metrics within thresholds"


def compute_node_status(
    reachable: bool,
    fenced: bool,
    anomaly_flag: bool,
    risk_score: float,
    risk_threshold: float = 0.7,
) -> str:
    """Computes node status per PRD 7.3: healthy, degraded, failed, fenced."""
    if fenced:
        return "fenced"
    if not reachable:
        return "failed"
    if anomaly_flag or risk_score >= risk_threshold:
        return "degraded"
    return "healthy"


def collect_observed_telemetry(
    adapter: StorageAdapter,
    conn: Any,
    config: AppConfig,
    timestamp_utc: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Gathers observed reachability/latency from storage adapter and records into SQLite."""
    ts = timestamp_utc or datetime.now(timezone.utc).isoformat()
    reachabilities = adapter.check_all_nodes()
    records = []

    for r in reachabilities:
        telem = {
            "telemetry_id": str(uuid.uuid4()),
            "node_id": r.node_id,
            "timestamp_utc": ts,
            "latency_ms": r.latency_ms,
            "error_rate": 0.0 if r.reachable else 1.0,
            "temperature_c": None,
            "capacity_free_bytes": r.free_bytes,
            "load_metric": 0.0,
            "reachable": r.reachable,
            "source_type": "observed",
            "replay_drive_serial": None,
            "replay_source_date": None,
        }
        record_telemetry(conn, telem)
        records.append(telem)

        # Baseline detection
        risk_score, anomaly_flag, explanation = evaluate_baseline_risk(
            telem, config.thresholds
        )
        status = compute_node_status(
            r.reachable,
            r.fenced,
            anomaly_flag,
            risk_score,
            config.thresholds.risk_threshold,
        )

        pred = {
            "prediction_id": str(uuid.uuid4()),
            "node_id": r.node_id,
            "timestamp_utc": ts,
            "risk_score": risk_score,
            "model_name": "baseline_threshold",
            "model_version": "1.0.0",
            "target_horizon": "10s",
            "anomaly_flag": anomaly_flag,
            "explanation": explanation,
        }
        record_prediction(conn, pred, node_status=status)

        # Structured event logging
        log_structured_event(
            "telemetry_observed",
            {
                "node_id": r.node_id,
                "timestamp": ts,
                "latency_ms": r.latency_ms,
                "reachable": r.reachable,
                "status": status,
                "risk_score": risk_score,
            },
        )
        if anomaly_flag:
            log_structured_event(
                "anomaly_detected",
                {
                    "node_id": r.node_id,
                    "timestamp": ts,
                    "risk_score": risk_score,
                    "explanation": explanation,
                },
            )

    return records


def ingest_telemetry_record(
    conn: Any,
    record: Dict[str, Any],
    config: AppConfig,
    is_fenced: bool = False,
) -> Dict[str, Any]:
    """Ingests a synthetic or replayed telemetry record, evaluates risk, and updates status."""
    if "telemetry_id" not in record:
        record["telemetry_id"] = str(uuid.uuid4())
    if "timestamp_utc" not in record:
        record["timestamp_utc"] = datetime.now(timezone.utc).isoformat()

    record_telemetry(conn, record)

    risk_score, anomaly_flag, explanation = evaluate_baseline_risk(
        record, config.thresholds
    )
    status = compute_node_status(
        bool(record.get("reachable", True)),
        is_fenced,
        anomaly_flag,
        risk_score,
        config.thresholds.risk_threshold,
    )

    pred = {
        "prediction_id": str(uuid.uuid4()),
        "node_id": record["node_id"],
        "timestamp_utc": record["timestamp_utc"],
        "risk_score": risk_score,
        "model_name": "baseline_threshold",
        "model_version": "1.0.0",
        "target_horizon": "10s",
        "anomaly_flag": anomaly_flag,
        "explanation": explanation,
    }
    record_prediction(conn, pred, node_status=status)

    log_structured_event(
        "telemetry_ingested",
        {
            "node_id": record["node_id"],
            "timestamp": record["timestamp_utc"],
            "source_type": record.get("source_type", "simulated"),
            "latency_ms": record.get("latency_ms"),
            "error_rate": record.get("error_rate"),
            "status": status,
            "risk_score": risk_score,
        },
    )
    if anomaly_flag:
        log_structured_event(
            "anomaly_detected",
            {
                "node_id": record["node_id"],
                "timestamp": record["timestamp_utc"],
                "risk_score": risk_score,
                "explanation": explanation,
            },
        )

    return {
        "prediction": pred,
        "node_status": status,
    }
