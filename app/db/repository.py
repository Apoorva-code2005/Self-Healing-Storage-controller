from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from app.config import NodeConfig


def sync_nodes(conn: sqlite3.Connection, node_configs: List[NodeConfig]) -> None:
    """Synchronizes configured storage nodes into the metadata store."""
    cursor = conn.cursor()
    for node in node_configs:
        cursor.execute(
            """
            INSERT INTO nodes (node_id, name, endpoint, virtual_capacity_bytes)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                name = excluded.name,
                endpoint = excluded.endpoint,
                virtual_capacity_bytes = excluded.virtual_capacity_bytes
            """,
            (node.id, node.name, node.endpoint_url, node.virtual_capacity_bytes),
        )
    conn.commit()


def get_nodes_with_capacity(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """Calculates used_bytes and free_bytes virtual capacity accounting for all nodes."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT n.node_id, n.name, n.endpoint, n.status, n.virtual_capacity_bytes,
               n.load_metric, n.risk_score, n.risk_updated_at, n.telemetry_source, n.last_seen_at,
               COALESCE(SUM(CASE WHEN r.status != 'lost' THEN o.size_bytes ELSE 0 END), 0) AS used_bytes
        FROM nodes n
        LEFT JOIN replicas r ON r.node_id = n.node_id
        LEFT JOIN objects o ON o.object_id = r.object_id
        GROUP BY n.node_id
        """
    )
    result = {}
    for row in cursor.fetchall():
        v_cap = int(row["virtual_capacity_bytes"])
        used = int(row["used_bytes"])
        free = max(0, v_cap - used)
        result[row["node_id"]] = {
            "node_id": row["node_id"],
            "name": row["name"],
            "endpoint": row["endpoint"],
            "status": row["status"],
            "virtual_capacity_bytes": v_cap,
            "used_bytes": used,
            "free_bytes": free,
            "load_metric": float(row["load_metric"]),
            "risk_score": float(row["risk_score"]),
            "risk_updated_at": row["risk_updated_at"],
            "telemetry_source": row["telemetry_source"],
            "last_seen_at": row["last_seen_at"],
        }
    return result


def get_node(conn: sqlite3.Connection, node_id: str) -> Optional[Dict[str, Any]]:
    """Fetches a single node's metadata and virtual capacity."""
    all_nodes = get_nodes_with_capacity(conn)
    return all_nodes.get(node_id)


def choose_initial_placement(
    conn: sqlite3.Connection,
    reachable_node_ids: List[str],
    size_bytes: int,
) -> Optional[Dict[str, Any]]:
    """Selects the reachable node with the most free virtual capacity that can fit the object.

    Ties are broken deterministically by node_id in alphabetical order.
    """
    capacities = get_nodes_with_capacity(conn)
    candidates = []

    for nid in reachable_node_ids:
        if nid in capacities:
            info = capacities[nid]
            if info["free_bytes"] >= size_bytes:
                candidates.append((info["free_bytes"], nid, info))

    if not candidates:
        return None

    # Sort descending by free_bytes, ascending by node_id
    candidates.sort(key=lambda x: (-x[0], x[1]))
    return candidates[0][2]


def create_object_and_replica(
    conn: sqlite3.Connection,
    object_data: Dict[str, Any],
    replica_data: Dict[str, Any],
) -> None:
    """Inserts Object and initial authoritative Replica records in a transaction."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO objects (
            object_id, object_name, size_bytes, criticality, access_count,
            access_frequency_normalized, origin_node_id, checksum_sha256,
            required_replica_count, status, created_at, updated_at, last_access_at
        ) VALUES (
            :object_id, :object_name, :size_bytes, :criticality, :access_count,
            :access_frequency_normalized, :origin_node_id, :checksum_sha256,
            :required_replica_count, :status, :created_at, :updated_at, :last_access_at
        )
        """,
        object_data,
    )

    cursor.execute(
        """
        INSERT INTO replicas (
            replica_id, object_id, node_id, checksum_sha256, verified,
            is_authoritative, status, created_at, verified_at
        ) VALUES (
            :replica_id, :object_id, :node_id, :checksum_sha256, :verified,
            :is_authoritative, :status, :created_at, :verified_at
        )
        """,
        replica_data,
    )
    conn.commit()


def get_object(conn: sqlite3.Connection, object_id: str) -> Optional[Dict[str, Any]]:
    """Fetches an Object record with all its associated replicas."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM objects WHERE object_id = ?", (object_id,))
    row = cursor.fetchone()
    if not row:
        return None

    obj = dict(row)
    cursor.execute(
        "SELECT * FROM replicas WHERE object_id = ? ORDER BY created_at ASC",
        (object_id,),
    )
    obj["replicas"] = [
        {
            "replica_id": r["replica_id"],
            "object_id": r["object_id"],
            "node_id": r["node_id"],
            "checksum_sha256": r["checksum_sha256"],
            "verified": bool(r["verified"]),
            "is_authoritative": bool(r["is_authoritative"]),
            "status": r["status"],
            "created_at": r["created_at"],
            "verified_at": r["verified_at"],
        }
        for r in cursor.fetchall()
    ]
    return obj


def list_objects(
    conn: sqlite3.Connection,
    limit: int = 50,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """Returns a paginated list of objects with replica states and total count."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM objects")
    total = int(cursor.fetchone()[0])

    cursor.execute(
        """
        SELECT * FROM objects
        ORDER BY created_at DESC, object_id ASC
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    )
    object_rows = cursor.fetchall()
    object_ids = [r["object_id"] for r in object_rows]

    replicas_by_obj: Dict[str, List[Dict[str, Any]]] = {oid: [] for oid in object_ids}
    if object_ids:
        placeholders = ",".join("?" for _ in object_ids)
        cursor.execute(
            f"""
            SELECT * FROM replicas
            WHERE object_id IN ({placeholders})
            ORDER BY created_at ASC
            """,
            object_ids,
        )
        for r in cursor.fetchall():
            replicas_by_obj[r["object_id"]].append(
                {
                    "replica_id": r["replica_id"],
                    "object_id": r["object_id"],
                    "node_id": r["node_id"],
                    "checksum_sha256": r["checksum_sha256"],
                    "verified": bool(r["verified"]),
                    "is_authoritative": bool(r["is_authoritative"]),
                    "status": r["status"],
                    "created_at": r["created_at"],
                    "verified_at": r["verified_at"],
                }
            )

    items = []
    for r in object_rows:
        item = dict(r)
        item["replicas"] = replicas_by_obj.get(item["object_id"], [])
        items.append(item)

    return items, total


def record_download_access(conn: sqlite3.Connection, object_id: str) -> None:
    """Increments object access_count, updates last access time, and recalculates access frequency normalization."""
    now_iso = datetime.now(timezone.utc).isoformat()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE objects
        SET access_count = access_count + 1,
            last_access_at = ?,
            updated_at = ?
        WHERE object_id = ?
        """,
        (now_iso, now_iso, object_id),
    )

    # Calculate max access count across all objects
    cursor.execute("SELECT MAX(access_count) FROM objects")
    row = cursor.fetchone()
    max_count = row[0] if (row and row[0] is not None) else 0

    if max_count > 0:
        cursor.execute(
            """
            UPDATE objects
            SET access_frequency_normalized = log(1.0 + access_count) / log(1.0 + ?)
            """,
            (float(max_count),),
        )
    else:
        cursor.execute("UPDATE objects SET access_frequency_normalized = 0.0")

    conn.commit()


def record_telemetry(conn: sqlite3.Connection, record: Dict[str, Any]) -> None:
    """Inserts a telemetry observation/simulation record into SQLite."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO telemetry (
            telemetry_id, node_id, timestamp_utc, latency_ms, error_rate,
            temperature_c, capacity_free_bytes, load_metric, reachable,
            source_type, replay_drive_serial, replay_source_date
        ) VALUES (
            :telemetry_id, :node_id, :timestamp_utc, :latency_ms, :error_rate,
            :temperature_c, :capacity_free_bytes, :load_metric, :reachable,
            :source_type, :replay_drive_serial, :replay_source_date
        )
        """,
        {
            "telemetry_id": record["telemetry_id"],
            "node_id": record["node_id"],
            "timestamp_utc": record["timestamp_utc"],
            "latency_ms": record.get("latency_ms"),
            "error_rate": record.get("error_rate", 0.0),
            "temperature_c": record.get("temperature_c"),
            "capacity_free_bytes": record.get("capacity_free_bytes"),
            "load_metric": record.get("load_metric", 0.0),
            "reachable": 1 if record.get("reachable", True) else 0,
            "source_type": record.get("source_type", "observed"),
            "replay_drive_serial": record.get("replay_drive_serial"),
            "replay_source_date": record.get("replay_source_date"),
        },
    )
    conn.commit()


def record_prediction(
    conn: sqlite3.Connection,
    prediction: Dict[str, Any],
    node_status: Optional[str] = None,
) -> None:
    """Inserts a prediction record and updates node risk score and status."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO predictions (
            prediction_id, node_id, timestamp_utc, risk_score, model_name,
            model_version, target_horizon, anomaly_flag, explanation
        ) VALUES (
            :prediction_id, :node_id, :timestamp_utc, :risk_score, :model_name,
            :model_version, :target_horizon, :anomaly_flag, :explanation
        )
        """,
        {
            "prediction_id": prediction["prediction_id"],
            "node_id": prediction["node_id"],
            "timestamp_utc": prediction["timestamp_utc"],
            "risk_score": float(prediction["risk_score"]),
            "model_name": prediction.get("model_name", "baseline_threshold"),
            "model_version": prediction.get("model_version", "1.0.0"),
            "target_horizon": prediction.get("target_horizon"),
            "anomaly_flag": 1 if prediction.get("anomaly_flag", False) else 0,
            "explanation": prediction.get("explanation"),
        },
    )

    if node_status is not None:
        cursor.execute(
            """
            UPDATE nodes
            SET status = ?,
                risk_score = ?,
                risk_updated_at = ?,
                last_seen_at = ?
            WHERE node_id = ?
            """,
            (
                node_status,
                float(prediction["risk_score"]),
                prediction["timestamp_utc"],
                prediction["timestamp_utc"],
                prediction["node_id"],
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE nodes
            SET risk_score = ?,
                risk_updated_at = ?,
                last_seen_at = ?
            WHERE node_id = ?
            """,
            (
                float(prediction["risk_score"]),
                prediction["timestamp_utc"],
                prediction["timestamp_utc"],
                prediction["node_id"],
            ),
        )

    conn.commit()


def update_node_status(
    conn: sqlite3.Connection,
    node_id: str,
    status: str,
    risk_score: Optional[float] = None,
) -> None:
    """Directly updates a node's status and optional risk score."""
    now_iso = datetime.now(timezone.utc).isoformat()
    cursor = conn.cursor()
    if risk_score is not None:
        cursor.execute(
            """
            UPDATE nodes
            SET status = ?, risk_score = ?, risk_updated_at = ?, last_seen_at = ?
            WHERE node_id = ?
            """,
            (status, float(risk_score), now_iso, now_iso, node_id),
        )
    else:
        cursor.execute(
            """
            UPDATE nodes
            SET status = ?, last_seen_at = ?
            WHERE node_id = ?
            """,
            (status, now_iso, node_id),
        )
    conn.commit()


def get_latest_predictions(
    conn: sqlite3.Connection,
    limit: int = 50,
    offset: int = 0,
    node_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Returns predictions ordered descending by timestamp."""
    cursor = conn.cursor()
    if node_id:
        cursor.execute(
            """
            SELECT * FROM predictions
            WHERE node_id = ?
            ORDER BY timestamp_utc DESC, prediction_id DESC
            LIMIT ? OFFSET ?
            """,
            (node_id, limit, offset),
        )
    else:
        cursor.execute(
            """
            SELECT * FROM predictions
            ORDER BY timestamp_utc DESC, prediction_id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )

    rows = cursor.fetchall()
    return [
        {
            "prediction_id": r["prediction_id"],
            "node_id": r["node_id"],
            "timestamp_utc": r["timestamp_utc"],
            "risk_score": float(r["risk_score"]),
            "model_name": r["model_name"],
            "model_version": r["model_version"],
            "target_horizon": r["target_horizon"],
            "anomaly_flag": bool(r["anomaly_flag"]),
            "explanation": r["explanation"],
        }
        for r in rows
    ]


def get_telemetry_history(
    conn: sqlite3.Connection,
    node_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Returns telemetry records ordered descending by timestamp."""
    cursor = conn.cursor()
    if node_id:
        cursor.execute(
            """
            SELECT * FROM telemetry
            WHERE node_id = ?
            ORDER BY timestamp_utc DESC
            LIMIT ?
            """,
            (node_id, limit),
        )
    else:
        cursor.execute(
            """
            SELECT * FROM telemetry
            ORDER BY timestamp_utc DESC
            LIMIT ?
            """,
            (limit,),
        )
    return [dict(r) for r in cursor.fetchall()]


def save_scenario(conn: sqlite3.Connection, scenario: Dict[str, Any]) -> None:
    """Inserts or updates a scenario definition in the scenarios table."""
    now_iso = datetime.now(timezone.utc).isoformat()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO scenarios (
            scenario_id, name, scenario_type, prediction_outcome, lead_time_s,
            seed, node_faults_json, replay_drive_map_json, initial_dataset_json, created_at
        ) VALUES (
            :scenario_id, :name, :scenario_type, :prediction_outcome, :lead_time_s,
            :seed, :node_faults_json, :replay_drive_map_json, :initial_dataset_json, :created_at
        )
        ON CONFLICT(scenario_id) DO UPDATE SET
            name = excluded.name,
            scenario_type = excluded.scenario_type,
            prediction_outcome = excluded.prediction_outcome,
            lead_time_s = excluded.lead_time_s,
            seed = excluded.seed,
            node_faults_json = excluded.node_faults_json,
            replay_drive_map_json = excluded.replay_drive_map_json,
            initial_dataset_json = excluded.initial_dataset_json
        """,
        {
            "scenario_id": scenario["scenario_id"],
            "name": scenario["name"],
            "scenario_type": scenario.get("scenario_type", "synthetic"),
            "prediction_outcome": scenario["prediction_outcome"],
            "lead_time_s": scenario.get("lead_time_s"),
            "seed": scenario["seed"],
            "node_faults_json": scenario["node_faults_json"],
            "replay_drive_map_json": scenario.get("replay_drive_map_json"),
            "initial_dataset_json": scenario.get("initial_dataset_json"),
            "created_at": now_iso,
        },
    )
    conn.commit()


def get_scenario(conn: sqlite3.Connection, scenario_id: str) -> Optional[Dict[str, Any]]:
    """Fetches a saved scenario by scenario_id."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM scenarios WHERE scenario_id = ?", (scenario_id,))
    row = cursor.fetchone()
    return dict(row) if row else None
