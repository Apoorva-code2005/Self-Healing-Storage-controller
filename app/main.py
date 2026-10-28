from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import email
from email.policy import default as default_email_policy
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any, Dict, Generator, List, Optional, Tuple
import uuid

log = logging.getLogger(__name__)

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from app.config import AppConfig, load_config
from app.db import (
    choose_initial_placement,
    create_connection,
    create_object_and_replica,
    get_latest_predictions,
    get_node,
    get_nodes_with_capacity,
    get_object,
    list_objects,
    record_download_access,
    run_migrations,
    sync_nodes,
    create_job,
    get_job,
    list_recent_jobs,
)
from app.jobs.evacuation import redundancy_status
from app.jobs.worker import process_next_job, run_worker
from app.schemas.objects import ObjectListResponse, UploadResponse
from app.schemas.telemetry import (
    PredictionResponse,
    SimulateFailureRequest,
    SimulateFailureResponse,
)
from app.services.failure_injection import execute_scenario, load_scenario
from app.services.placement import select_destinations
from app.services.priority import count_healthy_verified_replicas, rank_objects
from app.services.scenario_clock import ScenarioClock
from app.storage.adapter import NodeReachability, StorageAdapter

app_config: AppConfig = load_config()
storage_adapter: StorageAdapter = StorageAdapter(app_config.nodes)
_db_connection: Optional[sqlite3.Connection] = None


def get_db_session() -> Generator[sqlite3.Connection, None, None]:
    """Dependency provider for the SQLite metadata connection with WAL mode."""
    global _db_connection
    if _db_connection is None:
        _db_connection = create_connection()
        run_migrations(_db_connection)
        sync_nodes(_db_connection, app_config.nodes)
    yield _db_connection


def get_adapter() -> StorageAdapter:
    """Dependency provider for StorageAdapter to allow easy test mocking."""
    return storage_adapter


import urllib.parse


def validate_filename(filename: Optional[str]) -> str:
    """Validates filename per PRD Section 14 security rules.

    Ensures no path traversal, control characters, empty names, or filesystem abuse.
    """
    if not filename or not filename.strip():
        raise HTTPException(status_code=400, detail="Filename cannot be empty")
    cleaned = filename.strip()
    if len(cleaned) > 255:
        raise HTTPException(status_code=400, detail="Filename exceeds maximum length of 255 characters")

    # Check both raw and URL-decoded forms to prevent encoded bypasses like %00 or %2e%2e
    unquoted = urllib.parse.unquote(cleaned)
    for target in (cleaned, unquoted):
        if "\0" in target or "\r" in target or "\n" in target:
            raise HTTPException(status_code=400, detail="Filename contains illegal control characters")
        if "/" in target or "\\" in target:
            raise HTTPException(status_code=400, detail="Filename must not contain path separators ('/' or '\\')")
        if ".." in target:
            raise HTTPException(status_code=400, detail="Filename must not contain path traversal ('..')")
        if target in {".", ".."}:
            raise HTTPException(status_code=400, detail="Filename cannot be '.' or '..'")

    return cleaned


async def parse_multipart_upload(request: Request) -> Tuple[str, bytes, int, int]:
    """Parses multipart/form-data upload using standard library email parser."""
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise HTTPException(status_code=400, detail="Content-Type must be multipart/form-data")

    body = await request.body()
    msg = email.message_from_bytes(
        b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body,
        policy=default_email_policy,
    )

    if not msg.is_multipart():
        raise HTTPException(status_code=400, detail="Invalid multipart payload")

    filename = None
    file_bytes = None
    form_data = {}

    for part in msg.iter_parts():
        name = part.get_param("name", header="content-disposition")
        fname = part.get_filename()

        if fname or name == "file":
            filename = fname
            file_bytes = part.get_payload(decode=True)
        elif name:
            val = part.get_payload(decode=True)
            if val is not None:
                form_data[name] = val.decode("utf-8", errors="replace").strip()

    if file_bytes is None:
        raise HTTPException(status_code=400, detail="Missing required 'file' part in multipart upload")

    valid_filename = validate_filename(filename)

    # Criticality 1-5
    crit_raw = form_data.get("criticality") or request.query_params.get("criticality")
    if crit_raw is None:
        criticality = 1
    else:
        try:
            criticality = int(crit_raw)
        except ValueError:
            raise HTTPException(status_code=400, detail="Criticality must be an integer between 1 and 5")

    if not (1 <= criticality <= 5):
        raise HTTPException(status_code=400, detail="Criticality must be between 1 and 5")

    # Required replica count 1-3
    rep_raw = form_data.get("required_replica_count") or request.query_params.get("required_replica_count")
    if rep_raw is None:
        required_replica_count = 2
    else:
        try:
            required_replica_count = int(rep_raw)
        except ValueError:
            raise HTTPException(status_code=400, detail="Required replica count must be an integer between 1 and 3")

    if not (1 <= required_replica_count <= 3):
        raise HTTPException(status_code=400, detail="Required replica count must be between 1 and 3")

    return valid_filename, file_bytes, criticality, required_replica_count


def create_app(
    cfg: AppConfig | None = None,
    adapter: StorageAdapter | None = None,
    db_conn: sqlite3.Connection | None = None,
) -> FastAPI:
    current_cfg = cfg or app_config
    _clock = ScenarioClock()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        conn = db_conn or create_connection()
        run_migrations(conn)
        sync_nodes(conn, current_cfg.nodes)
        _adp = adapter or storage_adapter

        # Phase 6b: Restart Reconciliation
        from app.jobs.reconciliation import reconcile_on_startup
        reconcile_on_startup(conn, _adp, _clock, current_cfg)

        app.state.db = conn
        app.state.adapter = _adp
        app.state.clock = _clock
        app.state.config = current_cfg
        _worker_task = None
        if current_cfg.app.worker_enabled:
            _worker_task = asyncio.create_task(run_worker(app.state))
        yield
        if _worker_task is not None:
            _worker_task.cancel()
            try:
                await _worker_task
            except asyncio.CancelledError:
                pass

    application = FastAPI(
        title="Self-Healing Storage Controller",
        version="0.5.0",
        description="Phase 5 — Priority Engine, Placement Feasibility, and Recovery Plan endpoint",
        lifespan=lifespan,
    )

    @application.get("/health")
    def health_check():
        return {
            "status": "ok",
            "app": current_cfg.app.name,
            "version": current_cfg.app.version,
            "env": current_cfg.app.env,
        }

    @application.get("/nodes", response_model=List[NodeReachability])
    def get_nodes(
        adp: StorageAdapter = Depends(get_adapter),
        db: sqlite3.Connection = Depends(get_db_session),
    ):
        current_adp = adapter or adp
        reachabilities = current_adp.check_all_nodes()
        capacities = get_nodes_with_capacity(db)

        results = []
        for r in reachabilities:
            cap_info = capacities.get(r.node_id)
            if cap_info:
                v_cap = cap_info["virtual_capacity_bytes"]
                f_bytes = cap_info["free_bytes"]
                db_status = cap_info.get("status", "healthy")
                db_risk = float(cap_info.get("risk_score", 0.0))
            else:
                v_cap = r.virtual_capacity_bytes
                f_bytes = r.free_bytes
                db_status = "healthy"
                db_risk = 0.0

            if r.fenced:
                eff_status = "fenced"
                eff_risk = 1.0
            elif not r.reachable:
                eff_status = "failed"
                eff_risk = 1.0
            else:
                eff_status = db_status
                eff_risk = db_risk

            results.append(
                NodeReachability(
                    node_id=r.node_id,
                    endpoint_url=r.endpoint_url,
                    reachable=r.reachable,
                    fenced=r.fenced,
                    latency_ms=r.latency_ms,
                    error=r.error,
                    virtual_capacity_bytes=v_cap,
                    free_bytes=f_bytes,
                    status=eff_status,
                    risk_score=eff_risk,
                )
            )
        return results

    @application.post("/objects", response_model=UploadResponse)
    async def upload_object(
        request: Request,
        adp: StorageAdapter = Depends(get_adapter),
        db: sqlite3.Connection = Depends(get_db_session),
    ):
        current_adp = adapter or adp
        filename, file_bytes, criticality, required_replica_count = await parse_multipart_upload(request)

        # Stream computing SHA-256 while processing data
        hasher = hashlib.sha256()
        chunk_size = 65536
        for offset in range(0, len(file_bytes), chunk_size):
            hasher.update(file_bytes[offset:offset + chunk_size])
        checksum = hasher.hexdigest()
        size_bytes = len(file_bytes)

        # Check node reachability and filter candidates
        reachabilities = current_adp.check_all_nodes()
        reachable_node_ids = [n.node_id for n in reachabilities if n.reachable and not n.fenced]

        if not reachable_node_ids:
            raise HTTPException(status_code=503, detail="No reachable storage nodes available")

        # Initial placement: pick reachable node with the most free virtual capacity
        chosen_node = choose_initial_placement(db, reachable_node_ids, size_bytes)
        if not chosen_node:
            raise HTTPException(
                status_code=507,
                detail=f"Virtual capacity quota exceeded: insufficient space for {size_bytes} bytes on reachable nodes",
            )

        chosen_node_id = chosen_node["node_id"]
        object_id = str(uuid.uuid4())

        # Store through the storage adapter using object_id as key (never untrusted filename)
        try:
            current_adp.put_object(chosen_node_id, object_id, file_bytes)
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to store object on node '{chosen_node_id}': {exc}",
            )

        # Record metadata in SQLite only after storage write succeeds
        now_iso = datetime.now(timezone.utc).isoformat()
        object_data = {
            "object_id": object_id,
            "object_name": filename,
            "size_bytes": size_bytes,
            "criticality": criticality,
            "access_count": 0,
            "access_frequency_normalized": 0.0,
            "origin_node_id": chosen_node_id,
            "checksum_sha256": checksum,
            "required_replica_count": required_replica_count,
            "status": "ok",
            "created_at": now_iso,
            "updated_at": now_iso,
            "last_access_at": None,
        }

        replica_data = {
            "replica_id": str(uuid.uuid4()),
            "object_id": object_id,
            "node_id": chosen_node_id,
            "checksum_sha256": checksum,
            "verified": 1,
            "is_authoritative": 1,
            "status": "valid",
            "created_at": now_iso,
            "verified_at": now_iso,
        }

        create_object_and_replica(db, object_data, replica_data)

        return UploadResponse(
            object_id=object_id,
            checksum=checksum,
            checksum_sha256=checksum,
            object_name=filename,
            size_bytes=size_bytes,
            criticality=criticality,
            required_replica_count=required_replica_count,
            origin_node_id=chosen_node_id,
            status="ok",
        )

    @application.get("/objects/{object_id}/download")
    def download_object(
        object_id: str,
        adp: StorageAdapter = Depends(get_adapter),
        db: sqlite3.Connection = Depends(get_db_session),
    ):
        current_adp = adapter or adp
        obj = get_object(db, object_id)
        if not obj:
            raise HTTPException(status_code=404, detail=f"Object '{object_id}' not found")

        # Find a valid, verified replica on a reachable node
        valid_replica = None
        for replica in obj.get("replicas", []):
            if replica["verified"] and replica["status"] == "valid":
                node_status = current_adp.check_node_connectivity(replica["node_id"])
                if node_status.reachable and not node_status.fenced:
                    valid_replica = replica
                    break

        if not valid_replica:
            raise HTTPException(
                status_code=503,
                detail="No healthy verified replica is available on any reachable storage node",
            )

        try:
            data = current_adp.get_object(valid_replica["node_id"], object_id)
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to retrieve object from storage: {exc}",
            )

        # Increment access count and normalize access frequency
        record_download_access(db, object_id)

        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{obj["object_name"]}"'},
        )

    @application.get("/objects", response_model=ObjectListResponse)
    def get_objects(
        limit: int = Query(default=50, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        db: sqlite3.Connection = Depends(get_db_session),
    ):
        items, total = list_objects(db, limit=limit, offset=offset)
        return ObjectListResponse(items=items, total=total, limit=limit, offset=offset)

    @application.get("/predictions", response_model=List[PredictionResponse])
    def get_predictions(
        limit: int = Query(default=50, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        node_id: Optional[str] = Query(default=None),
        db: sqlite3.Connection = Depends(get_db_session),
    ):
        return get_latest_predictions(db, limit=limit, offset=offset, node_id=node_id)

    @application.post("/simulate-failure", response_model=SimulateFailureResponse)
    def simulate_failure(
        payload: SimulateFailureRequest,
        adp: StorageAdapter = Depends(get_adapter),
        db: sqlite3.Connection = Depends(get_db_session),
    ):
        import os

        current_adp = adapter or adp
        env = os.getenv("ENV") or current_cfg.app.env
        if env != "development":
            raise HTTPException(
                status_code=403,
                detail="Failure injection is only permitted in development mode (ENV=development)",
            )

        try:
            scenario_data = load_scenario(payload.scenario_name)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Failed to load scenario: {exc}")

        result = execute_scenario(
            scenario_data=scenario_data,
            adapter=current_adp,
            db_conn=db,
            config=current_cfg,
            max_ticks=payload.advance_ticks,
            override_seed=payload.seed,
        )
        return SimulateFailureResponse(**result)

    # ------------------------------------------------------------------
    # POST /recovery/{node_id}  (Phase 6a)
    # ------------------------------------------------------------------
    @application.post("/recovery/{node_id}", status_code=202)
    def trigger_recovery(
        node_id: str,
        mode: str = Query(default="priority", description="priority or fifo"),
        adp: StorageAdapter = Depends(get_adapter),
        db: sqlite3.Connection = Depends(get_db_session),
    ) -> Dict[str, Any]:
        if mode not in ("priority", "fifo"):
            raise HTTPException(status_code=400, detail=f"Invalid mode '{mode}'. Use 'priority' or 'fifo'.")

        capacities = get_nodes_with_capacity(db)
        if node_id not in capacities:
            raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")

        # Find all objects with a replica on this node
        cursor = db.execute(
            """
            SELECT o.object_id
            FROM objects o
            JOIN replicas r ON r.object_id = o.object_id
            WHERE r.node_id = ?
            GROUP BY o.object_id
            ORDER BY MIN(o.created_at) ASC, MIN(o.rowid) ASC
            """,
            (node_id,),
        )
        object_ids = [row[0] for row in cursor.fetchall()]

        # Objects that already have an active job
        active_cursor = db.execute(
            "SELECT DISTINCT object_id FROM migration_jobs WHERE status IN ('queued','running','verifying')"
        )
        active_objects = {row[0] for row in active_cursor.fetchall()}

        # Build ranked/fifo list
        if not object_ids:
            return {"node_id": node_id, "mode": mode, "jobs": []}

        all_items, _ = list_objects(db, limit=10000, offset=0)

        current_adp = adapter or adp
        from app.jobs.rereplication import fallback_evacuation_jobs, find_surviving_replicas, is_node_hard_failed

        node_failed = is_node_hard_failed(current_adp, node_id, db)
        if node_failed:
            # Fallback active evacuation jobs for this node to re_replicate
            fallback_evacuation_jobs(db, current_adp, node_id, _clock, current_cfg)

            candidates = []
            for oid in object_ids:
                if oid in active_objects:
                    continue
                surviving = find_surviving_replicas(db, current_adp, oid, exclude_node_ids={node_id})
                if not surviving:
                    from app.jobs.evacuation import _set_object_status
                    _set_object_status(db, oid, "lost")
                    log.error("ALERT: Object %s is lost: node %s hard-failed with zero surviving replicas", oid, node_id)
                else:
                    candidates.append((oid, surviving[0]["node_id"]))

            if not candidates:
                return {"node_id": node_id, "mode": mode, "jobs": []}

            cand_oids = {c[0] for c in candidates}
            obj_map = {o["object_id"]: o for o in all_items if o["object_id"] in cand_oids}
            cand_sources = {c[0]: c[1] for c in candidates}

            if mode == "priority":
                all_cap = get_nodes_with_capacity(db)
                reachabilities = current_adp.check_all_nodes()
                reachable_map = {r.node_id: r for r in reachabilities}
                healthy_node_ids = [
                    nid for nid, cap in all_cap.items()
                    if reachable_map.get(nid) and reachable_map[nid].reachable
                    and not reachable_map[nid].fenced
                    and cap.get("status") == "healthy"
                ]
                for obj in obj_map.values():
                    nc = all_cap.get(cand_sources[obj["object_id"]], {})
                    obj["_node_risk_score"] = float(nc.get("risk_score", 0.0))
                w = current_cfg.weights
                ordered = rank_objects(
                    list(obj_map.values()),
                    healthy_node_ids=healthy_node_ids,
                    w1=w.w1_criticality,
                    w2=w.w2_access_frequency,
                    w3=w.w3_node_risk,
                    w4=w.w4_redundancy,
                    gamma=current_cfg.policy.gamma,
                    use_rate=False,
                )
            else:
                ordered = [obj_map[oid] for oid in object_ids if oid in obj_map]

            now_iso = _clock.now_iso()
            jobs_created = []
            for obj in ordered:
                oid = obj["object_id"]
                priority_score = float(obj.get("priority", 0.0)) if mode == "priority" else 0.0
                jid = create_job(
                    db,
                    job_type="re_replicate",
                    object_id=oid,
                    source_node_id=cand_sources[oid],
                    target_node_id="",
                    priority_score=priority_score,
                    now=now_iso,
                )
                jobs_created.append({
                    "job_id": jid,
                    "object_id": oid,
                    "priority_score": priority_score,
                    "status": "queued",
                })
            return {"node_id": node_id, "mode": mode, "jobs": jobs_created}

        obj_map = {o["object_id"]: o for o in all_items if o["object_id"] in set(object_ids)}

        if mode == "priority":
            current_adp = adapter or adp
            all_cap = get_nodes_with_capacity(db)
            reachabilities = current_adp.check_all_nodes()
            reachable_map = {r.node_id: r for r in reachabilities}
            healthy_node_ids = [
                nid for nid, cap in all_cap.items()
                if reachable_map.get(nid) and reachable_map[nid].reachable
                and not reachable_map[nid].fenced
                and cap.get("status") == "healthy"
            ]
            for obj in obj_map.values():
                nc = all_cap.get(next(
                    (r["node_id"] for r in obj.get("replicas", []) if r.get("is_authoritative")),
                    node_id
                ), {})
                obj["_node_risk_score"] = float(nc.get("risk_score", 0.0))
            w = current_cfg.weights
            ranked = rank_objects(
                list(obj_map.values()),
                healthy_node_ids=healthy_node_ids,
                w1=w.w1_criticality,
                w2=w.w2_access_frequency,
                w3=w.w3_node_risk,
                w4=w.w4_redundancy,
                gamma=current_cfg.policy.gamma,
                use_rate=False,
            )
            ordered = ranked
        else:
            # fifo: sort by object created_at ascending (preserving creation order)
            ordered = [obj_map[oid] for oid in object_ids if oid in obj_map]

        now_iso = _clock.now_iso()
        jobs_created = []
        for i, obj in enumerate(ordered):
            oid = obj["object_id"]
            if oid in active_objects:
                continue
            priority_score = float(obj.get("priority", 0.0)) if mode == "priority" else 0.0
            # Find the best target: any healthy node not currently holding a verified replica
            all_nodes_list = list(capacities.values())
            existing_replicas = obj.get("replicas", [])
            destinations = select_destinations(
                source_node_id=node_id,
                object_size_bytes=int(obj["size_bytes"]),
                existing_replicas=existing_replicas,
                candidate_nodes=all_nodes_list,
                max_destination_risk=current_cfg.policy.max_destination_risk,
                max_destination_load=current_cfg.policy.max_destination_load,
                reserve_margin_bytes=current_cfg.policy.reserve_margin_bytes,
                needed=1,
            )
            target_node_id = destinations[0]["node_id"] if destinations else ""
            jid = create_job(
                db,
                job_type="evacuate",
                object_id=oid,
                source_node_id=node_id,
                target_node_id=target_node_id,
                priority_score=priority_score,
                now=now_iso,
            )
            jobs_created.append({
                "job_id": jid,
                "object_id": oid,
                "priority_score": priority_score,
                "status": "queued",
            })

        return {"node_id": node_id, "mode": mode, "jobs": jobs_created}

    # ------------------------------------------------------------------
    # GET /recovery/status  (Phase 6a)
    # ------------------------------------------------------------------
    @application.get("/recovery/status")
    def recovery_status(
        limit: int = Query(default=50, ge=1, le=200),
        db: sqlite3.Connection = Depends(get_db_session),
    ) -> Dict[str, Any]:
        jobs = list_recent_jobs(db, limit=limit)
        summary: Dict[str, int] = {}
        enriched = []
        for j in jobs:
            status = j["status"]
            summary[status] = summary.get(status, 0) + 1
            r_status = redundancy_status(j["object_id"], db)
            enriched.append({
                "job_id": j["job_id"],
                "object_id": j["object_id"],
                "status": status,
                "source_node_id": j["source_node_id"],
                "target_node_id": j["target_node_id"],
                "priority_score": j["priority_score"],
                "bytes_transferred": j["bytes_transferred"],
                "retries": j["retries"],
                "verification_result": j["verification_result"],
                "error_message": j["error_message"],
                "redundancy_status": r_status,
            })
        cursor = db.execute("SELECT object_id FROM objects WHERE status = 'lost'")
        lost_objects = [row[0] for row in cursor.fetchall()]
        return {"jobs": enriched, "summary": summary, "lost_objects": lost_objects}

    # ------------------------------------------------------------------
    # GET /priority  (Phase 5 — read-only)
    # ------------------------------------------------------------------
    @application.get("/priority")
    def get_priority(
        node_id: Optional[str] = Query(default=None),
        gamma: float = Query(default=current_cfg.policy.gamma, ge=0.0, le=1.0),
        use_rate: bool = Query(default=False),
        limit: int = Query(default=50, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        adp: StorageAdapter = Depends(get_adapter),
        db: sqlite3.Connection = Depends(get_db_session),
    ) -> Dict[str, Any]:
        current_adp = adapter or adp
        capacities = get_nodes_with_capacity(db)
        reachabilities = current_adp.check_all_nodes()
        reachable_map = {r.node_id: r for r in reachabilities}
        healthy_node_ids = [
            nid for nid, cap in capacities.items()
            if reachable_map.get(nid) and reachable_map[nid].reachable
            and not reachable_map[nid].fenced
            and cap.get("status") == "healthy"
        ]
        all_items, total = list_objects(db, limit=10000, offset=0)
        if node_id:
            if node_id not in capacities:
                raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
            all_items = [
                o for o in all_items
                if any(r.get("is_authoritative") and r["node_id"] == node_id
                       for r in o.get("replicas", []))
            ]
        for obj in all_items:
            node_risk = 0.0
            for rep in obj.get("replicas", []):
                if rep.get("is_authoritative"):
                    cap = capacities.get(rep["node_id"], {})
                    node_risk = float(cap.get("risk_score", 0.0))
                    break
            obj["_node_risk_score"] = node_risk
        w = current_cfg.weights
        ranked = rank_objects(
            all_items,
            healthy_node_ids=healthy_node_ids,
            w1=w.w1_criticality,
            w2=w.w2_access_frequency,
            w3=w.w3_node_risk,
            w4=w.w4_redundancy,
            gamma=gamma,
            use_rate=use_rate,
        )
        grand_total = len(ranked)
        page = ranked[offset: offset + limit]
        for obj in page:
            obj.pop("_node_risk_score", None)
        return {
            "total": grand_total, "limit": limit, "offset": offset,
            "gamma": gamma, "use_rate": use_rate,
            "weights": {
                "w1_criticality": w.w1_criticality,
                "w2_access_frequency": w.w2_access_frequency,
                "w3_node_risk": w.w3_node_risk,
                "w4_redundancy": w.w4_redundancy,
            },
            "items": page,
        }

    # ------------------------------------------------------------------
    # GET /experiments  (Phase 8)
    # ------------------------------------------------------------------
    @application.get("/experiments")
    def get_experiments(
        recovery_mode: Optional[str] = Query(default=None, description="Filter by recovery mode"),
        outcome: Optional[str] = Query(default=None, description="Filter by prediction outcome"),
    ) -> Dict[str, Any]:
        summary_path = Path("data/experiments_summary.json")
        if not summary_path.is_file():
            summary_path = Path(__file__).resolve().parent.parent / "data" / "experiments_summary.json"

        if not summary_path.is_file():
            return {
                "total": 0,
                "runs": [],
                "summary": {},
                "status": "no_experiments_recorded",
            }

        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            log.warning("Failed to read %s: %s", summary_path, exc)
            return {"total": 0, "runs": [], "summary": {}, "error": str(exc)}

        runs = data.get("runs", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        if recovery_mode:
            runs = [r for r in runs if r.get("recovery_mode") == recovery_mode]
        if outcome:
            runs = [r for r in runs if r.get("outcome") == outcome or r.get("prediction_outcome") == outcome]

        return {
            "total": len(runs),
            "runs": runs,
            "summary": data.get("summary", {}) if isinstance(data, dict) else {},
        }

    return application


app = create_app()
