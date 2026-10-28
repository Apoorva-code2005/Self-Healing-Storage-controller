from app.db.connection import create_connection, get_default_db_path
from app.db.schema import run_migrations
from app.db.repository import (
    choose_initial_placement,
    create_object_and_replica,
    get_latest_predictions,
    get_node,
    get_nodes_with_capacity,
    get_object,
    get_scenario,
    get_telemetry_history,
    list_objects,
    record_download_access,
    record_prediction,
    record_telemetry,
    save_scenario,
    sync_nodes,
    update_node_status,
)
from app.db.jobs_repo import (
    create_job,
    update_job,
    get_job,
    list_recent_jobs,
    acquire_lease,
    release_lease,
    reclaim_expired_leases,
)

__all__ = [
    "create_connection",
    "get_default_db_path",
    "run_migrations",
    "choose_initial_placement",
    "create_object_and_replica",
    "get_latest_predictions",
    "get_node",
    "get_nodes_with_capacity",
    "get_object",
    "get_scenario",
    "get_telemetry_history",
    "list_objects",
    "record_download_access",
    "record_prediction",
    "record_telemetry",
    "save_scenario",
    "sync_nodes",
    "update_node_status",
    # jobs
    "create_job",
    "update_job",
    "get_job",
    "list_recent_jobs",
    # leases
    "acquire_lease",
    "release_lease",
    "reclaim_expired_leases",
]
