from __future__ import annotations

import math
import os
from pathlib import Path
import sqlite3
from typing import Optional


def safe_log(x: float | None) -> float:
    """Safe natural logarithm for SQLite query usage."""
    if x is None or x <= 0:
        return 0.0
    return math.log(x)


def get_default_db_path(explicit_path: Optional[str | Path] = None) -> str:
    if explicit_path:
        return str(explicit_path)
    env_path = os.getenv("DATABASE_PATH")
    if env_path:
        return env_path
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir / "metadata.db")


def create_connection(db_path: Optional[str | Path] = None) -> sqlite3.Connection:
    """Creates a configured SQLite connection with WAL mode and busy timeout per PRD 7.5."""
    path_str = get_default_db_path(db_path)
    conn = sqlite3.connect(path_str, timeout=10.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # WAL mode is applicable to file databases
    if path_str != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL;")

    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.create_function("log", 1, safe_log)

    return conn
