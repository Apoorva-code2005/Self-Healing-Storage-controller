"""scripts/build_replay_pool.py

Constructs the held-out replay drive pool for realistic telemetry replay scenarios (PRD 10.5).
Saves individual drive timelines to data/replay_pool.json.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.prepare_backblaze import (
    SMART_FEATURE_COLS,
    generate_sample_smart_data,
    construct_failure_targets,
    drive_aware_split,
)


def build_replay_pool_dict(replay_df: pd.DataFrame) -> Dict[str, Any]:
    """Structures replay drive DataFrame into a dictionary indexed by drive serial."""
    pool: Dict[str, Any] = {}
    for serial, group in replay_df.groupby("serial_number"):
        group_sorted = group.sort_values("date")
        has_failure = int(group_sorted["failure"].max()) == 1
        fail_row = group_sorted[group_sorted["failure"] == 1]
        fail_date = fail_row["date"].iloc[0] if has_failure else None

        records: List[Dict[str, Any]] = []
        for _, row in group_sorted.iterrows():
            rec = {
                "date": row["date"],
                "failure": int(row["failure"]),
                "target": int(row.get("target", 0)),
            }
            for col in SMART_FEATURE_COLS:
                rec[col] = int(row.get(col, 0))
            records.append(rec)

        pool[serial] = {
            "serial_number": serial,
            "model": group_sorted["model"].iloc[0] if "model" in group_sorted else "unknown",
            "is_failing": has_failure,
            "failure_date": fail_date,
            "record_count": len(records),
            "records": records,
        }
    return pool


def build_and_save_replay_pool(
    replay_data_path: Optional[str | Path] = None,
    output_path: str = "data/replay_pool.json",
    seed: int = 42,
) -> Dict[str, Any]:
    """Builds and serialises the replay pool JSON."""
    if replay_data_path and Path(replay_data_path).is_file():
        df = pd.read_csv(replay_data_path)
    else:
        raw_df = generate_sample_smart_data(seed=seed)
        labeled = construct_failure_targets(raw_df)
        _, _, df = drive_aware_split(labeled, seed=seed)

    pool = build_replay_pool_dict(df)

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(pool, f, indent=2)

    return pool


if __name__ == "__main__":
    p = build_and_save_replay_pool()
    failing_count = sum(1 for d in p.values() if d["is_failing"])
    healthy_count = len(p) - failing_count
    print(f"Replay pool saved with {len(p)} drives ({failing_count} failing, {healthy_count} healthy).")
