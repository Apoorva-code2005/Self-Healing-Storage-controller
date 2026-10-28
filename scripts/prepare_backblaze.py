"""scripts/prepare_backblaze.py

Preprocesses Backblaze SMART telemetry and constructs failure prediction targets.
Uses drive-aware splitting to prevent data leakage between train, test, and replay pool.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import random
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd

SMART_FEATURE_COLS = [
    "smart_5_raw",    # Reallocated Sectors Count
    "smart_9_raw",    # Power-On Hours
    "smart_187_raw",  # Reported Uncorrectable Errors
    "smart_194_raw",  # Temperature
    "smart_197_raw",  # Current Pending Sector Count
    "smart_198_raw",  # Offline Uncorrectable Sector Count
]


def generate_sample_smart_data(
    n_healthy_drives: int = 30,
    n_failing_drives: int = 15,
    n_days: int = 30,
    start_date: str = "2026-01-01",
    seed: int = 42,
) -> pd.DataFrame:
    """Generates a realistic synthetic Backblaze SMART dataset for model development."""
    rng = random.Random(seed)
    base_date = datetime.fromisoformat(start_date)
    records: List[Dict] = []

    # Healthy drives: normal SMART metrics, no failure
    for i in range(n_healthy_drives):
        serial = f"HEALTHY_{i:04d}"
        model = "ST4000DM000"
        power_on = rng.randint(5000, 30000)
        temp_base = rng.randint(24, 32)

        for d in range(n_days):
            date_str = (base_date + timedelta(days=d)).strftime("%Y-%m-%d")
            records.append({
                "date": date_str,
                "serial_number": serial,
                "model": model,
                "failure": 0,
                "smart_5_raw": 0,
                "smart_9_raw": power_on + d * 24,
                "smart_187_raw": 0,
                "smart_194_raw": temp_base + rng.randint(-2, 3),
                "smart_197_raw": 0,
                "smart_198_raw": 0,
            })

    # Failing drives: SMART degradation leading to failure on fail_day
    for i in range(n_failing_drives):
        serial = f"FAILING_{i:04d}"
        model = "ST4000DM000"
        power_on = rng.randint(10000, 40000)
        min_fail = max(1, n_days // 3)
        max_fail = max(min_fail, n_days - 1)
        fail_day = rng.randint(min_fail, max_fail)

        for d in range(fail_day + 1):
            date_str = (base_date + timedelta(days=d)).strftime("%Y-%m-%d")
            is_fail_day = 1 if d == fail_day else 0
            days_before_fail = fail_day - d

            # Degrade SMART indicators as failure approaches
            if days_before_fail <= 7:
                s5 = rng.randint(10, 50) + (7 - days_before_fail) * 8
                s187 = rng.randint(1, 10) + (7 - days_before_fail) * 2
                s197 = rng.randint(2, 15)
                s198 = rng.randint(1, 8)
                temp = temp_base + rng.randint(4, 9)
            elif days_before_fail <= 14:
                s5 = rng.randint(0, 5)
                s187 = rng.randint(0, 2)
                s197 = 0
                s198 = 0
                temp = temp_base + rng.randint(0, 3)
            else:
                s5 = 0
                s187 = 0
                s197 = 0
                s198 = 0
                temp = temp_base

            records.append({
                "date": date_str,
                "serial_number": serial,
                "model": model,
                "failure": is_fail_day,
                "smart_5_raw": s5,
                "smart_9_raw": power_on + d * 24,
                "smart_187_raw": s187,
                "smart_194_raw": temp,
                "smart_197_raw": s197,
                "smart_198_raw": s198,
            })

    df = pd.DataFrame(records)
    return df


def construct_failure_targets(
    df: pd.DataFrame,
    horizon_days: int = 7,
) -> pd.DataFrame:
    """Labels records with failure_within_horizon = 1 if the drive fails within horizon_days."""
    df = df.copy()
    df["date_dt"] = pd.to_datetime(df["date"])

    # Identify failure date per drive
    failed_drives = df[df["failure"] == 1][["serial_number", "date_dt"]].rename(
        columns={"date_dt": "failure_date"}
    )
    df = df.merge(failed_drives, on="serial_number", how="left")

    # Target: 1 if failure occurs within horizon_days from current record date
    df["days_to_failure"] = (df["failure_date"] - df["date_dt"]).dt.days
    df["target"] = (
        (df["days_to_failure"] >= 0) & (df["days_to_failure"] <= horizon_days)
    ).astype(int)

    df = df.drop(columns=["date_dt", "failure_date", "days_to_failure"])
    return df


def drive_aware_split(
    df: pd.DataFrame,
    test_ratio: float = 0.20,
    replay_ratio: float = 0.20,
    seed: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Partitions dataset by unique drive serials so no drive appears in multiple splits."""
    rng = random.Random(seed)
    serials = sorted(df["serial_number"].unique())
    rng.shuffle(serials)

    n_total = len(serials)
    n_replay = max(2, int(n_total * replay_ratio))
    n_test = max(2, int(n_total * test_ratio))

    replay_serials = set(serials[:n_replay])
    test_serials = set(serials[n_replay: n_replay + n_test])
    train_serials = set(serials[n_replay + n_test:])

    # Verify disjointness
    assert not (train_serials & test_serials), "Train and test drive serials overlap!"
    assert not (train_serials & replay_serials), "Train and replay drive serials overlap!"
    assert not (test_serials & replay_serials), "Test and replay drive serials overlap!"

    train_df = df[df["serial_number"].isin(train_serials)].copy()
    test_df = df[df["serial_number"].isin(test_serials)].copy()
    replay_df = df[df["serial_number"].isin(replay_serials)].copy()

    return train_df, test_df, replay_df


def prepare_and_save_data(
    output_dir: str = "data",
    horizon_days: int = 7,
    seed: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Generates synthetic SMART dataset, constructs targets, splits, and saves to data/."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    raw_df = generate_sample_smart_data(seed=seed)
    labeled_df = construct_failure_targets(raw_df, horizon_days=horizon_days)

    train_df, test_df, replay_df = drive_aware_split(labeled_df, seed=seed)

    train_df.to_csv(out_path / "train_data.csv", index=False)
    test_df.to_csv(out_path / "test_data.csv", index=False)
    replay_df.to_csv(out_path / "replay_data.csv", index=False)

    return train_df, test_df, replay_df


if __name__ == "__main__":
    train, test, replay = prepare_and_save_data()
    print(f"Data prepared: train={len(train)}, test={len(test)}, replay={len(replay)}")
