"""scripts/train_model.py

Trains a Random Forest classifier on SMART drive telemetry to predict failures.
Saves model artifact to models/random_forest_v1.joblib alongside evaluation metrics.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)

from scripts.prepare_backblaze import (
    SMART_FEATURE_COLS,
    prepare_and_save_data,
)


def train_and_evaluate(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    n_estimators: int = 50,
    max_depth: int = 8,
    seed: int = 42,
) -> Tuple[RandomForestClassifier, Dict[str, Any]]:
    """Trains a Random Forest on train_df and computes metrics on test_df."""
    X_train = train_df[SMART_FEATURE_COLS]
    y_train = train_df["target"]

    X_test = test_df[SMART_FEATURE_COLS]
    y_test = test_df["target"]

    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        class_weight="balanced",
        random_state=seed,
    )
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)

    precision = float(precision_score(y_test, y_pred, zero_division=0))
    recall = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))

    cm = confusion_matrix(y_test, y_pred)
    tn, fp, fn, tp = cm.ravel() if cm.shape == (2, 2) else (0, 0, 0, 0)
    fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0

    metrics = {
        "model_name": "random_forest",
        "model_version": "1.0.0",
        "features": SMART_FEATURE_COLS,
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "train_samples": len(train_df),
        "test_samples": len(test_df),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_positive_rate": round(fpr, 4),
        "confusion_matrix": {
            "true_negatives": int(tn),
            "false_positives": int(fp),
            "false_negatives": int(fn),
            "true_positives": int(tp),
        },
    }
    return clf, metrics


def train_and_save_model(
    train_path: Optional[str | Path] = None,
    test_path: Optional[str | Path] = None,
    model_output_dir: str = "models",
    seed: int = 42,
) -> Tuple[RandomForestClassifier, Dict[str, Any]]:
    """Trains the model and serialises artifacts to model_output_dir."""
    if train_path and test_path and Path(train_path).is_file() and Path(test_path).is_file():
        train_df = pd.read_csv(train_path)
        test_df = pd.read_csv(test_path)
    else:
        train_df, test_df, _ = prepare_and_save_data(seed=seed)

    clf, metrics = train_and_evaluate(train_df, test_df, seed=seed)

    out_dir = Path(model_output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    model_file = out_dir / "random_forest_v1.joblib"
    joblib.dump(clf, model_file)

    metrics_file = out_dir / "metrics.json"
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    return clf, metrics


if __name__ == "__main__":
    clf, m = train_and_save_model()
    print("Model training complete:")
    print(f"  Precision: {m['precision']:.4f}")
    print(f"  Recall:    {m['recall']:.4f}")
    print(f"  F1 Score:  {m['f1']:.4f}")
    print(f"  FPR:       {m['false_positive_rate']:.4f}")
