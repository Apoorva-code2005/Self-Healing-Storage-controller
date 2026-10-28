"""app/services/predictor.py

Machine learning predictor for disk failure risk using Backblaze SMART telemetry.
Loads trained Random Forest model artifact and scores SMART metrics with target horizon.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import pandas as pd

from app.config import ThresholdsConfig

logger = logging.getLogger("self_healing_storage.predictor")

SMART_FEATURE_COLS = [
    "smart_5_raw",    # Reallocated Sectors Count
    "smart_9_raw",    # Power-On Hours
    "smart_187_raw",  # Reported Uncorrectable Errors
    "smart_194_raw",  # Temperature
    "smart_197_raw",  # Current Pending Sector Count
    "smart_198_raw",  # Offline Uncorrectable Sector Count
]

DEFAULT_MODEL_PATH = Path(__file__).resolve().parent.parent.parent / "models" / "random_forest_v1.joblib"


class SmartPredictor:
    """Predicts disk failure risk using a Random Forest model trained on SMART features."""

    def __init__(self, model_path: Optional[str | Path] = None) -> None:
        self.model_path = Path(model_path) if model_path else DEFAULT_MODEL_PATH
        self.model: Any = None
        self.load_model(self.model_path)

    def load_model(self, model_path: Optional[str | Path] = None) -> bool:
        """Loads the serialized model artifact using joblib."""
        path = Path(model_path) if model_path else self.model_path
        if path.is_file():
            try:
                self.model = joblib.load(path)
                self.model_path = path
                logger.info(f"Loaded SMART failure predictor model from {path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load model from {path}: {e}")
                self.model = None
                return False
        else:
            logger.warning(f"Model file not found at {path}. Predictor will use fallback.")
            self.model = None
            return False

    def predict_risk(
        self,
        record: Dict[str, Any],
        thresholds: Optional[ThresholdsConfig] = None,
    ) -> Tuple[float, bool, str]:
        """Scores a telemetry record containing SMART features.

        Returns:
            (risk_score in [0, 1], anomaly_flag: bool, explanation: str)
        """
        risk_thresh = thresholds.risk_threshold if thresholds else 0.7

        # Extract features
        features = [float(record.get(col, 0) or 0) for col in SMART_FEATURE_COLS]

        # Identify elevated indicators for explanation
        s5 = record.get("smart_5_raw", 0) or 0
        s187 = record.get("smart_187_raw", 0) or 0
        s194 = record.get("smart_194_raw", 0) or 0
        s197 = record.get("smart_197_raw", 0) or 0
        s198 = record.get("smart_198_raw", 0) or 0

        elevated = []
        if s5 > 0:
            elevated.append(f"smart_5_raw={int(s5)}")
        if s187 > 0:
            elevated.append(f"smart_187_raw={int(s187)}")
        if s197 > 0:
            elevated.append(f"smart_197_raw={int(s197)}")
        if s198 > 0:
            elevated.append(f"smart_198_raw={int(s198)}")
        if s194 > 45:
            elevated.append(f"smart_194_raw={int(s194)}C")

        if self.model is not None:
            df_in = pd.DataFrame([features], columns=SMART_FEATURE_COLS)
            proba = self.model.predict_proba(df_in)[0]
            classes = list(self.model.classes_)
            if 1 in classes:
                fail_idx = classes.index(1)
                prob = float(proba[fail_idx])
            else:
                prob = 0.0

            risk_score = round(prob, 4)
            anomaly_flag = risk_score >= risk_thresh

            if elevated:
                explanation = (
                    f"RF failure risk={risk_score:.4f} (horizon 7d): "
                    f"elevated indicators ({', '.join(elevated)})"
                )
            else:
                explanation = (
                    f"RF failure risk={risk_score:.4f} (horizon 7d): "
                    f"SMART metrics nominal"
                )
            return risk_score, anomaly_flag, explanation

        # Fallback heuristic if model file not available
        if elevated:
            risk_score = max(risk_thresh, 0.85)
            explanation = (
                f"Fallback heuristic failure risk={risk_score:.2f}: "
                f"elevated indicators ({', '.join(elevated)})"
            )
            return risk_score, True, explanation
        else:
            risk_score = 0.05
            explanation = "Fallback heuristic: SMART metrics nominal"
            return risk_score, False, explanation


_DEFAULT_PREDICTOR: Optional[SmartPredictor] = None


def get_default_predictor() -> SmartPredictor:
    """Returns singleton SmartPredictor instance."""
    global _DEFAULT_PREDICTOR
    if _DEFAULT_PREDICTOR is None:
        _DEFAULT_PREDICTOR = SmartPredictor()
    return _DEFAULT_PREDICTOR


def score_smart_telemetry(
    record: Dict[str, Any],
    thresholds: Optional[ThresholdsConfig] = None,
) -> Tuple[float, bool, str]:
    """Convenience function to score a SMART telemetry record."""
    predictor = get_default_predictor()
    return predictor.predict_risk(record, thresholds=thresholds)
