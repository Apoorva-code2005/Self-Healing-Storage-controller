from __future__ import annotations

from typing import Dict, Optional
from pydantic import BaseModel, Field


class PredictionResponse(BaseModel):
    prediction_id: str
    node_id: str
    timestamp_utc: str
    risk_score: float
    model_name: str
    model_version: str
    target_horizon: Optional[str] = None
    anomaly_flag: bool
    explanation: Optional[str] = None


class SimulateFailureRequest(BaseModel):
    scenario_name: str = Field(..., description="Name or identifier of the scenario in config/scenarios/")
    advance_ticks: Optional[int] = Field(default=None, description="Number of ticks to simulate (defaults to end of faults + 2)")
    seed: Optional[int] = Field(default=None, description="Optional seed override for repeatable telemetry")


class SimulateFailureResponse(BaseModel):
    scenario_id: str
    name: str
    prediction_outcome: str
    seed: int
    ticks_simulated: int
    alarm_tick: Optional[int] = None
    failure_tick: Optional[int] = None
    measured_lead_time_s: Optional[float] = None
    node_statuses: Dict[str, str] = Field(default_factory=dict)
