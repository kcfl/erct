"""Configuration loader and schema validator for ERCT."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import yaml


class DatabaseConfig(BaseModel):
    path: str = "data/erct.db"


class ExamConfig(BaseModel):
    id: str = "EX-2026-PS6-01"
    name: str = "MPOnline Assessment Ecosystem Exam 2026"
    duration_min: int = 120
    required_version: str = "4.2.1"
    exam_clock_speed: float = 1.0


class ControlConfig(BaseModel):
    key: str = "ctrl-secret-key-2026"


class FaultConfig(BaseModel):
    centre: str
    type: str
    at_s: Optional[int] = None
    duration_s: Optional[int] = None
    source: Optional[str] = None
    backup_minutes: Optional[int] = None


class SimulationConfig(BaseModel):
    centres: int = 5
    candidates_per_centre: int = 40
    heartbeat_interval_s: int = 2
    flaky_fraction: float = 0.1
    seed: int = 36
    restore_boot_delay_s: List[float] = Field(default_factory=lambda: [2.0, 20.0])
    faults: List[FaultConfig] = Field(default_factory=list)


class CentreConfig(BaseModel):
    id: str
    name: str
    city: str = "Bhopal"
    vendor: str = "EduTech Sol"
    capacity: int = 40
    power_backup: bool = True
    backup_minutes: int = 60
    software_version: str = "4.2.1"
    api_key: str = "key-default"


class ReadinessWeights(BaseModel):
    version: int = 40
    power_backup: int = 30
    capacity: int = 30


class ReadinessConfig(BaseModel):
    min_score: int = 70
    weights: ReadinessWeights = Field(default_factory=ReadinessWeights)


class AnomalyConfig(BaseModel):
    enabled: bool = True
    threshold: float = 0.9


class DetectionConfig(BaseModel):
    tick_s: float = 1.0
    startup_grace_s: float = 15.0
    min_active_sessions: int = 5
    close_fraction: float = 0.2
    close_ticks: int = 2
    recovery_resume_fraction: float = 0.9
    recovery_max_wait_s: float = 30.0
    settle_s: float = 5.0
    escalate_after_s: float = 120.0
    ingest_stall_s: float = 8.0
    heartbeat_gap_s: int = 6
    centre_loss_fraction: float = 0.6
    centre_loss_window_s: int = 10
    latency_p95_ms: int = 1500
    anomaly: AnomalyConfig = Field(default_factory=AnomalyConfig)


class IntegrityConfig(BaseModel):
    max_timestamp_skew_s: int = 15
    login_fail_burst_threshold: int = 5
    seq_gap_threshold: int = 10


class RemedyRuleConfig(BaseModel):
    id: str
    desc: str
    condition: str
    action: str
    buffer_s: Optional[int] = None


class RemedyFairnessConfig(BaseModel):
    max_extra_time_disparity_s: int = 300


class CommsConfig(BaseModel):
    candidate_message_delay_s: int = 30


class AuditImmudbConfig(BaseModel):
    enabled: bool = False


class AuditConfig(BaseModel):
    anchor_every: int = 200
    immudb: AuditImmudbConfig = Field(default_factory=AuditImmudbConfig)


class AppConfig(BaseModel):
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    exam: ExamConfig = Field(default_factory=ExamConfig)
    control: ControlConfig = Field(default_factory=ControlConfig)
    simulation: SimulationConfig = Field(default_factory=SimulationConfig)
    centres: List[CentreConfig] = Field(default_factory=list)
    readiness: ReadinessConfig = Field(default_factory=ReadinessConfig)
    detection: DetectionConfig = Field(default_factory=DetectionConfig)
    integrity: IntegrityConfig = Field(default_factory=IntegrityConfig)
    remedy_rules: List[RemedyRuleConfig] = Field(default_factory=list)
    remedy_fairness: RemedyFairnessConfig = Field(default_factory=RemedyFairnessConfig)
    comms: CommsConfig = Field(default_factory=CommsConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)


_CONFIG_CACHE: Optional[AppConfig] = None


def find_config_path(override_path: Optional[str] = None) -> Path:
    """Locate config.yaml across expected relative paths."""
    if override_path:
        p = Path(override_path)
        if p.exists():
            return p
        raise FileNotFoundError(f"Configuration file not found at: {override_path}")

    env_path = os.environ.get("ERCT_CONFIG_PATH")
    if env_path and Path(env_path).exists():
        return Path(env_path)

    # Search current directory, parent, or project root
    candidates = [
        Path("config.yaml"),
        Path("../config.yaml"),
        Path(__file__).resolve().parent.parent / "config.yaml",
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()

    raise FileNotFoundError("Could not locate config.yaml in workspace candidates.")


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """Load and validate config.yaml into an AppConfig instance."""
    path = find_config_path(config_path)
    with open(path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f) or {}
    return AppConfig.model_validate(raw_data)


def get_config() -> AppConfig:
    """Retrieve cached global AppConfig, loading from disk on first call."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        _CONFIG_CACHE = load_config()
    return _CONFIG_CACHE


def reload_config(config_path: Optional[str] = None) -> AppConfig:
    """Force reload of the configuration file."""
    global _CONFIG_CACHE
    _CONFIG_CACHE = load_config(config_path)
    return _CONFIG_CACHE
