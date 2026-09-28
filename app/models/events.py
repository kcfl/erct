"""Event envelope and payload validation models adhering strictly to PRD Part 3."""
from __future__ import annotations

import uuid
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    SESSION_STARTED = "SESSION_STARTED"
    SESSION_SUBMITTED = "SESSION_SUBMITTED"
    HEARTBEAT = "HEARTBEAT"
    ANSWER_SAVED = "ANSWER_SAVED"
    POWER_LOSS = "POWER_LOSS"
    POWER_RESTORED = "POWER_RESTORED"
    NETWORK_DOWN = "NETWORK_DOWN"
    NETWORK_UP = "NETWORK_UP"
    LATENCY_SAMPLE = "LATENCY_SAMPLE"
    SOFTWARE_VERSION_REPORT = "SOFTWARE_VERSION_REPORT"
    CAPACITY_SAMPLE = "CAPACITY_SAMPLE"
    CRASH = "CRASH"
    LOGIN_FAIL = "LOGIN_FAIL"


class Severity(str, Enum):
    INFO = "info"
    WARN = "warn"
    ERROR = "error"
    CRITICAL = "critical"


class EventEnvelope(BaseModel):
    event_id: str = Field(description="Unique source-generated UUID string acting as idempotency key")
    schema_ver: int = Field(default=1, description="Event schema version")
    ts: str = Field(description="Source clock timestamp in ISO-8601 UTC")
    exam_id: str = Field(description="Unique identifier of the exam")
    centre_id: str = Field(description="Unique identifier of the testing centre")
    candidate_id: Optional[str] = Field(default=None, description="Pseudonymous candidate ID, null for centre events")
    session_id: Optional[str] = Field(default=None, description="Unique session ID, null for centre events")
    seq: int = Field(description="Per-source monotonic sequence counter")
    type: EventType = Field(description="Standardized event type")
    severity: Severity = Field(default=Severity.INFO, description="Event severity level")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Event payload dictionary")

    @field_validator("event_id")
    @classmethod
    def validate_uuid(cls, v: str) -> str:
        try:
            uuid.UUID(str(v))
        except ValueError:
            raise ValueError(f"event_id must be a valid UUID string, got: {v}")
        return str(v)

    @model_validator(mode="after")
    def validate_payload_requirements(self) -> EventEnvelope:
        """Validate required payload fields per PRD Part 3 Section 1 specifications."""
        etype = self.type
        p = self.payload

        if etype == EventType.SESSION_STARTED:
            if "duration_s" not in p:
                raise ValueError("SESSION_STARTED payload requires 'duration_s'")

        elif etype == EventType.HEARTBEAT:
            if "latency_ms" not in p or "remaining_s" not in p:
                raise ValueError("HEARTBEAT payload requires 'latency_ms' and 'remaining_s'")

        elif etype == EventType.ANSWER_SAVED:
            missing = [k for k in ("question_id", "answer_hash", "saved_seq") if k not in p]
            if missing:
                raise ValueError(f"ANSWER_SAVED payload requires {missing}")

        elif etype == EventType.POWER_LOSS:
            missing = [k for k in ("source", "backup_minutes") if k not in p]
            if missing:
                raise ValueError(f"POWER_LOSS payload requires {missing}")

        elif etype == EventType.SOFTWARE_VERSION_REPORT:
            missing = [k for k in ("version", "required_version") if k not in p]
            if missing:
                raise ValueError(f"SOFTWARE_VERSION_REPORT payload requires {missing}")

        elif etype == EventType.LATENCY_SAMPLE:
            missing = [k for k in ("p50_ms", "p95_ms", "error_rate") if k not in p]
            if missing:
                raise ValueError(f"LATENCY_SAMPLE payload requires {missing}")

        elif etype == EventType.CAPACITY_SAMPLE:
            missing = [k for k in ("active_sessions", "max_sessions", "cpu_pct") if k not in p]
            if missing:
                raise ValueError(f"CAPACITY_SAMPLE payload requires {missing}")

        elif etype == EventType.CRASH:
            missing = [k for k in ("component", "error_code") if k not in p]
            if missing:
                raise ValueError(f"CRASH payload requires {missing}")

        return self


class BatchIngestRequest(BaseModel):
    events: List[EventEnvelope]


class BatchIngestResponse(BaseModel):
    accepted: int = 0
    duplicates: int = 0
    rejected: int = 0
    errors: List[str] = Field(default_factory=list)
