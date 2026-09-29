"""Database connection, WAL mode configuration, thread-safe writer lock, and schema management."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import threading
from contextlib import contextmanager
from typing import Generator, Optional

from app.config import get_config


def format_utc_iso(dt: Optional[datetime] = None) -> str:
    """Return canonical UTC ISO 8601 string: YYYY-MM-DDTHH:MM:SS.ffffff+00:00.

    Guarantees fixed length (32 chars) and fixed '+00:00' timezone suffix so that
    lexicographical text comparisons (<, <=, >, >=) in SQLite match true chronological order.
    """
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    else:
        d = d.astimezone(timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


# Global thread-safe re-entrant writer lock to serialize SQLite writes and audit appends
DB_WRITE_LOCK = threading.RLock()

SCHEMA_DDL = """
-- ERCT SQLite Schema (Converted from PostgreSQL specifications)

CREATE TABLE IF NOT EXISTS exams (
    exam_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    start_at TEXT NOT NULL,
    duration_min INTEGER NOT NULL,
    required_version TEXT NOT NULL,
    config_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS centres (
    centre_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT,
    vendor TEXT,
    capacity INTEGER NOT NULL,
    power_backup INTEGER NOT NULL DEFAULT 0,
    backup_minutes INTEGER DEFAULT 0,
    software_version TEXT,
    status TEXT NOT NULL DEFAULT 'ready'
);

CREATE TABLE IF NOT EXISTS candidates (
    candidate_id TEXT PRIMARY KEY,
    exam_id TEXT REFERENCES exams(exam_id),
    centre_id TEXT REFERENCES centres(centre_id),
    seat_no TEXT,
    status TEXT NOT NULL DEFAULT 'registered'
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    candidate_id TEXT REFERENCES candidates(candidate_id),
    exam_id TEXT REFERENCES exams(exam_id),
    centre_id TEXT REFERENCES centres(centre_id),
    started_at TEXT,
    last_heartbeat_at TEXT,
    last_saved_seq INTEGER DEFAULT 0,
    answers_saved INTEGER DEFAULT 0,
    remaining_s INTEGER,
    last_ingested_at TEXT,
    state TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS centre_liveness (
    centre_id TEXT PRIMARY KEY,
    last_ingested_at TEXT NOT NULL,
    last_event_ts TEXT NOT NULL,
    events_total INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    ts TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    exam_id TEXT,
    centre_id TEXT,
    candidate_id TEXT,
    session_id TEXT,
    seq INTEGER,
    type TEXT NOT NULL,
    severity TEXT NOT NULL,
    payload TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_centre_ts ON events(centre_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_session_ts ON events(session_id, ts);

CREATE TABLE IF NOT EXISTS readiness_checks (
    check_id INTEGER PRIMARY KEY AUTOINCREMENT,
    exam_id TEXT,
    centre_id TEXT,
    score INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    checks TEXT NOT NULL,
    checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id TEXT PRIMARY KEY,
    exam_id TEXT,
    centre_id TEXT,
    type TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    detected_at TEXT NOT NULL,
    window_start TEXT,
    window_end TEXT,
    resolved_at TEXT,
    detection_rule TEXT,
    evidence TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_incidents_active_centre
ON incidents(exam_id, centre_id)
WHERE status IN ('open', 'recovering');

CREATE TABLE IF NOT EXISTS incident_timeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_incident_timeline_inc ON incident_timeline(incident_id, ts);

CREATE TABLE IF NOT EXISTS incident_impacts (
    incident_id TEXT REFERENCES incidents(incident_id),
    candidate_id TEXT REFERENCES candidates(candidate_id),
    session_id TEXT,
    lost_seconds INTEGER,
    unsaved_answers INTEGER,
    last_good_seq INTEGER,
    evidence_quality TEXT,
    remedy_recommended TEXT,
    extra_seconds INTEGER,
    rationale TEXT,
    rule_id TEXT,
    PRIMARY KEY (incident_id, candidate_id)
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT,
    candidate_id TEXT,
    remedy TEXT NOT NULL,
    mode TEXT NOT NULL,
    decided_by TEXT,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_queue (
    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_id TEXT,
    candidate_id TEXT,
    reason TEXT,
    status TEXT DEFAULT 'pending',
    assignee TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notices (
    notice_id INTEGER PRIMARY KEY AUTOINCREMENT,
    audience TEXT NOT NULL,
    target_id TEXT,
    incident_id TEXT,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    entry_type TEXT NOT NULL,
    ref_id TEXT,
    payload TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    entry_hash TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_log_seq ON audit_log(seq);

CREATE TABLE IF NOT EXISTS fault_commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    centre_id TEXT NOT NULL,
    fault_type TEXT NOT NULL,
    duration_s INTEGER NOT NULL,
    params TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT
);
"""


def resolve_db_path(db_path: Optional[str] = None) -> str:
    """Resolve database path relative to project root or from override."""
    if db_path:
        return db_path
    cfg = get_config()
    p = Path(cfg.database.path)
    if not p.is_absolute():
        p = Path(__file__).resolve().parent.parent / p
    return str(p)


def configure_pragmas(conn: sqlite3.Connection, is_memory: bool = False) -> None:
    """Configure SQLite pragmas for high concurrency, WAL mode, and integrity."""
    cursor = conn.cursor()
    if not is_memory:
        cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA foreign_keys = ON;")
    cursor.execute("PRAGMA busy_timeout = 5000;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.close()


@contextmanager
def get_db_connection(db_path: Optional[str] = None) -> Generator[sqlite3.Connection, None, None]:
    """Provide a thread-safe read-friendly SQLite connection with WAL mode."""
    target_path = resolve_db_path(db_path)
    is_memory = target_path == ":memory:"

    if not is_memory:
        Path(target_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        target_path,
        check_same_thread=False,
        timeout=10.0,
        isolation_level=None,  # Autocommit mode by default, explicit transaction for writes
    )
    conn.row_factory = sqlite3.Row
    configure_pragmas(conn, is_memory=is_memory)

    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_transaction(db_path: Optional[str] = None) -> Generator[sqlite3.Connection, None, None]:
    """Execute write operations protected by the global writer lock and transaction."""
    target_path = resolve_db_path(db_path)
    is_memory = target_path == ":memory:"

    if not is_memory:
        Path(target_path).parent.mkdir(parents=True, exist_ok=True)

    with DB_WRITE_LOCK:
        conn = sqlite3.connect(
            target_path,
            check_same_thread=False,
            timeout=15.0,
        )
        conn.row_factory = sqlite3.Row
        configure_pragmas(conn, is_memory=is_memory)

        try:
            with conn:
                yield conn
        finally:
            conn.close()


def init_db(db_path: Optional[str] = None) -> None:
    """Initialize database schema if tables do not exist."""
    with write_transaction(db_path) as conn:
        conn.executescript(SCHEMA_DDL)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(sessions);")
        cols = {row["name"] for row in cursor.fetchall()}
        if cols and "last_ingested_at" not in cols:
            cursor.execute("ALTER TABLE sessions ADD COLUMN last_ingested_at TEXT;")
        cursor.close()
