"""Candidate and institutional notices generator (Phase 3b-ii).

Generates plain-language notifications in display_tz, guarantees idempotency via ref_key,
and emits audit log entries for candidate notices.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from app.config import get_config
from app.core.audit_chain import append_audit_entry


def format_display_time(utc_iso: Optional[str], tz_name: str = "Asia/Kolkata") -> str:
    """Format UTC ISO timestamp to localized HH:MM in display timezone."""
    if not utc_iso:
        return "recently"
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        local_dt = dt.astimezone(ZoneInfo(tz_name))
        return local_dt.strftime("%H:%M")
    except Exception:
        return "recently"


def format_decision_message(remedy: str, extra_seconds: int) -> str:
    """Return plain-language decision message strictly according to specification."""
    if remedy == "resume":
        if extra_seconds > 0:
            return f"Decision: your session is resumed. {extra_seconds} seconds lost in the interruption are added back to your time."
        return "Decision: your session is resumed. No time was lost."
    elif remedy == "extra_time":
        m = extra_seconds // 60
        s = extra_seconds % 60
        return f"Decision: you are granted {extra_seconds} seconds ({m} min {s} s) of extra time."
    elif remedy == "retest" or remedy == "retest_recommended":
        return "Decision: you are asked to re-sit the exam. Details will follow from your centre."
    elif remedy == "no_compensation":
        return "Decision: after review, no compensation is needed."
    return f"Decision: remedy applied is {remedy}."


def emit_notice(
    conn: sqlite3.Connection,
    audience: str,
    target_id: Optional[str],
    incident_id: Optional[str],
    kind: str,
    ref_key: str,
    message: str,
    now_iso: str,
) -> Optional[int]:
    """Insert a notice idempotently using ref_key. Emits audit entry for candidate notices."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO notices (audience, target_id, incident_id, kind, ref_key, message, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (audience, target_id, incident_id, kind, ref_key, message, now_iso),
    )

    if cursor.rowcount > 0:
        notice_id = cursor.lastrowid
        if audience == "candidate" and target_id:
            append_audit_entry(
                entry_type="notice",
                ref_id=f"notice:{notice_id}",
                payload={
                    "notice_id": notice_id,
                    "kind": kind,
                    "target_id": target_id,
                    "incident_id": incident_id,
                    "message": message,
                },
                ts_iso=now_iso,
                conn=conn,
            )
        cursor.close()
        return notice_id

    cursor.close()
    return None
