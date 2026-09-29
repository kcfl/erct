"""Centres public read-only API routes."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import APIRouter
from pydantic import BaseModel

from app.config import get_config
from app.core.detection import parse_utc_iso
from app.db import get_db_connection

router = APIRouter(prefix="/v1/centres", tags=["Centres"])


class CentreStatusResponse(BaseModel):
    centre_id: str
    name: str
    city: str
    vendor: str
    software_version: str
    status: str  # healthy | degraded | down
    sessions_total: int
    sessions_active: int
    sessions_silent: int
    silent_fraction: float
    last_ingested_age_s: Optional[float] = None
    open_incident_id: Optional[str] = None


@router.get("", response_model=List[CentreStatusResponse])
def get_centres() -> List[Dict[str, Any]]:
    """Return operational telemetry, silence fraction, and health status for all centres."""
    cfg = get_config()
    now = datetime.now(timezone.utc)
    heartbeat_gap_s = cfg.detection.heartbeat_gap_s
    exam_id = cfg.exam.id

    results: List[Dict[str, Any]] = []

    with get_db_connection() as conn:
        cursor = conn.cursor()

        for c in cfg.centres:
            cid = c.id

            # 1. Total sessions seeded for this centre
            cursor.execute("SELECT COUNT(*) AS c FROM sessions WHERE centre_id = ?;", (cid,))
            total_sessions = cursor.fetchone()["c"]

            # 2. Active sessions: started_at is NOT NULL and state IN ('active', 'resumed')
            cursor.execute(
                """
                SELECT session_id, last_ingested_at, state
                FROM sessions
                WHERE centre_id = ? AND started_at IS NOT NULL AND state IN ('active', 'resumed', 'interrupted', 'under_review');
                """,
                (cid,),
            )
            rows = cursor.fetchall()

            active_sessions = sum(1 for r in rows if r["state"] in ("active", "resumed"))

            # 3. Silent sessions: now - last_ingested_at > heartbeat_gap_s
            silent_sessions = 0
            for r in rows:
                l_ing = r["last_ingested_at"]
                if not l_ing:
                    silent_sessions += 1
                else:
                    age = (now - parse_utc_iso(l_ing)).total_seconds()
                    if age > heartbeat_gap_s:
                        silent_sessions += 1

            D = len(rows)
            silent_fraction = round(silent_sessions / D, 3) if D > 0 else 0.0

            # 4. Check for open or recovering incident
            cursor.execute(
                """
                SELECT incident_id FROM incidents
                WHERE exam_id = ? AND centre_id = ? AND status IN ('open', 'recovering')
                LIMIT 1;
                """,
                (exam_id, cid),
            )
            inc_row = cursor.fetchone()
            open_inc_id = inc_row["incident_id"] if inc_row else None

            # 5. Last ingested age from centre_liveness
            cursor.execute("SELECT last_ingested_at FROM centre_liveness WHERE centre_id = ?;", (cid,))
            live_row = cursor.fetchone()
            last_age_s = None
            if live_row and live_row["last_ingested_at"]:
                last_age_s = round(max(0.0, (now - parse_utc_iso(live_row["last_ingested_at"])).total_seconds()), 2)

            # 6. Status determination:
            # - down: when an incident is open or recovering, or silent_fraction >= 0.6
            # - degraded: when 0.2 < silent_fraction < 0.6
            # - healthy: otherwise (silent_fraction <= 0.2 and no open/recovering incident)
            if open_inc_id or silent_fraction >= 0.6:
                centre_status = "down"
            elif 0.2 < silent_fraction < 0.6:
                centre_status = "degraded"
            else:
                centre_status = "healthy"

            results.append({
                "centre_id": cid,
                "name": c.name,
                "city": c.city,
                "vendor": c.vendor,
                "software_version": c.software_version,
                "status": centre_status,
                "sessions_total": total_sessions,
                "sessions_active": active_sessions,
                "sessions_silent": silent_sessions,
                "silent_fraction": silent_fraction,
                "last_ingested_age_s": last_age_s,
                "open_incident_id": open_inc_id,
            })

        cursor.close()

    return results
