"""Incidents public read-only API routes."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.db import get_db_connection

router = APIRouter(prefix="/v1/incidents", tags=["Incidents"])


def _format_incident(row: Any, timeline_rows: List[Any]) -> Dict[str, Any]:
    evidence_raw = row["evidence"]
    if isinstance(evidence_raw, str):
        try:
            evidence_parsed = json.loads(evidence_raw)
        except Exception:
            evidence_parsed = {}
    elif isinstance(evidence_raw, dict):
        evidence_parsed = evidence_raw
    else:
        evidence_parsed = {}

    timeline = []
    for tr in timeline_rows:
        detail_raw = tr["detail"]
        if isinstance(detail_raw, str):
            try:
                detail_parsed = json.loads(detail_raw)
            except Exception:
                detail_parsed = {"raw": detail_raw}
        else:
            detail_parsed = detail_raw or {}

        timeline.append({
            "id": tr["id"],
            "ts": tr["ts"],
            "kind": tr["kind"],
            "detail": detail_parsed,
        })

    return {
        "id": row["incident_id"],
        "incident_id": row["incident_id"],
        "exam_id": row["exam_id"],
        "centre_id": row["centre_id"],
        "type": row["type"],
        "severity": row["severity"],
        "status": row["status"],
        "detected_at": row["detected_at"],
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "resolved_at": row["resolved_at"],
        "detection_rule": row["detection_rule"],
        "evidence": evidence_parsed,
        "timeline": timeline,
    }


@router.get("")
def list_incidents(
    status: Optional[str] = None,
    centre_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List incidents filtered by status and/or centre_id, including evidence and timeline."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        query = "SELECT * FROM incidents WHERE 1=1"
        params: List[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if centre_id:
            query += " AND centre_id = ?"
            params.append(centre_id)
        query += " ORDER BY detected_at DESC;"

        cursor.execute(query, params)
        inc_rows = cursor.fetchall()

        results = []
        for inc in inc_rows:
            cursor.execute(
                "SELECT id, ts, kind, detail FROM incident_timeline WHERE incident_id = ? ORDER BY ts ASC, id ASC;",
                (inc["incident_id"],),
            )
            tl_rows = cursor.fetchall()
            results.append(_format_incident(inc, tl_rows))

        cursor.close()

    return results


@router.get("/{incident_id}")
def get_incident(incident_id: str) -> Dict[str, Any]:
    """Retrieve detailed record of a single incident including evidence and timeline."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM incidents WHERE incident_id = ?;", (incident_id,))
        row = cursor.fetchone()
        if not row:
            cursor.close()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Incident '{incident_id}' not found.",
            )

        cursor.execute(
            "SELECT id, ts, kind, detail FROM incident_timeline WHERE incident_id = ? ORDER BY ts ASC, id ASC;",
            (incident_id,),
        )
        tl_rows = cursor.fetchall()
        cursor.close()

    return _format_incident(row, tl_rows)
