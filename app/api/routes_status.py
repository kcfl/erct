"""Candidate status and notification feed API endpoints (Phase 3b-ii)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, status

from app.config import get_config
from app.core.fairness import compute_exam_fairness
from app.db import get_db_connection

router = APIRouter(prefix="/v1", tags=["Status & Notices"])


@router.get("/status/{candidate_id}")
def get_candidate_status(candidate_id: str) -> Dict[str, Any]:
    """Retrieve isolated status, timeline, active notices, and remedy state for a specific candidate."""
    now_iso = datetime.now(timezone.utc).isoformat()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # 1. Fetch candidate
        cursor.execute(
            """
            SELECT c.candidate_id, c.exam_id, c.centre_id, s.session_id, s.state as session_state, s.last_saved_seq
            FROM candidates c
            LEFT JOIN sessions s ON c.candidate_id = s.candidate_id
            WHERE c.candidate_id = ?;
            """,
            (candidate_id,),
        )
        cand_row = cursor.fetchone()
        if not cand_row:
            cursor.close()
            raise HTTPException(status_code=404, detail=f"Candidate {candidate_id} not found")

        cid = cand_row["centre_id"]
        exam_id = cand_row["exam_id"]
        session_id = cand_row["session_id"]
        session_state = cand_row["session_state"] or "enrolled"
        last_save_seq = cand_row["last_saved_seq"] or 0

        # 2. Check for incident on candidate's centre
        cursor.execute(
            """
            SELECT incident_id, type, status, window_start, window_end, detected_at, resolved_at
            FROM incidents
            WHERE centre_id = ?
            ORDER BY detected_at DESC LIMIT 1;
            """,
            (cid,),
        )
        inc_row = cursor.fetchone()

        incident_data: Optional[Dict[str, Any]] = None
        remedy_status = "none"
        decision_data: Optional[Dict[str, Any]] = None

        if inc_row:
            inc_id = inc_row["incident_id"]
            incident_data = {
                "incident_id": inc_id,
                "type": inc_row["type"],
                "status": inc_row["status"],
                "started_at": inc_row["window_start"] or inc_row["detected_at"],
                "ended_at": inc_row["window_end"] or inc_row["resolved_at"],
            }

            # Check decision first (latest decision)
            cursor.execute(
                """
                SELECT decision_id, remedy, extra_seconds, decided_at
                FROM decisions
                WHERE incident_id = ? AND candidate_id = ?
                ORDER BY decision_id DESC LIMIT 1;
                """,
                (inc_id, candidate_id),
            )
            dec_row = cursor.fetchone()
            if dec_row:
                remedy_status = "decided"
                decision_data = {
                    "remedy": dec_row["remedy"],
                    "extra_seconds": dec_row["extra_seconds"],
                }
            else:
                # Check impact row
                cursor.execute(
                    """
                    SELECT remedy_recommended
                    FROM incident_impacts
                    WHERE incident_id = ? AND candidate_id = ?;
                    """,
                    (inc_id, candidate_id),
                )
                imp_row = cursor.fetchone()
                if imp_row:
                    if imp_row["remedy_recommended"] == "manual_review":
                        remedy_status = "under_review"
                    else:
                        remedy_status = "awaiting_decision"

        # 3. Candidate's notices
        cursor.execute(
            """
            SELECT notice_id, kind, message, created_at
            FROM notices
            WHERE audience = 'candidate' AND target_id = ?
            ORDER BY notice_id ASC;
            """,
            (candidate_id,),
        )
        notice_rows = cursor.fetchall()
        notices_list = [
            {
                "notice_id": r["notice_id"],
                "kind": r["kind"],
                "message": r["message"],
                "created_at": r["created_at"],
            }
            for r in notice_rows
        ]

        if notices_list:
            latest_message = notices_list[-1]["message"]
        elif not incident_data:
            latest_message = "Your session is proceeding normally. All responses are saved."
        else:
            latest_message = "Your session was affected by a centre disruption. The exam team is investigating."

        cursor.close()

        return {
            "candidate_id": candidate_id,
            "session_id": session_id,
            "centre_id": cid,
            "session_state": session_state,
            "incident": incident_data,
            "last_confirmed_save_seq": last_save_seq,
            "remedy": {
                "status": remedy_status,
                "decision": decision_data,
            },
            "latest_message": latest_message,
            "notices": notices_list,
            "generated_at": now_iso,
        }


@router.get("/notices")
def list_notices(
    audience: Optional[str] = Query(None, description="Filter by audience (candidate, centre, admin)"),
    target_id: Optional[str] = Query(None, description="Filter by target_id (candidate_id, centre_id)"),
    incident_id: Optional[str] = Query(None, description="Filter by incident_id"),
) -> List[Dict[str, Any]]:
    """Read-only feed of institutional and candidate notices."""
    query = "SELECT notice_id, audience, target_id, incident_id, kind, ref_key, message, created_at FROM notices WHERE 1=1"
    params: List[Any] = []

    if audience:
        query += " AND audience = ?"
        params.append(audience)
    if target_id:
        query += " AND target_id = ?"
        params.append(target_id)
    if incident_id:
        query += " AND incident_id = ?"
        params.append(incident_id)

    query += " ORDER BY notice_id ASC;"

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        return [dict(r) for r in rows]


@router.get("/fairness")
def get_exam_fairness(
    exam_id: Optional[str] = Query(None, description="Exam ID (defaults to active exam from config)"),
) -> Dict[str, Any]:
    """Retrieve cross-centre remedy fairness statistics and disparity flags."""
    cfg = get_config()
    target_exam = exam_id or cfg.exam.id
    with get_db_connection() as conn:
        return compute_exam_fairness(conn, target_exam, cfg)
