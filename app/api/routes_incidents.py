"""Incidents public read-only API routes."""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.config import get_config
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

    impact_summary = evidence_parsed.get("impact_summary")

    return {
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
        "impact_summary": impact_summary,
        "impact_computed_at": row["impact_computed_at"] if "impact_computed_at" in row.keys() else None,
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


@router.get("/{incident_id}/impact")
def get_incident_impact(
    incident_id: str,
    remedy: Optional[str] = None,
    quality: Optional[str] = None,
    decision: Optional[str] = None,
) -> Dict[str, Any]:
    """Retrieve computed impact analysis and remedy recommendations for an incident."""
    cfg = get_config()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM incidents WHERE incident_id = ?;", (incident_id,))
        inc = cursor.fetchone()
        if not inc:
            cursor.close()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Incident '{incident_id}' not found.",
            )

        # While incident is not resolved, return empty rows list with computed_at null
        if inc["status"] != "resolved":
            cursor.close()
            return {
                "incident_id": incident_id,
                "computed_at": None,
                "summary": None,
                "rows": [],
            }

        evidence_parsed = {}
        if inc["evidence"]:
            try:
                evidence_parsed = json.loads(inc["evidence"])
            except Exception:
                evidence_parsed = {}
        summary = evidence_parsed.get("impact_summary")

        # Fetch latest decisions for this incident
        cursor.execute(
            """
            SELECT decision_id, candidate_id, mode, remedy, extra_seconds, decided_by, decided_at
            FROM decisions
            WHERE incident_id = ?
            ORDER BY decision_id ASC;
            """,
            (incident_id,),
        )
        dec_rows = cursor.fetchall()
        latest_decisions: Dict[str, Any] = {}
        for dr in dec_rows:
            latest_decisions[dr["candidate_id"]] = dict(dr)

        query = "SELECT * FROM incident_impacts WHERE incident_id = ?"
        params: List[Any] = [incident_id]
        if remedy:
            query += " AND remedy_recommended = ?"
            params.append(remedy)
        if quality:
            query += " AND evidence_quality = ?"
            params.append(quality)
        query += " ORDER BY candidate_id ASC;"

        cursor.execute(query, params)
        raw_rows = cursor.fetchall()

        # Update summary with decision counts and fairness status
        if summary:
            from app.core.fairness import compute_exam_fairness
            total_exp = summary.get("exposed", len(raw_rows))
            decided_count = len(latest_decisions)
            pending_count = max(0, total_exp - decided_count)
            summary["pending_decisions"] = pending_count
            summary["decided"] = decided_count
            fairness = compute_exam_fairness(conn, inc["exam_id"], cfg)
            summary["fairness_status"] = fairness["status"]

        cursor.close()

        formatted_rows = []
        for r in raw_rows:
            cand_id = r["candidate_id"]
            d = latest_decisions.get(cand_id)
            if d:
                dec_block = {
                    "status": d["mode"],  # 'approved' or 'overridden'
                    "decision_id": d["decision_id"],
                    "remedy": d["remedy"],
                    "extra_seconds": d["extra_seconds"],
                    "decided_by": d["decided_by"],
                    "decided_at": d["decided_at"],
                }
            else:
                dec_block = {
                    "status": "pending",
                    "decision_id": None,
                    "remedy": None,
                    "extra_seconds": None,
                    "decided_by": None,
                    "decided_at": None,
                }

            # Filter by decision status if requested
            if decision == "pending" and dec_block["status"] != "pending":
                continue
            if decision == "decided" and dec_block["status"] not in ("approved", "overridden"):
                continue

            ev_dict = {}
            if r["evidence"]:
                try:
                    ev_dict = json.loads(r["evidence"])
                except Exception:
                    ev_dict = {}

            formatted_rows.append({
                "incident_id": r["incident_id"],
                "candidate_id": r["candidate_id"],
                "session_id": r["session_id"],
                "lost_seconds": r["lost_seconds"],
                "unsaved_answers": r["unsaved_answers"],
                "last_good_seq": r["last_good_seq"],
                "evidence_quality": r["evidence_quality"],
                "remedy_recommended": r["remedy_recommended"],
                "extra_seconds": r["extra_seconds"],
                "rationale": r["rationale"],
                "rule_id": r["rule_id"],
                "evidence": ev_dict,
                "computed_at": r["computed_at"],
                "impact_version": r["impact_version"],
                "decision": dec_block,
            })

    return {
        "incident_id": incident_id,
        "computed_at": inc["impact_computed_at"],
        "summary": summary,
        "rows": formatted_rows,
    }


review_router = APIRouter(prefix="/v1/review-queue", tags=["Review Queue"])


@review_router.get("")
def list_review_queue(status: Optional[str] = "pending") -> List[Dict[str, Any]]:
    """Retrieve review queue rows filtered by status (default 'pending')."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        query = "SELECT * FROM review_queue WHERE 1=1"
        params: List[Any] = []
        if status and status.lower() != "all":
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC, review_id DESC;"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()

        results = []
        for r in rows:
            results.append({
                "review_id": r["review_id"],
                "incident_id": r["incident_id"],
                "candidate_id": r["candidate_id"],
                "session_id": r["session_id"],
                "reason": r["reason"],
                "status": r["status"],
                "assignee": r["assignee"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
            })

    return results

