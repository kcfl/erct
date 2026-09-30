"""Operational, readiness, and audit management endpoints for ERCT control tower."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Body, Header, HTTPException, status
from pydantic import BaseModel, Field

from app.api.routes_control import verify_control_key
from app.config import get_config
from app.core.audit_chain import (
    get_audit_trail,
    restore_audit_entry_for_demo,
    tamper_audit_entry_for_demo,
    verify_audit_chain,
)
from app.core.detection import parse_utc_iso
from app.db import get_db_connection

router = APIRouter(tags=["Operations"])


class TamperAuditRequest(BaseModel):
    seq: int = Field(description="Audit log sequence number to tamper with")


class RestoreAuditRequest(BaseModel):
    seq: Optional[int] = Field(default=None, description="Audit log sequence number to restore. If null, restores all.")


@router.get(
    "/v1/centres/{centre_id}/sessions",
    status_code=status.HTTP_200_OK,
    summary="List all sessions for a specific centre",
)
def get_centre_sessions(centre_id: str) -> List[Dict[str, Any]]:
    """Return all session details for a specific centre in one query.
    
    Returns 404 if the centre is not found in the database.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT c.centre_id, s.session_id, s.candidate_id, s.state, s.last_ingested_at, s.remaining_s, s.last_saved_seq
            FROM centres c
            LEFT JOIN sessions s ON c.centre_id = s.centre_id
            WHERE c.centre_id = ?
            ORDER BY s.session_id ASC;
            """,
            (centre_id,),
        )
        rows = cursor.fetchall()
        cursor.close()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown centre '{centre_id}'.",
        )

    # Centre exists in database but has no sessions assigned
    if len(rows) == 1 and rows[0]["session_id"] is None:
        return []

    now = datetime.now(timezone.utc)
    results: List[Dict[str, Any]] = []
    for r in rows:
        l_ing = r["last_ingested_at"]
        age_s = round(max(0.0, (now - parse_utc_iso(l_ing)).total_seconds()), 2) if l_ing else None
        results.append({
            "session_id": r["session_id"],
            "candidate_id": r["candidate_id"],
            "state": r["state"],
            "last_ingested_age_s": age_s,
            "remaining_s": r["remaining_s"],
            "last_saved_seq": r["last_saved_seq"],
        })
    return results


@router.get(
    "/v1/readiness",
    status_code=status.HTTP_200_OK,
    summary="Retrieve centre readiness evaluation results",
)
def get_readiness() -> Dict[str, Any]:
    """Static config-based scoring only, no live probing.
    
    Evaluates centre readiness against exam requirements in config.yaml.
    Weights from readiness.weights (40/30/30), min_score 70.
    Software version mismatch is a HARD block (passed=false) regardless of score.
    """
    cfg = get_config()
    weights = cfg.readiness.weights
    min_score = cfg.readiness.min_score
    req_version = cfg.exam.required_version
    needed_capacity = cfg.simulation.candidates_per_centre

    centres_out: List[Dict[str, Any]] = []
    for c in cfg.centres:
        v_ok = (c.software_version == req_version)
        p_ok = bool(c.power_backup and c.backup_minutes > 0)
        cap_ok = (c.capacity >= needed_capacity)

        score = 0
        if v_ok:
            score += weights.version
        if p_ok:
            score += weights.power_backup
        if cap_ok:
            score += weights.capacity

        passed = (score >= min_score) and v_ok

        blocked_reason: Optional[str] = None
        if not v_ok:
            blocked_reason = f"Software version mismatch: have {c.software_version}, need {req_version}"
        elif score < min_score:
            reasons = []
            if not p_ok:
                reasons.append("insufficient power backup")
            if not cap_ok:
                reasons.append(f"insufficient capacity ({c.capacity} < {needed_capacity})")
            blocked_reason = f"Readiness score {score} below required {min_score}: " + ", ".join(reasons)

        centres_out.append({
            "centre_id": c.id,
            "name": c.name,
            "score": score,
            "passed": passed,
            "checks": {
                "version": {
                    "ok": v_ok,
                    "have": c.software_version,
                    "need": req_version,
                },
                "power_backup": {
                    "ok": p_ok,
                    "minutes": c.backup_minutes,
                },
                "capacity": {
                    "ok": cap_ok,
                    "have": c.capacity,
                    "need": needed_capacity,
                },
            },
            "blocked_reason": blocked_reason,
        })

    return {
        "exam_id": cfg.exam.id,
        "required_version": req_version,
        "min_score": min_score,
        "centres": centres_out,
    }


@router.get(
    "/v1/audit/verify",
    status_code=status.HTTP_200_OK,
    summary="Cryptographic verification of audit log chain",
)
def get_audit_verify() -> Dict[str, Any]:
    """Verify cryptographic integrity of the audit log hash chain."""
    res = verify_audit_chain()
    return {
        "ok": res.ok,
        "total_entries": res.total_entries,
        "failing_seq": res.failing_seq,
        "head_hash": res.head_hash,
        "error": res.error,
    }


@router.get(
    "/v1/audit/trail",
    status_code=status.HTTP_200_OK,
    summary="Retrieve recent audit entries (newest last)",
)
def get_audit_trail_endpoint(limit: int = 50) -> List[Dict[str, Any]]:
    """Retrieve the recent audit log entries, newest last.
    
    Clamps limit to 1..200. Returns newest last (ascending chronological sequence).
    """
    clamped_limit = max(1, min(200, limit))
    entries = get_audit_trail(limit=clamped_limit)
    sorted_entries = sorted(entries, key=lambda e: e.seq)
    return [
        {
            "seq": e.seq,
            "ts": e.ts,
            "entry_type": e.entry_type,
            "ref_id": e.ref_id,
            "entry_hash": e.entry_hash,
            "prev_hash": e.prev_hash,
        }
        for e in sorted_entries
    ]


@router.post(
    "/v1/audit/tamper",
    status_code=status.HTTP_200_OK,
    summary="[DEMO ONLY] Tamper with an audit chain entry",
)
def tamper_audit_endpoint(
    req: TamperAuditRequest,
    x_control_key: Optional[str] = Header(None, alias="X-Control-Key"),
) -> Dict[str, Any]:
    """[DEMO ONLY] Directly tampers with an audit log row to demonstrate detection."""
    verify_control_key(x_control_key)
    try:
        return tamper_audit_entry_for_demo(seq=req.seq)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))


@router.post(
    "/v1/audit/restore",
    status_code=status.HTTP_200_OK,
    summary="[DEMO ONLY] Restore tampered audit chain entry",
)
def restore_audit_endpoint(
    req: Optional[RestoreAuditRequest] = Body(None),
    x_control_key: Optional[str] = Header(None, alias="X-Control-Key"),
) -> Dict[str, Any]:
    """[DEMO ONLY] Restores tampered audit rows to their original payloads."""
    verify_control_key(x_control_key)
    target_seq = req.seq if req else None
    try:
        return restore_audit_entry_for_demo(seq=target_seq)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
