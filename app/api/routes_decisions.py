"""Decisions and institutional review API endpoints (Phase 3b-ii)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Header, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.config import get_config
from app.core.audit_chain import append_audit_entry
from app.core.fairness import compute_exam_fairness
from app.core.notices import emit_notice, format_decision_message
from app.db import get_db_connection, write_transaction

router = APIRouter(prefix="/v1", tags=["Decisions"])
HTTP_422 = getattr(status, "HTTP_422_UNPROCESSABLE_CONTENT", 422)


def verify_controller_key(x_controller_key: Optional[str] = Header(None, alias="X-Controller-Key")) -> None:
    """Validate X-Controller-Key header against config.yaml."""
    cfg = get_config()
    expected_key = getattr(cfg.decision, "controller_key", "demo-controller-key")
    if not x_controller_key or x_controller_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid X-Controller-Key header",
        )


class CreateDecisionRequest(BaseModel):
    incident_id: str
    candidate_id: str
    action: str = Field(description="'approve' or 'override'")
    expected_rule_id: Optional[str] = None
    expected_extra_seconds: Optional[int] = None
    remedy: Optional[str] = None  # required for override
    extra_seconds: Optional[int] = None  # required for extra_time override
    decided_by: str
    reason: Optional[str] = None  # required for override (min_reason_chars)


class BulkApproveRequest(BaseModel):
    decided_by: str
    reason: Optional[str] = None
    acknowledge_fairness: bool = False


@router.post("/decisions", status_code=status.HTTP_201_CREATED)
def create_decision(
    req: CreateDecisionRequest,
    x_controller_key: Optional[str] = Header(None, alias="X-Controller-Key"),
) -> Dict[str, Any]:
    """Create an approved or overridden remedy decision for a candidate."""
    verify_controller_key(x_controller_key)
    cfg = get_config()
    now_iso = datetime.now(timezone.utc).isoformat()

    with write_transaction() as conn:
        cursor = conn.cursor()

        # 1. Incident must have impact computed
        cursor.execute(
            "SELECT incident_id, exam_id, centre_id, impact_computed_at FROM incidents WHERE incident_id = ?;",
            (req.incident_id,),
        )
        inc_row = cursor.fetchone()
        if not inc_row:
            raise HTTPException(status_code=404, detail=f"Incident {req.incident_id} not found")
        if not inc_row["impact_computed_at"]:
            raise HTTPException(status_code=409, detail="Impact has not been computed for this incident yet")

        # 2. Candidate row in incident_impacts
        cursor.execute(
            """
            SELECT candidate_id, session_id, remedy_recommended, extra_seconds, rule_id, impact_version
            FROM incident_impacts
            WHERE incident_id = ? AND candidate_id = ?;
            """,
            (req.incident_id, req.candidate_id),
        )
        impact_row = cursor.fetchone()
        if not impact_row:
            raise HTTPException(
                status_code=404,
                detail=f"Candidate {req.candidate_id} not found in impact assessment for incident {req.incident_id}",
            )

        current_rule_id = impact_row["rule_id"]
        current_extra_s = impact_row["extra_seconds"]
        recommended_remedy = impact_row["remedy_recommended"]
        impact_version = impact_row["impact_version"] or 1
        session_id = impact_row["session_id"]

        # 3. Action rules
        if req.action == "approve":
            if recommended_remedy == "manual_review":
                raise HTTPException(
                    status_code=409,
                    detail="Manual review cases need an explicit override with a reason.",
                )
            decided_remedy = recommended_remedy
            decided_extra_seconds = current_extra_s
            mode = "approved"
        elif req.action == "override":
            mode = "overridden"
            if not req.remedy or req.remedy not in cfg.decision.allowed_remedies:
                raise HTTPException(
                    status_code=HTTP_422,
                    detail=f"Remedy must be one of allowed remedies: {cfg.decision.allowed_remedies}",
                )
            if req.remedy == "extra_time":
                if req.extra_seconds is None or req.extra_seconds <= 0 or req.extra_seconds > cfg.decision.max_extra_seconds:
                    raise HTTPException(
                        status_code=HTTP_422,
                        detail=f"extra_time requires 0 < extra_seconds <= {cfg.decision.max_extra_seconds}",
                    )
                decided_extra_seconds = int(req.extra_seconds)
            else:
                decided_extra_seconds = 0

            if not req.reason or len(req.reason.strip()) < cfg.decision.min_reason_chars:
                raise HTTPException(
                    status_code=HTTP_422,
                    detail=f"Reason is required and must have at least {cfg.decision.min_reason_chars} characters",
                )
            decided_remedy = req.remedy
        else:
            raise HTTPException(status_code=HTTP_422, detail="Action must be 'approve' or 'override'")

        # 4. Stale guard
        if req.expected_rule_id is not None and req.expected_rule_id != current_rule_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Stale recommendation (rule_id changed)",
                    "current_rule_id": current_rule_id,
                    "current_extra_seconds": current_extra_s,
                },
            )
        if req.expected_extra_seconds is not None and req.expected_extra_seconds != current_extra_s:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Stale recommendation (extra_seconds changed)",
                    "current_rule_id": current_rule_id,
                    "current_extra_seconds": current_extra_s,
                },
            )

        # 5. Repeating decision check
        cursor.execute(
            """
            SELECT decision_id, action, remedy, extra_seconds, reason
            FROM decisions
            WHERE incident_id = ? AND candidate_id = ?
            ORDER BY decision_id DESC LIMIT 1;
            """,
            (req.incident_id, req.candidate_id),
        )
        prev_dec = cursor.fetchone()
        supersedes_id: Optional[int] = None
        if prev_dec:
            if (
                prev_dec["action"] == req.action
                and prev_dec["remedy"] == decided_remedy
                and prev_dec["extra_seconds"] == decided_extra_seconds
                and (req.action == "approve" or prev_dec["reason"] == req.reason)
            ):
                raise HTTPException(status_code=409, detail="already decided")
            supersedes_id = prev_dec["decision_id"]

        # 6. Insert decision row
        cursor.execute(
            """
            INSERT INTO decisions (
                incident_id, candidate_id, action, remedy, extra_seconds,
                rule_id, recommended_remedy, impact_version, mode, decided_by,
                reason, supersedes, acknowledged_fairness, fairness_snapshot, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?);
            """,
            (
                req.incident_id,
                req.candidate_id,
                req.action,
                decided_remedy,
                decided_extra_seconds,
                current_rule_id,
                recommended_remedy,
                impact_version,
                mode,
                req.decided_by,
                req.reason,
                supersedes_id,
                now_iso,
            ),
        )
        decision_id = cursor.lastrowid

        # 7. Audit entry
        append_audit_entry(
            entry_type="decision",
            ref_id=f"{req.incident_id}:{req.candidate_id}",
            payload={
                "decision_id": decision_id,
                "action": req.action,
                "remedy": decided_remedy,
                "extra_seconds": decided_extra_seconds,
                "recommended_remedy": recommended_remedy,
                "rule_id": current_rule_id,
                "decided_by": req.decided_by,
                "reason": req.reason,
                "supersedes": supersedes_id,
                "acknowledged_fairness": 0,
            },
            ts_iso=now_iso,
            conn=conn,
        )

        # 8. Notice to candidate
        msg = format_decision_message(decided_remedy, decided_extra_seconds)
        emit_notice(
            conn=conn,
            audience="candidate",
            target_id=req.candidate_id,
            incident_id=req.incident_id,
            kind="decision_final",
            ref_key=f"decision_final:{decision_id}",
            message=msg,
            now_iso=now_iso,
        )

        # 9. Manual review row state transition
        if recommended_remedy == "manual_review":
            cursor.execute(
                """
                UPDATE review_queue
                SET status = 'resolved', updated_at = ?
                WHERE incident_id = ? AND candidate_id = ?;
                """,
                (now_iso, req.incident_id, req.candidate_id),
            )
            cursor.execute(
                """
                UPDATE sessions
                SET state = 'resumed'
                WHERE candidate_id = ? AND state = 'under_review';
                """,
                (req.candidate_id,),
            )

        cursor.close()

        return {
            "decision_id": decision_id,
            "incident_id": req.incident_id,
            "candidate_id": req.candidate_id,
            "action": req.action,
            "mode": mode,
            "remedy": decided_remedy,
            "extra_seconds": decided_extra_seconds,
            "rule_id": current_rule_id,
            "recommended_remedy": recommended_remedy,
            "decided_by": req.decided_by,
            "reason": req.reason,
            "supersedes": supersedes_id,
            "decided_at": now_iso,
        }


@router.post("/incidents/{incident_id}/decisions/approve-all")
def approve_all_decisions(
    incident_id: str,
    req: BulkApproveRequest,
    x_controller_key: Optional[str] = Header(None, alias="X-Controller-Key"),
) -> Dict[str, Any]:
    """Bulk approve all non-manual-review undecided candidate remedies for an incident."""
    verify_controller_key(x_controller_key)
    cfg = get_config()
    now_iso = datetime.now(timezone.utc).isoformat()

    with write_transaction() as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT incident_id, exam_id, impact_computed_at FROM incidents WHERE incident_id = ?;",
            (incident_id,),
        )
        inc_row = cursor.fetchone()
        if not inc_row:
            raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")
        if not inc_row["impact_computed_at"]:
            raise HTTPException(status_code=409, detail="Impact has not been computed for this incident yet")

        exam_id = inc_row["exam_id"]

        # FAIRNESS GATE
        fairness = compute_exam_fairness(conn, exam_id, cfg)
        is_flagged = fairness["status"] == "flagged"
        if is_flagged and not req.acknowledge_fairness:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Fairness gate flagged cross-centre disparities. Acknowledgment required.",
                    "fairness": fairness,
                },
            )

        snapshot_str = json.dumps(fairness) if is_flagged else None
        ack_val = 1 if is_flagged else 0

        # Fetch all candidate impact rows
        cursor.execute(
            """
            SELECT candidate_id, session_id, remedy_recommended, extra_seconds, rule_id, impact_version
            FROM incident_impacts
            WHERE incident_id = ?;
            """,
            (incident_id,),
        )
        impact_rows = cursor.fetchall()

        # Fetch existing decisions to skip already decided
        cursor.execute(
            "SELECT candidate_id, decision_id FROM decisions WHERE incident_id = ?;",
            (incident_id,),
        )
        existing_decisions = {r["candidate_id"]: r["decision_id"] for r in cursor.fetchall()}

        approved_count = 0
        skipped_manual_review = 0
        skipped_already_decided = 0

        for row in impact_rows:
            cand_id = row["candidate_id"]
            rec_remedy = row["remedy_recommended"]
            extra_s = row["extra_seconds"]
            rule_id = row["rule_id"]
            imp_ver = row["impact_version"] or 1

            if rec_remedy == "manual_review":
                skipped_manual_review += 1
                continue

            if cand_id in existing_decisions:
                skipped_already_decided += 1
                continue

            cursor.execute(
                """
                INSERT INTO decisions (
                    incident_id, candidate_id, action, remedy, extra_seconds,
                    rule_id, recommended_remedy, impact_version, mode, decided_by,
                    reason, supersedes, acknowledged_fairness, fairness_snapshot, decided_at
                ) VALUES (?, ?, 'approve', ?, ?, ?, ?, ?, 'approved', ?, ?, NULL, ?, ?, ?);
                """,
                (
                    incident_id,
                    cand_id,
                    rec_remedy,
                    extra_s,
                    rule_id,
                    rec_remedy,
                    imp_ver,
                    req.decided_by,
                    req.reason,
                    ack_val,
                    snapshot_str,
                    now_iso,
                ),
            )
            decision_id = cursor.lastrowid
            approved_count += 1

            # Audit
            append_audit_entry(
                entry_type="decision",
                ref_id=f"{incident_id}:{cand_id}",
                payload={
                    "decision_id": decision_id,
                    "action": "approve",
                    "remedy": rec_remedy,
                    "extra_seconds": extra_s,
                    "recommended_remedy": rec_remedy,
                    "rule_id": rule_id,
                    "decided_by": req.decided_by,
                    "reason": req.reason,
                    "supersedes": None,
                    "acknowledged_fairness": ack_val,
                },
                ts_iso=now_iso,
                conn=conn,
            )

            # Notice
            msg = format_decision_message(rec_remedy, extra_s)
            emit_notice(
                conn=conn,
                audience="candidate",
                target_id=cand_id,
                incident_id=incident_id,
                kind="decision_final",
                ref_key=f"decision_final:{decision_id}",
                message=msg,
                now_iso=now_iso,
            )

        cursor.close()

        return {
            "approved": approved_count,
            "skipped_manual_review": skipped_manual_review,
            "skipped_already_decided": skipped_already_decided,
            "acknowledged_fairness": bool(ack_val),
        }


@router.get("/incidents/{incident_id}/decisions")
def list_incident_decisions(incident_id: str) -> List[Dict[str, Any]]:
    """List historical decisions for an incident, ordered chronologically (oldest first)."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT decision_id, incident_id, candidate_id, action, mode, remedy,
                   extra_seconds, rule_id, recommended_remedy, decided_by, reason,
                   supersedes, acknowledged_fairness, decided_at
            FROM decisions
            WHERE incident_id = ?
            ORDER BY decision_id ASC;
            """,
            (incident_id,),
        )
        rows = cursor.fetchall()
        cursor.close()
        return [dict(r) for r in rows]
