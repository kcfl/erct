"""Control channel endpoints for orchestrating and monitoring simulated fault injections."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel, Field

from app.config import get_config
from app.core.audit_chain import append_audit_entry
from app.db import get_db_connection, write_transaction

router = APIRouter(prefix="/v1/control", tags=["Control"])


class CreateFaultCommandRequest(BaseModel):
    centre_id: str = Field(description="Centre target ID, e.g. C-BPL-02")
    fault_type: str = Field(description="Fault type: power_loss, network_drop")
    duration_s: int = Field(default=30, ge=1, description="Duration in real seconds")
    params: Dict[str, Any] = Field(default_factory=dict, description="Additional fault parameters")


class FaultCommandResponse(BaseModel):
    id: int
    centre_id: str
    fault_type: str
    duration_s: int
    params: Dict[str, Any]
    status: str
    created_at: str
    started_at: Optional[str] = None
    ended_at: Optional[str] = None


def verify_control_key(x_control_key: Optional[str]) -> None:
    """Validate X-Control-Key header against config.yaml."""
    cfg = get_config()
    expected_key = getattr(cfg.control, "key", "ctrl-secret-key-2026")
    if not x_control_key or x_control_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid X-Control-Key header",
        )


@router.post(
    "/faults",
    response_model=FaultCommandResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_fault_command(
    req: CreateFaultCommandRequest,
    x_control_key: Optional[str] = Header(None, alias="X-Control-Key"),
) -> FaultCommandResponse:
    """Create a pending fault injection command."""
    verify_control_key(x_control_key)
    cfg = get_config()

    # Validate centre exists
    valid_centres = {c.id for c in cfg.centres}
    if req.centre_id not in valid_centres:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown centre '{req.centre_id}'. Valid centres: {list(valid_centres)}",
        )

    # Validate fault type
    if req.fault_type not in ("power_loss", "network_drop", "wrong_version"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported fault type '{req.fault_type}'.",
        )

    now_iso = datetime.now(timezone.utc).isoformat()
    params_json = json.dumps(req.params, separators=(",", ":"))

    with write_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO fault_commands (centre_id, fault_type, duration_s, params, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?);
            """,
            (req.centre_id, req.fault_type, req.duration_s, params_json, now_iso),
        )
        cmd_id = cursor.lastrowid
        cursor.close()

        # Audit write with simulated: true
        append_audit_entry(
            entry_type="config",
            ref_id=f"FAULT-{cmd_id}",
            payload={
                "command_id": cmd_id,
                "centre_id": req.centre_id,
                "fault_type": req.fault_type,
                "duration_s": req.duration_s,
                "params": req.params,
                "simulated": True,
            },
            ts_iso=now_iso,
            conn=conn,
        )

    return FaultCommandResponse(
        id=cmd_id,
        centre_id=req.centre_id,
        fault_type=req.fault_type,
        duration_s=req.duration_s,
        params=req.params,
        status="pending",
        created_at=now_iso,
    )


@router.get(
    "/faults/pending",
    response_model=List[FaultCommandResponse],
    status_code=status.HTTP_200_OK,
)
def claim_pending_faults(
    x_control_key: Optional[str] = Header(None, alias="X-Control-Key"),
) -> List[FaultCommandResponse]:
    """Atomically claim all pending fault commands (status -> active)."""
    verify_control_key(x_control_key)
    now_iso = datetime.now(timezone.utc).isoformat()
    claimed: List[FaultCommandResponse] = []

    # Fast path: check for pending commands using read-only connection
    with get_db_connection() as r_conn:
        r_cur = r_conn.cursor()
        r_cur.execute("SELECT COUNT(*) AS c FROM fault_commands WHERE status = 'pending';")
        has_pending = (r_cur.fetchone()["c"] > 0)
        r_cur.close()

    if not has_pending:
        return []

    with write_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, centre_id, fault_type, duration_s, params, status, created_at
            FROM fault_commands
            WHERE status = 'pending'
            ORDER BY id ASC;
            """
        )
        rows = cursor.fetchall()

        if rows:
            ids = [r["id"] for r in rows]
            placeholders = ",".join("?" for _ in ids)
            cursor.execute(
                f"""
                UPDATE fault_commands
                SET status = 'active', started_at = ?
                WHERE id IN ({placeholders});
                """,
                [now_iso] + ids,
            )

            for r in rows:
                try:
                    p = json.loads(r["params"])
                except Exception:
                    p = {}
                claimed.append(
                    FaultCommandResponse(
                        id=r["id"],
                        centre_id=r["centre_id"],
                        fault_type=r["fault_type"],
                        duration_s=r["duration_s"],
                        params=p,
                        status="active",
                        created_at=r["created_at"],
                        started_at=now_iso,
                    )
                )

        cursor.close()

    return claimed


@router.post(
    "/faults/{fault_id}/done",
    response_model=FaultCommandResponse,
    status_code=status.HTTP_200_OK,
)
def complete_fault_command(
    fault_id: int,
    x_control_key: Optional[str] = Header(None, alias="X-Control-Key"),
) -> FaultCommandResponse:
    """Mark an active fault command as done."""
    verify_control_key(x_control_key)
    now_iso = datetime.now(timezone.utc).isoformat()

    with write_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM fault_commands WHERE id = ?;", (fault_id,))
        row = cursor.fetchone()
        if not row:
            cursor.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fault command not found")

        cursor.execute(
            "UPDATE fault_commands SET status = 'done', ended_at = ? WHERE id = ?;",
            (now_iso, fault_id),
        )
        cursor.close()

    try:
        p = json.loads(row["params"])
    except Exception:
        p = {}

    return FaultCommandResponse(
        id=row["id"],
        centre_id=row["centre_id"],
        fault_type=row["fault_type"],
        duration_s=row["duration_s"],
        params=p,
        status="done",
        created_at=row["created_at"],
        started_at=row["started_at"],
        ended_at=now_iso,
    )


@router.get(
    "/faults",
    response_model=List[FaultCommandResponse],
    status_code=status.HTTP_200_OK,
)
def list_fault_commands(
    x_control_key: Optional[str] = Header(None, alias="X-Control-Key"),
) -> List[FaultCommandResponse]:
    """List all registered fault commands."""
    verify_control_key(x_control_key)

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, centre_id, fault_type, duration_s, params, status, created_at, started_at, ended_at FROM fault_commands ORDER BY id ASC;"
        )
        rows = cursor.fetchall()
        cursor.close()

    res = []
    for r in rows:
        try:
            p = json.loads(r["params"])
        except Exception:
            p = {}
        res.append(
            FaultCommandResponse(
                id=r["id"],
                centre_id=r["centre_id"],
                fault_type=r["fault_type"],
                duration_s=r["duration_s"],
                params=p,
                status=r["status"],
                created_at=r["created_at"],
                started_at=r["started_at"],
                ended_at=r["ended_at"],
            )
        )
    return res
