"""Telemetry ingestion endpoint with idempotency, API key validation, and audit chain appending."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import ValidationError

from app.config import get_config
from app.core.audit_chain import append_audit_entry, canonical_json
from app.db import write_transaction
from app.models.events import BatchIngestResponse, EventEnvelope, EventType

router = APIRouter(prefix="/v1", tags=["Ingestion"])


def verify_centre_api_key(api_key: Optional[str], event_centre_ids: List[str]) -> None:
    """Validate X-API-Key header against configured centre credentials."""
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing required X-API-Key header",
        )

    cfg = get_config()
    # Map centre_id -> api_key and valid key pool
    key_to_centres: Dict[str, List[str]] = {}
    valid_keys = set()
    for c in cfg.centres:
        key_to_centres.setdefault(c.api_key, []).append(c.id)
        valid_keys.add(c.api_key)

    if api_key not in valid_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-API-Key credential",
        )

    # If the key is tied to specific centres, verify all events in batch belong to those centres
    allowed_centres = key_to_centres[api_key]
    for cid in event_centre_ids:
        if cid and cid not in allowed_centres:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"X-API-Key unauthorized for centre '{cid}'",
            )


@router.post(
    "/events",
    response_model=BatchIngestResponse,
    status_code=status.HTTP_200_OK,
)
async def ingest_events(
    request: Request,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> BatchIngestResponse:
    """Ingest one event or a batch of events with strict idempotency by event_id.

    - Rejects invalid / unknown event schemas with 422.
    - Validates centre authorization via X-API-Key header (401 if invalid).
    - Ignores duplicate event_ids without re-appending to the audit chain.
    - Updates candidate session telemetry (last heartbeat, answers saved, state).
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid JSON payload in request body",
        )

    raw_items: List[Dict[str, Any]] = []
    if isinstance(body, dict):
        if "events" in body and isinstance(body["events"], list):
            raw_items = body["events"]
        else:
            raw_items = [body]
    elif isinstance(body, list):
        raw_items = body
    else:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Body must be an event object or list of events",
        )

    if not raw_items:
        return BatchIngestResponse(accepted=0, duplicates=0, rejected=0)

    # 1. Parse and validate each event
    validated_events: List[EventEnvelope] = []
    rejected_count = 0
    errors: List[str] = []

    for idx, item in enumerate(raw_items):
        try:
            ev = EventEnvelope.model_validate(item)
            validated_events.append(ev)
        except ValidationError as val_err:
            rejected_count += 1
            errors.append(f"Item {idx}: {val_err.errors()[0]['msg']}")
        except Exception as e:
            rejected_count += 1
            errors.append(f"Item {idx}: {str(e)}")

    if rejected_count > 0 and len(validated_events) == 0:
        # If all events failed schema validation, return 422
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "All events failed schema validation", "errors": errors},
        )

    # 2. Check API Key
    event_centres = list({ev.centre_id for ev in validated_events})
    verify_centre_api_key(x_api_key, event_centres)

    # 3. Process accepted and duplicate events inside single DB transaction
    accepted_count = 0
    duplicate_count = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    with write_transaction() as conn:
        cursor = conn.cursor()

        for ev in validated_events:
            canon_payload = canonical_json(ev.payload)

            # Idempotent insert using primary key constraint on event_id
            cursor.execute(
                """
                INSERT OR IGNORE INTO events (
                    event_id, ts, ingested_at, exam_id, centre_id,
                    candidate_id, session_id, seq, type, severity, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    ev.event_id,
                    ev.ts,
                    now_iso,
                    ev.exam_id,
                    ev.centre_id,
                    ev.candidate_id,
                    ev.session_id,
                    ev.seq,
                    ev.type.value,
                    ev.severity.value,
                    canon_payload,
                ),
            )

            # In SQLite, cursor.rowcount == 1 means row was inserted, 0 means ignored duplicate
            if cursor.rowcount == 1:
                accepted_count += 1

                # Update session state if session_id is present
                if ev.session_id:
                    if ev.type == EventType.HEARTBEAT:
                        rem_s = ev.payload.get("remaining_s")
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET last_heartbeat_at = ?, remaining_s = ?, state = 'active'
                            WHERE session_id = ?;
                            """,
                            (ev.ts, rem_s, ev.session_id),
                        )
                    elif ev.type == EventType.ANSWER_SAVED:
                        saved_seq = ev.payload.get("saved_seq", 0)
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET last_saved_seq = MAX(last_saved_seq, ?),
                                answers_saved = answers_saved + 1,
                                last_heartbeat_at = ?,
                                state = 'active'
                            WHERE session_id = ?;
                            """,
                            (saved_seq, ev.ts, ev.session_id),
                        )
                    elif ev.type == EventType.SESSION_STARTED:
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET started_at = ?, state = 'active'
                            WHERE session_id = ?;
                            """,
                            (ev.ts, ev.session_id),
                        )
                    elif ev.type == EventType.SESSION_SUBMITTED:
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET state = 'submitted'
                            WHERE session_id = ?;
                            """,
                            (ev.session_id,),
                        )

                # Append to audit hash chain ONLY for brand-new events
                append_audit_entry(
                    entry_type="event",
                    ref_id=ev.event_id,
                    payload=ev.model_dump(),
                    ts_iso=now_iso,
                    conn=conn,
                )
            else:
                duplicate_count += 1

        cursor.close()

    return BatchIngestResponse(
        accepted=accepted_count,
        duplicates=duplicate_count,
        rejected=rejected_count,
        errors=errors,
    )
