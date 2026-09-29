"""Telemetry ingestion endpoint with idempotency, API key validation, and audit chain appending."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import ValidationError

from app.config import get_config
from app.core.audit_chain import GENESIS_PREV_HASH, append_audit_entry, canonical_json, compute_entry_hash
from app.db import format_utc_iso, write_transaction
from app.models.events import BatchIngestResponse, EventEnvelope, EventType

router = APIRouter(prefix="/v1", tags=["Ingestion"])
HTTP_422 = getattr(status, "HTTP_422_UNPROCESSABLE_CONTENT", 422)


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
            status_code=HTTP_422,
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
            status_code=HTTP_422,
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
            status_code=HTTP_422,
            detail={"message": "All events failed schema validation", "errors": errors},
        )

    # 2. Check API Key
    event_centres = list({ev.centre_id for ev in validated_events})
    verify_centre_api_key(x_api_key, event_centres)

    # 3. Process accepted and duplicate events inside single DB transaction
    accepted_count = 0
    duplicate_count = 0
    now_iso = format_utc_iso()
    centre_stats: Dict[str, Dict[str, Any]] = {}

    with write_transaction() as conn:
        cursor = conn.cursor()

        # Query latest audit entry once for the entire batch
        cursor.execute("SELECT seq, entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1;")
        latest_audit = cursor.fetchone()
        if latest_audit is None:
            current_seq = 0
            current_prev_hash = GENESIS_PREV_HASH
        else:
            current_seq = int(latest_audit["seq"])
            current_prev_hash = str(latest_audit["entry_hash"])

        audit_inserts = []

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

                # Track per-centre stats for centre_liveness table
                c_entry = centre_stats.setdefault(ev.centre_id, {"count": 0, "max_ts": ev.ts})
                c_entry["count"] += 1
                if ev.ts > c_entry["max_ts"]:
                    c_entry["max_ts"] = ev.ts

                # Update session state monotonically if session_id is present
                if ev.session_id:
                    if ev.type == EventType.HEARTBEAT:
                        rem_s = ev.payload.get("remaining_s")
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET last_ingested_at = ?,
                                last_heartbeat_at = CASE WHEN last_heartbeat_at IS NULL OR ? >= last_heartbeat_at THEN ? ELSE last_heartbeat_at END,
                                remaining_s = CASE WHEN last_heartbeat_at IS NULL OR ? >= last_heartbeat_at THEN ? ELSE remaining_s END
                            WHERE session_id = ?;
                            """,
                            (now_iso, ev.ts, ev.ts, ev.ts, rem_s, ev.session_id),
                        )
                    elif ev.type == EventType.ANSWER_SAVED:
                        saved_seq = ev.payload.get("saved_seq", 0)
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET last_ingested_at = ?,
                                answers_saved = answers_saved + 1,
                                last_saved_seq = MAX(last_saved_seq, ?)
                            WHERE session_id = ?;
                            """,
                            (now_iso, saved_seq, ev.session_id),
                        )
                    elif ev.type == EventType.SESSION_STARTED:
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET last_ingested_at = ?,
                                started_at = CASE WHEN started_at IS NULL OR ? < started_at THEN ? ELSE started_at END,
                                last_heartbeat_at = CASE WHEN last_heartbeat_at IS NULL OR ? >= last_heartbeat_at THEN ? ELSE last_heartbeat_at END,
                                state = CASE WHEN state IN ('interrupted', 'under_review') THEN state ELSE 'active' END
                            WHERE session_id = ?;
                            """,
                            (now_iso, ev.ts, ev.ts, ev.ts, ev.ts, ev.session_id),
                        )
                    elif ev.type == EventType.SESSION_SUBMITTED:
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET last_ingested_at = ?,
                                state = CASE WHEN state IN ('interrupted', 'under_review') THEN state ELSE 'submitted' END
                            WHERE session_id = ?;
                            """,
                            (now_iso, ev.session_id),
                        )
                    else:
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET last_ingested_at = ?
                            WHERE session_id = ?;
                            """,
                            (now_iso, ev.session_id),
                        )

                # Prepare audit log row in sequence
                current_seq += 1
                entry_hash = compute_entry_hash(
                    prev_hash=current_prev_hash,
                    seq=current_seq,
                    ts_iso=now_iso,
                    entry_type="event",
                    ref_id=ev.event_id,
                    canonical_payload=canon_payload,
                )
                audit_inserts.append(
                    (
                        current_seq,
                        now_iso,
                        "event",
                        ev.event_id,
                        canon_payload,
                        current_prev_hash,
                        entry_hash,
                    )
                )
                current_prev_hash = entry_hash
            else:
                duplicate_count += 1

        # Bulk insert all new audit entries for this batch
        if audit_inserts:
            cursor.executemany(
                """
                INSERT INTO audit_log (seq, ts, entry_type, ref_id, payload, prev_hash, entry_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                audit_inserts,
            )

        # Update centre_liveness per accepted batch in the same transaction
        for cid, stats_data in centre_stats.items():
            cursor.execute(
                """
                INSERT INTO centre_liveness (centre_id, last_ingested_at, last_event_ts, events_total)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(centre_id) DO UPDATE SET
                    last_ingested_at = excluded.last_ingested_at,
                    last_event_ts = CASE WHEN excluded.last_event_ts > centre_liveness.last_event_ts THEN excluded.last_event_ts ELSE centre_liveness.last_event_ts END,
                    events_total = centre_liveness.events_total + excluded.events_total;
                """,
                (cid, now_iso, stats_data["max_ts"], stats_data["count"]),
            )

        cursor.close()

    return BatchIngestResponse(
        accepted=accepted_count,
        duplicates=duplicate_count,
        rejected=rejected_count,
        errors=errors,
    )
