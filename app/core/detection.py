"""ERCT Detection Engine and Incident Manager.
Implements deterministic anomaly detection, explicit fault classification,
state machine transitions, and tamper-evident audit trail logging.
"""
from __future__ import annotations

import json
import logging
import statistics
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.config import get_config
from app.core.audit_chain import append_audit_entry
from app.db import format_utc_iso, get_db_connection, resolve_db_path, write_transaction

logger = logging.getLogger("erct.detection")


def parse_utc_iso(s: str) -> datetime:
    """Parse ISO-8601 string into a timezone-aware UTC datetime."""
    dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class DetectionEngine:
    """Deterministic, tick-based Detection Engine and Incident Manager.

    Can be ticked manually in unit tests with a fake clock, or ticked by a
    background daemon thread in the API lifespan.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.started_at: datetime = datetime.now(timezone.utc)
        self.ticks: int = 0
        self.tick_errors: int = 0
        self.last_tick_at: Optional[str] = None
        self.ingest_stalled: bool = False
        self.lock = threading.RLock()

        # Centre-level state tracking
        self.consecutive_close_ticks: Dict[str, int] = {}
        self.last_late_update_time: Dict[str, float] = {}  # incident_id -> monotonic time

    def reset_start_time(self, start_time: Optional[datetime] = None) -> None:
        """Reset the engine start time (used in testing startup grace)."""
        dt = start_time or datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        self.started_at = dt.astimezone(timezone.utc)

    def tick(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        """Execute one deterministic detection and incident management tick at timestamp `now`."""
        if now is None:
            now = datetime.now(timezone.utc)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        else:
            now = now.astimezone(timezone.utc)

        now_iso = format_utc_iso(now)
        cfg = get_config()
        exam_id = cfg.exam.id
        det_cfg = cfg.detection

        with self.lock:
            self.ticks += 1
            self.last_tick_at = now_iso

            # 1. Startup grace evaluation
            grace_duration_s = getattr(det_cfg, "startup_grace_s", 15.0)
            elapsed_since_start = (now - self.started_at).total_seconds()
            grace_remaining_s = max(0.0, grace_duration_s - elapsed_since_start)
            is_in_grace = grace_remaining_s > 0.0

            # 2. Ingest stall guard evaluation
            # Use max(centre_liveness.last_ingested_at)
            target_db = resolve_db_path(self.db_path)
            stall_threshold_s = getattr(det_cfg, "ingest_stall_s", 8.0)

            with get_db_connection(target_db) as conn:
                cur = conn.cursor()
                cur.execute("SELECT MAX(last_ingested_at) AS max_ingested FROM centre_liveness;")
                row = cur.fetchone()
                cur.close()

            if not row or not row["max_ingested"]:
                # No data has ever been ingested from any centre
                self.ingest_stalled = True
            else:
                max_ingested_dt = parse_utc_iso(row["max_ingested"])
                stall_age = (now - max_ingested_dt).total_seconds()
                self.ingest_stalled = stall_age > stall_threshold_s

            tick_summary = {
                "now": now_iso,
                "ticks": self.ticks,
                "is_in_grace": is_in_grace,
                "grace_remaining_s": round(grace_remaining_s, 2),
                "ingest_stalled": self.ingest_stalled,
                "incidents_opened": [],
                "incidents_updated": [],
            }

            try:
                self._evaluate_centres(
                    now=now,
                    now_iso=now_iso,
                    exam_id=exam_id,
                    det_cfg=det_cfg,
                    is_in_grace=is_in_grace,
                    target_db=target_db,
                    tick_summary=tick_summary,
                )
            except Exception as e:
                self.tick_errors += 1
                logger.exception("Error during detection tick at %s: %s", now_iso, e)
                raise

            return tick_summary

    def _evaluate_centres(
        self,
        now: datetime,
        now_iso: str,
        exam_id: str,
        det_cfg: Any,
        is_in_grace: bool,
        target_db: str,
        tick_summary: Dict[str, Any],
    ) -> None:
        cfg = get_config()
        heartbeat_gap_s = det_cfg.heartbeat_gap_s
        min_active_sessions = getattr(det_cfg, "min_active_sessions", 5)
        centre_loss_fraction = det_cfg.centre_loss_fraction
        escalate_after_s = getattr(det_cfg, "escalate_after_s", 120.0)
        settle_s = getattr(det_cfg, "settle_s", 5.0)
        close_fraction = getattr(det_cfg, "close_fraction", 0.2)
        close_ticks_req = getattr(det_cfg, "close_ticks", 2)
        recovery_resume_fraction = getattr(det_cfg, "recovery_resume_fraction", 0.9)
        recovery_max_wait_s = getattr(det_cfg, "recovery_max_wait_s", 30.0)

        for centre in cfg.centres:
            cid = centre.id

            with write_transaction(target_db) as conn:
                cursor = conn.cursor()

                # Query active incident for this centre (status IN ('open', 'recovering'))
                cursor.execute(
                    """
                    SELECT incident_id, exam_id, centre_id, type, severity, status,
                           detected_at, window_start, window_end, resolved_at,
                           detection_rule, evidence
                    FROM incidents
                    WHERE exam_id = ? AND centre_id = ? AND status IN ('open', 'recovering');
                    """,
                    (exam_id, cid),
                )
                active_inc = cursor.fetchone()

                # Query eligible sessions D for this centre
                # D = sessions with started_at NOT NULL and state IN ('active', 'resumed', 'interrupted')
                cursor.execute(
                    """
                    SELECT session_id, candidate_id, started_at, last_heartbeat_at, last_ingested_at, state
                    FROM sessions
                    WHERE centre_id = ? AND started_at IS NOT NULL AND state IN ('active', 'resumed', 'interrupted');
                    """,
                    (cid,),
                )
                sessions = cursor.fetchall()
                D = len(sessions)

                # Classify silent sessions based on server time: now - last_ingested_at > heartbeat_gap_s
                silent_sessions: List[Any] = []
                for s in sessions:
                    l_ing = s["last_ingested_at"]
                    if not l_ing:
                        silent_sessions.append(s)
                    else:
                        age_s = (now - parse_utc_iso(l_ing)).total_seconds()
                        if age_s > heartbeat_gap_s:
                            silent_sessions.append(s)

                silent_count = len(silent_sessions)
                silent_fraction = (silent_count / D) if D > 0 else 0.0

                # Check explicit events in the last 60s
                # RULE A: POWER_LOSS or NETWORK_DOWN event with ts inside last 60s
                explicit_window_start_iso = format_utc_iso(now - timedelta(seconds=60))
                cursor.execute(
                    """
                    SELECT event_id, type, ts, ingested_at, payload
                    FROM events
                    WHERE centre_id = ? AND type IN ('POWER_LOSS', 'NETWORK_DOWN')
                      AND ts >= ? AND ts <= ?
                    ORDER BY ts ASC;
                    """,
                    (cid, explicit_window_start_iso, format_utc_iso(now + timedelta(seconds=5))),
                )
                explicit_events = cursor.fetchall()

                # Query latest resolved window_end for this centre to ignore already-resolved outages
                cursor.execute(
                    "SELECT MAX(window_end) AS max_end FROM incidents WHERE centre_id = ? AND status = 'resolved';",
                    (cid,),
                )
                row_last_res = cursor.fetchone()
                last_resolved_end = row_last_res["max_end"] if row_last_res and row_last_res["max_end"] else None

                # Unhandled explicit events must have ts > last_resolved_end
                unhandled_explicit = []
                for e in explicit_events:
                    if last_resolved_end and e["ts"] <= last_resolved_end:
                        continue
                    unhandled_explicit.append(e)

                # Branch 1: NO ACTIVE INCIDENT -> Check if new incident should be opened
                if not active_inc:
                    # Reset close ticks counter for this centre
                    self.consecutive_close_ticks[cid] = 0

                    triggered = False
                    trigger_rule = ""
                    trigger_type = ""
                    trigger_confidence = ""
                    trigger_events: List[Any] = []
                    window_start: Optional[str] = None

                    # Check Rule A: explicit signals (NOT blocked by stall guard or startup grace)
                    pl_events = [e for e in unhandled_explicit if e["type"] == "POWER_LOSS"]
                    nd_events = [e for e in unhandled_explicit if e["type"] == "NETWORK_DOWN"]

                    if pl_events:
                        triggered = True
                        trigger_rule = "POWER_LOSS_EVENT"
                        trigger_type = "power"
                        trigger_confidence = "high"
                        trigger_events = pl_events
                        window_start = pl_events[0]["ts"]
                    elif nd_events:
                        triggered = True
                        trigger_rule = "NETWORK_DOWN_EVENT"
                        trigger_type = "network"
                        trigger_confidence = "high"
                        trigger_events = nd_events
                        window_start = nd_events[0]["ts"]
                    else:
                        # Check Rule B: silence inference
                        # Suppressed by startup grace or ingest stall guard
                        rule_b_allowed = (not is_in_grace) and (not self.ingest_stalled)
                        # Must meet: silent_fraction >= centre_loss_fraction AND D >= min_active_sessions
                        if rule_b_allowed and D >= min_active_sessions and silent_fraction >= centre_loss_fraction:
                            triggered = True
                            trigger_rule = "CENTRE_LOSS_FRACTION"
                            trigger_type = "network"
                            trigger_confidence = "low"
                            trigger_events = []

                            # Window start is MEDIAN last_heartbeat_at of silent sessions
                            hb_timestamps = [
                                parse_utc_iso(s["last_heartbeat_at"]).timestamp()
                                for s in silent_sessions
                                if s["last_heartbeat_at"]
                            ]
                            if hb_timestamps:
                                med_ts = statistics.median(hb_timestamps)
                                window_start = format_utc_iso(datetime.fromtimestamp(med_ts, timezone.utc))
                            else:
                                window_start = now_iso

                    if triggered and window_start:
                        # Generate incident ID: INC-<centre_id>-<n>
                        cursor.execute("SELECT COUNT(*) AS c FROM incidents WHERE centre_id = ?;", (cid,))
                        n = cursor.fetchone()["c"] + 1
                        inc_id = f"INC-{cid}-{n}"

                        # Format evidence payload
                        evidence = {
                            "rule": trigger_rule,
                            "silent_sessions": silent_count,
                            "total_sessions": D,
                            "silent_fraction": round(silent_fraction, 3),
                            "trigger_event_ids": [e["event_id"] for e in trigger_events],
                            "confidence": trigger_confidence,
                            "late_events": 0,
                            "recovered_sessions": 0,
                            "resumed_sessions": 0,
                            "reclassified_from": None,
                        }
                        evidence_str = json.dumps(evidence, sort_keys=True, separators=(",", ":"))

                        # 1. Insert incident row
                        cursor.execute(
                            """
                            INSERT INTO incidents (
                                incident_id, exam_id, centre_id, type, severity, status,
                                detected_at, window_start, window_end, resolved_at,
                                detection_rule, evidence
                            ) VALUES (?, ?, ?, ?, 'high', 'open', ?, ?, NULL, NULL, ?, ?);
                            """,
                            (inc_id, exam_id, cid, trigger_type, now_iso, window_start, trigger_rule, evidence_str),
                        )

                        # 2. Insert incident timeline entry
                        cursor.execute(
                            """
                            INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                            VALUES (?, ?, 'opened', ?);
                            """,
                            (inc_id, now_iso, json.dumps({"rule": trigger_rule, "confidence": trigger_confidence})),
                        )

                        # 3. Set centre's silent sessions to 'interrupted' in the same transaction
                        silent_sids = [s["session_id"] for s in silent_sessions]
                        if silent_sids:
                            placeholders = ",".join("?" for _ in silent_sids)
                            cursor.execute(
                                f"UPDATE sessions SET state = 'interrupted' WHERE session_id IN ({placeholders});",
                                silent_sids,
                            )

                        # 4. Append audit entry (entry_type = "incident")
                        append_audit_entry(
                            entry_type="incident",
                            ref_id=inc_id,
                            payload={
                                "action": "opened",
                                "incident_id": inc_id,
                                "evidence": evidence,
                            },
                            ts_iso=now_iso,
                            conn=conn,
                        )

                        # 5. Insert notices for admin and centre
                        cursor.execute(
                            """
                            INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                            VALUES ('admin', NULL, ?, ?, ?);
                            """,
                            (inc_id, f"Incident {inc_id} opened: {trigger_type.upper()} outage detected at centre {cid}.", now_iso),
                        )
                        cursor.execute(
                            """
                            INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                            VALUES ('centre', ?, ?, ?, ?);
                            """,
                            (cid, inc_id, f"Outage detected at centre {cid}. Incident {inc_id} is open.", now_iso),
                        )

                        tick_summary["incidents_opened"].append(inc_id)

                # Branch 2: ACTIVE INCIDENT EXISTS -> Progress / update active incident
                else:
                    inc_id = active_inc["incident_id"]
                    status = active_inc["status"]
                    inc_type = active_inc["type"]
                    severity = active_inc["severity"]
                    rule = active_inc["detection_rule"]
                    window_start = active_inc["window_start"]
                    window_end = active_inc["window_end"]
                    detected_at = active_inc["detected_at"]
                    evidence = json.loads(active_inc["evidence"]) if active_inc["evidence"] else {}

                    # Refresh telemetry numbers in evidence
                    evidence["silent_sessions"] = silent_count
                    evidence["total_sessions"] = D
                    evidence["silent_fraction"] = round(silent_fraction, 3)

                    # Update resumed sessions: sessions with last_heartbeat_at > window_end
                    if window_end:
                        cursor.execute(
                            """
                            UPDATE sessions
                            SET state = 'resumed'
                            WHERE centre_id = ? AND state = 'interrupted' AND last_heartbeat_at > ?;
                            """,
                            (cid, window_end),
                        )
                        # Count resumed sessions
                        cursor.execute(
                            "SELECT COUNT(*) AS c FROM sessions WHERE centre_id = ? AND last_heartbeat_at > ?;",
                            (cid, window_end),
                        )
                        evidence["resumed_sessions"] = cursor.fetchone()["c"]

                        # Calculate late events and recovered sessions
                        cursor.execute(
                            """
                            SELECT COUNT(*) AS late_count, COUNT(DISTINCT session_id) AS rec_sessions
                            FROM events
                            WHERE centre_id = ? AND ts >= ? AND ts <= ? AND ingested_at >= ?;
                            """,
                            (cid, window_start, window_end, window_end),
                        )
                        late_row = cursor.fetchone()
                        evidence["late_events"] = late_row["late_count"]
                        evidence["recovered_sessions"] = late_row["rec_sessions"]

                    # 1. Reclassification check (low-confidence silence upgraded to explicit event)
                    if evidence.get("confidence") == "low":
                        # Look for explicit event with ts in outage window
                        cursor.execute(
                            """
                            SELECT event_id, type, ts
                            FROM events
                            WHERE centre_id = ? AND type IN ('NETWORK_DOWN', 'POWER_LOSS')
                              AND ts >= ? AND ts <= ?
                            ORDER BY ts ASC LIMIT 1;
                            """,
                            (cid, format_utc_iso(parse_utc_iso(window_start) - timedelta(seconds=10)), window_end or now_iso),
                        )
                        exp_ev = cursor.fetchone()
                        if exp_ev:
                            old_from = {
                                "type": inc_type,
                                "confidence": evidence.get("confidence"),
                                "rule": rule,
                            }
                            new_type = "power" if exp_ev["type"] == "POWER_LOSS" else "network"
                            new_rule = "POWER_LOSS_EVENT" if exp_ev["type"] == "POWER_LOSS" else "NETWORK_DOWN_EVENT"

                            inc_type = new_type
                            rule = new_rule
                            evidence["confidence"] = "high"
                            evidence["rule"] = new_rule
                            evidence["reclassified_from"] = old_from
                            if exp_ev["event_id"] not in evidence.get("trigger_event_ids", []):
                                evidence.setdefault("trigger_event_ids", []).append(exp_ev["event_id"])

                            cursor.execute(
                                """
                                UPDATE incidents
                                SET type = ?, detection_rule = ?
                                WHERE incident_id = ?;
                                """,
                                (new_type, new_rule, inc_id),
                            )
                            cursor.execute(
                                """
                                INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                                VALUES (?, ?, 'reclassified', ?);
                                """,
                                (inc_id, now_iso, json.dumps({"from": old_from, "to": new_rule})),
                            )
                            append_audit_entry(
                                entry_type="incident",
                                ref_id=inc_id,
                                payload={
                                    "action": "reclassified",
                                    "incident_id": inc_id,
                                    "evidence": evidence,
                                },
                                ts_iso=now_iso,
                                conn=conn,
                            )

                    # 2. Escalation check: status 'open' and duration > escalate_after_s
                    if status == "open" and severity == "high":
                        det_dt = parse_utc_iso(detected_at)
                        if (now - det_dt).total_seconds() > escalate_after_s:
                            cursor.execute(
                                "UPDATE incidents SET severity = 'critical' WHERE incident_id = ?;",
                                (inc_id,),
                            )
                            cursor.execute(
                                """
                                INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                                VALUES (?, ?, 'escalated', ?);
                                """,
                                (inc_id, now_iso, json.dumps({"severity": "critical", "escalate_after_s": escalate_after_s})),
                            )
                            append_audit_entry(
                                entry_type="incident",
                                ref_id=inc_id,
                                payload={
                                    "action": "escalated",
                                    "incident_id": inc_id,
                                    "evidence": evidence,
                                },
                                ts_iso=now_iso,
                                conn=conn,
                            )
                            cursor.execute(
                                """
                                INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                VALUES ('admin', NULL, ?, ?, ?);
                                """,
                                (inc_id, f"Incident {inc_id} escalated to CRITICAL: duration exceeded {escalate_after_s}s.", now_iso),
                            )
                            cursor.execute(
                                """
                                INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                VALUES ('centre', ?, ?, ?, ?);
                                """,
                                (cid, inc_id, f"Incident {inc_id} at centre {cid} has been escalated to CRITICAL.", now_iso),
                            )

                    # 3. Recovery progression
                    # 3a. Power Incident Recovery
                    if inc_type == "power":
                        if status == "open":
                            cursor.execute(
                                """
                                SELECT event_id, ts
                                FROM events
                                WHERE centre_id = ? AND type = 'POWER_RESTORED' AND ts >= ?
                                ORDER BY ts ASC LIMIT 1;
                                """,
                                (cid, window_start),
                            )
                            pr_ev = cursor.fetchone()
                            if pr_ev:
                                status = "recovering"
                                window_end = pr_ev["ts"]
                                cursor.execute(
                                    """
                                    SELECT COUNT(*) AS late_count, COUNT(DISTINCT session_id) AS rec_sessions
                                    FROM events
                                    WHERE centre_id = ? AND ts >= ? AND ts <= ? AND ingested_at >= ?;
                                    """,
                                    (cid, window_start, window_end, window_end),
                                )
                                late_row = cursor.fetchone()
                                evidence["late_events"] = late_row["late_count"] if late_row else 0
                                evidence["recovered_sessions"] = late_row["rec_sessions"] if late_row else 0

                                cursor.execute(
                                    "UPDATE incidents SET status = 'recovering', window_end = ? WHERE incident_id = ?;",
                                    (window_end, inc_id),
                                )
                                cursor.execute(
                                    """
                                    INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                                    VALUES (?, ?, 'power_restored', ?);
                                    """,
                                    (inc_id, now_iso, json.dumps({"power_restored_ts": window_end})),
                                )
                                append_audit_entry(
                                    entry_type="incident",
                                    ref_id=inc_id,
                                    payload={
                                        "action": "recovering",
                                        "incident_id": inc_id,
                                        "evidence": evidence,
                                    },
                                    ts_iso=now_iso,
                                    conn=conn,
                                )

                        if status == "recovering":
                            # Count resumed sessions with last_heartbeat_at > window_end
                            cursor.execute(
                                "SELECT COUNT(*) AS c FROM sessions WHERE centre_id = ? AND last_heartbeat_at > ?;",
                                (cid, window_end),
                            )
                            resumed_c = cursor.fetchone()["c"]
                            resume_ratio = (resumed_c / D) if D > 0 else 1.0

                            we_dt = parse_utc_iso(window_end)
                            wait_elapsed_s = (now - we_dt).total_seconds()

                            if resume_ratio >= recovery_resume_fraction or wait_elapsed_s >= recovery_max_wait_s:
                                status = "resolved"
                                cursor.execute(
                                    "UPDATE incidents SET status = 'resolved', resolved_at = ? WHERE incident_id = ?;",
                                    (now_iso, inc_id),
                                )
                                cursor.execute(
                                    """
                                    INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                                    VALUES (?, ?, 'resolved', ?);
                                    """,
                                    (inc_id, now_iso, json.dumps({"resolved_at": now_iso, "resume_ratio": round(resume_ratio, 2)})),
                                )
                                append_audit_entry(
                                    entry_type="incident",
                                    ref_id=inc_id,
                                    payload={
                                        "action": "resolved",
                                        "incident_id": inc_id,
                                        "evidence": evidence,
                                    },
                                    ts_iso=now_iso,
                                    conn=conn,
                                )
                                cursor.execute(
                                    """
                                    INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                    VALUES ('admin', NULL, ?, ?, ?);
                                    """,
                                    (inc_id, f"Incident {inc_id} resolved: power restored and sessions resumed at centre {cid}.", now_iso),
                                )
                                cursor.execute(
                                    """
                                    INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                    VALUES ('centre', ?, ?, ?, ?);
                                    """,
                                    (cid, inc_id, f"Incident {inc_id} at centre {cid} has been RESOLVED.", now_iso),
                                )

                    # 3b. Network Incident Recovery
                    elif inc_type == "network":
                        # Check for explicit NETWORK_UP event
                        cursor.execute(
                            """
                            SELECT event_id, ts
                            FROM events
                            WHERE centre_id = ? AND type = 'NETWORK_UP' AND ts >= ?
                            ORDER BY ts ASC LIMIT 1;
                            """,
                            (cid, window_start),
                        )
                        nu_ev = cursor.fetchone()

                        if nu_ev:
                            if status == "open":
                                status = "recovering"
                                window_end = nu_ev["ts"]
                                cursor.execute(
                                    """
                                    SELECT COUNT(*) AS late_count, COUNT(DISTINCT session_id) AS rec_sessions
                                    FROM events
                                    WHERE centre_id = ? AND ts >= ? AND ts <= ? AND ingested_at >= ?;
                                    """,
                                    (cid, window_start, window_end, window_end),
                                )
                                late_row = cursor.fetchone()
                                evidence["late_events"] = late_row["late_count"] if late_row else 0
                                evidence["recovered_sessions"] = late_row["rec_sessions"] if late_row else 0

                                cursor.execute(
                                    "UPDATE incidents SET status = 'recovering', window_end = ? WHERE incident_id = ?;",
                                    (window_end, inc_id),
                                )
                                cursor.execute(
                                    """
                                    INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                                    VALUES (?, ?, 'network_restored', ?);
                                    """,
                                    (inc_id, now_iso, json.dumps({"network_up_ts": window_end})),
                                )
                                append_audit_entry(
                                    entry_type="incident",
                                    ref_id=inc_id,
                                    payload={
                                        "action": "recovering",
                                        "incident_id": inc_id,
                                        "evidence": evidence,
                                    },
                                    ts_iso=now_iso,
                                    conn=conn,
                                )

                            if status == "recovering":
                                cursor.execute(
                                    "SELECT COUNT(*) AS c FROM sessions WHERE centre_id = ? AND last_heartbeat_at > ?;",
                                    (cid, window_end),
                                )
                                resumed_c = cursor.fetchone()["c"]
                                resume_ratio = (resumed_c / D) if D > 0 else 1.0

                                we_dt = parse_utc_iso(window_end)
                                wait_elapsed_s = (now - we_dt).total_seconds()
                                resume_ok = (resume_ratio >= recovery_resume_fraction) or (wait_elapsed_s >= recovery_max_wait_s)

                                # Settle check: settle_s seconds with NO NEW EVENTS whose ts lies inside [window_start, window_end]
                                cursor.execute(
                                    """
                                    SELECT MAX(ingested_at) AS last_late_ingested
                                    FROM events
                                    WHERE centre_id = ? AND ts >= ? AND ts <= ?;
                                    """,
                                    (cid, window_start, window_end),
                                )
                                late_row = cursor.fetchone()
                                if late_row and late_row["last_late_ingested"]:
                                    late_ing_dt = parse_utc_iso(late_row["last_late_ingested"])
                                    settled = (now - late_ing_dt).total_seconds() >= settle_s
                                else:
                                    settled = True

                                if resume_ok and settled:
                                    status = "resolved"
                                    cursor.execute(
                                        "UPDATE incidents SET status = 'resolved', resolved_at = ? WHERE incident_id = ?;",
                                        (now_iso, inc_id),
                                    )
                                    cursor.execute(
                                        """
                                        INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                                        VALUES (?, ?, 'resolved', ?);
                                        """,
                                        (inc_id, now_iso, json.dumps({"resolved_at": now_iso, "settle_s": settle_s})),
                                    )
                                    append_audit_entry(
                                        entry_type="incident",
                                        ref_id=inc_id,
                                        payload={
                                            "action": "resolved",
                                            "incident_id": inc_id,
                                            "evidence": evidence,
                                        },
                                        ts_iso=now_iso,
                                        conn=conn,
                                    )
                                    cursor.execute(
                                        """
                                        INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                        VALUES ('admin', NULL, ?, ?, ?);
                                        """,
                                        (inc_id, f"Incident {inc_id} resolved: network restored and backlog drained at centre {cid}.", now_iso),
                                    )
                                    cursor.execute(
                                        """
                                        INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                        VALUES ('centre', ?, ?, ?, ?);
                                        """,
                                        (cid, inc_id, f"Incident {inc_id} at centre {cid} has been RESOLVED.", now_iso),
                                    )

                        else:
                            # Silence-only recovery: silent_fraction <= close_fraction for close_ticks ticks
                            if status == "open" and evidence.get("confidence") == "low":
                                if silent_fraction <= close_fraction:
                                    self.consecutive_close_ticks[cid] = self.consecutive_close_ticks.get(cid, 0) + 1
                                    if self.consecutive_close_ticks[cid] >= close_ticks_req:
                                        status = "resolved"
                                        window_end = now_iso
                                        evidence["window_end_estimated"] = True
                                        cursor.execute(
                                            """
                                            UPDATE incidents
                                            SET status = 'resolved', window_end = ?, resolved_at = ?
                                            WHERE incident_id = ?;
                                            """,
                                            (window_end, now_iso, inc_id),
                                        )
                                        cursor.execute(
                                            """
                                            INSERT INTO incident_timeline (incident_id, ts, kind, detail)
                                            VALUES (?, ?, 'resolved', ?);
                                            """,
                                            (inc_id, now_iso, json.dumps({"resolved_at": now_iso, "estimated": True})),
                                        )
                                        append_audit_entry(
                                            entry_type="incident",
                                            ref_id=inc_id,
                                            payload={
                                                "action": "resolved",
                                                "incident_id": inc_id,
                                                "evidence": evidence,
                                            },
                                            ts_iso=now_iso,
                                            conn=conn,
                                        )
                                        cursor.execute(
                                            """
                                            INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                            VALUES ('admin', NULL, ?, ?, ?);
                                            """,
                                            (inc_id, f"Incident {inc_id} resolved: traffic normalized at centre {cid}.", now_iso),
                                        )
                                        cursor.execute(
                                            """
                                            INSERT INTO notices (audience, target_id, incident_id, message, created_at)
                                            VALUES ('centre', ?, ?, ?, ?);
                                            """,
                                            (cid, inc_id, f"Incident {inc_id} at centre {cid} has been RESOLVED.", now_iso),
                                        )
                                else:
                                    self.consecutive_close_ticks[cid] = 0

                    # 4. Save evidence if changed
                    new_evidence_str = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
                    if new_evidence_str != active_inc["evidence"]:
                        cursor.execute(
                            "UPDATE incidents SET evidence = ? WHERE incident_id = ?;",
                            (new_evidence_str, inc_id),
                        )
                        # Stragglers after resolution update evidence quietly and add at most one action "late_update" per incident per 60 s
                        was_already_resolved = (active_inc["status"] == "resolved")
                        old_late_count = (json.loads(active_inc["evidence"]).get("late_events", 0) if active_inc["evidence"] else 0)
                        late_events_arrived = evidence.get("late_events", 0) > old_late_count

                        if was_already_resolved and late_events_arrived:
                            last_u = self.last_late_update_time.get(inc_id, 0.0)
                            now_mono = time.monotonic()
                            if (now_mono - last_u) >= 60.0:
                                append_audit_entry(
                                    entry_type="incident",
                                    ref_id=inc_id,
                                    payload={
                                        "action": "late_update",
                                        "incident_id": inc_id,
                                        "evidence": evidence,
                                    },
                                    ts_iso=now_iso,
                                    conn=conn,
                                )
                                self.last_late_update_time[inc_id] = now_mono

                    tick_summary["incidents_updated"].append(inc_id)

                cursor.close()


class DetectionWorker(threading.Thread):
    """Background daemon thread in API process that ticks the DetectionEngine every tick_s."""

    def __init__(self, engine: DetectionEngine, tick_s: float = 1.0):
        super().__init__(name="DetectionWorker", daemon=True)
        self.engine = engine
        self.tick_s = tick_s
        self.running = True

    def run(self) -> None:
        logger.info("DetectionWorker started (tick_s=%s)", self.tick_s)
        while self.running:
            try:
                self.engine.tick(datetime.now(timezone.utc))
            except Exception as e:
                logger.error("DetectionWorker unhandled tick error (worker will not die): %s", e)
            time.sleep(self.tick_s)
