"""Impact and Remedy Engine for ERCT (Phase 3b-i).

Evaluates candidate session telemetry to measure true downtime, unsaved answers,
and evidence quality, then computes explainable remedies according to typed rules.
Strictly pure functions with no eval() or exec().
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from app.config import AppConfig, get_config
from app.core.audit_chain import append_audit_entry
from app.core.notices import emit_notice
from app.db import write_transaction


def parse_iso(ts_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO UTC timestamp into datetime object."""
    if not ts_str:
        return None
    cleaned = ts_str.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


def format_iso(dt: datetime) -> str:
    """Format datetime into ISO UTC string."""
    return dt.astimezone(timezone.utc).isoformat()


@dataclass
class SessionFacts:
    session_id: str
    candidate_id: str
    centre_id: str
    last_good_ts: Optional[str]
    last_good_dt: Optional[datetime]
    L: Optional[int]
    last_hb_event_id: Optional[str]
    resume_ts: Optional[str]
    resume_dt: Optional[datetime]
    resume_hb_event_id: Optional[str]
    gap: float
    lost_s: float
    S: int
    last_save_event_id: Optional[str]
    lost_answers: int
    evidence_quality: str  # 'missing' | 'partial' | 'strong'
    quality_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RemedyDecision:
    rule_id: str  # 'R1' | 'R2' | 'R3' | 'R4'
    remedy: str   # 'resume' | 'extra_time' | 'retest_recommended' | 'manual_review'
    extra_seconds: int
    rationale: str
    fallback: Optional[Dict[str, Any]] = None


def compute_session_facts(
    events: List[Dict[str, Any]],
    session: Dict[str, Any],
    window_start: str,
    window_end: str,
    cfg: AppConfig,
) -> SessionFacts:
    """Pure function: calculate candidate session facts from ingested events."""
    ws_dt = parse_iso(window_start)
    we_dt = parse_iso(window_end)
    interval = cfg.impact.heartbeat_interval_s
    loss_gap_factor = cfg.impact.loss_gap_factor
    stale_factor = cfg.impact.stale_hb_factor
    baseline_slots_count = cfg.impact.baseline_slots
    baseline_missing_max = cfg.impact.baseline_missing_max

    # 1. Parse and partition events
    heartbeats: List[Dict[str, Any]] = []
    answer_saves: List[Dict[str, Any]] = []

    for ev in events:
        ev_type = ev.get("type") or ev.get("event_type")
        ts_dt = parse_iso(ev["ts"])
        if not ts_dt:
            continue
        payload = ev.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        elif not isinstance(payload, dict):
            payload = {}

        ev_copy = dict(ev)
        ev_copy["ts_dt"] = ts_dt
        ev_copy["payload"] = payload

        if ev_type == "HEARTBEAT":
            heartbeats.append(ev_copy)
        elif ev_type == "ANSWER_SAVED":
            answer_saves.append(ev_copy)

    # Sort heartbeats by timestamp
    heartbeats.sort(key=lambda x: x["ts_dt"])
    answer_saves.sort(key=lambda x: x["ts_dt"])

    # 2. last_good_ts and L: last HEARTBEAT with ts <= window_start
    pre_window_hbs = [hb for hb in heartbeats if ws_dt and hb["ts_dt"] <= ws_dt]
    last_good_hb = pre_window_hbs[-1] if pre_window_hbs else None

    last_good_ts = last_good_hb["ts"] if last_good_hb else None
    last_good_dt = last_good_hb["ts_dt"] if last_good_hb else None
    last_hb_event_id = last_good_hb["event_id"] if last_good_hb else None
    L = last_good_hb["payload"].get("local_seq") if last_good_hb else None

    # 3. resume_ts: first HEARTBEAT with ts > window_end
    post_window_hbs = [hb for hb in heartbeats if we_dt and hb["ts_dt"] > we_dt]
    resume_hb = post_window_hbs[0] if post_window_hbs else None

    resume_ts = resume_hb["ts"] if resume_hb else None
    resume_dt = resume_hb["ts_dt"] if resume_hb else None
    resume_hb_event_id = resume_hb["event_id"] if resume_hb else None

    # 4. gap: largest gap between consecutive heartbeats in [last_good_ts, resume_ts]
    gap = 0.0
    if last_good_dt and resume_dt:
        window_hbs = [hb for hb in heartbeats if last_good_dt <= hb["ts_dt"] <= resume_dt]
        if len(window_hbs) >= 2:
            gaps = [
                (window_hbs[i + 1]["ts_dt"] - window_hbs[i]["ts_dt"]).total_seconds()
                for i in range(len(window_hbs) - 1)
            ]
            gap = max(gaps) if gaps else 0.0
        elif len(window_hbs) == 1:
            gap = 0.0

    # 5. lost_s
    threshold_gap = loss_gap_factor * interval
    lost_s = gap if gap > threshold_gap else 0.0

    # 6. S: max saved_seq of ANSWER_SAVED events with ts <= resume_ts
    valid_saves: List[Dict[str, Any]] = []
    if resume_dt:
        valid_saves = [s for s in answer_saves if s["ts_dt"] <= resume_dt]
    elif ws_dt:
        valid_saves = [s for s in answer_saves if s["ts_dt"] <= ws_dt]

    S = 0
    last_save_event_id = None
    if valid_saves:
        best_save = max(valid_saves, key=lambda s: s["payload"].get("saved_seq", 0))
        S = best_save["payload"].get("saved_seq", 0)
        last_save_event_id = best_save["event_id"]

    # 7. lost_answers
    lost_answers = max(0, (L if L is not None else 0) - S)

    # 8. Evidence quality
    # missing: no heartbeat <= window_start, or no resume_ts
    quality_details: Dict[str, Any] = {
        "last_good_ts": last_good_ts,
        "resume_ts": resume_ts,
        "gap_s": round(gap, 2),
        "lost_s": round(lost_s, 2),
        "L": L,
        "S": S,
        "lost_answers": lost_answers,
        "last_heartbeat_event_id": last_hb_event_id,
        "resume_heartbeat_event_id": resume_hb_event_id,
        "last_save_event_id": last_save_event_id,
    }

    if not last_good_hb or not resume_hb:
        evidence_quality = "missing"
        quality_details["reason"] = "Missing pre-window heartbeat or post-window resume heartbeat"
    else:
        # Check partial criteria
        is_partial = False
        reasons = []

        # Stale check: if last heartbeat older than stale_factor * interval before window_start,
        # telemetry at fault inception is unobserved and potentially desynchronized
        stale_s = (ws_dt - last_good_dt).total_seconds() if ws_dt and last_good_dt else 0.0
        quality_details["stale_seconds"] = round(stale_s, 2)
        if stale_s > stale_factor * interval:
            is_partial = True
            reasons.append(f"last_good_ts stale ({round(stale_s, 1)}s > {round(stale_factor * interval, 1)}s)")

        # Local seq check
        if L is None:
            is_partial = True
            reasons.append("last heartbeat missing local_seq")

        # Baseline regularity check
        if ws_dt:
            session_started_at = session.get("started_at")
            started_dt = parse_iso(session_started_at) if session_started_at else None

            # Align slots against started_at forward up to window_start
            available_slots = []
            if started_dt:
                total_time_s = (ws_dt - started_dt).total_seconds()
                total_slots = int(total_time_s // interval)
                start_slot_idx = max(0, total_slots - baseline_slots_count)
                for slot_idx in range(start_slot_idx, total_slots):
                    slot_t0 = started_dt + timedelta(seconds=slot_idx * interval)
                    slot_t1 = started_dt + timedelta(seconds=(slot_idx + 1) * interval)
                    available_slots.append((slot_t0, slot_t1))
            else:
                for slot_idx in range(baseline_slots_count):
                    slot_t0 = ws_dt - timedelta(seconds=(baseline_slots_count - slot_idx) * interval)
                    slot_t1 = ws_dt - timedelta(seconds=(baseline_slots_count - slot_idx - 1) * interval)
                    available_slots.append((slot_t0, slot_t1))

            baseline_min_slots = getattr(cfg.impact, "baseline_min_slots", 3)
            if len(available_slots) < baseline_min_slots:
                is_partial = True
                reasons.append(f"too little history ({len(available_slots)} available slots < {baseline_min_slots})")
                quality_details["baseline_slots"] = len(available_slots)
                quality_details["missing_slots"] = 0
                quality_details["missing_fraction"] = 1.0
            else:
                missing_slots = 0
                first_hb_dt = pre_window_hbs[0]["ts_dt"] if pre_window_hbs else None

                for slot_t0, slot_t1 in available_slots:
                    # Slot before first heartbeat does not count as missing
                    if first_hb_dt and slot_t1 <= first_hb_dt:
                        continue

                    slot_has_hb = any(
                        slot_t0 < hb["ts_dt"] <= slot_t1
                        for hb in pre_window_hbs
                    )
                    if not slot_has_hb:
                        missing_slots += 1

                allowed_missing = int(math.floor(baseline_missing_max * len(available_slots)))
                missing_frac = missing_slots / len(available_slots)
                quality_details["baseline_slots"] = len(available_slots)
                quality_details["missing_slots"] = missing_slots
                quality_details["missing_fraction"] = round(missing_frac, 2)

                if missing_slots > allowed_missing:
                    is_partial = True
                    reasons.append(
                        f"baseline missing slots {missing_slots}/{len(available_slots)} ({round(missing_frac * 100, 1)}%) > {allowed_missing} allowed"
                    )

        if is_partial:
            evidence_quality = "partial"
            quality_details["reason"] = "; ".join(reasons)
        else:
            evidence_quality = "strong"

    return SessionFacts(
        session_id=session["session_id"],
        candidate_id=session["candidate_id"],
        centre_id=session["centre_id"],
        last_good_ts=last_good_ts,
        last_good_dt=last_good_dt,
        L=L,
        last_hb_event_id=last_hb_event_id,
        resume_ts=resume_ts,
        resume_dt=resume_dt,
        resume_hb_event_id=resume_hb_event_id,
        gap=gap,
        lost_s=lost_s,
        S=S,
        last_save_event_id=last_save_event_id,
        lost_answers=lost_answers,
        evidence_quality=evidence_quality,
        quality_details=quality_details,
    )


def evaluate_remedy(
    facts: SessionFacts,
    incident_facts: Dict[str, Any],
    cfg: AppConfig,
) -> RemedyDecision:
    """Pure function: evaluate remedy recommendations strictly in order R3 -> R4 -> R1 -> R2.

    No eval(), exec(), or dynamic expressions are used.
    """
    remedy_cfg = cfg.remedy

    # R3: Weak Evidence Guard
    if facts.evidence_quality != "strong":
        return RemedyDecision(
            rule_id="R3",
            remedy="manual_review",
            extra_seconds=0,
            rationale=(
                f"R3: Evidence quality is '{facts.evidence_quality}' "
                f"({facts.quality_details.get('reason', 'incomplete telemetry')}). "
                "Manual review required."
            ),
        )

    # R4: Centre-Level Anomaly Advisory
    centre_affected_fraction = float(incident_facts.get("centre_affected_fraction", 0.0))
    integrity_flag = bool(incident_facts.get("integrity_flag", False))

    if centre_affected_fraction >= remedy_cfg.retest_min_affected_fraction and integrity_flag:
        # Fallback evaluation for R1 vs R2
        if facts.lost_s <= remedy_cfg.resume_max_lost_s and facts.lost_answers <= remedy_cfg.resume_max_lost_answers:
            fb_rule = "R1"
            fb_rem = "resume"
            fb_extra = math.ceil(facts.lost_s) if facts.lost_s > 0 else 0
        else:
            fb_rule = "R2"
            fb_rem = "extra_time"
            fb_extra = math.ceil(facts.lost_s) + int(remedy_cfg.extra_time_buffer_s)

        fallback_dict = {
            "rule_id": fb_rule,
            "remedy": fb_rem,
            "extra_seconds": fb_extra,
        }
        return RemedyDecision(
            rule_id="R4",
            remedy="retest_recommended",
            extra_seconds=0,
            rationale=(
                f"R4: Centre affected fraction {centre_affected_fraction:.1%} >= "
                f"{remedy_cfg.retest_min_affected_fraction:.1%} with integrity flag active. "
                "Retest recommended for centre."
            ),
            fallback=fallback_dict,
        )

    # R1: Minor Interruption (Session Resumed)
    if facts.lost_s <= remedy_cfg.resume_max_lost_s and facts.lost_answers <= remedy_cfg.resume_max_lost_answers:
        extra_sec = math.ceil(facts.lost_s) if facts.lost_s > 0 else 0
        if facts.lost_s == 0 and facts.lost_answers == 0:
            rationale = "R1: No lost time or unsaved answers detected. Session resumed with 0 s compensation."
        else:
            rationale = (
                f"R1: {facts.lost_answers} unsaved answers (limit {remedy_cfg.resume_max_lost_answers}) "
                f"and lost {facts.lost_s:.1f} s (limit {remedy_cfg.resume_max_lost_s} s). "
                f"Session resumed; compensation = {extra_sec} s."
            )
        return RemedyDecision(
            rule_id="R1",
            remedy="resume",
            extra_seconds=extra_sec,
            rationale=rationale,
        )

    # R2: Moderate/Major Interruption or Lost Answers (Extra Time Compensation)
    extra_sec = math.ceil(facts.lost_s) + remedy_cfg.extra_time_buffer_s
    reasons: List[str] = []
    if facts.lost_answers > remedy_cfg.resume_max_lost_answers:
        reasons.append(f"{facts.lost_answers} answers were unsaved (limit {remedy_cfg.resume_max_lost_answers})")
    if facts.lost_s > remedy_cfg.resume_max_lost_s:
        reasons.append(f"lost time {int(round(facts.lost_s))} s exceeded {remedy_cfg.resume_max_lost_s} s")

    reason_str = "; ".join(reasons) if reasons else "Thresholds exceeded"
    lost_round = int(round(facts.lost_s))
    rationale = (
        f"R2: {reason_str}. You lost {lost_round} s; "
        f"compensation = {lost_round} s + {remedy_cfg.extra_time_buffer_s} s buffer = {extra_sec} s."
    )
    return RemedyDecision(
        rule_id="R2",
        remedy="extra_time",
        extra_seconds=extra_sec,
        rationale=rationale,
    )


def compute_incident_impact(
    conn: sqlite3.Connection,
    incident_id: str,
    computed_at_iso: str,
    cfg: Optional[AppConfig] = None,
) -> Dict[str, Any]:
    """Calculate impact for all exposed sessions of a resolved incident.

    Executes side-effects in the same write transaction:
    - Upserts incident_impacts
    - Appends audit log entries for new or changed candidate recommendations
    - Manages review_queue and session states for manual_review
    - Updates incident.evidence with impact summary and sets impact_computed_at
    """
    if cfg is None:
        cfg = get_config()

    cursor = conn.cursor()

    # 1. Fetch incident
    cursor.execute(
        "SELECT incident_id, centre_id, status, window_start, window_end, evidence, impact_computed_at FROM incidents WHERE incident_id = ?;",
        (incident_id,),
    )
    inc_row = cursor.fetchone()
    if not inc_row:
        cursor.close()
        return {}

    if inc_row["status"] != "resolved":
        cursor.close()
        return {}

    cid = inc_row["centre_id"]
    ws = inc_row["window_start"]
    we = inc_row["window_end"]
    ws_dt = parse_iso(ws)

    evidence_dict: Dict[str, Any] = {}
    if inc_row["evidence"]:
        try:
            evidence_dict = json.loads(inc_row["evidence"])
        except Exception:
            evidence_dict = {}

    integrity_flag = bool(evidence_dict.get("integrity_flag", False))
    was_previously_computed = bool(inc_row["impact_computed_at"])

    # 2. Derive Exposed Set:
    # Candidates of the incident's centre with started_at <= window_start
    # and not submitted before window_start.
    # Exclude all other centres.
    cursor.execute(
        """
        SELECT session_id, candidate_id, centre_id, started_at, state
        FROM sessions
        WHERE centre_id = ?;
        """,
        (cid,),
    )
    candidate_sessions = cursor.fetchall()

    exposed_sessions: List[sqlite3.Row] = []
    for s in candidate_sessions:
        s_start_dt = parse_iso(s["started_at"])
        # Check if session started before or at window_start
        if not s_start_dt or (ws_dt and s_start_dt > ws_dt):
            # Also check if there was a SESSION_STARTED event before ws
            cursor.execute(
                """
                SELECT ts FROM events
                WHERE session_id = ? AND type = 'SESSION_STARTED' AND ts <= ?
                LIMIT 1;
                """,
                (s["session_id"], ws),
            )
            if not cursor.fetchone():
                continue

        # Check if candidate submitted before window_start
        cursor.execute(
            """
            SELECT ts FROM events
            WHERE session_id = ? AND type = 'SESSION_SUBMITTED' AND ts <= ?
            LIMIT 1;
            """,
            (s["session_id"], ws),
        )
        if cursor.fetchone():
            continue  # Submitted before window_start, not exposed

        exposed_sessions.append(s)

    if not exposed_sessions:
        cursor.close()
        return {}

    # 3. Fetch all events for exposed sessions
    session_facts_list: List[SessionFacts] = []
    for s in exposed_sessions:
        cursor.execute(
            """
            SELECT event_id, type, ts, payload
            FROM events
            WHERE session_id = ?
            ORDER BY ts ASC, seq ASC;
            """,
            (s["session_id"],),
        )
        ev_rows = cursor.fetchall()
        events_data = [dict(r) for r in ev_rows]
        facts = compute_session_facts(events_data, dict(s), ws, we, cfg)
        session_facts_list.append(facts)

    # 4. Compute centre_affected_fraction
    affected_count = sum(1 for f in session_facts_list if f.lost_s > 0 or f.lost_answers > 0)
    centre_affected_fraction = (affected_count / len(session_facts_list)) if session_facts_list else 0.0

    incident_facts = {
        "centre_affected_fraction": centre_affected_fraction,
        "integrity_flag": integrity_flag,
    }

    # 5. Evaluate remedies
    decisions: List[Tuple[SessionFacts, RemedyDecision]] = []
    for facts in session_facts_list:
        dec = evaluate_remedy(facts, incident_facts, cfg)
        decisions.append((facts, dec))

    # 6. Read existing incident_impacts to detect changes and prevent audit spam
    cursor.execute(
        "SELECT candidate_id, remedy_recommended, extra_seconds, rule_id, evidence_quality, impact_version FROM incident_impacts WHERE incident_id = ?;",
        (incident_id,),
    )
    existing_rows = {row["candidate_id"]: dict(row) for row in cursor.fetchall()}

    by_remedy: Dict[str, int] = {}
    by_rule: Dict[str, int] = {}
    by_quality: Dict[str, int] = {}
    extra_seconds_list: List[int] = []

    new_or_changed_count = 0

    for facts, dec in decisions:
        cand_id = facts.candidate_id
        sess_id = facts.session_id

        by_remedy[dec.remedy] = by_remedy.get(dec.remedy, 0) + 1
        by_rule[dec.rule_id] = by_rule.get(dec.rule_id, 0) + 1
        by_quality[facts.evidence_quality] = by_quality.get(facts.evidence_quality, 0) + 1
        extra_seconds_list.append(dec.extra_seconds)

        existing = existing_rows.get(cand_id)
        is_new = existing is None
        has_remedy_changed = existing is not None and existing["remedy_recommended"] != dec.remedy

        if is_new:
            impact_version = 1
        else:
            prev_ver = existing.get("impact_version") or 1
            if has_remedy_changed or existing.get("extra_seconds") != dec.extra_seconds or existing.get("rule_id") != dec.rule_id:
                impact_version = prev_ver + 1
            else:
                impact_version = prev_ver

        evidence_dict = dict(facts.quality_details)
        if dec.fallback:
            evidence_dict["fallback"] = dec.fallback
        evidence_payload_json = json.dumps(evidence_dict)

        # Upsert impact row
        cursor.execute(
            """
            INSERT INTO incident_impacts (
                incident_id, candidate_id, session_id, lost_seconds, unsaved_answers,
                last_good_seq, evidence_quality, remedy_recommended, extra_seconds,
                rationale, rule_id, evidence, computed_at, impact_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (incident_id, candidate_id) DO UPDATE SET
                session_id = excluded.session_id,
                lost_seconds = excluded.lost_seconds,
                unsaved_answers = excluded.unsaved_answers,
                last_good_seq = excluded.last_good_seq,
                evidence_quality = excluded.evidence_quality,
                remedy_recommended = excluded.remedy_recommended,
                extra_seconds = excluded.extra_seconds,
                rationale = excluded.rationale,
                rule_id = excluded.rule_id,
                evidence = excluded.evidence,
                computed_at = excluded.computed_at,
                impact_version = excluded.impact_version;
            """,
            (
                incident_id,
                cand_id,
                sess_id,
                facts.lost_s,
                facts.lost_answers,
                facts.S,
                facts.evidence_quality,
                dec.remedy,
                dec.extra_seconds,
                dec.rationale,
                dec.rule_id,
                evidence_payload_json,
                computed_at_iso,
                impact_version,
            ),
        )

        # Audit entry: one per candidate row when first written or when recommendation changes
        if is_new or has_remedy_changed:
            new_or_changed_count += 1
            append_audit_entry(
                entry_type="impact",
                ref_id=f"{incident_id}:{cand_id}",
                payload={
                    "incident_id": incident_id,
                    "candidate_id": cand_id,
                    "session_id": sess_id,
                    "remedy": dec.remedy,
                    "rule_id": dec.rule_id,
                    "lost_seconds": round(facts.lost_s, 2),
                    "extra_seconds": dec.extra_seconds,
                    "evidence_quality": facts.evidence_quality,
                },
                ts_iso=computed_at_iso,
                conn=conn,
            )

        # Candidate notices: impact_pending or review_pending keyed by impact_version
        cand_kind = "review_pending" if dec.remedy == "manual_review" else "impact_pending"
        if dec.remedy == "manual_review":
            cand_msg = f"Your session requires manual review by the exam team. Your last confirmed save is #{facts.S}. An update will be provided once reviewed."
        else:
            cand_msg = f"The effect of the interruption on your session has been assessed. Your remedy is awaiting final confirmation by the exam controller. Your last confirmed save is #{facts.S}."

        emit_notice(
            conn=conn,
            audience="candidate",
            target_id=cand_id,
            incident_id=incident_id,
            kind=cand_kind,
            ref_key=f"{cand_kind}:{incident_id}:{cand_id}:{impact_version}",
            message=cand_msg,
            now_iso=computed_at_iso,
        )

        # Side effects for manual_review
        if dec.remedy == "manual_review":
            # Session state -> 'under_review' (do not touch submitted)
            cursor.execute(
                "UPDATE sessions SET state = 'under_review' WHERE session_id = ? AND state != 'submitted';",
                (sess_id,),
            )
            # Upsert into review_queue
            cursor.execute(
                """
                INSERT INTO review_queue (incident_id, candidate_id, session_id, reason, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?)
                ON CONFLICT(incident_id, candidate_id) DO UPDATE SET
                    status = 'pending',
                    reason = excluded.reason,
                    updated_at = excluded.updated_at;
                """,
                (incident_id, cand_id, sess_id, dec.rationale, computed_at_iso, computed_at_iso),
            )
        else:
            # If session was previously in review_queue as pending, supersede it and return state to resumed
            cursor.execute(
                """
                UPDATE review_queue
                SET status = 'superseded', updated_at = ?
                WHERE incident_id = ? AND candidate_id = ? AND status = 'pending';
                """,
                (computed_at_iso, incident_id, cand_id),
            )
            if cursor.rowcount > 0:
                cursor.execute(
                    "UPDATE sessions SET state = 'resumed' WHERE session_id = ? AND state = 'under_review';",
                    (sess_id,),
                )

    # 7. Summary metrics
    centre_retest = (
        centre_affected_fraction >= cfg.remedy.retest_min_affected_fraction and integrity_flag
    )
    summary_dict = {
        "exposed": len(exposed_sessions),
        "by_remedy": by_remedy,
        "by_rule": by_rule,
        "by_quality": by_quality,
        "avg_extra_seconds": round(sum(extra_seconds_list) / len(extra_seconds_list), 1) if extra_seconds_list else 0.0,
        "max_extra_seconds": max(extra_seconds_list) if extra_seconds_list else 0,
        "centre_affected_fraction": round(centre_affected_fraction, 4),
        "centre_retest_recommended": centre_retest,
    }

    # 8. Incident audit entry: one incident audit action "impact_computed" when first computed or updated
    if not was_previously_computed or new_or_changed_count > 0:
        append_audit_entry(
            entry_type="incident",
            ref_id=incident_id,
            payload={
                "action": "impact_computed",
                "incident_id": incident_id,
                "summary": by_remedy,
                "centre_affected_fraction": round(centre_affected_fraction, 4),
            },
            ts_iso=computed_at_iso,
            conn=conn,
        )

    # 9. Institutional notices: Admin and centre notices
    cursor.execute(
        """
        SELECT COUNT(*) AS c
        FROM incident_impacts i
        LEFT JOIN decisions d ON d.incident_id = i.incident_id AND d.candidate_id = i.candidate_id
        WHERE i.incident_id = ? AND d.decision_id IS NULL;
        """,
        (incident_id,),
    )
    pending_decisions = cursor.fetchone()["c"]
    summary_str = ", ".join(f"{k}: {v}" for k, v in by_remedy.items())
    admin_msg = f"Impact assessment completed for incident {incident_id}. Breakdown: {summary_str}. Pending decisions: {pending_decisions}."
    centre_msg = f"Impact assessment completed for incident {incident_id}. Breakdown: {summary_str}. Pending decisions: {pending_decisions}."

    inst_run = computed_at_iso if (not was_previously_computed or new_or_changed_count > 0) else "initial"
    emit_notice(
        conn=conn,
        audience="admin",
        target_id=None,
        incident_id=incident_id,
        kind="impact_ready",
        ref_key=f"impact_ready:admin:{incident_id}:{inst_run}",
        message=admin_msg,
        now_iso=computed_at_iso,
    )
    emit_notice(
        conn=conn,
        audience="centre",
        target_id=cid,
        incident_id=incident_id,
        kind="impact_ready",
        ref_key=f"impact_ready:centre:{incident_id}:{cid}:{inst_run}",
        message=centre_msg,
        now_iso=computed_at_iso,
    )

    # 10. Update incident record
    evidence_dict["centre_affected_fraction"] = round(centre_affected_fraction, 4)
    evidence_dict["impact_summary"] = summary_dict
    evidence_dict["centre_retest_recommended"] = centre_retest

    cursor.execute(
        """
        UPDATE incidents
        SET evidence = ?, impact_computed_at = ?
        WHERE incident_id = ?;
        """,
        (json.dumps(evidence_dict), computed_at_iso, incident_id),
    )

    cursor.close()
    return summary_dict
