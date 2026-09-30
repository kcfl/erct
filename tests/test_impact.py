"""Tests for Phase 3b-i Impact Engine and Remedy Rules.

Covers:
- Unit tests 1-8: pure logic, synthetic events, boundaries, quality, exposed set,
  idempotency, restart safety, stragglers, session states, and audit trail.
- Slow tests 9-11: full simulator execution, calibration, oracle validation against
  ground_truth.jsonl, network drop zero-loss, and multi-centre fault isolation.
"""
from __future__ import annotations

import json
import math
import os
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List

import httpx
import pytest
import uvicorn
import yaml

from app.config import AppConfig, ImpactConfig, RemedyConfig, get_config, reload_config
from app.core.audit_chain import append_audit_entry, verify_audit_chain
from app.core.detection import DetectionEngine
from app.core.impact import (
    RemedyDecision,
    SessionFacts,
    compute_incident_impact,
    compute_session_facts,
    evaluate_remedy,
    format_iso,
    parse_iso,
)
from app.db import format_utc_iso, get_db_connection, init_db, write_transaction
from app.main import app, seed_database
from simulator.agent_runner import SimulatorRunner


@pytest.fixture
def fresh_impact_db(tmp_path: Path):
    """Set up an isolated seeded database for unit testing."""
    db_file = tmp_path / "test_impact.db"
    cfg_file = tmp_path / "test_cfg_impact.yaml"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))
    with get_db_connection(str(db_file)) as conn:
        conn.execute("UPDATE sessions SET started_at = '2026-09-29T09:00:00+00:00', state = 'active';")
        conn.commit()

    yield str(db_file)

    if old_env is not None:
        os.environ["ERCT_CONFIG_PATH"] = old_env
        reload_config(old_env)
    else:
        os.environ.pop("ERCT_CONFIG_PATH", None)
        reload_config()


def create_dummy_facts(
    lost_s: float = 0.0,
    lost_answers: int = 0,
    quality: str = "strong",
    candidate_id: str = "CAND-000001",
    session_id: str = "SES-000001-1",
    centre_id: str = "C-BPL-02",
) -> SessionFacts:
    return SessionFacts(
        session_id=session_id,
        candidate_id=candidate_id,
        centre_id=centre_id,
        last_good_ts="2026-09-29T10:00:00+00:00",
        last_good_dt=datetime(2026, 9, 29, 10, 0, 0, tzinfo=timezone.utc),
        L=lost_answers,
        last_hb_event_id="ev-hb-1",
        resume_ts="2026-09-29T10:01:00+00:00",
        resume_dt=datetime(2026, 9, 29, 10, 1, 0, tzinfo=timezone.utc),
        resume_hb_event_id="ev-hb-2",
        gap=lost_s,
        lost_s=lost_s,
        S=0,
        last_save_event_id=None,
        lost_answers=lost_answers,
        evidence_quality=quality,
        quality_details={},
    )


# ---------------------------------------------------------------------------
# UNIT TEST 1: Remedy boundaries
# ---------------------------------------------------------------------------
def test_01_remedy_boundaries():
    cfg = get_config()

    # 1. (lost 300, answers 2) => R1, extra 300
    f1 = create_dummy_facts(lost_s=300.0, lost_answers=2, quality="strong")
    d1 = evaluate_remedy(f1, {"centre_affected_fraction": 0.1, "integrity_flag": False}, cfg)
    assert d1.rule_id == "R1"
    assert d1.remedy == "resume"
    assert d1.extra_seconds == 300

    # 2. (lost 300.01) => R2, extra ceil(300.01) + 120 = 421
    f2 = create_dummy_facts(lost_s=300.01, lost_answers=2, quality="strong")
    d2 = evaluate_remedy(f2, {"centre_affected_fraction": 0.1, "integrity_flag": False}, cfg)
    assert d2.rule_id == "R2"
    assert d2.remedy == "extra_time"
    assert d2.extra_seconds == math.ceil(300.01) + 120  # 421

    # 3. (answers 3) => R2, extra ceil(lost) + 120
    f3 = create_dummy_facts(lost_s=50.0, lost_answers=3, quality="strong")
    d3 = evaluate_remedy(f3, {"centre_affected_fraction": 0.1, "integrity_flag": False}, cfg)
    assert d3.rule_id == "R2"
    assert d3.remedy == "extra_time"
    assert d3.extra_seconds == 50 + 120  # 170

    # 4. Partial evidence with R4 conditions => R3 wins for that row
    f4 = create_dummy_facts(lost_s=100.0, lost_answers=1, quality="partial")
    d4 = evaluate_remedy(f4, {"centre_affected_fraction": 0.8, "integrity_flag": True}, cfg)
    assert d4.rule_id == "R3"
    assert d4.remedy == "manual_review"
    assert d4.extra_seconds == 0

    # 5. Strong + integrity + fraction 0.5 => R4
    f5 = create_dummy_facts(lost_s=100.0, lost_answers=1, quality="strong")
    d5 = evaluate_remedy(f5, {"centre_affected_fraction": 0.50, "integrity_flag": True}, cfg)
    assert d5.rule_id == "R4"
    assert d5.remedy == "retest_recommended"
    assert d5.extra_seconds == 0

    # 6. Strong + integrity + fraction 0.49 => not R4 (falls back to R1)
    f6 = create_dummy_facts(lost_s=100.0, lost_answers=1, quality="strong")
    d6 = evaluate_remedy(f6, {"centre_affected_fraction": 0.49, "integrity_flag": True}, cfg)
    assert d6.rule_id == "R1"
    assert d6.remedy == "resume"
    assert d6.extra_seconds == 100

    # 7. R1 with lost 0 => extra 0
    f7 = create_dummy_facts(lost_s=0.0, lost_answers=0, quality="strong")
    d7 = evaluate_remedy(f7, {"centre_affected_fraction": 0.1, "integrity_flag": False}, cfg)
    assert d7.rule_id == "R1"
    assert d7.remedy == "resume"
    assert d7.extra_seconds == 0


# ---------------------------------------------------------------------------
# UNIT TEST 2: Facts from synthetic events
# ---------------------------------------------------------------------------
def test_02_facts_from_synthetic_events():
    cfg = get_config()
    session = {"session_id": "SES-001", "candidate_id": "CAND-001", "centre_id": "C-BPL-02"}
    ws = "2026-09-29T10:00:20+00:00"
    we = "2026-09-29T10:01:20+00:00"

    t0 = datetime(2026, 9, 29, 10, 0, 0, tzinfo=timezone.utc)

    # 1. Power gap synthetic events: 10 baseline heartbeats, last at ws, resume at we + 2s, gap = 62s
    pre_hbs = [
        {
            "event_id": f"hb-pre-{i}",
            "type": "HEARTBEAT",
            "ts": format_iso(t0 + timedelta(seconds=i * 2)),
            "payload": {"local_seq": 5},
        }
        for i in range(11)  # 0 to 20s
    ]
    post_hb = {
        "event_id": "hb-post-1",
        "type": "HEARTBEAT",
        "ts": format_iso(parse_iso(we) + timedelta(seconds=2)),
        "payload": {"local_seq": 2},
    }
    saves = [
        {
            "event_id": "save-1",
            "type": "ANSWER_SAVED",
            "ts": format_iso(t0 + timedelta(seconds=10)),
            "payload": {"saved_seq": 2},
        }
    ]

    facts_power = compute_session_facts(pre_hbs + [post_hb] + saves, session, ws, we, cfg)
    assert facts_power.lost_s > 60.0
    assert facts_power.L == 5
    assert facts_power.S == 2
    assert facts_power.lost_answers == 3
    assert facts_power.evidence_quality == "strong"

    # 2. Network backfill synthetic events: heartbeats present every 2s throughout [ws, we]
    backfilled_hbs = [
        {
            "event_id": f"hb-net-{i}",
            "type": "HEARTBEAT",
            "ts": format_iso(t0 + timedelta(seconds=i * 2)),
            "payload": {"local_seq": i},
        }
        for i in range(45)  # covers past we
    ]
    # S catches up via backfilled saves
    net_saves = [
        {
            "event_id": "save-net-1",
            "type": "ANSWER_SAVED",
            "ts": format_iso(parse_iso(we) + timedelta(seconds=1)),
            "payload": {"saved_seq": 40},
        }
    ]
    facts_net = compute_session_facts(backfilled_hbs + net_saves, session, ws, we, cfg)
    assert facts_net.gap == 2.0
    assert facts_net.lost_s == 0.0  # 2.0s <= 10.0s threshold
    assert facts_net.lost_answers == 0
    assert facts_net.evidence_quality == "strong"

    # 3. Flaky session with a 6 s gap => lost 0 (below the loss_gap threshold 10s)
    flaky_hbs = list(pre_hbs)
    # Skip 3 heartbeats (6s gap) after window_start, then heartbeats resume
    t_after = parse_iso(ws) + timedelta(seconds=6.0)
    flaky_hbs.append({
        "event_id": "hb-flaky-1",
        "type": "HEARTBEAT",
        "ts": format_iso(t_after),
        "payload": {"local_seq": 5},
    })
    # Add regular heartbeats up to we and beyond
    for step in range(1, 10):
        flaky_hbs.append({
            "event_id": f"hb-flaky-{step+1}",
            "type": "HEARTBEAT",
            "ts": format_iso(t_after + timedelta(seconds=step * 2.0)),
            "payload": {"local_seq": 5},
        })
    facts_flaky = compute_session_facts(flaky_hbs + saves, session, ws, format_iso(t_after + timedelta(seconds=10)), cfg)
    assert facts_flaky.gap == 6.0
    assert facts_flaky.lost_s == 0.0  # 6.0s <= 10.0s threshold


# ---------------------------------------------------------------------------
# UNIT TEST 3: Evidence quality
# ---------------------------------------------------------------------------
def test_03_evidence_quality():
    cfg = get_config()
    session = {"session_id": "SES-001", "candidate_id": "CAND-001", "centre_id": "C-BPL-02"}
    ws = "2026-09-29T10:00:20+00:00"
    we = "2026-09-29T10:01:20+00:00"
    t0 = datetime(2026, 9, 29, 10, 0, 0, tzinfo=timezone.utc)

    # 1. Missing: no pre-window heartbeat
    post_hb = {
        "event_id": "hb-post",
        "type": "HEARTBEAT",
        "ts": format_iso(parse_iso(we) + timedelta(seconds=2)),
        "payload": {"local_seq": 1},
    }
    q1 = compute_session_facts([post_hb], session, ws, we, cfg)
    assert q1.evidence_quality == "missing"

    # 2. Missing: never resumed
    pre_hbs = [
        {
            "event_id": f"hb-pre-{i}",
            "type": "HEARTBEAT",
            "ts": format_iso(t0 + timedelta(seconds=i * 2)),
            "payload": {"local_seq": 1},
        }
        for i in range(11)
    ]
    q2 = compute_session_facts(pre_hbs, session, ws, we, cfg)
    assert q2.evidence_quality == "missing"

    # 3. Partial: stale (last_good_ts older than 2.5 * 2.0 = 5.0s before window_start)
    # ws is at t0 + 20s. Last heartbeat at t0 + 14s => 6s before ws (> 5.0s)
    stale_hbs = [
        {
            "event_id": f"hb-pre-{i}",
            "type": "HEARTBEAT",
            "ts": format_iso(t0 + timedelta(seconds=i * 2)),
            "payload": {"local_seq": 1},
        }
        for i in range(8)  # up to t0 + 14s
    ]
    q3 = compute_session_facts(stale_hbs + [post_hb], session, ws, we, cfg)
    assert q3.evidence_quality == "partial"
    assert "stale" in q3.quality_details.get("reason", "")

    # 4. Partial: no local_seq
    no_lseq_hbs = [
        {
            "event_id": f"hb-pre-{i}",
            "type": "HEARTBEAT",
            "ts": format_iso(t0 + timedelta(seconds=i * 2)),
            "payload": {"remaining_s": 3600},  # no local_seq
        }
        for i in range(11)
    ]
    q4 = compute_session_facts(no_lseq_hbs + [post_hb], session, ws, we, cfg)
    assert q4.evidence_quality == "partial"
    assert "local_seq" in q4.quality_details.get("reason", "")

    # 5. Partial: 30% baseline slots missing (3 out of 10 slots empty)
    # Baseline covers [t0, t0 + 20s]. Skip slots 1, 3, 5
    baseline_skipped_hbs = [
        {
            "event_id": f"hb-pre-{i}",
            "type": "HEARTBEAT",
            "ts": format_iso(t0 + timedelta(seconds=i * 2)),
            "payload": {"local_seq": 1},
        }
        for i in [0, 2, 4, 6, 7, 8, 9, 10]  # missing slots 1, 3, 5 (30% missing)
    ]
    q5 = compute_session_facts(baseline_skipped_hbs + [post_hb], session, ws, we, cfg)
    assert q5.evidence_quality == "partial"
    assert "baseline missing slots" in q5.quality_details.get("reason", "")

    # 6. Strong: everything clean
    q6 = compute_session_facts(pre_hbs + [post_hb], session, ws, we, cfg)
    assert q6.evidence_quality == "strong"

    # Step 1.b Baseline requirements:
    # (a) 5 regular heartbeats since start => strong
    t_ws = parse_iso(ws)
    t_start_5 = t_ws - timedelta(seconds=10)  # 5 slots of 2.0s
    sess_5 = {"session_id": "SES-B1", "candidate_id": "C-B1", "centre_id": "C-BPL-02", "started_at": format_iso(t_start_5)}
    hbs_5 = [
        {"event_id": f"hb-5-{i}", "type": "HEARTBEAT", "ts": format_iso(t_start_5 + timedelta(seconds=i * 2)), "payload": {"local_seq": i}}
        for i in range(1, 6)
    ]
    q_b1 = compute_session_facts(hbs_5 + [post_hb], sess_5, ws, we, cfg)
    assert q_b1.evidence_quality == "strong", f"5 regular heartbeats must be strong, got {q_b1.evidence_quality}: {q_b1.quality_details}"

    # (b) 2 heartbeats only since start => partial ("too little history")
    t_start_2 = t_ws - timedelta(seconds=4)  # 2 slots only (< min 3)
    sess_2 = {"session_id": "SES-B2", "candidate_id": "C-B2", "centre_id": "C-BPL-02", "started_at": format_iso(t_start_2)}
    hbs_2 = [
        {"event_id": f"hb-2-{i}", "type": "HEARTBEAT", "ts": format_iso(t_start_2 + timedelta(seconds=i * 2)), "payload": {"local_seq": i}}
        for i in range(1, 3)
    ]
    q_b2 = compute_session_facts(hbs_2 + [post_hb], sess_2, ws, we, cfg)
    assert q_b2.evidence_quality == "partial"
    assert "too little history" in q_b2.quality_details.get("reason", "")

    # (c) 5 available slots with 3 missing => partial
    hbs_5_missing = [
        {"event_id": f"hb-5m-{i}", "type": "HEARTBEAT", "ts": format_iso(t_start_5 + timedelta(seconds=i * 2)), "payload": {"local_seq": i}}
        for i in [2, 5]  # only 2 of 5 slots present, 3 missing (60% missing)
    ]
    q_b3 = compute_session_facts(hbs_5_missing + [post_hb], sess_5, ws, we, cfg)
    assert q_b3.evidence_quality == "partial"
    assert "baseline missing slots" in q_b3.quality_details.get("reason", "")

    # (d) Flaky pattern (~40% missing over 10 slots) => partial
    t_start_10 = t_ws - timedelta(seconds=20)
    sess_10 = {"session_id": "SES-B4", "candidate_id": "C-B4", "centre_id": "C-BPL-02", "started_at": format_iso(t_start_10)}
    # 10 slots: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9; provide 6 heartbeats (40% missing)
    hbs_flaky_10 = [
        {"event_id": f"hb-10-{i}", "type": "HEARTBEAT", "ts": format_iso(t_start_10 + timedelta(seconds=i * 2)), "payload": {"local_seq": i}}
        for i in [1, 3, 5, 7, 8, 10]
    ]
    q_b4 = compute_session_facts(hbs_flaky_10 + [post_hb], sess_10, ws, we, cfg)
    assert q_b4.evidence_quality == "partial"
    assert "baseline missing slots" in q_b4.quality_details.get("reason", "")


# ---------------------------------------------------------------------------
# UNIT TEST 4: Exposed set
# ---------------------------------------------------------------------------
def test_04_exposed_set(fresh_impact_db: str):
    ws = "2026-09-29T10:00:00+00:00"
    we = "2026-09-29T10:01:00+00:00"
    now_iso = "2026-09-29T10:02:00+00:00"

    # Set up resolved incident at C-BPL-02
    inc_id = "INC-C-BPL-02-1"
    with write_transaction(fresh_impact_db) as conn:
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at)
            VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'power', 'critical', 'resolved', ?, ?, ?, ?);
            """,
            (inc_id, ws, ws, we, now_iso),
        )

        # Alter candidate 1 at C-BPL-02: started after window_start
        conn.execute(
            "UPDATE sessions SET started_at = '2026-09-29T10:00:10+00:00' WHERE session_id = 'SES-000041-1';"
        )
        # Alter candidate 2 at C-BPL-02: submitted before window_start
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
            VALUES ('ev-sub-test', 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000042-1', 'CAND-000042', 99, '2026-09-29T09:55:00+00:00', 'SESSION_SUBMITTED', 'info', '{}', '2026-09-29T09:55:00+00:00');
            """
        )

        # For remaining 38 sessions of C-BPL-02, insert valid pre and post heartbeats
        for cid_num in range(43, 81):
            sess_id = f"SES-{cid_num:06d}-1"
            cand_id = f"CAND-{cid_num:06d}"
            # pre-window heartbeats
            for s in range(10):
                t_hb = format_iso(parse_iso(ws) - timedelta(seconds=(10 - s) * 2))
                conn.execute(
                    """
                    INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
                    VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', ?, ?, ?, ?, 'HEARTBEAT', 'info', ?, ?);
                    """,
                    (f"hb-pre-{cid_num}-{s}", sess_id, cand_id, s + 1, t_hb, json.dumps({"local_seq": 1}), t_hb),
                )
            # post-window heartbeat
            t_post = format_iso(parse_iso(we) + timedelta(seconds=2))
            conn.execute(
                """
                INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
                VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', ?, ?, 90, ?, 'HEARTBEAT', 'info', ?, ?);
                """,
                (f"hb-post-{cid_num}", sess_id, cand_id, t_post, json.dumps({"local_seq": 1}), t_post),
            )

    with write_transaction(fresh_impact_db) as conn:
        compute_incident_impact(conn, inc_id, now_iso)

    with get_db_connection(fresh_impact_db) as conn:
        rows = conn.execute("SELECT candidate_id FROM incident_impacts WHERE incident_id = ?;", (inc_id,)).fetchall()
        impact_cands = {r["candidate_id"] for r in rows}

        # 38 sessions exposed (40 minus 1 started-after minus 1 submitted-before)
        assert len(impact_cands) == 38
        assert "CAND-000041" not in impact_cands, "Candidate who started after window_start must be excluded"
        assert "CAND-000042" not in impact_cands, "Candidate who submitted before window_start must be excluded"

        # No other centre appears
        other_centres_c = conn.execute(
            """
            SELECT COUNT(*) AS c FROM incident_impacts ii
            JOIN sessions s ON ii.candidate_id = s.candidate_id
            WHERE ii.incident_id = ? AND s.centre_id != 'C-BPL-02';
            """,
            (inc_id,),
        ).fetchone()["c"]
        assert other_centres_c == 0, "No candidates from other centres must ever appear"


# ---------------------------------------------------------------------------
# UNIT TEST 5: Idempotency and restart
# ---------------------------------------------------------------------------
def test_05_idempotency_and_restart(fresh_impact_db: str):
    ws = "2026-09-29T10:00:00+00:00"
    we = "2026-09-29T10:01:00+00:00"
    now_iso = "2026-09-29T10:02:00+00:00"
    inc_id = "INC-C-BPL-02-1"

    with write_transaction(fresh_impact_db) as conn:
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at)
            VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'power', 'critical', 'resolved', ?, ?, ?, ?);
            """,
            (inc_id, ws, ws, we, now_iso),
        )
        # Add a couple heartbeats for 1 candidate
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
            VALUES ('hb-1', 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000041-1', 'CAND-000041', 1, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
            """,
            (ws, ws),
        )
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
            VALUES ('hb-2', 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000041-1', 'CAND-000041', 2, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
            """,
            (format_iso(parse_iso(we) + timedelta(seconds=2)), format_iso(parse_iso(we) + timedelta(seconds=2))),
        )

    # Compute first time
    with write_transaction(fresh_impact_db) as conn:
        compute_incident_impact(conn, inc_id, now_iso)

    with get_db_connection(fresh_impact_db) as conn:
        audit_c1 = conn.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'impact';").fetchone()["c"]
        rows1 = conn.execute("SELECT * FROM incident_impacts WHERE incident_id = ?;", (inc_id,)).fetchall()

    # Compute second time: must be completely idempotent
    with write_transaction(fresh_impact_db) as conn:
        compute_incident_impact(conn, inc_id, now_iso)

    with get_db_connection(fresh_impact_db) as conn:
        audit_c2 = conn.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'impact';").fetchone()["c"]
        rows2 = conn.execute("SELECT * FROM incident_impacts WHERE incident_id = ?;", (inc_id,)).fetchall()

    assert audit_c1 == audit_c2, "Running impact engine twice must produce 0 duplicate audit entries"
    assert len(rows1) == len(rows2)

    # Test Restart Safety: reset impact_computed_at to NULL
    with write_transaction(fresh_impact_db) as conn:
        conn.execute("UPDATE incidents SET impact_computed_at = NULL WHERE incident_id = ?;", (inc_id,))

    engine = DetectionEngine(fresh_impact_db)
    engine.reset_start_time(parse_iso(now_iso) - timedelta(seconds=120))
    engine.tick(parse_iso(now_iso) + timedelta(seconds=5))

    with get_db_connection(fresh_impact_db) as conn:
        inc_computed = conn.execute("SELECT impact_computed_at FROM incidents WHERE incident_id = ?;", (inc_id,)).fetchone()["impact_computed_at"]
        assert inc_computed is not None, "Resolved incident with impact_computed_at NULL must be computed on next tick"


# ---------------------------------------------------------------------------
# UNIT TEST 6: Stragglers
# ---------------------------------------------------------------------------
def test_06_stragglers(fresh_impact_db: str):
    ws = "2026-09-29T10:00:00+00:00"
    we = "2026-09-29T10:01:00+00:00"
    now_iso = "2026-09-29T10:01:30+00:00"
    inc_id = "INC-C-BPL-02-1"

    with write_transaction(fresh_impact_db) as conn:
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at)
            VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'power', 'critical', 'resolved', ?, ?, ?, ?);
            """,
            (inc_id, ws, ws, we, now_iso),
        )
        # 10 baseline pre-window heartbeats for CAND-000041 (no post-window heartbeat yet => missing)
        for s in range(10):
            t_hb = format_iso(parse_iso(ws) - timedelta(seconds=(9 - s) * 2))
            conn.execute(
                """
                INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
                VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000041-1', 'CAND-000041', ?, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
                """,
                (f"hb-pre-straggler-{s}", s + 1, t_hb, t_hb),
            )

        # Baseline heartbeats for CAND-000042: only 2 baseline heartbeats present => partial due to missing slots
        for s in (0, 1):
            t_hb = format_iso(parse_iso(ws) - timedelta(seconds=(9 - s) * 2))
            conn.execute(
                """
                INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
                VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000042-1', 'CAND-000042', ?, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
                """,
                (f"hb-pre-straggler42-{s}", s + 1, t_hb, t_hb),
            )
        # CAND-000042 has resume heartbeat at window_end => both last_good and resume exist, but evidence is 'partial'
        t_res42 = format_iso(parse_iso(we) + timedelta(seconds=2))
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
            VALUES ('hb-post-straggler42-init', 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000042-1', 'CAND-000042', 3, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
            """,
            (t_res42, t_res42),
        )

    # Initial impact computation:
    # CAND-000041 has quality 'missing', review queue 'pending', session 'under_review'
    # CAND-000042 has quality 'partial', review queue 'pending', session 'under_review'
    with write_transaction(fresh_impact_db) as conn:
        compute_incident_impact(conn, inc_id, now_iso)

    with get_db_connection(fresh_impact_db) as conn:
        row41 = conn.execute("SELECT evidence_quality, remedy_recommended FROM incident_impacts WHERE candidate_id = 'CAND-000041';").fetchone()
        assert row41["evidence_quality"] == "missing"
        assert row41["remedy_recommended"] == "manual_review"

        row42 = conn.execute("SELECT evidence_quality, remedy_recommended FROM incident_impacts WHERE candidate_id = 'CAND-000042';").fetchone()
        assert row42["evidence_quality"] == "partial"
        assert row42["remedy_recommended"] == "manual_review"

        rq = conn.execute("SELECT status FROM review_queue WHERE candidate_id = 'CAND-000041';").fetchone()
        assert rq["status"] == "pending"

        s_state = conn.execute("SELECT state FROM sessions WHERE candidate_id = 'CAND-000041';").fetchone()["state"]
        assert s_state == "under_review"

    # Stragglers resume and buffered baseline arrives!
    t_resume = format_iso(parse_iso(we) + timedelta(seconds=40))
    with write_transaction(fresh_impact_db) as conn:
        # CAND-000041 emits resume heartbeat post window_end
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
            VALUES ('hb-post-straggler', 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000041-1', 'CAND-000041', 2, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
            """,
            (t_resume, t_resume),
        )
        conn.execute("UPDATE sessions SET last_heartbeat_at = ? WHERE candidate_id = 'CAND-000041';", (t_resume,))

        # CAND-000042: remaining 8 buffered baseline heartbeats arrive, plus new heartbeat post window_end
        for s in range(2, 10):
            t_hb = format_iso(parse_iso(ws) - timedelta(seconds=(9 - s) * 2))
            conn.execute(
                """
                INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
                VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000042-1', 'CAND-000042', ?, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
                """,
                (f"hb-pre-straggler42-late-{s}", s + 10, t_hb, t_resume),
            )
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, session_id, candidate_id, seq, ts, type, severity, payload, ingested_at)
            VALUES ('hb-post-straggler42-late', 'EX-2026-PS6-01', 'C-BPL-02', 'SES-000042-1', 'CAND-000042', 25, ?, 'HEARTBEAT', 'info', '{"local_seq": 1}', ?);
            """,
            (t_resume, t_resume),
        )
        conn.execute("UPDATE sessions SET last_heartbeat_at = ? WHERE candidate_id = 'CAND-000042';", (t_resume,))

    # Next tick processes both missing and partial stragglers within recompute_window_s
    engine = DetectionEngine(fresh_impact_db)
    engine.reset_start_time(parse_iso(now_iso) - timedelta(seconds=120))
    engine.tick(parse_iso(t_resume) + timedelta(seconds=5))

    with get_db_connection(fresh_impact_db) as conn:
        row41_after = conn.execute("SELECT evidence_quality, remedy_recommended FROM incident_impacts WHERE candidate_id = 'CAND-000041';").fetchone()
        assert row41_after["evidence_quality"] in ("strong", "partial")
        assert row41_after["remedy_recommended"] != "manual_review"

        row42_after = conn.execute("SELECT evidence_quality, remedy_recommended FROM incident_impacts WHERE candidate_id = 'CAND-000042';").fetchone()
        assert row42_after["evidence_quality"] == "strong", f"CAND-000042 must upgrade to strong after baseline arrives, got {row42_after['evidence_quality']}"
        assert row42_after["remedy_recommended"] != "manual_review"

        rq2 = conn.execute("SELECT status FROM review_queue WHERE candidate_id = 'CAND-000041';").fetchone()
        assert rq2["status"] == "superseded", "Review queue entry must be superseded when remedy resolves"

        s_state2 = conn.execute("SELECT state FROM sessions WHERE candidate_id = 'CAND-000041';").fetchone()["state"]
        assert s_state2 == "resumed", "Session state must return to resumed"


# ---------------------------------------------------------------------------
# UNIT TEST 7: Session states and detection guards
# ---------------------------------------------------------------------------
def test_07_states_guards(fresh_impact_db: str):
    ws = "2026-09-29T10:00:00+00:00"
    we = "2026-09-29T10:01:00+00:00"
    now_iso = "2026-09-29T10:02:00+00:00"
    inc_id = "INC-C-BPL-02-1"

    # Set up resolved incident where candidate has quality 'missing' => manual_review
    with write_transaction(fresh_impact_db) as conn:
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at)
            VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'power', 'critical', 'resolved', ?, ?, ?, ?);
            """,
            (inc_id, ws, ws, we, now_iso),
        )

    with write_transaction(fresh_impact_db) as conn:
        compute_incident_impact(conn, inc_id, now_iso)

    # Check candidate state is 'under_review'
    with get_db_connection(fresh_impact_db) as conn:
        s_state = conn.execute("SELECT state FROM sessions WHERE session_id = 'SES-000041-1';").fetchone()["state"]
        assert s_state == "under_review"

    # Tick the detection engine multiple times: must never touch 'under_review'
    engine = DetectionEngine(fresh_impact_db)
    engine.reset_start_time(parse_iso(now_iso) - timedelta(seconds=120))
    for step in range(5):
        t_tick = parse_iso(now_iso) + timedelta(seconds=step * 2)
        engine.tick(t_tick)

    with get_db_connection(fresh_impact_db) as conn:
        s_state_after = conn.execute("SELECT state FROM sessions WHERE session_id = 'SES-000041-1';").fetchone()["state"]
        assert s_state_after == "under_review", "Detection engine must NEVER override an 'under_review' session state"


# ---------------------------------------------------------------------------
# UNIT TEST 8: Audit chain verification and no duplicates
# ---------------------------------------------------------------------------
def test_08_audit_verification(fresh_impact_db: str):
    ws = "2026-09-29T10:00:00+00:00"
    we = "2026-09-29T10:01:00+00:00"
    now_iso = "2026-09-29T10:02:00+00:00"
    inc_id = "INC-C-BPL-02-1"

    with write_transaction(fresh_impact_db) as conn:
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at)
            VALUES (?, 'EX-2026-PS6-01', 'C-BPL-02', 'power', 'critical', 'resolved', ?, ?, ?, ?);
            """,
            (inc_id, ws, ws, we, now_iso),
        )

    with write_transaction(fresh_impact_db) as conn:
        compute_incident_impact(conn, inc_id, now_iso)

    with get_db_connection(fresh_impact_db) as conn:
        impact_c = conn.execute("SELECT COUNT(*) AS c FROM incident_impacts WHERE incident_id = ?;", (inc_id,)).fetchone()["c"]
        audit_impact_c = conn.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'impact';").fetchone()["c"]
        assert audit_impact_c == impact_c, f"Impact audit entries ({audit_impact_c}) must equal rows written ({impact_c})"

        # Verify audit chain integrity
        chain_res = verify_audit_chain(fresh_impact_db)
        assert chain_res.ok is True, f"Audit chain verification failed: {chain_res.error}"


# ---------------------------------------------------------------------------
# UNIT TEST 8b: Late batch recomputation preserves decisions & emits notice
# ---------------------------------------------------------------------------
def test_late_batch_recomputation_preserves_decisions_and_emits_notice(fresh_impact_db: str):
    """When late events increment within recompute_window_s:
    1. All rows for the incident are recomputed.
    2. Changed recommendations append an audit entry of entry_type='impact'.
    3. Existing controller decisions in the decisions table are preserved.
    4. An admin notice with 'recommendation changed after decision' is emitted.
    """
    db_path = fresh_impact_db
    now_iso = "2026-09-29T10:05:00+00:00"
    cfg = get_config()

    with get_db_connection(db_path) as conn:
        # Create a resolved incident on C-BPL-02
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at, evidence)
            VALUES ('INC-LATE-01', 'EX-2026-PS6-01', 'C-BPL-02', 'power', 'critical', 'resolved',
                    '2026-09-29T10:00:00+00:00', '2026-09-29T10:00:00+00:00', '2026-09-29T10:01:00+00:00', '2026-09-29T10:01:00+00:00',
                    '{"late_events": 0}');
            """
        )
        # Candidate has regular heartbeats before window_start, but missing resume heartbeat (quality=missing -> manual_review)
        for i, s_off in enumerate(range(40, 60, 2)):
            conn.execute(
                f"""
                INSERT INTO events (event_id, ts, ingested_at, centre_id, candidate_id, session_id, seq, type, severity, payload)
                VALUES ('ev-hb-{i}', '2026-09-29T09:59:{s_off:02d}+00:00', '2026-09-29T09:59:{s_off:02d}+00:00', 'C-BPL-02', 'CAND-000041', 'SES-000041-1', {i+1}, 'HEARTBEAT', 'info', '{{"local_seq": {10+i}, "last_save_seq": 5}}');
                """
            )
        conn.commit()

        # 1. Compute initial impact
        compute_incident_impact(conn, 'INC-LATE-01', now_iso, cfg)

        # Candidate should have manual_review due to missing resume heartbeat
        row = conn.execute("SELECT remedy_recommended, evidence_quality FROM incident_impacts WHERE incident_id = 'INC-LATE-01' AND candidate_id = 'CAND-000041';").fetchone()
        assert row["evidence_quality"] == "missing"
        assert row["remedy_recommended"] == "manual_review"

        # 2. Record a controller decision
        conn.execute(
            """
            INSERT INTO decisions (incident_id, candidate_id, action, remedy, extra_seconds, rule_id, recommended_remedy, impact_version, mode, decided_by, reason, decided_at)
            VALUES ('INC-LATE-01', 'CAND-000041', 'apply', 'extra_time', 120, 'R2', 'manual_review', 1, 'manual', 'controller_alice', 'Controller granted extra time', '2026-09-29T10:02:00+00:00');
            """
        )
        conn.commit()

        # 3. Late batch of events arrives with a resume heartbeat
        conn.execute(
            """
            INSERT INTO events (event_id, ts, ingested_at, centre_id, candidate_id, session_id, seq, type, severity, payload)
            VALUES ('ev-hb-late', '2026-09-29T10:01:05+00:00', '2026-09-29T10:06:00+00:00', 'C-BPL-02', 'CAND-000041', 'SES-000041-1', 2, 'HEARTBEAT', 'info', '{"local_seq": 11, "last_save_seq": 5}');
            """
        )
        # Update incident evidence with incremented late_events
        conn.execute("UPDATE incidents SET evidence = '{\"late_events\": 1}' WHERE incident_id = 'INC-LATE-01';")
        conn.commit()

        # Trigger impact computation (as DetectionEngine does when late_events increments)
        compute_incident_impact(conn, 'INC-LATE-01', "2026-09-29T10:06:00+00:00", cfg)

        # Verify candidate row was recomputed to strong and recommendation changed
        updated_row = conn.execute("SELECT remedy_recommended, evidence_quality, impact_version FROM incident_impacts WHERE incident_id = 'INC-LATE-01' AND candidate_id = 'CAND-000041';").fetchone()
        assert updated_row["evidence_quality"] == "strong"
        assert updated_row["remedy_recommended"] != "manual_review"
        assert updated_row["impact_version"] == 2

        # Verify changed recommendation is audited under entry_type='impact'
        audit_entries = conn.execute(
            "SELECT seq, ref_id, json_extract(payload, '$.remedy') AS rem FROM audit_log WHERE entry_type = 'impact' AND ref_id = 'INC-LATE-01:CAND-000041' ORDER BY seq ASC;"
        ).fetchall()
        assert len(audit_entries) >= 2, f"Must have at least 2 audit entries for changed recommendation, got {len(audit_entries)}"
        assert audit_entries[0]["rem"] == "manual_review"
        assert audit_entries[1]["rem"] == updated_row["remedy_recommended"]

        # Verify decision in decisions table was PRESERVED
        dec_rows = conn.execute("SELECT * FROM decisions WHERE incident_id = 'INC-LATE-01' AND candidate_id = 'CAND-000041';").fetchall()
        assert len(dec_rows) == 1
        assert dec_rows[0]["remedy"] == "extra_time"
        assert dec_rows[0]["decided_by"] == "controller_alice"

        # Verify admin notice emitted with "recommendation changed after decision"
        notice = conn.execute(
            "SELECT * FROM notices WHERE audience = 'admin' AND message LIKE '%recommendation changed after decision%';"
        ).fetchone()
        assert notice is not None, "Admin notice with 'recommendation changed after decision' must be emitted"


# ---------------------------------------------------------------------------
# UNIT TEST 8c: Rule R4 fallback with synthetic integrity_flag
# ---------------------------------------------------------------------------
def test_rule_r4_fallback_integrity_flag(fresh_impact_db: str):
    """Rule R4: when integrity_flag is True and centre_affected_fraction >= threshold (0.5),
    remedy is retest_recommended, extra_seconds is 0, and evidence contains fallback dict
    with R1 or R2 calculation.
    """
    cfg = get_config()
    # Case A: facts indicate R1 fallback (lost_s <= 300, lost_answers <= 2)
    facts_r1 = create_dummy_facts(lost_s=10.0, lost_answers=0, quality="strong")
    incident_facts = {"centre_affected_fraction": 0.8, "integrity_flag": True}
    dec_r1 = evaluate_remedy(facts_r1, incident_facts, cfg)
    assert dec_r1.rule_id == "R4"
    assert dec_r1.remedy == "retest_recommended"
    assert dec_r1.extra_seconds == 0
    assert dec_r1.fallback is not None
    assert dec_r1.fallback["rule_id"] == "R1"
    assert dec_r1.fallback["remedy"] == "resume"
    assert dec_r1.fallback["extra_seconds"] == 10

    # Case B: facts indicate R2 fallback (lost_answers > 2)
    facts_r2 = create_dummy_facts(lost_s=40.0, lost_answers=3, quality="strong")
    dec_r2 = evaluate_remedy(facts_r2, incident_facts, cfg)
    assert dec_r2.rule_id == "R4"
    assert dec_r2.remedy == "retest_recommended"
    assert dec_r2.extra_seconds == 0
    assert dec_r2.fallback is not None
    assert dec_r2.fallback["rule_id"] == "R2"
    assert dec_r2.fallback["remedy"] == "extra_time"
    assert dec_r2.fallback["extra_seconds"] == 40 + 120  # 160s

    # Case C: compute_incident_impact with integrity_flag writes fallback into evidence json
    now_iso = "2026-09-29T10:02:00+00:00"
    with write_transaction(fresh_impact_db) as conn:
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at, evidence)
            VALUES ('INC-R4-01', 'EX-2026-PS6-01', 'C-BPL-02', 'power', 'critical', 'resolved',
                    '2026-09-29T10:00:00+00:00', '2026-09-29T10:00:00+00:00', '2026-09-29T10:01:00+00:00', '2026-09-29T10:01:00+00:00',
                    '{"integrity_flag": true}');
            """
        )
        for i, s_off in enumerate(range(40, 60, 2)):
            conn.execute(
                f"""
                INSERT INTO events (event_id, ts, ingested_at, centre_id, candidate_id, session_id, seq, type, severity, payload)
                VALUES ('ev-r4-hb-{i}', '2026-09-29T09:59:{s_off:02d}+00:00', '2026-09-29T09:59:{s_off:02d}+00:00', 'C-BPL-02', 'CAND-000041', 'SES-000041-1', {i+1}, 'HEARTBEAT', 'info', '{{"local_seq": {10+i}, "last_save_seq": 1}}');
                """
            )
        conn.execute(
            """
            INSERT INTO events (event_id, ts, ingested_at, centre_id, candidate_id, session_id, seq, type, severity, payload)
            VALUES ('ev-r4-2', '2026-09-29T10:01:05+00:00', '2026-09-29T10:01:05+00:00', 'C-BPL-02', 'CAND-000041', 'SES-000041-1', 20, 'HEARTBEAT', 'info', '{"local_seq": 20, "last_save_seq": 1}');
            """
        )
        conn.execute("DELETE FROM sessions WHERE centre_id = 'C-BPL-02' AND candidate_id != 'CAND-000041';")
        compute_incident_impact(conn, 'INC-R4-01', now_iso, cfg)

        row = conn.execute("SELECT remedy_recommended, rule_id, evidence FROM incident_impacts WHERE incident_id = 'INC-R4-01' AND candidate_id = 'CAND-000041';").fetchone()
        assert row["rule_id"] == "R4"
        assert row["remedy_recommended"] == "retest_recommended"
        ev_json = json.loads(row["evidence"])
        assert "fallback" in ev_json
        assert ev_json["fallback"]["rule_id"] in ("R1", "R2")


# ---------------------------------------------------------------------------
# SLOW TEST 9: ACCURACY - Power loss at C-BPL-02 (seed 36)
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_09_accuracy_power_loss(tmp_path: Path):
    """ACCURACY, power_loss at C-BPL-02 (seed 36). Read data/ground_truth.jsonl in the TEST only."""
    port = find_free_port()
    db_file = tmp_path / "accuracy_pl.db"
    cfg_file = tmp_path / "accuracy_pl_cfg.yaml"
    gt_file = tmp_path / "ground_truth.jsonl"
    buf_db = str(tmp_path / "sim_buffer_pl.db")

    interval_s = 0.5

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["impact"]["heartbeat_interval_s"] = interval_s
    cfg["simulation"]["restore_boot_delay_s"] = [0.5, 2.0]
    cfg["simulation"]["seed"] = 36
    cfg["simulation"]["faults"] = []

    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(30):
        try:
            if httpx.get(f"{base_url}/v1/health", timeout=0.5).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)

    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=buf_db,
        heartbeat_override_s=interval_s,
        ground_truth_path=str(gt_file),
    )

    try:
        # Run simulator
        sim_thread = threading.Thread(target=runner.start, kwargs={"duration_s": 17.0}, daemon=True)
        sim_thread.start()

        # Wait 6.0s so all 10 baseline slots (10 * 0.5s = 5.0s) are fully populated
        time.sleep(6.0)
        httpx.post(
            f"{base_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={"centre_id": "C-BPL-02", "fault_type": "power_loss", "duration_s": 3, "params": {"source": "grid"}},
            timeout=2.0,
        )

        sim_thread.join(timeout=20.0)

        # Wait for incident resolution and impact computation
        impact_data = None
        for _ in range(40):
            r = httpx.get(f"{base_url}/v1/incidents?centre_id=C-BPL-02")
            if r.status_code == 200 and r.json():
                inc = r.json()[0]
                if inc["status"] == "resolved" and inc.get("impact_computed_at"):
                    r_imp = httpx.get(f"{base_url}/v1/incidents/{inc['incident_id']}/impact")
                    if r_imp.status_code == 200 and r_imp.json().get("rows"):
                        impact_data = r_imp.json()
                        break
            time.sleep(0.5)

        assert impact_data is not None, "Impact rows must be computed and returned for resolved incident"

        # Read ground truth
        assert gt_file.exists(), f"Ground truth file {gt_file} must exist"
        with open(gt_file, "r", encoding="utf-8") as f:
            gt_records = [json.loads(line) for line in f if line.strip()]

        pl_gt = next((r for r in gt_records if r["centre_id"] == "C-BPL-02" and r["type"] == "power_loss"), None)
        assert pl_gt is not None, "Power loss ground truth record must exist"

        truth_cands = {c["candidate_id"]: c for c in pl_gt["candidates"]}
        impact_rows = {r["candidate_id"]: r for r in impact_data["rows"]}

        # (a) Candidate set equals truth set exactly (100% listed, 0 extra, 0 from other centres)
        assert set(impact_rows.keys()) == set(truth_cands.keys()), "Candidate set must match truth set exactly"

        stale_tol = 5.0  # Independent constant (5.0 s), NOT derived from cfg.impact.*
        app_cfg = get_config()

        strong_count = 0
        partial_count = 0
        flaky_strong = 0
        flaky_partial = 0
        violations = []

        for cand_id, row in impact_rows.items():
            gt = truth_cands[cand_id]
            is_flaky = gt.get("is_flaky", False)
            quality = row["evidence_quality"]

            if quality == "strong":
                strong_count += 1
                if is_flaky:
                    flaky_strong += 1

                # (b) CALIBRATION
                diff_lost = abs(row["lost_seconds"] - gt["lost_s_from_fault_start"])
                if diff_lost > stale_tol + 0.1:  # Allow 0.1s floating tolerance
                    violations.append(f"{cand_id}: lost_s {row['lost_seconds']} vs truth {gt['lost_s_from_fault_start']} (diff {diff_lost:.2f} > {stale_tol:.2f})")

                if row["unsaved_answers"] != gt["expected_lost_answers"]:
                    violations.append(f"{cand_id}: unsaved_answers {row['unsaved_answers']} != expected {gt['expected_lost_answers']}")

                # (c) ORACLE: Recompute remedy from truth values with same rules
                oracle_lost_s = gt["lost_s_from_fault_start"]
                oracle_answers = gt["expected_lost_answers"]
                if oracle_lost_s <= app_cfg.remedy.resume_max_lost_s and oracle_answers <= app_cfg.remedy.resume_max_lost_answers:
                    oracle_remedy = "resume"
                else:
                    oracle_remedy = "extra_time"

                if row["remedy_recommended"] != oracle_remedy:
                    violations.append(f"{cand_id}: remedy {row['remedy_recommended']} != oracle {oracle_remedy}")

            else:
                partial_count += 1
                if is_flaky:
                    flaky_partial += 1
                # (d) Every non-strong row is manual_review
                assert row["remedy_recommended"] == "manual_review", f"Non-strong candidate {cand_id} must have remedy manual_review"

        # (e) Print summary counts
        print("\n=== TEST 9 ACCURACY REPORT (power_loss C-BPL-02) ===")
        print(f"Total Rows: {len(impact_rows)} | Strong: {strong_count} | Non-Strong: {partial_count}")
        print(f"Flaky Candidates: {flaky_strong} strong, {flaky_partial} partial")
        print(f"Remedy Summary: {impact_data['summary']}")
        if violations:
            print("Violations:")
            for v in violations:
                print("  -", v)

        assert not violations, f"Calibration or Oracle violations found: {violations}"

    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


# ---------------------------------------------------------------------------
# SLOW TEST 10: ACCURACY - Network drop at C-BPL-04
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_10_accuracy_network_drop(tmp_path: Path):
    """ACCURACY, network_drop at C-BPL-04: zero lost time, zero unsaved answers, zero extra_time."""
    port = find_free_port()
    db_file = tmp_path / "accuracy_nd.db"
    cfg_file = tmp_path / "accuracy_nd_cfg.yaml"
    gt_file = tmp_path / "ground_truth_nd.jsonl"
    buf_db = str(tmp_path / "sim_buffer_nd.db")

    interval_s = 0.5

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["impact"]["heartbeat_interval_s"] = interval_s
    cfg["simulation"]["faults"] = []

    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(30):
        try:
            if httpx.get(f"{base_url}/v1/health", timeout=0.5).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)

    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=buf_db,
        heartbeat_override_s=interval_s,
        ground_truth_path=str(gt_file),
    )

    try:
        sim_thread = threading.Thread(target=runner.start, kwargs={"duration_s": 8.0}, daemon=True)
        sim_thread.start()

        time.sleep(1.0)
        httpx.post(
            f"{base_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={"centre_id": "C-BPL-04", "fault_type": "network_drop", "duration_s": 2, "params": {}},
            timeout=2.0,
        )

        sim_thread.join(timeout=20.0)

        impact_data = None
        inc_resolved_at = None
        for _ in range(40):
            r = httpx.get(f"{base_url}/v1/incidents?centre_id=C-BPL-04")
            if r.status_code == 200 and r.json():
                inc = r.json()[0]
                if inc["status"] == "resolved" and inc.get("impact_computed_at"):
                    inc_resolved_at = inc["resolved_at"]
                    r_imp = httpx.get(f"{base_url}/v1/incidents/{inc['incident_id']}/impact")
                    if r_imp.status_code == 200 and r_imp.json().get("rows"):
                        impact_data = r_imp.json()
                        break
            time.sleep(0.5)

        assert impact_data is not None, "Impact rows must be computed for resolved network incident"

        # All 40 sessions listed
        assert len(impact_data["rows"]) == 40

        # Every strong row has lost_seconds 0, lost_answers 0, remedy resume, extra_seconds 0
        for r in impact_data["rows"]:
            if r["evidence_quality"] == "strong":
                assert r["lost_seconds"] == 0, f"Candidate {r['candidate_id']} expected lost_seconds 0, got {r['lost_seconds']}"
                assert r["unsaved_answers"] == 0
                assert r["remedy_recommended"] == "resume"
                assert r["extra_seconds"] == 0

        # No extra_time anywhere
        assert impact_data["summary"]["by_remedy"].get("extra_time", 0) == 0

        # impact_computed_at >= resolved_at
        assert impact_data["computed_at"] >= inc_resolved_at

    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


# ---------------------------------------------------------------------------
# SLOW TEST 11: ISOLATION - Both faults at once
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_11_isolation_both_faults(tmp_path: Path):
    """Both faults at once (C-BPL-02 power_loss and C-BPL-04 network_drop).

    Two incidents, two independent impact sets, disjoint candidate sets, and
    the other three centres have zero rows.
    """
    port = find_free_port()
    db_file = tmp_path / "isolation.db"
    cfg_file = tmp_path / "isolation_cfg.yaml"
    gt_file = tmp_path / "ground_truth_iso.jsonl"
    buf_db = str(tmp_path / "sim_buffer_iso.db")

    interval_s = 0.5

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["impact"]["heartbeat_interval_s"] = interval_s
    cfg["simulation"]["faults"] = []

    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(30):
        try:
            if httpx.get(f"{base_url}/v1/health", timeout=0.5).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)

    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=buf_db,
        heartbeat_override_s=interval_s,
        ground_truth_path=str(gt_file),
    )

    try:
        sim_thread = threading.Thread(target=runner.start, kwargs={"duration_s": 12.0}, daemon=True)
        sim_thread.start()

        time.sleep(1.0)
        # Inject both faults concurrently
        httpx.post(
            f"{base_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={"centre_id": "C-BPL-02", "fault_type": "power_loss", "duration_s": 3, "params": {"source": "grid"}},
            timeout=2.0,
        )
        httpx.post(
            f"{base_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={"centre_id": "C-BPL-04", "fault_type": "network_drop", "duration_s": 2, "params": {}},
            timeout=2.0,
        )

        sim_thread.join(timeout=20.0)

        # Poll until both incidents resolved and impact computed
        inc_bpl2 = None
        inc_bpl4 = None
        for _ in range(40):
            r = httpx.get(f"{base_url}/v1/incidents")
            if r.status_code == 200:
                incs = r.json()
                for inc in incs:
                    if inc["centre_id"] == "C-BPL-02" and inc["status"] == "resolved" and inc.get("impact_computed_at"):
                        inc_bpl2 = inc
                    elif inc["centre_id"] == "C-BPL-04" and inc["status"] == "resolved" and inc.get("impact_computed_at"):
                        inc_bpl4 = inc
            if inc_bpl2 and inc_bpl4:
                break
            time.sleep(0.5)

        assert inc_bpl2 is not None, "C-BPL-02 incident must resolve with impact computed"
        assert inc_bpl4 is not None, "C-BPL-04 incident must resolve with impact computed"

        # Query impact rows for each
        imp2 = httpx.get(f"{base_url}/v1/incidents/{inc_bpl2['incident_id']}/impact").json()
        imp4 = httpx.get(f"{base_url}/v1/incidents/{inc_bpl4['incident_id']}/impact").json()

        cands2 = {r["candidate_id"] for r in imp2["rows"]}
        cands4 = {r["candidate_id"] for r in imp4["rows"]}

        assert len(cands2) == 40
        assert len(cands4) == 40

        # Disjoint sets: no candidate in both
        assert cands2.isdisjoint(cands4), "Candidates in C-BPL-02 and C-BPL-04 must be strictly disjoint"

        # The other three centres (C-BPL-01, C-BPL-03, C-BPL-05) have NO impact rows
        with get_db_connection(str(db_file)) as conn:
            other_rows = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM incident_impacts ii
                JOIN sessions s ON ii.candidate_id = s.candidate_id
                WHERE s.centre_id IN ('C-BPL-01', 'C-BPL-03', 'C-BPL-05');
                """
            ).fetchone()["c"]
            assert other_rows == 0, f"Other three centres must have 0 impact rows, got {other_rows}"

    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# UNIT TEST: Rule R4 Fallback & Weak Evidence Priority
# ---------------------------------------------------------------------------
def test_rule_r4_fallback_integrity_flag():
    """Verify behavior when a candidate has fewer than 3 regular heartbeats (<3 slots available)
    and the centre simultaneously has an integrity violation (integrity_flag=True, centre_affected_fraction=1.0).

    In the deterministic rule priority hierarchy R3 -> R4 -> R1 -> R2:
    Rule R3 (Weak Evidence Guard) fires BEFORE Rule R4 is reached.
    The candidate is assigned 'manual_review' under Rule R3 rather than 'retest_recommended' under R4,
    because unverified telemetry cannot be safely certified for automated advisory without human review.
    """
    cfg = get_config()
    facts = SessionFacts(
        session_id="SES-000001-1",
        candidate_id="CAND-000001",
        centre_id="C-BPL-01",
        last_good_ts="2026-09-29T10:00:04+00:00",
        last_good_dt=datetime(2026, 9, 29, 10, 0, 4, tzinfo=timezone.utc),
        L=2,
        last_hb_event_id="hb-2",
        resume_ts="2026-09-29T10:00:30+00:00",
        resume_dt=datetime(2026, 9, 29, 10, 0, 30, tzinfo=timezone.utc),
        resume_hb_event_id="hb-3",
        gap=26.0,
        lost_s=26.0,
        S=1,
        last_save_event_id="sav-1",
        lost_answers=1,
        evidence_quality="partial",
        quality_details={"reason": "too little history (2 available slots < 3)", "baseline_slots": 2},
    )

    incident_facts = {
        "centre_affected_fraction": 1.0,
        "integrity_flag": True,
    }

    decision = evaluate_remedy(facts, incident_facts, cfg)

    assert decision.rule_id == "R3", f"Expected rule R3, got {decision.rule_id}"
    assert decision.remedy == "manual_review", f"Expected manual_review, got {decision.remedy}"
    assert decision.extra_seconds == 0
    assert "Evidence quality is 'partial'" in decision.rationale
    assert "too little history (2 available slots < 3)" in decision.rationale

