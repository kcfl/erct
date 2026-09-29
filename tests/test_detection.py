"""Unit and integration test suite for the Detection Engine and Incident Manager.
Covers all 13 required test cases from Step 6.
"""
from __future__ import annotations

import json
import os
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx
import pytest
import uvicorn
import yaml

from app.config import get_config, reload_config
from app.core.audit_chain import verify_audit_chain
from app.core.detection import DetectionEngine, parse_utc_iso
from app.db import format_utc_iso, get_db_connection, init_db
from app.main import app, seed_database


@pytest.fixture
def fresh_db(tmp_path: Path):
    """Set up an isolated seeded database for unit testing."""
    db_file = tmp_path / "test_detection.db"
    cfg_file = tmp_path / "test_cfg.yaml"

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

    yield str(db_file)

    if old_env is not None:
        os.environ["ERCT_CONFIG_PATH"] = old_env
        reload_config(old_env)
    else:
        os.environ.pop("ERCT_CONFIG_PATH", None)
        reload_config()


def set_centre_liveness(db_path: str, centre_id: str, ts_iso: str):
    """Helper to populate centre_liveness to keep stall guard healthy."""
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO centre_liveness (centre_id, last_ingested_at, last_event_ts, events_total)
            VALUES (?, ?, ?, 100);
            """,
            (centre_id, ts_iso, ts_iso),
        )
        conn.commit()


def keep_centres_alive(db_path: str, centre_ids: List[str], now: datetime):
    """Keep non-faulted centres actively reporting so they do not falsely trip silence detection."""
    now_iso = format_utc_iso(now)
    with get_db_connection(db_path) as conn:
        for cid in centre_ids:
            conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE centre_id = ?;", (now_iso, now_iso, cid))
            conn.execute("INSERT OR REPLACE INTO centre_liveness (centre_id, last_ingested_at, last_event_ts, events_total) VALUES (?, ?, ?, 100);", (cid, now_iso, now_iso))
        conn.commit()


# ---------------------------------------------------------------------------
# TEST 1: Boundaries (23/40 silent => no incident; 24/40 => incident; 25/40 => incident)
# ---------------------------------------------------------------------------
def test_01_boundaries_silent_fraction(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 0, 30, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(now - timedelta(seconds=30))  # Past grace period

    # Keep other centres actively alive so only C-BPL-01 is evaluated
    keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], now)
    set_centre_liveness(fresh_db, "C-BPL-01", format_utc_iso(now))

    # Case A: 23 of 40 silent (23 * 10s old, 17 fresh)
    with get_db_connection(fresh_db) as conn:
        rows = conn.execute("SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-01';").fetchall()
        for i, r in enumerate(rows):
            ing_time = format_utc_iso(now - timedelta(seconds=10 if i < 23 else 1))
            conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE session_id = ?;", (ing_time, ing_time, r["session_id"]))
        conn.commit()

    res = engine.tick(now)
    assert len(res["incidents_opened"]) == 0, "23/40 silent (0.575 < 0.6) must NOT open an incident"

    # Case B: 24 of 40 silent (24 * 10s old, 16 fresh -> 24/40 = 0.60 >= 0.6)
    with get_db_connection(fresh_db) as conn:
        r24 = rows[23]
        old_time = format_utc_iso(now - timedelta(seconds=10))
        conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE session_id = ?;", (old_time, old_time, r24["session_id"]))
        conn.commit()

    res = engine.tick(now)
    assert len(res["incidents_opened"]) == 1, "24/40 silent (0.60 >= 0.6) MUST open an incident"
    assert res["incidents_opened"][0] == "INC-C-BPL-01-1"

    # Case C: 25 of 40 silent on centre 2
    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], now)
    set_centre_liveness(fresh_db, "C-BPL-02", format_utc_iso(now))
    with get_db_connection(fresh_db) as conn:
        rows2 = conn.execute("SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-02';").fetchall()
        for i, r in enumerate(rows2):
            ing_time = format_utc_iso(now - timedelta(seconds=10 if i < 25 else 1))
            conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE session_id = ?;", (ing_time, ing_time, r["session_id"]))
        conn.commit()

    res2 = engine.tick(now)
    assert "INC-C-BPL-02-1" in res2["incidents_opened"], "25/40 silent MUST open an incident"


# ---------------------------------------------------------------------------
# TEST 2: POWER_LOSS with only 1 silent session => incident, power, high confidence
# ---------------------------------------------------------------------------
def test_02_power_loss_explicit_signal(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 5, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(now - timedelta(seconds=30))

    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], now)

    # All sessions active on C-BPL-02 except 1
    with get_db_connection(fresh_db) as conn:
        rows = conn.execute("SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-02';").fetchall()
        for i, r in enumerate(rows):
            ing_time = format_utc_iso(now - timedelta(seconds=10 if i == 0 else 1))
            conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE session_id = ?;", (ing_time, ing_time, r["session_id"]))

        # Ingest explicit POWER_LOSS event
        pl_ts = format_utc_iso(now - timedelta(seconds=5))
        conn.execute(
            """
            INSERT INTO events (event_id, ts, ingested_at, exam_id, centre_id, type, severity, payload)
            VALUES (?, ?, ?, 'EX-2026-PS6-01', 'C-BPL-02', 'POWER_LOSS', 'critical', '{"source":"grid","backup_minutes":60}');
            """,
            (str(uuid.uuid4()), pl_ts, pl_ts),
        )
        conn.commit()

    res = engine.tick(now)
    assert "INC-C-BPL-02-1" in res["incidents_opened"]

    with get_db_connection(fresh_db) as conn:
        inc = conn.execute("SELECT * FROM incidents WHERE incident_id = 'INC-C-BPL-02-1';").fetchone()
        assert inc["type"] == "power"
        assert inc["detection_rule"] == "POWER_LOSS_EVENT"
        ev = json.loads(inc["evidence"])
        assert ev["confidence"] == "high"


# ---------------------------------------------------------------------------
# TEST 3: Silence only => network, low confidence, rule CENTRE_LOSS_FRACTION
# ---------------------------------------------------------------------------
def test_03_silence_only_classification(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 10, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(now - timedelta(seconds=30))

    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-02", "C-BPL-04", "C-BPL-05"], now)
    set_centre_liveness(fresh_db, "C-BPL-03", format_utc_iso(now))

    with get_db_connection(fresh_db) as conn:
        rows = conn.execute("SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-03';").fetchall()
        # 30 of 40 silent
        for i, r in enumerate(rows):
            ing_time = format_utc_iso(now - timedelta(seconds=12 if i < 30 else 1))
            conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE session_id = ?;", (ing_time, ing_time, r["session_id"]))
        conn.commit()

    res = engine.tick(now)
    assert "INC-C-BPL-03-1" in res["incidents_opened"]

    with get_db_connection(fresh_db) as conn:
        inc = conn.execute("SELECT * FROM incidents WHERE incident_id = 'INC-C-BPL-03-1';").fetchone()
        assert inc["type"] == "network"
        assert inc["detection_rule"] == "CENTRE_LOSS_FRACTION"
        ev = json.loads(inc["evidence"])
        assert ev["confidence"] == "low"
        assert ev["rule"] == "CENTRE_LOSS_FRACTION"


# ---------------------------------------------------------------------------
# TEST 4: Dedup: 20 ticks => exactly 1 incident; two centres => 2; new outage after resolution => n+1
# ---------------------------------------------------------------------------
def test_04_dedup_and_n_plus_one_naming(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 15, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(now - timedelta(seconds=30))

    keep_centres_alive(fresh_db, ["C-BPL-03", "C-BPL-04", "C-BPL-05"], now)
    set_centre_liveness(fresh_db, "C-BPL-01", format_utc_iso(now))
    keep_centres_alive(fresh_db, ["C-BPL-02"], now)

    # Outage on Centre 1
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE sessions SET last_ingested_at = ? WHERE centre_id = 'C-BPL-01';", (format_utc_iso(now - timedelta(seconds=15)),))
        conn.commit()

    # Tick 20 times across 20 seconds
    for t_step in range(20):
        t_curr = now + timedelta(seconds=t_step)
        keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_curr)
        engine.tick(t_curr)

    with get_db_connection(fresh_db) as conn:
        c1 = conn.execute("SELECT COUNT(*) AS c FROM incidents WHERE centre_id = 'C-BPL-01';").fetchone()["c"]
        assert c1 == 1, f"20 ticks must create exactly 1 incident, found {c1}"

    # Outage on Centre 2
    t_20 = now + timedelta(seconds=20)
    keep_centres_alive(fresh_db, ["C-BPL-03", "C-BPL-04", "C-BPL-05"], t_20)
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE sessions SET last_ingested_at = ? WHERE centre_id = 'C-BPL-02';", (format_utc_iso(now - timedelta(seconds=15)),))
        conn.commit()

    engine.tick(t_20)
    with get_db_connection(fresh_db) as conn:
        total = conn.execute("SELECT COUNT(*) AS c FROM incidents WHERE status IN ('open', 'recovering');").fetchone()["c"]
        assert total == 2

    # Resolve incident on Centre 1
    t_30 = now + timedelta(seconds=30)
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE incidents SET status = 'resolved', resolved_at = ? WHERE incident_id = 'INC-C-BPL-01-1';", (format_utc_iso(t_30),))
        conn.commit()

    # Next tick triggers second outage for Centre 1 -> INC-C-BPL-01-2
    t_35 = now + timedelta(seconds=35)
    keep_centres_alive(fresh_db, ["C-BPL-03", "C-BPL-04", "C-BPL-05"], t_35)
    engine.tick(t_35)

    with get_db_connection(fresh_db) as conn:
        inc2 = conn.execute("SELECT incident_id FROM incidents WHERE centre_id = 'C-BPL-01' AND status = 'open';").fetchone()
        assert inc2 is not None
        assert inc2["incident_id"] == "INC-C-BPL-01-2", f"Expected INC-C-BPL-01-2, got {inc2['incident_id']}"


# ---------------------------------------------------------------------------
# TEST 5: Startup grace and stall guard
# ---------------------------------------------------------------------------
def test_05_startup_grace_and_stall_guard(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 20, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)

    # Part A: All centres silent with no explicit events and NO fresh liveness
    # Engine started 30s ago, but no batches accepted => ingest_stalled is True
    engine.reset_start_time(now - timedelta(seconds=30))
    res = engine.tick(now)
    assert res["ingest_stalled"] is True
    assert len(res["incidents_opened"]) == 0, "Ingest stall guard must suppress Rule B incidents"

    # Part B: Fresh engine over stale sessions during startup grace (15s)
    # Give fresh liveness so stall guard is False
    set_centre_liveness(fresh_db, "C-BPL-01", format_utc_iso(now))
    engine.reset_start_time(now)  # Started right now, inside grace period

    # Make C-BPL-01 silent
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE sessions SET last_ingested_at = ? WHERE centre_id = 'C-BPL-01';", (format_utc_iso(now - timedelta(seconds=12)),))
        conn.commit()

    # Tick at +5s (grace remaining 10s)
    res_grace = engine.tick(now + timedelta(seconds=5))
    assert res_grace["is_in_grace"] is True
    assert res_grace["grace_remaining_s"] > 0
    assert len(res_grace["incidents_opened"]) == 0, "Startup grace must suppress Rule B incidents"


# ---------------------------------------------------------------------------
# TEST 6: One centre silent while 4 are alive, after grace => that centre gets an incident
# ---------------------------------------------------------------------------
def test_06_one_centre_silent_while_four_alive(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 25, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(now - timedelta(seconds=30))  # Grace elapsed

    # 4 centres alive, 1 centre (C-BPL-04) silent
    alive_centres = ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-05"]
    with get_db_connection(fresh_db) as conn:
        for cid in alive_centres:
            t_fresh = format_utc_iso(now - timedelta(seconds=1))
            conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE centre_id = ?;", (t_fresh, t_fresh, cid))
            conn.execute("INSERT OR REPLACE INTO centre_liveness (centre_id, last_ingested_at, last_event_ts, events_total) VALUES (?, ?, ?, 50);", (cid, t_fresh, t_fresh))

        # C-BPL-04 is completely silent
        t_silent = format_utc_iso(now - timedelta(seconds=15))
        conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE centre_id = 'C-BPL-04';", (t_silent, t_silent))
        conn.commit()

    res = engine.tick(now)
    assert res["ingest_stalled"] is False
    assert res["incidents_opened"] == ["INC-C-BPL-04-1"], "Only the silent centre gets an incident"


# ---------------------------------------------------------------------------
# TEST 7: Late events: silence-only -> ingest backlog -> upgrade to high confidence, settle_s, audit ok
# ---------------------------------------------------------------------------
def test_07_late_events_reclassification_and_settle(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 30, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(now - timedelta(seconds=30))

    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-05"], now)
    set_centre_liveness(fresh_db, "C-BPL-04", format_utc_iso(now))

    # 1. Open silence-only incident
    with get_db_connection(fresh_db) as conn:
        t_silent = format_utc_iso(now - timedelta(seconds=10))
        conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE centre_id = 'C-BPL-04';", (t_silent, t_silent))
        conn.commit()

    engine.tick(now)
    with get_db_connection(fresh_db) as conn:
        inc = conn.execute("SELECT * FROM incidents WHERE incident_id = 'INC-C-BPL-04-1';").fetchone()
        assert inc["status"] == "open"
        ev = json.loads(inc["evidence"])
        assert ev["confidence"] == "low"
        w_start = inc["window_start"]

    # 2. Replay backlog arrives at now + 10s: NETWORK_DOWN and NETWORK_UP
    t_replay = now + timedelta(seconds=10)
    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-05"], t_replay)

    nd_ts = format_utc_iso(parse_utc_iso(w_start) + timedelta(seconds=1))
    nu_ts = format_utc_iso(now + timedelta(seconds=2))
    replayed_ingested_at = format_utc_iso(t_replay)

    with get_db_connection(fresh_db) as conn:
        # Late NETWORK_DOWN
        conn.execute(
            """
            INSERT INTO events (event_id, ts, ingested_at, exam_id, centre_id, type, severity, payload)
            VALUES (?, ?, ?, 'EX-2026-PS6-01', 'C-BPL-04', 'NETWORK_DOWN', 'critical', '{"duration_s":12}');
            """,
            (str(uuid.uuid4()), nd_ts, replayed_ingested_at),
        )
        # Late NETWORK_UP
        conn.execute(
            """
            INSERT INTO events (event_id, ts, ingested_at, exam_id, centre_id, type, severity, payload)
            VALUES (?, ?, ?, 'EX-2026-PS6-01', 'C-BPL-04', 'NETWORK_UP', 'info', '{"duration_s":12}');
            """,
            (str(uuid.uuid4()), nu_ts, replayed_ingested_at),
        )
        # Replayed candidate heartbeats with ts inside [nd_ts, nu_ts]
        conn.execute(
            """
            INSERT INTO events (event_id, ts, ingested_at, exam_id, centre_id, candidate_id, session_id, type, severity, payload)
            VALUES (?, ?, ?, 'EX-2026-PS6-01', 'C-BPL-04', 'CAND-000001', 'SES-000001-1', 'HEARTBEAT', 'info', '{}');
            """,
            (str(uuid.uuid4()), nd_ts, replayed_ingested_at),
        )
        # Post-restore heartbeats to resume sessions
        post_nu_hb = format_utc_iso(now + timedelta(seconds=3))
        conn.execute(
            "UPDATE sessions SET last_heartbeat_at = ?, last_ingested_at = ? WHERE centre_id = 'C-BPL-04';",
            (post_nu_hb, replayed_ingested_at),
        )
        conn.commit()

    # 3. Next tick at t_replay + 1s: upgrades to high confidence and enters recovering
    t_rep1 = t_replay + timedelta(seconds=1)
    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-05"], t_rep1)
    engine.tick(t_rep1)

    with get_db_connection(fresh_db) as conn:
        inc = conn.execute("SELECT * FROM incidents WHERE incident_id = 'INC-C-BPL-04-1';").fetchone()
        assert inc["status"] == "recovering"
        ev = json.loads(inc["evidence"])
        assert ev["confidence"] == "high"
        assert ev["reclassified_from"]["confidence"] == "low"
        assert ev["late_events"] >= 1

    # 4. Settle test: settle_s = 5.0. At t_replay + 2s, settle is NOT met (only 2s elapsed)
    t_rep2 = t_replay + timedelta(seconds=2)
    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-05"], t_rep2)
    engine.tick(t_rep2)

    with get_db_connection(fresh_db) as conn:
        assert conn.execute("SELECT status FROM incidents WHERE incident_id = 'INC-C-BPL-04-1';").fetchone()["status"] == "recovering"

    # At t_replay + 6s (6s > 5s settle), incident resolves!
    t_rep6 = t_replay + timedelta(seconds=6)
    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-05"], t_rep6)
    engine.tick(t_rep6)

    with get_db_connection(fresh_db) as conn:
        inc_res = conn.execute("SELECT status, resolved_at FROM incidents WHERE incident_id = 'INC-C-BPL-04-1';").fetchone()
        assert inc_res["status"] == "resolved"
        assert inc_res["resolved_at"] is not None

    audit_res = verify_audit_chain(fresh_db)
    assert audit_res.ok is True


# ---------------------------------------------------------------------------
# TEST 8: Escalation: fake clock +121 s => critical exactly once across 10 more ticks
# ---------------------------------------------------------------------------
def test_08_escalation_to_critical(fresh_db: str):
    t0 = datetime(2026, 9, 29, 10, 40, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(t0 - timedelta(seconds=30))

    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t0)
    set_centre_liveness(fresh_db, "C-BPL-02", format_utc_iso(t0))

    # Trigger incident on C-BPL-02
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE sessions SET last_ingested_at = ? WHERE centre_id = 'C-BPL-02';", (format_utc_iso(t0 - timedelta(seconds=10)),))
        conn.commit()

    engine.tick(t0)
    with get_db_connection(fresh_db) as conn:
        inc = conn.execute("SELECT severity FROM incidents WHERE incident_id = 'INC-C-BPL-02-1';").fetchone()
        assert inc["severity"] == "high"

    # Advance +121 seconds
    t_esc = t0 + timedelta(seconds=121)
    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_esc)
    engine.tick(t_esc)

    with get_db_connection(fresh_db) as conn:
        inc = conn.execute("SELECT severity FROM incidents WHERE incident_id = 'INC-C-BPL-02-1';").fetchone()
        assert inc["severity"] == "critical"

    # Tick 10 more times
    for s in range(1, 11):
        t_step = t_esc + timedelta(seconds=s)
        keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_step)
        engine.tick(t_step)

    with get_db_connection(fresh_db) as conn:
        # Check timeline: escalated appears exactly ONCE
        esc_timeline = conn.execute("SELECT COUNT(*) AS c FROM incident_timeline WHERE incident_id = 'INC-C-BPL-02-1' AND kind = 'escalated';").fetchone()["c"]
        assert esc_timeline == 1, f"Escalated timeline must appear exactly once, got {esc_timeline}"


# ---------------------------------------------------------------------------
# TEST 9: Session states: interrupted after opening, resumed after newer heartbeat
# ---------------------------------------------------------------------------
def test_09_session_states_interrupted_and_resumed(fresh_db: str):
    now = datetime(2026, 9, 29, 10, 50, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(now - timedelta(seconds=30))

    keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], now)
    set_centre_liveness(fresh_db, "C-BPL-01", format_utc_iso(now))

    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE sessions SET last_ingested_at = ? WHERE centre_id = 'C-BPL-01';", (format_utc_iso(now - timedelta(seconds=10)),))
        conn.commit()

    engine.tick(now)

    # Verify all silent sessions set to 'interrupted'
    with get_db_connection(fresh_db) as conn:
        intr_count = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE centre_id = 'C-BPL-01' AND state = 'interrupted';").fetchone()["c"]
        assert intr_count == 40

    # Set window_end and simulate newer heartbeats
    we_iso = format_utc_iso(now + timedelta(seconds=10))
    newer_hb = format_utc_iso(now + timedelta(seconds=15))
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE incidents SET window_end = ? WHERE incident_id = 'INC-C-BPL-01-1';", (we_iso,))
        conn.execute(
            """
            UPDATE sessions SET last_heartbeat_at = ?
            WHERE session_id IN (SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-01' LIMIT 20);
            """,
            (newer_hb,),
        )
        conn.commit()

    t_resume = now + timedelta(seconds=20)
    keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_resume)
    engine.tick(t_resume)

    with get_db_connection(fresh_db) as conn:
        res_count = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE centre_id = 'C-BPL-01' AND state = 'resumed';").fetchone()["c"]
        assert res_count == 20, f"Expected 20 resumed sessions, found {res_count}"


# ---------------------------------------------------------------------------
# TEST 10: Flaky noise: 200 ticks with 10% of sessions skipping 30% of heartbeats => 0 incidents
# ---------------------------------------------------------------------------
def test_10_flaky_noise_seeded_zero_incidents(fresh_db: str):
    rng = random.Random(42)
    start_t = datetime(2026, 9, 29, 11, 0, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(start_t - timedelta(seconds=30))

    # Centre 1: 40 candidates. 4 candidates (10%) are flaky and skip 30% of heartbeats
    flaky_indices = set(range(4))

    for step in range(200):
        curr_t = start_t + timedelta(seconds=step)
        curr_iso = format_utc_iso(curr_t)

        with get_db_connection(fresh_db) as conn:
            # Centre liveness update for all centres
            for cid in ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"]:
                conn.execute("INSERT OR REPLACE INTO centre_liveness (centre_id, last_ingested_at, last_event_ts, events_total) VALUES (?, ?, ?, 100);", (cid, curr_iso, curr_iso))
                if cid != "C-BPL-01":
                    conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE centre_id = ?;", (curr_iso, curr_iso, cid))

            # Update C-BPL-01 with flaky candidates
            rows = conn.execute("SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-01';").fetchall()
            for idx, r in enumerate(rows):
                if idx in flaky_indices and rng.random() < 0.30:
                    continue
                conn.execute("UPDATE sessions SET last_ingested_at = ?, last_heartbeat_at = ? WHERE session_id = ?;", (curr_iso, curr_iso, r["session_id"]))
            conn.commit()

        res = engine.tick(curr_t)
        assert len(res["incidents_opened"]) == 0, f"Flaky noise should not open incident at tick {step}"

    with get_db_connection(fresh_db) as conn:
        total_inc = conn.execute("SELECT COUNT(*) AS c FROM incidents WHERE centre_id = 'C-BPL-01';").fetchone()["c"]
        assert total_inc == 0


# ---------------------------------------------------------------------------
# TEST 11: Audit: number of "incident" audit entries equals real transitions; verify() ok
# ---------------------------------------------------------------------------
def test_11_audit_entries_real_transitions_only(fresh_db: str):
    t0 = datetime(2026, 9, 29, 12, 0, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(t0 - timedelta(seconds=30))

    keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t0)
    set_centre_liveness(fresh_db, "C-BPL-01", format_utc_iso(t0))

    # 1. Opened
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE sessions SET last_ingested_at = ? WHERE centre_id = 'C-BPL-01';", (format_utc_iso(t0 - timedelta(seconds=10)),))
        conn.commit()
    engine.tick(t0)

    # 2. Escalated (+121s)
    t_121 = t0 + timedelta(seconds=121)
    keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_121)
    engine.tick(t_121)

    # 3. Recovering (POWER_RESTORED arrives)
    pr_ts = format_utc_iso(t0 + timedelta(seconds=130))
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE incidents SET type = 'power' WHERE incident_id = 'INC-C-BPL-01-1';")
        conn.execute(
            """
            INSERT INTO events (event_id, ts, ingested_at, exam_id, centre_id, type, severity, payload)
            VALUES (?, ?, ?, 'EX-2026-PS6-01', 'C-BPL-01', 'POWER_RESTORED', 'info', '{}');
            """,
            (str(uuid.uuid4()), pr_ts, pr_ts),
        )
        conn.commit()

    t_131 = t0 + timedelta(seconds=131)
    keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_131)
    engine.tick(t_131)

    # 4. Resolved (sessions resume)
    res_hb = format_utc_iso(t0 + timedelta(seconds=135))
    with get_db_connection(fresh_db) as conn:
        conn.execute("UPDATE sessions SET last_heartbeat_at = ? WHERE centre_id = 'C-BPL-01';", (res_hb,))
        conn.commit()

    t_136 = t0 + timedelta(seconds=136)
    keep_centres_alive(fresh_db, ["C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_136)
    engine.tick(t_136)

    # 5. Run 50 more ticks without changes (all centres normal post-recovery)
    for s in range(50):
        t_more = t0 + timedelta(seconds=140 + s)
        keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_more)
        engine.tick(t_more)

    # Check audit entries count for entry_type = 'incident' for INC-C-BPL-01-1
    with get_db_connection(fresh_db) as conn:
        inc_audit_rows = conn.execute("SELECT seq, payload FROM audit_log WHERE entry_type = 'incident' AND ref_id = 'INC-C-BPL-01-1';").fetchall()
        assert len(inc_audit_rows) == 5, f"Expected 5 audit entries (opened, escalated, recovering, resolved, impact_computed), got {len(inc_audit_rows)}"
        actions = [json.loads(r["payload"])["action"] for r in inc_audit_rows]
        assert actions == ["opened", "escalated", "recovering", "resolved", "impact_computed"]

    audit_res = verify_audit_chain(fresh_db)
    assert audit_res.ok is True


# ---------------------------------------------------------------------------
# Helper for slow real-process tests (Test 12 & 13)
# ---------------------------------------------------------------------------
def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(url: str, timeout: float = 6.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = httpx.get(f"{url}/v1/health", timeout=0.5)
            if r.status_code == 200:
                return True
        except Exception:
            time.sleep(0.1)
    return False


@pytest.mark.slow
def test_12_and_13_restart_safety_and_detection_latency(tmp_path: Path):
    """Real processes: open incident, kill API, restart on same DB, verify incident continued, latency <= 10s."""
    port = find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    db_file = tmp_path / "restart_safety.db"
    cfg_file = tmp_path / "restart_cfg.yaml"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    init_db(str(db_file))
    seed_database(str(db_file))

    # Start API process 1
    env = dict(os.environ)
    env["ERCT_CONFIG_PATH"] = str(cfg_file)

    p1 = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        assert wait_for_server(base_url, timeout=8.0), "API process 1 failed to start"

        # Cause power outage on C-BPL-02 via POST /v1/events
        pl_time = datetime.now(timezone.utc)
        pl_ev = {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": format_utc_iso(pl_time),
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-02",
            "seq": 1,
            "type": "POWER_LOSS",
            "severity": "critical",
            "payload": {"source": "grid", "backup_minutes": 60},
        }
        r = httpx.post(f"{base_url}/v1/events", json=pl_ev, headers={"X-API-Key": "key-cbpl02-secret"})
        assert r.status_code == 200

        # Wait for detection worker to tick and create incident
        time.sleep(2.0)

        # Query GET /v1/incidents
        r_inc = httpx.get(f"{base_url}/v1/incidents?centre_id=C-BPL-02")
        assert r_inc.status_code == 200
        incs = r_inc.json()
        assert len(incs) == 1
        orig_id = incs[0]["incident_id"]
        assert orig_id == "INC-C-BPL-02-1"

        # TEST 13 check: detection latency <= 10s
        detected_dt = parse_utc_iso(incs[0]["detected_at"])
        window_start_dt = parse_utc_iso(incs[0]["window_start"])
        latency_s = (detected_dt - window_start_dt).total_seconds()
        assert latency_s <= 10.0, f"Detection latency {latency_s:.2f}s exceeded 10.0s threshold"

        # Kill API process 1
        p1.terminate()
        try:
            p1.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            p1.kill()

        time.sleep(1.0)

        # Start API process 2 on the SAME database
        p2 = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            assert wait_for_server(base_url, timeout=8.0), "API process 2 failed to start on same DB"
            time.sleep(2.0)

            # Query incidents again: must be still exactly 1 incident with same id
            r_inc2 = httpx.get(f"{base_url}/v1/incidents?centre_id=C-BPL-02")
            assert r_inc2.status_code == 200
            incs2 = r_inc2.json()
            assert len(incs2) == 1, f"Expected exactly 1 incident after restart, found {len(incs2)}"
            assert incs2[0]["incident_id"] == orig_id
            assert incs2[0]["status"] in ("open", "recovering")

        finally:
            p2.terminate()
            try:
                p2.wait(timeout=3.0)
            except Exception:
                p2.kill()

    finally:
        if p1.poll() is None:
            p1.terminate()
            try:
                p1.wait(timeout=3.0)
            except Exception:
                p1.kill()


# ---------------------------------------------------------------------------
# TEST 14: Session state timeline (interrupted on Rule A, resumed after window_end, under_review/submitted immutable)
# ---------------------------------------------------------------------------
def test_14_session_state_timeline_and_guards(fresh_db: str):
    t0 = datetime(2026, 9, 29, 12, 0, 0, tzinfo=timezone.utc)
    engine = DetectionEngine(fresh_db)
    engine.reset_start_time(t0 - timedelta(seconds=60))

    # Pick two sessions in C-BPL-02 to mark as 'under_review' and 'submitted'
    with get_db_connection(fresh_db) as conn:
        sessions = conn.execute("SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-02' ORDER BY session_id;").fetchall()
        s_review = sessions[0]["session_id"]
        s_submitted = sessions[1]["session_id"]
        conn.execute("UPDATE sessions SET state = 'under_review' WHERE session_id = ?;", (s_review,))
        conn.execute("UPDATE sessions SET state = 'submitted' WHERE session_id = ?;", (s_submitted,))
        conn.commit()

    # Insert explicit POWER_LOSS event for C-BPL-02
    ev_pl = {
        "event_id": "ev-pl-test14",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-02",
        "seq": 100,
        "ts": format_utc_iso(t0),
        "type": "POWER_LOSS",
        "severity": "critical",
        "payload": json.dumps({"source": "grid", "backup_minutes": 0}),
        "ingested_at": format_utc_iso(t0),
    }
    with get_db_connection(fresh_db) as conn:
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, seq, ts, type, severity, payload, ingested_at)
            VALUES (:event_id, :exam_id, :centre_id, :seq, :ts, :type, :severity, :payload, :ingested_at);
            """,
            ev_pl,
        )
        conn.commit()

    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t0)
    engine.tick(t0)

    # Check session states: 38 must be 'interrupted', 1 'under_review', 1 'submitted'
    with get_db_connection(fresh_db) as conn:
        intr_count = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE centre_id = 'C-BPL-02' AND state = 'interrupted';").fetchone()["c"]
        rev_count = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE session_id = ? AND state = 'under_review';", (s_review,)).fetchone()["c"]
        sub_count = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE session_id = ? AND state = 'submitted';", (s_submitted,)).fetchone()["c"]
        assert intr_count == 38, f"Expected 38 interrupted sessions, found {intr_count}"
        assert rev_count == 1, "Session with 'under_review' must not be touched"
        assert sub_count == 1, "Session with 'submitted' must not be touched"

        inc = conn.execute("SELECT incident_id, window_start, window_end FROM incidents WHERE centre_id = 'C-BPL-02';").fetchone()
        inc_id = inc["incident_id"]

    # Now simulate recovery: insert POWER_RESTORED and set window_end
    t_res = t0 + timedelta(seconds=60)
    ev_pr = {
        "event_id": "ev-pr-test14",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-02",
        "seq": 101,
        "ts": format_utc_iso(t_res),
        "type": "POWER_RESTORED",
        "severity": "info",
        "payload": json.dumps({"source": "grid"}),
        "ingested_at": format_utc_iso(t_res),
    }
    with get_db_connection(fresh_db) as conn:
        conn.execute(
            """
            INSERT INTO events (event_id, exam_id, centre_id, seq, ts, type, severity, payload, ingested_at)
            VALUES (:event_id, :exam_id, :centre_id, :seq, :ts, :type, :severity, :payload, :ingested_at);
            """,
            ev_pr,
        )
        conn.commit()

    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_res)
    engine.tick(t_res)

    # Now send heartbeats for 20 interrupted sessions with ts > window_end
    t_hb = t_res + timedelta(seconds=10)
    with get_db_connection(fresh_db) as conn:
        conn.execute(
            f"""
            UPDATE sessions SET last_heartbeat_at = '{format_utc_iso(t_hb)}'
            WHERE session_id IN (SELECT session_id FROM sessions WHERE centre_id = 'C-BPL-02' AND state = 'interrupted' LIMIT 20);
            """
        )
        conn.commit()

    keep_centres_alive(fresh_db, ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"], t_hb)
    set_centre_liveness(fresh_db, "C-BPL-02", format_utc_iso(t_hb))
    engine.tick(t_hb)

    with get_db_connection(fresh_db) as conn:
        res_count = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE centre_id = 'C-BPL-02' AND state = 'resumed';").fetchone()["c"]
        still_intr = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE centre_id = 'C-BPL-02' AND state = 'interrupted';").fetchone()["c"]
        rev_count2 = conn.execute("SELECT COUNT(*) AS c FROM sessions WHERE session_id = ? AND state = 'under_review';", (s_review,)).fetchone()["c"]
        assert res_count == 20, f"Expected 20 resumed sessions, found {res_count}"
        assert still_intr == 18, f"Expected 18 still interrupted, found {still_intr}"
        assert rev_count2 == 1, "'under_review' must still remain untouched"

