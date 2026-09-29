"""Tests for monotonic session state and server-side centre liveness tracking."""
from __future__ import annotations

import socket
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List
import httpx
import pytest
import uvicorn

from app.db import format_utc_iso, get_db_connection, init_db
from app.main import app, seed_database


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


@pytest.fixture
def running_server(tmp_path: Path):
    port = find_free_port()
    db_file = tmp_path / "monotonic_test.db"
    base_url = f"http://127.0.0.1:{port}"

    import os
    import yaml
    from app.config import reload_config

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

    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="warning", ws="none")
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    assert wait_for_server(base_url), "Server failed to start"

    try:
        yield {"base_url": base_url, "db_path": str(db_file)}
    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


def test_newer_then_older_batch_preserves_newest_session_state(running_server: Dict[str, Any]):
    """Test 2a: Send a newer batch, then an older backlog batch: session state stays at the newest."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    # Base timestamps: t_newer is +10s after t_older
    t_base = datetime(2026, 9, 29, 10, 0, 0, tzinfo=timezone.utc)
    t_older_iso = format_utc_iso(t_base)
    t_newer_iso = format_utc_iso(t_base + timedelta(seconds=10))

    session_id = "SES-000001-1"

    # Step 1: Send the NEWER batch first (remaining_s = 7000, seq = 10, answer saved_seq = 5)
    newer_events = [
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": t_newer_iso,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": session_id,
            "seq": 10,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 20, "remaining_s": 7000, "local_seq": 5},
        },
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": t_newer_iso,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": session_id,
            "seq": 11,
            "type": "ANSWER_SAVED",
            "payload": {"question_id": "Q05", "saved_seq": 5, "answer_hash": "abc1234"},
        },
    ]

    r1 = httpx.post(f"{base_url}/v1/events", json=newer_events, headers=headers)
    assert r1.status_code == 200
    assert r1.json()["accepted"] == 2

    # Verify session reflects newer state
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["last_heartbeat_at"] == t_newer_iso
        assert row["remaining_s"] == 7000
        assert row["last_saved_seq"] == 5
        assert row["answers_saved"] == 1

    # Step 2: Now send an OLDER batch that was delayed in a backlog (remaining_s = 7010, seq = 2, saved_seq = 1)
    older_events = [
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": t_older_iso,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": session_id,
            "seq": 2,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 20, "remaining_s": 7010, "local_seq": 1},
        },
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": t_older_iso,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": session_id,
            "seq": 3,
            "type": "ANSWER_SAVED",
            "payload": {"question_id": "Q01", "saved_seq": 1, "answer_hash": "q01hash"},
        },
    ]

    r2 = httpx.post(f"{base_url}/v1/events", json=older_events, headers=headers)
    assert r2.status_code == 200
    assert r2.json()["accepted"] == 2

    # Step 3: Assert session state STILL reflects the newer values and did not move backwards
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["last_heartbeat_at"] == t_newer_iso, "last_heartbeat_at moved backwards!"
        assert row["remaining_s"] == 7000, "remaining_s moved backwards!"
        assert row["last_saved_seq"] == 5, "last_saved_seq moved backwards!"
        # answers_saved increased from 1 to 2 because Q01 was a newly inserted answer
        assert row["answers_saved"] == 2


def test_replaying_batch_does_not_increment_answers_saved(running_server: Dict[str, Any]):
    """Test 2b: Replaying the same batch ignores duplicates and leaves answers_saved unchanged."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    session_id = "SES-000002-1"
    now_iso = format_utc_iso()

    events = [
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": now_iso,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000002",
            "session_id": session_id,
            "seq": 1,
            "type": "ANSWER_SAVED",
            "payload": {"question_id": "Q01", "saved_seq": 1, "answer_hash": "hash_a"},
        },
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": now_iso,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000002",
            "session_id": session_id,
            "seq": 2,
            "type": "ANSWER_SAVED",
            "payload": {"question_id": "Q02", "saved_seq": 2, "answer_hash": "hash_b"},
        },
    ]

    # Initial ingestion
    r1 = httpx.post(f"{base_url}/v1/events", json=events, headers=headers)
    assert r1.status_code == 200
    assert r1.json()["accepted"] == 2

    with get_db_connection(db_path) as conn:
        row1 = conn.execute("SELECT answers_saved FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row1["answers_saved"] == 2

    # Replay identical batch
    r2 = httpx.post(f"{base_url}/v1/events", json=events, headers=headers)
    assert r2.status_code == 200
    assert r2.json()["duplicates"] == 2
    assert r2.json()["accepted"] == 0

    # answers_saved must be strictly unchanged
    with get_db_connection(db_path) as conn:
        row2 = conn.execute("SELECT answers_saved FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row2["answers_saved"] == 2, f"Expected 2 answers_saved, got {row2['answers_saved']}"


def test_last_ingested_at_is_server_time_not_event_ts(running_server: Dict[str, Any]):
    """Test 2c: last_ingested_at reflects real server ingestion time, not client event ts."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    session_id = "SES-000003-1"
    # Event ts is in the past (e.g. 5 minutes ago)
    old_event_ts = format_utc_iso(datetime.now(timezone.utc) - timedelta(minutes=5))
    t_before_req = format_utc_iso(datetime.now(timezone.utc) - timedelta(seconds=1))

    event = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": old_event_ts,
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "candidate_id": "CAND-000003",
        "session_id": session_id,
        "seq": 1,
        "type": "HEARTBEAT",
        "payload": {"latency_ms": 25, "remaining_s": 6900, "local_seq": 1},
    }

    r = httpx.post(f"{base_url}/v1/events", json=[event], headers=headers)
    assert r.status_code == 200

    t_after_req = format_utc_iso(datetime.now(timezone.utc) + timedelta(seconds=1))

    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT last_ingested_at, last_heartbeat_at FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["last_ingested_at"] is not None
        # last_ingested_at must be within [t_before_req, t_after_req] (server reception time), NOT old_event_ts!
        assert row["last_ingested_at"] != old_event_ts
        assert t_before_req <= row["last_ingested_at"] <= t_after_req


def test_centre_liveness_updated_per_batch(running_server: Dict[str, Any]):
    """Test 2d: centre_liveness table is updated once per accepted batch in the same transaction."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    now = datetime.now(timezone.utc)
    ts1 = format_utc_iso(now - timedelta(seconds=2))
    ts2 = format_utc_iso(now)

    batch1 = [
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": ts1,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": "SES-000001-1",
            "seq": 100,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 30, "remaining_s": 6500, "local_seq": 1},
        },
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": ts2,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000002",
            "session_id": "SES-000002-1",
            "seq": 100,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 30, "remaining_s": 6500, "local_seq": 1},
        },
    ]

    r1 = httpx.post(f"{base_url}/v1/events", json=batch1, headers=headers)
    assert r1.status_code == 200

    with get_db_connection(db_path) as conn:
        live_row = conn.execute("SELECT * FROM centre_liveness WHERE centre_id = 'C-BPL-01';").fetchone()
        assert live_row is not None
        assert live_row["events_total"] == 2
        assert live_row["last_event_ts"] == ts2
        assert live_row["last_ingested_at"] is not None

    # Post 3 more events
    ts3 = format_utc_iso(now + timedelta(seconds=4))
    batch2 = [
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": ts3,
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "seq": i,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 20, "remaining_s": 6400, "local_seq": 1},
        }
        for i in range(200, 203)
    ]
    r2 = httpx.post(f"{base_url}/v1/events", json=batch2, headers=headers)
    assert r2.status_code == 200

    with get_db_connection(db_path) as conn:
        live_row2 = conn.execute("SELECT * FROM centre_liveness WHERE centre_id = 'C-BPL-01';").fetchone()
        assert live_row2["events_total"] == 5
        assert live_row2["last_event_ts"] == ts3


def test_last_saved_seq_independent_of_ts(running_server: Dict[str, Any]):
    """Rule 1b-2: last_saved_seq must use MAX(old, new) independent of ts."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    session_id = "SES-000001-1"
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    # Seed session with last_saved_seq = 5, last_heartbeat_at = 10:05:00
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE sessions
            SET last_saved_seq = 5,
                last_heartbeat_at = '2026-09-29T10:05:00.000000+00:00',
                answers_saved = 5
            WHERE session_id = ?;
            """,
            (session_id,),
        )
        conn.commit()

    # Send an ANSWER_SAVED with an older timestamp (10:01:00) but higher saved_seq (12)
    ev = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": "2026-09-29T10:01:00.000000+00:00",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "candidate_id": "CAND-000001",
        "session_id": session_id,
        "seq": 10,
        "type": "ANSWER_SAVED",
        "payload": {
            "question_id": "Q-SEQ-TEST",
            "answer_hash": "a1b2c3d4",
            "saved_seq": 12,
        },
    }

    r = httpx.post(f"{base_url}/v1/events", json=ev, headers=headers)
    assert r.status_code == 200

    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT last_saved_seq, answers_saved FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["last_saved_seq"] == 12, f"Expected last_saved_seq=12, got {row['last_saved_seq']}"
        assert row["answers_saved"] == 6


def test_last_heartbeat_at_isolated_from_answer_saved(running_server: Dict[str, Any]):
    """Rule 1b-3: last_heartbeat_at may only be set by HEARTBEAT or SESSION_STARTED events."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    session_id = "SES-000001-1"
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    hb_ts = "2026-09-29T10:05:00.000000+00:00"
    with get_db_connection(db_path) as conn:
        conn.execute(
            "UPDATE sessions SET last_heartbeat_at = ? WHERE session_id = ?;",
            (hb_ts, session_id),
        )
        conn.commit()

    # Send ANSWER_SAVED with much newer timestamp (10:15:00)
    ev_ans = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": "2026-09-29T10:15:00.000000+00:00",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "candidate_id": "CAND-000001",
        "session_id": session_id,
        "seq": 20,
        "type": "ANSWER_SAVED",
        "payload": {
            "question_id": "Q-HB-TEST",
            "answer_hash": "a1b2c3d4",
            "saved_seq": 15,
        },
    }
    r = httpx.post(f"{base_url}/v1/events", json=ev_ans, headers=headers)
    assert r.status_code == 200

    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT last_heartbeat_at FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["last_heartbeat_at"] == hb_ts, "ANSWER_SAVED must NOT update last_heartbeat_at"


def test_ingest_path_never_moves_out_of_interrupted_or_under_review(running_server: Dict[str, Any]):
    """Rule 1b-4: The ingest path must never move a session out of 'interrupted' or 'under_review'."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    for protected_state in ("interrupted", "under_review"):
        session_id = f"SES-000001-1"
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE sessions SET state = ? WHERE session_id = ?;", (protected_state, session_id))
            conn.commit()

        # Try HEARTBEAT
        ev_hb = {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": "2026-09-29T10:20:00.000000+00:00",
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": session_id,
            "seq": 30,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 15, "remaining_s": 5000, "local_seq": 1},
        }
        httpx.post(f"{base_url}/v1/events", json=ev_hb, headers=headers)

        with get_db_connection(db_path) as conn:
            row = conn.execute("SELECT state FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
            assert row["state"] == protected_state, f"HEARTBEAT moved session out of {protected_state}"

        # Try SESSION_STARTED
        ev_start = {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": "2026-09-29T10:21:00.000000+00:00",
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": session_id,
            "seq": 31,
            "type": "SESSION_STARTED",
            "payload": {"duration_s": 7200},
        }
        httpx.post(f"{base_url}/v1/events", json=ev_start, headers=headers)

        with get_db_connection(db_path) as conn:
            row = conn.execute("SELECT state FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
            assert row["state"] == protected_state, f"SESSION_STARTED moved session out of {protected_state}"

        # Try SESSION_SUBMITTED
        ev_sub = {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": "2026-09-29T10:22:00.000000+00:00",
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "candidate_id": "CAND-000001",
            "session_id": session_id,
            "seq": 32,
            "type": "SESSION_SUBMITTED",
            "payload": {},
        }
        httpx.post(f"{base_url}/v1/events", json=ev_sub, headers=headers)

        with get_db_connection(db_path) as conn:
            row = conn.execute("SELECT state FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
            assert row["state"] == protected_state, f"SESSION_SUBMITTED moved session out of {protected_state}"


def test_timestamp_canonical_order_and_naive_rejection(running_server: Dict[str, Any]):
    """Step 1c: Rejects naive timestamp with 422; normalizes 'Z' and fractional offsets so real time order holds."""
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    session_id = "SES-000001-1"
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    # 1. Naive timestamp must be rejected with 422
    ev_naive = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": "2026-09-29T10:00:00",  # No timezone offset
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "candidate_id": "CAND-000001",
        "session_id": session_id,
        "seq": 40,
        "type": "HEARTBEAT",
        "payload": {"latency_ms": 20, "remaining_s": 7000, "local_seq": 1},
    }
    r_naive = httpx.post(f"{base_url}/v1/events", json=ev_naive, headers=headers)
    assert r_naive.status_code == 422, f"Expected 422 for naive timestamp, got {r_naive.status_code}"

    # 2. Text vs Time order test:
    # "2026-09-29T10:00:00.5+00:00" is at 10:00:00.500
    # "2026-09-29T10:00:00Z" is at 10:00:00.000 (earlier in time, but in raw text 'Z' > '.')
    # Send the later event first (10:00:00.5):
    ev_later = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": "2026-09-29T10:00:00.5+00:00",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "candidate_id": "CAND-000001",
        "session_id": session_id,
        "seq": 41,
        "type": "HEARTBEAT",
        "payload": {"latency_ms": 20, "remaining_s": 6995, "local_seq": 1},
    }
    r1 = httpx.post(f"{base_url}/v1/events", json=ev_later, headers=headers)
    assert r1.status_code == 200

    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT last_heartbeat_at, remaining_s FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["last_heartbeat_at"] == "2026-09-29T10:00:00.500000+00:00"
        assert row["remaining_s"] == 6995

    # Now send the chronologically earlier event with raw "Z"
    ev_earlier = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": "2026-09-29T10:00:00Z",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "candidate_id": "CAND-000001",
        "session_id": session_id,
        "seq": 42,
        "type": "HEARTBEAT",
        "payload": {"latency_ms": 20, "remaining_s": 7000, "local_seq": 1},
    }
    r2 = httpx.post(f"{base_url}/v1/events", json=ev_earlier, headers=headers)
    assert r2.status_code == 200

    # Monotonicity check: the earlier event must NOT overwrite the newer last_heartbeat_at or remaining_s
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT last_heartbeat_at, remaining_s FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["last_heartbeat_at"] == "2026-09-29T10:00:00.500000+00:00", (
            f"Expected 10:00:00.500000+00:00 preserved, got {row['last_heartbeat_at']}"
        )
        assert row["remaining_s"] == 6995


# ---------------------------------------------------------------------------
# TEST 9: STEP 1.4 - SESSION started_at invariant rules
# ---------------------------------------------------------------------------
def test_session_started_at_rules(running_server):
    """STEP 1.4:
    - a session that has not started is not exposed and not in D
    - started_at equals the event ts
    - replaying SESSION_STARTED does not change it (keeps earliest)
    """
    base_url = running_server["base_url"]
    db_path = running_server["db_path"]
    headers = {"X-API-Key": "key-cbpl03-secret", "Content-Type": "application/json"}
    session_id = "SES-000099-1"
    candidate_id = "CAND-000099"
    centre_id = "C-BPL-03"

    # 1. Unstarted session: seed leaves started_at NULL and state 'registered'
    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT started_at, state FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["started_at"] is None, f"Seeded session started_at must be NULL, got {row['started_at']}"
        assert row["state"] == "registered", f"Seeded session state must be registered, got {row['state']}"

    # Verify not in Detection domain D
    from app.core.detection import DetectionEngine
    from app.core.impact import compute_incident_impact
    engine = DetectionEngine(db_path)
    now_dt = datetime(2026, 9, 29, 10, 0, 0, tzinfo=timezone.utc)
    engine.reset_start_time(now_dt - timedelta(seconds=60))

    with get_db_connection(db_path) as conn:
        active_in_d = conn.execute(
            "SELECT session_id FROM sessions WHERE centre_id = ? AND started_at IS NOT NULL AND state IN ('active', 'resumed', 'interrupted');",
            (centre_id,),
        ).fetchall()
        assert session_id not in [r["session_id"] for r in active_in_d], "Unstarted session must not be in detection domain D"

    # Verify not exposed in impact computation
    inc_id = "INC-TEST-UNSTARTED-1"
    ws = "2026-09-29T10:00:30+00:00"
    we = "2026-09-29T10:01:00+00:00"
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO incidents (incident_id, exam_id, centre_id, type, severity, status, detected_at, window_start, window_end, resolved_at)
            VALUES (?, 'EX-2026-PS6-01', ?, 'power', 'high', 'resolved', ?, ?, ?, ?);
            """,
            (inc_id, centre_id, ws, ws, we, we),
        )
        compute_incident_impact(conn, inc_id, we)
        impact_cands = [r["candidate_id"] for r in conn.execute("SELECT candidate_id FROM incident_impacts WHERE incident_id = ?;", (inc_id,)).fetchall()]
        assert candidate_id not in impact_cands, "Unstarted session must NOT be in exposed impact rows"

    # 2. Ingest SESSION_STARTED event at T1
    t1_iso = "2026-09-29T10:00:15.123456+00:00"
    ev_start = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": t1_iso,
        "exam_id": "EX-2026-PS6-01",
        "centre_id": centre_id,
        "candidate_id": candidate_id,
        "session_id": session_id,
        "seq": 1,
        "type": "SESSION_STARTED",
        "severity": "info",
        "payload": {"duration_s": 7200},
    }
    r = httpx.post(f"{base_url}/v1/events", json=ev_start, headers=headers)
    assert r.status_code == 200

    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT started_at, state FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["started_at"] == t1_iso, f"started_at must equal event ts {t1_iso}, got {row['started_at']}"
        assert row["state"] == "active"

        # Now in domain D
        active_in_d = conn.execute(
            "SELECT session_id FROM sessions WHERE centre_id = ? AND started_at IS NOT NULL AND state IN ('active', 'resumed', 'interrupted');",
            (centre_id,),
        ).fetchall()
        assert session_id in [r["session_id"] for r in active_in_d], "Started session must now be in detection domain D"

    # 3. Replay SESSION_STARTED with a later ts (T2 > T1)
    t2_iso = "2026-09-29T10:00:25.999999+00:00"
    ev_replay = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": t2_iso,
        "exam_id": "EX-2026-PS6-01",
        "centre_id": centre_id,
        "candidate_id": candidate_id,
        "session_id": session_id,
        "seq": 2,
        "type": "SESSION_STARTED",
        "severity": "info",
        "payload": {"duration_s": 7200},
    }
    r_rep = httpx.post(f"{base_url}/v1/events", json=ev_replay, headers=headers)
    assert r_rep.status_code == 200

    with get_db_connection(db_path) as conn:
        row = conn.execute("SELECT started_at FROM sessions WHERE session_id = ?;", (session_id,)).fetchone()
        assert row["started_at"] == t1_iso, f"Replaying SESSION_STARTED must keep earliest {t1_iso}, got {row['started_at']}"


