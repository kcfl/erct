"""Unit and integration tests for ingest idempotency, authorization, store-and-forward buffer, and seeding."""
from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path
from typing import Generator
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app.config import get_config
from app.core.audit_chain import verify_audit_chain, get_audit_trail
from app.db import get_db_connection, init_db
from app.main import app, seed_database
from app.models.events import EventEnvelope, EventType, Severity
from simulator.buffer import StoreAndForwardBuffer


@pytest.fixture(autouse=True)
def clean_test_environment(tmp_path: Path):
    """Isolate database and buffer paths for each test."""
    db_file = tmp_path / "test_erct.db"
    buffer_file = tmp_path / "test_buffer.db"

    # Override config paths for testing
    cfg = get_config()
    orig_db = cfg.database.path
    cfg.database.path = str(db_file)

    init_db(str(db_file))
    seed_database(str(db_file))

    yield {
        "db_path": str(db_file),
        "buffer_path": str(buffer_file),
    }

    cfg.database.path = orig_db


def test_post_same_event_twice_idempotency(clean_test_environment):
    """(a) Posting the same event twice results in 1 events row and 1 audit entry."""
    client = TestClient(app)
    db_path = clean_test_environment["db_path"]

    event_id = str(uuid.uuid4())
    event_payload = {
        "event_id": event_id,
        "schema_ver": 1,
        "ts": "2026-09-29T10:00:00Z",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "candidate_id": "CAND-000001",
        "session_id": "SES-000001-1",
        "seq": 1,
        "type": "HEARTBEAT",
        "severity": "info",
        "payload": {"latency_ms": 45, "remaining_s": 7100},
    }

    headers = {"X-API-Key": "key-cbpl01-secret"}

    # First post: accepted = 1, duplicates = 0
    resp1 = client.post("/v1/events", json=event_payload, headers=headers)
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert data1["accepted"] == 1
    assert data1["duplicates"] == 0

    # Second post of the exact same event: accepted = 0, duplicates = 1
    resp2 = client.post("/v1/events", json=event_payload, headers=headers)
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["accepted"] == 0
    assert data2["duplicates"] == 1

    # Check database rows in events table
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS c FROM events WHERE event_id = ?;", (event_id,))
        count = cursor.fetchone()["c"]
        cursor.close()
    assert count == 1

    # Check audit log entries for this event_id
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS c FROM audit_log WHERE ref_id = ?;", (event_id,))
        audit_count = cursor.fetchone()["c"]
        cursor.close()
    assert audit_count == 1

    # Audit chain must remain 100% verified
    assert verify_audit_chain(db_path).ok is True


def test_batch_duplicates_and_invalid_events_counts(clean_test_environment):
    """(b) A batch with duplicates and invalid events gives correct {accepted, duplicates, rejected} counts."""
    client = TestClient(app)
    headers = {"X-API-Key": "key-cbpl01-secret"}

    existing_id = str(uuid.uuid4())
    first_ev = {
        "event_id": existing_id,
        "schema_ver": 1,
        "ts": "2026-09-29T10:00:01Z",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "seq": 1,
        "type": "SOFTWARE_VERSION_REPORT",
        "payload": {"version": "4.2.1", "required_version": "4.2.1"},
    }
    # Pre-insert one event
    client.post("/v1/events", json=first_ev, headers=headers)

    # Now construct a batch:
    # 1. New valid event (accepted)
    # 2. Existing event (duplicate)
    # 3. Invalid event (missing remaining_s in HEARTBEAT -> rejected)
    new_id = str(uuid.uuid4())
    batch = [
        {
            "event_id": new_id,
            "schema_ver": 1,
            "ts": "2026-09-29T10:00:02Z",
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "seq": 2,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 30, "remaining_s": 7000},
        },
        first_ev,  # duplicate
        {
            "event_id": str(uuid.uuid4()),
            "schema_ver": 1,
            "ts": "2026-09-29T10:00:03Z",
            "exam_id": "EX-2026-PS6-01",
            "centre_id": "C-BPL-01",
            "seq": 3,
            "type": "HEARTBEAT",
            "payload": {"latency_ms": 30},  # Missing remaining_s
        },
    ]

    resp = client.post("/v1/events", json=batch, headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] == 1
    assert data["duplicates"] == 1
    assert data["rejected"] == 1
    assert len(data["errors"]) == 1


def test_wrong_api_key_returns_401(clean_test_environment):
    """(c) A missing or wrong API key returns 401 Unauthorized."""
    client = TestClient(app)

    ev = {
        "event_id": str(uuid.uuid4()),
        "schema_ver": 1,
        "ts": "2026-09-29T10:00:00Z",
        "exam_id": "EX-2026-PS6-01",
        "centre_id": "C-BPL-01",
        "seq": 1,
        "type": "SOFTWARE_VERSION_REPORT",
        "payload": {"version": "4.2.1", "required_version": "4.2.1"},
    }

    # Missing header
    r1 = client.post("/v1/events", json=ev)
    assert r1.status_code == 401

    # Invalid key
    r2 = client.post("/v1/events", json=ev, headers={"X-API-Key": "wrong-secret-key"})
    assert r2.status_code == 401

    # Key from another centre
    r3 = client.post("/v1/events", json=ev, headers={"X-API-Key": "key-cbpl02-secret"})
    assert r3.status_code == 401


def test_store_and_forward_buffer_failover(clean_test_environment):
    """(d) BUFFER TEST:

    1. Start with 100 events dispatched to the API.
    2. API becomes unreachable; simulator buffers 60 more events.
    3. API becomes reachable again; buffer drains all 60 events.
    4. Database has exactly 160 unique rows, 0 duplicates, and audit verify passes with exactly 160 event entries.
    """
    db_path = clean_test_environment["db_path"]
    buffer_path = clean_test_environment["buffer_path"]
    buffer = StoreAndForwardBuffer(buffer_path)

    # Run a live test server on loopback port
    port = 8765
    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(server_config)

    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    # Wait for server to start
    live_url = f"http://127.0.0.1:{port}"
    time.sleep(0.8)

    try:
        # Phase 1: Send 100 events while API is up
        for i in range(1, 101):
            ev = {
                "event_id": str(uuid.uuid4()),
                "schema_ver": 1,
                "ts": "2026-09-29T10:00:00Z",
                "exam_id": "EX-2026-PS6-01",
                "centre_id": "C-BPL-01",
                "candidate_id": "CAND-000001",
                "session_id": "SES-000001-1",
                "seq": i,
                "type": "HEARTBEAT",
                "payload": {"latency_ms": 30, "remaining_s": 7200 - i},
            }
            buffer.enqueue(ev, centre_id="C-BPL-01", api_key="key-cbpl01-secret")

        # Drain the 100 events
        drained1 = buffer.drain_once(api_base_url=live_url, max_batch_size=100)
        assert drained1["accepted"] == 100
        assert buffer.get_pending_count() == 0

        # Phase 2: Stop/disconnect the API and buffer 60 more events
        dead_url = "http://127.0.0.1:59999"  # Non-existent endpoint simulating outage
        for i in range(101, 161):
            ev = {
                "event_id": str(uuid.uuid4()),
                "schema_ver": 1,
                "ts": "2026-09-29T10:05:00Z",
                "exam_id": "EX-2026-PS6-01",
                "centre_id": "C-BPL-01",
                "candidate_id": "CAND-000001",
                "session_id": "SES-000001-1",
                "seq": i,
                "type": "HEARTBEAT",
                "payload": {"latency_ms": 35, "remaining_s": 7200 - i},
            }
            buffer.enqueue(ev, centre_id="C-BPL-01", api_key="key-cbpl01-secret")

        # Attempt drain to dead URL: fails, items stay in buffer
        failed_drain = buffer.drain_once(api_base_url=dead_url, max_batch_size=100)
        assert failed_drain["failed"] == 60
        assert buffer.get_pending_count() == 60

        # Phase 3: Reconnect to live API and drain
        # Reset retry timestamps for immediate test drain
        with buffer._get_connection() as conn:
            conn.execute("UPDATE outbound_events SET next_retry_at = '2000-01-01T00:00:00Z';")
            conn.commit()

        drained2 = buffer.drain_once(api_base_url=live_url, max_batch_size=100)
        assert drained2["accepted"] == 60
        assert buffer.get_pending_count() == 0

        # Phase 4: Verification in DB
        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) AS c FROM events;")
            total_events = cursor.fetchone()["c"]

            cursor.execute("SELECT COUNT(DISTINCT event_id) AS c FROM events;")
            unique_events = cursor.fetchone()["c"]

            cursor.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'event';")
            audit_events = cursor.fetchone()["c"]
            cursor.close()

        assert total_events == 160
        assert unique_events == 160
        assert audit_events == 160

        # Audit chain must be fully valid
        audit_check = verify_audit_chain(db_path)
        assert audit_check.ok is True

    finally:
        server.should_exit = True
        server_thread.join(timeout=2.0)


def test_seeding_twice_idempotency(clean_test_environment):
    """(e) Running seed_database twice results in NO duplicate candidates or sessions."""
    db_path = clean_test_environment["db_path"]

    # Initial counts from clean_test_environment fixture (already seeded once)
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS c FROM candidates;")
        cands_before = cursor.fetchone()["c"]
        cursor.execute("SELECT COUNT(*) AS c FROM sessions;")
        sess_before = cursor.fetchone()["c"]
        cursor.close()

    assert cands_before == 200
    assert sess_before == 200

    # Seed a second time
    res2 = seed_database(db_path)
    assert res2["candidates"] == 0
    assert res2["sessions"] == 0
    assert res2["centres"] == 0
    assert res2["exams"] == 0

    # Ensure counts remain identical
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS c FROM candidates;")
        cands_after = cursor.fetchone()["c"]
        cursor.execute("SELECT COUNT(*) AS c FROM sessions;")
        sess_after = cursor.fetchone()["c"]
        cursor.close()

    assert cands_after == 200
    assert sess_after == 200
