"""Integration test for real process termination and buffered event replay without duplicates."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
import httpx
import pytest
import yaml

from app.core.audit_chain import verify_audit_chain
from app.db import get_db_connection, init_db
from simulator.buffer import StoreAndForwardBuffer


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_api(url: str, timeout: float = 8.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = httpx.get(f"{url}/v1/health", timeout=1.0)
            if r.status_code == 200:
                return True
        except Exception:
            time.sleep(0.15)
    return False


@pytest.mark.slow
def test_real_process_kill_restart_replay(tmp_path: Path):
    """Start real uvicorn subprocess, send 100 events, kill process, buffer 60, start new uvicorn, drain.

    Assert:
      - exactly 160 unique events
      - 0 duplicates
      - audit verify() is ok
      - exactly 160 audit entries of entry_type == 'event'
    """
    port = find_free_port()
    api_url = f"http://127.0.0.1:{port}"
    db_file = tmp_path / "kill_test.db"
    buffer_file = tmp_path / "kill_buffer.db"
    config_file = tmp_path / "test_config.yaml"

    # Create test config referencing the isolated database
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg_dict = yaml.safe_load(f)
    cfg_dict["database"]["path"] = str(db_file)
    with open(config_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg_dict, f)

    buffer = StoreAndForwardBuffer(str(buffer_file))
    env = os.environ.copy()
    env["ERCT_CONFIG_PATH"] = str(config_file)
    env["PYTHONUNBUFFERED"] = "1"

    proc1 = None
    proc2 = None

    try:
        # 1. Spawn Process 1
        proc1 = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        assert wait_for_api(api_url), "Process 1 failed to start"

        # 2. Enqueue and send 100 events
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
                "payload": {"latency_ms": 30, "remaining_s": 7200 - i, "local_seq": 1},
            }
            buffer.enqueue(ev, centre_id="C-BPL-01", api_key="key-cbpl01-secret")

        drained1 = buffer.drain_once(api_base_url=api_url, max_batch_size=500)
        assert drained1["accepted"] == 100
        assert buffer.get_pending_count() == 0

        # 3. Terminate Process 1 for real
        proc1.terminate()
        try:
            proc1.wait(timeout=4.0)
        except subprocess.TimeoutExpired:
            proc1.kill()
            proc1.wait(timeout=2.0)
        proc1 = None

        # Verify API is down
        with pytest.raises(Exception):
            httpx.get(f"{api_url}/v1/health", timeout=1.0)

        # 4. Enqueue 60 more events into the buffer while API is dead
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
                "payload": {"latency_ms": 35, "remaining_s": 7200 - i, "local_seq": 2},
            }
            buffer.enqueue(ev, centre_id="C-BPL-01", api_key="key-cbpl01-secret")

        assert buffer.get_pending_count() == 60

        # 5. Spawn Process 2 on the exact same port and DB
        proc2 = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        assert wait_for_api(api_url), "Process 2 failed to start"

        # 6. Drain remaining 60 buffered events
        drained2 = buffer.drain_once(api_base_url=api_url, max_batch_size=500)
        assert drained2["accepted"] == 60
        assert buffer.get_pending_count() == 0

        # 7. Assertions on Database and Audit Chain
        with get_db_connection(str(db_file)) as conn:
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

        verify_res = verify_audit_chain(str(db_file))
        assert verify_res.ok is True

    finally:
        for p in (proc1, proc2):
            if p and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    p.kill()
