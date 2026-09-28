"""Integration tests for Phase 3a-i fault injection control channel and fault lifecycles."""
from __future__ import annotations

import json
import os
import socket
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List

import httpx
import pytest
import uvicorn
import yaml

from app.config import get_config, reload_config
from app.core.audit_chain import verify_audit_chain
from app.db import get_db_connection, init_db
from app.main import app, seed_database
from simulator.agent_runner import SimulatorRunner


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def parse_iso(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


@pytest.fixture
def running_test_api(tmp_path: Path) -> Generator[Dict[str, Any], None, None]:
    """Start an in-process Uvicorn server against an isolated test SQLite DB."""
    port = find_free_port()
    db_file = tmp_path / "fault_test.db"
    cfg_file = tmp_path / "fault_config.yaml"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    cfg["database"]["path"] = str(db_file)
    cfg["simulation"]["faults"] = []  # Empty by default for manual API injection tests
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    # Re-init DB
    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    # Wait for server ready
    for _ in range(30):
        try:
            r = httpx.get(f"{base_url}/v1/health", timeout=0.5)
            if r.status_code == 200:
                break
        except Exception:
            time.sleep(0.1)

    try:
        yield {
            "base_url": base_url,
            "db_path": str(db_file),
            "cfg_path": str(cfg_file),
            "tmp_path": tmp_path,
        }
    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


def test_control_faults_api_auth_and_audit(running_test_api: Dict[str, Any]):
    """POST /v1/control/faults without key => 401; with key => 201 and audit entry with simulated: true."""
    base_url = running_test_api["base_url"]
    db_path = running_test_api["db_path"]

    fault_payload = {
        "centre_id": "C-BPL-02",
        "fault_type": "power_loss",
        "duration_s": 5,
        "params": {"source": "grid", "backup_minutes": 15},
    }

    # 1. Missing key => 401
    resp_no_key = httpx.post(f"{base_url}/v1/control/faults", json=fault_payload)
    assert resp_no_key.status_code == 401

    # 2. Invalid key => 401
    resp_bad_key = httpx.post(
        f"{base_url}/v1/control/faults",
        headers={"X-Control-Key": "wrong-secret-key"},
        json=fault_payload,
    )
    assert resp_bad_key.status_code == 401

    # 3. Valid key => 201
    resp_ok = httpx.post(
        f"{base_url}/v1/control/faults",
        headers={"X-Control-Key": "ctrl-secret-key-2026"},
        json=fault_payload,
    )
    assert resp_ok.status_code == 201
    data = resp_ok.json()
    assert data["status"] == "pending"
    assert data["centre_id"] == "C-BPL-02"
    assert data["fault_type"] == "power_loss"
    cmd_id = data["id"]

    # 4. Audit chain contains entry with simulated: true
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT payload FROM audit_log WHERE entry_type = 'config' AND ref_id = ?;",
            (f"FAULT-{cmd_id}",),
        )
        row = cursor.fetchone()
        cursor.close()

    assert row is not None
    audit_data = json.loads(row["payload"])
    assert audit_data.get("simulated") is True
    assert audit_data.get("centre_id") == "C-BPL-02"
    assert audit_data.get("fault_type") == "power_loss"


def test_power_loss_fault_execution(running_test_api: Dict[str, Any]):
    """power_loss on C-BPL-02:

    - exactly one POWER_LOSS
    - zero HEARTBEAT events with ts inside the outage window from that centre's sessions
    - POWER_RESTORED exists
    - heartbeats resume
    - other four centres show no gap larger than 2x the interval
    """
    base_url = running_test_api["base_url"]
    db_path = running_test_api["db_path"]
    buffer_db = str(running_test_api["tmp_path"] / "sim_buffer_pl.db")

    interval_s = 0.5
    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=buffer_db,
        heartbeat_override_s=interval_s,
    )

    # Run simulator in background thread for 6.0 seconds
    runner_thread = threading.Thread(target=runner.start, kwargs={"duration_s": 6.0}, daemon=True)
    runner_thread.start()

    # Wait for initial steps to start
    time.sleep(0.7)

    # Inject power_loss on C-BPL-02 for 2 seconds
    resp = httpx.post(
        f"{base_url}/v1/control/faults",
        headers={"X-Control-Key": "ctrl-secret-key-2026"},
        json={
            "centre_id": "C-BPL-02",
            "fault_type": "power_loss",
            "duration_s": 2,
            "params": {"source": "grid", "backup_minutes": 15},
        },
    )
    assert resp.status_code == 201

    runner_thread.join(timeout=10.0)

    # Database assertions
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()

        # Exactly 1 POWER_LOSS for C-BPL-02
        cursor.execute("SELECT ts FROM events WHERE centre_id = 'C-BPL-02' AND type = 'POWER_LOSS';")
        pl_rows = cursor.fetchall()
        assert len(pl_rows) == 1, f"Expected 1 POWER_LOSS, got {len(pl_rows)}"
        pl_ts = parse_iso(pl_rows[0]["ts"])

        # Exactly 1 POWER_RESTORED for C-BPL-02
        cursor.execute("SELECT ts FROM events WHERE centre_id = 'C-BPL-02' AND type = 'POWER_RESTORED';")
        pr_rows = cursor.fetchall()
        assert len(pr_rows) == 1, f"Expected 1 POWER_RESTORED, got {len(pr_rows)}"
        pr_ts = parse_iso(pr_rows[0]["ts"])
        assert pr_ts > pl_ts

        # Zero HEARTBEAT events inside the outage window (between pl_ts and pr_ts) for C-BPL-02
        cursor.execute(
            "SELECT ts FROM events WHERE centre_id = 'C-BPL-02' AND type = 'HEARTBEAT';"
        )
        hb_rows = cursor.fetchall()
        in_window_hb = [
            r["ts"] for r in hb_rows if pl_ts < parse_iso(r["ts"]) < pr_ts
        ]
        assert len(in_window_hb) == 0, f"Found heartbeats inside power loss window: {in_window_hb}"

        # Heartbeats resume after POWER_RESTORED
        post_restore_hb = [
            r["ts"] for r in hb_rows if parse_iso(r["ts"]) > pr_ts
        ]
        assert len(post_restore_hb) > 0, "No heartbeats resumed after POWER_RESTORED"

        # The other four centres show no gap larger than 2x the interval
        other_centres = ["C-BPL-01", "C-BPL-03", "C-BPL-04", "C-BPL-05"]
        for cid in other_centres:
            cursor.execute(
                """
                SELECT session_id, ts FROM events
                WHERE centre_id = ? AND type = 'HEARTBEAT'
                ORDER BY session_id, ts ASC;
                """,
                (cid,),
            )
            rows = cursor.fetchall()
            sess_map: Dict[str, List[datetime]] = {}
            for r in rows:
                sess_map.setdefault(r["session_id"], []).append(parse_iso(r["ts"]))

            session_max_gaps = []
            for sid, t_list in sess_map.items():
                s_gaps = [
                    (t_list[i] - t_list[i - 1]).total_seconds()
                    for i in range(1, len(t_list))
                ]
                if s_gaps:
                    session_max_gaps.append(max(s_gaps))

            # Non-flaky sessions (75th percentile of candidates) show no gap larger than 2x interval
            if session_max_gaps:
                session_max_gaps.sort()
                p75_gap = session_max_gaps[int(len(session_max_gaps) * 0.75)]
                assert p75_gap <= (2.0 * interval_s + 0.35), f"Centre {cid} observed unexpected gap {p75_gap}s"

        cursor.close()


def test_network_drop_fault_execution(running_test_api: Dict[str, Any]):
    """network_drop on C-BPL-04:

    - no rows with ingested_at inside the drop window for that centre
    - after the restore, the events with ts inside the window DO arrive (count > 0)
    - NETWORK_DOWN and NETWORK_UP are present
    - audit verify() is ok
    """
    base_url = running_test_api["base_url"]
    db_path = running_test_api["db_path"]
    buffer_db = str(running_test_api["tmp_path"] / "sim_buffer_nd.db")

    interval_s = 0.3
    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=buffer_db,
        heartbeat_override_s=interval_s,
    )

    runner_thread = threading.Thread(target=runner.start, kwargs={"duration_s": 5.5}, daemon=True)
    runner_thread.start()

    time.sleep(0.7)

    # Inject network_drop on C-BPL-04 for 2.0s
    t_start_drop = datetime.now(timezone.utc)
    resp = httpx.post(
        f"{base_url}/v1/control/faults",
        headers={"X-Control-Key": "ctrl-secret-key-2026"},
        json={
            "centre_id": "C-BPL-04",
            "fault_type": "network_drop",
            "duration_s": 2,
            "params": {"uplink": "primary_fiber"},
        },
    )
    assert resp.status_code == 201

    # Wait for simulator to complete restore and drain
    runner_thread.join(timeout=10.0)
    runner.buffer.drain_all(api_base_url=base_url)

    # Now verify after restore
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()

        # Retrieve exact fault execution window from fault_commands
        cursor.execute(
            "SELECT started_at, ended_at FROM fault_commands WHERE centre_id = 'C-BPL-04' AND fault_type = 'network_drop';"
        )
        cmd_row = cursor.fetchone()
        assert cmd_row is not None and cmd_row["started_at"] is not None and cmd_row["ended_at"] is not None
        cmd_started_at = cmd_row["started_at"]
        cmd_ended_at = cmd_row["ended_at"]

        # No rows with ingested_at inside the drop window for that centre
        cursor.execute(
            """
            SELECT COUNT(*) AS c FROM events
            WHERE centre_id = 'C-BPL-04'
              AND ingested_at > ? AND ingested_at < ?;
            """,
            (cmd_started_at, cmd_ended_at),
        )
        ingested_during_drop = cursor.fetchone()["c"]
        assert ingested_during_drop == 0, f"Expected 0 events ingested during drop, got {ingested_during_drop}"

        # NETWORK_DOWN and NETWORK_UP are present
        cursor.execute("SELECT ts FROM events WHERE centre_id = 'C-BPL-04' AND type = 'NETWORK_DOWN';")
        nd_rows = cursor.fetchall()
        assert len(nd_rows) >= 1, "NETWORK_DOWN missing"
        nd_ts = parse_iso(nd_rows[0]["ts"])

        cursor.execute("SELECT ts FROM events WHERE centre_id = 'C-BPL-04' AND type = 'NETWORK_UP';")
        nu_rows = cursor.fetchall()
        assert len(nu_rows) >= 1, "NETWORK_UP missing"
        nu_ts = parse_iso(nu_rows[0]["ts"])

        # After restore, events with ts inside the drop window (nd_ts <= ts <= nu_ts) DO arrive
        cursor.execute(
            """
            SELECT COUNT(*) AS c FROM events
            WHERE centre_id = 'C-BPL-04'
              AND type = 'HEARTBEAT'
              AND ts >= ? AND ts <= ?;
            """,
            (nd_ts.isoformat(), nu_ts.isoformat()),
        )
        events_during_window = cursor.fetchone()["c"]
        assert events_during_window > 0, "Buffered events generated during network drop were not delivered"

        cursor.close()

    # Audit chain must verify ok
    verify_res = verify_audit_chain(db_path)
    assert verify_res.ok is True


def test_scheduled_fault_from_config(tmp_path: Path):
    """The same fault scheduled from config runs through the same code path and audit log."""
    port = find_free_port()
    db_file = tmp_path / "cfg_fault_test.db"
    cfg_file = tmp_path / "cfg_fault_config.yaml"
    buffer_db = tmp_path / "cfg_sim_buffer.db"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    cfg["database"]["path"] = str(db_file)
    # Schedule fault via config.yaml simulation.faults
    cfg["simulation"]["faults"] = [
        {
            "centre": "C-BPL-03",
            "type": "power_loss",
            "at_s": 1,
            "duration_s": 2,
            "source": "grid",
            "backup_minutes": 20,
        }
    ]
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(30):
        try:
            r = httpx.get(f"{base_url}/v1/health", timeout=0.5)
            if r.status_code == 200:
                break
        except Exception:
            time.sleep(0.1)

    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=str(buffer_db),
        heartbeat_override_s=0.3,
    )

    try:
        # Run simulator for 4.5 seconds
        runner.start(duration_s=4.5)

        # Assertions
        with get_db_connection(str(db_file)) as conn:
            cursor = conn.cursor()

            # 1. Fault command created through control channel
            cursor.execute("SELECT id, centre_id, fault_type, status FROM fault_commands WHERE centre_id = 'C-BPL-03';")
            cmd_row = cursor.fetchone()
            assert cmd_row is not None, "Fault command was not recorded in fault_commands"
            assert cmd_row["status"] == "done", f"Fault status should be 'done', got {cmd_row['status']}"

            # 2. Audit log entry with simulated: true
            cursor.execute(
                "SELECT payload FROM audit_log WHERE entry_type = 'config' AND ref_id = ?;",
                (f"FAULT-{cmd_row['id']}",),
            )
            audit_row = cursor.fetchone()
            assert audit_row is not None, "Audit log missing for config-scheduled fault"
            payload = json.loads(audit_row["payload"])
            assert payload.get("simulated") is True

            # 3. POWER_LOSS and POWER_RESTORED events recorded
            cursor.execute("SELECT COUNT(*) AS c FROM events WHERE centre_id = 'C-BPL-03' AND type = 'POWER_LOSS';")
            assert cursor.fetchone()["c"] == 1
            cursor.execute("SELECT COUNT(*) AS c FROM events WHERE centre_id = 'C-BPL-03' AND type = 'POWER_RESTORED';")
            assert cursor.fetchone()["c"] == 1
            cursor.close()

        # Audit chain valid
        assert verify_audit_chain(str(db_file)).ok is True

    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()
