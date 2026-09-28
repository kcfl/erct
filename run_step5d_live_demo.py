"""Live run script for Step 5d:

Starts API, starts simulator (2s interval), injects power_loss on C-BPL-02 (duration 30s),
waits for restore, and prints SQL timestamps and peak buffer backlog.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
import httpx

from app.db import get_db_connection, init_db
from app.main import seed_database
from simulator.agent_runner import SimulatorRunner


def parse_iso(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def run_demo():
    print("=" * 70)
    print("ERCT STEP 5d: LIVE POWER LOSS FAULT DEMO (C-BPL-02, 30s outage)")
    print("=" * 70)

    # 1. Reset database and buffer
    db_path = "data/erct.db"
    buf_path = "data/simulator_buffer.db"
    if os.path.exists(buf_path):
        os.remove(buf_path)
    if os.path.exists(db_path):
        os.remove(db_path)

    init_db(db_path)
    seed_database(db_path)

    # 2. Launch API server
    port = 8000
    api_url = f"http://127.0.0.1:{port}"
    print(f"[DEMO] Launching API server on {api_url}...")

    api_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        # Wait for API ready
        api_ready = False
        for _ in range(40):
            try:
                r = httpx.get(f"{api_url}/v1/health", timeout=0.5)
                if r.status_code == 200:
                    api_ready = True
                    break
            except Exception:
                time.sleep(0.15)

        if not api_ready:
            raise RuntimeError("API failed to start within timeout.")

        print(f"[DEMO] API server is ready at {api_url}.")

        # 3. Start Simulator with normal 2.0s interval
        print("[DEMO] Starting simulator runner with normal 2.0s heartbeat interval...")
        runner = SimulatorRunner(
            api_base_url=api_url,
            buffer_db_path=buf_path,
            heartbeat_override_s=2.0,
        )

        import threading
        runner_thread = threading.Thread(
            target=runner.start,
            kwargs={"duration_s": 46.0},  # ~5s pre-fault, 30s outage, ~11s post-restore
            daemon=True,
        )
        runner_thread.start()

        # Let simulator establish initial heartbeat telemetry (step 0, 1, 2)
        print("[DEMO] Waiting 5 seconds for initial steady-state telemetry...")
        time.sleep(5.0)

        # 4. Inject 30s power loss on C-BPL-02 via POST /v1/control/faults
        print("\n[DEMO] >>> POSTing power_loss fault command for C-BPL-02 (duration: 30s)...")
        t_req_start = time.time()
        resp = httpx.post(
            f"{api_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={
                "centre_id": "C-BPL-02",
                "fault_type": "power_loss",
                "duration_s": 30,
                "params": {"source": "grid", "backup_minutes": 15},
            },
            timeout=5.0,
        )
        print(f"[DEMO] Control API responded: {resp.status_code} | {resp.text}")
        assert resp.status_code == 201

        # 5. Wait for outage duration (30s) + post-restore resumption
        print("[DEMO] Outage is active on C-BPL-02. Machines are dark. Telemetry paused for C-BPL-02.")
        print("[DEMO] Waiting for fault duration to complete and heartbeats to resume...")

        runner_thread.join(timeout=65.0)
        # Final safety drain if any events buffered during shutdown
        runner.buffer.drain_all(api_base_url=api_url)
        print("[DEMO] Simulator generation and buffer draining completed.")

        # 6. SQL Queries for C-BPL-02 timestamps and telemetry
        print("\n" + "=" * 70)
        print("SQL VERIFICATION RESULTS FOR CENTRE C-BPL-02")
        print("=" * 70)

        with get_db_connection(db_path) as conn:
            cursor = conn.cursor()

            # POWER_LOSS ts
            cursor.execute("SELECT ts FROM events WHERE centre_id = 'C-BPL-02' AND type = 'POWER_LOSS' ORDER BY ts ASC LIMIT 1;")
            row_pl = cursor.fetchone()
            pl_ts = row_pl["ts"] if row_pl else "NOT_FOUND"

            # POWER_RESTORED ts
            cursor.execute("SELECT ts FROM events WHERE centre_id = 'C-BPL-02' AND type = 'POWER_RESTORED' ORDER BY ts ASC LIMIT 1;")
            row_pr = cursor.fetchone()
            pr_ts = row_pr["ts"] if row_pr else "NOT_FOUND"

            # Last HEARTBEAT ts before loss
            cursor.execute(
                """
                SELECT ts FROM events
                WHERE centre_id = 'C-BPL-02' AND type = 'HEARTBEAT' AND ts < ?
                ORDER BY ts DESC LIMIT 1;
                """,
                (pl_ts,),
            )
            row_last_hb = cursor.fetchone()
            last_hb_ts = row_last_hb["ts"] if row_last_hb else "NOT_FOUND"

            # First HEARTBEAT ts after restore
            cursor.execute(
                """
                SELECT ts FROM events
                WHERE centre_id = 'C-BPL-02' AND type = 'HEARTBEAT' AND ts > ?
                ORDER BY ts ASC LIMIT 1;
                """,
                (pr_ts,),
            )
            row_first_hb = cursor.fetchone()
            first_hb_ts = row_first_hb["ts"] if row_first_hb else "NOT_FOUND"

            # Count heartbeats during outage window
            cursor.execute(
                """
                SELECT COUNT(*) AS c FROM events
                WHERE centre_id = 'C-BPL-02' AND type = 'HEARTBEAT' AND ts > ? AND ts < ?;
                """,
                (pl_ts, pr_ts),
            )
            outage_hb_count = cursor.fetchone()["c"]

            cursor.close()

        print(f"1. Last HEARTBEAT ts before loss : {last_hb_ts}")
        print(f"2. POWER_LOSS ts                 : {pl_ts}")
        print(f"3. POWER_RESTORED ts             : {pr_ts}")
        print(f"4. First HEARTBEAT ts after restore : {first_hb_ts}")
        print(f"5. Heartbeats inside outage window : {outage_hb_count} (strictly 0 expected)")
        print(f"6. Peak buffer backlog seen        : {runner.peak_backlog} events (target < 300)")
        print("=" * 70)

    finally:
        print("[DEMO] Cleaning up API process...")
        api_proc.terminate()
        try:
            api_proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            api_proc.kill()
        print("[DEMO] Cleanup done.")


if __name__ == "__main__":
    run_demo()
