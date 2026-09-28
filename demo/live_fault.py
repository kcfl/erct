"""Live fault demonstration script for ERCT.

Accepts CLI arguments: --type, --centre, --duration, --interval.
Uses isolated temp database and buffer.
Handles clean shutdown on normal termination or Ctrl+C, and prints whether port is free.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx
import yaml

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db import get_db_connection, init_db
from app.main import seed_database
from simulator.agent_runner import SimulatorRunner


def parse_iso(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def is_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def run_live_fault(
    fault_type: str = "power_loss",
    centre_id: str = "C-BPL-02",
    duration_s: float = 30.0,
    interval_s: float = 2.0,
    port: int = 8000,
) -> None:
    print("=" * 78)
    print(f"ERCT LIVE FAULT DEMO: {fault_type.upper()} on {centre_id} (duration: {duration_s}s, interval: {interval_s}s)")
    print("=" * 78)

    # 1. Setup isolated temporary directory for DB, config, and buffer
    temp_dir = tempfile.TemporaryDirectory(prefix="erct_demo_")
    temp_path = Path(temp_dir.name)
    db_file = temp_path / "demo_erct.db"
    buf_file = temp_path / "demo_buffer.db"
    cfg_file = temp_path / "demo_config.yaml"
    gt_file = temp_path / "ground_truth.jsonl"

    # Clone base config with isolated paths
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["simulation"]["ground_truth_path"] = str(gt_file)
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    init_db(str(db_file))
    seed_database(str(db_file))

    api_url = f"http://127.0.0.1:{port}"
    api_proc: Optional[subprocess.Popen] = None
    runner: Optional[SimulatorRunner] = None

    def cleanup():
        nonlocal api_proc
        if api_proc is not None:
            print("\n[DEMO] Stopping API server process...")
            api_proc.terminate()
            try:
                api_proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                api_proc.kill()
                api_proc.wait(timeout=2.0)
            api_proc = None

        time.sleep(0.5)
        port_free = is_port_free(port)
        print(f"[DEMO] Port {port} free again: {port_free}")
        try:
            temp_dir.cleanup()
        except Exception:
            pass

    # Register signal handler for clean Ctrl+C
    def sig_handler(sig, frame):
        print("\n[DEMO] Interrupted by user (Ctrl+C). Cleaning up...")
        cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)

    try:
        # 2. Launch API process with isolated environment
        print(f"[DEMO] Launching API server on {api_url}...")
        env = os.environ.copy()
        env["ERCT_CONFIG_PATH"] = str(cfg_file)
        env["PYTHONUNBUFFERED"] = "1"

        api_proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for API ready
        api_ready = False
        for _ in range(50):
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

        # 3. Start Simulator runner
        print(f"[DEMO] Starting simulator (interval: {interval_s}s)...")
        runner = SimulatorRunner(
            api_base_url=api_url,
            buffer_db_path=str(buf_file),
            heartbeat_override_s=interval_s,
            ground_truth_path=str(gt_file),
        )

        # Total duration: 5s pre-fault + duration_s + post-restore window (at least 20s for staggered boots)
        post_window_s = 25.0 if fault_type == "power_loss" else 12.0
        total_run_s = 5.0 + duration_s + post_window_s

        import threading

        runner_thread = threading.Thread(
            target=runner.start,
            kwargs={"duration_s": total_run_s},
            daemon=True,
        )
        runner_thread.start()

        # Let simulator establish initial steady state
        print("[DEMO] Waiting 5 seconds for initial steady-state telemetry...")
        time.sleep(5.0)

        # 4. Inject fault via POST /v1/control/faults
        params = {"source": "grid", "backup_minutes": 15} if fault_type == "power_loss" else {"uplink": "primary_fiber"}
        print(f"\n[DEMO] >>> POSTing {fault_type} for {centre_id} (duration: {duration_s}s)...")
        resp = httpx.post(
            f"{api_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={
                "centre_id": centre_id,
                "fault_type": fault_type,
                "duration_s": int(duration_s),
                "params": params,
            },
            timeout=5.0,
        )
        print(f"[DEMO] Control API responded: {resp.status_code} | {resp.text}")
        assert resp.status_code == 201

        # 5. Wait for simulator run to complete
        print(f"[DEMO] Fault active on {centre_id}. Waiting for completion and recovery...")
        runner_thread.join(timeout=total_run_s + 20.0)

        # Final drain
        runner.buffer.drain_all(api_base_url=api_url)
        print("[DEMO] Simulator generation and buffer draining completed.")

        # 6. SQL and Ground Truth Reporting
        print("\n" + "=" * 78)
        print("VERIFICATION & TELEMETRY REPORT")
        print("=" * 78)

        with get_db_connection(str(db_file)) as conn:
            cursor = conn.cursor()

            if fault_type == "power_loss":
                # POWER_LOSS ts
                cursor.execute("SELECT ts FROM events WHERE centre_id = ? AND type = 'POWER_LOSS' ORDER BY ts ASC LIMIT 1;", (centre_id,))
                row_pl = cursor.fetchone()
                pl_ts = row_pl["ts"] if row_pl else "NOT_FOUND"

                # POWER_RESTORED ts
                cursor.execute("SELECT ts FROM events WHERE centre_id = ? AND type = 'POWER_RESTORED' ORDER BY ts ASC LIMIT 1;", (centre_id,))
                row_pr = cursor.fetchone()
                pr_ts = row_pr["ts"] if row_pr else "NOT_FOUND"

                # Last HEARTBEAT before loss
                cursor.execute(
                    "SELECT ts FROM events WHERE centre_id = ? AND type = 'HEARTBEAT' AND ts < ? ORDER BY ts DESC LIMIT 1;",
                    (centre_id, pl_ts),
                )
                row_last_hb = cursor.fetchone()
                last_hb_ts = row_last_hb["ts"] if row_last_hb else "NOT_FOUND"

                # First HEARTBEAT after restore
                cursor.execute(
                    "SELECT ts FROM events WHERE centre_id = ? AND type = 'HEARTBEAT' AND ts > ? ORDER BY ts ASC LIMIT 1;",
                    (centre_id, pr_ts),
                )
                row_first_hb = cursor.fetchone()
                first_hb_ts = row_first_hb["ts"] if row_first_hb else "NOT_FOUND"

                # Outage window heartbeats
                cursor.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE centre_id = ? AND type = 'HEARTBEAT' AND ts > ? AND ts < ?;",
                    (centre_id, pl_ts, pr_ts),
                )
                leakage_count = cursor.fetchone()["c"]

                print(f"1. Last HEARTBEAT before loss : {last_hb_ts}")
                print(f"2. POWER_LOSS ts                 : {pl_ts}")
                print(f"3. POWER_RESTORED ts             : {pr_ts}")
                print(f"4. First HEARTBEAT after restore : {first_hb_ts}")
                print(f"5. Heartbeats in outage window   : {leakage_count} (strictly 0)")

                # Remaining_s for 3 sample candidates before loss vs after restore
                cursor.execute(
                    """
                    SELECT candidate_id FROM events
                    WHERE centre_id = ? AND candidate_id IS NOT NULL
                    GROUP BY candidate_id ORDER BY candidate_id LIMIT 3;
                    """,
                    (centre_id,),
                )
                cand_sample = [r["candidate_id"] for r in cursor.fetchall()]

                print("\nCandidate Remaining Time Sampling (pre-loss vs post-restore):")
                for cid in cand_sample:
                    cursor.execute(
                        "SELECT payload FROM events WHERE candidate_id = ? AND type = 'HEARTBEAT' AND ts < ? ORDER BY ts DESC LIMIT 1;",
                        (cid, pl_ts),
                    )
                    r_pre = cursor.fetchone()
                    rem_pre = json.loads(r_pre["payload"]).get("remaining_s") if r_pre else None

                    cursor.execute(
                        "SELECT payload FROM events WHERE candidate_id = ? AND type = 'HEARTBEAT' AND ts > ? ORDER BY ts ASC LIMIT 1;",
                        (cid, pr_ts),
                    )
                    r_post = cursor.fetchone()
                    rem_post = json.loads(r_post["payload"]).get("remaining_s") if r_post else None

                    diff = (rem_pre - rem_post) if (rem_pre and rem_post) else None
                    print(f"  {cid}: pre-loss remaining_s={rem_pre}s | post-restore remaining_s={rem_post}s (countdown: {diff}s)")

                # First heartbeat timestamps across all 40 candidates after restore
                cursor.execute(
                    """
                    SELECT candidate_id, MIN(ts) as first_ts FROM events
                    WHERE centre_id = ? AND type = 'HEARTBEAT' AND ts > ?
                    GROUP BY candidate_id;
                    """,
                    (centre_id, pr_ts),
                )
                first_ts_rows = cursor.fetchall()
                if first_ts_rows and pr_ts != "NOT_FOUND":
                    t_pr = parse_iso(pr_ts)
                    boot_delays = [(parse_iso(r["first_ts"]) - t_pr).total_seconds() for r in first_ts_rows]
                    min_delay = min(boot_delays)
                    med_delay = statistics.median(boot_delays)
                    max_delay = max(boot_delays)
                    spread = max_delay - min_delay
                    print(f"\nCandidate Resume-Delay Spread ({len(first_ts_rows)} candidates):")
                    print(f"  Min delay: {min_delay:.2f}s | Median delay: {med_delay:.2f}s | Max delay: {max_delay:.2f}s | Spread: {spread:.2f}s")

            elif fault_type == "network_drop":
                cursor.execute("SELECT ts FROM events WHERE centre_id = ? AND type = 'NETWORK_DOWN' LIMIT 1;", (centre_id,))
                r_nd = cursor.fetchone()
                nd_ts = r_nd["ts"] if r_nd else "NOT_FOUND"

                cursor.execute("SELECT ts, ingested_at FROM events WHERE centre_id = ? AND type = 'NETWORK_UP' LIMIT 1;", (centre_id,))
                r_nu = cursor.fetchone()
                nu_ts = r_nu["ts"] if r_nu else "NOT_FOUND"
                nu_ingested = r_nu["ingested_at"] if r_nu else "NOT_FOUND"

                # Check if NETWORK_DOWN arrived late (ingested_at >= nu_ts)
                cursor.execute("SELECT ingested_at FROM events WHERE centre_id = ? AND type = 'NETWORK_DOWN' LIMIT 1;", (centre_id,))
                nd_ingested = cursor.fetchone()["ingested_at"] if r_nd else "NOT_FOUND"
                nd_arrived_late = (nd_ingested > nu_ts) if (nd_ingested != "NOT_FOUND" and nu_ts != "NOT_FOUND") else False

                # Late arrival events: ts inside [nd_ts, nu_ts], but ingested_at > nu_ts
                cursor.execute(
                    """
                    SELECT COUNT(*) AS c FROM events
                    WHERE centre_id = ? AND ts >= ? AND ts <= ? AND ingested_at >= ?;
                    """,
                    (centre_id, nd_ts, nu_ts, nu_ts),
                )
                late_arrival_count = cursor.fetchone()["c"]

                print(f"1. NETWORK_DOWN ts               : {nd_ts}")
                print(f"2. NETWORK_UP ts                 : {nu_ts}")
                print(f"3. NETWORK_DOWN arrived late     : {nd_arrived_late} (ingested_at: {nd_ingested})")
                print(f"4. Late events buffered & replay : {late_arrival_count} events (ts in window, ingested after restore)")

            cursor.close()

        # 7. Ground Truth Summary from JSONL
        if os.path.exists(gt_file):
            print("\nGround Truth Summary (data/ground_truth.jsonl):")
            with open(gt_file, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f if line.strip()]
            for rec in records:
                cands = rec.get("candidates", [])
                aff_count = len(cands)
                unsaved_gt2 = sum(1 for c in cands if c.get("unsaved_answers_at_start", 0) > 2)
                le2_nonflaky = sum(1 for c in cands if c.get("unsaved_answers_at_start", 0) <= 2 and not c.get("is_flaky", False))
                flaky_cnt = sum(1 for c in cands if c.get("is_flaky", False))
                lost_times = [c.get("lost_s_true", 0.0) for c in cands]
                lost_min = min(lost_times) if lost_times else 0.0
                lost_med = statistics.median(lost_times) if lost_times else 0.0
                lost_max = max(lost_times) if lost_times else 0.0

                all_zero = all(t == 0.0 for t in lost_times)
                print(f"  Fault: {rec['type']} on {rec['centre_id']} (duration: {rec['actual_duration_s']:.2f}s)")
                print(f"  Affected candidates        : {aff_count}")
                print(f"  Candidates with unsaved > 2 : {unsaved_gt2}")
                print(f"  Candidates with <= 2 nonflaky: {le2_nonflaky}")
                print(f"  Flaky candidates           : {flaky_cnt}")
                if rec['type'] == 'network_drop':
                    print(f"  True lost seconds (s)      : lost_s_true = 0 for all candidates ({all_zero}) (buffered & delivered)")
                else:
                    print(f"  True lost seconds (s)      : min={lost_min:.2f}s, med={lost_med:.2f}s, max={lost_max:.2f}s")

        print(f"\nPeak buffer backlog after step 0: {getattr(runner, 'peak_backlog_after_step0', runner.peak_backlog)} events")
        print("=" * 78)

    finally:
        cleanup()


def main():
    parser = argparse.ArgumentParser(description="ERCT Live Fault Demonstration Runner")
    parser.add_argument("--type", choices=["power_loss", "network_drop"], default="power_loss")
    parser.add_argument("--centre", default="C-BPL-02")
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    run_live_fault(
        fault_type=args.type,
        centre_id=args.centre,
        duration_s=args.duration,
        interval_s=args.interval,
        port=args.port,
    )


if __name__ == "__main__":
    main()
