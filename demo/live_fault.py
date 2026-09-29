"""Live fault demonstration script for ERCT.

Accepts CLI arguments: --type, --centre, --duration, --interval, --port, --kill-api-at.
Uses isolated temp database and buffer.
Handles clean shutdown on normal termination or Ctrl+C, and prints whether port is free.
Queries the Incident API, prints incident row, timeline, evidence, session states, audit verify result,
and count of 'incident' audit entries.
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
from app.core.audit_chain import verify_audit_chain
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


def launch_api_server(cfg_file: Path, port: int) -> subprocess.Popen:
    env = os.environ.copy()
    env["ERCT_CONFIG_PATH"] = str(cfg_file)
    env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
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

    api_ready = False
    for _ in range(60):
        try:
            r = httpx.get(f"http://127.0.0.1:{port}/v1/health", timeout=0.5)
            if r.status_code == 200:
                api_ready = True
                break
        except Exception:
            time.sleep(0.15)

    if not api_ready:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except Exception:
            proc.kill()
        raise RuntimeError("API failed to start within timeout.")
    return proc


def run_live_fault(
    fault_type: str = "power_loss",
    centre_id: str = "C-BPL-02",
    duration_s: float = 60.0,
    interval_s: float = 2.0,
    port: int = 8000,
    kill_api_at: Optional[float] = None,
) -> None:
    print("=" * 78)
    print(f"ERCT LIVE FAULT DEMO: {fault_type.upper()} on {centre_id}")
    print(f"Configuration: duration={duration_s}s, interval={interval_s}s, kill_api_at={kill_api_at}")
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
        api_proc = launch_api_server(cfg_file, port)
        print(f"[DEMO] API server is ready at {api_url}.")

        # 3. Start Simulator runner
        print(f"[DEMO] Starting simulator (interval: {interval_s}s)...")
        runner = SimulatorRunner(
            api_base_url=api_url,
            buffer_db_path=str(buf_file),
            heartbeat_override_s=interval_s,
            ground_truth_path=str(gt_file),
        )

        # Calculate run timing
        if fault_type == "none":
            pre_fault_s = 0.0
            post_window_s = 0.0
            total_run_s = duration_s
        else:
            pre_fault_s = 5.0
            post_window_s = 25.0 if fault_type == "power_loss" else 20.0
            total_run_s = pre_fault_s + duration_s + post_window_s

        import threading

        runner_thread = threading.Thread(
            target=runner.start,
            kwargs={"duration_s": total_run_s},
            daemon=True,
        )
        runner_thread.start()

        pre_kill_id: Optional[str] = None
        post_restart_id: Optional[str] = None
        api_killed_flag = False

        if fault_type != "none":
            # Wait pre-fault period to establish steady state
            print(f"[DEMO] Waiting {pre_fault_s}s for initial steady-state telemetry...")
            time.sleep(pre_fault_s)

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

        # 5. Monitor run and handle --kill-api-at if configured
        print(f"[DEMO] Simulation running for {total_run_s:.1f}s total. Monitoring...")
        start_mono = time.monotonic()
        while runner_thread.is_alive():
            elapsed = time.monotonic() - start_mono
            if kill_api_at is not None and not api_killed_flag and elapsed >= kill_api_at:
                api_killed_flag = True
                print(f"\n[DEMO] >>> t={elapsed:.1f}s: Reached --kill-api-at ({kill_api_at}s). Querying API before kill...")
                try:
                    r = httpx.get(f"{api_url}/v1/incidents", timeout=2.0)
                    if r.status_code == 200:
                        incs = r.json()
                        if incs:
                            pre_kill_id = incs[0].get("id") or incs[0].get("incident_id")
                            print(f"[DEMO] Pre-kill incident captured: {pre_kill_id} (status: {incs[0]['status']})")
                except Exception as e:
                    print(f"[DEMO] Warning reading pre-kill incident: {e}")

                print(f"[DEMO] >>> Killing API process (pid={api_proc.pid})...")
                api_proc.terminate()
                try:
                    api_proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    api_proc.kill()
                    api_proc.wait(timeout=2.0)
                api_proc = None

                print("[DEMO] API server killed. Sleeping 2 seconds to simulate downtime...")
                time.sleep(2.0)

                print(f"[DEMO] >>> Restarting API server on {api_url} with same DB...")
                api_proc = launch_api_server(cfg_file, port)
                print(f"[DEMO] API server successfully restarted (pid={api_proc.pid}).")

                # Immediately query incidents after restart
                try:
                    r = httpx.get(f"{api_url}/v1/incidents", timeout=2.0)
                    if r.status_code == 200:
                        incs = r.json()
                        if incs:
                            post_restart_id = incs[0].get("id") or incs[0].get("incident_id")
                            print(f"[DEMO] Post-restart incident captured: {post_restart_id} (status: {incs[0]['status']})")
                except Exception as e:
                    print(f"[DEMO] Warning reading post-restart incident: {e}")

            time.sleep(0.5)

        runner_thread.join(timeout=30.0)

        # Allow final buffer drain and final ticks
        print("\n[DEMO] Simulation loop finished. Performing final buffer drain...")
        runner.buffer.drain_all(api_base_url=api_url)
        time.sleep(1.0)  # Wait for DetectionWorker tick to settle

        # 6. Comprehensive Reporting
        print("\n" + "=" * 78)
        print("ERCT PHASE 3a-ii INCIDENT & VERIFICATION REPORT")
        print("=" * 78)

        # Health & Telemetry
        try:
            r_health = httpx.get(f"{api_url}/v1/health", timeout=3.0)
            health_data = r_health.json() if r_health.status_code == 200 else {}
        except Exception as e:
            health_data = {"error": str(e)}

        print("\n--- 1. SYSTEM HEALTH & DETECTION TELEMETRY ---")
        print(f"Health Status: {health_data.get('status')}")
        det_info = health_data.get("detection", {})
        print(f"Detection Telemetry: {json.dumps(det_info, indent=2)}")

        # Incidents List & Details
        try:
            r_inc = httpx.get(f"{api_url}/v1/incidents", timeout=3.0)
            incidents_list = r_inc.json() if r_inc.status_code == 200 else []
        except Exception as e:
            incidents_list = []
            print(f"Error fetching incidents: {e}")

        print(f"\n--- 2. INCIDENTS SUMMARY (Total Incidents: {len(incidents_list)}) ---")
        for inc in incidents_list:
            inc_id = inc.get("id") or inc.get("incident_id")
            print(f"\nIncident Row: {inc_id}")
            print(f"  Exam ID       : {inc.get('exam_id')}")
            print(f"  Centre ID     : {inc.get('centre_id')}")
            print(f"  Type          : {inc.get('type')}")
            print(f"  Severity      : {inc.get('severity')}")
            print(f"  Status        : {inc.get('status')}")
            print(f"  Rule          : {inc.get('detection_rule')}")
            print(f"  Detected At   : {inc.get('detected_at')}")
            print(f"  Window Start  : {inc.get('window_start')}")
            print(f"  Window End    : {inc.get('window_end')}")
            print(f"  Resolved At   : {inc.get('resolved_at')}")

            try:
                r_det = httpx.get(f"{api_url}/v1/incidents/{inc_id}", timeout=3.0)
                det_data = r_det.json() if r_det.status_code == 200 else inc
            except Exception:
                det_data = inc

            print("\n  Incident Timeline:")
            timeline = det_data.get("timeline", [])
            if timeline:
                for t in timeline:
                    print(f"    [{t.get('ts')}] kind={t.get('kind')} | detail={t.get('detail')}")
            else:
                print("    (No timeline entries)")

            print("\n  Evidence JSON:")
            ev = det_data.get("evidence", {})
            print(json.dumps(ev, indent=4))

        # DB Session States & Audit Checks
        with get_db_connection(str(db_file)) as conn:
            cursor = conn.cursor()

            print("\n--- 3. SESSION STATE BREAKDOWN ---")
            if fault_type != "none":
                cursor.execute(
                    "SELECT state, COUNT(*) as count FROM sessions WHERE centre_id = ? GROUP BY state ORDER BY state;",
                    (centre_id,),
                )
                rows = cursor.fetchall()
                print(f"  Centre {centre_id} session states:")
                for r in rows:
                    print(f"    {r['state']}: {r['count']}")

            cursor.execute(
                "SELECT centre_id, state, COUNT(*) as count FROM sessions GROUP BY centre_id, state ORDER BY centre_id, state;"
            )
            rows_all = cursor.fetchall()
            print("  All centres session state breakdown:")
            for r in rows_all:
                print(f"    {r['centre_id']} -> {r['state']}: {r['count']}")

            print("\n--- 4. AUDIT CHAIN INTEGRITY & 'incident' ENTRIES ---")
            audit_res = verify_audit_chain(str(db_file))
            print(f"  Audit Verify Result: ok={audit_res.ok}, total_entries={audit_res.total_entries}, error={audit_res.error}")

            cursor.execute(
                "SELECT seq, ts, entry_type, ref_id, payload FROM audit_log WHERE entry_type = 'incident' ORDER BY seq ASC;"
            )
            inc_audits = cursor.fetchall()
            print(f"  Total 'incident' Audit Entries: {len(inc_audits)}")
            for r in inc_audits:
                p = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
                print(f"    seq={r['seq']} ts={r['ts']} action={p.get('action')} incident_id={p.get('incident_id')}")

            print("\n--- 5. SCENARIO-SPECIFIC VERIFICATION ---")
            if fault_type == "power_loss":
                cursor.execute("SELECT ts FROM events WHERE centre_id = ? AND type = 'POWER_LOSS' ORDER BY ts ASC LIMIT 1;", (centre_id,))
                r_pl = cursor.fetchone()
                pl_ts = r_pl["ts"] if r_pl else "NOT_FOUND"

                cursor.execute(
                    "SELECT ts FROM events WHERE centre_id = ? AND type = 'HEARTBEAT' AND ts < ? ORDER BY ts DESC LIMIT 1;",
                    (centre_id, pl_ts),
                )
                r_last_hb = cursor.fetchone()
                last_hb_ts = r_last_hb["ts"] if r_last_hb else "NOT_FOUND"

                target_inc = next((i for i in incidents_list if i["centre_id"] == centre_id), None)
                if target_inc and last_hb_ts != "NOT_FOUND":
                    t_det = parse_iso(target_inc["detected_at"])
                    t_hb = parse_iso(last_hb_ts)
                    latency_s = (t_det - t_hb).total_seconds()
                    print(f"  Last good heartbeat ts : {last_hb_ts}")
                    print(f"  POWER_LOSS ts          : {pl_ts}")
                    print(f"  Incident detected_at   : {target_inc['detected_at']}")
                    print(f"  Time from last good heartbeat to detected_at: {latency_s:.2f} s")

                if kill_api_at is not None:
                    print(f"\n  Kill/Restart Verification:")
                    print(f"    Pre-kill incident ID    : {pre_kill_id}")
                    print(f"    Post-restart incident ID: {post_restart_id}")
                    print(f"    Same ID preserved       : {pre_kill_id == post_restart_id}")
                    c_target = sum(1 for i in incidents_list if i["centre_id"] == centre_id)
                    c_other = sum(1 for i in incidents_list if i["centre_id"] != centre_id)
                    print(f"    Total incidents for {centre_id}: {c_target} (expected 1)")
                    print(f"    Total incidents for other centres: {c_other} (expected 0)")

            elif fault_type == "network_drop":
                target_inc = next((i for i in incidents_list if i["centre_id"] == centre_id), None)
                if target_inc:
                    ev = target_inc.get("evidence", {})
                    print(f"  Opened Rule/Confidence : {ev.get('reclassified_from', ev.get('confidence'))} (Rule: CENTRE_LOSS_FRACTION)")
                    print(f"  Final Rule/Confidence  : {ev.get('confidence')} (Rule: {target_inc.get('detection_rule')})")
                    print(f"  Late events counted    : {ev.get('late_events')}")
                    print(f"  Incident resolved_at   : {target_inc.get('resolved_at')}")
                    print(f"  Incident status        : {target_inc.get('status')}")

            elif fault_type == "none":
                print(f"  Total incidents count  : {len(incidents_list)} (expected 0)")
                print(f"  Ticks recorded         : {det_info.get('ticks')} (expected >= 55)")
                print(f"  Ingest stalled         : {det_info.get('ingest_stalled')} (expected False)")

            cursor.close()

        # Ground Truth Summary from JSONL if present
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
    parser.add_argument("--type", choices=["power_loss", "network_drop", "none"], default="power_loss")
    parser.add_argument("--centre", default="C-BPL-02")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--kill-api-at", type=float, default=None, help="Seconds into run when API is terminated and restarted")
    args = parser.parse_args()

    run_live_fault(
        fault_type=args.type,
        centre_id=args.centre,
        duration_s=args.duration,
        interval_s=args.interval,
        port=args.port,
        kill_api_at=args.kill_api_at,
    )


if __name__ == "__main__":
    main()
