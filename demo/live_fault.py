"""Live fault demonstration script for ERCT (Phase 3b-i).

Supports repeatable --fault type:centre:duration[:start_offset_s] flags.
Executes real faults, monitors centre and session states mid-outage and post-outage,
validates impact computation, calibration against ground truth, audit trail integrity,
and review queue consistency.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import yaml

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.audit_chain import verify_audit_chain
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


@dataclass
class FaultSpec:
    fault_type: str
    centre_id: str
    duration_s: float
    start_offset_s: float = 22.0
    injected: bool = False
    mid_printed: bool = False
    post_printed: bool = False
    mid_status: str = ""
    mid_states: Dict[str, int] = None
    post_status: str = ""
    post_states: Dict[str, int] = None


def run_live_demo(
    faults: List[FaultSpec],
    interval_s: float = 2.0,
    port: int = 8000,
    no_fault_duration: float = 60.0,
) -> None:
    print("=" * 78)
    print("ERCT LIVE DEMONSTRATION RUNNER (Phase 3b-i)")
    if faults:
        print("Scheduled Faults:")
        for f in faults:
            print(f"  - {f.fault_type} on {f.centre_id} for {f.duration_s}s (start offset: {f.start_offset_s}s)")
    else:
        print(f"No faults configured. Clean run for {no_fault_duration}s.")
    print(f"Heartbeat interval: {interval_s}s | API Port: {port}")
    print("=" * 78)

    # 1. Setup isolated temporary directory
    temp_dir = tempfile.TemporaryDirectory(prefix="erct_demo_")
    temp_path = Path(temp_dir.name)
    db_file = temp_path / "demo_erct.db"
    buf_file = temp_path / "demo_buffer.db"
    cfg_file = temp_path / "demo_config.yaml"
    gt_file = temp_path / "ground_truth.jsonl"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["simulation"]["ground_truth_path"] = str(gt_file)
    cfg["impact"]["heartbeat_interval_s"] = interval_s
    cfg["simulation"]["faults"] = []

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
            except Exception:
                api_proc.kill()
            api_proc = None

        time.sleep(0.5)
        port_free = is_port_free(port)
        print(f"[DEMO] Port {port} free again: {port_free}")
        try:
            temp_dir.cleanup()
        except Exception:
            pass

    def sig_handler(sig, frame):
        print("\n[DEMO] Interrupted by user (Ctrl+C). Cleaning up...")
        cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)

    try:
        # 2. Launch API
        print(f"[DEMO] Launching API server on {api_url}...")
        api_proc = launch_api_server(cfg_file, port)
        print(f"[DEMO] API server ready.")

        # 3. Determine run duration
        if faults:
            max_fault_end = max(f.start_offset_s + f.duration_s for f in faults)
            buffer_s = 32.0 if any(f.fault_type == "power_loss" for f in faults) else 18.0
            total_run_s = max_fault_end + buffer_s
        else:
            total_run_s = no_fault_duration

        print(f"[DEMO] Starting simulator for {total_run_s:.1f}s total...")
        runner = SimulatorRunner(
            api_base_url=api_url,
            buffer_db_path=str(buf_file),
            heartbeat_override_s=interval_s,
            ground_truth_path=str(gt_file),
        )

        import threading
        runner_thread = threading.Thread(
            target=runner.start,
            kwargs={"duration_s": total_run_s},
            daemon=True,
        )
        runner_thread.start()

        start_mono = time.monotonic()

        # 4. Monitoring loop: inject faults, print mid-outage and post-outage
        while runner_thread.is_alive():
            elapsed = time.monotonic() - start_mono

            # Check injection
            for f in faults:
                if not f.injected and elapsed >= f.start_offset_s:
                    f.injected = True
                    params = {"source": "grid", "backup_minutes": 15} if f.fault_type == "power_loss" else {"uplink": "primary_fiber"}
                    print(f"\n[DEMO +{elapsed:.1f}s] >>> Injecting {f.fault_type} on {f.centre_id} for {f.duration_s}s...")
                    try:
                        resp = httpx.post(
                            f"{api_url}/v1/control/faults",
                            headers={"X-Control-Key": "ctrl-secret-key-2026"},
                            json={
                                "centre_id": f.centre_id,
                                "fault_type": f.fault_type,
                                "duration_s": int(f.duration_s),
                                "params": params,
                            },
                            timeout=5.0,
                        )
                        print(f"[DEMO] Fault command registered: status {resp.status_code}")
                    except Exception as e:
                        print(f"[DEMO ERROR] Failed registering fault: {e}")

                # Mid-outage check (at start + duration / 2)
                mid_time = f.start_offset_s + (f.duration_s / 2.0)
                if f.injected and not f.mid_printed and elapsed >= mid_time:
                    f.mid_printed = True
                    try:
                        r_c = httpx.get(f"{api_url}/v1/centres", timeout=2.0)
                        centres_data = r_c.json() if r_c.status_code == 200 else []
                        c_match = next((c for c in centres_data if c["centre_id"] == f.centre_id), None)
                        c_status = c_match.get("status") if c_match else "unknown"
                    except Exception:
                        c_status = "error"

                    with get_db_connection(str(db_file)) as conn:
                        rows = conn.execute(
                            "SELECT state, COUNT(*) as c FROM sessions WHERE centre_id = ? GROUP BY state;",
                            (f.centre_id,),
                        ).fetchall()
                        st_dict = {r["state"]: r["c"] for r in rows}

                    f.mid_status = c_status
                    f.mid_states = st_dict
                    print(f"\n[MID-OUTAGE {f.centre_id} @ +{elapsed:.1f}s]")
                    print(f"  Centre Status: {c_status} (expected: down)")
                    print(f"  Session State Breakdown: {st_dict} (expect interrupted)")

                # Post-outage check (at start + duration + post_offset)
                post_offset = 15.0 if f.fault_type == "power_loss" else 5.0
                post_time = f.start_offset_s + f.duration_s + post_offset
                if f.injected and not f.post_printed and elapsed >= post_time:
                    f.post_printed = True
                    try:
                        r_c = httpx.get(f"{api_url}/v1/centres", timeout=2.0)
                        centres_data = r_c.json() if r_c.status_code == 200 else []
                        c_match = next((c for c in centres_data if c["centre_id"] == f.centre_id), None)
                        c_status = c_match.get("status") if c_match else "unknown"
                    except Exception:
                        c_status = "error"

                    with get_db_connection(str(db_file)) as conn:
                        rows = conn.execute(
                            "SELECT state, COUNT(*) as c FROM sessions WHERE centre_id = ? GROUP BY state;",
                            (f.centre_id,),
                        ).fetchall()
                        st_dict = {r["state"]: r["c"] for r in rows}

                    f.post_status = c_status
                    f.post_states = st_dict
                    print(f"\n[POST-OUTAGE {f.centre_id} @ +{elapsed:.1f}s]")
                    print(f"  Centre Status: {c_status} (recovering/ready)")
                    print(f"  Session State Breakdown: {st_dict}")

            time.sleep(0.5)

        runner_thread.join(timeout=30.0)

        # Allow final buffer drain and settle
        print("\n[DEMO] Simulation loop complete. Draining remaining events...")
        runner.buffer.drain_all(api_base_url=api_url)
        time.sleep(1.5)

        # Poll until all expected incidents have impact_computed_at
        expected_inc_count = len(faults)
        incidents_list: List[Dict[str, Any]] = []
        for _ in range(30):
            try:
                r_inc = httpx.get(f"{api_url}/v1/incidents", timeout=2.0)
                if r_inc.status_code == 200:
                    incidents_list = r_inc.json()
                    all_resolved_and_computed = (
                        len(incidents_list) >= expected_inc_count
                        and all(i.get("status") == "resolved" and i.get("impact_computed_at") for i in incidents_list)
                    )
                    if all_resolved_and_computed or expected_inc_count == 0:
                        break
            except Exception:
                pass
            time.sleep(0.5)

        # =======================================================================
        # VERIFICATION AND REPORTING
        # =======================================================================
        print("\n" + "=" * 78)
        print("ERCT LIVE RUN REPORT & VERIFICATION")
        print("=" * 78)

        # (1) Status and state counts mid-outage and after end
        if faults:
            print("\n(1) OUTAGE TELEMETRY & SESSION STATES (MID-OUTAGE & POST-OUTAGE)")
            for f in faults:
                print(f"  Centre {f.centre_id}:")
                print(f"    Mid-outage status : {f.mid_status} | Session states: {f.mid_states}")
                print(f"    Post-outage status: {f.post_status} | Session states: {f.post_states}")

        # (2) & (3) & (4) & (5) Incidents and Impacts
        print(f"\n(2) INCIDENTS & IMPACT SUMMARIES (Total Incidents: {len(incidents_list)})")
        if not incidents_list and not faults:
            print("  Zero incidents recorded as expected for clean run.")

        centre_avg_extra: Dict[str, float] = {}

        for inc in incidents_list:
            inc_id = inc["incident_id"]
            cid = inc["centre_id"]
            print(f"\n  -------------------------------------------------------------------")
            print(f"  Incident ID   : {inc_id} ({inc['type']} on {cid})")
            print(f"  Status        : {inc['status']}")
            print(f"  Window        : {inc['window_start']} -> {inc['window_end']}")
            print(f"  Resolved At   : {inc['resolved_at']}")
            print(f"  Computed At   : {inc.get('impact_computed_at')}")

            # (5) Computed_at vs resolved_at
            if inc.get("impact_computed_at") and inc.get("resolved_at"):
                t_comp = parse_iso(inc["impact_computed_at"])
                t_res = parse_iso(inc["resolved_at"])
                delta_s = (t_comp - t_res).total_seconds()
                print(f"  Latency (computed_at - resolved_at): {delta_s:.2f} s (computed_at >= resolved_at: {delta_s >= 0})")

            # Fetch impact
            r_imp = httpx.get(f"{api_url}/v1/incidents/{inc_id}/impact", timeout=3.0)
            imp_data = r_imp.json() if r_imp.status_code == 200 else {}
            summary = imp_data.get("summary") or {}
            rows = imp_data.get("rows") or []

            print(f"\n  Impact Summary:")
            print(f"    Exposed Sessions         : {summary.get('exposed')}")
            print(f"    By Remedy                : {summary.get('by_remedy')}")
            print(f"    By Rule                  : {summary.get('by_rule')}")
            print(f"    By Quality               : {summary.get('by_quality')}")
            print(f"    Avg Extra Seconds        : {summary.get('avg_extra_seconds')} s")
            print(f"    Max Extra Seconds        : {summary.get('max_extra_seconds')} s")
            print(f"    Centre Affected Fraction : {summary.get('centre_affected_fraction')}")
            print(f"    Centre Retest Recommended: {summary.get('centre_retest_recommended')}")

            centre_avg_extra[cid] = summary.get("avg_extra_seconds", 0.0)

            # (3) Calibration and Oracle numbers against ground truth if present
            if gt_file.exists():
                with open(gt_file, "r", encoding="utf-8") as f_gt:
                    gt_records = [json.loads(line) for line in f_gt if line.strip()]
                def types_match(t1: str, t2: str) -> bool:
                    return t1 == t2 or (t1 in t2) or (t2 in t1)

                gt_match = next((r for r in gt_records if r["centre_id"] == cid and types_match(r["type"], inc["type"])), None)
                if gt_match:
                    gt_cands = {c["candidate_id"]: c for c in gt_match.get("candidates", [])}
                    strong_rows = [r for r in rows if r["evidence_quality"] == "strong"]
                    partial_rows = [r for r in rows if r["evidence_quality"] != "strong"]

                    calib_matches = 0
                    oracle_matches = 0
                    for r in strong_rows:
                        cand_id = r["candidate_id"]
                        if cand_id in gt_cands:
                            gt_c = gt_cands[cand_id]
                            # Calibration check
                            stale_factor = cfg.get("impact", {}).get("stale_hb_factor", 2.5)
                            stale_tol = stale_factor * interval_s
                            diff = abs(r["lost_seconds"] - gt_c.get("lost_s_from_fault_start", 0.0))
                            ans_diff = (r["unsaved_answers"] == gt_c.get("expected_lost_answers", 0))
                            if diff <= stale_tol + 0.1 and ans_diff:
                                calib_matches += 1

                            # Oracle check
                            o_lost = gt_c.get("lost_s_from_fault_start", 0.0)
                            o_ans = gt_c.get("expected_lost_answers", 0)
                            oracle_rem = "resume" if (o_lost <= 300 and o_ans <= 2) else "extra_time"
                            if r["remedy_recommended"] == oracle_rem:
                                oracle_matches += 1

                    print(f"\n  (3) Calibration & Oracle Results (against ground truth):")
                    print(f"    Total Evaluated Candidates  : {len(rows)}")
                    print(f"    Strong Quality Candidates   : {len(strong_rows)}")
                    print(f"    Partial / Non-Strong        : {len(partial_rows)}")
                    if strong_rows:
                        print(f"    Calibration Accuracy (b)   : {calib_matches}/{len(strong_rows)} ({calib_matches / len(strong_rows):.1%})")
                        print(f"    Oracle Match Rate (c)       : {oracle_matches}/{len(strong_rows)} ({oracle_matches / len(strong_rows):.1%})")

            # (4) Example Rows with Rationale (one R1, one R2, one manual_review)
            print(f"\n  (4) Representative Rows & Plain-Language Rationales:")
            r1_sample = next((r for r in rows if r["rule_id"] == "R1"), None)
            r2_sample = next((r for r in rows if r["rule_id"] == "R2"), None)
            r3_sample = next((r for r in rows if r["rule_id"] == "R3" or r["remedy_recommended"] == "manual_review"), None)

            if r1_sample:
                print(f"    [Sample R1 Row - {r1_sample['candidate_id']}]:")
                print(f"      Remedy: {r1_sample['remedy_recommended']} | Extra Time: {r1_sample['extra_seconds']}s | Lost: {r1_sample['lost_seconds']}s | Unsaved: {r1_sample['unsaved_answers']}")
                print(f"      Rationale: \"{r1_sample['rationale']}\"")
            if r2_sample:
                print(f"    [Sample R2 Row - {r2_sample['candidate_id']}]:")
                print(f"      Remedy: {r2_sample['remedy_recommended']} | Extra Time: {r2_sample['extra_seconds']}s | Lost: {r2_sample['lost_seconds']}s | Unsaved: {r2_sample['unsaved_answers']}")
                print(f"      Rationale: \"{r2_sample['rationale']}\"")
            if r3_sample:
                print(f"    [Sample Manual Review Row - {r3_sample['candidate_id']}]:")
                print(f"      Remedy: {r3_sample['remedy_recommended']} | Quality: {r3_sample['evidence_quality']}")
                print(f"      Rationale: \"{r3_sample['rationale']}\"")

        # Multi-fault average extra seconds per centre
        if len(centre_avg_extra) > 1:
            print("\n  Average Extra Seconds Per Centre (Fairness Benchmark):")
            for cid, avg_s in centre_avg_extra.items():
                print(f"    {cid}: {avg_s:.1f} s")

        # (6) Audit Verification & Entry Counts
        with get_db_connection(str(db_file)) as conn:
            cursor = conn.cursor()
            audit_res = verify_audit_chain(str(db_file))
            cursor.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'impact';")
            impact_audit_c = cursor.fetchone()["c"]
            cursor.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'incident';")
            inc_audit_c = cursor.fetchone()["c"]
            cursor.execute("SELECT COUNT(*) AS c FROM incident_impacts;")
            total_impact_rows = cursor.fetchone()["c"]

            print(f"\n(6) AUDIT HASH CHAIN VERIFICATION")
            print(f"  Verification Result : ok={audit_res.ok} (Head Hash: {audit_res.head_hash[:16]}...)")
            print(f"  'impact' Entries   : {impact_audit_c} (Total impact rows: {total_impact_rows})")
            print(f"  'incident' Entries : {inc_audit_c}")

            # (7) Review Queue Count vs 'under_review' Sessions
            cursor.execute("SELECT COUNT(*) AS c FROM review_queue WHERE status = 'pending';")
            pending_rq_c = cursor.fetchone()["c"]
            cursor.execute("SELECT COUNT(*) AS c FROM sessions WHERE state = 'under_review';")
            under_rev_c = cursor.fetchone()["c"]

            print(f"\n(7) REVIEW QUEUE CONSISTENCY")
            print(f"  Review Queue Pending Rows : {pending_rq_c}")
            print(f"  Sessions in 'under_review': {under_rev_c}")
            print(f"  Counts Equal              : {pending_rq_c == under_rev_c}")

            # (8) Backlog & Residual Metric
            max_res = getattr(runner, "max_residual_after_step0", 0)
            peak_bl = getattr(runner, "peak_backlog_after_step0", runner.peak_backlog)
            print(f"\n(8) SIMULATOR RESIDUAL BACKLOG METRIC")
            print(f"  Max Residual Pending (post-step 0): {max_res} events")
            print(f"  Peak Queue Backlog   (post-step 0): {peak_bl} events")

            cursor.close()

        print("=" * 78)

    finally:
        cleanup()


def main():
    parser = argparse.ArgumentParser(description="ERCT Live Fault Demonstration Runner")
    parser.add_argument(
        "--fault",
        action="append",
        dest="faults",
        help="Repeatable fault specification: type:centre:duration[:start_offset_s]",
    )
    # Old shortcut flags
    parser.add_argument("--power-loss", action="store_true", help="Shortcut for --fault power_loss:C-BPL-02:60")
    parser.add_argument("--network-drop", action="store_true", help="Shortcut for --fault network_drop:C-BPL-04:30")
    parser.add_argument("--type", choices=["power_loss", "network_drop", "none"], default=None)
    parser.add_argument("--centre", default="C-BPL-02")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--port", type=int, default=8000)

    args = parser.parse_args()

    fault_specs: List[FaultSpec] = []
    if args.faults:
        for f_str in args.faults:
            parts = f_str.split(":")
            f_type = parts[0]
            f_centre = parts[1]
            f_dur = float(parts[2])
            f_start = float(parts[3]) if len(parts) > 3 else 22.0
            fault_specs.append(FaultSpec(fault_type=f_type, centre_id=f_centre, duration_s=f_dur, start_offset_s=f_start))
    elif args.power_loss:
        fault_specs.append(FaultSpec(fault_type="power_loss", centre_id="C-BPL-02", duration_s=60.0, start_offset_s=22.0))
    elif args.network_drop:
        fault_specs.append(FaultSpec(fault_type="network_drop", centre_id="C-BPL-04", duration_s=30.0, start_offset_s=22.0))
    elif args.type and args.type != "none":
        fault_specs.append(FaultSpec(fault_type=args.type, centre_id=args.centre, duration_s=args.duration, start_offset_s=22.0))

    no_fault_dur = args.duration if not fault_specs else 60.0

    run_live_demo(
        faults=fault_specs,
        interval_s=args.interval,
        port=args.port,
        no_fault_duration=no_fault_dur,
    )


if __name__ == "__main__":
    main()
