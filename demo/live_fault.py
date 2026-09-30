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

    log_file = Path("data/api_server.log").open("w", encoding="utf-8")
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
        stdout=log_file,
        stderr=subprocess.STDOUT,
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


class TeeLogger:
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.terminal = sys.stdout
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(filepath, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.file.write(message)
        self.file.flush()

    def flush(self):
        self.terminal.flush()
        self.file.flush()

    def close(self):
        self.file.close()


def run_live_demo(
    faults: List[FaultSpec],
    interval_s: float = 2.0,
    port: int = 8000,
    no_fault_duration: float = 60.0,
    controller_flow: bool = False,
) -> None:
    ts_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fault_desc = "_".join(f"{f.fault_type}_{f.centre_id}" for f in faults) if faults else "clean_run"
    log_file_path = Path("data/runs") / f"{ts_utc}_{fault_desc}.txt"
    tee = TeeLogger(log_file_path)
    old_stdout = sys.stdout
    sys.stdout = tee

    print("=" * 78)
    print("ERCT LIVE DEMONSTRATION RUNNER")
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

    mid_cand_statuses: Dict[str, Dict[str, Any]] = {}

    def cleanup():
        nonlocal api_proc
        if api_proc is not None:
            print("\n[DEMO] Stopping API server process...")
            api_proc.terminate()
            try:
                api_proc.wait(timeout=5.0)
            except Exception:
                api_proc.kill()
            api_proc = None

        try:
            import sqlite3
            if db_file.exists():
                Path("data").mkdir(parents=True, exist_ok=True)
                dest_db = Path("data/demo_last_run.db")
                if dest_db.exists():
                    try:
                        dest_db.unlink()
                    except Exception:
                        pass
                with sqlite3.connect(str(db_file)) as src_conn:
                    src_conn.execute(f"VACUUM INTO '{dest_db.as_posix()}';")
                print(f"[DEMO] Preserved demo database to data/demo_last_run.db")
        except Exception as e:
            print(f"[DEMO] Failed to preserve demo database: {e}")

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
            base_buffer = 45.0 if any(f.fault_type == "power_loss" for f in faults) else 25.0
            buffer_s = base_buffer + (25.0 if controller_flow else 0.0)
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
        incidents_list: List[Dict[str, Any]] = []

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

                    if controller_flow:
                        with get_db_connection(str(db_file)) as c_conn:
                            s_rows = c_conn.execute("SELECT candidate_id FROM sessions WHERE centre_id = ?;", (f.centre_id,)).fetchall()
                        for s_r in s_rows:
                            cid = s_r["candidate_id"]
                            try:
                                r_st = httpx.get(f"{api_url}/v1/status/{cid}", timeout=2.0)
                                if r_st.status_code == 200:
                                    mid_cand_statuses[cid] = r_st.json()
                            except Exception:
                                pass

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

            # Check if all incidents are resolved with impact computed
            if faults and all(f.post_printed for f in faults):
                try:
                    r_inc = httpx.get(f"{api_url}/v1/incidents", timeout=2.0)
                    if r_inc.status_code == 200:
                        inc_data = r_inc.json()
                        all_res = (
                            len(inc_data) >= len(faults)
                            and all(i.get("status") == "resolved" and i.get("impact_computed_at") for i in inc_data)
                        )
                        if all_res:
                            incidents_list = inc_data
                            print(f"\n[DEMO +{elapsed:.1f}s] All {len(faults)} incidents resolved and impact computed.")
                            break
                except Exception:
                    pass

            time.sleep(0.5)

        # Allow buffer to catch up while runner stays active
        time.sleep(1.0)

        # If not resolved during loop, poll for final settlement
        if faults and not incidents_list:
            expected_inc_count = len(faults)
            for _ in range(30):
                try:
                    r_inc = httpx.get(f"{api_url}/v1/incidents", timeout=2.0)
                    if r_inc.status_code == 200:
                        inc_data = r_inc.json()
                        all_resolved_and_computed = (
                            len(inc_data) >= expected_inc_count
                            and all(i.get("status") == "resolved" and i.get("impact_computed_at") for i in inc_data)
                        )
                        if all_resolved_and_computed:
                            incidents_list = inc_data
                            break
                except Exception:
                    pass
                time.sleep(0.5)

        # =======================================================================
        # VERIFICATION AND REPORTING (Runner still active for healthy telemetry)
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

            # Calculate reason breakdown
            reasons_count: Dict[str, int] = {}
            for r in rows:
                ev_str = r.get("evidence")
                if ev_str:
                    try:
                        ev_dict = json.loads(ev_str) if isinstance(ev_str, str) else ev_str
                        r_reas = ev_dict.get("reason")
                        if r_reas:
                            reasons_count[r_reas] = reasons_count.get(r_reas, 0) + 1
                    except Exception:
                        pass

            late_count = 0
            if db_file.exists():
                try:
                    with get_db_connection(str(db_file)) as conn:
                        we = inc.get("window_end")
                        if we:
                            row_l = conn.execute(
                                "SELECT COUNT(*) FROM events WHERE centre_id = ? AND ts <= ? AND ingested_at > ?;",
                                (cid, we, we)
                            ).fetchone()
                            if row_l:
                                late_count = row_l[0]
                except Exception:
                    pass

            print(f"    Reason Breakdown         : {reasons_count if reasons_count else {'clean': len(rows)}}")
            print(f"    Late Events Count        : {late_count}")

            # (2.b) Distribution of (window_start - last_good_ts) across candidates
            stale_vals = []
            for r in rows:
                ev_str = r.get("evidence")
                if ev_str:
                    try:
                        ev_dict = json.loads(ev_str) if isinstance(ev_str, str) else ev_str
                        st_s = ev_dict.get("stale_seconds")
                        if st_s is not None:
                            stale_vals.append(float(st_s))
                        elif ev_dict.get("last_good_ts") and inc.get("window_start"):
                            ws_d = parse_iso(inc["window_start"])
                            lg_d = parse_iso(ev_dict["last_good_ts"])
                            stale_vals.append((ws_d - lg_d).total_seconds())
                    except Exception:
                        pass

            if stale_vals:
                min_s = min(stale_vals)
                med_s = statistics.median(stale_vals)
                max_s = max(stale_vals)
                near_limit_c = sum(1 for s in stale_vals if 3.3 <= s <= 3.6)
                over_limit_c = sum(1 for s in stale_vals if s > 3.6)
                print(f"\n  (2.b) Staleness Distribution (window_start - last_good_ts) [N={len(stale_vals)}]:")
                print(f"    Min    : {min_s:.3f} s")
                print(f"    Median : {med_s:.3f} s")
                print(f"    Max    : {max_s:.3f} s")
                print(f"    Count within 0.3s of 3.6s limit (3.3s - 3.6s): {near_limit_c}")
                if over_limit_c > 0:
                    print(f"    Count exceeding 3.6s limit (>3.6s)            : {over_limit_c}")

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

        # Multi-fault average extra seconds per centre (Always printed, also in single-centre runs)
        print("\n  Average Extra Seconds Per Centre (Fairness Benchmark):")
        if centre_avg_extra:
            for cid, avg_s in sorted(centre_avg_extra.items()):
                print(f"    {cid}: {avg_s:.1f} s")
        else:
            print("    No compensation required.")

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

        # Clean run verification (No faults)
        if not faults or len(faults) == 0:
            with get_db_connection(str(db_file)) as conn:
                inc_c = conn.execute("SELECT COUNT(*) as c FROM incidents;").fetchone()["c"]
                dec_c = conn.execute("SELECT COUNT(*) as c FROM decisions;").fetchone()["c"]
                not_c = conn.execute("SELECT COUNT(*) as c FROM notices;").fetchone()["c"]
            print("\n" + "=" * 78)
            print("CLEAN RUN AUDIT & ACTIVITY VERIFICATION (No Faults)")
            print("=" * 78)
            print(f"  Total Incidents : {inc_c} (Expected: 0)")
            print(f"  Total Decisions : {dec_c} (Expected: 0)")
            print(f"  Total Notices   : {not_c} (Expected: 0)")

        # Controller Workflow (--controller-flow)
        if controller_flow and incidents_list:
            print("\n" + "=" * 78)
            print("CONTROLLER WORKFLOW EXECUTION (--controller-flow)")
            print("=" * 78)

            # 1. Query Fairness API
            r_fair = httpx.get(f"{api_url}/v1/fairness", timeout=3.0)
            fair_data = r_fair.json() if r_fair.status_code == 200 else {}
            print(f"\n(1) FAIRNESS EVALUATION RESULT")
            print(f"  Overall Status : {fair_data.get('status')}")
            print(f"  Flags Raised   : {json.dumps(fair_data.get('flags'), indent=2)}")
            print("  Centre Statistics:")
            c_stats = fair_data.get("stats_per_centre") or fair_data.get("centres") or {}
            for cid, stat in c_stats.items():
                m_share = stat.get('share_manual_review', 0.0)
                e_share = stat.get('share_extra_time', 0.0)
                c_ratio = stat.get('compensation_ratio')
                print(f"    {cid}: rows={stat.get('rows')}, manual_review_share={m_share:.1%}, extra_time_share={e_share:.1%}, compensation_ratio={c_ratio}")

            # 2. Iterate through resolved incidents
            for inc in incidents_list:
                inc_id = inc["incident_id"]
                cid = inc["centre_id"]
                print(f"\n  -------------------------------------------------------------------")
                print(f"  INCIDENT DECISION CYCLE: {inc_id} ({cid})")
                print(f"  -------------------------------------------------------------------")

                r_imp = httpx.get(f"{api_url}/v1/incidents/{inc_id}/impact", timeout=3.0)
                imp_data = r_imp.json() if r_imp.status_code == 200 else {}
                rows = imp_data.get("rows") or []

                r1_cands = [r["candidate_id"] for r in rows if r["rule_id"] == "R1" or r["remedy_recommended"] == "resume"]
                r2_cands = [r["candidate_id"] for r in rows if r["rule_id"] == "R2" or r["remedy_recommended"] == "extra_time"]
                mr_cands = [r["candidate_id"] for r in rows if r["remedy_recommended"] == "manual_review"]

                c_r1 = r1_cands[0] if r1_cands else (rows[0]["candidate_id"] if rows else None)
                c_r2 = r2_cands[0] if r2_cands else (rows[1]["candidate_id"] if len(rows) > 1 else None)
                c_mr = mr_cands[0] if mr_cands else None

                tracked = [("R1 (Resume)", c_r1), ("R2 (Extra Time)", c_r2), ("Manual Review", c_mr)]

                print("\n  [MOMENT 1: MID-OUTAGE STATUS]")
                for label, cand_id in tracked:
                    if cand_id:
                        mid_st = mid_cand_statuses.get(cand_id, {})
                        rem_info = mid_st.get("remedy") or {}
                        print(f"    {label:<18} [{cand_id}]: state={mid_st.get('session_state')} | remedy_status={rem_info.get('status')} | msg=\"{mid_st.get('latest_message')}\"")

                print("\n  [MOMENT 2: AFTER IMPACT COMPUTATION & BEFORE DECISION]")
                for label, cand_id in tracked:
                    if cand_id:
                        st = httpx.get(f"{api_url}/v1/status/{cand_id}", timeout=2.0).json()
                        rem_info = st.get("remedy") or {}
                        print(f"    {label:<18} [{cand_id}]: state={st.get('session_state')} | remedy_status={rem_info.get('status')} | decision={rem_info.get('decision')} | msg=\"{st.get('latest_message')}\"")

                # Approve-All execution
                ctrl_headers = {"X-Controller-Key": "demo-controller-key"}
                print("\n  [CONTROLLER ACTION: APPROVE-ALL]")
                app_res = httpx.post(
                    f"{api_url}/v1/incidents/{inc_id}/decisions/approve-all",
                    headers=ctrl_headers,
                    json={"decided_by": "controller-lead", "acknowledge_fairness": False},
                    timeout=5.0,
                )
                if app_res.status_code == 409:
                    print(f"    Approve-All Gate: BLOCKED by fairness check (HTTP 409): {app_res.json().get('detail')}")
                    print("    Retrying Approve-All with acknowledge_fairness=True...")
                    app_res = httpx.post(
                        f"{api_url}/v1/incidents/{inc_id}/decisions/approve-all",
                        headers=ctrl_headers,
                        json={"decided_by": "controller-lead", "acknowledge_fairness": True},
                        timeout=5.0,
                    )
                    print(f"    Approve-All with Acknowledgement: HTTP {app_res.status_code} => {app_res.json()}")
                else:
                    print(f"    Approve-All Gate: ALLOWED (HTTP {app_res.status_code}) => {app_res.json()}")

                # Override manual review row
                if c_mr:
                    print("\n  [CONTROLLER ACTION: OVERRIDE ONE MANUAL REVIEW ROW]")
                    ov_res = httpx.post(
                        f"{api_url}/v1/decisions",
                        headers=ctrl_headers,
                        json={
                            "incident_id": inc_id,
                            "candidate_id": c_mr,
                            "action": "override",
                            "remedy": "extra_time",
                            "extra_seconds": 120,
                            "reason": "Terminal battery degraded faster than expected during power sag",
                            "decided_by": "controller-lead",
                        },
                        timeout=5.0,
                    )
                    print(f"    Manual Review Override for {c_mr}: HTTP {ov_res.status_code} => {ov_res.json()}")

                print("\n  [MOMENT 3: AFTER DECISION APPLIED]")
                for label, cand_id in tracked:
                    if cand_id:
                        st = httpx.get(f"{api_url}/v1/status/{cand_id}", timeout=2.0).json()
                        rem_info = st.get("remedy") or {}
                        print(f"    {label:<18} [{cand_id}]: state={st.get('session_state')} | remedy_status={rem_info.get('status')} | decision={rem_info.get('decision')} | msg=\"{st.get('latest_message')}\"")

            # Candidate Notices Max Latency
            with get_db_connection(str(db_file)) as conn:
                notice_rows = conn.execute(
                    """
                    SELECT n.created_at as n_created, i.detected_at as i_detected
                    FROM notices n
                    JOIN incidents i ON n.incident_id = i.incident_id
                    WHERE n.kind = 'incident_opened';
                    """
                ).fetchall()
                latencies = []
                for nr in notice_rows:
                    dt_n = parse_iso(nr["n_created"])
                    dt_i = parse_iso(nr["i_detected"])
                    latencies.append((dt_n - dt_i).total_seconds())

                max_lat = max(latencies) if latencies else 0.0
                print(f"\n(3) CANDIDATE NOTICES LATENCY METRIC")
                print(f"  'incident_opened' Notices Count : {len(latencies)}")
                print(f"  Max Latency (created_at - detected_at): {max_lat:.3f} s (Target: <= 30.0 s)")

            # Final session states and centre status
            print(f"\n(4) FINAL SESSION STATES & CENTRE STATUS")
            for f in faults:
                with get_db_connection(str(db_file)) as conn:
                    st_rows = conn.execute(
                        "SELECT state, count(*) as c FROM sessions WHERE centre_id = ? GROUP BY state;",
                        (f.centre_id,),
                    ).fetchall()
                    st_map = {r["state"]: r["c"] for r in st_rows}
                r_c = httpx.get(f"{api_url}/v1/centres", timeout=2.0)
                centres_data = r_c.json() if r_c.status_code == 200 else []
                c_match = next((c for c in centres_data if c["centre_id"] == f.centre_id), None)
                c_status = c_match.get("status") if c_match else "unknown"
                resumed_or_review = st_map.get("resumed", 0) + st_map.get("under_review", 0)
                print(f"  Centre {f.centre_id}: Status={c_status} (Expected: healthy)")
                print(f"    Session States: {st_map} (resumed + under_review = {resumed_or_review})")

            # Audit counts by entry_type
            with get_db_connection(str(db_file)) as conn:
                audit_type_rows = conn.execute(
                    "SELECT entry_type, count(*) as c FROM audit_log GROUP BY entry_type ORDER BY entry_type;"
                ).fetchall()
                print(f"\n(5) AUDIT COUNTS BY ENTRY_TYPE")
                for atr in audit_type_rows:
                    print(f"  {atr['entry_type']:<15}: {atr['c']}")
                dec_seqs = [r["seq"] for r in conn.execute("SELECT seq FROM audit_log WHERE entry_type = 'decision' ORDER BY seq;").fetchall()]
                print(f"  Decision Audit Entry Seqs: {dec_seqs[:10]}... (Total {len(dec_seqs)})")

            # Final Audit Chain Verification
            ver_res = verify_audit_chain(str(db_file))
            print(f"\n(6) FINAL AUDIT HASH CHAIN VERIFICATION")
            print(f"  Result : ok={ver_res.ok}, total_entries={ver_res.total_entries}, head={ver_res.head_hash[:16]}...")

        print("=" * 78)

        # Stop simulator runner cleanly now that all reports and controller actions are complete
        if runner:
            runner.running = False
        if 'runner_thread' in locals() and runner_thread.is_alive():
            runner_thread.join(timeout=5.0)

    finally:
        cleanup()
        sys.stdout = old_stdout
        tee.close()
        print(f"[DEMO] Complete report and log written to: {log_file_path}")


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
    parser.add_argument("--controller-flow", action="store_true", help="Execute decisions, fairness check, and notices flow")

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
        controller_flow=args.controller_flow,
    )


if __name__ == "__main__":
    main()
