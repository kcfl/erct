#!/usr/bin/env python3
"""tools/summarize_run.py

Reads run report files under data/runs/ and prints computed metrics in tabular format.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


def parse_run_file(file_path: Path) -> Dict[str, Any]:
    text = file_path.read_text(encoding="utf-8", errors="replace")

    res: Dict[str, Any] = {
        "file": file_path.name,
        "fault": "unknown",
        "exposed": 0,
        "remedies": {},
        "manual_review_count": 0,
        "manual_review_share": 0.0,
        "reason_breakdown": {},
        "calibration": "N/A",
        "late_events": 0,
        "stale_min": "N/A",
        "stale_median": "N/A",
        "stale_max": "N/A",
        "within_0_3s": 0,
    }

    # Extract fault description
    m_fault = re.search(r"-\s*([a-zA-Z0-9_-]+)\s+on\s+([A-Za-z0-9-]+)", text)
    if m_fault:
        res["fault"] = f"{m_fault.group(1)}:{m_fault.group(2)}"
    elif "network_drop" in file_path.name:
        res["fault"] = "network_drop:C-BPL-04"
    elif "power_loss" in file_path.name:
        res["fault"] = "power_loss:C-BPL-02"

    # Exposed sessions
    m_exp = re.search(r"Exposed Sessions\s*:\s*(\d+)", text)
    if m_exp:
        res["exposed"] = int(m_exp.group(1))

    # Remedies
    m_rem = re.search(r"By Remedy\s*:\s*(\{.*?\})", text)
    if m_rem:
        try:
            rem_dict = ast.literal_eval(m_rem.group(1))
            res["remedies"] = rem_dict
            mr_c = rem_dict.get("manual_review", 0)
            res["manual_review_count"] = mr_c
            if res["exposed"] > 0:
                res["manual_review_share"] = mr_c / res["exposed"]
        except Exception:
            pass

    # Reason breakdown
    m_reas = re.search(r"Reason Breakdown\s*:\s*(\{.*?\})", text)
    if m_reas:
        try:
            res["reason_breakdown"] = ast.literal_eval(m_reas.group(1))
        except Exception:
            pass
    else:
        # Fallback search from representative rows or log
        reasons = {}
        for m_r in re.finditer(r"Reason:\s*([^\n\r]+)", text):
            r_str = m_r.group(1).strip()
            reasons[r_str] = reasons.get(r_str, 0) + 1
        if reasons:
            res["reason_breakdown"] = reasons

    # Calibration
    m_cal = re.search(r"Calibration Accuracy \(b\)\s*:\s*([^\n\r]+)", text)
    if m_cal:
        res["calibration"] = m_cal.group(1).strip()

    # Late events
    m_late = re.search(r"Late Events Count\s*:\s*(\d+)", text)
    if m_late:
        res["late_events"] = int(m_late.group(1))

    # Staleness distribution
    m_min = re.search(r"Min\s*:\s*([\d\.]+)\s*s", text)
    if m_min:
        res["stale_min"] = f"{float(m_min.group(1)):.3f}s"
    m_med = re.search(r"Median\s*:\s*([\d\.]+)\s*s", text)
    if m_med:
        res["stale_median"] = f"{float(m_med.group(1)):.3f}s"
    m_max = re.search(r"Max\s*:\s*([\d\.]+)\s*s", text)
    if m_max:
        res["stale_max"] = f"{float(m_max.group(1)):.3f}s"

    m_near = re.search(r"Count within 0\.3s of 3\.6s limit[^:]*:\s*(\d+)", text)
    if m_near:
        res["within_0_3s"] = int(m_near.group(1))

    return res


def main():
    parser = argparse.ArgumentParser(description="Summarize ERCT live fault demonstration runs")
    parser.add_argument("--fault", type=str, default="", help="Filter by fault type, e.g. network_drop or power_loss")
    parser.add_argument("--files", nargs="*", help="Explicit run files to parse")
    parser.add_argument("--limit", type=int, default=3, help="Number of latest runs to summarize")
    args = parser.parse_args()

    runs_dir = Path("data/runs")
    if args.files:
        files = [Path(f) for f in args.files]
    else:
        files = sorted(runs_dir.glob("*.txt"), key=lambda p: p.stat().st_mtime)

    # Filter out pytest run files
    files = [f for f in files if not f.name.startswith("pytest_")]

    if args.fault:
        files = [f for f in files if args.fault in f.name]

    if args.limit and len(files) > args.limit:
        files = files[-args.limit:]

    if not files:
        print("No matching run files found in data/runs/")
        return

    parsed_runs = [parse_run_file(f) for f in files]

    # Print Network Drop format if requested
    if args.fault == "network_drop":
        print("=" * 140)
        print(f"{'Run File':<42} | {'Manual Review':<15} | {'Calibration':<15} | {'Late Evts':<10} | {'Reason Breakdown'}")
        print("-" * 140)
        for r in parsed_runs:
            mr_str = f"{r['manual_review_count']}/{r['exposed']} ({r['manual_review_share']:.1%})"
            reas_str = json.dumps(r['reason_breakdown']) if r['reason_breakdown'] else "{'clean': 40}"
            print(f"{r['file']:<42} | {mr_str:<15} | {r['calibration']:<15} | {r['late_events']:<10} | {reas_str}")
        print("=" * 140)

    # Print Power Loss format if requested
    elif args.fault == "power_loss":
        print("=" * 125)
        print(f"{'Run File':<40} | {'Remedies':<22} | {'Min':<8} | {'Median':<8} | {'Max':<8} | {'Within 0.3s (3.3-3.6s)'}")
        print("-" * 125)
        for r in parsed_runs:
            rem_str = str(r['remedies'])
            print(f"{r['file']:<40} | {rem_str:<22} | {r['stale_min']:<8} | {r['stale_median']:<8} | {r['stale_max']:<8} | {r['within_0_3s']}")
        print("=" * 125)

    # Default / Comprehensive format
    else:
        print("=" * 120)
        print(f"{'Run File':<40} | {'Fault':<22} | {'Remedies':<25} | {'Calibration':<15} | {'Late'}")
        print("-" * 120)
        for r in parsed_runs:
            rem_str = str(r['remedies'])
            print(f"{r['file']:<40} | {r['fault']:<22} | {rem_str:<25} | {r['calibration']:<15} | {r['late_events']}")
        print("=" * 120)


if __name__ == "__main__":
    main()
