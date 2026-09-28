"""Benchmark script: posts 5000 events in batches of 500 to measure events/sec throughput."""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import datetime, timezone
import httpx


def generate_batch(batch_size: int, start_seq: int, centre_id: str = "C-BPL-01") -> list:
    batch = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for i in range(batch_size):
        seq = start_seq + i
        batch.append(
            {
                "event_id": str(uuid.uuid4()),
                "schema_ver": 1,
                "ts": now_iso,
                "exam_id": "EX-2026-PS6-01",
                "centre_id": centre_id,
                "candidate_id": f"CAND-{(i % 40) + 1:06d}",
                "session_id": f"SES-{(i % 40) + 1:06d}-1",
                "seq": seq,
                "type": "HEARTBEAT",
                "severity": "info",
                "payload": {"latency_ms": 25, "remaining_s": 7000, "local_seq": 10},
            }
        )
    return batch


def run_benchmark(api_url: str = "http://127.0.0.1:8000", total_events: int = 5000, batch_size: int = 500) -> float:
    print(f"=== INGEST BENCHMARK: {total_events} events in batches of {batch_size} ===")
    target_url = f"{api_url.rstrip('/')}/v1/events"
    headers = {"X-API-Key": "key-cbpl01-secret", "Content-Type": "application/json"}

    batches = total_events // batch_size
    t_start = time.perf_counter()
    accepted_total = 0

    with httpx.Client(timeout=30.0) as client:
        for b_idx in range(batches):
            batch = generate_batch(batch_size=batch_size, start_seq=(b_idx * batch_size) + 1)
            b_t0 = time.perf_counter()
            resp = client.post(target_url, json=batch, headers=headers)
            b_elapsed = time.perf_counter() - b_t0

            if resp.status_code != 200:
                print(f"[FAIL] Batch {b_idx + 1} failed: {resp.status_code} - {resp.text}")
                sys.exit(1)

            data = resp.json()
            accepted = data.get("accepted", 0)
            accepted_total += accepted
            batch_rate = accepted / max(0.0001, b_elapsed)
            print(f"  Batch {b_idx + 1:02d}/{batches}: {accepted} events in {b_elapsed:.3f}s ({batch_rate:.1f} ev/s)")

    total_time = time.perf_counter() - t_start
    overall_rate = accepted_total / max(0.0001, total_time)

    print("\n=== BENCHMARK RESULTS ===")
    print(f"  Total Events Ingested: {accepted_total}")
    print(f"  Total Time:            {total_time:.3f} s")
    print(f"  Throughput:            {overall_rate:.1f} events/second")
    print(f"  Target (>= 400 ev/s):  {'PASS' if overall_rate >= 400 else 'FAIL'}")
    return overall_rate


if __name__ == "__main__":
    import subprocess
    import socket

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--events", type=int, default=5000)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    # Check if target server is live
    server_proc = None
    target_url = args.url
    try:
        r = httpx.get(f"{target_url}/v1/health", timeout=0.5)
        server_live = (r.status_code == 200)
    except Exception:
        server_live = False

    if not server_live:
        print("[BENCH] Target server not live. Spawning ephemeral uvicorn server...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            free_port = s.getsockname()[1]
        target_url = f"http://127.0.0.1:{free_port}"
        server_proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(free_port), "--log-level", "warning"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(30):
            try:
                r = httpx.get(f"{target_url}/v1/health", timeout=0.5)
                if r.status_code == 200:
                    break
            except Exception:
                time.sleep(0.1)

    try:
        run_benchmark(api_url=target_url, total_events=args.events, batch_size=args.batch_size)
    finally:
        if server_proc:
            print("[BENCH] Terminating ephemeral uvicorn server...")
            server_proc.terminate()
            server_proc.wait()

