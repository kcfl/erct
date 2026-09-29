"""Replay Speed Benchmark: Tests draining a 6,000-event backlog after centre resumption.
Target: >= 1,500 events/second.
"""
from __future__ import annotations

import os
import socket
import sys
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
import pytest
import uvicorn
import yaml

from app.config import reload_config
from app.core.audit_chain import verify_audit_chain
from app.db import format_utc_iso, get_db_connection, init_db
from app.main import app, seed_database
from simulator.agent_runner import SenderThread
from simulator.buffer import StoreAndForwardBuffer


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(url: str, timeout: float = 6.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = httpx.get(f"{url}/v1/health", timeout=0.5)
            if r.status_code == 200:
                return True
        except Exception:
            time.sleep(0.1)
    return False


def run_benchmark(event_count: int = 6000) -> Dict[str, Any]:
    port = find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    tmp_dir = Path("data/bench_replay_tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_file = tmp_dir / f"bench_{int(time.time())}.db"
    buf_file = tmp_dir / f"buf_{int(time.time())}.db"
    cfg_file = tmp_dir / f"cfg_{int(time.time())}.yaml"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="warning", ws="none")
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    assert wait_for_server(base_url), "Server failed to start"

    sender = None
    try:
        buffer = StoreAndForwardBuffer(str(buf_file))
        centre_id = "C-BPL-04"
        api_key = "key-cbpl04-secret"

        # 1. Pause the centre to simulate network outage accumulation
        buffer.pause_centre(centre_id)
        assert buffer.is_centre_paused(centre_id) is True

        # 2. Enqueue 6,000 events in batches of 1,000
        now = datetime.now(timezone.utc)
        print(f"\n[BENCHMARK] Enqueuing {event_count} events for paused centre {centre_id}...")
        t_enq_start = time.perf_counter()

        batch_chunk_size = 1000
        total_enqueued = 0
        for chunk_idx in range(event_count // batch_chunk_size):
            chunk = []
            for i in range(batch_chunk_size):
                global_idx = chunk_idx * batch_chunk_size + i
                cand_idx = (global_idx % 40) + 1
                cand_id = f"CAND-{cand_idx:06d}"
                sess_id = f"SES-{cand_idx:06d}-1"
                t_ev = now - timedelta(seconds=120) + timedelta(milliseconds=global_idx * 10)
                chunk.append({
                    "event_id": str(uuid.uuid4()),
                    "schema_ver": 1,
                    "ts": format_utc_iso(t_ev),
                    "exam_id": "EX-2026-PS6-01",
                    "centre_id": centre_id,
                    "candidate_id": cand_id,
                    "session_id": sess_id,
                    "seq": global_idx + 1,
                    "type": "HEARTBEAT",
                    "payload": {
                        "latency_ms": 25,
                        "remaining_s": 7000 - (global_idx // 40),
                        "local_seq": 1,
                    },
                })
            inserted = buffer.enqueue_batch(chunk, centre_id=centre_id, api_key=api_key)
            total_enqueued += inserted

        t_enq_end = time.perf_counter()
        print(f"[BENCHMARK] Enqueued {total_enqueued} events in {t_enq_end - t_enq_start:.2f}s")
        assert buffer.get_pending_count(centre_id=centre_id) == event_count

        # 3. Resume centre and benchmark SenderThread drain speed
        print(f"[BENCHMARK] Resuming centre {centre_id} and starting SenderThread drain...")
        buffer.resume_centre(centre_id)
        assert buffer.is_centre_paused(centre_id) is False

        sender = SenderThread(buffer, base_url)
        t_drain_start = time.perf_counter()
        sender.start()

        # Wait until buffer is drained
        while True:
            pending = buffer.get_pending_count(centre_id=centre_id)
            if pending == 0:
                break
            time.sleep(0.01)

        t_drain_end = time.perf_counter()
        drain_elapsed = t_drain_end - t_drain_start
        drain_rate = event_count / drain_elapsed

        sender.running = False
        sender.join(timeout=2.0)

        # 4. Verify all events persisted in DB and audit chain is valid
        with get_db_connection(str(db_file)) as conn:
            c = conn.execute("SELECT COUNT(*) AS c FROM events WHERE centre_id = ?;", (centre_id,)).fetchone()["c"]
            assert c == event_count, f"Expected {event_count} events in DB, found {c}"

        audit_res = verify_audit_chain(str(db_file))
        assert audit_res.ok, f"Audit chain verification failed at seq {audit_res.failing_seq}: {audit_res.error}"

        print(f"[BENCHMARK RESULT] Drained {event_count} events in {drain_elapsed:.3f}s -> {drain_rate:.1f} events/s")
        print(f"[BENCHMARK RESULT] Audit trail verified valid: {audit_res.ok} (total: {audit_res.total_entries})")
        print(f"[BENCHMARK RESULT] Target >= 1500 events/s: {'PASS' if drain_rate >= 1500 else 'FAIL'}")

        return {
            "events": event_count,
            "elapsed_s": round(drain_elapsed, 3),
            "rate_events_per_s": round(drain_rate, 1),
            "audit_valid": audit_res.ok,
        }

    finally:
        if sender and sender.is_alive():
            sender.running = False
            sender.join(timeout=2.0)
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


def test_bench_replay():
    """Pytest wrapper: asserts that replay drain rate meets or exceeds 1,500 events/s."""
    results = run_benchmark(event_count=6000)
    assert results["rate_events_per_s"] >= 1500.0, (
        f"Replay throughput {results['rate_events_per_s']} ev/s below target 1500 ev/s"
    )
    assert results["audit_valid"] is True


if __name__ == "__main__":
    res = run_benchmark(event_count=6000)
    print(res)
