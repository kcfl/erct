"""Multi-centre and candidate agent simulator runner with store-and-forward telemetry buffering."""
from __future__ import annotations

import argparse
import hashlib
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.config import get_config
from app.models.events import EventEnvelope, EventType, Severity
from simulator.buffer import StoreAndForwardBuffer


class CentreAgent:
    """Simulates edge agent telemetry for a physical examination centre."""

    def __init__(self, centre_cfg: Any, exam_id: str):
        self.centre_id = centre_cfg.id
        self.api_key = centre_cfg.api_key
        self.software_version = centre_cfg.software_version
        self.exam_id = exam_id
        self.seq = 1

    def generate_version_report(self) -> EventEnvelope:
        cfg = get_config()
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=None,
            session_id=None,
            seq=self.seq,
            type=EventType.SOFTWARE_VERSION_REPORT,
            severity=Severity.INFO,
            payload={
                "version": self.software_version,
                "required_version": cfg.exam.required_version,
            },
        )
        self.seq += 1
        return ev

    def generate_telemetry_samples(self, active_sessions: int, max_sessions: int) -> List[EventEnvelope]:
        now_iso = datetime.now(timezone.utc).isoformat()
        events = []

        # Latency sample
        lat_ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=now_iso,
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=None,
            session_id=None,
            seq=self.seq,
            type=EventType.LATENCY_SAMPLE,
            severity=Severity.INFO,
            payload={
                "p50_ms": random.randint(20, 45),
                "p95_ms": random.randint(55, 95),
                "error_rate": 0.0,
            },
        )
        self.seq += 1
        events.append(lat_ev)

        # Capacity sample
        cap_ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=now_iso,
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=None,
            session_id=None,
            seq=self.seq,
            type=EventType.CAPACITY_SAMPLE,
            severity=Severity.INFO,
            payload={
                "active_sessions": active_sessions,
                "max_sessions": max_sessions,
                "cpu_pct": round(random.uniform(15.0, 35.0), 1),
            },
        )
        self.seq += 1
        events.append(cap_ev)

        return events


class CandidateClient:
    """Simulates a candidate examination terminal session."""

    def __init__(self, candidate_id: str, session_id: str, centre_id: str, api_key: str, exam_id: str, duration_min: int):
        self.candidate_id = candidate_id
        self.session_id = session_id
        self.centre_id = centre_id
        self.api_key = api_key
        self.exam_id = exam_id
        self.duration_min = duration_min
        self.remaining_s = duration_min * 60
        self.seq = 1
        self.saved_answers_count = 0
        self.session_started = False

    def start_session(self) -> EventEnvelope:
        self.session_started = True
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=self.candidate_id,
            session_id=self.session_id,
            seq=self.seq,
            type=EventType.SESSION_STARTED,
            severity=Severity.INFO,
            payload={"duration_s": self.duration_min * 60},
        )
        self.seq += 1
        return ev

    def tick_heartbeat(self, elapsed_s: int) -> EventEnvelope:
        self.remaining_s = max(0, self.remaining_s - elapsed_s)
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=self.candidate_id,
            session_id=self.session_id,
            seq=self.seq,
            type=EventType.HEARTBEAT,
            severity=Severity.INFO,
            payload={
                "latency_ms": random.randint(15, 65),
                "remaining_s": self.remaining_s,
            },
        )
        self.seq += 1
        return ev

    def save_answer(self, q_num: int) -> EventEnvelope:
        self.saved_answers_count += 1
        ans_hash = hashlib.sha256(f"{self.candidate_id}:Q{q_num}:ANS".encode()).hexdigest()[:16]
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=self.candidate_id,
            session_id=self.session_id,
            seq=self.seq,
            type=EventType.ANSWER_SAVED,
            severity=Severity.INFO,
            payload={
                "question_id": f"Q{q_num:02d}",
                "answer_hash": ans_hash,
                "saved_seq": self.saved_answers_count,
            },
        )
        self.seq += 1
        return ev


class SimulatorRunner:
    """Coordinates edge agents, candidate sessions, and store-and-forward dispatch."""

    def __init__(
        self,
        api_base_url: str = "http://127.0.0.1:8000",
        buffer_db_path: str = "data/simulator_buffer.db",
        time_compression: Optional[float] = None,
    ):
        self.api_base_url = api_base_url
        self.buffer = StoreAndForwardBuffer(buffer_db_path)
        self.cfg = get_config()
        self.exam_id = self.cfg.exam.id
        self.time_compression = time_compression or getattr(self.cfg.exam, "demo_time_scale", 1.0) or 1.0
        self.running = False
        self._init_entities()

    def _init_entities(self) -> None:
        self.centre_agents: Dict[str, CentreAgent] = {}
        self.candidate_clients: List[CandidateClient] = []

        global_cand_num = 1
        for c in self.cfg.centres:
            self.centre_agents[c.id] = CentreAgent(c, self.exam_id)

            for _ in range(self.cfg.simulation.candidates_per_centre):
                cand_id = f"CAND-{global_cand_num:06d}"
                session_id = f"SES-{global_cand_num:06d}-1"
                client = CandidateClient(
                    candidate_id=cand_id,
                    session_id=session_id,
                    centre_id=c.id,
                    api_key=c.api_key,
                    exam_id=self.exam_id,
                    duration_min=self.cfg.exam.duration_min,
                )
                self.candidate_clients.append(client)
                global_cand_num += 1

    def run_step(self, step_number: int) -> int:
        """Run a single simulation cycle: emit version reports, heartbeats, and answers into the buffer."""
        buffered_this_step = 0

        # Step 0: Emit version report and start sessions
        if step_number == 0:
            for c_agent in self.centre_agents.values():
                v_ev = c_agent.generate_version_report()
                if self.buffer.enqueue(v_ev, c_agent.centre_id, c_agent.api_key):
                    buffered_this_step += 1

            for client in self.candidate_clients:
                start_ev = client.start_session()
                if self.buffer.enqueue(start_ev, client.centre_id, client.api_key):
                    buffered_this_step += 1

        # Periodic Centre edge telemetry (every 5 steps)
        if step_number % 5 == 0:
            for c_agent in self.centre_agents.values():
                samples = c_agent.generate_telemetry_samples(
                    active_sessions=self.cfg.simulation.candidates_per_centre,
                    max_sessions=50,
                )
                for s_ev in samples:
                    if self.buffer.enqueue(s_ev, c_agent.centre_id, c_agent.api_key):
                        buffered_this_step += 1

        # Candidate Heartbeats and occasional Answer Saves
        interval_s = self.cfg.simulation.heartbeat_interval_s
        for client in self.candidate_clients:
            hb_ev = client.tick_heartbeat(elapsed_s=interval_s)
            if self.buffer.enqueue(hb_ev, client.centre_id, client.api_key):
                buffered_this_step += 1

            # Save answer with ~15% chance per cycle
            if random.random() < 0.15:
                ans_ev = client.save_answer(q_num=client.saved_answers_count + 1)
                if self.buffer.enqueue(ans_ev, client.centre_id, client.api_key):
                    buffered_this_step += 1

        # Drain buffered events to API
        self.buffer.drain_once(api_base_url=self.api_base_url, max_batch_size=100)
        return buffered_this_step

    def start(self, duration_s: Optional[float] = None) -> None:
        """Run continuous simulation loop with time compression."""
        self.running = True
        step = 0
        start_time = time.time()
        base_interval = self.cfg.simulation.heartbeat_interval_s / max(0.1, self.time_compression)

        print(f"[SIMULATOR] Started 5 centres x 40 candidates. Interval: {base_interval:.2f}s (scale={self.time_compression}x)")

        try:
            while self.running:
                loop_start = time.time()
                buffered = self.run_step(step)
                pending = self.buffer.get_pending_count()
                print(f"[SIMULATOR] Step {step}: queued {buffered} events, {pending} pending in buffer")

                step += 1
                if duration_s and (time.time() - start_time) >= duration_s:
                    print(f"[SIMULATOR] Target duration {duration_s}s reached. Stopping.")
                    break

                elapsed = time.time() - loop_start
                sleep_time = max(0.05, base_interval - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            print("[SIMULATOR] Interrupted by user.")
        finally:
            self.running = False


def main() -> None:
    parser = argparse.ArgumentParser(description="ERCT Examination Simulation Agent Runner")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000", help="ERCT API Base URL")
    parser.add_argument("--duration", type=float, default=None, help="Run duration in seconds")
    parser.add_argument("--buffer-db", default="data/simulator_buffer.db", help="Path to buffer database")
    parser.add_argument("--scale", type=float, default=None, help="Time compression scale factor")
    args = parser.parse_args()

    runner = SimulatorRunner(
        api_base_url=args.api_url,
        buffer_db_path=args.buffer_db,
        time_compression=args.scale,
    )
    runner.start(duration_s=args.duration)


if __name__ == "__main__":
    main()
