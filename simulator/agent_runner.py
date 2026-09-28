"""Multi-centre and candidate examination simulator with decoupled generation, sender thread, and fault management."""
from __future__ import annotations

import argparse
import hashlib
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
import httpx

from app.config import get_config
from app.models.events import EventEnvelope, EventType, Severity
from simulator.buffer import StoreAndForwardBuffer


class ActiveFault:
    def __init__(self, command_id: int, centre_id: str, fault_type: str, duration_s: int, params: Dict[str, Any]):
        self.command_id = command_id
        self.centre_id = centre_id
        self.fault_type = fault_type
        self.duration_s = duration_s
        self.params = params
        self.start_mono = time.monotonic()
        self.start_utc = datetime.now(timezone.utc).isoformat()
        self.ended = False

    @property
    def is_expired(self) -> bool:
        return (time.monotonic() - self.start_mono) >= self.duration_s


class CentreAgent:
    """Simulates physical edge telemetry for an examination centre."""

    def __init__(self, centre_cfg: Any, exam_id: str):
        self.centre_id = centre_cfg.id
        self.api_key = centre_cfg.api_key
        self.backup_minutes = centre_cfg.backup_minutes
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

    def generate_power_loss(self) -> EventEnvelope:
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=None,
            session_id=None,
            seq=self.seq,
            type=EventType.POWER_LOSS,
            severity=Severity.CRITICAL,
            payload={"source": "grid", "backup_minutes": self.backup_minutes},
        )
        self.seq += 1
        return ev

    def generate_power_restored(self) -> EventEnvelope:
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=None,
            session_id=None,
            seq=self.seq,
            type=EventType.POWER_RESTORED,
            severity=Severity.INFO,
            payload={"source": "grid", "backup_minutes": self.backup_minutes},
        )
        self.seq += 1
        return ev

    def generate_network_down(self) -> EventEnvelope:
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=None,
            session_id=None,
            seq=self.seq,
            type=EventType.NETWORK_DOWN,
            severity=Severity.ERROR,
            payload={"uplink": "primary_fiber"},
        )
        self.seq += 1
        return ev

    def generate_network_up(self, duration_s: int) -> EventEnvelope:
        ev = EventEnvelope(
            event_id=str(uuid.uuid4()),
            schema_ver=1,
            ts=datetime.now(timezone.utc).isoformat(),
            exam_id=self.exam_id,
            centre_id=self.centre_id,
            candidate_id=None,
            session_id=None,
            seq=self.seq,
            type=EventType.NETWORK_UP,
            severity=Severity.INFO,
            payload={"uplink": "primary_fiber", "duration_s": duration_s},
        )
        self.seq += 1
        return ev


class CandidateClient:
    """Simulates an individual candidate examination terminal session."""

    def __init__(
        self,
        candidate_id: str,
        session_id: str,
        centre_id: str,
        api_key: str,
        exam_id: str,
        duration_min: int,
        is_flaky: bool = False,
    ):
        self.candidate_id = candidate_id
        self.session_id = session_id
        self.centre_id = centre_id
        self.api_key = api_key
        self.exam_id = exam_id
        self.duration_min = duration_min
        self.remaining_s = duration_min * 60
        self.seq = 1
        self.local_seq = 0
        self.saved_seq = 0
        self.is_flaky = is_flaky
        self.rng = random.Random(f"{candidate_id}-seed")

        # Deterministic unsaved answers skew (0 to 3 answers ahead initially)
        self.local_seq = self.rng.randint(0, 3)

    def start_session(self) -> EventEnvelope:
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

    def tick_heartbeat(self, elapsed_real_s: float, clock_speed: float = 1.0) -> Optional[EventEnvelope]:
        # Flaky candidates randomly skip ~30% of heartbeats
        if self.is_flaky and self.rng.random() < 0.30:
            return None

        decrement = int(elapsed_real_s * clock_speed)
        self.remaining_s = max(0, self.remaining_s - decrement)

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
                "latency_ms": self.rng.randint(15, 65),
                "remaining_s": self.remaining_s,
                "local_seq": self.local_seq,
            },
        )
        self.seq += 1
        return ev

    def advance_answer(self) -> Optional[EventEnvelope]:
        """Candidate submits an answer locally, with ~40% chance of triggering immediate server save."""
        self.local_seq += 1
        if self.rng.random() < 0.40 or (self.local_seq - self.saved_seq) > 3:
            self.saved_seq = self.local_seq
            ans_hash = hashlib.sha256(f"{self.candidate_id}:Q{self.saved_seq}".encode()).hexdigest()[:16]
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
                    "question_id": f"Q{self.saved_seq:02d}",
                    "answer_hash": ans_hash,
                    "saved_seq": self.saved_seq,
                },
            )
            self.seq += 1
            return ev
        return None

    def on_power_loss(self) -> None:
        """Power loss wipes all unsaved local answers."""
        self.local_seq = self.saved_seq


class SenderThread(threading.Thread):
    """Background worker that continuously drains the buffer in batches up to 500."""

    def __init__(self, buffer: StoreAndForwardBuffer, api_base_url: str):
        super().__init__(name="SenderThread", daemon=True)
        self.buffer = buffer
        self.api_base_url = api_base_url
        self.running = True
        self.total_dispatched = 0
        self.http_client = httpx.Client(timeout=30.0)

    def run(self) -> None:
        try:
            while self.running:
                try:
                    res = self.buffer.drain_once(
                        api_base_url=self.api_base_url,
                        max_batch_size=500,
                        client=self.http_client,
                    )
                    dispatched = res["dispatched"]
                    self.total_dispatched += dispatched

                    if dispatched == 0:
                        # Buffer empty or all pending items paused/backing off: sleep briefly
                        time.sleep(0.02)
                except Exception as e:
                    print(f"[SENDER THREAD ERROR] {type(e).__name__}: {e}")
                    time.sleep(0.05)
        finally:
            try:
                self.http_client.close()
            except Exception:
                pass


class SimulatorRunner:
    """Coordinates monotonic event generation, sender thread, and fault lifecycles."""

    def __init__(
        self,
        api_base_url: str = "http://127.0.0.1:8000",
        buffer_db_path: str = "data/simulator_buffer.db",
        heartbeat_override_s: Optional[float] = None,
    ):
        self.api_base_url = api_base_url
        self.buffer = StoreAndForwardBuffer(buffer_db_path)
        self.cfg = get_config()
        self.exam_id = self.cfg.exam.id
        self.heartbeat_interval_s = heartbeat_override_s or float(self.cfg.simulation.heartbeat_interval_s)
        self.exam_clock_speed = getattr(self.cfg.exam, "exam_clock_speed", 1.0)
        self.control_key = getattr(self.cfg.control, "key", "ctrl-secret-key-2026")
        self.running = False

        self.centre_agents: Dict[str, CentreAgent] = {}
        self.candidate_clients: List[CandidateClient] = []
        self.active_faults: Dict[str, ActiveFault] = {}  # centre_id -> ActiveFault
        self.processed_config_faults: Set[str] = set()
        self.peak_backlog: int = 0

        self._init_entities()
        self.http_client = httpx.Client(timeout=2.0)
        self.sender = SenderThread(self.buffer, self.api_base_url)

    def _init_entities(self) -> None:
        global_cand_num = 1
        flaky_threshold = self.cfg.simulation.flaky_fraction

        for c in self.cfg.centres:
            self.centre_agents[c.id] = CentreAgent(c, self.exam_id)

            for seat_idx in range(self.cfg.simulation.candidates_per_centre):
                cand_id = f"CAND-{global_cand_num:06d}"
                session_id = f"SES-{global_cand_num:06d}-1"
                is_flaky = (random.random() < flaky_threshold)

                client = CandidateClient(
                    candidate_id=cand_id,
                    session_id=session_id,
                    centre_id=c.id,
                    api_key=c.api_key,
                    exam_id=self.exam_id,
                    duration_min=self.cfg.exam.duration_min,
                    is_flaky=is_flaky,
                )
                self.candidate_clients.append(client)
                global_cand_num += 1

    def _poll_and_apply_faults(self, sim_elapsed_s: float) -> None:
        """Poll API control channel and inspect config.yaml faults."""
        # 1. Inspect config faults and submit them via the control API
        for f in self.cfg.simulation.faults:
            f_key = f"{f.centre}-{f.type}-{f.at_s}"
            if f_key not in self.processed_config_faults and f.at_s is not None and sim_elapsed_s >= f.at_s:
                self.processed_config_faults.add(f_key)
                try:
                    with httpx.Client(timeout=2.0) as client:
                        client.post(
                            f"{self.api_base_url.rstrip('/')}/v1/control/faults",
                            headers={"X-Control-Key": self.control_key},
                            json={
                                "centre_id": f.centre,
                                "fault_type": f.type,
                                "duration_s": f.duration_s or 30,
                                "params": {
                                    "source": getattr(f, "source", "grid"),
                                    "backup_minutes": getattr(f, "backup_minutes", 15),
                                },
                            },
                        )
                except Exception as e:
                    print(f"[SIMULATOR] Error submitting config fault to control API: {e}")

        # 2. Poll API control channel
        try:
            resp = self.http_client.get(
                f"{self.api_base_url.rstrip('/')}/v1/control/faults/pending",
                headers={"X-Control-Key": self.control_key},
            )
            if resp.status_code == 200:
                for item in resp.json():
                    self._activate_fault(
                        command_id=item["id"],
                        centre_id=item["centre_id"],
                        fault_type=item["fault_type"],
                        duration_s=item["duration_s"],
                        params=item["params"],
                    )
        except Exception:
            pass  # API temporarily offline or unreachable

    def _check_expired_faults(self) -> None:
        """Check for active faults that have exceeded their duration."""
        expired_centres = [cid for cid, af in self.active_faults.items() if af.is_expired]
        for cid in expired_centres:
            af = self.active_faults.pop(cid)
            self._deactivate_fault(af)

    def _activate_fault(self, command_id: int, centre_id: str, fault_type: str, duration_s: int, params: Dict[str, Any]) -> None:
        if centre_id in self.active_faults:
            return  # Already active on this centre

        af = ActiveFault(command_id, centre_id, fault_type, duration_s, params)
        self.active_faults[centre_id] = af
        c_agent = self.centre_agents.get(centre_id)
        if not c_agent:
            return

        print(f"[SIMULATOR] Activated {fault_type} on {centre_id} for {duration_s}s")

        if fault_type == "power_loss":
            # Edge agent emits last-gasp POWER_LOSS into buffer
            pl_ev = c_agent.generate_power_loss()
            self.buffer.enqueue(pl_ev, centre_id, c_agent.api_key)
            # Power loss wipes unsaved local answers
            for cl in self.candidate_clients:
                if cl.centre_id == centre_id:
                    cl.on_power_loss()

        elif fault_type == "network_drop":
            # Edge agent emits NETWORK_DOWN into buffer, then pauses delivery
            nd_ev = c_agent.generate_network_down()
            self.buffer.enqueue(nd_ev, centre_id, c_agent.api_key)
            self.buffer.pause_centre(centre_id)

    def _deactivate_fault(self, af: ActiveFault) -> None:
        centre_id = af.centre_id
        c_agent = self.centre_agents.get(centre_id)
        print(f"[SIMULATOR] Restored {af.fault_type} on {centre_id}")

        if af.fault_type == "power_loss" and c_agent:
            pr_ev = c_agent.generate_power_restored()
            self.buffer.enqueue(pr_ev, centre_id, c_agent.api_key)

        elif af.fault_type == "network_drop" and c_agent:
            elapsed = int(time.monotonic() - af.start_mono)
            nu_ev = c_agent.generate_network_up(duration_s=elapsed)
            self.buffer.enqueue(nu_ev, centre_id, c_agent.api_key)

        # Notify control API that fault is completed (sets ended_at in DB)
        if af.command_id > 0:
            try:
                with httpx.Client(timeout=2.0) as client:
                    client.post(
                        f"{self.api_base_url.rstrip('/')}/v1/control/faults/{af.command_id}/done",
                        headers={"X-Control-Key": self.control_key},
                    )
            except Exception:
                pass

        # For network drop, unpause buffer AFTER ending the fault command
        if af.fault_type == "network_drop":
            self.buffer.resume_centre(centre_id)

    def generate_step(self, step_idx: int) -> int:
        """Monotonic generation step: generates events and enqueues in buffer in bulk."""
        events_by_centre: Dict[str, List[EventEnvelope]] = {c.id: [] for c in self.cfg.centres}

        # Step 0 initialization
        if step_idx == 0:
            for cid, agent in self.centre_agents.items():
                events_by_centre[cid].append(agent.generate_version_report())
            for client in self.candidate_clients:
                events_by_centre[client.centre_id].append(client.start_session())

        # Periodic Centre edge telemetry (every 5 steps)
        if step_idx % 5 == 0:
            for cid, agent in self.centre_agents.items():
                if cid in self.active_faults and self.active_faults[cid].fault_type == "power_loss":
                    continue  # Machine off
                events_by_centre[cid].extend(
                    agent.generate_telemetry_samples(
                        active_sessions=self.cfg.simulation.candidates_per_centre,
                        max_sessions=50,
                    )
                )

        # Candidate Heartbeats and Answers
        for client in self.candidate_clients:
            cid = client.centre_id
            if cid in self.active_faults and self.active_faults[cid].fault_type == "power_loss":
                continue  # Machine powered off: emit nothing, buffer nothing

            hb = client.tick_heartbeat(self.heartbeat_interval_s, clock_speed=self.exam_clock_speed)
            if hb:
                events_by_centre[cid].append(hb)

            ans = client.advance_answer()
            if ans:
                events_by_centre[cid].append(ans)

        # Enqueue in bulk per centre
        total_queued = 0
        for cid, ev_list in events_by_centre.items():
            if ev_list:
                api_key = self.centre_agents[cid].api_key
                total_queued += self.buffer.enqueue_batch(ev_list, centre_id=cid, api_key=api_key)

        return total_queued

    def start(self, duration_s: Optional[float] = None) -> None:
        """Run generation loop using monotonic clock for zero drift."""
        self.running = True
        self.sender.start()
        step = 0
        start_mono = time.monotonic()
        peak_backlog = 0
        last_poll_mono = 0.0

        print(f"[SIMULATOR] Decoupled runner started. Heartbeat: {self.heartbeat_interval_s}s, clock_speed: {self.exam_clock_speed}")

        try:
            next_tick = start_mono
            while self.running:
                now_mono = time.monotonic()
                sim_elapsed_s = now_mono - start_mono

                # Poll control channel every 1.0 real second (or on first step)
                if now_mono - last_poll_mono >= 1.0 or step == 0:
                    self._poll_and_apply_faults(sim_elapsed_s)
                    last_poll_mono = now_mono

                self._check_expired_faults()

                queued = self.generate_step(step)
                pending = self.buffer.get_pending_count()
                self.peak_backlog = max(self.peak_backlog, pending)

                print(f"[SIMULATOR] Step {step:02d} (+{sim_elapsed_s:.1f}s): queued {queued} evts | {pending} pending (peak: {self.peak_backlog})")

                step += 1
                if duration_s and sim_elapsed_s >= duration_s:
                    print(f"[SIMULATOR] Reached duration {duration_s}s. Generation finished.")
                    break

                # Monotonic sleep schedule
                next_tick += self.heartbeat_interval_s
                sleep_s = max(0.001, next_tick - time.monotonic())
                time.sleep(sleep_s)

        except KeyboardInterrupt:
            print("[SIMULATOR] Stopped by user.")
        finally:
            self.running = False
            try:
                self.http_client.close()
            except Exception:
                pass

            # Wait up to 5s for sender thread to drain
            print("[SIMULATOR] Waiting up to 5s for sender to drain remaining events...")
            t_drain_start = time.time()
            while time.time() - t_drain_start < 5.0 and self.buffer.get_pending_count() > 0:
                time.sleep(0.1)

            final_pending = self.buffer.get_pending_count()
            self.sender.running = False
            print(f"[SIMULATOR] Done. Dispatched: {self.sender.total_dispatched}, Final pending: {final_pending}, Peak backlog: {self.peak_backlog}")


def main() -> None:
    parser = argparse.ArgumentParser(description="ERCT Simulator Runner")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--duration", type=float, default=None)
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--buffer-db", default="data/simulator_buffer.db")
    args = parser.parse_args()

    runner = SimulatorRunner(
        api_base_url=args.api_url,
        buffer_db_path=args.buffer_db,
        heartbeat_override_s=args.interval,
    )
    runner.start(duration_s=args.duration)


if __name__ == "__main__":
    main()
