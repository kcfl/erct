"""FastAPI main application entrypoint for ERCT with deterministic database seeding."""
from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes_ingest import router as ingest_router
from app.api.routes_control import router as control_router
from app.api.routes_centres import router as centres_router
from app.api.routes_incidents import router as incidents_router, review_router
from app.api.routes_decisions import router as decisions_router
from app.api.routes_status import router as status_router
from app.config import get_config
from app.core.audit_chain import append_audit_entry, verify_audit_chain
from app.core.detection import DetectionEngine, DetectionWorker, parse_utc_iso
from app.db import format_utc_iso, get_db_connection, init_db, write_transaction


def seed_database(db_path: str = None) -> Dict[str, int]:
    """Deterministically seeds the exam, 5 centres, and 200 candidates/sessions.

    Strictly idempotent: uses INSERT OR IGNORE so running multiple times causes zero duplicates.
    """
    cfg = get_config()
    exam = cfg.exam
    config_hash = hashlib.sha256(exam.id.encode()).hexdigest()[:16]
    now_iso = datetime.now(timezone.utc).isoformat()

    counts = {"exams": 0, "centres": 0, "candidates": 0, "sessions": 0}

    with write_transaction(db_path) as conn:
        cursor = conn.cursor()

        # 1. Seed Exam
        cursor.execute(
            """
            INSERT OR IGNORE INTO exams (exam_id, name, start_at, duration_min, required_version, config_hash)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (exam.id, exam.name, now_iso, exam.duration_min, exam.required_version, config_hash),
        )
        counts["exams"] += cursor.rowcount

        # 2. Seed Centres
        for c in cfg.centres:
            cursor.execute(
                """
                INSERT OR IGNORE INTO centres (
                    centre_id, name, city, vendor, capacity,
                    power_backup, backup_minutes, software_version, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ready');
                """,
                (
                    c.id,
                    c.name,
                    c.city,
                    c.vendor,
                    c.capacity,
                    1 if c.power_backup else 0,
                    c.backup_minutes,
                    c.software_version,
                ),
            )
            counts["centres"] += cursor.rowcount

        # 3. Seed Candidates and Sessions (5 centres x 40 candidates = 200)
        global_cand_num = 1
        for c_idx, c in enumerate(cfg.centres):
            for seat_idx in range(1, cfg.simulation.candidates_per_centre + 1):
                cand_id = f"CAND-{global_cand_num:06d}"
                session_id = f"SES-{global_cand_num:06d}-1"
                seat_no = f"SEAT-{seat_idx:02d}"

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO candidates (candidate_id, exam_id, centre_id, seat_no, status)
                    VALUES (?, ?, ?, ?, 'registered');
                    """,
                    (cand_id, exam.id, c.id, seat_no),
                )
                counts["candidates"] += cursor.rowcount

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO sessions (
                        session_id, candidate_id, exam_id, centre_id,
                        started_at, last_heartbeat_at, last_saved_seq, answers_saved, remaining_s, state
                    ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, 'active');
                    """,
                    (
                        session_id,
                        cand_id,
                        exam.id,
                        c.id,
                        now_iso,
                        now_iso,
                        exam.duration_min * 60,
                    ),
                )
                counts["sessions"] += cursor.rowcount

                global_cand_num += 1

        cursor.close()

    # Log initial seed event to audit chain if this is a fresh database
    if counts["exams"] > 0:
        append_audit_entry(
            entry_type="config",
            ref_id=exam.id,
            payload={
                "action": "database_seed",
                "exam_id": exam.id,
                "centres_seeded": len(cfg.centres),
                "candidates_seeded": cfg.simulation.centres * cfg.simulation.candidates_per_centre,
            },
            ts_iso=now_iso,
            db_path=db_path,
        )

    return counts


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan handler for database initialization, deterministic seed, and detection engine worker."""
    init_db()
    seed_database()

    cfg = get_config()
    engine = DetectionEngine()
    app.state.detection_engine = engine

    # Run initial tick on startup so telemetry is available immediately
    try:
        engine.tick(datetime.now(timezone.utc))
    except Exception:
        pass

    worker = DetectionWorker(engine, tick_s=cfg.detection.tick_s)
    app.state.detection_worker = worker
    worker.start()

    try:
        yield
    finally:
        worker.running = False
        worker.join(timeout=2.0)


app = FastAPI(
    title="Exam Resilience Control Tower (ERCT)",
    version="1.0.0",
    description="Operational resilience and audit control plane for high-stakes online examinations",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routes
app.include_router(ingest_router)
app.include_router(control_router)
app.include_router(centres_router)
app.include_router(incidents_router)
app.include_router(review_router)
app.include_router(decisions_router)
app.include_router(status_router)


@app.get("/v1/health", status_code=status.HTTP_200_OK)
def get_health() -> Dict[str, Any]:
    """System health check and quick status summary including detection telemetry."""
    cfg = get_config()
    now = datetime.now(timezone.utc)
    engine: Optional[DetectionEngine] = getattr(app.state, "detection_engine", None)

    health_status = "healthy"
    detection_telemetry = {
        "last_tick_at": None,
        "ticks": 0,
        "tick_errors": 0,
        "grace_remaining_s": 0.0,
        "ingest_stalled": False,
    }

    if engine:
        last_tick_at = engine.last_tick_at
        grace_rem = max(0.0, (engine.started_at + timedelta(seconds=cfg.detection.startup_grace_s) - now).total_seconds())
        detection_telemetry = {
            "last_tick_at": last_tick_at,
            "ticks": engine.ticks,
            "tick_errors": engine.tick_errors,
            "grace_remaining_s": round(grace_rem, 2),
            "ingest_stalled": engine.ingest_stalled,
        }
        if not last_tick_at:
            health_status = "degraded"
        else:
            tick_age = (now - parse_utc_iso(last_tick_at)).total_seconds()
            if tick_age > 5.0:
                health_status = "degraded"
    else:
        health_status = "degraded"

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS c FROM events;")
        events_count = cursor.fetchone()["c"]
        cursor.execute("SELECT COUNT(*) AS c FROM candidates;")
        candidates_count = cursor.fetchone()["c"]
        cursor.execute("SELECT COUNT(*) AS c FROM centres;")
        centres_count = cursor.fetchone()["c"]
        cursor.close()

    audit_res = verify_audit_chain()

    return {
        "status": health_status,
        "exam_id": cfg.exam.id,
        "centres_count": centres_count,
        "candidates_count": candidates_count,
        "events_count": events_count,
        "audit_chain_ok": audit_res.ok,
        "audit_head_hash": audit_res.head_hash,
        "total_audit_entries": audit_res.total_entries,
        "server_time": format_utc_iso(now),
        "detection": detection_telemetry,
    }
