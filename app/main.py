"""FastAPI main application entrypoint for ERCT with deterministic database seeding."""
from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes_ingest import router as ingest_router
from app.api.routes_control import router as control_router
from app.config import get_config
from app.core.audit_chain import append_audit_entry, verify_audit_chain
from app.db import get_db_connection, init_db, write_transaction


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
    """Lifespan handler for database initialization and deterministic seed."""
    init_db()
    seed_database()
    yield


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


@app.get("/v1/health", status_code=status.HTTP_200_OK)
def get_health() -> Dict[str, Any]:
    """System health check and quick status summary."""
    cfg = get_config()
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
        "status": "healthy",
        "exam_id": cfg.exam.id,
        "centres_count": centres_count,
        "candidates_count": candidates_count,
        "events_count": events_count,
        "audit_chain_ok": audit_res.ok,
        "audit_head_hash": audit_res.head_hash,
        "total_audit_entries": audit_res.total_entries,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }
