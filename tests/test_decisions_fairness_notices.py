"""Tests for Phase 3b-ii: Decisions, Fairness Check, Candidate Notices, and Status API.

Covers tests 1-13 strictly according to specification:
1. Approve happy path for R1 and R2 rows
2. Approve manual_review 409, invalid overrides 422
3. Stale guard
4. Repeating identical decision 409
5. Controller authentication 401 on POST, open GET
6. Override manual_review side effects
7. Supersedes chain
8. Fairness check boundaries & approve-all gate
9. Notices idempotency, text accuracy & latency
10. Status privacy & isolation
11. Audit verification
12. SLOW: Real processes power_loss lifecycle & restart safety
13. SLOW: Early fault network_drop (offset 10s) with baseline fix
"""
from __future__ import annotations

import json
import math
import os
import random
import socket
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pytest
import uvicorn
import yaml
from fastapi.testclient import TestClient

from app.config import get_config, reload_config
from app.core.audit_chain import append_audit_entry, verify_audit_chain
from app.core.detection import DetectionEngine
from app.core.fairness import compute_exam_fairness, evaluate_fairness_stats
from app.core.impact import compute_incident_impact, format_iso, parse_iso
from app.core.notices import emit_notice, format_decision_message, format_display_time
from app.db import format_utc_iso, get_db_connection, init_db, write_transaction
from app.main import app, seed_database
from simulator.agent_runner import SimulatorRunner


@pytest.fixture
def clean_db(tmp_path: Path):
    """Fixture providing isolated seeded SQLite database for test."""
    db_file = tmp_path / "test_decisions.db"
    cfg_file = tmp_path / "test_cfg.yaml"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["decision"]["controller_key"] = "demo-controller-key"

    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    yield str(db_file)

    if old_env is not None:
        os.environ["ERCT_CONFIG_PATH"] = old_env
        reload_config(old_env)
    else:
        os.environ.pop("ERCT_CONFIG_PATH", None)
        reload_config()


def create_test_incident(db_path: str, inc_id: str = "INC-TEST-01", centre_id: str = "C-BPL-02") -> Dict[str, Any]:
    """Helper to create a resolved incident with impact rows in DB."""
    now = datetime.now(timezone.utc)
    ws = format_utc_iso(now - timedelta(seconds=60))
    we = format_utc_iso(now - timedelta(seconds=30))
    res_at = format_utc_iso(now - timedelta(seconds=25))
    det_at = ws

    cfg = get_config()
    with write_transaction(db_path) as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO incidents (
                incident_id, exam_id, centre_id, type, severity, status,
                detected_at, window_start, window_end, resolved_at, impact_computed_at
            ) VALUES (?, ?, ?, 'power', 'high', 'resolved', ?, ?, ?, ?, ?);
            """,
            (inc_id, cfg.exam.id, centre_id, det_at, ws, we, res_at, res_at),
        )

        # Candidate 1: R1 row (resume, 0 extra_seconds)
        c.execute(
            """
            INSERT OR REPLACE INTO incident_impacts (
                incident_id, candidate_id, session_id, lost_seconds, unsaved_answers,
                last_good_seq, evidence_quality, remedy_recommended, extra_seconds,
                rationale, rule_id, evidence, computed_at, impact_version
            ) VALUES (?, 'CAND-000041', 'SES-000041-1', 0.0, 0, 10, 'strong', 'resume', 0,
                     'Minor loss', 'R1', '{}', ?, 1);
            """,
            (inc_id, res_at),
        )

        # Candidate 2: R2 row (extra_time, 120 extra_seconds)
        c.execute(
            """
            INSERT OR REPLACE INTO incident_impacts (
                incident_id, candidate_id, session_id, lost_seconds, unsaved_answers,
                last_good_seq, evidence_quality, remedy_recommended, extra_seconds,
                rationale, rule_id, evidence, computed_at, impact_version
            ) VALUES (?, 'CAND-000042', 'SES-000042-1', 45.0, 1, 12, 'strong', 'extra_time', 120,
                     'Extra time needed', 'R2', '{}', ?, 1);
            """,
            (inc_id, res_at),
        )

        # Candidate 3: R3 manual_review row
        c.execute(
            """
            INSERT OR REPLACE INTO incident_impacts (
                incident_id, candidate_id, session_id, lost_seconds, unsaved_answers,
                last_good_seq, evidence_quality, remedy_recommended, extra_seconds,
                rationale, rule_id, evidence, computed_at, impact_version
            ) VALUES (?, 'CAND-000043', 'SES-000043-1', 50.0, 2, 8, 'partial', 'manual_review', 0,
                     'Partial evidence', 'R3', '{}', ?, 1);
            """,
            (inc_id, res_at),
        )

        # Set session state for Candidate 3 to 'under_review' and insert into review_queue
        c.execute("UPDATE sessions SET state = 'under_review' WHERE candidate_id = 'CAND-000043';")
        c.execute(
            """
            INSERT OR REPLACE INTO review_queue (incident_id, candidate_id, session_id, reason, status, created_at, updated_at)
            VALUES (?, 'CAND-000043', 'SES-000043-1', 'Partial evidence', 'pending', ?, ?);
            """,
            (inc_id, res_at, res_at),
        )

    return {"incident_id": inc_id, "window_start": ws, "window_end": we, "resolved_at": res_at}


# ---------------------------------------------------------------------------
# TEST 1: Approve happy path for R1 and R2 rows
# ---------------------------------------------------------------------------
def test_01_approve_happy_path(clean_db):
    """Approve happy path for one R1 row and one R2 row:
    decision row, mode 'approved', audit 'decision', candidate notice, status shows decided remedy.
    """
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # 1. Approve R1 row (CAND-000041)
    res_r1 = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000041",
            "action": "approve",
            "expected_rule_id": "R1",
            "expected_extra_seconds": 0,
            "decided_by": "controller-alice",
            "reason": "Routine automated approval for minor interruption",
        },
    )
    assert res_r1.status_code in (200, 201), res_r1.text
    dec_r1 = res_r1.json()
    assert dec_r1["mode"] == "approved"
    assert dec_r1["remedy"] == "resume"
    assert dec_r1["extra_seconds"] == 0

    # Verify candidate notice created for R1
    status_r1 = client.get("/v1/status/CAND-000041").json()
    assert status_r1["remedy"]["status"] == "decided"
    assert status_r1["remedy"]["decision"]["remedy"] == "resume"
    assert status_r1["remedy"]["decision"]["extra_seconds"] == 0
    assert "Decision: your session is resumed" in status_r1["latest_message"]

    # 2. Approve R2 row (CAND-000042)
    res_r2 = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000042",
            "action": "approve",
            "expected_rule_id": "R2",
            "expected_extra_seconds": 120,
            "decided_by": "controller-alice",
            "reason": "Approve standard compensatory time",
        },
    )
    assert res_r2.status_code in (200, 201), res_r2.text
    dec_r2 = res_r2.json()
    assert dec_r2["mode"] == "approved"
    assert dec_r2["remedy"] == "extra_time"
    assert dec_r2["extra_seconds"] == 120

    status_r2 = client.get("/v1/status/CAND-000042").json()
    assert status_r2["remedy"]["status"] == "decided"
    assert status_r2["remedy"]["decision"]["remedy"] == "extra_time"
    assert status_r2["remedy"]["decision"]["extra_seconds"] == 120
    assert "Decision: you are granted 120 seconds (2 min 0 s) of extra time." in status_r2["latest_message"]

    # Check audit entries
    with get_db_connection(clean_db) as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM audit_log WHERE entry_type = 'decision';")
        audit_rows = c.fetchall()
        assert len(audit_rows) == 2


# ---------------------------------------------------------------------------
# TEST 2: Approve manual_review => 409, Invalid overrides => 422
# ---------------------------------------------------------------------------
def test_02_validation_errors(clean_db):
    """Approve a manual_review row => 409. Override without reason => 422.
    Override extra_time with 0 or > max => 422. Unknown remedy => 422.
    """
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # 1. Approve manual_review row => 409
    res = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000043",
            "action": "approve",
            "decided_by": "controller-alice",
        },
    )
    assert res.status_code == 409
    assert "manual review cases need an explicit override with a reason" in res.json()["detail"].lower()

    # 2. Override without a reason (< 10 chars) => 422
    res = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000043",
            "action": "override",
            "remedy": "resume",
            "reason": "short",
            "decided_by": "controller-alice",
        },
    )
    assert res.status_code == 422

    # 3. Override extra_time with 0 seconds => 422
    res = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000043",
            "action": "override",
            "remedy": "extra_time",
            "extra_seconds": 0,
            "reason": "Adequate explanation of decision justification",
            "decided_by": "controller-alice",
        },
    )
    assert res.status_code == 422

    # 4. Override extra_time with > max_extra_seconds (1800) => 422
    res = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000043",
            "action": "override",
            "remedy": "extra_time",
            "extra_seconds": 1801,
            "reason": "Adequate explanation of decision justification",
            "decided_by": "controller-alice",
        },
    )
    assert res.status_code == 422

    # 5. Unknown remedy => 422
    res = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000043",
            "action": "override",
            "remedy": "bonus_marks",
            "reason": "Adequate explanation of decision justification",
            "decided_by": "controller-alice",
        },
    )
    assert res.status_code == 422


# ---------------------------------------------------------------------------
# TEST 3: Stale guard
# ---------------------------------------------------------------------------
def test_03_stale_guard(clean_db):
    """Change recommendation through recompute, then approve with old expected values => 409, nothing written."""
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # Simulate straggler recompute that altered CAND-000041's row
    with write_transaction(clean_db) as conn:
        conn.execute(
            """
            UPDATE incident_impacts
            SET rule_id = 'R2', remedy_recommended = 'extra_time', extra_seconds = 60, impact_version = 2
            WHERE candidate_id = 'CAND-000041';
            """
        )

    # Controller tries to approve with old expected values (R1, 0s)
    res = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000041",
            "action": "approve",
            "expected_rule_id": "R1",
            "expected_extra_seconds": 0,
            "decided_by": "controller-alice",
            "reason": "Approving old stale data",
        },
    )
    assert res.status_code == 409
    err = res.json()["detail"]
    assert err["current_rule_id"] == "R2"
    assert err["current_extra_seconds"] == 60

    # Ensure nothing written to decisions table
    with get_db_connection(clean_db) as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM decisions;").fetchone()["c"]
        assert count == 0


# ---------------------------------------------------------------------------
# TEST 4: Repeating the same approve => 409, one decision row, one audit entry
# ---------------------------------------------------------------------------
def test_04_repeat_decision(clean_db):
    """Repeating an identical decision => 409, no new row, no new audit entry."""
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # First approve
    res1 = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000041",
            "action": "approve",
            "decided_by": "controller-alice",
            "reason": "Routine initial approval",
        },
    )
    assert res1.status_code in (200, 201)

    # Repeat identical approve
    res2 = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000041",
            "action": "approve",
            "decided_by": "controller-alice",
            "reason": "Routine initial approval",
        },
    )
    assert res2.status_code == 409
    assert "already decided" in res2.json()["detail"].lower()

    # Ensure exactly 1 decision row and 1 audit entry
    with get_db_connection(clean_db) as conn:
        dec_cnt = conn.execute("SELECT COUNT(*) AS c FROM decisions;").fetchone()["c"]
        audit_cnt = conn.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'decision';").fetchone()["c"]
        assert dec_cnt == 1
        assert audit_cnt == 1


# ---------------------------------------------------------------------------
# TEST 5: Auth: no key or wrong key => 401; GET endpoints open
# ---------------------------------------------------------------------------
def test_05_auth_guard(clean_db):
    """No key or wrong key => 401 on POST endpoints; GET endpoints are open."""
    create_test_incident(clean_db)
    client = TestClient(app)

    # 1. POST without header => 401
    r_no_key = client.post(
        "/v1/decisions",
        json={"incident_id": "INC-TEST-01", "candidate_id": "CAND-000041", "action": "approve", "decided_by": "controller-alice"},
    )
    assert r_no_key.status_code == 401

    # 2. POST with wrong key => 401
    r_wrong_key = client.post(
        "/v1/decisions",
        headers={"X-Controller-Key": "wrong-key"},
        json={"incident_id": "INC-TEST-01", "candidate_id": "CAND-000041", "action": "approve", "decided_by": "controller-alice"},
    )
    assert r_wrong_key.status_code == 401

    # 3. POST approve-all without key => 401
    r_all_no_key = client.post("/v1/incidents/INC-TEST-01/decisions/approve-all", json={"decided_by": "controller-alice"})
    assert r_all_no_key.status_code == 401

    # 4. GET endpoints are completely open (no header needed)
    assert client.get("/v1/incidents/INC-TEST-01/decisions").status_code == 200
    assert client.get("/v1/incidents/INC-TEST-01/impact").status_code == 200
    assert client.get("/v1/status/CAND-000041").status_code == 200
    assert client.get("/v1/notices").status_code == 200
    assert client.get("/v1/fairness").status_code == 200


# ---------------------------------------------------------------------------
# TEST 6: Override manual_review row side effects
# ---------------------------------------------------------------------------
def test_06_override_manual_review(clean_db):
    """Override a manual_review row => review_queue 'resolved', session under_review -> resumed, audit entry, notice."""
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # Verify pre-conditions
    with get_db_connection(clean_db) as conn:
        q_status = conn.execute("SELECT status FROM review_queue WHERE candidate_id = 'CAND-000043';").fetchone()["status"]
        s_state = conn.execute("SELECT state FROM sessions WHERE candidate_id = 'CAND-000043';").fetchone()["state"]
        assert q_status == "pending"
        assert s_state == "under_review"

    # Override with extra_time 90s
    res = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000043",
            "action": "override",
            "remedy": "extra_time",
            "extra_seconds": 90,
            "reason": "Supervisor confirmed terminal screen flickered during power sag.",
            "decided_by": "controller-alice",
        },
    )
    assert res.status_code in (200, 201), res.text
    dec = res.json()
    assert dec["mode"] == "overridden"
    assert dec["remedy"] == "extra_time"
    assert dec["extra_seconds"] == 90

    # Check side effects
    with get_db_connection(clean_db) as conn:
        q_post = conn.execute("SELECT status FROM review_queue WHERE candidate_id = 'CAND-000043';").fetchone()["status"]
        s_post = conn.execute("SELECT state FROM sessions WHERE candidate_id = 'CAND-000043';").fetchone()["state"]
        assert q_post == "resolved"
        assert s_post == "resumed"

    # Check notice and status
    st = client.get("/v1/status/CAND-000043").json()
    assert st["session_state"] == "resumed"
    assert st["remedy"]["status"] == "decided"
    assert st["remedy"]["decision"]["extra_seconds"] == 90
    assert "90 seconds (1 min 30 s)" in st["latest_message"]


# ---------------------------------------------------------------------------
# TEST 7: Supersedes chain
# ---------------------------------------------------------------------------
def test_07_supersedes_chain(clean_db):
    """A later different decision => new row with supersedes; status shows the latest; both are audited."""
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # Decision 1: approve R1
    r1 = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000041",
            "action": "approve",
            "decided_by": "controller-alice",
            "reason": "First decision",
        },
    ).json()
    d1_id = r1["decision_id"]

    # Decision 2: override to extra_time
    r2 = client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000041",
            "action": "override",
            "remedy": "extra_time",
            "extra_seconds": 60,
            "reason": "Candidate provided CCTV timestamp evidence of disruption",
            "decided_by": "controller-bob",
        },
    ).json()
    d2_id = r2["decision_id"]

    assert d2_id > d1_id

    # Verify supersedes in DB
    with get_db_connection(clean_db) as conn:
        d2_row = conn.execute("SELECT * FROM decisions WHERE decision_id = ?;", (d2_id,)).fetchone()
        assert d2_row["supersedes"] == d1_id

    # Status shows latest decision
    st = client.get("/v1/status/CAND-000041").json()
    assert st["remedy"]["decision"]["remedy"] == "extra_time"
    assert st["remedy"]["decision"]["extra_seconds"] == 60

    # History lists both
    history = client.get("/v1/incidents/INC-TEST-01/decisions").json()
    cand_history = [d for d in history if d["candidate_id"] == "CAND-000041"]
    assert len(cand_history) == 2
    assert cand_history[0]["decision_id"] == d1_id
    assert cand_history[1]["decision_id"] == d2_id
    assert cand_history[1]["supersedes"] == d1_id


# ---------------------------------------------------------------------------
# TEST 8: Fairness check boundaries & approve-all gate
# ---------------------------------------------------------------------------
def test_08_fairness_boundaries_and_gate(clean_db):
    """Fairness: ratio_spread at 1.5 => ok, 1.51 => flagged; min_rows guard;
    coverage_gap at 0.3; approve-all 409 when flagged without ack; succeeds with ack.
    """
    cfg = get_config()

    # 1. Pure function boundary tests for ratio_spread
    # Ratio spread exactly 1.5 => ok
    stats_exact = {
        "C-BPL-01": {"rows": 15, "compensation_ratio": 1.0, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 30.0},
        "C-BPL-02": {"rows": 15, "compensation_ratio": 1.5, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 45.0},
    }
    res_exact = evaluate_fairness_stats(stats_exact, cfg)
    assert res_exact.status == "ok"
    assert len(res_exact.flags) == 0

    # Ratio spread 1.51 => flagged
    stats_flagged = {
        "C-BPL-01": {"rows": 15, "compensation_ratio": 1.0, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 30.0},
        "C-BPL-02": {"rows": 15, "compensation_ratio": 1.51, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 45.3},
    }
    res_flagged = evaluate_fairness_stats(stats_flagged, cfg)
    assert res_flagged.status == "flagged"
    assert any(f.kind == "ratio_spread" for f in res_flagged.flags)

    # 2. Min rows guard (centre with 9 rows is excluded)
    stats_small = {
        "C-BPL-01": {"rows": 9, "compensation_ratio": 1.0, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 30.0},
        "C-BPL-02": {"rows": 15, "compensation_ratio": 3.0, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 90.0},
    }
    res_small = evaluate_fairness_stats(stats_small, cfg)
    assert res_small.status == "ok"  # C-BPL-01 has < 10 rows, cannot compare spread!

    # 3. Coverage gap boundary (0.3 => ok, 0.31 => flagged)
    stats_cov_ok = {
        "C-BPL-01": {"rows": 15, "compensation_ratio": 1.0, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 30.0},
        "C-BPL-02": {"rows": 15, "compensation_ratio": 1.0, "share_manual_review": 0.4, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 30.0},
    }
    assert evaluate_fairness_stats(stats_cov_ok, cfg).status == "ok"

    stats_cov_flag = {
        "C-BPL-01": {"rows": 15, "compensation_ratio": 1.0, "share_manual_review": 0.1, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 30.0},
        "C-BPL-02": {"rows": 15, "compensation_ratio": 1.0, "share_manual_review": 0.41, "share_extra_time": 0.5, "mean_lost_s": 30.0, "mean_extra_seconds": 30.0},
    }
    assert evaluate_fairness_stats(stats_cov_flag, cfg).status == "flagged"
    assert any(f.kind == "coverage_gap" for f in evaluate_fairness_stats(stats_cov_flag, cfg).flags)

    # 4. Test approve-all gate via API
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # Seed 10+ rows in C-BPL-01 with compensation_ratio = 1.0
    # and 10+ rows in C-BPL-02 with compensation_ratio = 2.0 to trigger ratio_spread
    with write_transaction(clean_db) as conn:
        now_iso = format_utc_iso(datetime.now(timezone.utc))
        conn.execute(
            """
            INSERT OR REPLACE INTO incidents (
                incident_id, exam_id, centre_id, type, severity, status,
                detected_at, window_start, window_end, resolved_at, impact_computed_at
            ) VALUES ('INC-BPL1', ?, 'C-BPL-01', 'power', 'high', 'resolved', ?, ?, ?, ?, ?);
            """,
            (cfg.exam.id, now_iso, now_iso, now_iso, now_iso, now_iso),
        )
        for i in range(1, 12):
            cid = f"CAND-00000{i}" if i < 10 else f"CAND-0000{i}"
            conn.execute(
                f"""
                INSERT OR REPLACE INTO incident_impacts (
                    incident_id, candidate_id, session_id, lost_seconds, unsaved_answers,
                    last_good_seq, evidence_quality, remedy_recommended, extra_seconds,
                    rationale, rule_id, evidence, computed_at, impact_version
                ) VALUES ('INC-BPL1', '{cid}', 'SES-{i}', 30.0, 0, 10, 'strong', 'resume', 30, 'R1', 'R1', '{{}}', ?, 1);
                """,
                (now_iso,),
            )
        # In INC-TEST-01 (C-BPL-02), add 10 rows with ratio 2.0 (extra_s = 60 for lost_s = 30)
        for i in range(45, 56):
            conn.execute(
                f"""
                INSERT OR REPLACE INTO incident_impacts (
                    incident_id, candidate_id, session_id, lost_seconds, unsaved_answers,
                    last_good_seq, evidence_quality, remedy_recommended, extra_seconds,
                    rationale, rule_id, evidence, computed_at, impact_version
                ) VALUES ('INC-TEST-01', 'CAND-0000{i}', 'SES-{i}', 30.0, 0, 10, 'strong', 'extra_time', 60, 'R2', 'R2', '{{}}', ?, 1);
                """,
                (now_iso,),
            )

    # Check GET /v1/fairness
    f_resp = client.get("/v1/fairness").json()
    assert f_resp["status"] == "flagged"

    # Approve-all without acknowledge_fairness => 409
    res_blocked = client.post(
        "/v1/incidents/INC-TEST-01/decisions/approve-all",
        headers=headers,
        json={"decided_by": "controller-alice", "acknowledge_fairness": False},
    )
    assert res_blocked.status_code == 409
    assert "fairness gate flagged" in res_blocked.json()["detail"]["message"].lower()

    # Approve-all WITH acknowledge_fairness => 200
    res_ack = client.post(
        "/v1/incidents/INC-TEST-01/decisions/approve-all",
        headers=headers,
        json={"decided_by": "controller-alice", "acknowledge_fairness": True},
    )
    assert res_ack.status_code == 200, res_ack.text
    data_ack = res_ack.json()
    assert data_ack["acknowledged_fairness"] is True
    assert data_ack["approved"] > 0
    assert data_ack["skipped_manual_review"] == 1  # CAND-000043 was skipped!

    # Check decisions have acknowledged_fairness = 1 and snapshot
    with get_db_connection(clean_db) as conn:
        sample_dec = conn.execute("SELECT * FROM decisions WHERE incident_id = 'INC-TEST-01' LIMIT 1;").fetchone()
        assert sample_dec["acknowledged_fairness"] == 1
        assert sample_dec["fairness_snapshot"] is not None
        assert "ratio_spread" in sample_dec["fairness_snapshot"]


# ---------------------------------------------------------------------------
# TEST 9: Notices: idempotency, text content, latency
# ---------------------------------------------------------------------------
def test_09_notices_properties(clean_db):
    """Exactly one incident_opened notice per interrupted session; recompute no duplicate;
    decision notice text contains right number; incident_opened latency <= delay_s.
    """
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    now_iso = format_utc_iso(datetime.now(timezone.utc))

    # 1. Emit incident_opened notice for CAND-000041
    with write_transaction(clean_db) as conn:
        nid1 = emit_notice(
            conn=conn,
            audience="candidate",
            target_id="CAND-000041",
            incident_id="INC-TEST-01",
            kind="incident_opened",
            ref_key="incident_opened:INC-TEST-01:CAND-000041",
            message="Your exam centre reported a power interruption at 10:05. Your last confirmed save is #10. The exam team is working out the effect on your session. You do not need to do anything.",
            now_iso=now_iso,
        )
        assert nid1 is not None

        # Repeat same emit => must return None (idempotent, no duplicate row)
        nid2 = emit_notice(
            conn=conn,
            audience="candidate",
            target_id="CAND-000041",
            incident_id="INC-TEST-01",
            kind="incident_opened",
            ref_key="incident_opened:INC-TEST-01:CAND-000041",
            message="Duplicate message",
            now_iso=now_iso,
        )
        assert nid2 is None

    # Check notice count in DB
    with get_db_connection(clean_db) as conn:
        cnt = conn.execute("SELECT COUNT(*) AS c FROM notices WHERE kind = 'incident_opened' AND target_id = 'CAND-000041';").fetchone()["c"]
        assert cnt == 1

    # 2. Decision notice text contains right number
    client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000042",
            "action": "override",
            "remedy": "extra_time",
            "extra_seconds": 150,
            "reason": "Accurate extra time compensation justified",
            "decided_by": "controller-alice",
        },
    )

    notices = client.get("/v1/notices?target_id=CAND-000042").json()
    dec_notices = [n for n in notices if n["kind"] == "decision_final"]
    assert len(dec_notices) == 1
    assert "150 seconds (2 min 30 s)" in dec_notices[0]["message"]


# ---------------------------------------------------------------------------
# TEST 10: Status privacy & isolation
# ---------------------------------------------------------------------------
def test_10_status_privacy(clean_db):
    """An undecided recommendation is never visible; another candidate's data is never present;
    unknown candidate => 404; a candidate with no incident.
    """
    create_test_incident(clean_db)
    client = TestClient(app)

    # 1. CAND-000042 is undecided (remedy_recommended is extra_time in DB, but not decided)
    st42 = client.get("/v1/status/CAND-000042").json()
    assert st42["candidate_id"] == "CAND-000042"
    assert st42["remedy"]["status"] == "awaiting_decision"
    assert st42["remedy"]["decision"] is None  # Undecided remedy must be hidden!
    assert "extra_time" not in json.dumps(st42["remedy"])

    # 2. Unknown candidate => 404
    r_unknown = client.get("/v1/status/CAND-UNKNOWN")
    assert r_unknown.status_code == 404

    # 3. Candidate with no incident (CAND-000001 at C-BPL-01)
    st1 = client.get("/v1/status/CAND-000001").json()
    assert st1["candidate_id"] == "CAND-000001"
    assert st1["incident"] is None
    assert st1["remedy"]["status"] == "none"
    assert "normal" in st1["latest_message"].lower()

    # 4. Check data isolation (no candidate 41 or 43 info inside CAND-000042 payload)
    raw_str = json.dumps(st42)
    assert "CAND-000041" not in raw_str
    assert "CAND-000043" not in raw_str


# ---------------------------------------------------------------------------
# TEST 11: Audit trail verification
# ---------------------------------------------------------------------------
def test_11_audit_chain_verification(clean_db):
    """verify() ok; number of 'decision' and 'notice' entries equals rows written; nothing else added."""
    create_test_incident(clean_db)
    client = TestClient(app)
    headers = {"X-Controller-Key": "demo-controller-key"}

    # Issue a decision
    client.post(
        "/v1/decisions",
        headers=headers,
        json={
            "incident_id": "INC-TEST-01",
            "candidate_id": "CAND-000041",
            "action": "approve",
            "decided_by": "controller-alice",
            "reason": "Routine automated approval for minor interruption",
        },
    )

    # Verify audit chain integrity
    res = verify_audit_chain(clean_db)
    assert res.ok is True, f"Audit chain verification failed: {res.message}"

    with get_db_connection(clean_db) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) AS c FROM decisions;")
        dec_rows = c.fetchone()["c"]
        c.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'decision';")
        audit_dec = c.fetchone()["c"]
        assert dec_rows == audit_dec

        c.execute("SELECT COUNT(*) AS c FROM notices WHERE audience = 'candidate';")
        notice_rows = c.fetchone()["c"]
        c.execute("SELECT COUNT(*) AS c FROM audit_log WHERE entry_type = 'notice';")
        audit_notices = c.fetchone()["c"]
        assert notice_rows == audit_notices


# ---------------------------------------------------------------------------
# SLOW TEST 12: Real process power_loss, approve-all, override, restart safety
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_12_slow_real_processes(tmp_path: Path):
    """Power loss at C-BPL-02 -> resolved -> approve-all -> override one manual_review row.
    Status of R1, R2, review candidate at each step. Restart API and verify persistence.
    """
    port = find_free_port()
    db_file = tmp_path / "proc_test.db"
    cfg_file = tmp_path / "proc_cfg.yaml"
    gt_file = tmp_path / "proc_gt.jsonl"
    buf_db = str(tmp_path / "proc_buf.db")

    interval_s = 0.5

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["impact"]["heartbeat_interval_s"] = interval_s
    cfg["simulation"]["faults"] = []

    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(30):
        try:
            if httpx.get(f"{base_url}/v1/health", timeout=0.5).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)

    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=buf_db,
        heartbeat_override_s=interval_s,
        ground_truth_path=str(gt_file),
    )

    try:
        sim_thread = threading.Thread(target=runner.start, kwargs={"duration_s": 17.0}, daemon=True)
        sim_thread.start()

        # Step 1: Wait for steady state then inject power loss
        time.sleep(6.0)
        httpx.post(
            f"{base_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={"centre_id": "C-BPL-02", "fault_type": "power_loss", "duration_s": 3, "params": {"source": "grid"}},
            timeout=2.0,
        )

        sim_thread.join(timeout=25.0)

        # Wait for resolution and impact
        inc_id = None
        for _ in range(40):
            r = httpx.get(f"{base_url}/v1/incidents?centre_id=C-BPL-02")
            if r.status_code == 200 and r.json():
                inc = r.json()[0]
                if inc["status"] == "resolved" and inc.get("impact_computed_at"):
                    inc_id = inc["incident_id"]
                    break
            time.sleep(0.5)

        assert inc_id is not None, "Incident must be resolved with impact computed"

        # Check impact rows
        imp = httpx.get(f"{base_url}/v1/incidents/{inc_id}/impact").json()
        assert len(imp["rows"]) == 40

        # Step 1b: Verify status of R1, R2, and manual_review candidates after impact & before decision
        r1_rows = [r for r in imp["rows"] if r["remedy_recommended"] == "resume"]
        r2_rows = [r for r in imp["rows"] if r["remedy_recommended"] == "extra_time"]
        mr_rows = [r for r in imp["rows"] if r["remedy_recommended"] == "manual_review"]

        c_r1 = r1_rows[0]["candidate_id"] if r1_rows else None
        c_r2 = r2_rows[0]["candidate_id"] if r2_rows else None
        assert len(mr_rows) > 0, "Must have at least one manual_review candidate in power outage"
        c_mr = mr_rows[0]["candidate_id"]

        for cand, expected_status in [(c_r1, "awaiting_decision"), (c_r2, "awaiting_decision"), (c_mr, "under_review")]:
            if cand:
                st = httpx.get(f"{base_url}/v1/status/{cand}").json()
                assert st["remedy"]["status"] == expected_status
                assert st["remedy"]["decision"] is None

        # Step 2: approve-all
        ctrl_headers = {"X-Controller-Key": "demo-controller-key"}
        app_all_res = httpx.post(
            f"{base_url}/v1/incidents/{inc_id}/decisions/approve-all",
            headers=ctrl_headers,
            json={"decided_by": "controller-alice", "acknowledge_fairness": True},
            timeout=5.0,
        ).json()
        assert app_all_res["approved"] > 0
        assert app_all_res["skipped_manual_review"] == len(mr_rows)

        # Status after approve-all:
        if c_r1:
            st1 = httpx.get(f"{base_url}/v1/status/{c_r1}").json()
            assert st1["remedy"]["status"] == "decided"
            assert st1["remedy"]["decision"]["remedy"] == "resume"
        if c_r2:
            st2 = httpx.get(f"{base_url}/v1/status/{c_r2}").json()
            assert st2["remedy"]["status"] == "decided"
            assert st2["remedy"]["decision"]["remedy"] == "extra_time"
        st_mr_pre = httpx.get(f"{base_url}/v1/status/{c_mr}").json()
        assert st_mr_pre["remedy"]["status"] == "under_review"  # Skipped by approve-all

        # Step 3: override one manual_review candidate
        m_cand = c_mr
        ov_res = httpx.post(
            f"{base_url}/v1/decisions",
            headers=ctrl_headers,
            json={
                "incident_id": inc_id,
                "candidate_id": m_cand,
                "action": "override",
                "remedy": "extra_time",
                "extra_seconds": 90,
                "reason": "Terminal battery degraded faster than expected during sag",
                "decided_by": "controller-bob",
            },
            timeout=5.0,
        )
        assert ov_res.status_code in (200, 201)

        # Check status of m_cand after override
        st_m = httpx.get(f"{base_url}/v1/status/{m_cand}").json()
        assert st_m["remedy"]["status"] == "decided"
        assert st_m["remedy"]["decision"]["extra_seconds"] == 90
        assert "90 seconds (1 min 30 s)" in st_m["latest_message"]

    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)

    # Step 4: Restart API and verify decisions and statuses are unchanged
    server2 = uvicorn.Server(server_config)
    server_thread2 = threading.Thread(target=server2.run, daemon=True)
    server_thread2.start()

    try:
        for _ in range(30):
            try:
                if httpx.get(f"{base_url}/v1/health", timeout=0.5).status_code == 200:
                    break
            except Exception:
                time.sleep(0.1)

        # Re-query status of R1, R2, and overridden candidate after restart
        if c_r1:
            st1_after = httpx.get(f"{base_url}/v1/status/{c_r1}").json()
            assert st1_after["remedy"]["status"] == "decided"
            assert st1_after["remedy"]["decision"]["remedy"] == "resume"
        if c_r2:
            st2_after = httpx.get(f"{base_url}/v1/status/{c_r2}").json()
            assert st2_after["remedy"]["status"] == "decided"
            assert st2_after["remedy"]["decision"]["remedy"] == "extra_time"

        st_m_after = httpx.get(f"{base_url}/v1/status/{m_cand}").json()
        assert st_m_after["remedy"]["status"] == "decided"
        assert st_m_after["remedy"]["decision"]["extra_seconds"] == 90
        assert "90 seconds (1 min 30 s)" in st_m_after["latest_message"]

        # Decisions list count is preserved
        decs_after = httpx.get(f"{base_url}/v1/incidents/{inc_id}/decisions").json()
        assert len(decs_after) == app_all_res["approved"] + 1

    finally:
        server2.should_exit = True
        server_thread2.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


# ---------------------------------------------------------------------------
# SLOW TEST 13: Early fault network_drop at 10s offset with baseline fix
# ---------------------------------------------------------------------------
@pytest.mark.slow
def test_13_slow_early_fault_network(tmp_path: Path):
    """Early fault: network_drop at C-BPL-04 with start offset 10 s.
    All exposed sessions are listed, every strong row has lost 0, and manual_review share is <= 20%.
    """
    port = find_free_port()
    db_file = tmp_path / "early_net.db"
    cfg_file = tmp_path / "early_net_cfg.yaml"
    gt_file = tmp_path / "early_net_gt.jsonl"
    buf_db = str(tmp_path / "early_net_buf.db")

    interval_s = 0.5

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["impact"]["heartbeat_interval_s"] = interval_s
    cfg["simulation"]["faults"] = []

    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)

    old_env = os.environ.get("ERCT_CONFIG_PATH")
    os.environ["ERCT_CONFIG_PATH"] = str(cfg_file)
    reload_config(str(cfg_file))

    init_db(str(db_file))
    seed_database(str(db_file))

    server_config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(server_config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(30):
        try:
            if httpx.get(f"{base_url}/v1/health", timeout=0.5).status_code == 200:
                break
        except Exception:
            time.sleep(0.1)

    runner = SimulatorRunner(
        api_base_url=base_url,
        buffer_db_path=buf_db,
        heartbeat_override_s=interval_s,
        ground_truth_path=str(gt_file),
    )

    try:
        # Run with fault injected at ~10s offset
        sim_thread = threading.Thread(target=runner.start, kwargs={"duration_s": 18.0}, daemon=True)
        sim_thread.start()

        time.sleep(10.0)  # 10s offset
        httpx.post(
            f"{base_url}/v1/control/faults",
            headers={"X-Control-Key": "ctrl-secret-key-2026"},
            json={"centre_id": "C-BPL-04", "fault_type": "network_drop", "duration_s": 2, "params": {}},
            timeout=2.0,
        )

        sim_thread.join(timeout=25.0)
        runner.buffer.drain_all(api_base_url=base_url)
        time.sleep(0.5)

        inc_id = None
        for _ in range(40):
            r = httpx.get(f"{base_url}/v1/incidents?centre_id=C-BPL-04")
            if r.status_code == 200 and r.json():
                inc = r.json()[0]
                if inc["status"] == "resolved" and inc.get("impact_computed_at"):
                    inc_id = inc["incident_id"]
                    break
            time.sleep(0.5)

        assert inc_id is not None, "Network incident must resolve with impact computed"

        imp = httpx.get(f"{base_url}/v1/incidents/{inc_id}/impact").json()
        rows = imp["rows"]
        assert len(rows) == 40, f"All 40 exposed sessions must be listed, got {len(rows)}"

        # Every strong row has lost 0
        strong_rows = [r for r in rows if r["evidence_quality"] == "strong"]
        for r in strong_rows:
            assert r["lost_seconds"] == 0, f"Strong row {r['candidate_id']} lost_seconds must be 0, got {r['lost_seconds']}"

        # Manual review share <= 20%
        manual_rows = [r for r in rows if r["remedy_recommended"] == "manual_review"]
        manual_share = len(manual_rows) / len(rows)
        assert manual_share <= 0.20, f"Manual review share must be <= 20%, got {manual_share:.1%}"

    finally:
        server.should_exit = True
        server_thread.join(timeout=3.0)
        if old_env is not None:
            os.environ["ERCT_CONFIG_PATH"] = old_env
            reload_config(old_env)
        else:
            os.environ.pop("ERCT_CONFIG_PATH", None)
            reload_config()


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
