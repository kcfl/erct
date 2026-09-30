"""Tests for operational, readiness, and audit management API routes."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Generator
from fastapi.testclient import TestClient
import pytest
import yaml

from app.config import reload_config
from app.db import init_db
from app.main import app, seed_database


@pytest.fixture
def clean_db(tmp_path: Path) -> Generator[str, None, None]:
    """Fixture providing isolated seeded SQLite database for test."""
    db_file = tmp_path / "test_ops.db"
    cfg_file = tmp_path / "test_cfg.yaml"

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["database"]["path"] = str(db_file)
    cfg["control"]["key"] = "ctrl-secret-key-2026"

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


def test_centre_sessions_success_and_unknown_404(clean_db: str) -> None:
    """Sessions endpoint returns 40 rows for C-BPL-01 with required keys; unknown centre returns 404."""
    client = TestClient(app)

    # 1. Known centre C-BPL-01
    res = client.get("/v1/centres/C-BPL-01/sessions")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}: {res.text}"
    sessions = res.json()
    assert isinstance(sessions, list)
    assert len(sessions) == 40, f"Expected 40 sessions, got {len(sessions)}"

    required_keys = {"session_id", "candidate_id", "state", "last_ingested_age_s", "remaining_s", "last_saved_seq"}
    for s in sessions:
        assert required_keys.issubset(s.keys())
        assert s["session_id"].startswith("SES-")
        assert s["candidate_id"].startswith("CAND-")
        assert s["state"] == "registered"
        assert s["remaining_s"] == 120 * 60
        assert s["last_saved_seq"] == 0

    # 2. Unknown centre returns 404
    res_404 = client.get("/v1/centres/C-UNKNOWN-99/sessions")
    assert res_404.status_code == 404, f"Expected 404, got {res_404.status_code}"


def test_readiness_version_block_and_passes(clean_db: str) -> None:
    """Readiness: C-BPL-03 has passed=false with version blocked_reason; the other four pass."""
    client = TestClient(app)
    res = client.get("/v1/readiness")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}: {res.text}"
    data = res.json()

    assert data["exam_id"] == "EX-2026-PS6-01"
    assert data["required_version"] == "4.2.1"
    assert data["min_score"] == 70
    assert len(data["centres"]) == 5

    centres_map = {c["centre_id"]: c for c in data["centres"]}

    # C-BPL-03 must fail due to version mismatch (have 4.2.0, need 4.2.1)
    c3 = centres_map["C-BPL-03"]
    assert c3["passed"] is False
    assert c3["blocked_reason"] is not None
    assert "version" in c3["blocked_reason"].lower()
    assert c3["checks"]["version"]["ok"] is False
    assert c3["checks"]["version"]["have"] == "4.2.0"
    assert c3["checks"]["version"]["need"] == "4.2.1"

    # The other 4 centres must pass
    for cid in ["C-BPL-01", "C-BPL-02", "C-BPL-04", "C-BPL-05"]:
        c = centres_map[cid]
        assert c["passed"] is True, f"Centre {cid} should have passed"
        assert c["blocked_reason"] is None
        assert c["score"] >= 70
        assert c["checks"]["version"]["ok"] is True
        assert c["checks"]["power_backup"]["ok"] is True
        assert c["checks"]["capacity"]["ok"] is True


def test_audit_verify_clean_db(clean_db: str) -> None:
    """Verify is ok on a clean database."""
    client = TestClient(app)
    res = client.get("/v1/audit/verify")
    assert res.status_code == 200
    data = res.json()
    assert data["ok"] is True
    assert data["total_entries"] >= 1
    assert data["failing_seq"] is None
    assert data["head_hash"] is not None
    assert data["error"] is None


def test_audit_tamper_then_verify_fails(clean_db: str) -> None:
    """Tamper a seq then verify returns ok=false with failing_seq equal to that seq."""
    client = TestClient(app)
    headers = {"X-Control-Key": "ctrl-secret-key-2026"}

    # Tamper seq 1
    res_tamper = client.post("/v1/audit/tamper", headers=headers, json={"seq": 1})
    assert res_tamper.status_code == 200
    assert res_tamper.json()["status"] == "tampered"
    assert res_tamper.json()["seq"] == 1

    # Verify fails at seq 1
    res_verify = client.get("/v1/audit/verify")
    assert res_verify.status_code == 200
    v_data = res_verify.json()
    assert v_data["ok"] is False
    assert v_data["failing_seq"] == 1
    assert v_data["error"] is not None


def test_audit_restore_then_verify_is_ok(clean_db: str) -> None:
    """Restore then verify is ok again."""
    client = TestClient(app)
    headers = {"X-Control-Key": "ctrl-secret-key-2026"}

    # Tamper seq 1
    res_tamper = client.post("/v1/audit/tamper", headers=headers, json={"seq": 1})
    assert res_tamper.status_code == 200

    # Restore seq 1
    res_restore = client.post("/v1/audit/restore", headers=headers, json={"seq": 1})
    assert res_restore.status_code == 200
    assert res_restore.json()["status"] == "restored"

    # Verify passes again
    res_verify = client.get("/v1/audit/verify")
    assert res_verify.status_code == 200
    assert res_verify.json()["ok"] is True
    assert res_verify.json()["failing_seq"] is None


def test_audit_tamper_unauthorized_without_control_key(clean_db: str) -> None:
    """Tamper without the control key returns 401."""
    client = TestClient(app)

    # Missing header
    res_no_key = client.post("/v1/audit/tamper", json={"seq": 1})
    assert res_no_key.status_code == 401

    # Invalid header
    res_bad_key = client.post("/v1/audit/tamper", headers={"X-Control-Key": "wrong-key"}, json={"seq": 1})
    assert res_bad_key.status_code == 401

    # Restore without header also returns 401
    res_restore_no_key = client.post("/v1/audit/restore", json={"seq": 1})
    assert res_restore_no_key.status_code == 401


def test_audit_trail_and_error_handling(clean_db: str) -> None:
    """Trail returns entries newest last with required keys, and unknown seq returns 404."""
    client = TestClient(app)
    headers = {"X-Control-Key": "ctrl-secret-key-2026"}

    res_trail = client.get("/v1/audit/trail?limit=10")
    assert res_trail.status_code == 200
    entries = res_trail.json()
    assert len(entries) >= 1

    expected_keys = {"seq", "ts", "entry_type", "ref_id", "entry_hash", "prev_hash"}
    for e in entries:
        assert expected_keys.issubset(e.keys())

    # Check newest last ordering (ascending seq)
    seqs = [e["seq"] for e in entries]
    assert seqs == sorted(seqs)

    # Tampering unknown seq returns 404
    res_tamper_404 = client.post("/v1/audit/tamper", headers=headers, json={"seq": 99999})
    assert res_tamper_404.status_code == 404
