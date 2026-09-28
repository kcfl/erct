"""Unit and integration tests for the ERCT Audit Hash Chain."""
from __future__ import annotations

import concurrent.futures
import json
import sqlite3
import pytest
from pathlib import Path

from app.db import init_db, write_transaction
from app.core.audit_chain import (
    GENESIS_PREV_HASH,
    append_audit_entry,
    canonical_json,
    compute_entry_hash,
    restore_audit_entry_for_demo,
    tamper_audit_entry_for_demo,
    verify_audit_chain,
    get_audit_trail,
)


@pytest.fixture
def test_db(tmp_path: Path) -> str:
    """Fixture to provide a clean, isolated SQLite database file."""
    db_file = tmp_path / "test_erct.db"
    init_db(str(db_file))
    return str(db_file)


def test_empty_audit_chain(test_db: str) -> None:
    """Empty audit log must pass verification with 0 entries."""
    res = verify_audit_chain(test_db)
    assert res.ok is True
    assert res.total_entries == 0
    assert res.head_hash == GENESIS_PREV_HASH


def test_genesis_block(test_db: str) -> None:
    """First block must use 64 zeros as prev_hash and have seq 1."""
    entry = append_audit_entry(
        entry_type="config",
        ref_id="CFG-INIT",
        payload={"exam_id": "EX-2026-PS6-01", "version": "4.2.1"},
        ts_iso="2026-09-29T10:00:00.000Z",
        db_path=test_db,
    )

    assert entry.seq == 1
    assert entry.prev_hash == GENESIS_PREV_HASH
    assert len(entry.entry_hash) == 64

    # Verify manual hash calculation
    expected_canon = canonical_json({"exam_id": "EX-2026-PS6-01", "version": "4.2.1"})
    expected_hash = compute_entry_hash(
        prev_hash=GENESIS_PREV_HASH,
        seq=1,
        ts_iso="2026-09-29T10:00:00.000Z",
        entry_type="config",
        ref_id="CFG-INIT",
        canonical_payload=expected_canon,
    )
    assert entry.entry_hash == expected_hash

    res = verify_audit_chain(test_db)
    assert res.ok is True
    assert res.total_entries == 1
    assert res.head_hash == entry.entry_hash


def test_canonical_json_ordering() -> None:
    """Different key orders must produce identical canonical representation and hash."""
    d1 = {"z": 1, "a": 2, "m": {"b": 3, "a": 4}}
    d2 = {"a": 2, "m": {"a": 4, "b": 3}, "z": 1}

    canon1 = canonical_json(d1)
    canon2 = canonical_json(d2)

    assert canon1 == canon2
    assert canon1 == '{"a":2,"m":{"a":4,"b":3},"z":1}'

    h1 = compute_entry_hash(GENESIS_PREV_HASH, 1, "2026-01-01T00:00:00Z", "test", "R1", canon1)
    h2 = compute_entry_hash(GENESIS_PREV_HASH, 1, "2026-01-01T00:00:00Z", "test", "R1", canon2)
    assert h1 == h2


def test_multi_entry_verification(test_db: str) -> None:
    """Appending multiple varied entries must create a cryptographically valid chain."""
    entries_to_append = [
        ("event", "EVT-001", {"type": "HEARTBEAT", "latency_ms": 42}),
        ("event", "EVT-002", {"type": "POWER_LOSS", "source": "ups"}),
        ("incident", "INC-001", {"type": "power", "severity": "critical"}),
        ("impact", "CAND-001", {"lost_s": 240, "unsaved": 1, "remedy": "resume"}),
        ("decision", "DEC-001", {"mode": "auto", "rule": "R1"}),
    ]

    created = []
    for etype, ref, payload in entries_to_append:
        e = append_audit_entry(etype, ref, payload, db_path=test_db)
        created.append(e)

    assert len(created) == 5

    # Check each prev_hash links to predecessor entry_hash
    for i in range(1, 5):
        assert created[i].prev_hash == created[i - 1].entry_hash
        assert created[i].seq == created[i - 1].seq + 1

    res = verify_audit_chain(test_db)
    assert res.ok is True
    assert res.total_entries == 5
    assert res.head_hash == created[-1].entry_hash


def test_tamper_detection_pinpoint(test_db: str) -> None:
    """Tampering with a single row must fail verification at that EXACT sequence number."""
    # Seed 20 entries
    for i in range(1, 21):
        append_audit_entry(
            entry_type="event",
            ref_id=f"EVT-{i:03d}",
            payload={"counter": i, "score": 100 + i},
            db_path=test_db,
        )

    # Initial verification passes
    initial_check = verify_audit_chain(test_db)
    assert initial_check.ok is True
    assert initial_check.total_entries == 20

    # Tamper with row 14
    tamper_result = tamper_audit_entry_for_demo(
        seq=14,
        tampered_payload={"counter": 14, "score": 9999, "tampered": True},
        db_path=test_db,
    )
    assert tamper_result["status"] == "tampered"
    assert tamper_result["seq"] == 14

    # Verify must fail precisely at seq 14
    verify_fail = verify_audit_chain(test_db)
    assert verify_fail.ok is False
    assert verify_fail.failing_seq == 14
    assert "Hash mismatch at seq 14" in (verify_fail.error or "")

    # Restore the entry without providing payload and confirm verification passes again
    restore_result = restore_audit_entry_for_demo(
        seq=14,
        db_path=test_db,
    )
    assert restore_result["status"] == "restored"
    assert 14 in restore_result["restored_seqs"]

    verify_restored = verify_audit_chain(test_db)
    assert verify_restored.ok is True
    assert verify_restored.total_entries == 20


def test_tamper_and_restore_no_args(test_db: str) -> None:
    """Tampering entries and calling restore with NO arguments restores all tampered rows."""
    for i in range(1, 11):
        append_audit_entry("event", f"EVT-{i}", {"step": i}, db_path=test_db)

    # Tamper rows 3 and 7
    tamper_audit_entry_for_demo(seq=3, db_path=test_db)
    tamper_audit_entry_for_demo(seq=7, db_path=test_db)

    # Verification must fail at first tampered row (seq 3)
    res_fail = verify_audit_chain(test_db)
    assert res_fail.ok is False
    assert res_fail.failing_seq == 3

    # Restore with ZERO arguments (seq=None, payload=None)
    restore_all = restore_audit_entry_for_demo(db_path=test_db)
    assert restore_all["status"] == "restored"
    assert set(restore_all["restored_seqs"]) == {3, 7}

    # Verification must now pass completely
    res_pass = verify_audit_chain(test_db)
    assert res_pass.ok is True
    assert res_pass.total_entries == 10


def test_tamper_prev_hash_detection(test_db: str) -> None:
    """Modifying the prev_hash link directly must fail verification at that sequence number."""
    for i in range(1, 6):
        append_audit_entry("event", f"EVT-{i}", {"i": i}, db_path=test_db)

    with write_transaction(test_db) as conn:
        conn.execute("UPDATE audit_log SET prev_hash = ? WHERE seq = 3;", ("f" * 64,))

    res = verify_audit_chain(test_db)
    assert res.ok is False
    assert res.failing_seq == 3
    assert "Broken hash link at seq 3" in (res.error or "")


def test_sequence_gap_detection(test_db: str) -> None:
    """Deleting an intermediate row must trigger sequence discontinuity failure."""
    for i in range(1, 6):
        append_audit_entry("event", f"EVT-{i}", {"i": i}, db_path=test_db)

    with write_transaction(test_db) as conn:
        conn.execute("DELETE FROM audit_log WHERE seq = 3;")

    res = verify_audit_chain(test_db)
    assert res.ok is False
    assert res.failing_seq == 4
    assert "Sequence discontinuity" in (res.error or "")


def test_concurrent_appends_thread_safety(test_db: str) -> None:
    """Multiple threads appending simultaneously must produce a strictly continuous, verified chain with 200 appends."""
    num_threads = 10
    appends_per_thread = 20
    total_expected = num_threads * appends_per_thread  # 200 appends

    def worker(worker_id: int):
        for i in range(appends_per_thread):
            append_audit_entry(
                entry_type="thread_event",
                ref_id=f"W{worker_id}-{i}",
                payload={"worker": worker_id, "index": i},
                db_path=test_db,
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, tid) for tid in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    # Verify full chain integrity after concurrent writes
    res = verify_audit_chain(test_db)
    assert res.ok is True
    assert res.total_entries == total_expected

    # Verify trail inspect
    trail = get_audit_trail(limit=5, db_path=test_db)
    assert len(trail) == 5
    assert trail[0].seq == total_expected
