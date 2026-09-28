"""Verifiable, tamper-evident audit hash chain implementation.

Specification:
  entry_hash = SHA256(prev_hash | seq | ts_iso | entry_type | ref_id | canonical_json(payload))
  Genesis prev_hash: 64 zeros ("0" * 64)
  Delimiter: "|"
  canonical_json: UTF-8, keys sorted, zero whitespace separators.
  Serialization: Single-writer lock serialized.
  Verification: Linear sequence walk with pinpoint failure identification.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
from pydantic import BaseModel

from app.db import get_db_connection, write_transaction, resolve_db_path, init_db

GENESIS_PREV_HASH: str = "0" * 64


class AuditEntry(BaseModel):
    seq: int
    ts: str
    entry_type: str
    ref_id: Optional[str]
    payload: Dict[str, Any]
    prev_hash: str
    entry_hash: str


class AuditVerificationResult(BaseModel):
    ok: bool
    total_entries: int
    failing_seq: Optional[int] = None
    head_hash: Optional[str] = None
    error: Optional[str] = None


def canonical_json(data: Union[Dict[str, Any], List[Any], str, int, float, bool, None]) -> str:
    """Format payload into deterministic canonical JSON: sorted keys, no whitespace, UTF-8."""
    if isinstance(data, str):
        try:
            # If a JSON string is passed, parse to dictionary/list to ensure canonical formatting
            parsed = json.loads(data)
            return json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except json.JSONDecodeError:
            return json.dumps(data, ensure_ascii=False)
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_entry_hash(
    prev_hash: str,
    seq: int,
    ts_iso: str,
    entry_type: str,
    ref_id: Optional[str],
    canonical_payload: str,
) -> str:
    """Compute SHA-256 hash using the exact PRD specification:

    entry_hash = SHA256(prev_hash | seq | ts_iso | entry_type | ref_id | canonical_json(payload))
    """
    ref_str = "" if ref_id is None else str(ref_id)
    raw_payload_str = canonical_payload if isinstance(canonical_payload, str) else canonical_json(canonical_payload)
    message = f"{prev_hash}|{seq}|{ts_iso}|{entry_type}|{ref_str}|{raw_payload_str}"
    return hashlib.sha256(message.encode("utf-8")).hexdigest()


def append_audit_entry(
    entry_type: str,
    ref_id: Optional[str],
    payload: Union[Dict[str, Any], List[Any], Any],
    ts_iso: Optional[str] = None,
    db_path: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> AuditEntry:
    """Append a new record to the audit chain under the thread-safe writer lock.

    Guarantees monotonic seq, canonical payload encoding, and link to previous entry_hash.
    If an existing write connection is provided, reuses it to participate in the same transaction.
    """
    # Exact canonical representation
    canon_payload_str = canonical_json(payload)
    parsed_payload = json.loads(canon_payload_str) if isinstance(payload, str) else payload

    # Exact timestamp string that will be both stored and hashed
    if ts_iso is None:
        ts_iso = datetime.now(timezone.utc).isoformat()

    def _execute_append(c: sqlite3.Connection) -> Tuple[int, str, str]:
        cursor = c.cursor()
        cursor.execute("SELECT seq, entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1;")
        latest = cursor.fetchone()

        if latest is None:
            seq = 1
            prev_hash = GENESIS_PREV_HASH
        else:
            seq = int(latest["seq"]) + 1
            prev_hash = str(latest["entry_hash"])

        entry_hash = compute_entry_hash(
            prev_hash=prev_hash,
            seq=seq,
            ts_iso=ts_iso,
            entry_type=entry_type,
            ref_id=ref_id,
            canonical_payload=canon_payload_str,
        )

        cursor.execute(
            """
            INSERT INTO audit_log (seq, ts, entry_type, ref_id, payload, prev_hash, entry_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (seq, ts_iso, entry_type, ref_id, canon_payload_str, prev_hash, entry_hash),
        )
        cursor.close()
        return seq, prev_hash, entry_hash

    if conn is not None:
        seq, prev_hash, entry_hash = _execute_append(conn)
    else:
        target_db = resolve_db_path(db_path)
        init_db(target_db)
        with write_transaction(target_db) as new_conn:
            seq, prev_hash, entry_hash = _execute_append(new_conn)

    return AuditEntry(
        seq=seq,
        ts=ts_iso,
        entry_type=entry_type,
        ref_id=ref_id,
        payload=parsed_payload,
        prev_hash=prev_hash,
        entry_hash=entry_hash,
    )


def verify_audit_chain(db_path: Optional[str] = None) -> AuditVerificationResult:
    """Walk all entries in seq order, recompute hashes, and verify cryptographic chain integrity.

    Returns AuditVerificationResult with ok=True or the FIRST failing sequence number.
    """
    target_db = resolve_db_path(db_path)
    # Ensure database schema exists
    init_db(target_db)

    with get_db_connection(target_db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT seq, ts, entry_type, ref_id, payload, prev_hash, entry_hash FROM audit_log ORDER BY seq ASC;"
        )
        rows = cursor.fetchall()
        cursor.close()

    if not rows:
        return AuditVerificationResult(ok=True, total_entries=0, head_hash=GENESIS_PREV_HASH)

    expected_prev_hash = GENESIS_PREV_HASH
    expected_seq = 1

    for row in rows:
        curr_seq = int(row["seq"])
        curr_ts = str(row["ts"])
        curr_type = str(row["entry_type"])
        curr_ref = row["ref_id"]
        curr_payload_raw = row["payload"]
        curr_prev_hash = str(row["prev_hash"])
        curr_entry_hash = str(row["entry_hash"])

        # Check sequence continuity
        if curr_seq != expected_seq:
            return AuditVerificationResult(
                ok=False,
                total_entries=len(rows),
                failing_seq=curr_seq,
                error=f"Sequence discontinuity: expected seq {expected_seq}, found {curr_seq}",
            )

        # Check prev_hash link
        if curr_prev_hash != expected_prev_hash:
            return AuditVerificationResult(
                ok=False,
                total_entries=len(rows),
                failing_seq=curr_seq,
                error=f"Broken hash link at seq {curr_seq}: expected prev_hash {expected_prev_hash}, found {curr_prev_hash}",
            )

        # Recompute entry hash with canonical payload
        canon_payload = canonical_json(curr_payload_raw)
        recomputed_hash = compute_entry_hash(
            prev_hash=curr_prev_hash,
            seq=curr_seq,
            ts_iso=curr_ts,
            entry_type=curr_type,
            ref_id=curr_ref,
            canonical_payload=canon_payload,
        )

        if recomputed_hash != curr_entry_hash:
            return AuditVerificationResult(
                ok=False,
                total_entries=len(rows),
                failing_seq=curr_seq,
                error=f"Hash mismatch at seq {curr_seq}: stored {curr_entry_hash}, recomputed {recomputed_hash}",
            )

        expected_prev_hash = curr_entry_hash
        expected_seq += 1

    return AuditVerificationResult(ok=True, total_entries=len(rows), head_hash=expected_prev_hash)


def tamper_audit_entry_for_demo(
    seq: int,
    tampered_payload: Optional[Dict[str, Any]] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """[DEMO ONLY] Directly executes a raw SQL UPDATE bypassing hash calculation to simulate tampering.
    Saves the original payload to audit_tamper_backup so that restore can run with no arguments.
    """
    if tampered_payload is None:
        tampered_payload = {"tampered": True, "unauthorized_change": "Malicious score/remedy overwrite"}

    raw_json = json.dumps(tampered_payload, separators=(",", ":"))
    target_db = resolve_db_path(db_path)
    init_db(target_db)

    with write_transaction(target_db) as conn:
        cursor = conn.cursor()
        # Ensure side table for demo backups exists
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_tamper_backup (
                seq INTEGER PRIMARY KEY,
                original_payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

        cursor.execute("SELECT seq, payload, entry_hash FROM audit_log WHERE seq = ?;", (seq,))
        row = cursor.fetchone()
        if not row:
            cursor.close()
            raise ValueError(f"Audit log entry with seq={seq} does not exist.")

        original_payload = row["payload"]
        now_iso = datetime.now(timezone.utc).isoformat()

        # Save to side table
        cursor.execute(
            "INSERT OR REPLACE INTO audit_tamper_backup (seq, original_payload, created_at) VALUES (?, ?, ?);",
            (seq, original_payload, now_iso),
        )

        # Tamper payload
        cursor.execute("UPDATE audit_log SET payload = ? WHERE seq = ?;", (raw_json, seq))
        cursor.close()

    return {
        "status": "tampered",
        "seq": seq,
        "original_payload": original_payload,
        "new_payload": raw_json,
    }


def restore_audit_entry_for_demo(
    seq: Optional[int] = None,
    original_payload: Optional[str] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """[DEMO ONLY] Restores altered audit rows to their original payloads.
    If seq is None, restores ALL entries recorded in audit_tamper_backup.
    """
    target_db = resolve_db_path(db_path)
    init_db(target_db)

    with write_transaction(target_db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_tamper_backup (
                seq INTEGER PRIMARY KEY,
                original_payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

        restored_seqs = []
        if seq is not None and original_payload is not None:
            cursor.execute("UPDATE audit_log SET payload = ? WHERE seq = ?;", (original_payload, seq))
            cursor.execute("DELETE FROM audit_tamper_backup WHERE seq = ?;", (seq,))
            restored_seqs.append(seq)
        elif seq is not None:
            cursor.execute("SELECT original_payload FROM audit_tamper_backup WHERE seq = ?;", (seq,))
            row = cursor.fetchone()
            if row:
                cursor.execute("UPDATE audit_log SET payload = ? WHERE seq = ?;", (row["original_payload"], seq))
                cursor.execute("DELETE FROM audit_tamper_backup WHERE seq = ?;", (seq,))
                restored_seqs.append(seq)
            else:
                cursor.close()
                raise ValueError(f"No backup payload found for seq={seq}")
        else:
            # Restore all backed-up entries
            cursor.execute("SELECT seq, original_payload FROM audit_tamper_backup;")
            rows = cursor.fetchall()
            for r in rows:
                cursor.execute("UPDATE audit_log SET payload = ? WHERE seq = ?;", (r["original_payload"], r["seq"]))
                restored_seqs.append(r["seq"])
            cursor.execute("DELETE FROM audit_tamper_backup;")

        cursor.close()

    return {"status": "restored", "restored_seqs": restored_seqs}


def get_audit_trail(limit: int = 50, db_path: Optional[str] = None) -> List[AuditEntry]:
    """Retrieve the recent audit log entries."""
    target_db = resolve_db_path(db_path)
    init_db(target_db)

    with get_db_connection(target_db) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT seq, ts, entry_type, ref_id, payload, prev_hash, entry_hash
            FROM audit_log
            ORDER BY seq DESC
            LIMIT ?;
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        cursor.close()

    entries = []
    for r in rows:
        try:
            p = json.loads(r["payload"])
        except Exception:
            p = {"raw": r["payload"]}
        entries.append(
            AuditEntry(
                seq=r["seq"],
                ts=r["ts"],
                entry_type=r["entry_type"],
                ref_id=r["ref_id"],
                payload=p,
                prev_hash=r["prev_hash"],
                entry_hash=r["entry_hash"],
            )
        )
    return entries


def main() -> None:
    """CLI utility for verifying and testing the audit chain."""
    parser = argparse.ArgumentParser(description="ERCT Audit Hash Chain CLI")
    parser.add_argument("--verify", action="store_true", help="Verify the integrity of the audit log")
    parser.add_argument("--tamper", type=int, help="[DEMO ONLY] Alter payload of entry at given seq")
    parser.add_argument(
        "--restore",
        nargs="?",
        const=-1,
        type=int,
        default=None,
        help="[DEMO ONLY] Restore tampered entries. If no seq given, restores all.",
    )
    parser.add_argument("--inspect", type=int, default=10, help="Show the last N audit entries")
    args = parser.parse_args()

    if args.verify:
        result = verify_audit_chain()
        if result.ok:
            print(f"[OK] Audit chain valid. Total entries: {result.total_entries}. Head: {result.head_hash}")
            sys.exit(0)
        else:
            print(f"[FAIL] Audit verification failed at seq {result.failing_seq}: {result.error}")
            sys.exit(1)

    elif args.tamper is not None:
        res = tamper_audit_entry_for_demo(args.tamper)
        print(f"[DEMO] Tampered entry seq {args.tamper}. Original payload: {res['original_payload']}")
        sys.exit(0)

    elif args.restore is not None:
        target_seq = None if args.restore == -1 else args.restore
        res = restore_audit_entry_for_demo(seq=target_seq)
        print(f"[DEMO] Restored entries: {res['restored_seqs']}.")
        sys.exit(0)

    else:
        entries = get_audit_trail(limit=args.inspect)
        print(f"--- Showing last {len(entries)} audit entries ---")
        for e in reversed(entries):
            print(f"[{e.seq}] {e.ts} | {e.entry_type} | ref:{e.ref_id} | hash:{e.entry_hash[:16]}... | prev:{e.prev_hash[:16]}...")


if __name__ == "__main__":
    main()
