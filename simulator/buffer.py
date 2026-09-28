"""Disk-backed store-and-forward queue with exponential backoff, dead-letter routing, and per-centre pause.

Runs in the simulator process. Operates on its own dedicated SQLite buffer file.
Never accesses the primary ERCT application database.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
import httpx


def format_utc_iso(dt: Optional[datetime] = None) -> str:
    """Return canonical UTC ISO 8601 string: YYYY-MM-DDTHH:MM:SS.ffffff+00:00.

    Guarantees fixed length (32 chars) and fixed '+00:00' timezone suffix so that
    lexicographical text comparisons (<, <=, >, >=) in SQLite match true chronological order.
    """
    d = dt or datetime.now(timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    else:
        d = d.astimezone(timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


class StoreAndForwardBuffer:
    """Manages outbound event buffering, retries, dead-lettering, per-centre pause, and confirmed dispatch."""

    def __init__(self, buffer_db_path: str = "data/simulator_buffer.db"):
        self.buffer_db_path = Path(buffer_db_path)
        self.buffer_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.buffer_db_path), timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    def _init_db(self) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS outbound_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE NOT NULL,
                    centre_id TEXT NOT NULL,
                    api_key TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_outbound_retry ON outbound_events(next_retry_at, id);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_outbound_centre ON outbound_events(centre_id);"
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dead_letter (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    centre_id TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    error_detail TEXT,
                    failed_at TEXT NOT NULL
                );
                """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS centre_pause_status (
                    centre_id TEXT PRIMARY KEY,
                    is_paused INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.commit()

    def clear(self) -> None:
        """Clear all buffered events and dead letter records (used in test fixtures)."""
        with self._lock, self._get_connection() as conn:
            conn.execute("DELETE FROM outbound_events;")
            conn.execute("DELETE FROM dead_letter;")
            conn.execute("DELETE FROM centre_pause_status;")
            conn.commit()

    def pause_centre(self, centre_id: str) -> None:
        """Pause outgoing delivery for a specific centre (simulates network drop)."""
        now_iso = format_utc_iso()
        with self._lock, self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO centre_pause_status (centre_id, is_paused, updated_at)
                VALUES (?, 1, ?);
                """,
                (centre_id, now_iso),
            )
            conn.commit()

    def resume_centre(self, centre_id: str) -> None:
        """Resume outgoing delivery for a specific centre."""
        now_iso = format_utc_iso()
        with self._lock, self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO centre_pause_status (centre_id, is_paused, updated_at)
                VALUES (?, 0, ?);
                """,
                (centre_id, now_iso),
            )
            conn.commit()

    def is_centre_paused(self, centre_id: str) -> bool:
        """Check whether delivery is currently paused for a centre."""
        with self._lock, self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT is_paused FROM centre_pause_status WHERE centre_id = ?;",
                (centre_id,),
            )
            row = cursor.fetchone()
            return bool(row and row["is_paused"] == 1)

    def get_paused_centres(self) -> Set[str]:
        """Return the set of currently paused centre IDs."""
        with self._lock, self._get_connection() as conn:
            cursor = conn.execute("SELECT centre_id FROM centre_pause_status WHERE is_paused = 1;")
            rows = cursor.fetchall()
            return {r["centre_id"] for r in rows}

    def enqueue(
        self,
        event: Union[Dict[str, Any], Any],
        centre_id: str,
        api_key: str,
    ) -> bool:
        """Enqueue a single event into the store-and-forward buffer."""
        return self.enqueue_batch([event], centre_id=centre_id, api_key=api_key) == 1

    def enqueue_batch(
        self,
        events: List[Union[Dict[str, Any], Any]],
        centre_id: str,
        api_key: str,
    ) -> int:
        """Enqueue multiple events into the buffer atomically."""
        if not events:
            return 0

        now_iso = format_utc_iso()
        rows_to_insert = []

        for ev in events:
            if hasattr(ev, "model_dump"):
                raw_dict = ev.model_dump()
            elif isinstance(ev, dict):
                raw_dict = ev
            else:
                raw_dict = json.loads(str(ev))

            event_id = str(raw_dict.get("event_id"))
            event_json = json.dumps(raw_dict, separators=(",", ":"))
            rows_to_insert.append((event_id, centre_id, api_key, event_json, now_iso, now_iso))

        with self._lock, self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT OR IGNORE INTO outbound_events (
                    event_id, centre_id, api_key, event_json, created_at, attempts, next_retry_at
                ) VALUES (?, ?, ?, ?, ?, 0, ?);
                """,
                rows_to_insert,
            )
            inserted = cursor.rowcount
            conn.commit()

        return inserted

    def get_pending_count(self, centre_id: Optional[str] = None) -> int:
        """Return the count of unconfirmed events pending in the buffer."""
        with self._lock, self._get_connection() as conn:
            if centre_id:
                cursor = conn.execute(
                    "SELECT COUNT(*) AS c FROM outbound_events WHERE centre_id = ?;",
                    (centre_id,),
                )
            else:
                cursor = conn.execute("SELECT COUNT(*) AS c FROM outbound_events;")
            row = cursor.fetchone()
            return row["c"] if row else 0

    def get_dead_letter_count(self) -> int:
        """Return the count of unprocessable events moved to dead-letter storage."""
        with self._lock, self._get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) AS c FROM dead_letter;")
            row = cursor.fetchone()
            return row["c"] if row else 0

    def drain_once(
        self,
        api_base_url: str = "http://127.0.0.1:8000",
        max_batch_size: int = 500,
        timeout: float = 5.0,
        client: Optional[httpx.Client] = None,
    ) -> Dict[str, int]:
        """Dispatch up to max_batch_size oldest eligible events to the ingestion API.

        - Skips centres that are currently paused.
        - 200/208: removes confirmed events.
        - 422: permanently invalid events moved to dead_letter table, never retried.
        - 401, 5xx, or network failure: applies capped exponential backoff.
        """
        now_iso = format_utc_iso()
        stats = {"dispatched": 0, "accepted": 0, "duplicates": 0, "dead_lettered": 0, "failed": 0}

        paused_centres = self.get_paused_centres()

        # Step 1: Select eligible rows under lock and temporarily mark them in-flight
        with self._lock:
            with self._get_connection() as conn:
                if paused_centres:
                    placeholders = ",".join("?" for _ in paused_centres)
                    query = f"""
                        SELECT id, event_id, centre_id, api_key, event_json, attempts
                        FROM outbound_events
                        WHERE next_retry_at <= ? AND centre_id NOT IN ({placeholders})
                        ORDER BY id ASC
                        LIMIT ?;
                    """
                    params = [now_iso] + list(paused_centres) + [max_batch_size]
                else:
                    query = """
                        SELECT id, event_id, centre_id, api_key, event_json, attempts
                        FROM outbound_events
                        WHERE next_retry_at <= ?
                        ORDER BY id ASC
                        LIMIT ?;
                    """
                    params = [now_iso, max_batch_size]

                cursor = conn.execute(query, params)
                rows = cursor.fetchall()

                if not rows:
                    return stats

        # Step 2: Group rows by (centre_id, api_key) and dispatch over HTTP (NO LOCK HELD)
        batches: Dict[tuple, List[sqlite3.Row]] = {}
        for r in rows:
            key = (r["centre_id"], r["api_key"])
            batches.setdefault(key, []).append(r)

        target_url = f"{api_base_url.rstrip('/')}/v1/events"
        own_client = client is None
        http_client = client or httpx.Client(timeout=timeout)

        try:
            for (centre_id, api_key), batch_rows in batches.items():
                parsed_events = [json.loads(r["event_json"]) for r in batch_rows]
                row_ids = [r["id"] for r in batch_rows]
                event_ids = [r["event_id"] for r in batch_rows]
                event_jsons = [r["event_json"] for r in batch_rows]
                attempts = [r["attempts"] for r in batch_rows]

                try:
                    resp = http_client.post(
                        target_url,
                        json=parsed_events,
                        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
                    )

                    if resp.status_code in (200, 208):
                        data = resp.json()
                        stats["accepted"] += data.get("accepted", 0)
                        stats["duplicates"] += data.get("duplicates", 0)
                        stats["dispatched"] += len(batch_rows)

                        with self._lock, self._get_connection() as conn:
                            placeholders = ",".join("?" for _ in row_ids)
                            conn.execute(
                                f"DELETE FROM outbound_events WHERE id IN ({placeholders});",
                                row_ids,
                            )
                            conn.commit()

                    elif resp.status_code == 422:
                        error_detail = resp.text
                        now_dl = format_utc_iso()
                        dl_rows = [
                            (eid, centre_id, ejson, error_detail, now_dl)
                            for eid, ejson in zip(event_ids, event_jsons)
                        ]
                        with self._lock, self._get_connection() as conn:
                            conn.executemany(
                                """
                                INSERT INTO dead_letter (event_id, centre_id, event_json, error_detail, failed_at)
                                VALUES (?, ?, ?, ?, ?);
                                """,
                                dl_rows,
                            )
                            placeholders = ",".join("?" for _ in row_ids)
                            conn.execute(
                                f"DELETE FROM outbound_events WHERE id IN ({placeholders});",
                                row_ids,
                            )
                            conn.commit()
                        stats["dead_lettered"] += len(batch_rows)

                    else:
                        print(f"[DRAIN HTTP STATUS] {resp.status_code}: {resp.text[:120]}")
                        with self._lock:
                            self._handle_failure(row_ids, attempts)
                        stats["failed"] += len(batch_rows)

                except Exception as e:
                    print(f"[DRAIN EXCEPTION] {type(e).__name__}: {e}")
                    with self._lock:
                        self._handle_failure(row_ids, attempts)
                    stats["failed"] += len(batch_rows)
        finally:
            if own_client:
                http_client.close()

        return stats

    def drain_all(
        self,
        api_base_url: str = "http://127.0.0.1:8000",
        max_batch_size: int = 500,
        max_iterations: int = 50,
    ) -> Dict[str, int]:
        """Keep draining batches until no more eligible events remain or max_iterations reached."""
        totals = {"dispatched": 0, "accepted": 0, "duplicates": 0, "dead_lettered": 0, "failed": 0}
        for _ in range(max_iterations):
            if self.get_pending_count() == 0:
                break
            res = self.drain_once(api_base_url=api_base_url, max_batch_size=max_batch_size)
            if res["dispatched"] == 0 and res["dead_lettered"] == 0:
                # No progress made (e.g. paused or backing off)
                break
            for k in totals:
                totals[k] += res.get(k, 0)
        return totals

    def _handle_failure(self, row_ids: List[int], attempts_list: List[int]) -> None:
        """Apply capped exponential backoff to failed items."""
        now = datetime.now(timezone.utc)
        with self._get_connection() as conn:
            for r_id, att in zip(row_ids, attempts_list):
                new_att = att + 1
                backoff_s = min(30.0, 0.5 * (2 ** min(new_att, 6)))
                next_retry = format_utc_iso(now + timedelta(seconds=backoff_s))
                conn.execute(
                    """
                    UPDATE outbound_events
                    SET attempts = ?, next_retry_at = ?
                    WHERE id = ?;
                    """,
                    (new_att, next_retry, r_id),
                )
            conn.commit()
