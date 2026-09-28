"""Disk-backed store-and-forward queue with exponential backoff and batch drain.

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
from typing import Any, Dict, List, Optional, Union
import httpx


class StoreAndForwardBuffer:
    """Manages outbound event buffering, retries, and confirmed dispatch."""

    def __init__(self, buffer_db_path: str = "data/simulator_buffer.db"):
        self.buffer_db_path = Path(buffer_db_path)
        self.buffer_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.buffer_db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
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
            conn.commit()

    def clear(self) -> None:
        """Clear all buffered events (used in test fixtures)."""
        with self._lock, self._get_connection() as conn:
            conn.execute("DELETE FROM outbound_events;")
            conn.commit()

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

        now_iso = datetime.now(timezone.utc).isoformat()
        inserted = 0

        with self._lock, self._get_connection() as conn:
            for ev in events:
                if hasattr(ev, "model_dump"):
                    raw_dict = ev.model_dump()
                elif isinstance(ev, dict):
                    raw_dict = ev
                else:
                    raw_dict = json.loads(str(ev))

                event_id = str(raw_dict.get("event_id"))
                event_json = json.dumps(raw_dict, separators=(",", ":"))

                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO outbound_events (
                        event_id, centre_id, api_key, event_json, created_at, attempts, next_retry_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?);
                    """,
                    (event_id, centre_id, api_key, event_json, now_iso, now_iso),
                )
                if cursor.rowcount == 1:
                    inserted += 1

            conn.commit()

        return inserted

    def get_pending_count(self) -> int:
        """Return the number of unconfirmed events in the buffer."""
        with self._lock, self._get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) AS c FROM outbound_events;")
            row = cursor.fetchone()
            return row["c"] if row else 0

    def drain_once(
        self,
        api_base_url: str = "http://127.0.0.1:8000",
        max_batch_size: int = 50,
        timeout: float = 3.0,
    ) -> Dict[str, int]:
        """Attempt to dispatch the oldest ready events in the buffer to the ingestion API.

        Groups by centre_id and api_key to attach the correct X-API-Key header.
        Only removes events from the buffer after the API confirms receipt.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        stats = {"dispatched": 0, "accepted": 0, "duplicates": 0, "failed": 0}

        with self._lock:
            with self._get_connection() as conn:
                # Select oldest ready events
                cursor = conn.execute(
                    """
                    SELECT id, event_id, centre_id, api_key, event_json, attempts
                    FROM outbound_events
                    WHERE next_retry_at <= ?
                    ORDER BY id ASC
                    LIMIT ?;
                    """,
                    (now_iso, max_batch_size),
                )
                rows = cursor.fetchall()

            if not rows:
                return stats

            # Group rows by (centre_id, api_key)
            batches: Dict[tuple, List[sqlite3.Row]] = {}
            for r in rows:
                key = (r["centre_id"], r["api_key"])
                batches.setdefault(key, []).append(r)

            target_url = f"{api_base_url.rstrip('/')}/v1/events"

            for (centre_id, api_key), batch_rows in batches.items():
                parsed_events = [json.loads(r["event_json"]) for r in batch_rows]
                row_ids = [r["id"] for r in batch_rows]
                attempts = [r["attempts"] for r in batch_rows]

                try:
                    with httpx.Client(timeout=timeout) as client:
                        resp = client.post(
                            target_url,
                            json=parsed_events,
                            headers={"X-API-Key": api_key, "Content-Type": "application/json"},
                        )

                    if resp.status_code in (200, 208):
                        data = resp.json()
                        acc = data.get("accepted", 0)
                        dup = data.get("duplicates", 0)
                        stats["accepted"] += acc
                        stats["duplicates"] += dup
                        stats["dispatched"] += len(batch_rows)

                        # Delete confirmed rows from buffer
                        with self._get_connection() as conn:
                            placeholders = ",".join("?" for _ in row_ids)
                            conn.execute(
                                f"DELETE FROM outbound_events WHERE id IN ({placeholders});",
                                row_ids,
                            )
                            conn.commit()
                    else:
                        # HTTP error: schedule retry with backoff
                        self._handle_failure(row_ids, attempts)
                        stats["failed"] += len(batch_rows)

                except Exception:
                    # Connection refused, timeout, or network drop: schedule retry
                    self._handle_failure(row_ids, attempts)
                    stats["failed"] += len(batch_rows)

        return stats

    def _handle_failure(self, row_ids: List[int], attempts_list: List[int]) -> None:
        """Apply capped exponential backoff to failed items."""
        now = datetime.now(timezone.utc)
        with self._get_connection() as conn:
            for r_id, att in zip(row_ids, attempts_list):
                new_att = att + 1
                backoff_s = min(30.0, 0.5 * (2 ** min(new_att, 6)))
                next_retry = (now + timedelta(seconds=backoff_s)).isoformat()
                conn.execute(
                    """
                    UPDATE outbound_events
                    SET attempts = ?, next_retry_at = ?
                    WHERE id = ?;
                    """,
                    (new_att, next_retry, r_id),
                )
            conn.commit()
