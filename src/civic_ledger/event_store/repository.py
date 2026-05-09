import uuid
import json
import hashlib
from datetime import datetime, timezone
from psycopg.errors import SerializationFailure

from src.civic_ledger.db import get_connection
from src.civic_ledger.event_store.exceptions import ConcurrencyError
from typing import List, Optional
from src.civic_ledger.upcasting.registry import UpcasterRegistry
from src.civic_ledger.db import get_connection

class EventStore:
    """
    EventStore provides append-only event sourcing with hash-chain integrity,
    concurrency control, and outbox publishing for civic audit ledgers.
    """

    def append(
        self,
        stream_id: str,
        event_type: str,
        payload_dict: dict,
        expected_last_position: int | None = None,
        metadata_dict: dict | None = None,
        recorded_at: datetime | None = None,
    ):
        metadata_dict = metadata_dict or {}
        if "correlation_id" not in metadata_dict:
            metadata_dict["correlation_id"] = str(uuid.uuid4())
        if recorded_at is None:
            recorded_at = datetime.now(timezone.utc).isoformat()
        elif isinstance(recorded_at, datetime):
            recorded_at = recorded_at.isoformat()

        with get_connection() as conn:
            try:
                conn.execute("BEGIN ISOLATION LEVEL SERIALIZABLE;")
                cur = conn.cursor()

                # Lock the stream row
                cur.execute(
                    "SELECT current_version FROM event_streams WHERE stream_id=%s FOR UPDATE;",
                    (stream_id,)
                )
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "INSERT INTO event_streams (stream_id, aggregate_type, current_version) VALUES (%s, %s, %s);",
                        (stream_id, "CaseReport", 0)
                    )
                    current_version = 0
                else:
                    current_version = row[0]

                if expected_last_position != current_version:
                    raise ConcurrencyError(
                        f"Expected last position {expected_last_position}, but current_version is {current_version}"
                    )

                new_position = current_version + 1
                event_id = str(uuid.uuid4())
                recorded_at = datetime.now(timezone.utc).isoformat()
                payload_json = json.dumps(payload_dict, separators=(",", ":"))
                metadata_json = json.dumps(metadata_dict, separators=(",", ":"))

                # Get previous hash
                cur.execute(
                    "SELECT integrity_hash FROM events WHERE stream_id=%s AND stream_position=%s;",
                    (stream_id, current_version)
                )
                prev_row = cur.fetchone()
                previous_hash = prev_row[0] if prev_row else ""

                # Compute integrity hash
                hash_input = f"{stream_id}:{new_position}:{event_type}:{payload_json}:{previous_hash}:{recorded_at}"
                integrity_hash = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

                # Insert event
                cur.execute(
                    """
                    INSERT INTO events (event_id, stream_id, stream_position, event_type, payload, metadata,
                                        recorded_at, integrity_hash, previous_hash)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::timestamptz, %s, %s)
                    RETURNING event_id, stream_id, stream_position, event_type, payload, metadata, recorded_at, integrity_hash;
                    """,
                    (event_id, stream_id, new_position, event_type, payload_json, metadata_json, recorded_at, integrity_hash, previous_hash)
                )
                event_row = cur.fetchone()

                # Update stream version
                cur.execute(
                    "UPDATE event_streams SET current_version=%s WHERE stream_id=%s;",
                    (new_position, stream_id)
                )

                # Insert into outbox
                summary = {
                    "event_id": event_id,
                    "stream_id": stream_id,
                    "stream_position": new_position,
                    "event_type": event_type,
                    "recorded_at": recorded_at
                }
                cur.execute(
                    """
                    INSERT INTO outbox (event_id, destination, payload)
                    VALUES (%s, %s, %s::jsonb);
                    """,
                    (event_id, "default", json.dumps(summary, separators=(",", ":")))
                )

                conn.commit()
                return dict(zip(
                    ["event_id", "stream_id", "stream_position", "event_type", "payload", "metadata", "recorded_at", "integrity_hash"],
                    event_row
                ))

            except SerializationFailure as e:
                conn.rollback()
                # Translate DB-level concurrency abort into civic ConcurrencyError
                raise ConcurrencyError("Concurrent update detected") from e

            except Exception as e:
                conn.rollback()
                if isinstance(e, ConcurrencyError):
                    raise
                raise
    def load_stream(
        self,
        stream_id: str,
        upcaster_registry: Optional[UpcasterRegistry] = None
    ) -> List[dict]:
        """
        Return all events for a given stream ordered by stream_position.
        If an UpcasterRegistry is provided, upcast each event in-memory before returning.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT event_id, stream_id, stream_position, global_position,
                           event_type, payload, metadata, recorded_at,
                           integrity_hash, previous_hash, event_version
                    FROM events
                    WHERE stream_id = %s
                    ORDER BY stream_position ASC
                    """,
                    (stream_id,),
                )
                rows = cur.fetchall()

        events = []
        for r in rows:
            event = {
                "event_id": r[0],
                "stream_id": r[1],
                "stream_position": r[2],
                "global_position": r[3],
                "event_type": r[4],
                "payload": r[5],
                "metadata": r[6],
                "recorded_at": r[7],
                "integrity_hash": r[8],
                "previous_hash": r[9],
                "event_version": r[10],
            }
            if upcaster_registry:
                event = upcaster_registry.upcast(event)
            events.append(event)

        return events

    def load_all(
        self,
        from_global_position: int = 0,
        upcaster_registry: Optional[UpcasterRegistry] = None
    ) -> List[dict]:
        """
        Return all events with global_position > from_global_position ordered by global_position.
        If an UpcasterRegistry is provided, upcast each event in-memory before returning.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT global_position, event_id, stream_id, stream_position,
                           event_type, payload, metadata, recorded_at,
                           integrity_hash, previous_hash, event_version
                    FROM events
                    WHERE global_position > %s
                    ORDER BY global_position ASC
                    """,
                    (from_global_position,),
                )
                rows = cur.fetchall()

        events = []
        for r in rows:
            event = {
                "global_position": r[0],
                "event_id": r[1],
                "stream_id": r[2],
                "stream_position": r[3],
                "event_type": r[4],
                "payload": r[5],
                "metadata": r[6],
                "recorded_at": r[7],
                "integrity_hash": r[8],
                "previous_hash": r[9],
                "event_version": r[10],
            }
            if upcaster_registry:
                event = upcaster_registry.upcast(event)
            events.append(event)

        return events


    def verify_stream_integrity(self, stream_id):
        """Recompute the hash chain for a stream and verify integrity."""
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT stream_position, event_type, payload, recorded_at, integrity_hash, previous_hash "
                "FROM events WHERE stream_id=%s ORDER BY stream_position ASC;",
                (stream_id,)
            )
            events = cur.fetchall()

        prev_hash = ""
        for pos, etype, payload, recorded_at, integrity_hash, previous_hash in events:
            payload_json = json.dumps(payload, separators=(",", ":"))
            hash_input = f"{stream_id}:{pos}:{etype}:{payload_json}:{prev_hash}:{recorded_at.isoformat()}"
            recomputed = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
            if recomputed != integrity_hash or previous_hash != prev_hash:
                return False
            prev_hash = integrity_hash
        return True
