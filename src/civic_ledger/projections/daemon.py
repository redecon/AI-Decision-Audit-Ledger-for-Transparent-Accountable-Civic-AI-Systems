# src/civic_ledger/projections/daemon.py

import asyncio
import logging
from typing import List, Dict
from src.civic_ledger.db import get_connection
from src.civic_ledger.event_store import EventStore
from src.civic_ledger.projections.base import Projection

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    def __init__(self, store: EventStore, projections: List[Projection]):
        self.store = store
        # Register projections by name
        self.projections: Dict[str, Projection] = {p.name: p for p in projections}
        self._running = False

    async def start(self, poll_interval_seconds: float = 0.2):
        """Run the daemon loop until cancelled."""
        self._running = True
        while self._running:
            try:
                processed = await asyncio.to_thread(self._process_batch)
                if processed == 0:
                    await asyncio.sleep(poll_interval_seconds)
            except Exception as e:
                logger.exception("ProjectionDaemon error: %s", e)
                await asyncio.sleep(poll_interval_seconds)

    def stop(self):
        """Signal the daemon loop to stop."""
        self._running = False

    def _process_batch(self) -> int:
        """Synchronously process a batch of events for all projections."""
        # Step 1: read checkpoints
        checkpoints: Dict[str, int] = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                for name in self.projections.keys():
                    cur.execute(
                        "SELECT last_position FROM projection_checkpoints WHERE projection_name = %s",
                        (name,),
                    )
                    row = cur.fetchone()
                    checkpoints[name] = row[0] if row else 0

        # Step 2: load events once from the minimum checkpoint
        min_position = min(checkpoints.values()) if checkpoints else 0
        events = self.store.load_all(from_global_position=min_position)
        if not events:
            return 0

        # Step 3: dispatch events to each projection independently
        for name, projection in self.projections.items():
            last_pos = checkpoints[name]
            new_events = [ev for ev in events if ev["global_position"] > last_pos]
            if not new_events:
                continue

            try:
                with get_connection() as conn:
                    projection.conn = conn
                    for ev in new_events:
                        try:
                            projection.handle_event(ev)
                            projection._last_processed = ev["global_position"]
                            projection._last_event_time = ev.get("recorded_at")
                        except Exception as e:
                            logger.error(
                                "Projection %s failed on event %s: %s",
                                name,
                                ev.get("event_id"),
                                e,
                            )
                            continue

                    # Update checkpoint safely
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (projection_name)
                            DO UPDATE SET last_position = EXCLUDED.last_position, updated_at = NOW()
                            """,
                            (name, projection._last_processed or 0),
                        )
                    conn.commit()
            except Exception as e:
                logger.exception("Projection %s transaction failed: %s", name, e)

        return len(events)
