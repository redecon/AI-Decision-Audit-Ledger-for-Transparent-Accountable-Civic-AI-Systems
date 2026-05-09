# src/civic_ledger/projections/agent_accountability.py

import logging
from typing import Dict, Any
from datetime import datetime, timezone
from src.civic_ledger.projections.base import Projection

logger = logging.getLogger(__name__)


class AgentAccountabilityProjection(Projection):
    @property
    def name(self) -> str:
        return "agent_accountability"

    def __init__(self, conn):
        super().__init__(conn)
        self._ensure_table()
        # In-memory mapping: case_id -> contributing agent-model pairs
        self.case_agents: Dict[str, list] = {}
        # Initialize checkpoint tracking
        self._last_processed: int = 0
        self._last_event_time: datetime | None = None

    def _ensure_table(self):
        """Create the projection table if it does not exist."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_accountability_ledger (
                    agent_id TEXT NOT NULL,
                    model_version TEXT NOT NULL,
                    analyses_count INT DEFAULT 0,
                    decisions_count INT DEFAULT 0,
                    escalate_count INT DEFAULT 0,
                    archive_count INT DEFAULT 0,
                    review_count INT DEFAULT 0,
                    override_count INT DEFAULT 0,
                    total_confidence DOUBLE PRECISION DEFAULT 0.0,
                    confidence_action_count INT DEFAULT 0,
                    first_seen_at TIMESTAMPTZ,
                    last_seen_at TIMESTAMPTZ,
                    PRIMARY KEY (agent_id, model_version)
                )
                """
            )
        self.conn.commit()

    def handle_event(self, event: Dict[str, Any]) -> None:
        etype = event["event_type"]
        payload = event.get("payload", {})
        recorded_at = event.get("recorded_at", datetime.now(timezone.utc))

        if etype == "AgentActionRecorded":
            agent_id = payload.get("agent_id")
            model_version = payload.get("model_version")
            confidence = payload.get("confidence_score")
            if not agent_id or not model_version:
                return

            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_accountability_ledger (
                        agent_id, model_version, analyses_count,
                        total_confidence, confidence_action_count,
                        first_seen_at, last_seen_at
                    )
                    VALUES (%s, %s, 1, %s, %s, %s, %s)
                    ON CONFLICT (agent_id, model_version) DO UPDATE SET
                        analyses_count = agent_accountability_ledger.analyses_count + 1,
                        total_confidence = agent_accountability_ledger.total_confidence + EXCLUDED.total_confidence,
                        confidence_action_count = agent_accountability_ledger.confidence_action_count + EXCLUDED.confidence_action_count,
                        last_seen_at = EXCLUDED.last_seen_at
                    """,
                    (
                        agent_id,
                        model_version,
                        confidence or 0.0,
                        1 if confidence is not None else 0,
                        recorded_at,
                        recorded_at,
                    ),
                )
            self.conn.commit()

        elif etype == "RecommendationGenerated":
            contributing = payload.get("contributing_sessions", [])
            recommendation = payload.get("recommendation")
            case_id = event["stream_id"].replace("case-", "")
            self.case_agents[case_id] = []

            for contrib in contributing:
                agent_id = contrib.get("agent_id")
                model_version = contrib.get("model_version")
                if not agent_id or not model_version:
                    continue
                self.case_agents[case_id].append((agent_id, model_version))

                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO agent_accountability_ledger (
                            agent_id, model_version, decisions_count,
                            escalate_count, archive_count, review_count,
                            first_seen_at, last_seen_at
                        )
                        VALUES (%s, %s, 1,
                                %s, %s, %s,
                                %s, %s)
                        ON CONFLICT (agent_id, model_version) DO UPDATE SET
                            decisions_count = agent_accountability_ledger.decisions_count + 1,
                            escalate_count = agent_accountability_ledger.escalate_count + EXCLUDED.escalate_count,
                            archive_count = agent_accountability_ledger.archive_count + EXCLUDED.archive_count,
                            review_count = agent_accountability_ledger.review_count + EXCLUDED.review_count,
                            last_seen_at = EXCLUDED.last_seen_at
                        """,
                        (
                            agent_id,
                            model_version,
                            1 if recommendation == "ESCALATE" else 0,
                            1 if recommendation == "ARCHIVE" else 0,
                            1 if recommendation == "REVIEW" else 0,
                            recorded_at,
                            recorded_at,
                        ),
                    )
            self.conn.commit()

        elif etype == "HumanReviewCompleted":
            case_id = event["stream_id"].replace("case-", "")
            decision = payload.get("decision")
            if decision in ("rejected", "additional_review_needed"):
                agents = self.case_agents.get(case_id, [])
                for agent_id, model_version in agents:
                    with self.conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE agent_accountability_ledger
                            SET override_count = override_count + 1,
                                last_seen_at = %s
                            WHERE agent_id = %s AND model_version = %s
                            """,
                            (recorded_at, agent_id, model_version),
                        )
                self.conn.commit()

        # Always update checkpoint tracking
        self._last_processed = event["global_position"]
        self._last_event_time = recorded_at
