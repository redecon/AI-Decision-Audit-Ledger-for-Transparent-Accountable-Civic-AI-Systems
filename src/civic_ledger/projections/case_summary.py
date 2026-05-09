import logging
from typing import Dict, Any
from datetime import datetime, timezone

from src.civic_ledger.projections.base import Projection

logger = logging.getLogger(__name__)


class CaseSummaryProjection(Projection):
    @property
    def name(self) -> str:
        return "case_summary"

    def __init__(self, conn):
        super().__init__(conn)
        self._ensure_table()
        self._last_processed: int = 0
        self._last_event_time: datetime | None = None

    def _ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS case_summary_projection (
                    case_id TEXT PRIMARY KEY,
                    state TEXT,
                    source TEXT,
                    category TEXT,
                    urgency_level TEXT,
                    submitted_at TIMESTAMPTZ,
                    last_updated_at TIMESTAMPTZ,
                    assigned_authority TEXT,
                    policy_status TEXT,
                    decision TEXT,
                    agent_sessions_completed TEXT[] DEFAULT '{}',
                    last_event_type TEXT,
                    last_event_at TIMESTAMPTZ,
                    human_reviewer_id TEXT,
                    final_decision_at TIMESTAMPTZ
                )
                """
            )
        self.conn.commit()

    def handle_event(self, event: Dict[str, Any]) -> None:
        etype = event["event_type"]
        payload = event.get("payload", {})
        case_id = event["stream_id"].replace("case-", "")
        recorded_at = event.get("recorded_at", datetime.now(timezone.utc))

        updates = {
            "case_id": case_id,
            "state": None,
            "source": None,
            "category": None,
            "urgency_level": None,
            "submitted_at": None,
            "last_updated_at": recorded_at,
            "assigned_authority": None,
            "policy_status": None,
            "decision": None,
            "agent_sessions_completed": [],
            "last_event_type": etype,
            "last_event_at": recorded_at,
            "human_reviewer_id": None,
            "final_decision_at": None,
        }

        if etype == "CaseSubmitted":
            updates.update({
                "state": "SUBMITTED",
                "source": payload.get("source"),
                "submitted_at": recorded_at,
            })

        elif etype == "CaseCategorized":
            updates.update({
                "state": "ANALYZED",
                "category": payload.get("category"),
            })
            agent_id = payload.get("agent_id")
            session_id = payload.get("session_id")
            if agent_id and session_id:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT agent_sessions_completed FROM case_summary_projection WHERE case_id = %s",
                        (case_id,)
                    )
                    row = cur.fetchone()
                    current_sessions = row[0] if row and row[0] else []
                current_sessions.append(f"{agent_id}:{session_id}")
                updates["agent_sessions_completed"] = current_sessions

        elif etype == "RecommendationGenerated":
            updates.update({
                "state": "PENDING_DECISION",
                "decision": payload.get("recommendation"),
            })

        elif etype == "HumanReviewCompleted":
            decision = payload.get("decision")
            updates.update({
                "human_reviewer_id": payload.get("reviewer_id"),
                "final_decision_at": recorded_at,
            })
            if decision in ("approved", "rejected", "additional_review_needed"):
                updates["state"] = "PENDING_DECISION" if decision == "approved" else "UNDER_REVIEW"

        elif etype == "CaseEscalated":
            updates.update({
                "state": "ESCALATED",
                "assigned_authority": payload.get("target_authority"),
            })

        elif etype == "CasePublished":
            updates.update({"state": "PUBLISHED"})

        elif etype == "PolicyCheckRequested":
            updates.update({"policy_status": "pending"})

        elif etype == "PolicyRulePassed":
            updates.update({"policy_status": "passed"})

        elif etype == "PolicyRuleFailed":
            updates.update({"policy_status": "failed"})

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO case_summary_projection (
                    case_id, state, source, category, urgency_level,
                    submitted_at, last_updated_at, assigned_authority,
                    policy_status, decision, agent_sessions_completed,
                    last_event_type, last_event_at, human_reviewer_id,
                    final_decision_at
                )
                VALUES (
                    %(case_id)s, %(state)s, %(source)s, %(category)s, %(urgency_level)s,
                    %(submitted_at)s, %(last_updated_at)s, %(assigned_authority)s,
                    %(policy_status)s, %(decision)s, %(agent_sessions_completed)s,
                    %(last_event_type)s, %(last_event_at)s, %(human_reviewer_id)s,
                    %(final_decision_at)s
                )
                ON CONFLICT (case_id) DO UPDATE SET
                    state = COALESCE(EXCLUDED.state, case_summary_projection.state),
                    source = COALESCE(EXCLUDED.source, case_summary_projection.source),
                    category = COALESCE(EXCLUDED.category, case_summary_projection.category),
                    urgency_level = COALESCE(EXCLUDED.urgency_level, case_summary_projection.urgency_level),
                    submitted_at = COALESCE(EXCLUDED.submitted_at, case_summary_projection.submitted_at),
                    last_updated_at = EXCLUDED.last_updated_at,
                    assigned_authority = COALESCE(EXCLUDED.assigned_authority, case_summary_projection.assigned_authority),
                    policy_status = COALESCE(EXCLUDED.policy_status, case_summary_projection.policy_status),
                    decision = COALESCE(EXCLUDED.decision, case_summary_projection.decision),
                    agent_sessions_completed = COALESCE(EXCLUDED.agent_sessions_completed, case_summary_projection.agent_sessions_completed),
                    last_event_type = EXCLUDED.last_event_type,
                    last_event_at = EXCLUDED.last_event_at,
                    human_reviewer_id = COALESCE(EXCLUDED.human_reviewer_id, case_summary_projection.human_reviewer_id),
                    final_decision_at = COALESCE(EXCLUDED.final_decision_at, case_summary_projection.final_decision_at)
                """,
                updates,
            )
        self.conn.commit()
        self._last_processed = event["global_position"]
        self._last_event_time = recorded_at
