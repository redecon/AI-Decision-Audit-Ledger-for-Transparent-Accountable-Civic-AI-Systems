# src/civic_ledger/commands/handlers.py

import uuid
import hashlib
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from src.civic_ledger.domain.aggregates import DomainError, CaseReportAggregate
from src.civic_ledger.domain.agent_session import AgentSessionAggregate
from src.civic_ledger.domain.policy_compliance import PolicyComplianceRecord
from src.civic_ledger.event_store.repository import EventStore


# --- Case Categorization Command ---

@dataclass
class CaseCategorizedCommand:
    case_id: str
    agent_id: str
    session_id: str
    category: str
    confidence_score: float
    input_data: dict
    model_version: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


def handle_case_categorized(cmd: CaseCategorizedCommand, store: EventStore) -> Dict[str, Any]:
    """
    Handle CaseCategorizedCommand by orchestrating updates to both CaseReport and AgentSession aggregates.

    Governance rules enforced:
    - Case must be open for analysis.
    - Agent session must have context loaded (Gas Town).
    - Agent session must use the declared model version.
    - Confidence floor: low-confidence (<0.6) classifications are recorded but
      immediately flagged for human review.
    - All events carry correlation_id/causation_id for traceability.
    """

    case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")
    agent = AgentSessionAggregate.load(store, f"agent-{cmd.agent_id}-{cmd.session_id}")

    case.assert_open_for_analysis()
    agent.assert_context_loaded()
    agent.assert_model_version_current(cmd.model_version)

    input_data_hash = hashlib.sha256(
        json.dumps(cmd.input_data, sort_keys=True).encode("utf-8")
    ).hexdigest()

    case_payload = {
        "category": cmd.category,
        "confidence_score": cmd.confidence_score,
        "input_data_hash": input_data_hash,
        "agent_id": cmd.agent_id,
        "session_id": cmd.session_id,
        "model_version": cmd.model_version,
    }

    correlation_id = cmd.correlation_id or str(uuid.uuid4())

    case_event = store.append(
        stream_id=f"case-{cmd.case_id}",
        expected_last_position=case.version,
        event_type="CaseCategorized",
        payload_dict=case_payload,
        metadata_dict={
            "correlation_id": correlation_id,
            "causation_id": cmd.causation_id,
            "source": "AI",
            "model_version": cmd.model_version,
        },
    )
    case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")

    agent_payload = {
        "action_type": "CaseCategorized",
        "case_id": cmd.case_id,
        "category": cmd.category,
        "confidence_score": cmd.confidence_score,
        "outcome": "categorized",
        "timestamp": case_event["recorded_at"].isoformat(),
        "model_version": cmd.model_version,
    }

    agent_event = store.append(
        stream_id=f"agent-{cmd.agent_id}-{cmd.session_id}",
        expected_last_position=agent.version,
        event_type="AgentActionRecorded",
        payload_dict=agent_payload,
        metadata_dict={
            "correlation_id": correlation_id,
            "causation_id": cmd.causation_id,
            "source": "AI",
            "model_version": cmd.model_version,
        },
    )

    recommendation_event = None
    if cmd.confidence_score < 0.6:
        rec_payload = {
            "recommendation": "REVIEW",
            "confidence_score": cmd.confidence_score,
            "automated_override": False,
        }
        recommendation_event = store.append(
            stream_id=f"case-{cmd.case_id}",
            expected_last_position=case.version,
            event_type="RecommendationGenerated",
            payload_dict=rec_payload,
            metadata_dict={
                "correlation_id": correlation_id,
                "causation_id": cmd.causation_id,
                "source": "AI",
                "model_version": cmd.model_version,
            },
        )
        case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")

    return {
        "case_events": [case_event] + ([recommendation_event] if recommendation_event else []),
        "agent_event": agent_event,
    }


# --- Recommendation Command (Rule 6: Causal Chain) ---

@dataclass
class GenerateRecommendationCommand:
    case_id: str
    recommendation: str   # ESCALATE, PUBLISH, ARCHIVE, REVIEW
    confidence_score: float
    supporting_agents: List[Dict[str, str]]  # [{agent_id, session_id}, ...]
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


def handle_generate_recommendation(cmd: GenerateRecommendationCommand, store: EventStore) -> Dict[str, Any]:
    case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")
    if case.state not in ("ANALYZED", "POLICY_CHECKED"):
        raise DomainError("Case must be analyzed or policy checked before recommendation")

    contributing_sessions = []
    for agent in cmd.supporting_agents:
        agent_id = agent.get("agent_id")
        session_id = agent.get("session_id")
        if not agent_id or not session_id:
            raise DomainError("supporting_agents must include agent_id and session_id")

        agent_session = AgentSessionAggregate.load(store, f"agent-{agent_id}-{session_id}")
        agent_session.assert_context_loaded()
        # check: ensure agent actually contributed to this case
        if agent_session.last_case_id != cmd.case_id:
         raise DomainError(f"Causal chain broken: agent {agent_id} did not contribute to case {cmd.case_id}")
        contributing_sessions.append({"agent_id": agent_id, "session_id": session_id})

    payload = {
        "recommendation": cmd.recommendation,
        "confidence_score": cmd.confidence_score,
        "contributing_sessions": contributing_sessions,
    }
    correlation_id = cmd.correlation_id or str(uuid.uuid4())

    event = store.append(
        stream_id=f"case-{cmd.case_id}",
        expected_last_position=case.version,
        event_type="RecommendationGenerated",
        payload_dict=payload,
        metadata_dict={
            "correlation_id": correlation_id,
            "causation_id": cmd.causation_id,
            "source": "AI",
        },
    )
    return {"case_event": event}


# --- Human Review Command (Rule 7: Override Requirement) ---

@dataclass
class HumanReviewCommand:
    case_id: str
    reviewer_id: str
    decision: str   # "approved", "rejected", "additional_review_needed"
    override_reason: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


def handle_human_review(cmd: HumanReviewCommand, store: EventStore) -> Dict[str, Any]:
    """
    Handle HumanReviewCommand by recording human oversight.

    Governance rules enforced:
    - Case must be in PENDING_DECISION.
    - A recommendation must exist before review.
    - HumanReviewCompleted event records reviewer_id, decision, override_reason.
    - Approved decisions keep case in PENDING_DECISION; escalation/publish
      will be enforced later by their handlers.
    - Rejected or additional_review_needed decisions push case back to UNDER_REVIEW.
    """

    case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")
    if case.state != "PENDING_DECISION":
        raise DomainError("Human review can only be performed when case is pending decision")

    if not case.recommendation:
        raise DomainError("Cannot complete human review without an existing recommendation")

    payload = {
        "reviewer_id": cmd.reviewer_id,
        "decision": cmd.decision,
        "override_reason": cmd.override_reason,
    }
    correlation_id = cmd.correlation_id or str(uuid.uuid4())

    event = store.append(
        stream_id=f"case-{cmd.case_id}",
        expected_last_position=case.version,
        event_type="HumanReviewCompleted",
        payload_dict=payload,
        metadata_dict={
            "correlation_id": correlation_id,
            "causation_id": cmd.causation_id,
            "source": "Human",
        },
    )

    # Reload aggregate to apply decision (UNDER_REVIEW if rejected/extra review)
    case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")

    return {"case_event": event}

@dataclass
class EscalateCommand:
    case_id: str
    target_authority: str
    reason: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


def handle_escalate(cmd: EscalateCommand, store: EventStore) -> Dict[str, Any]:
    case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")
    if case.state != "PENDING_DECISION":
        raise DomainError("Case must be pending decision before escalation")

    compliance = PolicyComplianceRecord.load(store, cmd.case_id)
    if not compliance.all_checks_completed():
        raise DomainError("Policy checks are incomplete – cannot escalate")

    # Human override check
    case.assert_review_approved("escalate")

    payload = {
        "target_authority": cmd.target_authority,
        "reason": cmd.reason,
        "reviewer_id": case.reviewer_id,
    }
    correlation_id = cmd.correlation_id or str(uuid.uuid4())

    event = store.append(
        stream_id=f"case-{cmd.case_id}",
        expected_last_position=case.version,
        event_type="CaseEscalated",
        payload_dict=payload,
        metadata_dict={
            "correlation_id": correlation_id,
            "causation_id": cmd.causation_id,
            "source": "Human",
        },
    )

    return {"case_event": event}

@dataclass
class PublishCommand:
    case_id: str
    publication_channel: str
    summary: str
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


def handle_publish(cmd: PublishCommand, store: EventStore) -> Dict[str, Any]:
    case = CaseReportAggregate.load(store, f"case-{cmd.case_id}")
    if case.state != "PENDING_DECISION":
        raise DomainError("Case must be pending decision before publication")

    compliance = PolicyComplianceRecord.load(store, cmd.case_id)
    if not compliance.all_checks_completed():
        raise DomainError("Policy checks are incomplete – cannot publish")

    # Human override check
    case.assert_review_approved("publish")

    payload = {
        "publication_channel": cmd.publication_channel,
        "summary": cmd.summary,
        "reviewer_id": case.reviewer_id,
    }
    correlation_id = cmd.correlation_id or str(uuid.uuid4())

    event = store.append(
        stream_id=f"case-{cmd.case_id}",
        expected_last_position=case.version,
        event_type="CasePublished",
        payload_dict=payload,
        metadata_dict={
            "correlation_id": correlation_id,
            "causation_id": cmd.causation_id,
            "source": "Human",
        },
    )

    return {"case_event": event}
