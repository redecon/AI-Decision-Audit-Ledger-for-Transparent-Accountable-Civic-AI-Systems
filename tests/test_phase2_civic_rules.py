import uuid
import pytest
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.domain.aggregates import DomainError
from src.civic_ledger.domain.aggregates import CaseReportAggregate
from src.civic_ledger.commands.handlers import (
    CaseCategorizedCommand,
    GenerateRecommendationCommand,
    HumanReviewCommand,
    EscalateCommand,
    PublishCommand,
    handle_case_categorized,
    handle_generate_recommendation,
    handle_human_review,
    handle_escalate,
    handle_publish,
)


@pytest.mark.usefixtures("event_store")
class TestPhase2GovernanceRules:

    def test_policy_compliance_required_for_escalation(self, event_store: EventStore):
        case_id = str(uuid.uuid4())
        agent_id = "agent-1"
        session_id = str(uuid.uuid4())

        # Case submitted
        event_store.append(
            f"case-{case_id}", 0, "CaseSubmitted",
            {"citizen": "Amina", "complaint": "Water shortage"},
            {"source": "portal"}
        )
        # Agent context loaded
        event_store.append(
            f"agent-{agent_id}-{session_id}", 0, "AgentContextLoaded",
            {"context_source": "civic-ai", "model_version": "v1.2.0"}, {}
        )
        # Categorize
        cmd = CaseCategorizedCommand(case_id, agent_id, session_id,
                                     "Infrastructure", 0.9, {"text": "Water shortage"}, "v1.2.0")
        handle_case_categorized(cmd, event_store)

        # Recommendation
        rec_cmd = GenerateRecommendationCommand(case_id, "ESCALATE", 0.9,
                                                [{"agent_id": agent_id, "session_id": session_id}])
        handle_generate_recommendation(rec_cmd, event_store)

        # Human review approved
        review_cmd = HumanReviewCommand(case_id, "reviewer-1", "approved", "ok")
        handle_human_review(review_cmd, event_store)

        # Attempt escalation without policy checks
        esc_cmd = EscalateCommand(case_id, "CityCouncil", "urgent")
        with pytest.raises(DomainError):
            handle_escalate(esc_cmd, event_store)

    def test_human_review_required_for_escalation(self, event_store: EventStore):
        case_id = str(uuid.uuid4())
        agent_id = "agent-2"
        session_id = str(uuid.uuid4())

        event_store.append(f"case-{case_id}", 0, "CaseSubmitted",
                           {"citizen": "Biko", "complaint": "Road damage"},
                           {"source": "portal"})
        event_store.append(f"agent-{agent_id}-{session_id}", 0, "AgentContextLoaded",
                           {"context_source": "civic-ai", "model_version": "v1.2.0"}, {})

        cmd = CaseCategorizedCommand(case_id, agent_id, session_id,
                                     "Infrastructure", 0.9, {"text": "Road damage"}, "v1.2.0")
        handle_case_categorized(cmd, event_store)

        rec_cmd = GenerateRecommendationCommand(case_id, "ESCALATE", 0.9,
                                                [{"agent_id": agent_id, "session_id": session_id}])
        handle_generate_recommendation(rec_cmd, event_store)

        # Add policy checks passed
        event_store.append(f"compliance-{case_id}", 0, "PolicyCheckRequested", {"rule_id": "R1"}, {})
        event_store.append(f"compliance-{case_id}", 1, "PolicyRulePassed", {"rule_id": "R1"}, {})

        esc_cmd = EscalateCommand(case_id, "CityCouncil", "urgent")
        with pytest.raises(DomainError):
            handle_escalate(esc_cmd, event_store)

    def test_human_review_required_for_publication(self, event_store: EventStore):
        case_id = str(uuid.uuid4())
        agent_id = "agent-3"
        session_id = str(uuid.uuid4())

        event_store.append(f"case-{case_id}", 0, "CaseSubmitted",
                           {"citizen": "Chika", "complaint": "Electricity outage"},
                           {"source": "portal"})
        event_store.append(f"agent-{agent_id}-{session_id}", 0, "AgentContextLoaded",
                           {"context_source": "civic-ai", "model_version": "v1.2.0"}, {})

        cmd = CaseCategorizedCommand(case_id, agent_id, session_id,
                                     "Infrastructure", 0.9, {"text": "Electricity outage"}, "v1.2.0")
        handle_case_categorized(cmd, event_store)

        rec_cmd = GenerateRecommendationCommand(case_id, "PUBLISH", 0.9,
                                                [{"agent_id": agent_id, "session_id": session_id}])
        handle_generate_recommendation(rec_cmd, event_store)

        # Add policy checks passed
        event_store.append(f"compliance-{case_id}", 0, "PolicyCheckRequested", {"rule_id": "R1"}, {})
        event_store.append(f"compliance-{case_id}", 1, "PolicyRulePassed", {"rule_id": "R1"}, {})

        pub_cmd = PublishCommand(case_id, "Bulletin", "Summary text")
        with pytest.raises(DomainError):
            handle_publish(pub_cmd, event_store)

    def test_human_review_override_can_change_decision(self, event_store: EventStore):
        case_id = str(uuid.uuid4())
        agent_id = "agent-4"
        session_id = str(uuid.uuid4())

        event_store.append(f"case-{case_id}", 0, "CaseSubmitted",
                           {"citizen": "Dina", "complaint": "Flooding"},
                           {"source": "portal"})
        event_store.append(f"agent-{agent_id}-{session_id}", 0, "AgentContextLoaded",
                           {"context_source": "civic-ai", "model_version": "v1.2.0"}, {})

        cmd = CaseCategorizedCommand(case_id, agent_id, session_id,
                                     "Disaster", 0.9, {"text": "Flooding"}, "v1.2.0")
        handle_case_categorized(cmd, event_store)

        rec_cmd = GenerateRecommendationCommand(case_id, "ESCALATE", 0.9,
                                                [{"agent_id": agent_id, "session_id": session_id}])
        handle_generate_recommendation(rec_cmd, event_store)

        review_cmd = HumanReviewCommand(case_id, "reviewer-2", "rejected", "not valid")
        event = handle_human_review(review_cmd, event_store)
        assert event["case_event"]["event_type"] == "HumanReviewCompleted"
        # Case state should be UNDER_REVIEW again
        case = CaseReportAggregate.load(event_store, f"case-{case_id}")
        assert case.state == "UNDER_REVIEW"

    def test_escalation_with_approved_review_succeeds(self, event_store: EventStore):
        case_id = str(uuid.uuid4())
        agent_id = "agent-5"
        session_id = str(uuid.uuid4())

        event_store.append(f"case-{case_id}", 0, "CaseSubmitted",
                           {"citizen": "Eleni", "complaint": "Water contamination"},
                           {"source": "portal"})
        event_store.append(f"agent-{agent_id}-{session_id}", 0, "AgentContextLoaded",
                           {"context_source": "civic-ai", "model_version": "v1.2.0"}, {})

        cmd = CaseCategorizedCommand(case_id, agent_id, session_id,
                                     "Health", 0.9, {"text": "Water contamination"}, "v1.2.0")
        handle_case_categorized(cmd, event_store)

        rec_cmd = GenerateRecommendationCommand(case_id, "ESCALATE", 0.9,
                                                [{"agent_id": agent_id, "session_id": session_id}])
        handle_generate_recommendation(rec_cmd, event_store)

        # Policy checks passed
        event_store.append(f"compliance-{case_id}", 0, "PolicyCheckRequested", {"rule_id": "R1"}, {})
        event_store.append(f"compliance-{case_id}", 1, "PolicyRulePassed", {"rule_id": "R1"}, {})

        # Human review approved
        review_cmd = HumanReviewCommand(case_id, "reviewer-3", "approved", "valid escalation")
        handle_human_review(review_cmd, event_store)

        esc_cmd = EscalateCommand(case_id, "HealthAuthority", "critical")
        event = handle_escalate(esc_cmd, event_store)
        assert event["case_event"]["event_type"] == "CaseEscalated"
