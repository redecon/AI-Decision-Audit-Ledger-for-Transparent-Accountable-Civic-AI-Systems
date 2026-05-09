# src/civic_ledger/domain/aggregates.py

import uuid
from typing import List, Dict, Optional


class DomainError(Exception):
    """
    Raised when a domain rule or state transition is violated.
    This enforces civic accountability by preventing invalid or
    unsafe transitions in the case lifecycle.
    """
    pass


class Aggregate:
    """
    Base class for event-sourced aggregates.
    Provides loading from the event store and applying new changes.
    """

    def __init__(self, stream_id: str):
        self.stream_id: str = stream_id
        self.version: int = 0
        self._pending_events: List[Dict] = []

    @classmethod
    def load(cls, store, stream_id: str) -> "Aggregate":
        """
        Load an aggregate from the event store by replaying its events.
        """
        events = store.load_stream(stream_id)
        agg = cls(stream_id)
        for ev in events:
            agg._apply(ev)
            agg.version = ev["stream_position"]
        return agg

    def _apply(self, event: Dict) -> None:
        """
        Abstract method: apply an event to in-memory state.
        Must be overridden by concrete aggregates.
        """
        raise NotImplementedError

    def apply_change(self, event_type: str, payload: Dict, metadata: Optional[Dict] = None) -> None:
        """
        Create a new event and apply it to in-memory state.
        This is used by command handlers before persisting to the store.
        """
        metadata = metadata or {}
        event = {
            "event_id": str(uuid.uuid4()),
            "stream_id": self.stream_id,
            "stream_position": self.version + 1,
            "event_type": event_type,
            "payload": payload,
            "metadata": metadata,
            "recorded_at": None,  # simplified; real timestamp added by EventStore
        }
        self._apply(event)
        self._pending_events.append(event)
        self.version += 1

    def collect_events(self) -> List[Dict]:
        """
        Return pending events and reset the list.
        This ensures command handlers can batch append events atomically.
        """
        events = self._pending_events
        self._pending_events = []
        return events


class CaseReportAggregate(Aggregate):
    """
    Aggregate for citizen case reports.
    Implements strict state machine transitions to enforce civic accountability.
    """

    STATES = {
        "SUBMITTED",
        "UNDER_REVIEW",
        "ANALYZED",
        "POLICY_CHECKED",
        "PENDING_DECISION",
        "ESCALATED",
        "ARCHIVED",
        "PUBLISHED",
        "RESOLVED",
    }

    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.state: Optional[str] = None
        self.source: Optional[str] = None
        self.description: Optional[str] = None
        self.category: Optional[str] = None
        self.urgency_level: Optional[int] = None
        self.recommendation: Optional[str] = None
        self.reviewer_id: Optional[str] = None
        self.review_decision: Optional[str] = None

    def _apply(self, event: Dict) -> None:
        etype = event["event_type"]
        handler = getattr(self, f"_on_{etype}", None)
        if not handler:
            raise DomainError(f"No handler for event type {etype}")
        handler(event["payload"], event["metadata"])

    # --- Event Handlers ---

    def _on_CaseSubmitted(self, payload: Dict, metadata: Dict):
        """Citizen complaint submitted; must always start lifecycle."""
        self.state = "SUBMITTED"
        self.source = metadata.get("source")
        self.description = payload.get("complaint")

    def _on_CaseCategorized(self, payload: Dict, metadata: Dict):
        """Categorization only valid from SUBMITTED or UNDER_REVIEW."""
        if self.state not in ("SUBMITTED", "UNDER_REVIEW"):
            raise DomainError("Case cannot be categorized from current state")
        self.state = "ANALYZED"
        self.category = payload.get("category")

    def _on_UrgencyScored(self, payload: Dict, metadata: Dict):
        """Urgency scoring requires analysis; state remains ANALYZED."""
        if self.state != "ANALYZED":
            raise DomainError("Urgency can only be scored after analysis")
        self.urgency_level = payload.get("urgency")

    def _on_PolicyCheckRequested(self, payload: Dict, metadata: Dict):
        """Policy compliance check follows analysis."""
        if self.state != "ANALYZED":
            raise DomainError("Policy check must follow analysis")
        self.state = "POLICY_CHECKED"

    def _on_RecommendationGenerated(self, payload: Dict, metadata: Dict):
        """AI recommendation must follow analysis or policy check."""
        if self.state not in ("ANALYZED", "POLICY_CHECKED"):
            raise DomainError("Recommendation must follow analysis or policy check")
        self.state = "PENDING_DECISION"
        self.recommendation = payload.get("recommendation")

    def _on_HumanReviewCompleted(self, payload: Dict, metadata: Dict):
        """
        Human review ensures accountability:
        - Approved decisions are recorded but do not change state directly.
        - Escalation/publication transitions happen only via EscalateCommand/PublishCommand
        after policy compliance checks.
        - Rejected or additional_review_needed decisions send case back to UNDER_REVIEW.
        """
        if self.state != "PENDING_DECISION":
            raise DomainError("Human review must follow pending decision")

        self.reviewer_id = payload.get("reviewer_id")
        self.review_decision = payload.get("decision")

        if self.review_decision in ("rejected", "additional_review_needed"):
            self.state = "UNDER_REVIEW"
        else:
            # Approved decisions leave the case in PENDING_DECISION.
            # Escalate/Publish handlers will enforce final transition.
            pass



    def _on_CaseEscalated(self, payload: Dict, metadata: Dict):
        self.state = "ESCALATED"

    def _on_CasePublished(self, payload: Dict, metadata: Dict):
        self.state = "PUBLISHED"

    def _on_CaseResolved(self, payload: Dict, metadata: Dict):
        self.state = "RESOLVED"

    # --- Assertions for command handlers ---

    def assert_open_for_analysis(self):
        """
        Ensure case is open for analysis.
        Prevents skipping citizen submission or review.
        """
        if self.state not in ("SUBMITTED", "UNDER_REVIEW"):
            raise DomainError("Case is not open for analysis")

    def assert_can_escalate(self):
        """
        Ensure escalation only occurs after recommendation and human review.
        Prevents AI-only escalation without oversight.
        """
        if not self.recommendation or self.review_decision is None:
            raise DomainError("Case cannot be escalated without review approval")
        if self.recommendation != "ESCALATE" or self.review_decision.lower() != "approved":
            raise DomainError("Escalation requires approved human review with recommendation=ESCALATE")


    def assert_can_publish(self):
        """
        Ensure publication only occurs after human review approval.
        Prevents automated dissemination of unverified claims.
        """
        if self.state != "PUBLISHED":
            raise DomainError("Case cannot be published without human review approval")
    def assert_human_review_completed(self):
        """
            Ensure that a HumanReviewCompleted event exists before final actions.
            Governance rule: escalation or publication cannot proceed without
            explicit human validation.

            Raises DomainError if no reviewer_id is recorded.
            """
        if not getattr(self, "reviewer_id", None):
                raise DomainError("Human review is required before this action")
    def assert_review_approved(self, action: str):
        """
        Ensure a HumanReviewCompleted event exists and approves the given action.
        """
        if not getattr(self, "reviewer_id", None):
            raise DomainError("Human review is required before this action")

        if action == "escalate":
            if self.recommendation != "ESCALATE" or self.review_decision.lower() != "approved":
                raise DomainError("Escalation not approved by human review")
        elif action == "publish":
            if self.recommendation != "PUBLISH" or self.review_decision.lower() != "approved":
                raise DomainError("Publication not approved by human review")
        else:
            raise DomainError(f"Unknown action {action}")


