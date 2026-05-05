# src/civic_ledger/domain/aggregates.py

import uuid
from typing import List, Dict, Optional


class DomainError(Exception):
    """Raised when a domain rule or state transition is violated."""
    pass


class Aggregate:
    """
    Base class for event-sourced aggregates.
    Provides loading from an event stream and applying new changes.
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

    def _apply(self, event: Dict):
        """
        Abstract method: apply an event to in-memory state.
        Must be overridden by concrete aggregates.
        """
        raise NotImplementedError

    def apply_change(self, event_type: str, payload: Dict, metadata: Optional[Dict] = None):
        """
        Create a new event and apply it to in-memory state.
        """
        metadata = metadata or {}
        event = {
            "event_id": str(uuid.uuid4()),
            "stream_id": self.stream_id,
            "stream_position": self.version + 1,
            "event_type": event_type,
            "payload": payload,
            "metadata": metadata,
        }
        self._apply(event)
        self._pending_events.append(event)
        self.version += 1

    def collect_events(self) -> List[Dict]:
        """Return pending events to be appended to the store."""
        return self._pending_events


class CaseReportAggregate(Aggregate):
    """
    Aggregate for citizen case reports.
    Implements strict state machine transitions.
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
        self.urgency: Optional[int] = None
        self.recommendation: Optional[str] = None
        self.reviewer_id: Optional[str] = None
        self.review_override: Optional[str] = None

    def _apply(self, event: Dict):
        etype = event["event_type"]
        handler = getattr(self, f"_on_{etype}", None)
        if not handler:
            raise DomainError(f"No handler for event type {etype}")
        handler(event["payload"], event["metadata"])

    # --- Event Handlers ---

    def _on_CaseSubmitted(self, payload: Dict, metadata: Dict):
        self.state = "SUBMITTED"
        self.source = metadata.get("source")
        self.description = payload.get("complaint")

    def _on_CaseCategorized(self, payload: Dict, metadata: Dict):
        if self.state not in ("SUBMITTED", "UNDER_REVIEW"):
            raise DomainError("Case cannot be categorized from current state")
        self.state = "ANALYZED"

    def _on_UrgencyScored(self, payload: Dict, metadata: Dict):
        if self.state != "ANALYZED":
            raise DomainError("Urgency can only be scored after analysis")
        self.urgency = payload.get("urgency")

    def _on_PolicyCheckRequested(self, payload: Dict, metadata: Dict):
        if self.state != "ANALYZED":
            raise DomainError("Policy check must follow analysis")
        self.state = "POLICY_CHECKED"

    def _on_RecommendationGenerated(self, payload: Dict, metadata: Dict):
        if self.state not in ("ANALYZED", "POLICY_CHECKED"):
            raise DomainError("Recommendation must follow analysis or policy check")
        self.state = "PENDING_DECISION"
        self.recommendation = payload.get("recommendation")

    def _on_HumanReviewCompleted(self, payload: Dict, metadata: Dict):
        if self.state != "PENDING_DECISION":
            raise DomainError("Human review must follow pending decision")
        self.reviewer_id = payload.get("reviewer_id")
        self.review_override = payload.get("decision")
        if self.review_override == "ESCALATE":
            self.state = "ESCALATED"
        elif self.review_override == "PUBLISH":
            self.state = "PUBLISHED"
        elif self.review_override == "RESOLVE":
            self.state = "RESOLVED"
        else:
            raise DomainError("Invalid human review decision")

    def _on_CaseEscalated(self, payload: Dict, metadata: Dict):
        self.state = "ESCALATED"

    def _on_CasePublished(self, payload: Dict, metadata: Dict):
        self.state = "PUBLISHED"

    def _on_CaseResolved(self, payload: Dict, metadata: Dict):
        self.state = "RESOLVED"

    # --- Assertions for command handlers ---

    def assert_open_for_analysis(self):
        if self.state not in ("SUBMITTED", "UNDER_REVIEW", "ANALYZED"):
            raise DomainError("Case is not open for analysis")

    def assert_pending_decision(self):
        if self.state not in ("ANALYZED", "POLICY_CHECKED", "PENDING_DECISION"):
            raise DomainError("Case is not pending decision")

    def assert_can_publish(self):
        if self.state != "PUBLISHED":
            raise DomainError("Case cannot be published without review approval")

    def assert_no_duplicate_model(self, model_version: str):
        # Example check: ensure recommendation not already generated by same model
        if self.recommendation and self.recommendation == model_version:
            raise DomainError("Duplicate recommendation from same model")
