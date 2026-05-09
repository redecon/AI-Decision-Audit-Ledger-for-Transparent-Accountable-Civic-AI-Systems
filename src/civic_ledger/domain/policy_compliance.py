# src/civic_ledger/domain/policy_compliance.py

from typing import Dict, Set
from src.civic_ledger.domain.aggregates import Aggregate, DomainError


class PolicyComplianceRecord(Aggregate):
    """
    Aggregate for policy compliance checks (stream id pattern: compliance-{case_id}).

    Governance rules:
    - Every case must have required policy checks before escalation/publication.
    - Tracks PolicyCheckRequested, PolicyRulePassed, PolicyRuleFailed.
    - all_checks_completed() returns True only if at least one check exists
      and no unresolved failures remain.
    """

    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.checked_rules: Set[str] = set()
        self.failed_rules: Set[str] = set()
        self.passed_rules: Set[str] = set()

    def _apply(self, event: Dict) -> None:
        etype = event["event_type"]
        handler = getattr(self, f"_on_{etype}", None)
        if not handler:
            raise DomainError(f"No handler for event type {etype}")
        handler(event["payload"], event["metadata"])

    # --- Event Handlers ---

    def _on_PolicyCheckRequested(self, payload: Dict, metadata: Dict):
        rule_id = payload.get("rule_id")
        if not rule_id:
            raise DomainError("PolicyCheckRequested requires rule_id")
        self.checked_rules.add(rule_id)

    def _on_PolicyRulePassed(self, payload: Dict, metadata: Dict):
        rule_id = payload.get("rule_id")
        if not rule_id:
            raise DomainError("PolicyRulePassed requires rule_id")
        self.passed_rules.add(rule_id)
        # If previously failed, remove from failed
        self.failed_rules.discard(rule_id)

    def _on_PolicyRuleFailed(self, payload: Dict, metadata: Dict):
        rule_id = payload.get("rule_id")
        if not rule_id:
            raise DomainError("PolicyRuleFailed requires rule_id")
        self.failed_rules.add(rule_id)

    # --- Assertions / Queries ---

    def all_checks_completed(self) -> bool:
        """
        Returns True only if:
        - At least one check has been requested
        - No unresolved failures remain
        """
        if not self.checked_rules:
            return False
        if self.failed_rules:
            return False
        return True

    # Convenience loader
    @classmethod
    def load(cls, store, case_id: str) -> "PolicyComplianceRecord":
        return super().load(store, f"compliance-{case_id}")
