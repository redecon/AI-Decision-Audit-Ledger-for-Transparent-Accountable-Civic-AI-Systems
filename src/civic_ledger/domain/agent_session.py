# src/civic_ledger/domain/agent_session.py

from typing import Dict, Optional
from src.civic_ledger.domain.aggregates import Aggregate, DomainError


class AgentSessionAggregate(Aggregate):
    """
    Aggregate for agent sessions (stream id pattern: agent-{agent_id}-{session_id}).

    Enforces Gas Town Pattern:
    - The first event must always be AgentContextLoaded.
    - No other events are allowed until context is loaded.
    - This enforces AI transparency and prevents 'black-box' actions,
      aligning with Code for Africa's anti-black-box rule.
    """

    def __init__(self, stream_id: str):
        super().__init__(stream_id)
        self.context_loaded: bool = False
        self.model_version: Optional[str] = None
        self.context_source: Optional[str] = None
        self.action_count: int = 0
        self.last_action_type: Optional[str] = None
        self.last_case_id: Optional[str] = None

    def _apply(self, event: Dict) -> None:
        etype = event["event_type"]
        handler = getattr(self, f"_on_{etype}", None)
        if not handler:
            raise DomainError(f"No handler for event type {etype}")
        handler(event["payload"], event["metadata"])

    # --- Event Handlers ---

    def _on_AgentContextLoaded(self, payload: Dict, metadata: Dict):
        """
        First event in any agent session.
        Records context source and model version.
        """
        if self.version > 0:
            raise DomainError("AgentContextLoaded must be the first event in a session")
        self.context_source = payload.get("context_source")
        self.model_version = payload.get("model_version")
        self.context_loaded = True

    def _on_AgentActionRecorded(self, payload: Dict, metadata: Dict):
        """
        Records an agent action (categorization, urgency scoring, etc.).
        Only allowed if context has been loaded.
        Enforces CfA transparency: every action is tied to context, model version,
        and outcome, preventing black-box decisions.
        """
        if not self.context_loaded:
            raise DomainError("Agent context not loaded")
        self.action_count += 1
        self.last_action_type = payload.get("action_type")
        self.last_case_id = payload.get("case_id")
        self.last_outcome = payload.get("outcome")
        self.last_timestamp = payload.get("timestamp")
        self.last_model_version = payload.get("model_version")


    # --- Assertions for command handlers ---

    def assert_context_loaded(self):
        """
        Ensure that context has been loaded before any actions.
        Prevents phantom AI actions without declared context.
        """
        if not self.context_loaded:
            raise DomainError("Agent session context not loaded")

    def assert_model_version_current(self, cmd_model_version: str):
        """
        Ensure that the agent session is using the expected model version.
        This enforces AI transparency and prevents silent model drift.
        """
        if self.model_version != cmd_model_version:
            raise DomainError(
                f"Model version mismatch; agent used {self.model_version} "
                f"but command expects {cmd_model_version}"
            )
