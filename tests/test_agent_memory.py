# tests/test_agent_memory.py

import pytest
from datetime import datetime, timezone

from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.domain.agent_memory import reconstruct_agent_context


@pytest.fixture(scope="function")
def fresh_db():
    """Reset DB state before each test."""
    from src.civic_ledger.db import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM projection_checkpoints")
            cur.execute("DELETE FROM event_streams")
            cur.execute("DELETE FROM outbox")
            cur.execute("DELETE FROM events")
        conn.commit()
    yield


def test_agent_context_reconciliation(fresh_db):
    store = EventStore()
    now = datetime.now(timezone.utc)

    stream_id = "agent-A1-S1"

    # Append AgentContextLoaded
    store.append(stream_id, "AgentContextLoaded",
                 {"context_source": "init", "model_version": "v1"},
                 expected_last_position=0, recorded_at=now)

    # Append AgentActionRecorded (completed)
    store.append(stream_id, "AgentActionRecorded",
                 {"action_type": "categorize", "case_id": "C1", "outcome": "completed"},
                 expected_last_position=1, recorded_at=now)

    # Append AgentDecisionRequested (partial, no completion yet)
    store.append(stream_id, "AgentDecisionRequested",
                 {"decision_type": "recommendation", "case_id": "C1"},
                 expected_last_position=2, recorded_at=now)

    # Reconstruct context after partial decision
    ctx = reconstruct_agent_context(store, "A1", "S1")
    assert ctx["session_health_status"] == "NEEDS_RECONCILIATION"
    assert any(ev["event_type"] == "AgentDecisionRequested" for ev in ctx["pending_work"])

    # Append AgentDecisionCompleted
    store.append(stream_id, "AgentDecisionCompleted",
                 {"decision_type": "recommendation", "case_id": "C1"},
                 expected_last_position=3, recorded_at=now)

    # Reconstruct context again after completion
    ctx2 = reconstruct_agent_context(store, "A1", "S1")
    assert ctx2["session_health_status"] == "OK"
    assert ctx2["pending_work"] == []
    assert ctx2["last_completed_action"] == "decision"
