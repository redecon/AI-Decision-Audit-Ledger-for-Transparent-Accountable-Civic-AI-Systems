# src/civic_ledger/domain/agent_memory.py

from typing import Dict, Any, List
from src.civic_ledger.event_store.repository import EventStore


def reconstruct_agent_context(store: EventStore, agent_id: str, session_id: str, token_budget: int = 8000) -> Dict[str, Any]:
    """
    Rebuild full agent state from event history.
    Returns a structured dict representing AgentContext:
    {
        "agent_id": ...,
        "session_id": ...,
        "last_completed_action": str or None,
        "pending_work": list of event summaries,
        "history_summary": str (compressed),
        "last_raw_events": list of the last 3 events verbatim,
        "session_health_status": "OK" or "NEEDS_RECONCILIATION"
    }
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events = store.load_stream(stream_id)

    last_completed_action = None
    pending_work: List[Dict[str, Any]] = []
    session_health_status = "OK"

    # Heuristic for completion vs. pending work
    if events:
        last_event = events[-1]
        etype = last_event["event_type"]
        payload = last_event["payload"]

        if etype == "AgentActionRecorded":
            outcome = payload.get("outcome")
            if outcome == "completed":
                last_completed_action = payload.get("action_type")
            else:
                pending_work.append({"event_type": etype, "payload": payload})
                session_health_status = "NEEDS_RECONCILIATION"

        elif etype == "AgentDecisionRequested":
            # If no subsequent AgentDecisionCompleted, mark as pending
            if not any(ev["event_type"] == "AgentDecisionCompleted" for ev in events):
                pending_work.append({"event_type": etype, "payload": payload})
                session_health_status = "NEEDS_RECONCILIATION"

        elif etype == "AgentDecisionCompleted":
            last_completed_action = "decision"

    # Build compressed history summary (all except last 3 events)
    history_events = events[:-3] if len(events) > 3 else []
    summary_parts = []
    token_count = 0
    for ev in history_events:
        snippet = f"{ev['event_type']}:{str(ev['payload'])[:50]}"
        token_count += len(snippet.split())
        if token_count > token_budget:
            summary_parts.append("...truncated...")
            break
        summary_parts.append(snippet)
    history_summary = " | ".join(summary_parts)

    # Last 3 raw events
    last_raw_events = events[-3:] if len(events) >= 3 else events

    return {
        "agent_id": agent_id,
        "session_id": session_id,
        "last_completed_action": last_completed_action,
        "pending_work": pending_work,
        "history_summary": history_summary,
        "last_raw_events": last_raw_events,
        "session_health_status": session_health_status,
    }
