# src/civic_ledger/upcasting/civic_upcasters.py

from typing import List, Dict
from src.civic_ledger.upcasting.registry import UpcasterRegistry
from src.civic_ledger.event_store.repository import EventStore

def infer_model_versions_from_sessions(contributing_sessions: List[Dict], store: EventStore) -> Dict[str, str]:
    """
    Infer model_versions from agent session streams.
    For each contributing session, load the agent stream and extract model_version
    from the AgentContextLoaded event. Returns dict {session_id: model_version}.
    """
    versions = {}
    for sess in contributing_sessions:
        agent_id = sess.get("agent_id")
        session_id = sess.get("session_id")
        if not agent_id or not session_id:
            continue
        stream_id = f"agent-{agent_id}-{session_id}"
        events = store.load_stream(stream_id)
        for ev in events:
            if ev["event_type"] == "AgentContextLoaded":
                versions[session_id] = ev["payload"].get("model_version", "UNKNOWN")
                break
    return versions


def register_civic_upcasters(registry: UpcasterRegistry, store: EventStore):
    """
    Register all required civic upcasters into the given registry.
    """

    @registry.register("CaseCategorized", 1)
    def upcast_case_v1(payload: dict) -> dict:
        # Ensure model_version, confidence_score, model_provider fields
        if "model_version" not in payload:
            payload["model_version"] = "legacy-pre-2026"
        if "confidence_score" not in payload:
            payload["confidence_score"] = None
        if "model_provider" not in payload:
            payload["model_provider"] = "UNKNOWN"
        return payload

    @registry.register("RecommendationGenerated", 1)
    def upcast_recommendation_v1(payload: dict) -> dict:
        # If model_versions already present, skip
        if "model_versions" in payload:
            return payload
        sessions = payload.get("contributing_sessions", [])
        if sessions:
            # Heavy cross-stream read; note risk
            payload["model_versions"] = infer_model_versions_from_sessions(sessions, store)
        return payload

    @registry.register("PolicyRulePassed", 1)
    def upcast_policy_passed_v1(payload: dict) -> dict:
        if "regulatory_basis" not in payload:
            payload["regulatory_basis"] = "UNKNOWN"
        return payload
