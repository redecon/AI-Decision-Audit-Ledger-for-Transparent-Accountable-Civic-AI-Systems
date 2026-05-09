# tests/test_mcp_integration.py

import pytest
import uuid
from fastapi.testclient import TestClient
from src.civic_ledger.api.main import app

client = TestClient(app)


def test_full_case_lifecycle():
    # Generate unique IDs for each run
    agent_id = f"agent-{uuid.uuid4()}"
    session_id = f"sess-{uuid.uuid4()}"
    case_id = f"case-{uuid.uuid4()}"
    rule_id = f"R{uuid.uuid4()}"

    # 1. Start agent session
    resp = client.post("/tools/start_agent_session", json={
        "agent_id": agent_id,
        "session_id": session_id,
        "context_source": "civic-ai",
        "model_version": "v1.2.0"
    })
    assert resp.status_code == 200

    # 2. Submit case
    resp = client.post("/tools/submit_case", json={
        "case_id": case_id,
        "source": "web",
        "description": "Water contamination"
    })
    assert resp.status_code == 200

    # 3. Categorize case
    resp = client.post("/tools/categorize_case", json={
        "case_id": case_id,
        "agent_id": agent_id,
        "session_id": session_id,
        "category": "environmental",
        "confidence_score": 0.9,
        "input_data": {"symptom": "dirty water"},
        "model_version": "v1.2.0"
    })
    assert resp.status_code == 200

    # 4. Request policy check
    resp = client.post("/tools/request_policy_check", json={
        "case_id": case_id,
        "rule_id": rule_id
    })
    assert resp.status_code == 200

    # 5. Record policy check result
    resp = client.post("/tools/record_policy_check_result", json={
        "case_id": case_id,
        "rule_id": rule_id,
        "passed": True
    })
    assert resp.status_code == 200

    # 6. Generate recommendation
    resp = client.post("/tools/generate_recommendation", json={
        "case_id": case_id,
        "recommendation": "ESCALATE",
        "confidence_score": 0.95,
        "supporting_agents": [{"agent_id": agent_id, "session_id": session_id}]
    })
    assert resp.status_code == 200

    # 7. Human review
    resp = client.post("/tools/human_review", json={
        "case_id": case_id,
        "reviewer_id": "hrd-1",
        "decision": "approved"
    })
    assert resp.status_code == 200

    # 8. Escalate case
    resp = client.post("/tools/escalate", json={
        "case_id": case_id,
        "target_authority": "Health Ministry",
        "reason": "urgent"
    })
    assert resp.status_code == 200

    # 9. Verify case summary
    resp = client.get(f"/resources/applications/{case_id}")
    assert resp.status_code == 200
    summary = resp.json()
    assert summary["state"] == "ESCALATED"

    # 10. Verify compliance
    resp = client.get(f"/resources/applications/{case_id}/compliance")
    assert resp.status_code == 200
    compliance = resp.json()
    # policy_checks is a list of check objects; verify the first check passed
    checks = compliance["policy_checks"]
    assert isinstance(checks, list)
    assert len(checks) > 0
    assert checks[0]["status"] == "passed"

    # 11. Verify audit trail
    resp = client.get(f"/resources/applications/{case_id}/audit-trail")
    assert resp.status_code == 200
    events = resp.json()
    # The case stream should contain exactly the 5 case-lifecycle events
    assert len(events) >= 5
    event_types = [ev["event_type"] for ev in events]
    assert "CaseSubmitted" in event_types
    assert "CaseCategorized" in event_types
    assert "RecommendationGenerated" in event_types
    assert "HumanReviewCompleted" in event_types
    assert "CaseEscalated" in event_types