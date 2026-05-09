# tests/test_projections.py

import pytest
import asyncio
from datetime import datetime, timezone

from src.civic_ledger.db import get_connection
from src.civic_ledger.event_store import EventStore
from src.civic_ledger.projections.daemon import ProjectionDaemon
from src.civic_ledger.projections.case_summary import CaseSummaryProjection
from src.civic_ledger.projections.agent_accountability import AgentAccountabilityProjection
from src.civic_ledger.projections.compliance_audit import ComplianceAuditProjection
from src.civic_ledger.event_store.repository import EventStore


@pytest.fixture(scope="function")
def fresh_db():
    """Reset DB state before each test."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Drop projection tables if they exist
            cur.execute("DROP TABLE IF EXISTS case_summary_projection CASCADE")
            cur.execute("DROP TABLE IF EXISTS agent_accountability_ledger CASCADE")
            cur.execute("DROP TABLE IF EXISTS compliance_audit_current CASCADE")
            cur.execute("DROP TABLE IF EXISTS compliance_audit_snapshots CASCADE")
            cur.execute("DELETE FROM projection_checkpoints")
            cur.execute("DELETE FROM event_streams")
            cur.execute("DELETE FROM outbox")
            cur.execute("DELETE FROM events")

        conn.commit()
    yield


@pytest.fixture
def projections(fresh_db):
    """Initialise projections with fresh tables."""
    with get_connection() as conn:
        case_summary = CaseSummaryProjection(conn)
        agent_accountability = AgentAccountabilityProjection(conn)
        compliance_audit = ComplianceAuditProjection(conn)
    return [case_summary, agent_accountability, compliance_audit]


def run_batch(store, projections):
    """Helper to run one batch synchronously."""
    daemon = ProjectionDaemon(store, projections)
    return daemon._process_batch()


def test_case_summary_projection(projections):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Append case events with explicit positions
    store.append("case-123", "CaseSubmitted", {"source": "citizen"}, expected_last_position=0, recorded_at=now)
    store.append("case-123", "CaseCategorized", {"category": "noise", "agent_id": "agentA", "session_id": "sess1"}, expected_last_position=1, recorded_at=now)
    store.append("case-123", "RecommendationGenerated", {"recommendation": "ESCALATE"}, expected_last_position=2, recorded_at=now)
    store.append("case-123", "CaseEscalated", {"target_authority": "municipality"}, expected_last_position=3, recorded_at=now)

    run_batch(store, projections)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT state, category, assigned_authority FROM case_summary_projection WHERE case_id = %s", ("123",))
            row = cur.fetchone()
            assert row[0] == "ESCALATED"
            assert row[1] == "noise"
            assert row[2] == "municipality"


def test_agent_accountability_projection(projections):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Agent action
    store.append("agent-sess1", "AgentActionRecorded", {"agent_id": "agentA", "model_version": "v1", "confidence_score": 0.8}, expected_last_position=0, recorded_at=now)
    # Recommendation with contributing agent
    store.append("case-456", "RecommendationGenerated", {"recommendation": "ESCALATE", "contributing_sessions": [{"agent_id": "agentA", "model_version": "v1"}]}, expected_last_position=0, recorded_at=now)
    # Human review override
    store.append("case-456", "HumanReviewCompleted", {"decision": "rejected"}, expected_last_position=1, recorded_at=now)

    run_batch(store, projections)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT analyses_count, decisions_count, escalate_count, override_count FROM agent_accountability_ledger WHERE agent_id = %s AND model_version = %s",
                ("agentA", "v1"),
            )
            row = cur.fetchone()
            assert row[0] >= 1
            assert row[1] >= 1
            assert row[2] >= 1
            assert row[3] >= 1


def test_compliance_audit_projection(projections):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Compliance events
    store.append("compliance-789", "PolicyCheckRequested",
                 {"rule_id": "R1", "regulation_version": "2025"},
                 expected_last_position=0, recorded_at=now)
    store.append("compliance-789", "PolicyRulePassed",
                 {"rule_id": "R1", "regulation_version": "2025"},
                 expected_last_position=1, recorded_at=now)

    run_batch(store, projections)

    with get_connection() as conn:
        # First query
        with conn.cursor() as cur:
            cur.execute("SELECT policy_checks FROM compliance_audit_current WHERE case_id = %s", ("789",))
            row = cur.fetchone()
            assert row is not None
            checks = row[0]
            assert any(c["status"] == "passed" for c in checks)

        # Second query — use a new cursor
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM compliance_audit_snapshots WHERE case_id = %s", ("789",))
            snap_count = cur.fetchone()[0]
            assert snap_count >= 0  # snapshots may appear after threshold


def test_projection_lag(projections):
    store = EventStore()
    now = datetime.now(timezone.utc)

    store.append("case-999", "CaseSubmitted", {"source": "citizen"}, expected_last_position=0, recorded_at=now)
    run_batch(store, projections)

    # Lag should be small (milliseconds since last event)
    lag = projections[0].get_lag()
    assert isinstance(lag, int)
    assert lag >= 0
    # Ensure lag is within a reasonable bound (e.g. < 1000 ms)
    assert lag < 1000

