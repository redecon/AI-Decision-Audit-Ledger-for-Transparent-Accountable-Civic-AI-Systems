# src/civic_ledger/api/resources.py

from fastapi import APIRouter, HTTPException,  Query
from typing import Optional
from datetime import datetime
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.db import get_connection
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.domain.aggregates import CaseReportAggregate
from src.civic_ledger.domain.policy_compliance import PolicyComplianceRecord




router = APIRouter(prefix="/resources", tags=["mcp-resources"])

@router.get("/applications/{case_id}")
def get_case_summary(case_id: str):
    # 1. Try projection first
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM case_summary_projection WHERE case_id = %s", (case_id,))
        row = cur.fetchone()
        if row:
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))

    # 2. Fallback: reconstruct from event stream
    store = EventStore()
    events = store.load_stream(f"case-{case_id}")
    if not events:
        raise HTTPException(status_code=404, detail="Case not found")

    # Load aggregate to get the correct state machine interpretation
    case_agg = CaseReportAggregate.load(store, f"case-{case_id}")

    # Build a dictionary with same structure as the projection table
    summary = {
        "case_id": case_id,
        "state": case_agg.state,
        "source": case_agg.source,
        "category": case_agg.category,
        "urgency_level": case_agg.urgency_level,
        "submitted_at": events[0]["recorded_at"].isoformat() if events else None,
        "last_updated_at": events[-1]["recorded_at"].isoformat() if events else None,
        "assigned_authority": getattr(case_agg, "target_authority", None) or "",
        "policy_status": "unknown",
        "decision": case_agg.recommendation,
        "agent_sessions_completed": [],
        "last_event_type": events[-1]["event_type"] if events else None,
        "last_event_at": events[-1]["recorded_at"].isoformat() if events else None,
        "human_reviewer_id": case_agg.reviewer_id,
        "final_decision_at": events[-1]["recorded_at"].isoformat() if events and case_agg.state in ("ESCALATED", "PUBLISHED") else None,
    }
    return summary


@router.get("/applications/{case_id}/compliance")
def get_compliance(case_id: str, as_of: Optional[datetime] = Query(None)):
    # 1. Try projection first (current state only)
    if not as_of:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT policy_checks, last_updated_at FROM compliance_audit_current WHERE case_id = %s",
                (case_id,)
            )
            row = cur.fetchone()
            if row:
                return {
                    "case_id": case_id,
                    "policy_checks": row[0],
                    "last_updated_at": row[1].isoformat()
                }

    # 2. Fallback: reconstruct from compliance event stream
    store = EventStore()
    stream_id = f"compliance-{case_id}"
    events = store.load_stream(stream_id)

    # Temporal filter if requested
    if as_of:
        events = [e for e in events if e["recorded_at"] <= as_of]

    # Rebuild compliance state manually from events
    checks = []
    for event in events:
        payload = event["payload"]
        etype = event["event_type"]
        rule_id = payload.get("rule_id")
        if etype == "PolicyCheckRequested":
            checks.append({
                "rule_id": rule_id,
                "status": "pending",
                "regulation_version": payload.get("regulation_version"),
                "checked_at": event["recorded_at"].isoformat()
            })
        elif etype == "PolicyRulePassed":
            # Update existing or add new
            for check in checks:
                if check["rule_id"] == rule_id:
                    check["status"] = "passed"
                    check["checked_at"] = event["recorded_at"].isoformat()
                    break
            else:
                checks.append({
                    "rule_id": rule_id,
                    "status": "passed",
                    "checked_at": event["recorded_at"].isoformat()
                })
        elif etype == "PolicyRuleFailed":
            for check in checks:
                if check["rule_id"] == rule_id:
                    check["status"] = "failed"
                    check["checked_at"] = event["recorded_at"].isoformat()
                    break
            else:
                checks.append({
                    "rule_id": rule_id,
                    "status": "failed",
                    "checked_at": event["recorded_at"].isoformat()
                })

    return {
        "case_id": case_id,
        "policy_checks": checks,
        "last_updated_at": events[-1]["recorded_at"].isoformat() if events else None
    }

@router.get("/applications/{case_id}/audit-trail")
def get_audit_trail(case_id: str, from_pos: int = 0, to_pos: Optional[int] = None):
    """Return raw event stream for a case, optionally filtered by position range."""
    store = EventStore()
    events = store.load_stream(f"case-{case_id}")
    filtered = [
        e for e in events
        if e["stream_position"] >= from_pos and (to_pos is None or e["stream_position"] <= to_pos)
    ]
    return filtered


@router.get("/agents/{agent_id}/performance")
def get_agent_performance(agent_id: str):
    """Return accountability ledger entries for an agent."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM agent_accountability_ledger WHERE agent_id = %s", (agent_id,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/agents/{agent_id}/sessions/{session_id}")
def get_agent_session(agent_id: str, session_id: str):
    """Return full event stream for an agent session."""
    store = EventStore()
    events = store.load_stream(f"agent-{agent_id}-{session_id}")
    return events


@router.get("/ledger/health")
def health():
    """Return projection checkpoint status for health monitoring."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT projection_name, last_position, updated_at FROM projection_checkpoints")
        rows = cur.fetchall()
        health_data = {
            r[0]: {"last_position": r[1], "updated_at": r[2].isoformat()}
            for r in rows
        }
    return {"status": "ok", "projections": health_data}
