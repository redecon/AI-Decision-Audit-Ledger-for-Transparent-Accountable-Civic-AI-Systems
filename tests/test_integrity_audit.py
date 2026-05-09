# tests/test_integrity_audit.py

import pytest
import json
from datetime import datetime, timezone

from src.civic_ledger.db import get_connection
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.integrity.audit import run_integrity_check, IntegrityError


@pytest.fixture(scope="function")
def fresh_db():
    """Reset DB state before each test."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM projection_checkpoints")
            cur.execute("DELETE FROM event_streams")
            cur.execute("DELETE FROM outbox")
            cur.execute("DELETE FROM events")
        conn.commit()
    yield


def test_audit_integrity_check_initial_run(fresh_db):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Append two valid case events
    store.append("case-200", "CaseSubmitted", {"source": "citizen"}, expected_last_position=0, recorded_at=now)
    store.append("case-200", "CaseCategorized", {"category": "noise"}, expected_last_position=1, recorded_at=now)

    # Run integrity check
    audit_event = run_integrity_check(store, "case", "200")

    payload = audit_event["payload"]
    assert payload["entity_id"] == "200"
    assert payload["events_verified_count"] == 2
    assert "integrity_hash" in payload
    assert payload["last_included_global_position"] > 0

    # Verify audit stream contains the event
    audit_stream = store.load_stream("audit-case-200")
    assert any(ev["event_type"] == "AuditIntegrityCheckRun" for ev in audit_stream)


def test_audit_integrity_check_incremental_run(fresh_db):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Append initial events
    store.append("case-201", "CaseSubmitted", {"source": "citizen"}, expected_last_position=0, recorded_at=now)
    store.append("case-201", "CaseCategorized", {"category": "noise"}, expected_last_position=1, recorded_at=now)

    # First audit run
    first_audit = run_integrity_check(store, "case", "201")
    first_count = first_audit["payload"]["events_verified_count"]

    # Append more events
    store.append("case-201", "RecommendationGenerated", {"recommendation": "ESCALATE"}, expected_last_position=2, recorded_at=now)
    store.append("case-201", "CaseEscalated", {"target_authority": "municipality"}, expected_last_position=3, recorded_at=now)

    # Second audit run
    second_audit = run_integrity_check(store, "case", "201")
    second_count = second_audit["payload"]["events_verified_count"]

    # Count should have increased by 2
    assert second_count == first_count + 2

    # Integrity hash should differ between runs
    assert second_audit["payload"]["integrity_hash"] != first_audit["payload"]["integrity_hash"]


def test_audit_integrity_check_tamper_detection(fresh_db):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Append a valid event
    store.append("case-202", "CaseSubmitted", {"source": "citizen"}, expected_last_position=0, recorded_at=now)

    # Tamper with payload directly in DB
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE events SET payload = %s WHERE stream_id = %s",
                (json.dumps({"source": "tampered"}), "case-202"),
            )
        conn.commit()

    # Integrity check should raise IntegrityError
    with pytest.raises(IntegrityError):
        run_integrity_check(store, "case", "202")
