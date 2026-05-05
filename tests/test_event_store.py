"""
Unit tests for EventStore core functionality.
"""

import pytest
import uuid
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.event_store.exceptions import ConcurrencyError
from src.civic_ledger.db import get_connection


@pytest.fixture(scope="module")
def store():
    return EventStore()


def test_append_and_load_stream(store):
    """Happy path: append events and load them back."""
    stream_id = f"case-{uuid.uuid4()}"
    store.append(stream_id, expected_last_position=0,
                 event_type="CaseSubmitted",
                 payload_dict={"citizen": "Amina", "complaint": "Water shortage"},
                 metadata_dict={"source": "portal"})
    store.append(stream_id, expected_last_position=1,
                 event_type="CaseCategorized",
                 payload_dict={"category": "Infrastructure"},
                 metadata_dict={})
    events = store.load_stream(stream_id)
    assert len(events) == 2
    # Now check dict keys instead of tuple indices
    assert events[0]["event_type"] == "CaseSubmitted"
    assert events[1]["event_type"] == "CaseCategorized"
    assert store.verify_stream_integrity(stream_id) is True


def test_concurrency_conflict(store):
    """Simulate double-decision conflict: wrong expected_last_position."""
    stream_id = f"case-{uuid.uuid4()}"
    store.append(stream_id, expected_last_position=0,
                 event_type="CaseSubmitted",
                 payload_dict={"citizen": "Biniam"},
                 metadata_dict={})
    with pytest.raises(ConcurrencyError):
        store.append(stream_id, expected_last_position=0,  # should be 1
                     event_type="CaseCategorized",
                     payload_dict={"category": "Health"},
                     metadata_dict={})


def test_tampering_detection(store):
    """Manually alter payload to simulate tampering and verify integrity fails."""
    stream_id = f"case-{uuid.uuid4()}"
    store.append(stream_id, expected_last_position=0,
                 event_type="CaseSubmitted",
                 payload_dict={"citizen": "Sara"},
                 metadata_dict={})

    # Tamper with payload directly in DB
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE events SET payload = %s WHERE stream_id=%s AND stream_position=1;",
                ('{"citizen":"Tampered"}', stream_id)
            )
        conn.commit()

    assert store.verify_stream_integrity(stream_id) is False
