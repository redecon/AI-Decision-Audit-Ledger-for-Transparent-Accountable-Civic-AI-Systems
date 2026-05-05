"""
tests/test_concurrency.py
Double-Decision Test (Civic Concurrency)

This test proves the EventStore prevents conflicting AI decisions
on the same citizen complaint. In a real Sahel deployment, this
ensures that two different AI agents cannot both classify the
same human rights report differently at the same time.
"""

import threading
import uuid
import pytest
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.event_store.exceptions import ConcurrencyError
from src.civic_ledger.db import get_connection


def ensure_stream(stream_id, aggregate_type="CaseReport"):
    """Helper to ensure a stream row exists in event_streams."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO event_streams (stream_id, aggregate_type, current_version) "
            "VALUES (%s, %s, %s) ON CONFLICT (stream_id) DO NOTHING;",
            (stream_id, aggregate_type, 0)
        )
        conn.commit()


def test_double_decision_conflict():
    store = EventStore()
    stream_id = f"case-test-{uuid.uuid4()}"
    ensure_stream(stream_id)


    # Step 1: Submit the case
    store.append(stream_id, expected_last_position=0,
                 event_type="CaseSubmitted",
                 payload_dict={"citizen": "Amina", "complaint": "Water shortage"},
                 metadata_dict={"source": "portal"})

    # Step 2: Two agents both read version=1 and try to classify
    results = []

    def agent(name):
        try:
            ev = store.append(stream_id, expected_last_position=1,
                              event_type="CaseCategorized",
                              payload_dict={"category": "Infrastructure", "agent": name},
                              metadata_dict={})
            results.append((name, "success", ev))
        except ConcurrencyError as e:
            results.append((name, "conflict", str(e)))

    t1 = threading.Thread(target=agent, args=("agent-1",))
    t2 = threading.Thread(target=agent, args=("agent-2",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    # Step 3: Exactly one succeeds, one fails
    successes = [r for r in results if r[1] == "success"]
    conflicts = [r for r in results if r[1] == "conflict"]
    assert len(successes) == 1
    assert len(conflicts) == 1

    # Step 4: Losing agent retries with correct expected_last_position
    store.append(stream_id, expected_last_position=2,
                 event_type="CaseCategorized",
                 payload_dict={"category": "Infrastructure", "agent": "retry"},
                 metadata_dict={})

    # Step 5: Print final stream events
    events = store.load_stream(stream_id)
    print("\nFinal stream events:")
    for ev in events:
        print(ev)

    # Verify no duplicate classification at same position
    positions = [ev["stream_position"] for ev in events if ev["event_type"] == "CaseCategorized"]
    assert positions == sorted(set(positions)), "No duplicate stream positions"
