# tests/conftest.py
import pytest
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.db import get_connection

@pytest.fixture
def event_store():
    store = EventStore()  # no args
    # Clean slate before each test
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE events, event_streams, projection_checkpoints, outbox CASCADE;")
        conn.commit()
    return store
