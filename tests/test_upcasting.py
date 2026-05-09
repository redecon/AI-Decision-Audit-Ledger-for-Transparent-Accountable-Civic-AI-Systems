# tests/test_upcasting.py

import pytest
import json
from datetime import datetime, timezone

from src.civic_ledger.db import get_connection
from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.upcasting.registry import UpcasterRegistry
from src.civic_ledger.upcasting.civic_upcasters import register_civic_upcasters


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


def test_upcasting_immutability(fresh_db):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Insert a raw v1 CaseCategorized event directly into DB (missing new fields)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (
                    event_id, stream_id, stream_position,
                    event_type, payload, metadata, recorded_at,
                    integrity_hash, previous_hash, event_version
                )
                VALUES (gen_random_uuid(), %s, %s,
                        %s, %s::jsonb, %s::jsonb, %s,
                        %s, %s, %s)
                """,
                (
                    "case-immutability",
                    1,
                    "CaseCategorized",
                    json.dumps({"category": "noise"}),  # old payload, missing model_version etc.
                    json.dumps({}),
                    now,
                    "dummyhash",
                    None,
                    1,  # event_version = 1
                ),
            )
        conn.commit()

    # Query raw payload directly from DB
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload, event_version FROM events WHERE stream_id = %s",
                ("case-immutability",),
            )
            raw_payload, raw_version = cur.fetchone()
    assert "model_version" not in raw_payload
    assert raw_version == 1

    # Load stream with upcaster registry
    registry = UpcasterRegistry()
    register_civic_upcasters(registry, store)
    events = store.load_stream("case-immutability", upcaster_registry=registry)

    # Returned payload should be upcasted
    upcasted = events[0]["payload"]
    assert upcasted["model_version"] == "legacy-pre-2026"
    assert upcasted["confidence_score"] is None
    assert upcasted["model_provider"] == "UNKNOWN"
    assert events[0]["event_version"] == 2  # incremented

    # Re-query raw DB again to confirm immutability
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload, event_version FROM events WHERE stream_id = %s",
                ("case-immutability",),
            )
            raw_payload2, raw_version2 = cur.fetchone()
    # Raw DB row must remain unchanged
    assert raw_payload2 == raw_payload
    assert raw_version2 == 1


def test_recommendation_upcasting_immutability(fresh_db):
    store = EventStore()
    now = datetime.now(timezone.utc)

    # Insert a raw v1 RecommendationGenerated event directly into DB
    payload_v1 = {
        "recommendation": "ESCALATE",
        "contributing_sessions": [
            {"agent_id": "agentX", "session_id": "sess1"}
        ]
        # Note: no model_versions field
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (
                    event_id, stream_id, stream_position,
                    event_type, payload, metadata, recorded_at,
                    integrity_hash, previous_hash, event_version
                )
                VALUES (gen_random_uuid(), %s, %s,
                        %s, %s::jsonb, %s::jsonb, %s,
                        %s, %s, %s)
                """,
                (
                    "case-reco-immutability",
                    1,
                    "RecommendationGenerated",
                    json.dumps(payload_v1),
                    json.dumps({}),
                    now,
                    "dummyhash",
                    None,
                    1,
                ),
            )
        conn.commit()

    # Query raw payload directly from DB
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload, event_version FROM events WHERE stream_id = %s",
                ("case-reco-immutability",),
            )
            raw_payload, raw_version = cur.fetchone()
    assert "model_versions" not in raw_payload
    assert raw_version == 1

    # Load stream with civic upcasters
    registry = UpcasterRegistry()
    register_civic_upcasters(registry, store)
    events = store.load_stream("case-reco-immutability", upcaster_registry=registry)

    # Returned payload should be upcasted with model_versions
    upcasted = events[0]["payload"]
    assert "model_versions" in upcasted
    assert events[0]["event_version"] == 2

    # Re-query raw DB again to confirm immutability
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload, event_version FROM events WHERE stream_id = %s",
                ("case-reco-immutability",),
            )
            raw_payload2, raw_version2 = cur.fetchone()
    # Raw DB row must remain unchanged
    assert raw_payload2 == raw_payload
    assert raw_version2 == 1
