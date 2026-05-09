# src/civic_ledger/integrity/audit.py

import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from src.civic_ledger.event_store.repository import EventStore
from src.civic_ledger.db import get_connection


class IntegrityError(Exception):
    """Raised when per-event hash chain verification fails."""


def _compute_batch_hash(previous_integrity_hash: str, event_hashes: list[str]) -> str:
    """
    Compute SHA256 over previous_integrity_hash + concatenated event_hashes.
    """
    concat = previous_integrity_hash + "".join(event_hashes)
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


def run_integrity_check(store: EventStore, entity_type: str, entity_id: str) -> Dict[str, Any]:
    """
    Run a cryptographic integrity check over all events in the given entity's stream.
    If per-event hash chain verification fails, raise IntegrityError.
    Otherwise append an AuditIntegrityCheckRun event to the audit stream.
    Returns the new audit event dict.
    """
    stream_id = f"{entity_type}-{entity_id}"
    audit_stream_id = f"audit-{entity_type}-{entity_id}"

    # First verify per-event hash chain integrity
    if not store.verify_stream_integrity(stream_id):
        raise IntegrityError(f"Stream integrity compromised for {stream_id}")

    # Load audit history
    audit_events = store.load_stream(audit_stream_id)
    last_audit: Optional[Dict[str, Any]] = None
    if audit_events:
        last_audit = audit_events[-1]

    prev_count = 0
    prev_hash = ""
    last_included_global_position = 0
    if last_audit:
        payload = last_audit["payload"]
        prev_count = payload.get("events_verified_count", 0)
        prev_hash = payload.get("integrity_hash", "")
        last_included_global_position = payload.get("last_included_global_position", 0)

    # Collect new events since last audit
    all_events = store.load_stream(stream_id)
    new_events = [ev for ev in all_events if ev["global_position"] > last_included_global_position]

    event_hashes = [ev["integrity_hash"] for ev in new_events]
    new_hash = _compute_batch_hash(prev_hash, event_hashes)

    new_count = prev_count + len(new_events)
    max_global_position = max([ev["global_position"] for ev in new_events], default=last_included_global_position)

    # Build payload for AuditIntegrityCheckRun
    payload = {
        "entity_id": entity_id,
        "events_verified_count": new_count,
        "integrity_hash": new_hash,
        "last_included_global_position": max_global_position,
        "previous_integrity_hash": prev_hash,
    }

    # Determine expected position for audit stream
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_version FROM event_streams WHERE stream_id=%s;", (audit_stream_id,))
            row = cur.fetchone()
            if row is None:
                expected_pos = 0
            else:
                expected_pos = row[0]  # ✅ must equal current_version, not current_version+1

    # Append audit event
    audit_event = store.append(
        stream_id=audit_stream_id,
        event_type="AuditIntegrityCheckRun",
        payload_dict=payload,
        expected_last_position=expected_pos,
        metadata_dict={"checked_at": datetime.now(timezone.utc).isoformat()},
    )

    return audit_event
