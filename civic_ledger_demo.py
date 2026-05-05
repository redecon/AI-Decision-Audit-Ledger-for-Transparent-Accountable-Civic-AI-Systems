import sqlite3
import hashlib
import json
from datetime import datetime, timezone

class ConcurrencyError(Exception):
    """Raised when optimistic concurrency check fails."""
    pass

def create_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS events (
        stream_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        previous_hash TEXT,
        current_hash TEXT NOT NULL,
        PRIMARY KEY (stream_id, version)
    )
    """)
    conn.commit()

def compute_hash(stream_id, version, event_type, payload_json, previous_hash, timestamp):
    """
    Civic audit hash chain:
    SHA-256 of concatenation:
    stream_id:version:event_type:payload_json:previous_hash:timestamp
    """
    data = f"{stream_id}:{version}:{event_type}:{payload_json}:{previous_hash}:{timestamp}"
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

def append_event(conn, stream_id, expected_last_version, event_type, payload_dict):
    conn.execute("BEGIN IMMEDIATE")  # acquire write lock instantly
    try:
        cur = conn.cursor()
        cur.execute("SELECT version, current_hash FROM events WHERE stream_id=? ORDER BY version DESC LIMIT 1", (stream_id,))
        row = cur.fetchone()

        latest_version = row[0] if row else 0
        previous_hash = row[1] if row else ""

        # expected_last_version is what caller saw
        if expected_last_version != latest_version:
            raise ConcurrencyError(f"Expected last version {expected_last_version}, but latest is {latest_version}")

        new_version = latest_version + 1
        timestamp = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload_dict, sort_keys=True)
        current_hash = compute_hash(stream_id, new_version, event_type, payload_json, previous_hash, timestamp)

        cur.execute("""
            INSERT INTO events (stream_id, version, event_type, payload, timestamp, previous_hash, current_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (stream_id, new_version, event_type, payload_json, timestamp, previous_hash, current_hash))

        conn.commit()
        return {
            "stream_id": stream_id,
            "version": new_version,
            "event_type": event_type,
            "payload": payload_json,
            "timestamp": timestamp,
            "previous_hash": previous_hash,
            "current_hash": current_hash
        }
    except:
        conn.rollback()
        raise

def verify_integrity(conn, stream_id):
    """
    Recalculate the hash chain from scratch.
    Returns True only if all stored hashes match recomputed values.
    """
    cur = conn.cursor()
    cur.execute("SELECT version, event_type, payload, timestamp, previous_hash, current_hash FROM events WHERE stream_id=? ORDER BY version ASC", (stream_id,))
    rows = cur.fetchall()

    prev_hash = ""
    for version, event_type, payload, timestamp, stored_prev, stored_current in rows:
        recomputed = compute_hash(stream_id, version, event_type, payload, prev_hash, timestamp)
        if recomputed != stored_current or stored_prev != prev_hash:
            return False
        prev_hash = stored_current
    return True

def get_event_history(conn, stream_id):
    """Return all events for a stream in order, suitable for auditor review."""
    cur = conn.execute(
        "SELECT version, event_type, payload, timestamp FROM events WHERE stream_id=? ORDER BY version ASC",
        (stream_id,)
    )
    return cur.fetchall()

# ---------------- DEMO ----------------
if __name__ == "__main__":
    conn = sqlite3.connect(":memory:")
    create_table(conn)

    # Append events
    append_event(conn, "case-001", expected_last_version=0, event_type="CaseSubmitted", payload_dict={
        "case_id": "case-001",
        "source": "web",
        "description": "Garbage not collected for 2 weeks in neighbourhood X",
        "location": "Bamako, Commune IV",
        "submitted_at": datetime.now(timezone.utc).isoformat()
    })

    append_event(conn, "case-001", expected_last_version=1, event_type="CaseCategorized", payload_dict={
        "case_id": "case-001",
        "category": "service_failure",
        "confidence_score": 0.92,
        "model_version": "v1.2.0",
        "model_provider": "civic-ai"
    })

    append_event(conn, "case-001", expected_last_version=2, event_type="HumanReviewCompleted", payload_dict={
        "case_id": "case-001",
        "reviewer_id": "hrd_mali_01",
        "final_decision": "Valid",
        "override_reason": None
    })

    print("Integrity check (should be True):", verify_integrity(conn, "case-001"))

    # Print audit history
    for v, etype, payload, ts in get_event_history(conn, "case-001"):
        print(f"v{v} {etype} {payload} at {ts}")

    # Tamper demo
    conn.execute("UPDATE events SET payload='{\"category\":\"Tampered\"}' WHERE stream_id='case-001' AND version=2")
    conn.commit()  # <-- ensure transaction closes
    print("Integrity check after tampering (should be False):", verify_integrity(conn, "case-001"))

    # Concurrency demo
    try:
        append_event(conn, "case-001", expected_last_version=3, event_type="CaseEscalated", payload_dict={"reason": "Duplicate"})
    except ConcurrencyError as e:
        print("Caught concurrency error:", e)

    # Concurrency demo
    try:
        append_event(conn, "case-001", expected_last_version=3, event_type="CaseEscalated", payload_dict={"reason": "Duplicate"})
    except ConcurrencyError as e:
        print("Caught concurrency error:", e)
