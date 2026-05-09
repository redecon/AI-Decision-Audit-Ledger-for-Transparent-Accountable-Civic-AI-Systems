import logging
import json
from typing import Dict, Any
from datetime import datetime, timezone

from src.civic_ledger.projections.base import Projection
from src.civic_ledger.db import get_connection

logger = logging.getLogger(__name__)


class ComplianceAuditProjection(Projection):
    @property
    def name(self) -> str:
        return "compliance_audit"

    def __init__(self, conn):
        super().__init__(conn)
        self._ensure_tables()
        self._last_processed: int = 0
        self._last_event_time: datetime | None = None

    def _ensure_tables(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS compliance_audit_current (
                    case_id TEXT PRIMARY KEY,
                    policy_checks JSONB,
                    last_updated_at TIMESTAMPTZ
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS compliance_audit_snapshots (
                    case_id TEXT NOT NULL,
                    snapshot_position BIGINT NOT NULL,
                    snapshot_data JSONB NOT NULL,
                    snapshot_time TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (case_id, snapshot_position)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS compliance_event_counter (
                    case_id TEXT PRIMARY KEY,
                    event_count INT DEFAULT 0
                )
                """
            )
        self.conn.commit()

    def handle_event(self, event: Dict[str, Any]) -> None:
        etype = event["event_type"]
        payload = event.get("payload", {})
        case_id = event["stream_id"].replace("compliance-", "")
        recorded_at = event.get("recorded_at", datetime.now(timezone.utc))
        gpos = event["global_position"]

        if etype not in ("PolicyCheckRequested", "PolicyRulePassed", "PolicyRuleFailed"):
            return

        with self.conn.cursor() as cur:
            # Load current state
            cur.execute(
                "SELECT policy_checks FROM compliance_audit_current WHERE case_id = %s",
                (case_id,),
            )
            row = cur.fetchone()
            checks = row[0] if row else []

            # Update policy checks
            rule_id = payload.get("rule_id")
            if etype == "PolicyCheckRequested":
                status = "pending"
            elif etype == "PolicyRulePassed":
                status = "passed"
            elif etype == "PolicyRuleFailed":
                status = "failed"
            else:
                status = None

            updated = False
            for check in checks:
                if check.get("rule_id") == rule_id:
                    check["status"] = status
                    check["checked_at"] = recorded_at.isoformat()
                    updated = True
                    break
            if not updated:
                checks.append(
                    {
                        "rule_id": rule_id,
                        "status": status,
                        "regulation_version": payload.get("regulation_version"),
                        "checked_at": recorded_at.isoformat(),
                    }
                )

            # Upsert current state
            cur.execute(
                """
                INSERT INTO compliance_audit_current (case_id, policy_checks, last_updated_at)
                VALUES (%s, %s::jsonb, %s)
                ON CONFLICT (case_id) DO UPDATE SET
                    policy_checks = EXCLUDED.policy_checks,
                    last_updated_at = EXCLUDED.last_updated_at
                """,
                (case_id, json.dumps(checks), recorded_at),
            )

            # Update persistent event counter
            cur.execute(
                """
                INSERT INTO compliance_event_counter (case_id, event_count)
                VALUES (%s, 1)
                ON CONFLICT (case_id) DO UPDATE SET event_count = compliance_event_counter.event_count + 1
                RETURNING event_count
                """,
                (case_id,),
            )
            new_count = cur.fetchone()[0]

            # Snapshot every 10 events
            if new_count % 10 == 0:
                cur.execute(
                    """
                    INSERT INTO compliance_audit_snapshots (
                        case_id, snapshot_position, snapshot_data, snapshot_time
                    )
                    VALUES (%s, %s, %s::jsonb, %s)
                    ON CONFLICT (case_id, snapshot_position) DO NOTHING
                    """,
                    (case_id, gpos, json.dumps(checks), recorded_at),
                )

        self.conn.commit()
        self._last_processed = gpos
        self._last_event_time = recorded_at

    def get_compliance_at(self, case_id: str, timestamp: datetime) -> Dict[str, Any]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT snapshot_position, snapshot_data, snapshot_time
                    FROM compliance_audit_snapshots
                    WHERE case_id = %s AND snapshot_time <= %s
                    ORDER BY snapshot_time DESC
                    LIMIT 1
                    """,
                    (case_id, timestamp),
                )
                snap = cur.fetchone()
                if not snap:
                    return {"case_id": case_id, "policy_checks": []}

                snapshot_position, snapshot_data, snapshot_time = snap

                cur.execute(
                    """
                    SELECT payload, event_type, recorded_at
                    FROM events
                    WHERE stream_id = %s
                      AND global_position > %s
                      AND recorded_at <= %s
                    ORDER BY global_position ASC
                    """,
                    (f"compliance-{case_id}", snapshot_position, timestamp),
                )
                events = cur.fetchall()

        checks = snapshot_data
        for payload, etype, rec_at in events:
            rule_id = payload.get("rule_id")
            if etype == "PolicyCheckRequested":
                status = "pending"
            elif etype == "PolicyRulePassed":
                status = "passed"
            elif etype == "PolicyRuleFailed":
                status = "failed"
            else:
                status = None

            updated = False
            for check in checks:
                if check.get("rule_id") == rule_id:
                    check["status"] = status
                    check["checked_at"] = rec_at.isoformat()
                    updated = True
                    break
            if not updated:
                checks.append(
                    {
                        "rule_id": rule_id,
                        "status": status,
                        "regulation_version": payload.get("regulation_version"),
                        "checked_at": rec_at.isoformat(),
                    }
                )

        return {"case_id": case_id, "policy_checks": checks}
