"""
migrate.py
Database migration script for Civic Audit Ledger.
Creates core tables if they do not exist.
"""

from src.civic_ledger.db import get_connection

def run_migration():
    ddl_statements = [

        # Events table: immutable audit log of civic decisions
        """
        CREATE TABLE IF NOT EXISTS events (
            event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            stream_id TEXT NOT NULL,
            stream_position BIGINT NOT NULL,
            global_position BIGINT GENERATED ALWAYS AS IDENTITY,
            event_type TEXT NOT NULL,
            event_version SMALLINT NOT NULL DEFAULT 1,
            payload JSONB NOT NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            integrity_hash TEXT,
            previous_hash TEXT,
            CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
        );
        COMMENT ON TABLE events IS 'Immutable civic audit log: every decision/event is chained with hashes for tamper-evidence.';
        """,

        # Event streams: track aggregate boundaries and current version
        """
        CREATE TABLE IF NOT EXISTS event_streams (
            stream_id TEXT PRIMARY KEY,
            aggregate_type TEXT NOT NULL,
            current_version BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            archived_at TIMESTAMPTZ,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        COMMENT ON TABLE event_streams IS 'Tracks each aggregate stream (e.g. CaseReport) and its current version for concurrency control.';
        """,

        # Projection checkpoints: track progress of read models
        """
        CREATE TABLE IF NOT EXISTS projection_checkpoints (
            projection_name TEXT PRIMARY KEY,
            last_position BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        COMMENT ON TABLE projection_checkpoints IS 'Keeps track of how far each projection has processed the event log, ensuring reproducible dashboards.';
        """,

        # Outbox: reliable event publishing to external systems
        """
        CREATE TABLE IF NOT EXISTS outbox (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            event_id UUID NOT NULL REFERENCES events(event_id),
            destination TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            published_at TIMESTAMPTZ,
            attempts SMALLINT NOT NULL DEFAULT 0
        );
        COMMENT ON TABLE outbox IS 'Outbox pattern: ensures civic events are reliably published to external systems (e.g. public audit ledger).';
        """
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for ddl in ddl_statements:
                cur.execute(ddl)
        conn.commit()
    print("Migration completed successfully.")

if __name__ == "__main__":
    run_migration()
