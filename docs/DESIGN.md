# Civic Ledger Database Schema Design

This document describes the schema for the Civic Ledger database, focusing on civic accountability, auditability, and transparency. It covers four core tables: `events`, `event_streams`, `projection_checkpoints`, and `outbox`.

---

## 1. `events` Table

### SQL DDL
```sql
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
```

### Column Justification

**event_id –** Immutable identifier for each civic decision; ensures uniqueness for forensic audits.

**stream_id –** Maps to a citizen case ID, enabling full lifecycle reconstruction of a complaint from submission to publication.

**stream_position –** Sequential ordering within a case stream; prevents duplicate or conflicting decisions.

**global_position –** Provides cross‑stream ordering, useful for reconstructing timelines (e.g., CivicSignal media monitoring).

**event_type –** Captures the nature of the civic action (CaseSubmitted, CaseCategorized, HumanReviewCompleted).

**event_version –** Supports schema evolution and upcasting, ensuring older events remain interpretable.

**payload –** Stores the substantive content (citizen complaint, AI classification, human review notes).

**metadata –** Includes correlation_id (link events across aggregates), causation_id (trace triggers), source (citizen, AI agent, reviewer), and model_version (AI provenance). Enables PesaCheck to trace fact‑check decisions back to the exact model and dataset used.

**recorded_at –** Immutable timestamp for accountability; critical for reconstructing decision timelines.

**integrity_hash and previous_hash –** Create a cryptographic chain that makes tampering detectable, essential for iLAB forensic investigations.

## 2. event_streams Table
### SQL DDL
```sql
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    current_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);
```
### Column Justification

**stream_id –** Identifies the aggregate boundary (e.g., CaseReport, PolicyComplianceRecord).

**aggregate_type –** Defines the type of civic entity; supports separation of concerns in accountability.

**current_version –** Tracks latest position for concurrency control; prevents double‑decisions.

**created_at –** Records when the case stream was initiated.

**archived_at –** Supports GDPR‑style data retention policies for HRD partners.

**metadata –** Allows attaching contextual information (e.g., jurisdiction, partner organization).

## 3. projection_checkpoints Table
### SQL DDL
```sql
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_position BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```
#### Column Justification
**projection_name –** Identifies dashboards or read models (e.g., “PublicAuditLedger”).

**last_position –** Ensures reproducible dashboards by tracking how far each projection has processed the event log.

**updated_at –** Provides transparency on when dashboards were last refreshed.

## 4. outbox Table
### SQL DDL
```sql
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES events(event_id),
    destination TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts SMALLINT NOT NULL DEFAULT 0
);
```
#### Column Justification
**id –** Unique identifier for outbox entry.

**event_id –** Links back to the original civic event for traceability.

**destination –** Specifies external system (e.g., public dashboard, partner API).

**payload –** Contains the event summary for reliable publishing.

**created_at –** Timestamp of outbox entry creation.

**published_at –** Records when the event was successfully delivered.

**attempts –** Tracks retries, ensuring no silent loss of civic decisions.

## Governance Contract: Immutability, Traceability, Auditability
Together, these tables enforce the governance contract:

**Immutability –** Events are append‑only, chained with hashes, making tampering detectable.

**Traceability –** Metadata fields (correlation_id, causation_id, source, model_version) allow investigators to trace decisions back to their origin.

**Auditability –** Projection checkpoints and outbox guarantee reproducible dashboards and reliable delivery.

**Reconstructability –** Stream IDs and positions enable full lifecycle reconstruction of citizen complaints.

Placeholder: Add CfA‑specific context here (e.g., how PesaCheck, source.AFRICA, and iLAB use these features in practice).