# Civic Audit Ledger – DESIGN.md
# AI Decision Audit Ledger — Design & Governance Rationale

**For Transparent & Accountable Civic AI Systems**

## 1. Overview

The AI Decision Audit Ledger is an event‑sourced, tamper‑evident platform that transforms black‑box AI decisions into fully auditable, legally defensible records. It is built specifically for public‑interest deployments — human rights monitoring, investigative journalism, anti‑corruption platforms — where a single unverifiable output can destroy trust and cause real‑world harm.

The system guarantees that every AI‑assisted action on a citizen complaint, policy analysis, or human rights report is:

- **Immutable** — events are append‑only, cryptographically chained, and never overwritten.
- **Traceable** — every decision links back to the exact data, model version, and human reviewer involved.
- **Auditable** — temporal queries can reconstruct the system’s state at any point in time, and a hash‑chain integrity check detects any tampering.
- **Reconstructable** — agent state is rebuilt from events, not volatile memory, so crashes never corrupt an investigation.

These properties directly support the work of Code for Africa’s teams — PesaCheck fact‑checkers, source.AFRICA evidence curators, iLAB forensic investigators, and CivicSignal media monitors — by making AI a trustworthy partner in civic accountability rather than an opaque risk.

## 2. System Architecture

### 2.1 Event Sourcing & CQRS

The ledger uses event sourcing: every state change is captured as a domain event (`CaseSubmitted`, `CaseCategorized`, `HumanReviewCompleted`, etc.) and stored in an append‑only `events` table. The current state of any case, agent session, or compliance record is rebuilt by replaying its event stream. This means:

- The full decision history is always available for audit.
- No state can be silently altered or deleted.
- Historical analyses can be reproduced exactly.

Command Query Responsibility Segregation (CQRS) separates the write path (commands that produce events) from the read path (projections that transform events into queryable views). This allows the system to serve fast, tailored dashboards for journalists without compromising the integrity of the source events.

### 2.2 High‑Level Flow
Citizen Report → Command Handler → validate business rules → append event(s) → Event Store
↓
Outbox → Projection Daemon → Projection Tables
↓
MCP Resources (read API) → Dashboards, Auditors


All writes go through command handlers that enforce governance rules before any event is persisted. Reads go through projections or direct event‑stream queries (for temporal audits), never mutating state.

## 3. Database Schema Justification

Every column exists to serve a specific public‑accountability function.

### 3.1 `events`

| Column | Type | Civic Purpose |
|--------|------|---------------|
| `event_id` | UUID | Globally unique identifier for forensic cross‑referencing |
| `stream_id` | TEXT | Maps to `case-{id}`, `agent-{id}-{session}`, `compliance-{id}`, etc. — enables full lifecycle reconstruction per entity |
| `stream_position` | BIGINT | Deterministic ordering of events within a stream; prevents reordering attacks |
| `global_position` | BIGINT (auto‑increment) | Cross‑stream total ordering for system‑wide audit timelines |
| `event_type` | TEXT | Describes the business action (e.g., `CaseEscalated`) — enables projection logic and audit queries |
| `event_version` | SMALLINT | Supports schema evolution via upcasting without mutating stored events |
| `payload` | JSONB | Stores the citizen report, AI output, human decision — the evidentiary record |
| `metadata` | JSONB | Contains `correlation_id`, `causation_id`, `source` (citizen/AI/human), and `model_version` — links events across aggregates and to source documents for PesaCheck and source.AFRICA |
| `recorded_at` | TIMESTAMPTZ | Critical for temporal audits: “what did the system know at time T?” |
| `integrity_hash` | TEXT | SHA‑256 of all event fields; creates a per‑stream hash chain |
| `previous_hash` | TEXT | Links to previous event’s hash; breakage → tampering detected by iLAB |

### 3.2 `event_streams`

| Column | Civic Purpose |
|--------|---------------|
| `stream_id` | Primary identifier for a citizen case, agent session, or compliance record |
| `aggregate_type` | Distinguishes `CaseReport` from `AgentSession`; enables aggregation‑specific logic |
| `current_version` | Optimistic concurrency control — prevents two actors from silently overwriting each other’s decisions |
| `archived_at` | Supports GDPR‑style data retention policies for HRD partners |

### 3.3 `projection_checkpoints`

Enables the projection daemon to resume processing after a crash without replaying all events — critical for deployments on unreliable hardware.

### 3.4 `outbox`

| Column | Civic Purpose |
|--------|---------------|
| `event_id` | References the event that must be published |
| `destination` | Target dashboard, alerting system, or watchdog platform |
| `payload` | The message to deliver |
| `published_at`, `attempts` | Guarantees at‑least‑once delivery; no civic decision is silently lost |

## 4. Governance Rules Enforced

Seven business rules are embedded directly in the domain logic, not in external configuration that could be bypassed.

### Rule 1: Case Lifecycle State Machine

A citizen case must follow a strict sequence: `Submitted → UnderReview → Analyzed → PolicyChecked → PendingDecision → Escalated/Archived → Published/Resolved`. Any invalid transition raises a `DomainError`. This prevents a complaint from being silently closed or published without proper review — a safeguard against automated censorship.

### Rule 2: Agent Context Requirement (Gas Town Pattern)

Every AI agent session must begin with `AgentContextLoaded`, which records the data source and model version. No categorization, scoring, or recommendation can be performed without this context. This enforces the CfA anti‑black‑box rule: every AI action declares what it knew and which model version it used.

### Rule 3: Model Version Transparency

When a command references an agent session, the session’s model version must match the version declared in the command. Mismatch → `DomainError`. This prevents silent model drift that could change decisions without accountability.

### Rule 4: Confidence Floor

If an AI classification has a confidence score below 0.6, the system automatically generates a `RecommendationGenerated` with `recommendation='REVIEW'`, blocking escalation until a human validates the output. Example: a low‑confidence hate‑speech flag will not be auto‑published; it must be manually checked by a PesaCheck analyst.

### Rule 5: Policy Compliance Dependency

A case cannot be escalated or published unless all required policy checks are completed and passed. The `handle_escalate` command loads the `PolicyComplianceRecord` aggregate and calls `all_checks_completed()`. This ensures that no action violates local laws or human rights frameworks — an essential protection for HRD partners in the Sahel.

### Rule 6: Causal Chain Enforcement

The `handle_generate_recommendation` command requires a list of `supporting_agents` and validates that each referenced agent session actually contributed to the case (i.e., has an `AgentActionRecorded` event with that `case_id`). Fabricating a decision chain is impossible.

### Rule 7: Human Override Requirement

Escalation and publication require an approved `HumanReviewCompleted` event. Even if all policy checks pass, the system refuses to proceed without a human signature. This makes AI a decision‑support tool, not an autonomous actor.

## 5. Hash‑Chain Integrity & Cryptographic Audit

### Per‑Event Hash Chain

Every event carries an `integrity_hash` computed as:

SHA‑256(stream_id + version + event_type + payload_json + previous_hash + recorded_at)

The `previous_hash` links to the hash of the preceding event in the same stream. Any modification to a stored event — even a single character — will cause the entire chain to fail verification.

### Cross‑Stream Audit Batches

The `run_integrity_check` function loads all events for a stream since the last check, computes a batch hash over their per‑event hashes, and appends an `AuditIntegrityCheckRun` to the `PublicAuditLedger`. Each run includes:

- `events_verified_count` — total events checked
- `integrity_hash` — chain of batch hashes
- `previous_integrity_hash` — link to previous audit run

This creates a meta‑level audit trail that can be published externally (e.g., on a public bulletin board) so that anyone can independently verify the entire system’s integrity.

### Tamper Detection

If a single event is altered (e.g., by a compromised server), `verify_stream_integrity` returns `False`, and `run_integrity_check` raises an `IntegrityError`. The system refuses to record a new audit run until integrity is restored — making silent tampering impossible.

## 6. Upcasting & Immutable Schema Evolution

Schemas evolve, but historical events must never be rewritten. The `UpcasterRegistry` applies transformations at read time:

1. An event is loaded from the database with its original payload and `event_version`.
2. The registry finds all registered upcasters for that event type and version.
3. Each upcaster transforms the payload in memory and increments the version.
4. The upcasted event is returned to the caller; the stored row is untouched.

**Example: CaseCategorized v1 → v2**

```python
def upcast_case_categorized_v1_to_v2(payload):
    return {
        **payload,
        "model_version": payload.get("model_version") or "legacy-pre-2026",
        "model_provider": payload.get("model_provider") or "UNKNOWN",
    }

```
## 7. Agent Memory Pattern (Gas Town)

AI agents cannot rely on in‑process memory. On restart, `reconstruct_agent_context` replays the full agent session stream and rebuilds:

- The last completed action  
- Any partially completed decision (detected by the absence of a completion event after an `AgentDecisionRequested`)  
- A compressed summary of older events (for token‑efficient context)  

If an incomplete decision is found, `session_health_status` is set to **NEEDS_RECONCILIATION**, and the system refuses to proceed until a human resolves the state.  
This pattern prevents duplicated work, lost decisions, and inconsistent compliance actions after power outages or process crashes — a reality in field deployments.

---

# 8. Projections & Transparency Layer

Three projections transform raw events into queryable views for different audiences.

## 8.1 CaseSummary
A read‑optimized table (`case_summary_projection`) with columns for state, source, category, urgency, assigned authority, policy status, decision, human reviewer, and timestamps.  
This is what dashboards and journalists query. **Lag target: < 500ms.**

## 8.2 AgentAccountabilityLedger
Tracks per‑agent‑model metrics: analyses completed, decisions generated, confidence averages, escalate/review/archive rates, and human override rate.  
Enables bias audits (“is model v2.3 more aggressive than v2.2?”) and model‑drift detection. Queryable by CfA’s iLAB for forensic analysis.

## 8.3 ComplianceAuditView
The most critical projection. It maintains:

- `compliance_audit_current` — the latest policy check results per case.  
- `compliance_audit_snapshots` — periodic snapshots (every N events) with timestamps.  

The `get_compliance_at(case_id, timestamp)` function reconstructs the exact compliance state as of any historical moment by loading the nearest snapshot and replaying subsequent events — essential for regulatory audits and fact‑checking retrospective claims.

All projections expose a `get_lag()` method returning milliseconds since the last processed event, so dashboards can display **“data as of X seconds ago.”**

---

# 9. MCP Governance Interface

The FastAPI server exposes two groups of endpoints:

## Tools (Write Side)
Each tool enforces preconditions, validates inputs with Pydantic, runs the corresponding command handler, and returns a structured `ToolResponse`.  
Preconditions are documented in the API schema (e.g., `start_agent_session` must be called before `categorize_case`).  

Errors are machine‑actionable:

```json
{
  "error_type": "OptimisticConcurrencyError",
  "message": "Stream version mismatch",
  "stream_id": "case-123",
  "expected_version": 3,
  "actual_version": 5,
  "suggested_action": "reload_stream_and_retry"
}
```

This enables both AI agents and human operators to recover gracefully.

## Resources (Read Side)
GET endpoints serve projection data and, when projections are not yet built (e.g., immediately after a write), fall back to reconstructing state directly from the event store.
This ensures that even without the projection daemon running, the API always returns accurate, up‑to‑date information.

# 10. Deployment & Operational Considerations

- **Database:** PostgreSQL 16 with psycopg3. Chosen for its wide availability, low infrastructure cost, and ability to run on modest hardware typical of field offices.

- **Containerization:** docker-compose.yml provided; the system can be deployed with two commands.

- **Offline‑first:** Event writes happen locally; the outbox publisher syncs to remote dashboards when connectivity is available.

- **Configuration:** All connection parameters are read from environment variables; no hardcoded credentials.


| CfA Team | How the Ledger Supports Their Work |
| --- | --- |
| **PesaCheck** | Fact‑checkers can replay an AI classification (e.g., “is this claim false?”) months later, see which model version produced it, which source documents were used, and verify via the hash chain that the record has not been altered. |
| **source.AFRICA** | Evidence document IDs are embedded in event metadata, creating a direct link from AI output to primary sources — making automated claims verifiable against the documentary record. |
| **iLAB** | Forensic investigators can run ``verify_stream_integrity`` on any case stream and detect tampering instantly. The cross‑stream audit chain can be published externally as an additional transparency layer. |
| **CivicSignal** | The outbox pattern delivers events reliably to media monitoring dashboards, enabling real‑time tracking of disinformation trends without risking data loss. |
| **HRD Partners** | The combination of strict state machines, confidence floors, and mandatory human review ensures that no AI output can directly cause harm — every escalation, publication, or public action has a human signature on it. |

