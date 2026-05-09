# Civic Audit Ledger – DESIGN.md

## Overview
The Civic Audit Ledger is an event‑sourced accountability system built with Python and PostgreSQL. It records every AI and human decision in civic complaint handling, ensuring **immutability, traceability, and auditability**. The ledger exists to prevent black‑box AI actions, enforce governance rules, and provide tamper‑evident audit trails for journalists, fact‑checkers, and civic oversight teams.

---

## Schema Justification

### `events`
- **event_id** – unique identifier for each event; ensures reconstructability.
- **stream_id** – links events to a case or aggregate (e.g., `case-123`); allows lifecycle replay.
- **event_type** – describes the action (CaseSubmitted, HumanReviewCompleted, etc.).
- **payload** – JSON body with civic‑relevant details (citizen complaint, reviewer decision).
- **metadata**:
  - **correlation_id** – groups related events (e.g., all actions tied to one complaint).
  - **causation_id** – identifies the triggering event, enabling causal chain reconstruction.
  - **source** – records whether the event came from a citizen, AI agent, or human reviewer.
  - **model_version** – ensures transparency; PesaCheck can trace fact‑checks back to the exact AI model used.
- **integrity_hash / previous_hash** – cryptographic chain for tamper‑evidence.

### `event_streams`
- **stream_id** – identifies the aggregate stream (case, agent session).
- **archived_at** – supports GDPR‑style retention policies for HRD partners.
- **global_position** – cross‑stream ordering; CivicSignal can build timelines of civic events.

### `projection_checkpoints`
- **projection_name** – identifies a read model (e.g., CaseStatusProjection).
- **last_position** – ensures projections are consistent and can resume after downtime.

### `outbox`
- **id, stream_id, event_type, payload** – guarantees reliable delivery of events to dashboards.
- Prevents silent loss of civic decisions; ensures transparency in public reporting.

---

## Hash‑Chain Integrity
Each event stores:
- **integrity_hash** – SHA‑256 of the event payload + metadata.
- **previous_hash** – links to the prior event in the stream.

Together they form a **cryptographic chain**:
- Any tampering breaks the chain, making it detectable.
- Crucial for iLAB forensic investigations: investigators can prove that no event was altered after recording.

**Example:**  
“A journalist can verify that an AI’s categorisation of a complaint has not been altered since the fact‑check was published, by checking the hash chain.”

---

## Governance Rules

1. **Case lifecycle state machine**  
   - Enforced by `CaseReportAggregate`.  
   - Prevents invalid transitions (e.g., publishing before review).  

2. **Agent context must be loaded (Gas Town pattern)**  
   - `AgentSessionAggregate.assert_context_loaded()` ensures no AI action occurs without context.  

3. **Model version transparency**  
   - Command handlers reject mismatched versions.  
   - Guarantees reproducibility of AI outputs.  

4. **Confidence floor**  
   - If confidence < 0.6, system auto‑flags for human review.  
   - Prevents low‑confidence AI decisions from being published unchecked.  

5. **Policy compliance dependency**  
   - `PolicyComplianceRecord.all_checks_completed()` must be true before escalation/publish.  
   - Blocks premature actions.  

6. **Causal chain enforcement**  
   - `handle_generate_recommendation` verifies each supporting agent session actually contributed to the case (`last_case_id == case_id`).  
   - Prevents fabricated causal chains.  

7. **Human override requirement**  
   - Escalation/publication require a prior `HumanReviewCompleted` with decision = approved.  
   - Enforced by `CaseReportAggregate.assert_review_approved()`.  

**Example:**  
“CfA’s iLAB can replay the exact sequence of agent decisions that led to a disputed report being escalated to authorities.”

---

## Deployment Considerations
- **PostgreSQL** – chosen for reliability and accessibility in low‑resource civic environments.  
- **Docker** – containerized deployment for reproducibility.  
- **Offline‑first design** – projections and checkpoints allow local replay even without continuous connectivity.  

---

## CfA Alignment
- **PesaCheck** – can trace AI fact‑checks back to model version and source document.  
- **source.AFRICA** – metadata fields (correlation_id, causation_id) link fact‑checks to original evidence.  
- **iLAB** – hash‑chain integrity ensures forensic tamper‑evidence.  
- **CivicSignal** – global_position enables timeline reconstruction for media monitoring.  

---

  
