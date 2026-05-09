# AI Decision Audit Ledger for Transparent & Accountable Civic AI Systems

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://python.org)

A production‑grade, event‑sourced audit ledger that makes AI‑assisted civic decisions **immutable, traceable, and verifiable**. Built for Code for Africa’s [PesaCheck](https://pesacheck.org/), [source.AFRICA](https://source.africa/), [iLAB](https://www.codeforafrica.org/ilab/), and [CivicSignal](https://civicsignal.africa/) teams to deploy ethical AI in human‑rights monitoring, investigative journalism, and anti‑corruption platforms.

**The Problem**: AI outputs in public‑interest systems are frequently unauditable — black boxes that cannot be verified or trusted in court.  
**The Solution**: An append‑only, cryptographically chained event store that records every decision, every model version, and every human override, giving journalists, auditors, and regulators a complete, tamper‑evident history.

---

## Key Features

- **Immutable Event Store** – Append‑only persistence with per‑event SHA‑256 hash chains; any tampering is detected instantly.
- **Strict Civic Business Rules** – 7 governance rules embedded directly in the domain layer (lifecycle state machine, confidence floor, policy compliance, causal chain enforcement, mandatory human review…).
- **CQRS & Projections** – Fast read models for dashboards, AI‑bias audits, and temporal compliance investigations.
- **Gas Town Agent Memory** – AI agent state is rebuilt from events after crashes, preventing duplicated work and lost decisions.
- **Upcaster Registry** – Evolve event schemas without mutating historical records; missing fields are explicitly marked `"UNKNOWN"`.
- **Cross‑Stream Audit Chain** – Periodic `AuditIntegrityCheckRun` events create a public‑ly verifiable meta‑audit trail.
- **MCP Governance Interface** – FastAPI server exposing all write commands and read resources, with structured machine‑actionable error contracts.

---

## Architecture (high‑level)
Citizen Report → Command Handler (enforce rules) → Event Store (append‑only)
↓
Outbox → Projection Daemon → Projection Tables
↓
FastAPI (MCP Tools / Resources) → Dashboards, Auditors


- **Write path**: commands load aggregates, validate invariants, and append events.
- **Read path**: projections are built asynchronously; the API falls back to direct event‑stream replay when projections are cold.
- **Integrity**: per‑stream hash chains + cross‑stream batch audits provide cryptographic trust.

---

## Tech Stack

- **Python 3.11+** with `psycopg3`
- **PostgreSQL 16** (primary database)
- **FastAPI** (MCP governance server)
- **Docker Compose** for local development
- **Pytest** for all tests

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-org/civic-ledger.git
cd civic-ledger
```
### 2. Start PostgreSQL

**Using Docker Compose:**

```bash
docker-compose up -d
```
Or connect to an existing PostgreSQL instance by copying .env.example to .env and filling in your credentials.

### 3. Install dependencies
```bash
python -m venv venv
source venv/bin/activate   # or .\venv\Scripts\activate on Windows
pip install -e .
```
### 4. Run the database migration
```bash
python scripts/run_migration.py
```
### 5. Run the MCP server (development)
```bash
uvicorn src.civic_ledger.api.main:app --reload
```
The API is now available at http://localhost:8000. Interactive docs are at http://localhost:8000/docs.

## Testing
Run the complete test suite (all phases):

```bash
pytest -v
```
Key test files:
```
Test file ------------------------------What it validates
test_event_store.py---------------------Immutability, concurrency, hash‑chain integrity
test_phase2_civic_rules.py--------------All 7 governance rules
test_projections.py--------------------	Projection correctness and lag
test_upcasting.py-----------------------Immutable schema evolution
test_integrity_audit.py-----------------Cross‑stream audit chain and tamper detection
test_agent_memory.py--------------------Gas Town agent context reconstruction
test_mcp_integration.py-----------------Full end‑to‑end lifecycle via the API
```

## Documentation
**DOMAIN_NOTES.md –** Event sourcing rationale, aggregates, and upcasting.

**DESIGN.md –** Full architectural and governance justification.

License
This project is licensed under the MIT License – see the LICENSE file for details.

