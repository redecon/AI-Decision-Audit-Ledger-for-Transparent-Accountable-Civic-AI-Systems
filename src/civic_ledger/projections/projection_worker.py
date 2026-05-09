# src/civic_ledger/projection_worker.py

import asyncio
import logging

from src.civic_ledger.db import get_connection
from src.civic_ledger.event_store import EventStore
from src.civic_ledger.projections.daemon import ProjectionDaemon
from src.civic_ledger.projections.case_summary import CaseSummaryProjection
from src.civic_ledger.projections.agent_accountability import AgentAccountabilityProjection
from src.civic_ledger.projections.compliance_audit import ComplianceAuditProjection

logging.basicConfig(level=logging.INFO)

async def main():
    # Initialize EventStore
    store = EventStore()

    # Initialize projections with a fresh connection
    with get_connection() as conn:
        case_summary = CaseSummaryProjection(conn)
        agent_accountability = AgentAccountabilityProjection(conn)
        compliance_audit = ComplianceAuditProjection(conn)

        # Ensure tables exist
        case_summary._ensure_table()
        agent_accountability._ensure_table()
        compliance_audit._ensure_tables()

    projections = [case_summary, agent_accountability, compliance_audit]

    # Start daemon
    daemon = ProjectionDaemon(store, projections)
    await daemon.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Projection worker stopped.")
