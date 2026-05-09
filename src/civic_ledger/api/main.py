# src/civic_ledger/api/main.py

from fastapi import FastAPI
from src.civic_ledger.api.tools import router as tools_router
from src.civic_ledger.api.resources import router as resources_router
from src.civic_ledger.api.errors import concurrency_error_handler, domain_error_handler
from src.civic_ledger.event_store.exceptions import ConcurrencyError
from src.civic_ledger.domain.aggregates import DomainError

app = FastAPI(title="Civic Ledger MCP Server", version="1.0.0")

app.include_router(tools_router)
app.include_router(resources_router)

app.add_exception_handler(ConcurrencyError, concurrency_error_handler)
app.add_exception_handler(DomainError, domain_error_handler)


@app.get("/")
def root():
    return {"status": "operational"}
