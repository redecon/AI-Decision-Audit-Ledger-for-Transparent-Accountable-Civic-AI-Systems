# src/civic_ledger/api/errors.py

from fastapi import Request
from fastapi.responses import JSONResponse
from src.civic_ledger.event_store.exceptions import ConcurrencyError
from src.civic_ledger.domain.aggregates import DomainError


async def concurrency_error_handler(request: Request, exc: ConcurrencyError):
    """
    Handle optimistic concurrency errors from EventStore.
    Returns a machine-actionable JSON error contract.
    """
    return JSONResponse(
        status_code=409,
        content={
            "error_type": "OptimisticConcurrencyError",
            "message": str(exc),
            "suggested_action": "reload_stream_and_retry"
        }
    )


async def domain_error_handler(request: Request, exc: DomainError):
    """
    Handle domain rule violations raised by aggregates or command handlers.
    Returns a machine-actionable JSON error contract.
    """
    return JSONResponse(
        status_code=422,
        content={
            "error_type": "DomainRuleViolation",
            "message": str(exc),
            "suggested_action": "Check preconditions and retry"
        }
    )


async def generic_error_handler(request: Request, exc: Exception):
    """
    Catch-all handler for unexpected errors.
    Ensures clients always receive structured JSON instead of raw stack traces.
    """
    return JSONResponse(
        status_code=500,
        content={
            "error_type": "InternalServerError",
            "message": "An unexpected error occurred.",
            "details": str(exc),
            "suggested_action": "Contact system administrator"
        }
    )
