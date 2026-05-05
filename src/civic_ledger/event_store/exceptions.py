class ConcurrencyError(Exception):
    """Raised when optimistic concurrency control fails in the EventStore."""
    pass

class TamperingError(Exception):
    """Raised when an integrity check detects tampering in the event log."""
    pass

class EventStoreError(Exception):
    """Base class for all EventStore-related errors."""
    pass
