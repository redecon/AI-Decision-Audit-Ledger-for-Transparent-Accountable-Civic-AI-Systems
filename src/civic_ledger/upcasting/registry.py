# src/civic_ledger/upcasting/registry.py

import copy
from typing import Callable, Dict, Tuple

class UpcasterRegistry:
    """
    Registry for event upcasters. Each upcaster transforms an event payload
    from one version to the next. The original stored event is never modified.
    """

    def __init__(self):
        # Maps (event_type, from_version) -> upcaster function
        self._upcasters: Dict[Tuple[str, int], Callable[[dict], dict]] = {}

    def register(self, event_type: str, from_version: int):
        """
        Decorator that registers a function as an upcaster for event_type@from_version.
        Usage:
            @registry.register("CaseCategorized", 1)
            def upcast_case_v1(payload): ...
        """
        def decorator(fn: Callable[[dict], dict]) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        """
        Apply all registered upcasters in deterministic version order.
        Returns a new event dict (deep copy) with transformed payload and incremented version.
        """
        current = copy.deepcopy(event)
        v = current.get("event_version", 1)
        while (current["event_type"], v) in self._upcasters:
            upcaster = self._upcasters[(current["event_type"], v)]
            # Transform payload
            new_payload = upcaster(current["payload"])
            current["payload"] = new_payload
            v += 1
            current["event_version"] = v
        return current
