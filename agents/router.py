# atlas/agents/router.py (simplified)
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RouteResult:
    metric: str
    start: str
    end: str
    filters: Optional[Dict[str, Any]] = None


def route_query(user_query: str) -> RouteResult:
    q = user_query.lower()
    # you will replace these with better parsing over time
    if "storage" in q:
        return RouteResult("working_gas_storage_lower48", "2024-01-01", "2024-06-01")
    if "henry hub" in q:
        return RouteResult("henry_hub_spot", "2024-01-01", "2024-06-01")
    if "lng" in q and "export" in q:
        return RouteResult("lng_exports", "2024-01-01", "2024-06-01")
    raise ValueError("No route for query")
