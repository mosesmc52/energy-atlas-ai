# atlas/agents/router.py (simplified)
from dataclasses import dataclass
from typing import Any, Dict, Optional

from utils.dates import resolve_date_range


@dataclass(frozen=True)
class RouteResult:
    metric: str
    start: str
    end: str
    filters: Optional[Dict[str, Any]] = None


def route_metric(user_query: str) -> str:
    q = user_query.lower()

    if "storage" in q:
        return "working_gas_storage_lower48"

    if "henry hub" in q or "spot price" in q:
        return "henry_hub_spot"

    if "lng" in q and "export" in q:
        return "lng_exports"

    raise ValueError("No route for query")


def route_query(user_query: str) -> RouteResult:
    metric = route_metric(user_query)
    start, end = resolve_date_range(user_query)
    return RouteResult(metric=metric, start=start, end=end)
