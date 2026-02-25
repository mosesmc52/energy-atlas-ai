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
        return "ng_working_gas_storage_lower48"

    if "henry hub" in q or "spot price" in q:
        return "ng_henry_hub_spot"

    if "lng" in q and "export" in q:
        return "ng_lng_exports"

    if "lng" in q and "import" in q:
        return "ng_lng_imports"

    if "consumption" in q:
        return "ng_consumption_lower48"

    if "electricity" in q:
        return "ng_electricity"

    if "production" in q:
        return "ng_production_lower48"

    if "exploration" in q or "reserves" in q:
        return "ng_exploration_reserves_lower48"

    raise ValueError("No route for query")


def route_query(user_query: str) -> RouteResult:
    metric = route_metric(user_query)
    start, end = resolve_date_range(user_query)
    return RouteResult(metric=metric, start=start, end=end)
