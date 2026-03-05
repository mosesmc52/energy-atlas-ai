# atlas/agents/router.py (simplified)
from dataclasses import dataclass
from typing import Any, Dict, Optional

from utils.dates import resolve_date_range
from utils.helpers import contains_any


@dataclass(frozen=True)
class RouteResult:
    metric: str
    start: str
    end: str
    filters: Optional[Dict[str, Any]] = None


ROUTE_MAP = {
    "working_gas_storage_lower48": [
        "storage",
        "inventory",
        "working gas",
        "injection",
        "withdrawal",
    ],
    "henry_hub_spot": ["henry hub", "spot price", "gas price", "benchmark price"],
    "lng_exports": ["lng exports", "export lng", "liquefied natural gas export"],
    "lng_imports": ["lng imports", "import lng", "liquefied natural gas import"],
    "ng_consumption_lower48": ["consumption", "demand", "consumes", "usage"],
    "ng_electricity": ["electricity", "power plants", "power generation"],
    "ng_production_lower48": ["production", "output", "supply", "dry gas production"],
    "ng_exploration_reserves_lower48": ["exploration", "reserves", "proved reserves"],
}


def route_metric(user_query: str) -> str:
    q = user_query.lower()

    for metric, keywords in ROUTE_MAP.items():
        if contains_any(keywords, q):
            return metric

    raise ValueError("No route for query")


def route_query(user_query: str) -> RouteResult:
    metric = route_metric(user_query)
    start, end = resolve_date_range(user_query)
    return RouteResult(metric=metric, start=start, end=end)
