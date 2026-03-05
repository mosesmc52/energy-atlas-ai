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


# atlas/agents/router.py
ISO_KEYWORDS = {
    "ercot": ["ercot", "texas"],
    "pjm": ["pjm", "mid-atlantic", "pennsylvania", "new jersey", "maryland", "dc"],
    "isone": [
        "isone",
        "iso-ne",
        "new england",
        "massachusetts",
        "connecticut",
        "maine",
        "nh",
        "vermont",
        "rhode island",
    ],
    "nyiso": ["nyiso", "new york iso", "new york"],
    "caiso": ["caiso", "california iso", "california"],
}


def route_iso(q: str) -> str | None:
    q = q.lower()
    for iso, keys in ISO_KEYWORDS.items():
        if contains_any(keys, q):
            return iso
    return None


ROUTE_MAP = {
    # --- GridStatus (power market) ---
    "iso_gas_dependency": [
        "gas share",
        "gas dependency",
        "grid gas",
        "gas burn",
        "gas-fired",
        "fuel mix",
        "generation mix",
        "power mix",
        "dispatch",
        "how much gas generation",
    ],
    "iso_fuel_mix": [
        "fuel mix",
        "generation mix",
        "power mix",
        "by fuel",
        "gas wind solar",
    ],
    "iso_load": [
        "load",
        "demand",
        "electric demand",
        "power demand",
        "system demand",
    ],
    # --- EIA (your existing) ---
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
    "ng_consumption_lower48": [
        "consumption",
        "consumes",
        "usage",
    ],  # keep "demand" out; demand is ambiguous w/ load
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

    filters: Dict[str, Any] = {}

    # Only attach ISO for grid metrics
    if metric.startswith("iso_"):
        iso = route_iso(user_query) or "ercot"  # v1 default (sensible)
        filters["iso"] = iso

    return RouteResult(metric=metric, start=start, end=end, filters=filters or None)
