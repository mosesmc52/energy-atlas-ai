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

STORAGE_REGION_KEYWORDS = {
    "lower48": ["lower48", "lower 48"],
    "east": ["east", "eastern"],
    "midwest": ["midwest", "mid-west"],
    "south_central": ["south_central", "south central"],
    "mountain": ["mountain"],
    "pacific": ["pacific", "west coast"],
}

TRADE_REGION_KEYWORDS = {
    "united_states_pipeline_total": [
        "us total",
        "u.s. total",
        "united states total",
        "total pipeline",
    ],
    "canada_pipeline": ["canada_pipeline", "canada pipeline", "canadian pipeline"],
    "mexico_pipeline": ["mexico_pipeline", "mexico pipeline"],
}


def route_iso(q: str) -> str | None:
    q = q.lower()
    for iso, keys in ISO_KEYWORDS.items():
        if contains_any(keys, q):
            return iso
    return None


def route_storage_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in STORAGE_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_trade_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in TRADE_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
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
    "iso_renewables": [
        "renewables",
        "renewable generation",
        "renewable share",
        "wind and solar",
        "wind solar",
        "solar and wind",
        "wind generation",
        "solar generation",
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
    "working_gas_storage_change_weekly": [
        "storage change",
        "weekly storage change",
        "week over week storage",
        "storage wow",
        "net injection",
        "net withdrawal",
        "change in storage",
    ],
    "working_gas_storage_lower48": [
        "storage",
        "inventory",
        "working gas",
        "injection",
        "withdrawal",
    ],
    "henry_hub_spot": ["henry hub", "spot price", "gas price", "benchmark price"],
    "lng_exports": [
        "lng exports",
        "export lng",
        "liquefied natural gas export",
        "gas exports",
        "pipeline exports",
    ],
    "lng_imports": [
        "lng imports",
        "import lng",
        "liquefied natural gas import",
        "gas imports",
        "pipeline imports",
    ],
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
    elif metric in {"working_gas_storage_lower48", "working_gas_storage_change_weekly"}:
        filters["region"] = route_storage_region(user_query) or "lower48"
    elif metric in {"lng_exports", "lng_imports"}:
        filters["region"] = (
            route_trade_region(user_query) or "united_states_pipeline_total"
        )

    return RouteResult(metric=metric, start=start, end=end, filters=filters or None)
