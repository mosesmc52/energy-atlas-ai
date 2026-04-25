from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass(frozen=True)
class FilterResolverDeps:
    route_iso: Callable[[str], Optional[str]]
    route_storage_region: Callable[[str], Optional[str]]
    wants_storage_level_and_change: Callable[[str], bool]
    wants_regional_grouping: Callable[[str], bool]
    wants_storage_ranking_by_region: Callable[[str], bool]
    route_export_region: Callable[[str], Optional[str]]
    route_import_region: Callable[[str], Optional[str]]
    route_consumption_state: Callable[[str], Optional[str]]
    route_production_state: Callable[[str], Optional[str]]
    resolve_ng_electricity_normal_years: Callable[[str], Optional[int]]
    route_reserves_state: Callable[[str], Optional[str]]
    route_reserves_resource_category: Callable[[str], Optional[str]]
    route_pipeline_dataset: Callable[[str], Optional[str]]
    route_weather_region: Callable[[str], Optional[str]]
    route_weather_normal_years: Callable[[str], Optional[int]]
    allowed_weather_normal_years: set[int]


def _resolve_iso_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric
    filters: Dict[str, Any] = {}
    iso = deps.route_iso(q)
    if iso:
        filters["iso"] = iso
    elif confidence >= 0.85:
        filters["iso"] = "ercot"
    return filters


def _resolve_storage_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    region = deps.route_storage_region(q)
    if metric == "working_gas_storage_lower48" and deps.wants_storage_level_and_change(q):
        if region:
            filters["region"] = region
        filters["include_weekly_change"] = True
    elif region:
        filters["region"] = region
    elif deps.wants_regional_grouping(q) or deps.wants_storage_ranking_by_region(q):
        filters["group_by"] = "region"
    elif confidence >= 0.85:
        filters["region"] = "lower48"
    return filters


def _resolve_lng_export_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric
    filters: Dict[str, Any] = {}
    region = deps.route_export_region(q)
    if region:
        filters["region"] = region
    elif confidence >= 0.85:
        filters["region"] = "united_states_pipeline_total"
    return filters


def _resolve_lng_import_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric
    filters: Dict[str, Any] = {}
    region = deps.route_import_region(q)
    if region:
        filters["region"] = region
    elif confidence >= 0.85:
        filters["region"] = "united_states_pipeline_total"
    return filters


def _resolve_ng_consumption_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric, confidence
    state = deps.route_consumption_state(q)
    return {"region": state if state else "united_states_total"}


def _resolve_ng_production_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric, confidence
    state = deps.route_production_state(q)
    return {"region": state if state else "united_states_total"}


def _resolve_ng_electricity_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric, confidence
    filters: Dict[str, Any] = {}
    normal_years = deps.resolve_ng_electricity_normal_years(q)
    if normal_years is not None:
        filters["normal_years"] = normal_years
    return filters


def _resolve_ng_supply_balance_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric, q, confidence, deps
    return {"region": "united_states_total"}


def _resolve_ng_reserves_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric, confidence
    filters: Dict[str, Any] = {}
    state = deps.route_reserves_state(q)
    if state:
        filters["region"] = state
    category = deps.route_reserves_resource_category(q)
    if category:
        filters["resource_category"] = category
    return filters


def _resolve_ng_pipeline_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric
    filters: Dict[str, Any] = {}
    dataset = deps.route_pipeline_dataset(q)
    if dataset:
        filters["dataset"] = dataset
    elif confidence >= 0.85:
        filters["dataset"] = "natural_gas_pipeline_projects"
    return filters


def _resolve_weather_degree_day_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric, confidence
    filters: Dict[str, Any] = {}
    region = deps.route_weather_region(q)
    filters["region"] = region if region else "lower48"
    normal_years = deps.route_weather_normal_years(q)
    filters["normal_years"] = (
        normal_years
        if normal_years in deps.allowed_weather_normal_years
        else 5
    )
    return filters


def _resolve_weather_regional_driver_filters(metric: str, q: str, confidence: float, deps: FilterResolverDeps) -> Dict[str, Any]:
    del metric, confidence
    normal_years = deps.route_weather_normal_years(q)
    return {
        "normal_years": (
            normal_years
            if normal_years in deps.allowed_weather_normal_years
            else 5
        )
    }


FILTER_RESOLVERS = {
    "working_gas_storage_lower48": _resolve_storage_filters,
    "working_gas_storage_change_weekly": _resolve_storage_filters,
    "lng_exports": _resolve_lng_export_filters,
    "lng_imports": _resolve_lng_import_filters,
    "ng_consumption_lower48": _resolve_ng_consumption_filters,
    "ng_production_lower48": _resolve_ng_production_filters,
    "ng_electricity": _resolve_ng_electricity_filters,
    "ng_supply_balance_regime": _resolve_ng_supply_balance_filters,
    "ng_exploration_reserves_lower48": _resolve_ng_reserves_filters,
    "ng_pipeline": _resolve_ng_pipeline_filters,
    "weather_degree_days_forecast_vs_5y": _resolve_weather_degree_day_filters,
    "weather_regional_demand_drivers": _resolve_weather_regional_driver_filters,
}


def build_filters(
    metric: str,
    q: str,
    confidence: float,
    deps: FilterResolverDeps,
) -> Optional[Dict[str, Any]]:
    if metric.startswith("iso_"):
        filters = _resolve_iso_filters(metric, q, confidence, deps)
        return filters or None

    resolver = FILTER_RESOLVERS.get(metric)
    if resolver is None:
        return None

    filters = resolver(metric, q, confidence, deps)
    return filters or None
