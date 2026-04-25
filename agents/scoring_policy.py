from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class ScoreAdjustmentDeps:
    route_consumption_state: Callable[[str], Optional[str]]
    route_production_state: Callable[[str], Optional[str]]
    route_reserves_state: Callable[[str], Optional[str]]
    route_reserves_resource_category: Callable[[str], Optional[str]]
    route_import_region: Callable[[str], Optional[str]]
    route_export_region: Callable[[str], Optional[str]]


def apply_metric_score_adjustments(
    *,
    metric: str,
    q: str,
    score: float,
    deps: ScoreAdjustmentDeps,
) -> float:
    # penalize known overlaps a bit
    if metric == "iso_load" and "gas" in q and "demand" in q:
        score -= 1.0
    if metric == "ng_consumption_lower48" and "texas" in q:
        score -= 0.5
    if metric == "ng_consumption_by_sector" and "most" in q:
        score += 0.75
    if metric == "ng_electricity" and "power burn" in q:
        score += 2.0
    if metric == "ng_electricity" and "seasonal" in q and any(
        token in q for token in ("power", "electricity", "burn")
    ):
        score += 1.5
    if metric == "iso_gas_dependency" and any(
        phrase in q
        for phrase in (
            "percentage of electricity generation",
            "percent of electricity generation",
            "electricity generation from natural gas",
            "share of electricity from natural gas",
        )
    ):
        score += 2.5
    if metric == "iso_gas_dependency" and "renewables" in q and any(
        token in q for token in ("gas demand", "power sector", "electricity")
    ):
        score += 2.0
    if metric == "iso_gas_dependency" and all(
        token in q for token in ("renewables", "power sector", "demand")
    ):
        score += 2.5
    if metric == "ng_consumption_lower48" and deps.route_consumption_state(q) and any(
        token in q for token in ("consumption", "usage")
    ):
        score += 1.5
    if metric == "ng_production_lower48" and deps.route_production_state(q) and any(
        token in q for token in ("production", "output", "supply")
    ):
        score += 1.5
    if metric == "ng_supply_balance_regime" and "supply" in q and any(
        token in q
        for token in ("tight", "tightening", "expand", "expanding", "loosen", "loosening")
    ):
        score += 2.0
    if metric == "ng_supply_balance_regime" and any(
        token in q for token in ("market balance", "fundamentals")
    ):
        score += 1.0
    if metric == "weather_degree_days_forecast_vs_5y" and "weather" in q and any(
        token in q
        for token in (
            "forecast",
            "demand",
            "normal",
            "seasonal",
            "bullish",
            "bearish",
            "region",
        )
    ):
        score += 2.0
    if metric == "weather_degree_days_forecast_vs_5y" and any(
        phrase in q
        for phrase in (
            "power burn",
            "electricity generation",
            "power sector",
        )
    ):
        score -= 2.0
    if metric == "weather_regional_demand_drivers" and "weather" in q and any(
        token in q for token in ("region", "regions", "driving", "driver")
    ):
        score += 2.5
    if metric == "ng_exploration_reserves_lower48" and (
        deps.route_reserves_state(q) or deps.route_reserves_resource_category(q)
    ):
        score += 1.5
    if metric == "lng_imports" and deps.route_import_region(q) and "import" in q:
        score += 1.5
    if metric == "lng_exports" and deps.route_export_region(q) and "export" in q:
        score += 1.5
    if metric == "ng_electricity" and "share" in q:
        score -= 0.75
    if metric == "ng_consumption_by_sector" and "renewables" in q and "power sector" in q:
        score -= 2.0
    if metric == "ng_consumption_by_sector" and all(
        token in q for token in ("renewables", "power sector", "demand")
    ):
        score -= 2.0
    if metric == "working_gas_storage_change_weekly" and any(
        term in q for term in ("build", "injection", "withdrawal", "storage injection")
    ):
        score += 1.5
    if metric == "working_gas_storage_lower48" and any(
        term in q for term in ("build", "injection", "withdrawal", "storage injection")
    ):
        score -= 1.0
    if metric == "iso_fuel_mix" and "consumption" in q:
        score -= 1.0
    if metric == "lng_exports" and any(
        term in q
        for term in (
            "pipeline projects",
            "pipeline capacity",
            "state to state capacity",
            "inflow",
            "outflow",
            "major pipeline",
        )
    ):
        score -= 1.25
    if metric == "lng_imports" and any(
        term in q
        for term in ("pipeline capacity", "inflow", "outflow", "major pipeline")
    ):
        score -= 1.0
    if metric == "des_gas_production_index" and "consumption" in q:
        score -= 2.0
    if metric == "open_interest" and "interest rate" in q:
        score -= 1.5
    if metric.startswith("des_") and "survey" not in q and "index" not in q and "dallas fed" not in q:
        if metric not in {
            "des_breakeven_oil_us",
            "des_breakeven_gas_us",
            "des_wti_price_expectation_1y",
            "des_hh_price_expectation_1y",
        }:
            score -= 0.5
    if metric.startswith("des_") and "demand" in q and "iso" in q:
        score -= 1.0

    return score
