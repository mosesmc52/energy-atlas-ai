from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from agents.llm_query_parser import LLMQueryParseOutput
from agents.llm_router import METRICS


@dataclass(frozen=True)
class SourceCall:
    adapter: str
    metric: str
    filters: dict | None
    calculation: str | None


@dataclass(frozen=True)
class SourcePlan:
    intent: str
    calls: list[SourceCall]
    comparison: str | None
    time_window: str | None
    requires_multiple_sources: bool
    ambiguous: bool
    reason: str | None


METRIC_TO_ADAPTER = {
    "working_gas_storage_lower48": "eia",
    "working_gas_storage_change_weekly": "eia",
    "henry_hub_spot": "eia",
    "lng_exports": "eia",
    "lng_imports": "eia",
    "ng_consumption_lower48": "eia",
    "ng_consumption_by_sector": "eia",
    "ng_electricity": "eia",
    "ng_production_lower48": "eia",
    "ng_exploration_reserves_lower48": "eia",
    "ng_pipeline": "pipeline",
    "weather_degree_days_forecast_vs_5y": "weather",
    "weekly_energy_atlas_summary": "derived",
    "iso_load": "gridstatus",
    "iso_fuel_mix": "gridstatus",
    "iso_gas_dependency": "gridstatus",
    "iso_renewables": "gridstatus",
    "managed_money_long": "cftc",
    "managed_money_short": "cftc",
    "managed_money_net": "cftc",
    "managed_money_net_percentile_156w": "cftc",
    "open_interest": "cftc",
}

for _metric in METRICS:
    if _metric.startswith("des_"):
        METRIC_TO_ADAPTER[_metric] = "dallas_fed"


def _valid_metric(metric: str) -> bool:
    return metric in METRIC_TO_ADAPTER


def _append_call(calls: list[SourceCall], *, metric: str, filters: dict | None, calculation: str | None) -> None:
    if not _valid_metric(metric):
        return
    if any(call.metric == metric for call in calls):
        return
    calls.append(
        SourceCall(
            adapter=METRIC_TO_ADAPTER[metric],
            metric=metric,
            filters=filters,
            calculation=calculation,
        )
    )


def build_source_plan(parsed: LLMQueryParseOutput) -> SourcePlan:
    metrics = [m for m in parsed.metrics if _valid_metric(m)]
    if parsed.primary_metric and _valid_metric(parsed.primary_metric) and parsed.primary_metric not in metrics:
        metrics.insert(0, parsed.primary_metric)

    calls: list[SourceCall] = []
    for metric in metrics:
        _append_call(
            calls,
            metric=metric,
            filters=dict(parsed.filters or {}),
            calculation=parsed.calculation,
        )

    topics = {t.lower().strip() for t in (parsed.question_topics or []) if t and t.strip()}

    # Multi-source expansion rules
    if "ng_electricity" in metrics and parsed.intent in {"derived", "explain"}:
        _append_call(calls, metric="iso_load", filters=dict(parsed.filters or {}), calculation="summary")
        _append_call(calls, metric="weather_degree_days_forecast_vs_5y", filters=dict(parsed.filters or {}), calculation="summary")

    if "weekly_energy_atlas_summary" in metrics:
        for metric in (
            "weather_degree_days_forecast_vs_5y",
            "working_gas_storage_change_weekly",
            "lng_exports",
            "ng_production_lower48",
            "henry_hub_spot",
        ):
            _append_call(calls, metric=metric, filters=dict(parsed.filters or {}), calculation="summary")

    if "price" in topics and "storage" in topics:
        _append_call(calls, metric="henry_hub_spot", filters=dict(parsed.filters or {}), calculation="change")
        _append_call(calls, metric="working_gas_storage_lower48", filters=dict(parsed.filters or {}), calculation="change")

    if "supply" in topics:
        _append_call(calls, metric="ng_production_lower48", filters=dict(parsed.filters or {}), calculation="summary")
        _append_call(calls, metric="lng_imports", filters=dict(parsed.filters or {}), calculation="summary")
        _append_call(calls, metric="ng_pipeline", filters=dict(parsed.filters or {}), calculation="summary")

    if not calls:
        return SourcePlan(
            intent="unsupported",
            calls=[],
            comparison=parsed.comparison,
            time_window=parsed.time_window,
            requires_multiple_sources=False,
            ambiguous=parsed.ambiguous or parsed.confidence < 0.55,
            reason=parsed.reason or "No valid metrics were parsed.",
        )

    return SourcePlan(
        intent=parsed.intent,
        calls=calls,
        comparison=parsed.comparison,
        time_window=parsed.time_window,
        requires_multiple_sources=parsed.requires_multiple_sources or len(calls) > 1,
        ambiguous=parsed.ambiguous or parsed.confidence < 0.55,
        reason=parsed.reason,
    )
