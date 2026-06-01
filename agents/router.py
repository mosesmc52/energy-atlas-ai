from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from agents.llm_query_parser import llm_parse_query
from agents.llm_router import STORAGE_METRIC_BY_VALUE_TYPE, STORAGE_REGIONS
from utils.dates import resolve_date_range

NORMAL_DEVIATION_TERMS = (
    "deficit",
    "surplus",
    "above normal",
    "below normal",
    "above average",
    "below average",
    "tighter",
    "tight",
    "loose",
    "looser",
    "tightening",
    "loosening",
    "vs normal",
    "versus normal",
    "compared to normal",
    "storage gap",
    "inventory gap",
)

WEEKLY_CHANGE_TERMS = (
    "injection",
    "injections",
    "withdrawal",
    "withdrawals",
    "build",
    "builds",
    "draw",
    "draws",
    "weekly change",
    "net change",
)

CHANGE_DIRECTION_TERMS = (
    "accelerating",
    "acceleration",
    "slowing",
    "decelerating",
    "shrinking",
    "widening",
    "growing",
    "increasing",
    "decreasing",
    "improving",
    "worsening",
)


@dataclass(frozen=True)
class EnergyRouteResult:
    domain: str
    analysis_type: str
    primary_metric: Optional[str]
    metrics: list[str]
    regions: list[str]
    start_date: Optional[str]
    end_date: Optional[str]
    date_expression: Optional[str]
    value_type: str
    comparisons: list[str]
    chart_type: str
    output_mode: str
    filters: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    ambiguous: bool = False
    reason: Optional[str] = None
    normalized_query: Optional[str] = None


def normalize_query(user_query: str) -> str:
    q = user_query.lower().strip()
    q = q.replace("’", "'").replace("–", "-").replace("—", "-")
    q = re.sub(r"\s+", " ", q).strip()
    return q


def _has_term(normalized_query: str, terms: tuple[str, ...]) -> bool:
    return any(term in normalized_query for term in terms)


def _append_comparison(comparisons: list[str], comparison: str) -> list[str]:
    cleaned = [comp for comp in comparisons if comp and comp != "none"]
    if comparison == "five_year_avg":
        cleaned = [comp for comp in cleaned if comp != "seasonal_normal"]
    if comparison not in cleaned:
        cleaned.append(comparison)
    return cleaned or ["none"]


def infer_storage_analysis_type_from_text(
    normalized_query: str,
    current_analysis_type: str,
    value_type: str,
    comparisons: list[str],
) -> tuple[str, str, list[str]]:
    analysis_type = current_analysis_type
    resolved_value_type = value_type
    resolved_comparisons = list(comparisons or ["none"])

    has_normal_deviation = _has_term(normalized_query, NORMAL_DEVIATION_TERMS)
    has_weekly_change = _has_term(normalized_query, WEEKLY_CHANGE_TERMS)
    has_change_direction = _has_term(normalized_query, CHANGE_DIRECTION_TERMS)

    if analysis_type == "ranking":
        return analysis_type, resolved_value_type, resolved_comparisons

    if has_weekly_change and has_change_direction:
        analysis_type = "weekly_change"
        resolved_value_type = "weekly_change"
        resolved_comparisons = _append_comparison(resolved_comparisons, "prior_week")
        return analysis_type, resolved_value_type, resolved_comparisons

    if has_normal_deviation:
        analysis_type = "deviation_from_normal"
        if has_weekly_change:
            resolved_value_type = "weekly_change"
        resolved_comparisons = _append_comparison(resolved_comparisons, "five_year_avg")

    return analysis_type, resolved_value_type, resolved_comparisons


def _metrics_for_route(domain: str, value_type: str) -> tuple[Optional[str], list[str]]:
    if domain != "storage":
        return None, []
    metric = STORAGE_METRIC_BY_VALUE_TYPE.get(value_type)
    return metric, [metric] if metric else []


def _filters_for_route(domain: str, regions: list[str]) -> dict[str, Any]:
    if domain != "storage":
        return {}
    return {
        "regions": regions,
    }


def route_query(user_query: str) -> EnergyRouteResult:
    normalized = normalize_query(user_query)
    start_date, end_date = resolve_date_range(user_query)
    parsed = llm_parse_query(user_query=user_query, normalized_query=normalized)

    regions = list(parsed.regions or [])
    if parsed.domain == "storage" and not regions:
        regions = ["lower48"]
    if parsed.domain == "storage":
        regions = [region for region in regions if region in STORAGE_REGIONS] or ["lower48"]

    analysis_type = parsed.analysis_type
    value_type = parsed.value_type
    comparisons = list(parsed.comparisons or ["none"])
    if parsed.domain == "storage":
        analysis_type, value_type, comparisons = infer_storage_analysis_type_from_text(
            normalized_query=normalized,
            current_analysis_type=analysis_type,
            value_type=value_type,
            comparisons=comparisons,
        )

    primary_metric, metrics = _metrics_for_route(parsed.domain, value_type)

    return EnergyRouteResult(
        domain=parsed.domain,
        analysis_type=analysis_type,
        primary_metric=primary_metric,
        metrics=metrics,
        regions=regions,
        start_date=start_date,
        end_date=end_date,
        date_expression=parsed.date_expression,
        value_type=value_type,
        comparisons=comparisons,
        chart_type=parsed.chart_type,
        output_mode=parsed.output_mode,
        filters=_filters_for_route(parsed.domain, regions),
        confidence=parsed.confidence,
        ambiguous=parsed.ambiguous,
        reason=parsed.reason,
        normalized_query=normalized,
    )
