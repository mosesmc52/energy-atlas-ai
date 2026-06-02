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

TIME_SERIES_INFERENCE_TERMS = (
    "how has",
    "changed since",
    "over the last",
    "since",
    "from",
    "trend",
    "history",
    "historical",
    "over time",
)

NORMAL_RANKING_TERMS = (
    "below normal",
    "above normal",
    "deficit",
    "surplus",
    "tight",
    "tightest",
    "loose",
    "loosest",
    "storage deficit",
    "storage surplus",
    "inventory deficit",
    "inventory surplus",
)

RANKING_INTENT_TERMS = (
    "which region",
    "rank",
    "ranking",
    "by region",
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
    ranking_basis: str
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
) -> tuple[str, str, list[str], str, Optional[str], Optional[str]]:
    analysis_type = current_analysis_type
    resolved_value_type = value_type
    resolved_comparisons = list(comparisons or ["none"])
    ranking_basis = "current_storage"
    resolved_chart_type: Optional[str] = None
    resolved_output_mode: Optional[str] = None

    has_normal_deviation = _has_term(normalized_query, NORMAL_DEVIATION_TERMS)
    has_normal_ranking = _has_term(normalized_query, NORMAL_RANKING_TERMS)
    has_ranking_intent = _has_term(normalized_query, RANKING_INTENT_TERMS)
    has_weekly_change = _has_term(normalized_query, WEEKLY_CHANGE_TERMS)
    has_change_direction = _has_term(normalized_query, CHANGE_DIRECTION_TERMS)
    has_time_series_inference = _has_term(normalized_query, TIME_SERIES_INFERENCE_TERMS)

    if analysis_type == "ranking" and has_normal_ranking and has_ranking_intent:
        resolved_comparisons = _append_comparison(resolved_comparisons, "five_year_avg")
        ranking_basis = "deviation_from_normal"
        resolved_chart_type = "bar"
        resolved_output_mode = "chart_and_answer"
        return (
            analysis_type,
            resolved_value_type,
            resolved_comparisons,
            ranking_basis,
            resolved_chart_type,
            resolved_output_mode,
        )

    if analysis_type == "ranking":
        return (
            analysis_type,
            resolved_value_type,
            resolved_comparisons,
            ranking_basis,
            resolved_chart_type,
            resolved_output_mode,
        )

    if has_weekly_change and has_change_direction:
        analysis_type = "weekly_change"
        resolved_value_type = "weekly_change"
        resolved_comparisons = _append_comparison(resolved_comparisons, "prior_week")
        return (
            analysis_type,
            resolved_value_type,
            resolved_comparisons,
            ranking_basis,
            resolved_chart_type,
            resolved_output_mode,
        )

    if has_normal_deviation:
        analysis_type = "deviation_from_normal"
        if has_weekly_change:
            resolved_value_type = "weekly_change"
        resolved_comparisons = _append_comparison(resolved_comparisons, "five_year_avg")

    if has_normal_ranking and has_ranking_intent:
        analysis_type = "ranking"
        ranking_basis = "deviation_from_normal"
        resolved_comparisons = _append_comparison(resolved_comparisons, "five_year_avg")
        resolved_chart_type = "bar"
        resolved_output_mode = "chart_and_answer"

    if has_time_series_inference and not has_weekly_change:
        resolved_value_type = "level"

    if (
        resolved_value_type == "level"
        and has_time_series_inference
        and analysis_type in {"latest", "time_series", "unsupported"}
    ):
        analysis_type = "time_series"
        resolved_chart_type = "line"
        resolved_output_mode = "chart_and_answer"

    return (
        analysis_type,
        resolved_value_type,
        resolved_comparisons,
        ranking_basis,
        resolved_chart_type,
        resolved_output_mode,
    )


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
    chart_type = parsed.chart_type
    output_mode = parsed.output_mode
    ranking_basis = "current_storage"
    if parsed.domain == "storage":
        (
            analysis_type,
            value_type,
            comparisons,
            ranking_basis,
            inferred_chart_type,
            inferred_output_mode,
        ) = infer_storage_analysis_type_from_text(
            normalized_query=normalized,
            current_analysis_type=analysis_type,
            value_type=value_type,
            comparisons=comparisons,
        )
        if inferred_chart_type:
            chart_type = inferred_chart_type
        if inferred_output_mode:
            output_mode = inferred_output_mode
        if analysis_type in {"ranking", "regional_compare"} and (
            not regions
            or regions == ["lower48"]
            or regions == list(STORAGE_REGIONS[:1])
        ):
            if _has_term(normalized, RANKING_INTENT_TERMS):
                regions = list(STORAGE_REGIONS)
        if analysis_type in {"ranking", "regional_compare"} and chart_type == "bar":
            output_mode = "chart_and_answer"

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
        ranking_basis=ranking_basis,
        chart_type=chart_type,
        output_mode=output_mode,
        filters=_filters_for_route(parsed.domain, regions),
        confidence=parsed.confidence,
        ambiguous=parsed.ambiguous,
        reason=parsed.reason,
        normalized_query=normalized,
    )
