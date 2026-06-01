from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from agents.llm_query_parser import LLMQueryParserError, llm_parse_query
from agents.llm_router import (
    DATASET_FILTERS,
    METRICS,
    REGION_FILTERS,
    RESOURCE_CATEGORY_FILTERS,
)
from agents.source_planner import build_source_plan
from utils.dates import resolve_date_range

ALLOWED_METRICS = set(METRICS)
ALLOWED_REGIONS = set(REGION_FILTERS)
ALLOWED_RESOURCE_CATEGORIES = set(RESOURCE_CATEGORY_FILTERS)
ALLOWED_DATASETS = set(DATASET_FILTERS)
ALLOWED_WEATHER_NORMAL_YEARS = {1, 2, 3, 5}


@dataclass(frozen=True)
class HybridRouteResult:
    intent: Literal[
        "single_metric",
        "compare",
        "ranking",
        "derived",
        "explain",
        "ambiguous",
        "unsupported",
    ]
    primary_metric: Optional[str]
    metrics: List[str]
    start: str
    end: str
    filters: Optional[Dict[str, Any]] = None
    confidence: float = 0.0
    ambiguous: bool = False
    candidates: List[Any] = field(default_factory=list)
    source: Literal["rule", "llm"] = "llm"
    reason: Optional[str] = None
    normalized_query: Optional[str] = None
    include_forecast: bool = False
    forecast_horizon_days: Optional[int] = None


@dataclass(frozen=True)
class LLMRouteOutput:
    intent: str
    primary_metric: Optional[str]
    metrics: List[str]
    filters: Optional[Dict[str, Any]]
    reason: Optional[str]
    confidence: float
    ambiguous: bool


def normalize_query(user_query: str) -> str:
    q = user_query.lower().strip()
    q = q.replace("’", "'").replace("–", "-").replace("—", "-")
    q = re.sub(r"\s+", " ", q).strip()
    return q


def detect_forecast_request(q: str) -> bool:
    return bool(
        re.search(r"\b(forecast|predict|projection|outlook)\b", q)
        or re.search(r"\bnext\s+\d+\s+days?\b", q)
        or re.search(r"\bnext\s+(one|two|three|four)\s+weeks?\b", q)
    )


def detect_forecast_horizon_days(q: str) -> Optional[int]:
    if not detect_forecast_request(q):
        return None
    match = re.search(r"\bnext\s+(\d+)\s+days?\b", q)
    if match:
        return int(match.group(1))
    if re.search(r"\b(14|fourteen)\s*day", q) or re.search(r"\b(two|2)\s+weeks?\b", q):
        return 14
    return 7


def llm_route_structured(user_query: str, normalized_query: str) -> LLMRouteOutput:
    try:
        parsed = llm_parse_query(
            user_query=user_query,
            normalized_query=normalized_query,
        )
        plan = build_source_plan(parsed)
        primary_metric = plan.calls[0].metric if plan.calls else None
        metrics = [call.metric for call in plan.calls]
        if plan.intent == "unsupported":
            primary_metric = None
            metrics = []
        return LLMRouteOutput(
            intent=plan.intent,
            primary_metric=primary_metric,
            metrics=metrics,
            filters=plan.calls[0].filters if plan.calls else None,
            reason=plan.reason,
            confidence=parsed.confidence,
            ambiguous=plan.ambiguous,
        )
    except LLMQueryParserError as err:
        return LLMRouteOutput(
            intent="unsupported",
            primary_metric=None,
            metrics=[],
            filters=None,
            reason=f"LLM router error: {err}",
            confidence=0.0,
            ambiguous=False,
        )


def _sanitize_filters(
    *,
    primary_metric: Optional[str],
    filters: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    out = dict(filters or {})

    if "region" in out and out["region"] not in ALLOWED_REGIONS:
        out.pop("region", None)
    if "resource_category" in out:
        if (
            primary_metric != "ng_exploration_reserves_lower48"
            or out["resource_category"] not in ALLOWED_RESOURCE_CATEGORIES
        ):
            out.pop("resource_category", None)
    if "dataset" in out:
        if primary_metric != "ng_pipeline" or out["dataset"] not in ALLOWED_DATASETS:
            out.pop("dataset", None)
    if "normal_years" in out:
        try:
            normal_years = int(out["normal_years"])
        except (TypeError, ValueError):
            out.pop("normal_years", None)
        else:
            if (
                primary_metric == "weather_degree_days_forecast_vs_5y"
                and normal_years in ALLOWED_WEATHER_NORMAL_YEARS
            ):
                out["normal_years"] = normal_years
            else:
                out.pop("normal_years", None)

    return out or None


def validate_llm_route(
    llm: LLMRouteOutput,
    start: str,
    end: str,
    normalized_query: str,
) -> HybridRouteResult:
    metrics = [m for m in llm.metrics if m in ALLOWED_METRICS]

    primary_metric = (
        llm.primary_metric if llm.primary_metric in ALLOWED_METRICS else None
    )
    if primary_metric and primary_metric not in metrics:
        metrics = [primary_metric] + metrics

    intent = (
        llm.intent
        if llm.intent
        in {
            "single_metric",
            "compare",
            "ranking",
            "derived",
            "explain",
            "ambiguous",
            "unsupported",
        }
        else "unsupported"
    )
    if not metrics and primary_metric is None:
        intent = "unsupported"

    return HybridRouteResult(
        intent=intent,
        primary_metric=primary_metric,
        metrics=metrics,
        start=start,
        end=end,
        filters=_sanitize_filters(primary_metric=primary_metric, filters=llm.filters),
        confidence=max(0.0, min(1.0, float(llm.confidence))),
        ambiguous=bool(llm.ambiguous),
        source="llm",
        reason=llm.reason,
        normalized_query=normalized_query,
        include_forecast=detect_forecast_request(normalized_query),
        forecast_horizon_days=detect_forecast_horizon_days(normalized_query),
    )


def route_query(user_query: str) -> HybridRouteResult:
    normalized = normalize_query(user_query)
    start, end = resolve_date_range(user_query)
    llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
    return validate_llm_route(
        llm,
        start=start,
        end=end,
        normalized_query=normalized,
    )
