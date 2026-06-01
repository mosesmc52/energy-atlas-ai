from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from agents.llm_query_parser import llm_parse_query
from agents.llm_router import STORAGE_METRIC_BY_VALUE_TYPE, STORAGE_REGIONS
from utils.dates import resolve_date_range


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

    primary_metric, metrics = _metrics_for_route(parsed.domain, parsed.value_type)

    return EnergyRouteResult(
        domain=parsed.domain,
        analysis_type=parsed.analysis_type,
        primary_metric=primary_metric,
        metrics=metrics,
        regions=regions,
        start_date=start_date,
        end_date=end_date,
        date_expression=parsed.date_expression,
        value_type=parsed.value_type,
        comparisons=list(parsed.comparisons or ["none"]),
        chart_type=parsed.chart_type,
        output_mode=parsed.output_mode,
        filters=_filters_for_route(parsed.domain, regions),
        confidence=parsed.confidence,
        ambiguous=parsed.ambiguous,
        reason=parsed.reason,
        normalized_query=normalized,
    )
