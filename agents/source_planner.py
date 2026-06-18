from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agents.llm_router import STORAGE_METRIC_BY_VALUE_TYPE, SUPPORTED_METRICS


@dataclass(frozen=True)
class SourceCall:
    adapter: str
    metric: str
    filters: dict | None
    calculation: str | None
    start_date: str | None = None
    end_date: str | None = None


@dataclass(frozen=True)
class SourcePlan:
    intent: str
    calls: list[SourceCall]
    comparison: str | None
    time_window: str | None
    requires_multiple_sources: bool
    ambiguous: bool
    reason: str | None


METRIC_TO_ADAPTER = {metric: "eia" for metric in SUPPORTED_METRICS}


def _valid_metric(metric: str) -> bool:
    return metric in METRIC_TO_ADAPTER


def _metrics_from_parsed(parsed: Any) -> list[str]:
    metrics = [m for m in getattr(parsed, "metrics", []) if _valid_metric(m)]
    primary_metric = getattr(parsed, "primary_metric", None)
    if primary_metric and _valid_metric(primary_metric) and primary_metric not in metrics:
        metrics.insert(0, primary_metric)
    if metrics:
        return metrics

    if getattr(parsed, "domain", None) != "storage":
        return []
    metric = STORAGE_METRIC_BY_VALUE_TYPE.get(getattr(parsed, "value_type", "level"))
    return [metric] if metric and _valid_metric(metric) else []


def build_source_plan(parsed: Any) -> SourcePlan:
    metrics = _metrics_from_parsed(parsed)
    analysis_type = getattr(parsed, "analysis_type", "unsupported")
    if getattr(parsed, "domain", None) == "storage" and getattr(parsed, "value_type", "level") == "weekly_change":
        analysis_type = "weekly_change"
        metrics = ["working_gas_storage_change_weekly"]
    comparisons = list(getattr(parsed, "comparisons", []) or [])
    comparison = None if comparisons in ([], ["none"]) else ",".join(comparisons)
    filters = dict(getattr(parsed, "filters", {}) or {})
    start_date = getattr(parsed, "start_date", None)
    end_date = getattr(parsed, "end_date", None)
    time_window = "custom" if start_date or end_date else analysis_type

    calls: list[SourceCall] = []
    for metric in metrics:
        if not _valid_metric(metric) or any(call.metric == metric for call in calls):
            continue
        calls.append(
            SourceCall(
                adapter=METRIC_TO_ADAPTER[metric],
                metric=metric,
                filters=filters,
                calculation=analysis_type,
                start_date=start_date,
                end_date=end_date,
            )
        )

    if not calls:
        return SourcePlan(
            intent="unsupported",
            calls=[],
            comparison=comparison,
            time_window=time_window,
            requires_multiple_sources=False,
            ambiguous=bool(getattr(parsed, "ambiguous", False)),
            reason=getattr(parsed, "reason", None) or "No valid storage metrics were parsed.",
        )

    return SourcePlan(
        intent=analysis_type,
        calls=calls,
        comparison=comparison,
        time_window=time_window,
        requires_multiple_sources=len(calls) > 1,
        ambiguous=bool(getattr(parsed, "ambiguous", False)),
        reason=getattr(parsed, "reason", None),
    )
