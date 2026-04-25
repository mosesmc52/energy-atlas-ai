from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from agents.metric_capabilities import MetricCapability


@dataclass(frozen=True)
class WindowPolicyDeps:
    get_metric_capability: Callable[[str], MetricCapability]
    wants_seasonal_norm_comparison: Callable[[str], bool]
    route_weather_normal_years: Callable[[str], Optional[int]]
    allowed_weather_normal_years: set[int]


def resolved_normal_years_for_query(
    *,
    metric: str,
    q: str,
    deps: WindowPolicyDeps,
) -> Optional[int]:
    capability = deps.get_metric_capability(metric)
    if not capability.seasonal_norm_supported:
        return None
    if not deps.wants_seasonal_norm_comparison(q):
        return None
    normal_years = deps.route_weather_normal_years(q)
    if normal_years in deps.allowed_weather_normal_years:
        return normal_years
    return capability.default_normal_years


def resolve_metric_lookback_years(
    *,
    metric: str,
    q: str,
    has_explicit_dates: bool,
    current_like_only: bool,
    deps: WindowPolicyDeps,
) -> Optional[int]:
    if has_explicit_dates and not current_like_only:
        return None

    capability = deps.get_metric_capability(metric)
    normal_years = resolved_normal_years_for_query(metric=metric, q=q, deps=deps)
    if normal_years is not None:
        return normal_years
    return capability.default_lookback_years
