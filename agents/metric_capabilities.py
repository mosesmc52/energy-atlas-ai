from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MetricCapability:
    metric: str
    fallback_metric: Optional[str] = None
    fallback_note: Optional[str] = None
    seasonal_norm_supported: bool = False
    default_normal_years: int = 5
    default_lookback_years: Optional[int] = None


_REGISTRY: dict[str, MetricCapability] = {
    "iso_gas_dependency": MetricCapability(
        metric="iso_gas_dependency",
        fallback_metric="ng_electricity",
        fallback_note=(
            "Direct ISO gas-share data unavailable; using natural gas power-burn trend as proxy."
        ),
    ),
    "ng_electricity": MetricCapability(
        metric="ng_electricity",
        fallback_metric="ng_consumption_by_sector",
        fallback_note=(
            "Direct ng_electricity observations unavailable; using power-sector rows from "
            "consumption-by-sector as a proxy."
        ),
        seasonal_norm_supported=True,
        default_normal_years=5,
        default_lookback_years=2,
    ),
    "ng_consumption_lower48": MetricCapability(
        metric="ng_consumption_lower48",
        default_lookback_years=2,
    ),
    "ng_consumption_by_sector": MetricCapability(
        metric="ng_consumption_by_sector",
        default_lookback_years=2,
    ),
}


def get_metric_capability(metric: str) -> MetricCapability:
    return _REGISTRY.get(metric, MetricCapability(metric=metric))
