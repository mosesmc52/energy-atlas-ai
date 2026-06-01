from __future__ import annotations

from typing import Final

ACTIVE_DOMAINS: Final[tuple[str, ...]] = ("storage", "unsupported")
FUTURE_DOMAINS: Final[tuple[str, ...]] = (
    "price",
    "production",
    "lng",
    "imports_exports",
    "consumption",
    "weather",
    "power",
)

STORAGE_ANALYSIS_TYPES: Final[tuple[str, ...]] = (
    "latest",
    "time_series",
    "regional_compare",
    "seasonal_compare",
    "weekly_change",
    "deviation_from_normal",
    "ranking",
    "explain",
    "unsupported",
)

STORAGE_REGIONS: Final[tuple[str, ...]] = (
    "lower48",
    "east",
    "midwest",
    "mountain",
    "pacific",
    "south_central",
    "south_central_salt",
    "south_central_nonsalt",
)

VALUE_TYPES: Final[tuple[str, ...]] = ("level", "weekly_change")

COMPARISONS: Final[tuple[str, ...]] = (
    "none",
    "prior_week",
    "last_year",
    "five_year_avg",
    "five_year_range",
    "seasonal_normal",
)

CHART_TYPES: Final[tuple[str, ...]] = (
    "none",
    "line",
    "bar",
    "seasonal_line",
    "table",
)

OUTPUT_MODES: Final[tuple[str, ...]] = (
    "answer",
    "chart",
    "chart_and_answer",
)

STORAGE_METRIC_BY_VALUE_TYPE: Final[dict[str, str]] = {
    "level": "working_gas_storage_lower48",
    "weekly_change": "working_gas_storage_change_weekly",
}

SUPPORTED_METRICS: Final[tuple[str, ...]] = tuple(STORAGE_METRIC_BY_VALUE_TYPE.values())
