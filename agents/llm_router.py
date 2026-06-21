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

UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS: Final[tuple[str, ...]] = (
    "lower48",
    "east",
    "midwest",
    "mountain",
    "pacific",
    "south_central",
)

STORAGE_DATASETS: Final[tuple[str, ...]] = (
    "weekly_working_gas",
    "underground_storage_all_operators",
    "underground_storage_by_type",
    "lng_storage",
)

STORAGE_FREQUENCIES: Final[tuple[str, ...]] = (
    "weekly",
    "monthly",
    "annual",
)

STORAGE_METRIC_TYPES: Final[tuple[str, ...]] = (
    "working_gas",
    "base_gas",
    "total_gas",
    "lng_storage_additions",
    "lng_storage_withdrawals",
    "lng_storage_net_withdrawals",
    "total_capacity",
    "working_gas_capacity",
    "storage_field_count",
    "net_withdrawals",
    "injections",
    "withdrawals",
    "working_gas_yoy_volume_change",
    "working_gas_yoy_pct_change",
)

STORAGE_TYPES: Final[tuple[str, ...]] = (
    "salt_cavern",
    "depleted_field",
    "aquifer",
)

UNDERGROUND_STORAGE_STATES: Final[tuple[str, ...]] = (
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "de",
    "fl",
    "ga",
    "ia",
    "id",
    "il",
    "in",
    "ks",
    "ky",
    "la",
    "ma",
    "md",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nj",
    "nm",
    "ny",
    "oh",
    "ok",
    "or",
    "pa",
    "tx",
    "ut",
    "va",
    "wa",
    "wv",
    "wy",
    "united_states_total",
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

UNDERGROUND_STORAGE_METRIC_BY_TYPE_AND_FREQUENCY: Final[dict[tuple[str, str], str]] = {
    ("total_gas", "monthly"): "underground_storage_total_gas_monthly",
    ("base_gas", "monthly"): "underground_storage_base_gas_monthly",
    ("working_gas", "monthly"): "underground_storage_working_gas_monthly",
    ("net_withdrawals", "monthly"): "underground_storage_net_withdrawals_monthly",
    ("injections", "monthly"): "underground_storage_injections_monthly",
    ("withdrawals", "monthly"): "underground_storage_withdrawals_monthly",
    ("working_gas_yoy_volume_change", "monthly"): "underground_storage_working_gas_yoy_volume_change_monthly",
    ("working_gas_yoy_pct_change", "monthly"): "underground_storage_working_gas_yoy_pct_change_monthly",
    ("total_gas", "annual"): "underground_storage_total_gas_annual",
    ("base_gas", "annual"): "underground_storage_base_gas_annual",
    ("working_gas", "annual"): "underground_storage_working_gas_annual",
    ("net_withdrawals", "annual"): "underground_storage_net_withdrawals_annual",
    ("injections", "annual"): "underground_storage_injections_annual",
    ("withdrawals", "annual"): "underground_storage_withdrawals_annual",
    ("working_gas_yoy_volume_change", "annual"): "underground_storage_working_gas_yoy_volume_change_annual",
    ("working_gas_yoy_pct_change", "annual"): "underground_storage_working_gas_yoy_pct_change_annual",
}

UNDERGROUND_STORAGE_BY_TYPE_METRIC_BY_TYPE_AND_FREQUENCY: Final[dict[tuple[str, str], str]] = {
    ("working_gas", "monthly"): "underground_storage_by_type_working_gas_monthly",
    ("base_gas", "monthly"): "underground_storage_by_type_base_gas_monthly",
    ("total_gas", "monthly"): "underground_storage_by_type_total_gas_monthly",
    ("injections", "monthly"): "underground_storage_by_type_injections_monthly",
    ("withdrawals", "monthly"): "underground_storage_by_type_withdrawals_monthly",
    ("net_withdrawals", "monthly"): "underground_storage_by_type_net_withdrawals_monthly",
    ("working_gas", "annual"): "underground_storage_by_type_working_gas_annual",
    ("base_gas", "annual"): "underground_storage_by_type_base_gas_annual",
    ("total_gas", "annual"): "underground_storage_by_type_total_gas_annual",
    ("injections", "annual"): "underground_storage_by_type_injections_annual",
    ("withdrawals", "annual"): "underground_storage_by_type_withdrawals_annual",
    ("net_withdrawals", "annual"): "underground_storage_by_type_net_withdrawals_annual",
}

UNDERGROUND_STORAGE_CAPACITY_METRIC_BY_TYPE_AND_FREQUENCY: Final[dict[tuple[str, str], str]] = {
    ("total_capacity", "monthly"): "underground_storage_total_capacity_monthly",
    ("total_capacity", "annual"): "underground_storage_total_capacity_annual",
    ("working_gas_capacity", "monthly"): "underground_storage_working_gas_capacity_monthly",
    ("working_gas_capacity", "annual"): "underground_storage_working_gas_capacity_annual",
    ("storage_field_count", "monthly"): "underground_storage_field_count_monthly",
    ("storage_field_count", "annual"): "underground_storage_field_count_annual",
}

LNG_STORAGE_METRIC_BY_TYPE_AND_FREQUENCY: Final[dict[tuple[str, str], str]] = {
    ("lng_storage_additions", "monthly"): "lng_storage_additions_monthly",
    ("lng_storage_withdrawals", "monthly"): "lng_storage_withdrawals_monthly",
    ("lng_storage_net_withdrawals", "monthly"): "lng_storage_net_withdrawals_monthly",
}

SUPPORTED_METRICS: Final[tuple[str, ...]] = tuple(
    list(STORAGE_METRIC_BY_VALUE_TYPE.values())
    + list(UNDERGROUND_STORAGE_METRIC_BY_TYPE_AND_FREQUENCY.values())
    + list(UNDERGROUND_STORAGE_BY_TYPE_METRIC_BY_TYPE_AND_FREQUENCY.values())
    + list(UNDERGROUND_STORAGE_CAPACITY_METRIC_BY_TYPE_AND_FREQUENCY.values())
    + list(LNG_STORAGE_METRIC_BY_TYPE_AND_FREQUENCY.values())
)
