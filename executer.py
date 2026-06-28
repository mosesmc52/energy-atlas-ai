from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import logging
from typing import Any, Dict, Optional

import pandas as pd
from schemas.answer import SourceRef
from agents.router import EnergyRouteResult
from tools.eia_adapter import EIAAdapter, EIAResult
from utils.dates import has_explicit_date_reference

logger = logging.getLogger(__name__)


@dataclass
class MetricResult:
    df: pd.DataFrame
    source: SourceRef
    meta: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class ExecuteRequest:
    metric: str
    start: str
    end: str
    filters: Dict[str, Any] | None = None


def _storage_requires_baseline_history(route: EnergyRouteResult) -> bool:
    comparisons = set(route.comparisons or [])
    return route.domain == "storage" and (
        route.analysis_type in {"seasonal_compare", "deviation_from_normal"}
        or bool(comparisons & {"five_year_avg", "five_year_range", "seasonal_normal"})
        or _storage_is_weekly_report_query(route)
    )


def _storage_is_weekly_report_query(route: EnergyRouteResult) -> bool:
    if route.domain != "storage":
        return False
    if route.storage_dataset != "weekly_working_gas":
        return False
    query = str(route.normalized_query or "").strip().lower()
    if not query:
        return False
    report_terms = (
        "report",
        "commentary",
        "what did eia say",
        "natural gas weekly",
        "weekly storage report",
        "summarize",
    )
    return any(term in query for term in report_terms)


def _expand_storage_fetch_window_for_baseline(
    *,
    start_date: str | None,
    end_date: str | None,
    years: int = 6,
) -> tuple[str, str]:
    resolved_end = pd.Timestamp(end_date or date.today().isoformat())
    fetch_start = resolved_end - pd.DateOffset(years=years)
    if start_date:
        requested_start = pd.Timestamp(start_date)
        if requested_start < fetch_start:
            fetch_start = requested_start
    return fetch_start.date().isoformat(), resolved_end.date().isoformat()


def _storage_should_expand_for_latest_all_operators(route: EnergyRouteResult) -> bool:
    if route.domain != "storage":
        return False
    if route.storage_dataset not in {"underground_storage_all_operators", "underground_storage_by_type", "lng_storage"}:
        return False
    if route.analysis_type not in {"latest", "ranking", "regional_compare"}:
        return False
    return not has_explicit_date_reference(str(route.normalized_query or ""))


def _storage_should_expand_for_default_time_series_history(route: EnergyRouteResult) -> bool:
    if route.domain != "storage":
        return False
    if route.storage_dataset not in {"underground_storage_by_type", "lng_storage"}:
        return False
    if route.analysis_type != "time_series":
        return False
    return not has_explicit_date_reference(str(route.normalized_query or ""))


def _expand_storage_fetch_window_for_latest_all_operators(
    *,
    end_date: str | None,
    frequency: str,
) -> tuple[str, str]:
    resolved_end = pd.Timestamp(end_date or date.today().isoformat())
    if frequency == "annual":
        fetch_start = resolved_end - pd.DateOffset(years=10)
    else:
        fetch_start = resolved_end - pd.DateOffset(years=2)
    return fetch_start.date().isoformat(), resolved_end.date().isoformat()


def _storage_should_retry_with_latest_available_all_operators(
    route: EnergyRouteResult,
    result: MetricResult,
) -> bool:
    if route.domain != "storage":
        return False
    if route.storage_dataset not in {"underground_storage_all_operators", "underground_storage_by_type", "lng_storage"}:
        return False
    if route.analysis_type not in {"latest", "ranking", "regional_compare"}:
        return False
    df = getattr(result, "df", None)
    return df is None or df.empty


def _storage_should_retry_with_latest_available_time_series(
    route: EnergyRouteResult,
    result: MetricResult,
) -> bool:
    if route.domain != "storage":
        return False
    if route.storage_dataset not in {"underground_storage_all_operators", "underground_storage_by_type", "lng_storage"}:
        return False
    if route.analysis_type != "time_series":
        return False
    if has_explicit_date_reference(str(route.normalized_query or "")):
        return False
    df = getattr(result, "df", None)
    return df is None or df.empty


def _latest_completed_storage_end_date(*, end_date: str | None, frequency: str) -> str:
    resolved_end = pd.Timestamp(end_date or date.today().isoformat())
    if frequency == "annual":
        fallback_end = pd.Timestamp(year=resolved_end.year - 1, month=12, day=31)
    else:
        current_month_start = pd.Timestamp(year=resolved_end.year, month=resolved_end.month, day=1)
        fallback_end = current_month_start - pd.Timedelta(days=1)
    return fallback_end.date().isoformat()


def _normalize_storage_regions(filters: dict) -> list[str]:
    raw_regions = filters.get("regions")
    if raw_regions is None:
        raw_region = filters.get("region")
        raw_regions = [raw_region] if raw_region else ["lower48"]
    elif isinstance(raw_regions, str):
        raw_regions = [raw_regions]

    regions: list[str] = []
    for raw_region in raw_regions or []:
        region = str(raw_region or "").strip().lower()
        if not region:
            continue
        if region == "all":
            regions.extend(sorted(EIAAdapter.STORAGE_REGIONS - {"lower48"}))
            continue
        if region not in EIAAdapter.STORAGE_REGIONS:
            raise ValueError(
                f"Invalid storage region '{region}'. Expected one of: {sorted(EIAAdapter.STORAGE_REGIONS)}"
            )
        if region not in regions:
            regions.append(region)

    return regions or ["lower48"]


def _normalize_storage_states(
    filters: dict,
    *,
    valid_states: set[str] | None = None,
) -> list[str]:
    raw_states = filters.get("states")
    states_all = bool(filters.get("states_all"))
    if raw_states is None:
        raw_state = filters.get("state")
        raw_states = [raw_state] if raw_state else ["united_states_total"]
    elif isinstance(raw_states, str):
        raw_states = [raw_states]
    return _resolve_storage_states(
        raw_states,
        states_all,
        valid_states=valid_states or EIAAdapter.UNDERGROUND_STORAGE_STATES,
    )


def _resolve_storage_states(
    states: list[str],
    states_all: bool,
    *,
    valid_states: set[str],
) -> list[str]:
    if states_all:
        return [
            state
            for state in valid_states
            if state != "united_states_total"
        ]

    resolved_states: list[str] = []
    for raw_state in states or []:
        state = str(raw_state or "").strip().lower()
        if not state:
            continue
        if state == "all":
            resolved_states.extend(
                [s for s in valid_states if s != "united_states_total"]
            )
            continue
        if state not in valid_states:
            raise ValueError(
                f"Invalid storage state '{state}'. Expected one of: {sorted(valid_states)}"
            )
        if state not in resolved_states:
            resolved_states.append(state)
    return resolved_states or ["united_states_total"]


def _resolve_capacity_count_geographies(filters: dict) -> list[str]:
    if filters.get("states") or filters.get("state") or filters.get("states_all"):
        return _normalize_storage_states(filters)

    raw_regions = filters.get("regions")
    if raw_regions is None:
        raw_regions = []
    elif isinstance(raw_regions, str):
        raw_regions = [raw_regions]

    regions: list[str] = []
    for raw_region in raw_regions or []:
        region = str(raw_region or "").strip().lower()
        if not region:
            continue
        if region not in EIAAdapter.UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS:
            raise ValueError(
                f"Invalid storage geography '{region}'. Expected one of: {sorted(EIAAdapter.UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS)}"
            )
        if region not in regions:
            regions.append(region)
    return regions or ["united_states_total"]


def _normalize_storage_types(filters: dict) -> list[str]:
    raw_storage_types = filters.get("storage_types")
    storage_types_all = bool(filters.get("storage_types_all"))
    storage_type = filters.get("storage_type")
    if raw_storage_types is None:
        raw_storage_types = [storage_type] if storage_type else []
    elif isinstance(raw_storage_types, str):
        raw_storage_types = [raw_storage_types]

    if storage_types_all:
        return list(EIAAdapter.STORAGE_TYPES)

    resolved_storage_types: list[str] = []
    for raw_storage_type in raw_storage_types or []:
        value = str(raw_storage_type or "").strip().lower()
        if not value:
            continue
        if value == "all":
            return list(EIAAdapter.STORAGE_TYPES)
        if value not in EIAAdapter.STORAGE_TYPES:
            raise ValueError(
                f"Invalid storage type '{value}'. Expected one of: {sorted(EIAAdapter.STORAGE_TYPES)}"
            )
        if value not in resolved_storage_types:
            resolved_storage_types.append(value)
    return resolved_storage_types


def _concat_storage_region_results(results: list[EIAResult]) -> EIAResult:
    frames: list[pd.DataFrame] = []
    source_references: list[str] = []
    region_meta: dict[str, Any] = {}

    for result in results:
        raw_params = result.source.parameters or {}
        params = raw_params if isinstance(raw_params, dict) else {}
        region = str(params.get("region") or "").strip().lower()
        source_references.append(result.source.reference)
        if region:
            region_meta[region] = result.meta or {}

        if result.df is None or result.df.empty:
            continue
        frame = result.df.copy()
        if "region" not in frame.columns:
            frame["region"] = region
        frames.append(frame[["date", "value", "region"]].copy())

    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["date", "value", "region"])
    )
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["region"] = df["region"].astype(str)
        df = df.dropna(subset=["date", "value", "region"]).sort_values(
            ["region", "date"]
        ).reset_index(drop=True)
    regions = [region for region in region_meta if region]
    source = SourceRef(
        source_type="eia_api",
        label="EIA Natural Gas Storage by Region",
        reference="eia-ng-client:natural_gas.storage_by_region",
        parameters={
            "regions": regions,
            "source_references": source_references,
        },
    )
    return EIAResult(
        df=df,
        source=source,
        meta={
            "regions": region_meta,
            "source_references": source_references,
        },
    )


def _concat_storage_state_results(results: list[EIAResult]) -> EIAResult:
    frames: list[pd.DataFrame] = []
    source_references: list[str] = []
    state_meta: dict[str, Any] = {}

    for result in results:
        raw_params = result.source.parameters or {}
        params = raw_params if isinstance(raw_params, dict) else {}
        state = str(params.get("state") or "").strip().lower()
        source_references.append(result.source.reference)
        if state:
            state_meta[state] = result.meta or {}

        if result.df is None or result.df.empty:
            continue
        frame = result.df.copy()
        if "state" not in frame.columns:
            frame["state"] = state
        frames.append(frame[["date", "value", "state"]].copy())

    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["date", "value", "state"])
    )
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["state"] = df["state"].astype(str)
        df = df.dropna(subset=["date", "value", "state"]).sort_values(
            ["state", "date"]
        ).reset_index(drop=True)
    states = [state for state in state_meta if state]
    source = SourceRef(
        source_type="eia_api",
        label="EIA Underground Natural Gas Storage by State",
        reference="eia-ng-client:natural_gas.underground_storage_all_operators_by_state",
        parameters={
            "states": states,
            "source_references": source_references,
        },
    )
    return EIAResult(
        df=df,
        source=source,
        meta={
            "states": state_meta,
            "source_references": source_references,
        },
    )


def _concat_storage_type_results(results: list[EIAResult]) -> EIAResult:
    frames: list[pd.DataFrame] = []
    source_references: list[str] = []
    storage_type_meta: dict[str, Any] = {}

    for result in results:
        raw_params = result.source.parameters or {}
        params = raw_params if isinstance(raw_params, dict) else {}
        storage_type = str(params.get("storage_type") or "").strip().lower()
        source_references.append(result.source.reference)
        if storage_type:
            storage_type_meta[storage_type] = result.meta or {}

        if result.df is None or result.df.empty:
            continue
        frame = result.df.copy()
        if "storage_type" not in frame.columns:
            frame["storage_type"] = storage_type
        frames.append(frame[["date", "value", "storage_type"]].copy())

    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["date", "value", "storage_type"])
    )
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["storage_type"] = df["storage_type"].astype(str)
        df = df.dropna(subset=["date", "value", "storage_type"]).sort_values(
            ["storage_type", "date"]
        ).reset_index(drop=True)
    storage_types = [value for value in storage_type_meta if value]
    source = SourceRef(
        source_type="eia_api",
        label="EIA Underground Natural Gas Storage by Type",
        reference="eia-ng-client:natural_gas.underground_storage_type",
        parameters={
            "storage_types": storage_types,
            "source_references": source_references,
        },
    )
    return EIAResult(
        df=df,
        source=source,
        meta={
            "storage_types": storage_type_meta,
            "source_references": source_references,
        },
    )


def _concat_storage_geography_results(results: list[EIAResult]) -> EIAResult:
    frames: list[pd.DataFrame] = []
    source_references: list[str] = []
    geography_meta: dict[str, Any] = {}

    for result in results:
        raw_params = result.source.parameters or {}
        params = raw_params if isinstance(raw_params, dict) else {}
        geography = str(params.get("geography") or "").strip().lower()
        source_references.append(result.source.reference)
        if geography:
            geography_meta[geography] = result.meta or {}

        if result.df is None or result.df.empty:
            continue
        frame = result.df.copy()
        if "geography" not in frame.columns:
            frame["geography"] = geography
        columns = [column for column in ("date", "value", "geography", "state", "region") if column in frame.columns]
        frames.append(frame[columns].copy())

    df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["date", "value", "geography"])
    )
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["geography"] = df["geography"].astype(str)
        sort_columns = ["geography", "date"]
        if "state" in df.columns:
            df["state"] = df["state"].astype(str)
        if "region" in df.columns:
            df["region"] = df["region"].astype(str)
        df = df.dropna(subset=["date", "value", "geography"]).sort_values(
            sort_columns
        ).reset_index(drop=True)
    geographies = [value for value in geography_meta if value]
    source = SourceRef(
        source_type="eia_api",
        label="EIA Underground Natural Gas Storage Capacity and Field Count",
        reference="eia-ng-client:natural_gas.underground_storage_capacity_or_count",
        parameters={
            "geographies": geographies,
            "source_references": source_references,
        },
    )
    return EIAResult(
        df=df,
        source=source,
        meta={
            "geographies": geography_meta,
            "source_references": source_references,
        },
    )


class MetricExecutor:
    """
    Deterministic dispatcher: metric -> implementation.
    """

    def __init__(
        self,
        *,
        eia: EIAAdapter,
    ):
        self.eia = eia

        self._metric_to_handler = {
            # --- EIA ---
            "working_gas_storage_lower48": self._eia_storage_lower48,
            "working_gas_storage_change_weekly": self._eia_storage_change_weekly,
            "underground_storage_total_gas_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_base_gas_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_working_gas_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_net_withdrawals_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_injections_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_withdrawals_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_working_gas_yoy_volume_change_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_working_gas_yoy_pct_change_monthly": self._eia_underground_storage_all_operators,
            "underground_storage_total_gas_annual": self._eia_underground_storage_all_operators,
            "underground_storage_base_gas_annual": self._eia_underground_storage_all_operators,
            "underground_storage_working_gas_annual": self._eia_underground_storage_all_operators,
            "underground_storage_net_withdrawals_annual": self._eia_underground_storage_all_operators,
            "underground_storage_injections_annual": self._eia_underground_storage_all_operators,
            "underground_storage_withdrawals_annual": self._eia_underground_storage_all_operators,
            "underground_storage_working_gas_yoy_volume_change_annual": self._eia_underground_storage_all_operators,
            "underground_storage_working_gas_yoy_pct_change_annual": self._eia_underground_storage_all_operators,
            "underground_storage_total_capacity_monthly": self._eia_underground_storage_capacity_or_count,
            "underground_storage_total_capacity_annual": self._eia_underground_storage_capacity_or_count,
            "underground_storage_working_gas_capacity_monthly": self._eia_underground_storage_capacity_or_count,
            "underground_storage_working_gas_capacity_annual": self._eia_underground_storage_capacity_or_count,
            "underground_storage_field_count_monthly": self._eia_underground_storage_capacity_or_count,
            "underground_storage_field_count_annual": self._eia_underground_storage_capacity_or_count,
            "lng_storage_additions_annual": self._eia_lng_storage,
            "lng_storage_withdrawals_annual": self._eia_lng_storage,
            "lng_storage_net_withdrawals_annual": self._eia_lng_storage,
            "underground_storage_by_type_working_gas_monthly": self._eia_underground_storage_by_type,
            "underground_storage_by_type_base_gas_monthly": self._eia_underground_storage_by_type,
            "underground_storage_by_type_total_gas_monthly": self._eia_underground_storage_by_type,
            "underground_storage_by_type_injections_monthly": self._eia_underground_storage_by_type,
            "underground_storage_by_type_withdrawals_monthly": self._eia_underground_storage_by_type,
            "underground_storage_by_type_net_withdrawals_monthly": self._eia_underground_storage_by_type,
            "underground_storage_by_type_working_gas_annual": self._eia_underground_storage_by_type,
            "underground_storage_by_type_base_gas_annual": self._eia_underground_storage_by_type,
            "underground_storage_by_type_total_gas_annual": self._eia_underground_storage_by_type,
            "underground_storage_by_type_injections_annual": self._eia_underground_storage_by_type,
            "underground_storage_by_type_withdrawals_annual": self._eia_underground_storage_by_type,
            "underground_storage_by_type_net_withdrawals_annual": self._eia_underground_storage_by_type,
            "henry_hub_spot": self._eia_henry_hub_spot,
            "lng_exports": self._eia_lng_exports,
            "lng_imports": self._eia_lng_imports,
            "ng_electricity": self._eia_ng_electricity,
            "ng_consumption_lower48": self._eia_ng_consumption_lower48,
            "ng_consumption_by_sector": self._eia_ng_consumption_by_sector,
            "ng_production_lower48": self._eia_ng_production_lower48,
            "ng_supply_balance_regime": self._eia_ng_supply_balance_regime,
            "ng_exploration_reserves_lower48": self._eia_ng_exploration_reserves_lower48,
            "ng_pipeline": self._eia_ng_pipeline,
            "weather_degree_days_forecast_vs_5y": self._eia_weather_degree_days_forecast_vs_5y,
            "weather_regional_demand_drivers": self._eia_weather_regional_demand_drivers,
            "weekly_energy_atlas_summary": self._eia_weekly_energy_atlas_summary,
        }

    def execute(self, req: ExecuteRequest) -> MetricResult:
        if req.metric not in self._metric_to_handler:
            raise ValueError(f"Unsupported metric: {req.metric}")

        handler = self._metric_to_handler[req.metric]

        # ---- execute adapter handler ----
        runtime_filters = dict(req.filters or {})
        res = handler(start=req.start, end=req.end, filters=runtime_filters)

        # Normalize to MetricResult
        result = self._to_metric_result(res)

        # ---- attach execution context ----
        if result.meta is None:
            result.meta = {}

        result.meta.update(
            {
                "metric": req.metric,
                "start": req.start,
                "end": req.end,
                "filters": req.filters or {},
                "executed_at": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            }
        )

        return result

    def execute_storage_route(self, route: EnergyRouteResult) -> MetricResult:
        if route.domain != "storage":
            raise ValueError(f"Unsupported route domain for storage execution: {route.domain}")
        if not route.primary_metric:
            raise ValueError("Storage route is missing a primary metric.")

        filters = dict(route.filters or {})
        filters["regions"] = list(route.regions or filters.get("regions") or [])
        filters["states"] = list(route.states or filters.get("states") or [])
        filters["states_all"] = bool(route.states_all or filters.get("states_all"))
        filters["storage_dataset"] = route.storage_dataset
        filters["storage_frequency"] = route.storage_frequency
        filters["storage_metric_type"] = route.storage_metric_type
        filters["storage_type"] = route.storage_type
        filters["storage_types_all"] = bool(route.storage_types_all or filters.get("storage_types_all"))
        filters["storage_insight_type"] = getattr(route, "storage_insight_type", None)
        if route.analysis_type == "explain" and getattr(route, "storage_insight_type", None):
            return self.execute_storage_insight_route(route)
        if route.storage_metric_type in {"total_capacity", "working_gas_capacity", "storage_field_count"}:
            filters["regions"] = [
                region
                for region in filters.get("regions", [])
                if region in EIAAdapter.UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS
            ]
        if route.storage_dataset == "weekly_working_gas" and route.analysis_type in {"regional_compare", "ranking"}:
            filters["regions"] = ["all"]
        requested_start_date = route.start_date
        requested_end_date = route.end_date
        fetch_start_date = route.start_date or ""
        fetch_end_date = route.end_date or date.today().isoformat()
        if _storage_requires_baseline_history(route):
            fetch_start_date, fetch_end_date = _expand_storage_fetch_window_for_baseline(
                start_date=route.start_date,
                end_date=route.end_date,
                years=6,
            )
        elif _storage_should_expand_for_default_time_series_history(route):
            fetch_start_date, fetch_end_date = _expand_storage_fetch_window_for_baseline(
                start_date=route.start_date,
                end_date=route.end_date,
                years=6,
            )
        elif _storage_should_expand_for_latest_all_operators(route):
            fetch_start_date, fetch_end_date = _expand_storage_fetch_window_for_latest_all_operators(
                end_date=route.end_date,
                frequency=route.storage_frequency,
            )
        logger.info(
            "storage_execute dataset=%s metric=%s route_regions=%s route_states=%s fetch=%s..%s",
            route.storage_dataset,
            route.primary_metric,
            list(route.regions or []),
            list(route.states or []),
            fetch_start_date,
            fetch_end_date,
        )

        result = self.execute(
            ExecuteRequest(
                metric=route.primary_metric,
                start=fetch_start_date,
                end=fetch_end_date,
                filters=filters,
            )
        )
        fallback_fetch_start_date: str | None = None
        fallback_fetch_end_date: str | None = None
        latest_available_fallback = False
        if _storage_should_retry_with_latest_available_all_operators(route, result):
            fallback_fetch_start_date, fallback_fetch_end_date = (
                _expand_storage_fetch_window_for_latest_all_operators(
                    end_date=route.end_date,
                    frequency=route.storage_frequency,
                )
            )
            if (
                fallback_fetch_start_date != fetch_start_date
                or fallback_fetch_end_date != fetch_end_date
            ):
                logger.info(
                    "storage_execute empty_result_retry dataset=%s metric=%s retry_fetch=%s..%s",
                    route.storage_dataset,
                    route.primary_metric,
                    fallback_fetch_start_date,
                    fallback_fetch_end_date,
                )
                result = self.execute(
                    ExecuteRequest(
                        metric=route.primary_metric,
                        start=fallback_fetch_start_date,
                        end=fallback_fetch_end_date,
                        filters=filters,
                    )
                )
                latest_available_fallback = True
                fetch_start_date = fallback_fetch_start_date
                fetch_end_date = fallback_fetch_end_date
        elif _storage_should_retry_with_latest_available_time_series(route, result):
            latest_completed_end_date = _latest_completed_storage_end_date(
                end_date=route.end_date,
                frequency=route.storage_frequency,
            )
            fallback_fetch_start_date, fallback_fetch_end_date = (
                _expand_storage_fetch_window_for_baseline(
                    start_date=None,
                    end_date=latest_completed_end_date,
                    years=6,
                )
            )
            if (
                fallback_fetch_start_date != fetch_start_date
                or fallback_fetch_end_date != fetch_end_date
            ):
                logger.info(
                    "storage_execute empty_time_series_retry dataset=%s metric=%s retry_fetch=%s..%s",
                    route.storage_dataset,
                    route.primary_metric,
                    fallback_fetch_start_date,
                    fallback_fetch_end_date,
                )
                result = self.execute(
                    ExecuteRequest(
                        metric=route.primary_metric,
                        start=fallback_fetch_start_date,
                        end=fallback_fetch_end_date,
                        filters=filters,
                    )
                )
                latest_available_fallback = True
                fetch_start_date = fallback_fetch_start_date
                fetch_end_date = fallback_fetch_end_date
        if result.meta is None:
            result.meta = {}
        result.meta.update(
            {
                "domain": "storage",
                "analysis_type": route.analysis_type,
                "value_type": route.value_type,
                "comparisons": list(route.comparisons or []),
                "storage_dataset": route.storage_dataset,
                "storage_frequency": route.storage_frequency,
                "storage_metric_type": route.storage_metric_type,
                "storage_type": route.storage_type,
                "storage_types_all": route.storage_types_all,
                "chart_type": route.chart_type,
                "output_mode": route.output_mode,
                "regions": list(route.regions or []),
                "states": list(route.states or []),
                "states_all": route.states_all,
                "start_date": route.start_date,
                "end_date": route.end_date,
                "requested_start_date": requested_start_date,
                "requested_end_date": requested_end_date,
                "fetch_start_date": fetch_start_date,
                "fetch_end_date": fetch_end_date,
                "latest_available_fallback": latest_available_fallback,
                "fallback_fetch_start_date": fallback_fetch_start_date,
                "fallback_fetch_end_date": fallback_fetch_end_date,
            }
        )
        if result.df is not None and "region" in result.df.columns:
            logger.info(
                "storage_execute result_regions=%s rows=%s",
                sorted(result.df["region"].dropna().astype(str).unique().tolist()),
                len(result.df),
            )
        if result.df is not None and "state" in result.df.columns:
            logger.info(
                "storage_execute result_states=%s rows=%s",
                sorted(result.df["state"].dropna().astype(str).unique().tolist()),
                len(result.df),
            )
        if result.df is not None and "storage_type" in result.df.columns:
            logger.info(
                "storage_execute result_storage_types=%s rows=%s",
                sorted(result.df["storage_type"].dropna().astype(str).unique().tolist()),
                len(result.df),
            )
        return result

    def execute_storage_insight_route(self, route: EnergyRouteResult) -> MetricResult:
        insight_type = str(getattr(route, "storage_insight_type", "") or "")
        if not insight_type:
            raise ValueError("Storage insight route is missing storage_insight_type.")

        fetch_end_date = route.end_date or date.today().isoformat()
        if insight_type == "historical_max_compare":
            if route.start_date:
                fetch_start_date = route.start_date
            else:
                fetch_start_date = (pd.Timestamp(fetch_end_date) - pd.DateOffset(years=20)).date().isoformat()
        elif insight_type == "weekly_report_card":
            fetch_start_date, fetch_end_date = _expand_storage_fetch_window_for_baseline(
                start_date=route.start_date,
                end_date=route.end_date,
                years=6,
            )
        elif route.start_date:
            fetch_start_date = route.start_date
        else:
            fetch_start_date, fetch_end_date = _expand_storage_fetch_window_for_latest_all_operators(
                end_date=route.end_date,
                frequency=route.storage_frequency,
            )

        if insight_type == "storage_utilization":
            result = self._build_storage_utilization_result(route, fetch_start_date, fetch_end_date)
        elif insight_type == "remaining_capacity":
            result = self._build_storage_remaining_capacity_result(route, fetch_start_date, fetch_end_date)
        elif insight_type == "capacity_per_field":
            result = self._build_storage_capacity_per_field_result(route, fetch_start_date, fetch_end_date)
        elif insight_type == "historical_max_compare":
            result = self._build_storage_historical_max_compare_result(route, fetch_start_date, fetch_end_date)
        elif insight_type == "weekly_report_card":
            result = self._build_weekly_storage_report_card_result(route, fetch_start_date, fetch_end_date)
        else:
            raise ValueError(f"Unsupported storage insight type: {insight_type}")

        if result.meta is None:
            result.meta = {}
        result.meta.update(
            {
                "domain": "storage",
                "analysis_type": route.analysis_type,
                "storage_dataset": route.storage_dataset,
                "storage_frequency": route.storage_frequency,
                "storage_metric_type": route.storage_metric_type,
                "storage_insight_type": insight_type,
                "chart_type": route.chart_type,
                "output_mode": route.output_mode,
                "regions": list(route.regions or []),
                "states": list(route.states or []),
                "states_all": route.states_all,
                "start_date": route.start_date,
                "end_date": route.end_date,
                "fetch_start_date": fetch_start_date,
                "fetch_end_date": fetch_end_date,
            }
        )
        return result

    def _execute_storage_metric(
        self,
        *,
        metric: str,
        start: str,
        end: str,
        filters: Dict[str, Any],
    ) -> MetricResult:
        return self.execute(
            ExecuteRequest(
                metric=metric,
                start=start,
                end=end,
                filters=filters,
            )
        )

    def _normalize_storage_insight_geography_df(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy() if df is not None else pd.DataFrame()
        if out.empty:
            return pd.DataFrame(columns=["date", "value", "geography"])
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
        if "geography" not in out.columns:
            if "state" in out.columns:
                out["geography"] = out["state"]
            elif "region" in out.columns:
                out["geography"] = out["region"]
            else:
                out["geography"] = "united_states_total"
        out["geography"] = out["geography"].astype(str)
        return out.dropna(subset=["date", "value", "geography"]).reset_index(drop=True)

    def _latest_by_geography(self, df: pd.DataFrame) -> pd.DataFrame:
        d = self._normalize_storage_insight_geography_df(df)
        if d.empty:
            return d
        return (
            d.sort_values(["geography", "date"])
            .groupby("geography", as_index=False, sort=False)
            .tail(1)
            .reset_index(drop=True)
        )

    def _build_derived_storage_source(
        self,
        *,
        label: str,
        reference: str,
        parameters: Dict[str, Any],
    ) -> SourceRef:
        return SourceRef(
            source_type="manual",
            label=label,
            reference=reference,
            parameters=parameters,
        )

    def _working_gas_component_result(
        self,
        route: EnergyRouteResult,
        *,
        start: str,
        end: str,
    ) -> MetricResult:
        if route.regions and not route.states:
            return self._execute_storage_metric(
                metric="working_gas_storage_lower48",
                start=start,
                end=end,
                filters={"regions": list(route.regions or ["lower48"])},
            )
        frequency = route.storage_frequency if route.storage_frequency in {"monthly", "annual"} else "monthly"
        metric = f"underground_storage_working_gas_{frequency}"
        return self._execute_storage_metric(
            metric=metric,
            start=start,
            end=end,
            filters={
                "states": list(route.states or []),
                "states_all": bool(route.states_all),
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": frequency,
                "storage_metric_type": "working_gas",
            },
        )

    def _build_storage_utilization_frame(
        self,
        route: EnergyRouteResult,
        *,
        start: str,
        end: str,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        working_gas_result = self._working_gas_component_result(route, start=start, end=end)
        capacity_frequency = route.storage_frequency if route.storage_frequency in {"monthly", "annual"} else "monthly"
        capacity_result = self._execute_storage_metric(
            metric=f"underground_storage_working_gas_capacity_{capacity_frequency}",
            start=start,
            end=end,
            filters={
                "states": list(route.states or []),
                "states_all": bool(route.states_all),
                "regions": list(route.regions or []),
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": capacity_frequency,
                "storage_metric_type": "working_gas_capacity",
            },
        )
        working_gas_df = self._latest_by_geography(working_gas_result.df).rename(columns={"value": "working_gas"})
        capacity_df = self._latest_by_geography(capacity_result.df).rename(columns={"value": "working_gas_capacity"})
        merged = working_gas_df.merge(
            capacity_df[["geography", "date", "working_gas_capacity"]],
            on="geography",
            how="inner",
            suffixes=("_working_gas", "_capacity"),
        )
        if merged.empty:
            return pd.DataFrame(columns=["date", "geography", "working_gas", "working_gas_capacity", "utilization_pct", "remaining_capacity", "value"]), {
                "component_metrics": ["working_gas", "working_gas_capacity"],
            }
        merged["date"] = merged[["date_working_gas", "date_capacity"]].max(axis=1)
        merged["remaining_capacity"] = merged["working_gas_capacity"] - merged["working_gas"]
        merged["utilization_pct"] = (merged["working_gas"] / merged["working_gas_capacity"]) * 100.0
        if route.states or route.states_all:
            merged["state"] = merged["geography"]
        elif route.regions:
            merged["region"] = merged["geography"]
        return merged[
            [column for column in ("date", "geography", "state", "region", "working_gas", "working_gas_capacity", "utilization_pct", "remaining_capacity") if column in merged.columns]
        ], {
            "component_metrics": ["working_gas", "working_gas_capacity"],
            "component_sources": [working_gas_result.source.reference, capacity_result.source.reference],
        }

    def _build_storage_utilization_result(self, route: EnergyRouteResult, start: str, end: str) -> MetricResult:
        df, meta = self._build_storage_utilization_frame(route, start=start, end=end)
        if not df.empty:
            df["value"] = df["utilization_pct"]
        source = self._build_derived_storage_source(
            label="Derived Storage Utilization",
            reference="energy_atlas:storage_utilization",
            parameters={"start": start, "end": end},
        )
        return MetricResult(df=df, source=source, meta={"metric": "storage_utilization", "units": "%", **meta})

    def _build_storage_remaining_capacity_result(self, route: EnergyRouteResult, start: str, end: str) -> MetricResult:
        df, meta = self._build_storage_utilization_frame(route, start=start, end=end)
        if not df.empty:
            df["value"] = df["remaining_capacity"]
        source = self._build_derived_storage_source(
            label="Derived Remaining Storage Capacity",
            reference="energy_atlas:storage_remaining_capacity",
            parameters={"start": start, "end": end},
        )
        return MetricResult(df=df, source=source, meta={"metric": "storage_remaining_capacity", "units": "MMcf", **meta})

    def _build_storage_capacity_per_field_result(self, route: EnergyRouteResult, start: str, end: str) -> MetricResult:
        frequency = route.storage_frequency if route.storage_frequency in {"monthly", "annual"} else "monthly"
        capacity_result = self._execute_storage_metric(
            metric=f"underground_storage_working_gas_capacity_{frequency}",
            start=start,
            end=end,
            filters={
                "states": list(route.states or []),
                "states_all": bool(route.states_all),
                "regions": list(route.regions or []),
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": frequency,
                "storage_metric_type": "working_gas_capacity",
            },
        )
        count_result = self._execute_storage_metric(
            metric=f"underground_storage_field_count_{frequency}",
            start=start,
            end=end,
            filters={
                "states": list(route.states or []),
                "states_all": bool(route.states_all),
                "regions": list(route.regions or []),
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": frequency,
                "storage_metric_type": "storage_field_count",
            },
        )
        capacity_df = self._latest_by_geography(capacity_result.df).rename(columns={"value": "capacity"})
        count_df = self._latest_by_geography(count_result.df).rename(columns={"value": "storage_field_count"})
        merged = capacity_df.merge(
            count_df[["geography", "date", "storage_field_count"]],
            on="geography",
            how="inner",
            suffixes=("_capacity", "_count"),
        )
        if merged.empty:
            df = pd.DataFrame(columns=["date", "geography", "capacity", "storage_field_count", "capacity_per_field", "value"])
        else:
            merged = merged.loc[pd.to_numeric(merged["storage_field_count"], errors="coerce") > 0].copy()
            merged["date"] = merged[["date_capacity", "date_count"]].max(axis=1)
            merged["capacity_per_field"] = merged["capacity"] / merged["storage_field_count"]
            merged["value"] = merged["capacity_per_field"]
            if route.states or route.states_all:
                merged["state"] = merged["geography"]
            elif route.regions:
                merged["region"] = merged["geography"]
            df = merged[
                [column for column in ("date", "geography", "state", "region", "capacity", "storage_field_count", "capacity_per_field", "value") if column in merged.columns]
            ]
        source = self._build_derived_storage_source(
            label="Derived Storage Capacity per Field",
            reference="energy_atlas:storage_capacity_per_field",
            parameters={"start": start, "end": end},
        )
        return MetricResult(
            df=df,
            source=source,
            meta={
                "metric": "storage_capacity_per_field",
                "units": "MMcf/field",
                "component_metrics": ["working_gas_capacity", "storage_field_count"],
                "component_sources": [capacity_result.source.reference, count_result.source.reference],
            },
        )

    def _build_storage_historical_max_compare_result(self, route: EnergyRouteResult, start: str, end: str) -> MetricResult:
        if route.states:
            frequency = route.storage_frequency if route.storage_frequency in {"monthly", "annual"} else "monthly"
            result = self._execute_storage_metric(
                metric=f"underground_storage_working_gas_{frequency}",
                start=start,
                end=end,
                filters={
                    "states": list(route.states or []),
                    "states_all": bool(route.states_all),
                    "storage_dataset": "underground_storage_all_operators",
                    "storage_frequency": frequency,
                    "storage_metric_type": "working_gas",
                },
            )
            df = self._normalize_storage_insight_geography_df(result.df)
            units = "MMcf"
        else:
            result = self._execute_storage_metric(
                metric="working_gas_storage_lower48",
                start=start,
                end=end,
                filters={"regions": list(route.regions or ["lower48"])},
            )
            df = self._normalize_storage_insight_geography_df(result.df)
            units = "Bcf"
        rows: list[pd.DataFrame] = []
        for geography, group in df.groupby("geography", sort=False):
            group = group.sort_values("date").reset_index(drop=True)
            if group.empty:
                continue
            max_idx = group["value"].idxmax()
            max_row = group.loc[max_idx]
            current_row = group.iloc[-1]
            group = group.copy()
            group["current_storage"] = float(current_row["value"])
            group["max_observed_storage"] = float(max_row["value"])
            group["max_observed_date"] = pd.Timestamp(max_row["date"]).date().isoformat()
            group["pct_of_max_observed"] = (
                (float(current_row["value"]) / float(max_row["value"])) * 100.0 if float(max_row["value"]) else pd.NA
            )
            group["difference_from_max"] = float(current_row["value"]) - float(max_row["value"])
            group["value"] = pd.to_numeric(group["value"], errors="coerce")
            if route.states:
                group["state"] = geography
            else:
                group["region"] = geography
            rows.append(group)
        out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
            columns=["date", "value", "geography", "current_storage", "max_observed_storage", "max_observed_date", "pct_of_max_observed", "difference_from_max"]
        )
        source = self._build_derived_storage_source(
            label="Derived Storage Historical Maximum Comparison",
            reference="energy_atlas:storage_historical_max_compare",
            parameters={"start": start, "end": end},
        )
        return MetricResult(
            df=out,
            source=source,
            meta={"metric": "storage_historical_max_compare", "units": units, "component_sources": [result.source.reference]},
        )

    def _same_week_samples(self, df: pd.DataFrame, *, value_column: str, target_date: pd.Timestamp, years: int = 5, tolerance_days: int = 10) -> list[float]:
        samples: list[float] = []
        latest_year = int(target_date.year)
        for year in range(latest_year - years, latest_year):
            shifted = target_date - pd.DateOffset(years=(latest_year - year))
            candidates = df.loc[
                (df["date"].dt.year == year)
                & (df["date"] >= (shifted - pd.Timedelta(days=tolerance_days)))
                & (df["date"] <= (shifted + pd.Timedelta(days=tolerance_days)))
            ].copy()
            if candidates.empty or value_column not in candidates.columns:
                continue
            candidates["days_from_target"] = (candidates["date"] - shifted).abs().dt.days
            picked = candidates.sort_values(["days_from_target", "date"]).iloc[0]
            value = pd.to_numeric(picked[value_column], errors="coerce")
            if pd.notna(value):
                samples.append(float(value))
        return samples

    def _build_weekly_storage_report_card_result(self, route: EnergyRouteResult, start: str, end: str) -> MetricResult:
        result = self._execute_storage_metric(
            metric="working_gas_storage_lower48",
            start=start,
            end=end,
            filters={"regions": list(route.regions or ["lower48"]), "include_weekly_change": True},
        )
        df = self._normalize_storage_insight_geography_df(result.df)
        if "weekly_change" not in df.columns:
            df["weekly_change"] = pd.NA
        if df.empty:
            out = pd.DataFrame(
                columns=[
                    "date",
                    "current_storage",
                    "weekly_change",
                    "prior_weekly_change",
                    "five_year_avg_storage",
                    "storage_deviation_bcf",
                    "storage_deviation_pct",
                    "weekly_change_vs_prior",
                    "weekly_change_vs_5y_avg",
                    "value",
                ]
            )
        else:
            df = df.sort_values("date").reset_index(drop=True)
            latest = df.iloc[-1]
            prior = df.iloc[-2] if len(df) > 1 else None
            target_date = pd.Timestamp(latest["date"])
            storage_samples = self._same_week_samples(df, value_column="value", target_date=target_date)
            weekly_change_samples = self._same_week_samples(df, value_column="weekly_change", target_date=target_date)
            five_year_avg_storage = float(pd.Series(storage_samples, dtype="float64").mean()) if storage_samples else pd.NA
            five_year_avg_change = float(pd.Series(weekly_change_samples, dtype="float64").mean()) if weekly_change_samples else pd.NA
            current_storage = float(latest["value"])
            weekly_change = float(latest["weekly_change"]) if pd.notna(latest["weekly_change"]) else pd.NA
            prior_weekly_change = float(prior["weekly_change"]) if prior is not None and pd.notna(prior["weekly_change"]) else pd.NA
            storage_deviation_bcf = current_storage - five_year_avg_storage if pd.notna(five_year_avg_storage) else pd.NA
            storage_deviation_pct = (
                ((current_storage - five_year_avg_storage) / five_year_avg_storage) * 100.0
                if pd.notna(five_year_avg_storage) and five_year_avg_storage
                else pd.NA
            )
            weekly_change_vs_prior = (
                weekly_change - prior_weekly_change
                if pd.notna(weekly_change) and pd.notna(prior_weekly_change)
                else pd.NA
            )
            weekly_change_vs_5y_avg = (
                weekly_change - five_year_avg_change
                if pd.notna(weekly_change) and pd.notna(five_year_avg_change)
                else pd.NA
            )
            out = pd.DataFrame(
                [
                    {
                        "date": target_date,
                        "geography": str(latest.get("geography") or "lower48"),
                        "current_storage": current_storage,
                        "weekly_change": weekly_change,
                        "prior_weekly_change": prior_weekly_change,
                        "five_year_avg_storage": five_year_avg_storage,
                        "storage_deviation_bcf": storage_deviation_bcf,
                        "storage_deviation_pct": storage_deviation_pct,
                        "weekly_change_vs_prior": weekly_change_vs_prior,
                        "weekly_change_vs_5y_avg": weekly_change_vs_5y_avg,
                        "value": current_storage,
                    }
                ]
            )
        source = self._build_derived_storage_source(
            label="Derived Weekly Storage Report Card",
            reference="energy_atlas:storage_weekly_report_card",
            parameters={"start": start, "end": end},
        )
        return MetricResult(
            df=out,
            source=source,
            meta={"metric": "storage_weekly_report_card", "units": "Bcf", "component_sources": [result.source.reference]},
        )

    def _to_metric_result(self, res: Any) -> MetricResult:
        """
        Convert adapter-specific result types into a unified MetricResult.
        """
        if isinstance(res, MetricResult):
            return res
        if isinstance(res, EIAResult):
            return MetricResult(df=res.df, source=res.source, meta=res.meta)
        # fallback: duck-typing if needed
        if hasattr(res, "df") and hasattr(res, "source"):
            return MetricResult(
                df=res.df, source=res.source, meta=getattr(res, "meta", None)
            )
        raise TypeError(f"Unsupported result type from handler: {type(res)}")

    # -----------------------
    # Metric handlers (EIA)
    # -----------------------

    def _eia_storage_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        regions = _normalize_storage_regions(filters)
        if len(regions) == 1:
            region = regions[0]
            base = self.eia.storage_working_gas(start=start, end=end, region=region)
            if not bool(filters.get("include_weekly_change")):
                frame = base.df.copy() if base.df is not None else pd.DataFrame(columns=["date", "value"])
                if "region" not in frame.columns:
                    frame["region"] = region
                frame = frame[["date", "value", "region"]]
                return EIAResult(df=frame, source=base.source, meta=base.meta)

            weekly_change = self.eia.storage_working_gas_change_weekly(
                start=start, end=end, region=region
            )
            left = base.df.copy()
            right = weekly_change.df.copy()
            if left is None or left.empty:
                merged = pd.DataFrame(columns=["date", "value", "weekly_change", "region"])
            else:
                merged = left.copy()
                if right is not None and not right.empty:
                    right = right.rename(columns={"value": "weekly_change"})
                    merged = merged.merge(right[["date", "weekly_change"]], on="date", how="left")
                else:
                    merged["weekly_change"] = pd.NA
                merged["region"] = region

            return EIAResult(
                df=merged,
                source=SourceRef(
                    source_type="eia_api",
                    label=f"EIA Natural Gas Storage: Working Gas and Weekly Change ({region.replace('_', ' ').title()})",
                    reference="eia-ng-client:natural_gas.storage_with_weekly_change",
                    parameters={
                        "region": region,
                        "start": start,
                        "end": end,
                        "include_weekly_change": True,
                        "source_storage_reference": base.source.reference,
                        "source_change_reference": weekly_change.source.reference,
                    },
                ),
                meta={
                    "cache": {
                        "storage": (base.meta or {}).get("cache"),
                        "weekly_change": (weekly_change.meta or {}).get("cache"),
                    },
                    "note": "Working gas storage merged with derived weekly change for the same region.",
                },
            )

        results = []
        for region in regions:
            regional_result = self.eia.storage_working_gas(start=start, end=end, region=region)
            frame = regional_result.df.copy() if regional_result.df is not None else pd.DataFrame(columns=["date", "value"])
            if not frame.empty:
                frame["region"] = region
            results.append(
                EIAResult(df=frame, source=regional_result.source, meta=regional_result.meta)
            )
        if not bool(filters.get("include_weekly_change")):
            return _concat_storage_region_results(results)

        weekly_change_results = []
        for region in regions:
            regional_result = self.eia.storage_working_gas_change_weekly(
                start=start, end=end, region=region
            )
            right = regional_result.df.copy() if regional_result.df is not None else pd.DataFrame(columns=["date", "value"])
            right["region"] = region
            weekly_change_results.append(
                EIAResult(df=right, source=regional_result.source, meta=regional_result.meta)
            )
        right_df = _concat_storage_region_results(weekly_change_results).df.rename(
            columns={"value": "weekly_change"}
        )
        merged = _concat_storage_region_results(results).df.merge(
            right_df[["date", "region", "weekly_change"]],
            on=["date", "region"],
            how="left",
        )
        return EIAResult(
            df=merged,
            source=SourceRef(
                source_type="eia_api",
                label="EIA Natural Gas Storage: Working Gas and Weekly Change by Region",
                reference="eia-ng-client:natural_gas.storage_with_weekly_change_by_region",
                parameters={
                    "regions": regions,
                    "start": start,
                    "end": end,
                    "include_weekly_change": True,
                },
            ),
            meta={"regions": regions},
        )

    def _eia_storage_change_weekly(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        regions = _normalize_storage_regions(filters)
        if len(regions) > 1:
            results = []
            for region in regions:
                regional_result = self.eia.storage_working_gas_change_weekly(
                    start=start, end=end, region=region
                )
                frame = regional_result.df.copy() if regional_result.df is not None else pd.DataFrame(columns=["date", "value"])
                if not frame.empty:
                    frame["region"] = region
                results.append(
                    EIAResult(df=frame, source=regional_result.source, meta=regional_result.meta)
                )
            return _concat_storage_region_results(results)

        region = regions[0]
        result = self.eia.storage_working_gas_change_weekly(
            start=start, end=end, region=region
        )
        frame = result.df.copy() if result.df is not None else pd.DataFrame(columns=["date", "value"])
        if "region" not in frame.columns:
            frame["region"] = region
        frame = frame[["date", "value", "region"]]
        return EIAResult(df=frame, source=result.source, meta=result.meta)

    def _eia_underground_storage_all_operators(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        states = _normalize_storage_states(filters)
        metric_type = str(filters.get("storage_metric_type") or "working_gas")
        frequency = str(filters.get("storage_frequency") or "monthly")

        if len(states) == 1:
            state = states[0]
            result = self.eia.underground_storage_all_operators(
                start=start,
                end=end,
                state=state,
                metric_type=metric_type,
                frequency=frequency,
            )
            frame = result.df.copy() if result.df is not None else pd.DataFrame(columns=["date", "value"])
            if "state" not in frame.columns:
                frame["state"] = state
            frame = frame[["date", "value", "state"]]
            return EIAResult(df=frame, source=result.source, meta=result.meta)

        results = []
        for state in states:
            state_result = self.eia.underground_storage_all_operators(
                start=start,
                end=end,
                state=state,
                metric_type=metric_type,
                frequency=frequency,
            )
            frame = state_result.df.copy() if state_result.df is not None else pd.DataFrame(columns=["date", "value"])
            if not frame.empty:
                frame["state"] = state
            results.append(
                EIAResult(df=frame, source=state_result.source, meta=state_result.meta)
            )
        return _concat_storage_state_results(results)

    def _eia_underground_storage_capacity_or_count(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        geographies = _resolve_capacity_count_geographies(filters)
        metric_type = str(filters.get("storage_metric_type") or "total_capacity")
        frequency = str(filters.get("storage_frequency") or "monthly")

        def attach_geography_columns(frame: pd.DataFrame, geography: str) -> pd.DataFrame:
            out = frame.copy() if frame is not None else pd.DataFrame(columns=["date", "value"])
            if "geography" not in out.columns:
                out["geography"] = geography
            else:
                out["geography"] = out["geography"].fillna(geography)
            if geography in EIAAdapter.UNDERGROUND_STORAGE_STATES:
                out["state"] = geography
            elif geography in EIAAdapter.STORAGE_REGIONS:
                out["region"] = geography
            return out

        def fetch_for_geography(geography: str) -> EIAResult:
            if metric_type == "storage_field_count":
                return self.eia.underground_storage_count(
                    start=start,
                    end=end,
                    geography=geography,
                    frequency=frequency,
                )
            capacity_type = "working_gas" if metric_type == "working_gas_capacity" else "total"
            return self.eia.underground_storage_capacity(
                start=start,
                end=end,
                geography=geography,
                capacity_type=capacity_type,
                frequency=frequency,
            )

        if len(geographies) == 1:
            geography = geographies[0]
            result = fetch_for_geography(geography)
            frame = attach_geography_columns(
                result.df if result.df is not None else pd.DataFrame(columns=["date", "value"]),
                geography,
            )
            columns = [column for column in ("date", "value", "geography", "state", "region") if column in frame.columns]
            return EIAResult(df=frame[columns], source=result.source, meta=result.meta)

        results = []
        for geography in geographies:
            geography_result = fetch_for_geography(geography)
            frame = attach_geography_columns(
                geography_result.df if geography_result.df is not None else pd.DataFrame(columns=["date", "value"]),
                geography,
            )
            columns = [column for column in ("date", "value", "geography", "state", "region") if column in frame.columns]
            results.append(
                EIAResult(df=frame[columns], source=geography_result.source, meta=geography_result.meta)
            )
        return _concat_storage_geography_results(results)

    def _eia_underground_storage_by_type(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        storage_types = _normalize_storage_types(filters)
        metric_type = str(filters.get("storage_metric_type") or "working_gas")
        frequency = str(filters.get("storage_frequency") or "monthly")

        if len(storage_types) == 1:
            storage_type = storage_types[0]
            result = self.eia.underground_storage_by_type(
                start=start,
                end=end,
                storage_type=storage_type,
                metric_type=metric_type,
                frequency=frequency,
            )
            frame = result.df.copy() if result.df is not None else pd.DataFrame(columns=["date", "value"])
            if "storage_type" not in frame.columns:
                frame["storage_type"] = storage_type
            frame = frame[["date", "value", "storage_type"]]
            return EIAResult(df=frame, source=result.source, meta=result.meta)

        results = []
        for storage_type in storage_types:
            type_result = self.eia.underground_storage_by_type(
                start=start,
                end=end,
                storage_type=storage_type,
                metric_type=metric_type,
                frequency=frequency,
            )
            frame = type_result.df.copy() if type_result.df is not None else pd.DataFrame(columns=["date", "value"])
            if not frame.empty:
                frame["storage_type"] = storage_type
            results.append(
                EIAResult(df=frame, source=type_result.source, meta=type_result.meta)
            )
        return _concat_storage_type_results(results)

    def _eia_lng_storage(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        states = _normalize_storage_states(
            filters,
            valid_states=EIAAdapter.LNG_STORAGE_STATES,
        )
        metric_type = str(filters.get("storage_metric_type") or "lng_storage_additions")
        frequency = str(filters.get("storage_frequency") or "annual")

        def fetch_for_state(state: str) -> EIAResult:
            if metric_type == "lng_storage_additions":
                return self.eia.lng_storage_additions(
                    start=start,
                    end=end,
                    geography=state,
                    frequency=frequency,
                )
            if metric_type == "lng_storage_withdrawals":
                return self.eia.lng_storage_withdrawals(
                    start=start,
                    end=end,
                    geography=state,
                    frequency=frequency,
                )
            return self.eia.lng_storage_net_withdrawals(
                start=start,
                end=end,
                geography=state,
                frequency=frequency,
            )

        if len(states) == 1:
            state = states[0]
            result = fetch_for_state(state)
            frame = result.df.copy() if result.df is not None else pd.DataFrame(columns=["date", "value", "geography"])
            if "geography" not in frame.columns:
                frame["geography"] = state
            frame["state"] = state
            frame = frame[["date", "value", "geography", "state"]]
            return EIAResult(df=frame, source=result.source, meta=result.meta)

        results = []
        for state in states:
            state_result = fetch_for_state(state)
            frame = state_result.df.copy() if state_result.df is not None else pd.DataFrame(columns=["date", "value", "geography"])
            if not frame.empty:
                if "geography" not in frame.columns:
                    frame["geography"] = state
                frame["state"] = state
            results.append(
                EIAResult(
                    df=frame[["date", "value", "geography", "state"]] if not frame.empty else pd.DataFrame(columns=["date", "value", "geography", "state"]),
                    source=state_result.source,
                    meta=state_result.meta,
                )
            )
        return _concat_storage_geography_results(results)

    def _eia_henry_hub_spot(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.henry_hub_spot(start=start, end=end)

    def _eia_lng_exports(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        # Default LNG exports to LNG totals when no region is explicitly requested.
        region = str(filters.get("region") or "united_states_lng_total")
        return self.eia.lng_exports(start=start, end=end, region=region)

    def _eia_lng_imports(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        region = str(filters.get("region") or "united_states_pipeline_total")
        return self.eia.lng_imports(start=start, end=end, region=region)

    def _eia_ng_electricity(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.ng_electricity(start=start, end=end)

    def _eia_ng_consumption_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        state = str(filters.get("region") or "united_states_total")
        return self.eia.ng_consumption_lower48(start=start, end=end, state=state)

    def _eia_ng_consumption_by_sector(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.ng_consumption_by_sector(start=start, end=end)

    def _eia_ng_production_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        if str(filters.get("group_by") or "") == "region":
            regional_frames: list[pd.DataFrame] = []
            sources: list[str] = []
            region_meta: dict[str, Any] = {}
            for region in sorted(EIAAdapter.PRODUCTION_STATES - {"united_states_total"}):
                regional_result = self.eia.ng_production_lower48(
                    start=start, end=end, state=region
                )
                frame = regional_result.df.copy()
                if frame is None or frame.empty:
                    continue
                frame["region"] = region
                regional_frames.append(frame)
                sources.append(regional_result.source.reference)
                region_meta[region] = (regional_result.meta or {}).get("cache")

            df = (
                pd.concat(regional_frames, ignore_index=True)
                if regional_frames
                else pd.DataFrame(columns=["date", "value", "region"])
            )
            src = SourceRef(
                source_type="eia_api",
                label="EIA Natural Gas: Production by Region/State",
                reference="eia-ng-client:natural_gas.production_by_region",
                parameters={
                    "regions": sorted(EIAAdapter.PRODUCTION_STATES - {"united_states_total"}),
                    "start": start,
                    "end": end,
                    "group_by": "region",
                    "source_references": sources,
                },
            )
            return EIAResult(
                df=df,
                source=src,
                meta={
                    "cache": {"regions": region_meta},
                    "note": "Regional/state production levels for ranking contribution to latest period change.",
                },
            )
        state = str(filters.get("region") or "united_states_total")
        return self.eia.ng_production_lower48(start=start, end=end, state=state)

    def _eia_ng_supply_balance_regime(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        region = str(filters.get("region") or "united_states_total")
        production = self.eia.ng_production_lower48(start=start, end=end, state=region)
        storage_change = self.eia.storage_working_gas_change_weekly(
            start=start, end=end, region="lower48"
        )

        weather = None
        weather_error = None
        try:
            weather = self.eia.weather_degree_days_forecast_vs_5y(
                start=start,
                end=end,
                region="lower48",
                normal_years=int(filters.get("normal_years") or 5),
            )
        except Exception as e:  # noqa: BLE001
            weather_error = str(e)

        prod_df = production.df.copy()
        prod_df["date"] = pd.to_datetime(prod_df["date"], errors="coerce")
        prod_df["value"] = pd.to_numeric(prod_df["value"], errors="coerce")
        prod_df = prod_df.dropna(subset=["date", "value"]).sort_values("date")

        stor_df = storage_change.df.copy()
        stor_df["date"] = pd.to_datetime(stor_df["date"], errors="coerce")
        stor_df["value"] = pd.to_numeric(stor_df["value"], errors="coerce")
        stor_df = stor_df.dropna(subset=["date", "value"]).sort_values("date")

        # Build monthly production trend.
        if not prod_df.empty:
            prod_monthly = prod_df.copy()
            prod_monthly["month"] = prod_monthly["date"].dt.to_period("M")
            prod_monthly = (
                prod_monthly.groupby("month", as_index=False)
                .agg(date=("date", "max"), production_latest=("value", "last"))
                .sort_values("date")
            )
            prod_monthly["production_delta_abs"] = (
                prod_monthly["production_latest"].diff().fillna(0.0)
            )
            prior = prod_monthly["production_latest"].shift(1)
            prod_monthly["production_delta_pct"] = (
                (prod_monthly["production_delta_abs"] / prior.replace({0.0: pd.NA}))
                * 100.0
            ).fillna(0.0)
            prod_monthly = prod_monthly[
                ["month", "date", "production_latest", "production_delta_abs", "production_delta_pct"]
            ]
        else:
            prod_monthly = pd.DataFrame(
                columns=[
                    "month",
                    "date",
                    "production_latest",
                    "production_delta_abs",
                    "production_delta_pct",
                ]
            )

        # Build monthly storage signal from weekly changes (sum across each month).
        if not stor_df.empty:
            stor_monthly = stor_df.copy()
            stor_monthly["month"] = stor_monthly["date"].dt.to_period("M")
            stor_monthly = (
                stor_monthly.groupby("month", as_index=False)
                .agg(date=("date", "max"), storage_weekly_change=("value", "sum"))
                .sort_values("date")
            )
            stor_monthly = stor_monthly[["month", "date", "storage_weekly_change"]]
        else:
            stor_monthly = pd.DataFrame(
                columns=["month", "date", "storage_weekly_change"]
            )

        weather_demand_delta_bcfd = 0.0
        weather_as_of = None
        if weather is not None and weather.df is not None and not weather.df.empty:
            wdf = weather.df.copy()
            wdf["demand_delta_bcfd"] = pd.to_numeric(wdf["demand_delta_bcfd"], errors="coerce")
            wdf = wdf.dropna(subset=["demand_delta_bcfd"])
            if not wdf.empty:
                weather_demand_delta_bcfd = float(wdf["demand_delta_bcfd"].mean())
                weather_as_of = str(wdf.iloc[-1].get("as_of") or "").strip() or None

        # Merge into a monthly regime time series.
        monthly = pd.merge(
            prod_monthly,
            stor_monthly[["month", "storage_weekly_change"]] if not stor_monthly.empty else stor_monthly,
            on="month",
            how="outer",
        )
        if monthly.empty:
            monthly = pd.DataFrame(
                columns=[
                    "month",
                    "date",
                    "production_latest",
                    "production_delta_abs",
                    "production_delta_pct",
                    "storage_weekly_change",
                ]
            )

        monthly["date"] = pd.to_datetime(monthly.get("date"), errors="coerce")
        if monthly["date"].isna().all() and "month" in monthly.columns:
            monthly["date"] = monthly["month"].astype("period[M]").dt.to_timestamp("M")
        else:
            month_fill = monthly["month"].astype("period[M]").dt.to_timestamp("M")
            monthly["date"] = monthly["date"].fillna(month_fill)

        for col in ("production_latest", "production_delta_abs", "production_delta_pct", "storage_weekly_change"):
            monthly[col] = pd.to_numeric(monthly.get(col), errors="coerce")

        monthly["production_delta_abs"] = monthly["production_delta_abs"].fillna(0.0)
        monthly["production_delta_pct"] = monthly["production_delta_pct"].fillna(0.0)
        monthly["storage_weekly_change"] = monthly["storage_weekly_change"].fillna(0.0)
        monthly["weather_demand_delta_bcfd"] = 0.0
        if not monthly.empty:
            latest_idx = monthly["date"].idxmax()
            monthly.loc[latest_idx, "weather_demand_delta_bcfd"] = weather_demand_delta_bcfd

        # Regime score: positive -> expanding/looser, negative -> tightening.
        score = (
            monthly["production_delta_pct"].apply(lambda v: 1.0 if v > 0 else -1.0 if v < 0 else 0.0)
            + monthly["storage_weekly_change"].apply(lambda v: 1.0 if v > 0 else -1.0 if v < 0 else 0.0)
            + monthly["weather_demand_delta_bcfd"].apply(lambda v: -1.0 if v > 0 else 1.0 if v < 0 else 0.0)
        )
        monthly["score"] = score
        monthly["regime"] = monthly["score"].apply(
            lambda v: "expanding" if v >= 1.0 else "tightening" if v <= -1.0 else "mixed"
        )
        monthly["region"] = region
        monthly["weather_as_of"] = weather_as_of
        out = monthly[
            [
                "date",
                "region",
                "regime",
                "score",
                "production_latest",
                "production_delta_abs",
                "production_delta_pct",
                "storage_weekly_change",
                "weather_demand_delta_bcfd",
                "weather_as_of",
            ]
        ].sort_values("date")

        if out.empty:
            out = pd.DataFrame(
                [
                    {
                        "date": datetime.now(timezone.utc).date().isoformat(),
                        "region": region,
                        "regime": "mixed",
                        "score": 0.0,
                        "production_latest": None,
                        "production_delta_abs": 0.0,
                        "production_delta_pct": 0.0,
                        "storage_weekly_change": 0.0,
                        "weather_demand_delta_bcfd": round(weather_demand_delta_bcfd, 3),
                        "weather_as_of": weather_as_of,
                    }
                ]
            )

        return EIAResult(
            df=out,
            source=SourceRef(
                source_type="eia_api",
                label="U.S. Natural Gas Supply Balance Regime (Derived)",
                reference="eia-ng-client:derived_natural_gas.supply_balance_regime",
                parameters={
                    "region": region,
                    "start": start,
                    "end": end,
                    "production_reference": production.source.reference,
                    "storage_change_reference": storage_change.source.reference,
                    "weather_reference": (
                        weather.source.reference if weather is not None else None
                    ),
                },
            ),
            meta={
                "cache": {
                    "production": (production.meta or {}).get("cache"),
                    "storage_change": (storage_change.meta or {}).get("cache"),
                    "weather": (weather.meta or {}).get("cache") if weather is not None else None,
                },
                "weather_error": weather_error,
                "note": "Derived regime blends production trend, latest storage change, and weather-driven demand pressure.",
            },
        )

    def _eia_ng_exploration_reserves_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        state = str(filters.get("region") or "all")
        resource_category = str(
            filters.get("resource_category") or "proved_associated_gas"
        )
        return self.eia.ng_exploration_reserves_lower48(
            start=start,
            end=end,
            state=state,
            resource_category=resource_category,
        )

    def _eia_ng_pipeline(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        dataset = str(filters.get("dataset") or "natural_gas_pipeline_projects")
        return self.eia.ng_pipeline(start=start, end=end, dataset=dataset)

    def _eia_weather_degree_days_forecast_vs_5y(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        region = str(filters.get("region") or "lower48")
        normal_years = int(filters.get("normal_years") or 5)
        return self.eia.weather_degree_days_forecast_vs_5y(
            start=start,
            end=end,
            region=region,
            normal_years=normal_years,
        )

    def _eia_weather_regional_demand_drivers(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        normal_years = int(filters.get("normal_years") or 5)
        regions = ["east", "midwest", "south", "west"]

        rows: list[dict[str, Any]] = []
        source_refs: dict[str, str] = {}
        region_meta: dict[str, Any] = {}
        for region in regions:
            regional = self.eia.weather_degree_days_forecast_vs_5y(
                start=start,
                end=end,
                region=region,
                normal_years=normal_years,
            )
            source_refs[region] = regional.source.reference
            region_meta[region] = (regional.meta or {}).get("cache")
            rdf = regional.df.copy()
            if rdf is None or rdf.empty:
                continue
            rdf["demand_delta_bcfd"] = pd.to_numeric(rdf["demand_delta_bcfd"], errors="coerce")
            rdf["delta_hdd"] = pd.to_numeric(rdf["delta_hdd"], errors="coerce")
            rdf["delta_cdd"] = pd.to_numeric(rdf["delta_cdd"], errors="coerce")
            rdf = rdf.dropna(subset=["demand_delta_bcfd"])
            if rdf.empty:
                continue
            avg_demand_delta = float(rdf["demand_delta_bcfd"].mean())
            total_hdd = float(pd.to_numeric(rdf["delta_hdd"], errors="coerce").sum())
            total_cdd = float(pd.to_numeric(rdf["delta_cdd"], errors="coerce").sum())
            as_of = str(rdf.iloc[-1].get("as_of") or "").strip() or None
            rows.append(
                {
                    "date": as_of,
                    "region": region,
                    "demand_delta_bcfd": round(avg_demand_delta, 3),
                    "total_delta_hdd": round(total_hdd, 3),
                    "total_delta_cdd": round(total_cdd, 3),
                    "normal_years": normal_years,
                }
            )

        out = pd.DataFrame(
            rows,
            columns=[
                "date",
                "region",
                "demand_delta_bcfd",
                "total_delta_hdd",
                "total_delta_cdd",
                "normal_years",
            ],
        )

        if not out.empty:
            out["abs_demand_delta_bcfd"] = out["demand_delta_bcfd"].abs()
            out = out.sort_values("abs_demand_delta_bcfd", ascending=False).reset_index(drop=True)
            out = out.drop(columns=["abs_demand_delta_bcfd"])

        return EIAResult(
            df=out,
            source=SourceRef(
                source_type="eia_api",
                label="Weather Regional Demand Drivers (Derived)",
                reference="open-meteo:degree_days.regional_drivers",
                parameters={
                    "regions": regions,
                    "start": start,
                    "end": end,
                    "normal_years": normal_years,
                    "source_references": source_refs,
                },
            ),
            meta={
                "cache": {"regions": region_meta},
                "note": "Derived by ranking regional weather-demand deltas from East/Midwest/South/West.",
            },
        )

    @staticmethod
    def _latest_with_delta(df: pd.DataFrame | None) -> tuple[Any, float | None, float | None]:
        if df is None or df.empty or "date" not in df.columns or "value" not in df.columns:
            return None, None, None
        ordered = df.copy()
        ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
        ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
        ordered = ordered.dropna(subset=["date", "value"]).sort_values("date")
        if ordered.empty:
            return None, None, None
        latest_row = ordered.iloc[-1]
        latest_date = latest_row["date"].date().isoformat()
        latest_value = float(latest_row["value"])
        if len(ordered) < 2:
            return latest_date, latest_value, None
        prior_value = float(ordered.iloc[-2]["value"])
        return latest_date, latest_value, latest_value - prior_value

    @staticmethod
    def _storage_surprise_vs_recent_average(
        df: pd.DataFrame | None, lookback_weeks: int = 5
    ) -> tuple[float | None, float | None, float | None]:
        if df is None or df.empty or "date" not in df.columns or "value" not in df.columns:
            return None, None, None
        ordered = df.copy()
        ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
        ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
        ordered = ordered.dropna(subset=["date", "value"]).sort_values("date")
        if ordered.empty:
            return None, None, None
        values = ordered["value"].tolist()
        latest = float(values[-1])
        if len(values) < 2:
            return latest, None, None
        recent_prior = values[-(lookback_weeks + 1) : -1]
        if not recent_prior:
            return latest, None, None
        expected = float(pd.Series(recent_prior).mean())
        return latest, expected, latest - expected

    def _eia_weekly_energy_atlas_summary(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        del filters

        weather = self.eia.weather_degree_days_forecast_vs_5y(
            start=start,
            end=end,
            region="lower48",
            normal_years=5,
        )
        storage_change = self.eia.storage_working_gas_change_weekly(
            start=start,
            end=end,
            region="lower48",
        )
        lng_exports = self.eia.lng_exports(
            start=start,
            end=end,
            region="united_states_lng_total",
        )
        production = self.eia.ng_production_lower48(
            start=start,
            end=end,
            state="united_states_total",
        )
        price = self.eia.henry_hub_spot(start=start, end=end)

        weather_df = weather.df.copy() if weather.df is not None else pd.DataFrame()
        weather_demand_delta = None
        weather_delta_hdd = None
        weather_delta_cdd = None
        weather_as_of = None
        if not weather_df.empty:
            weather_df["demand_delta_bcfd"] = pd.to_numeric(
                weather_df.get("demand_delta_bcfd"), errors="coerce"
            )
            weather_df["delta_hdd"] = pd.to_numeric(weather_df.get("delta_hdd"), errors="coerce")
            weather_df["delta_cdd"] = pd.to_numeric(weather_df.get("delta_cdd"), errors="coerce")
            weather_demand_delta = float(weather_df["demand_delta_bcfd"].mean())
            weather_delta_hdd = float(weather_df["delta_hdd"].sum())
            weather_delta_cdd = float(weather_df["delta_cdd"].sum())
            weather_as_of = str(weather_df.iloc[-1].get("as_of") or "").strip() or None

        storage_latest, storage_expected, storage_surprise = (
            self._storage_surprise_vs_recent_average(storage_change.df)
        )
        _, lng_latest, lng_delta = self._latest_with_delta(lng_exports.df)
        _, production_latest, production_delta = self._latest_with_delta(production.df)
        price_date, price_latest, price_delta = self._latest_with_delta(price.df)

        summary_date = price_date or end
        out = pd.DataFrame(
            [
                {
                    "date": summary_date,
                    "weather_as_of": weather_as_of,
                    "weather_demand_delta_bcfd": weather_demand_delta,
                    "weather_delta_hdd": weather_delta_hdd,
                    "weather_delta_cdd": weather_delta_cdd,
                    "storage_latest_bcf": storage_latest,
                    "storage_expected_bcf": storage_expected,
                    "storage_surprise_bcf": storage_surprise,
                    "lng_latest_mmcf": lng_latest,
                    "lng_delta_mmcf": lng_delta,
                    "production_latest_mmcf": production_latest,
                    "production_delta_mmcf": production_delta,
                    "price_latest_usd_mmbtu": price_latest,
                    "price_delta_usd_mmbtu": price_delta,
                }
            ]
        )

        return EIAResult(
            df=out,
            source=SourceRef(
                source_type="eia_api",
                label="Energy Atlas Weekly Summary (Derived)",
                reference="eia-ng-client:derived_natural_gas.weekly_energy_atlas_summary",
                parameters={
                    "start": start,
                    "end": end,
                    "weather_reference": weather.source.reference,
                    "storage_change_reference": storage_change.source.reference,
                    "lng_exports_reference": lng_exports.source.reference,
                    "production_reference": production.source.reference,
                    "price_reference": price.source.reference,
                    "storage_expectation_proxy": "recent_5_week_average",
                },
            ),
            meta={
                "cache": {
                    "weather": (weather.meta or {}).get("cache"),
                    "storage_change": (storage_change.meta or {}).get("cache"),
                    "lng_exports": (lng_exports.meta or {}).get("cache"),
                    "production": (production.meta or {}).get("cache"),
                    "price": (price.meta or {}).get("cache"),
                },
                "note": (
                    "Derived weekly summary combining weather-driven demand impact, storage surprise vs "
                    "recent average proxy, LNG/supply shifts, and Henry Hub price result."
                ),
            },
        )
