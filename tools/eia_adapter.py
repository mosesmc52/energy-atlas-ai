# atlas/tools/eia_adapter.py
from __future__ import annotations

import os
import re
from io import StringIO
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from eia_ng import EIAClient
from schemas.answer import SourceRef
from tools.cache_base import CacheBackedTimeseriesAdapterBase
from tools.forecasting import ForecastResult, forecast_linear_trend

load_dotenv()
DEBUG_ENABLED = os.getenv("ATLAS_DEBUG", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


@dataclass(frozen=True)
class EIAResult:
    df: pd.DataFrame
    source: SourceRef
    meta: Dict[str, Any] | None = (
        None  # optional extra metadata (units, frequency, notes)
    )


class EIAAdapter(CacheBackedTimeseriesAdapterBase):
    """
    Thin adapter around your eia-ng-client package.

    Responsibilities:
      - Call library methods
      - Normalize outputs to a consistent DataFrame shape
      - Attach provenance (SourceRef) for auditability
      - (Optional) attach meta like units/frequency
    """

    STORAGE_REGIONS = {
        "lower48",
        "east",
        "midwest",
        "south_central",
        "mountain",
        "pacific",
    }
    TRADE_REGIONS = {
        "united_states_pipeline_total",
        "canada_pipeline",
        "mexico_pipeline",
    }
    IMPORT_REGIONS = {
        "canada_pipeline",
        "mexico_pipeline",
        "united_states_pipeline_total",
        "canada_compressed",
        "united_states_compressed_total",
        "algeria",
        "australia",
        "brunei",
        "egypt",
        "equatorial_guinea",
        "france",
        "indonesia",
        "jamaica",
        "malaysia",
        "nigeria",
        "norway",
        "oman",
        "peru",
        "qatar",
        "trinidad_and_tobago",
        "united_arab_emirates",
        "united_kingdom",
        "yemen",
    }
    EXPORT_REGIONS = {
        "canada_pipeline",
        "mexico_pipeline",
        "united_states_pipeline_total",
        "united_states_lng_total",
        "canada_truck",
        "mexico_truck",
        "united_states_truck_total",
        "canada_compressed",
        "united_states_compressed_total",
        "argentina",
        "australia",
        "bahrain",
        "bangladesh",
        "barbados",
        "belgium",
        "brazil",
        "chile",
        "china",
        "colombia",
        "croatia",
        "dominican_republic",
        "egypt",
        "el_salvador",
        "finland",
        "france",
        "germany",
        "greece",
        "haiti",
        "india",
        "indonesia",
        "israel",
        "italy",
        "jamaica",
        "japan",
        "jordan",
        "kuwait",
        "lithuania",
        "malta",
        "mauritania",
        "mexico",
        "netherlands",
        "nicaragua",
        "pakistan",
        "panama",
        "philippines",
        "poland",
        "portugal",
        "russia",
        "senegal",
        "singapore",
        "south_korea",
        "spain",
        "taiwan",
        "thailand",
        "turkiye",
        "united_arab_emirates",
        "united_kingdom",
    }
    CONSUMPTION_STATES = {
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
        "hi",
        "id",
        "il",
        "in",
        "ia",
        "ks",
        "ky",
        "la",
        "me",
        "md",
        "ma",
        "mi",
        "mn",
        "ms",
        "mo",
        "mt",
        "ne",
        "nv",
        "nh",
        "nj",
        "nm",
        "ny",
        "nc",
        "nd",
        "oh",
        "ok",
        "or",
        "pa",
        "ri",
        "sc",
        "sd",
        "tn",
        "tx",
        "ut",
        "vt",
        "va",
        "wa",
        "wv",
        "wi",
        "wy",
        "united_states_total",
    }
    PRODUCTION_STATES = {
        "al",
        "ak",
        "az",
        "ar",
        "ca",
        "co",
        "fl",
        "il",
        "in",
        "ks",
        "ky",
        "la",
        "md",
        "mi",
        "mo",
        "ms",
        "mt",
        "ne",
        "nv",
        "nm",
        "ny",
        "nd",
        "oh",
        "ok",
        "or",
        "pa",
        "sd",
        "tn",
        "tx",
        "ut",
        "va",
        "wv",
        "united_states_total",
    }
    RESERVES_STATES = {
        "al",
        "ak",
        "ar",
        "ca",
        "co",
        "fl",
        "ks",
        "ky",
        "la",
        "mi",
        "ms",
        "mt",
        "nd",
        "nm",
        "ny",
        "oh",
        "ok",
        "pa",
        "tx",
        "ut",
        "va",
        "wv",
        "wy",
        "us",
        "all",
    }
    RESERVES_RESOURCE_CATEGORIES = {
        "proved_associated_gas",
        "proved_nonassociated_gas",
        "proved_ngl",
        "expected_future_gas_production",
    }
    WEATHER_REQUIRED_COLUMNS = {
        "region_id",
        "date",
        "n_stations_used",
        "tavg_c_median",
        "tavg_f_median",
        "hdd_median",
        "tavg_c_mean",
        "tavg_f_mean",
        "hdd_mean",
    }
    WEATHER_METRICS = {
        "hdd_mean",
        "hdd_median",
        "cdd_mean",
        "cdd_median",
        "tavg_f_mean",
        "tavg_f_median",
        "tavg_c_mean",
        "tavg_c_median",
    }
    WEATHER_REGION_COORDS = {
        "east": (40.7128, -74.0060),  # New York
        "midwest": (41.8781, -87.6298),  # Chicago
        "south": (32.7767, -96.7970),  # Dallas
        "west": (34.0522, -118.2437),  # Los Angeles
    }
    WEATHER_REGION_WEIGHTS = {
        "east": 0.173,
        "midwest": 0.208,
        "south": 0.383,
        "west": 0.236,
    }
    NG_CONSUMPTION_SECTOR_SERIES = {
        "residential": "N3010US2",
        "commercial": "N3020US2",
        "industrial": "N3035US2",
        "electric_power": "N3045US2",
    }
    NG_CONSUMPTION_SECTOR_DNAV_URLS = {
        "residential": "https://www.eia.gov/dnav/ng/hist/n3010us2m.htm",
        "commercial": "https://www.eia.gov/dnav/ng/hist/n3020us2m.htm",
        "industrial": "https://www.eia.gov/dnav/ng/hist/n3035us2m.htm",
        "electric_power": "https://www.eia.gov/dnav/ng/hist/n3045us2m.htm",
    }
    PIPELINE_DATASET_FILES = {
        "historical_projects": "Historical_Projects_1996-2024.parquet",
        "inflow_by_region": "Inflow_By_Region.parquet",
        "inflow_by_state": "Inflow_By_State.parquet",
        "inflow_single_year": "InFlow_Single_Year.parquet",
        "major_pipeline_summary": "Major_Pipeline_Summary.parquet",
        "major_pipeline_sumamry": "Major_Pipeline_Summary.parquet",
        "natural_gas_pipeline_projects": "Natural_Gas_Pipeline_Projects.parquet",
        "outflow_by_region": "Outflow_By_Region.parquet",
        "outflow_by_state": "Outflow_By_State.parquet",
        "pipeline_state2_state_capacity": "Pipeline_State2State_Capacity.parquet",
    }
    PIPELINE_LABELS = {
        "historical_projects": "Historical Projects (1996-2024)",
        "inflow_by_region": "Inflow By Region",
        "inflow_by_state": "Inflow By State",
        "inflow_single_year": "Inflow Single Year",
        "major_pipeline_summary": "Major Pipeline Summary",
        "natural_gas_pipeline_projects": "Natural Gas Pipeline Projects",
        "outflow_by_region": "Outflow By Region",
        "outflow_by_state": "Outflow By State",
        "pipeline_state2_state_capacity": "Pipeline State-To-State Capacity",
    }
    PIPELINE_WIDE_DATASETS = {
        "inflow_by_region": ["Region To", "Region From"],
        "inflow_by_state": ["State To", "State From"],
        "major_pipeline_summary": ["Pipeline", "Segment", "State From", "State To"],
        "outflow_by_region": ["Region From", "Region To"],
        "outflow_by_state": ["State From", "State To"],
    }
    PIPELINE_PROJECT_DATASETS = {
        "historical_projects",
        "natural_gas_pipeline_projects",
    }

    def __init__(
        self,
        cache_dir: str = "data/cache/eia",
        api_key: str | None = None,
        weather_csv_path: str | Path | None = None,
    ):
        super().__init__(
            cache_dir=cache_dir,
            date_col="date",
            enable_debug_timing=DEBUG_ENABLED,
        )
        self.client = EIAClient(api_key=api_key or os.getenv("EIA_API_KEY"))
        repo_root = Path(__file__).resolve().parents[1]
        resolved_weather_path: Path | None = None
        if weather_csv_path is not None:
            configured_path = Path(weather_csv_path).expanduser()
            if configured_path.is_absolute():
                resolved_weather_path = configured_path
            else:
                # Support launching from non-repo working directories by resolving
                # relative paths against the repository root as a fallback.
                repo_relative_path = repo_root / configured_path
                resolved_weather_path = (
                    configured_path
                    if configured_path.exists()
                    else repo_relative_path
                )
        else:
            relative_candidates = [
                Path("data/raw/noaa/regional/daily_region_weather.csv"),
                Path("data/raw/noaa/regional/lower_48_region_daily.csv"),
            ]
            candidates = relative_candidates + [repo_root / c for c in relative_candidates]
            for candidate in candidates:
                if candidate.exists():
                    resolved_weather_path = candidate
                    break
            if resolved_weather_path is None:
                # Keep a deterministic default so downstream errors are explicit
                # (missing file) rather than ambiguous (path not configured).
                resolved_weather_path = repo_root / relative_candidates[0]
        self.weather_csv_path = resolved_weather_path
        self._weather_df_cache: pd.DataFrame | None = None

    # ----------------------------
    # Public methods (router calls these)
    # ----------------------------

    def forecast_result(
        self,
        result: EIAResult,
        *,
        metric: str,
        horizon_days: int = 7,
        lookback_observations: int = 30,
        include_overlay: bool = False,
    ) -> ForecastResult:
        return forecast_linear_trend(
            result.df,
            metric=metric,
            horizon_days=horizon_days,
            lookback_observations=lookback_observations,
            include_overlay=include_overlay,
            source_reference=result.source.reference,
        )

    def storage_working_gas(
        self, start: str, end: str, region: str = "lower48"
    ) -> EIAResult:
        """
        Working gas in storage (weekly), optionally by region.
        Cache-first: load CSV, fetch missing edges via eia-ng, save, return window.
        """
        if region not in self.STORAGE_REGIONS:
            raise ValueError(
                f"Invalid storage region '{region}'. Expected one of: {sorted(self.STORAGE_REGIONS)}"
            )

        df, cache_info = self._cached_timeseries(
            metric_key="working_gas_storage_lower48",
            start=start,
            end=end,
            cache_key_parts={"region": region},
            fetch_ctx={"_fetch": "storage_working_gas", "region": region},
            allow_internal_gap_fill_daily=True,  # weekly series: edge fill is safer initially
            expected_calendar="B",
        )

        src = self._make_source(
            label=f"EIA Natural Gas Storage: Working Gas ({region.replace('_', ' ').title()})",
            reference="eia-ng-client:natural_gas.storage",
            parameters={
                "region": region,
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,  # <-- include cache behavior
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def storage_working_gas_lower48(self, start: str, end: str) -> EIAResult:
        return self.storage_working_gas(start=start, end=end, region="lower48")

    def storage_working_gas_change_weekly(
        self, start: str, end: str, region: str = "lower48"
    ) -> EIAResult:
        """
        Weekly change in working gas storage, derived from weekly storage levels.
        v1 uses simple row-to-row difference on the weekly series.
        """
        base = self.storage_working_gas(start=start, end=end, region=region)
        df = base.df.copy()

        if df is None or df.empty:
            out = pd.DataFrame(columns=["date", "value"])
        else:
            out = df[["date", "value"]].copy()
            out["value"] = pd.to_numeric(out["value"], errors="coerce").diff()
            out = out.dropna(subset=["date", "value"]).reset_index(drop=True)

        src = self._make_source(
            label=f"EIA Natural Gas Storage: Weekly Change ({region.replace('_', ' ').title()})",
            reference="eia-ng-client:derived_natural_gas.storage_change_weekly",
            parameters={
                "region": region,
                "start": start,
                "end": end,
                "source_storage_reference": base.source.reference,
            },
        )
        meta = {
            "cache": (base.meta or {}).get("cache"),
            "note": "Derived as row-to-row weekly difference of working gas storage levels.",
        }
        return EIAResult(df=out, source=src, meta=meta)

    def get_weather_metric(
        self,
        *,
        region_id: str,
        start: str,
        end: str,
        metric: str,
    ) -> pd.DataFrame:
        """
        Generic weather metric getter backed by the configured weather CSV.
        """
        if metric not in self.WEATHER_METRICS:
            raise ValueError(
                f"Invalid weather metric '{metric}'. Expected one of: {sorted(self.WEATHER_METRICS)}"
            )

        df = self._weather_timeseries(
            region_id=region_id,
            start=start,
            end=end,
            value_columns=[metric],
        )
        return df.rename(columns={metric: "value"})

    def get_weather_hdd(
        self,
        *,
        region_id: str,
        start: str,
        end: str,
        method: str = "mean",
    ) -> pd.DataFrame:
        """
        Daily heating degree days for a region.
        Returns columns: date, region_id, hdd.
        """
        metric_map = {
            "mean": "hdd_mean",
            "median": "hdd_median",
        }
        if method not in metric_map:
            raise ValueError(
                "Invalid method for HDD. Expected one of: ['mean', 'median']"
            )

        col = metric_map[method]
        df = self._weather_timeseries(
            region_id=region_id,
            start=start,
            end=end,
            value_columns=[col],
        )
        return df.rename(columns={col: "hdd"})

    def get_weather_tavg(
        self,
        *,
        region_id: str,
        start: str,
        end: str,
        unit: str = "f",
        method: str = "mean",
    ) -> pd.DataFrame:
        """
        Daily average temperature for a region.
        Returns columns: date, region_id, tavg.
        """
        metric_map = {
            ("f", "mean"): "tavg_f_mean",
            ("f", "median"): "tavg_f_median",
            ("c", "mean"): "tavg_c_mean",
            ("c", "median"): "tavg_c_median",
        }
        key = (unit, method)
        if key not in metric_map:
            raise ValueError(
                "Invalid unit/method combination for tavg. "
                "Expected unit in ['f','c'] and method in ['mean','median']."
            )

        col = metric_map[key]
        df = self._weather_timeseries(
            region_id=region_id,
            start=start,
            end=end,
            value_columns=[col],
        )
        return df.rename(columns={col: "tavg"})

    def weather_degree_days_forecast_vs_5y(
        self,
        *,
        start: str,
        end: str,
        region: str = "lower48",
        normal_years: int = 5,
    ) -> EIAResult:
        """
        Build 1-5 / 6-10 / 11-15 day HDD/CDD forecast anomalies versus rolling
        5-year normals using the configured weather history and live forecast data.
        """
        canonical_region = self._canonical_weather_region(region)
        if normal_years not in {1, 2, 3, 5}:
            raise ValueError("normal_years must be one of: 1, 2, 3, 5")
        forecast_df, forecast_as_of = self._fetch_open_meteo_degree_day_forecast()
        if canonical_region != "lower_48":
            forecast_df = forecast_df.loc[
                forecast_df["region_id"] == canonical_region
            ].copy()
        else:
            forecast_df = forecast_df.loc[
                forecast_df["region_id"] == "lower_48"
            ].copy()

        if forecast_df.empty:
            raise RuntimeError("No weather forecast data was returned for requested region.")

        history = self._load_weather_csv().copy()
        history["region_id"] = history["region_id"].astype(str)
        history = history.loc[history["region_id"] == canonical_region].copy()
        if history.empty:
            raise RuntimeError(
                f"No historical weather data available for region '{canonical_region}'."
            )

        rows: list[dict[str, Any]] = []
        for bucket_start, bucket_end, bucket_label in (
            (1, 5, "days_1_5"),
            (6, 10, "days_6_10"),
            (11, 15, "days_11_15"),
        ):
            bucket_forecast = self._bucket_degree_days(
                forecast_df, start_day=bucket_start, end_day=bucket_end
            )
            if bucket_forecast is None:
                continue
            bucket_dates = bucket_forecast["dates"]
            hdd_forecast = float(bucket_forecast["hdd"])
            cdd_forecast = float(bucket_forecast["cdd"])

            historical_bucket_values: list[tuple[float, float]] = []
            for years_back in range(1, normal_years + 1):
                shifted_dates = [self._shift_date_back_n_years(d, years_back) for d in bucket_dates]
                hist_bucket = history.loc[history["date"].isin(shifted_dates)].copy()
                if hist_bucket.empty:
                    continue
                hdd_hist = float(pd.to_numeric(hist_bucket["hdd_mean"], errors="coerce").sum())
                cdd_col = "cdd_mean" if "cdd_mean" in hist_bucket.columns else None
                if cdd_col is None:
                    tavg_f = pd.to_numeric(hist_bucket["tavg_f_mean"], errors="coerce")
                    cdd_hist = float((tavg_f - 65.0).clip(lower=0.0).sum())
                else:
                    cdd_hist = float(pd.to_numeric(hist_bucket[cdd_col], errors="coerce").sum())
                historical_bucket_values.append((hdd_hist, cdd_hist))

            if not historical_bucket_values:
                continue

            hdd_normal = sum(v[0] for v in historical_bucket_values) / len(historical_bucket_values)
            cdd_normal = sum(v[1] for v in historical_bucket_values) / len(historical_bucket_values)
            delta_hdd = hdd_forecast - hdd_normal
            delta_cdd = cdd_forecast - cdd_normal

            bucket_days = int(bucket_end - bucket_start + 1)
            demand_delta_bcfd = self._estimate_gas_demand_delta_bcfd(
                delta_hdd=delta_hdd,
                delta_cdd=delta_cdd,
                days=bucket_days,
                region_id=canonical_region,
            )

            rows.append(
                {
                    "date": bucket_dates[-1],
                    "region_id": canonical_region,
                    "bucket": bucket_label,
                    "bucket_start_day": bucket_start,
                    "bucket_end_day": bucket_end,
                    "forecast_hdd": round(hdd_forecast, 2),
                    "normal_hdd_5y": round(hdd_normal, 2),
                    "delta_hdd": round(delta_hdd, 2),
                    "forecast_cdd": round(cdd_forecast, 2),
                    "normal_cdd_5y": round(cdd_normal, 2),
                    "delta_cdd": round(delta_cdd, 2),
                    "demand_delta_bcfd": round(demand_delta_bcfd, 3),
                    "normal_years": normal_years,
                    "as_of": forecast_as_of,
                }
            )

        out = pd.DataFrame(
            rows,
            columns=[
                "date",
                "region_id",
                "bucket",
                "bucket_start_day",
                "bucket_end_day",
                "forecast_hdd",
                "normal_hdd_5y",
                "delta_hdd",
                "forecast_cdd",
                "normal_cdd_5y",
                "delta_cdd",
                "demand_delta_bcfd",
                "normal_years",
                "as_of",
            ],
        )

        src = self._make_source(
            label=f"Weather Degree Days Forecast vs 5-Year Normal ({canonical_region.replace('_', ' ').title()})",
            reference="open-meteo:degree_days.forecast_vs_5y",
            parameters={
                "region": canonical_region,
                "start": start,
                "end": end,
                "forecast_provider": "open_meteo",
                "forecast_as_of": forecast_as_of,
                "buckets": ["days_1_5", "days_6_10", "days_11_15"],
                "normal_years": normal_years,
            },
        )
        return EIAResult(df=out, source=src, meta={"forecast_as_of": forecast_as_of})

    def henry_hub_spot(self, start: str, end: str) -> EIAResult:
        """
        Henry Hub spot price (daily).
        Cache-first: load CSV, fill internal daily gaps via eia-ng, save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="henry_hub_spot",
            start=start,
            end=end,
            cache_key_parts={},  # add facets if you later support variants
            fetch_ctx={"_fetch": "henry_hub_spot"},
            allow_internal_gap_fill_daily=True,
            expected_calendar="B",
        )

        src = self._make_source(
            label="EIA Natural Gas Price: Henry Hub Spot",
            reference="eia-ng-client:natural_gas.spot_prices",
            parameters={
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def lng_exports(
        self, start: str, end: str, region: str = "united_states_pipeline_total"
    ) -> EIAResult:
        """
        Natural gas exports (canonical series), optionally by trade region.
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        if region not in self.EXPORT_REGIONS:
            raise ValueError(
                f"Invalid export region '{region}'. Expected one of: {sorted(self.EXPORT_REGIONS)}"
            )

        df, cache_info = self._cached_timeseries(
            metric_key="lng_exports",
            start=start,
            end=end,
            cache_key_parts={"region": region},
            fetch_ctx={"_fetch": "lng_exports", "region": region},
            allow_internal_gap_fill_daily=False,
            expected_calendar="M",
        )

        src = self._make_source(
            label=f"EIA Natural Gas: Exports ({region.replace('_', ' ').title()})",
            reference="eia-ng-client:natural_gas.exports",
            parameters={
                "region": region,
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def lng_imports(
        self, start: str, end: str, region: str = "united_states_pipeline_total"
    ) -> EIAResult:
        """
        Natural gas imports (canonical series), optionally by trade region.
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        if region not in self.IMPORT_REGIONS:
            raise ValueError(
                f"Invalid import region '{region}'. Expected one of: {sorted(self.IMPORT_REGIONS)}"
            )

        df, cache_info = self._cached_timeseries(
            metric_key="lng_imports",
            start=start,
            end=end,
            cache_key_parts={"region": region},
            fetch_ctx={"_fetch": "lng_imports", "region": region},
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="M",
        )

        src = self._make_source(
            label=f"EIA Natural Gas: Imports ({region.replace('_', ' ').title()})",
            reference="eia-ng-client:natural_gas.imports",
            parameters={
                "region": region,
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_electricity(self, start: str, end: str) -> EIAResult:
        """
        NG Electricity (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="ng_electricity",
            start=start,
            end=end,
            cache_key_parts={},  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "ng_electricity"},
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="M",
        )

        src = self._make_source(
            label="EIA Natural Gas: Electricity",
            reference="eia-ng-client:electricity.generation_natural_gas",
            parameters={
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_consumption_lower48(
        self, start: str, end: str, state: str = "united_states_total"
    ) -> EIAResult:
        """
        NG Consumption (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        if state not in self.CONSUMPTION_STATES:
            raise ValueError(
                f"Invalid consumption state '{state}'. Expected one of: {sorted(self.CONSUMPTION_STATES)}"
            )

        df, cache_info = self._cached_timeseries(
            metric_key="consumption",
            start=start,
            end=end,
            cache_key_parts={"region": state},
            fetch_ctx={"_fetch": "ng_consumption", "state": state},
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="M",
        )

        src = self._make_source(
            label="EIA Natural Gas: Consumption",
            reference="eia-ng-client:natural_gas.consumption",
            parameters={
                "start": start,
                "end": end,
                "state": state,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_consumption_by_sector(self, start: str, end: str) -> EIAResult:
        """
        U.S. natural gas consumption by end-use sector (monthly).
        Returns long-form rows with columns: date, value, series.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="ng_consumption_by_sector",
            start=start,
            end=end,
            cache_key_parts={},
            fetch_ctx={"_fetch": "ng_consumption_by_sector"},
            allow_internal_gap_fill_daily=False,
            expected_calendar="M",
        )

        src = self._make_source(
            label="EIA Natural Gas: Consumption by Sector",
            reference="eia-ng-client:natural_gas.consumption_by_sector",
            parameters={
                "start": start,
                "end": end,
                "sectors": sorted(self.NG_CONSUMPTION_SECTOR_SERIES.keys()),
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_production_lower48(
        self, start: str, end: str, state: str = "united_states_total"
    ) -> EIAResult:
        """
        NG Production (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        if state not in self.PRODUCTION_STATES:
            raise ValueError(
                f"Invalid production state '{state}'. Expected one of: {sorted(self.PRODUCTION_STATES)}"
            )

        df, cache_info = self._cached_timeseries(
            metric_key="ng_production",
            start=start,
            end=end,
            cache_key_parts={"region": state},
            fetch_ctx={"_fetch": "ng_production", "state": state},
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="M",
        )

        src = self._make_source(
            label=f"EIA Natural Gas: Production ({state.replace('_', ' ').upper() if len(state) == 2 else state.replace('_', ' ').title()})",
            reference="eia-ng-client:natural_gas.production",
            parameters={
                "start": start,
                "end": end,
                "state": state,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_exploration_reserves_lower48(
        self,
        start: str,
        end: str,
        state: str = "all",
        resource_category: str = "proved_associated_gas",
    ) -> EIAResult:
        """
        NG Exploration (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        if state not in self.RESERVES_STATES:
            raise ValueError(
                f"Invalid reserves state '{state}'. Expected one of: {sorted(self.RESERVES_STATES)}"
            )
        if resource_category not in self.RESERVES_RESOURCE_CATEGORIES:
            raise ValueError(
                "Invalid reserves resource category "
                f"'{resource_category}'. Expected one of: {sorted(self.RESERVES_RESOURCE_CATEGORIES)}"
            )

        df, cache_info = self._cached_timeseries(
            metric_key="ng_exploration_reserves",
            start=start,
            end=end,
            cache_key_parts={
                "state": state,
                "resource_category": resource_category,
            },
            fetch_ctx={
                "_fetch": "ng_exploration_reserves",
                "state": state,
                "resource_category": resource_category,
            },
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="A",
        )

        src = self._make_source(
            label="EIA Natural Gas: Exploration And Reserves",
            reference="eia-ng-client:natural_gas.exploration_and_reserves",
            parameters={
                "start": start,
                "end": end,
                "state": state,
                "resource_category": resource_category,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_pipeline(
        self,
        start: str,
        end: str,
        dataset: str = "natural_gas_pipeline_projects",
    ) -> EIAResult:
        dataset_key = self._canonical_pipeline_dataset(dataset)
        dataset_file = self.PIPELINE_DATASET_FILES[dataset]
        dataset_path = (
            Path(__file__).resolve().parent.parent
            / "data"
            / "processed"
            / "eia"
            / "ng"
            / "pipeline"
            / dataset_file
        )
        if not dataset_path.exists():
            raise FileNotFoundError(
                f"Pipeline parquet dataset not found at: {dataset_path}"
            )

        try:
            raw = pd.read_parquet(dataset_path)
        except ImportError as exc:
            raise ImportError(
                "Reading pipeline parquet data requires 'pyarrow' or 'fastparquet'. "
                "This project declares pyarrow in pyproject.toml; install dependencies "
                "for the runtime environment before using ng_pipeline."
            ) from exc

        df = self._normalize_pipeline_df(
            raw,
            dataset=dataset_key,
            start=start,
            end=end,
        )

        src = self._make_source(
            label=f"EIA Natural Gas Pipeline: {self.PIPELINE_LABELS[dataset_key]}",
            reference=f"local-parquet:eia.ng.pipeline.{dataset_key}",
            parameters={
                "dataset": dataset_key,
                "start": start,
                "end": end,
                "path": str(dataset_path),
            },
        )
        meta = {
            "dataset": dataset_key,
            "dataset_path": str(dataset_path),
            "row_count": int(len(df)),
        }
        return EIAResult(df=df, source=src, meta=meta)

    # ----------------------------
    # Library calling + normalization helpers
    # ----------------------------

    def _load_weather_csv(self) -> pd.DataFrame:
        """
        Load weather CSV once, validate schema, parse dates, and coerce numeric columns.
        """
        if self._weather_df_cache is not None:
            return self._weather_df_cache

        if self.weather_csv_path is None:
            raise FileNotFoundError(
                "Weather CSV path is not configured. Pass weather_csv_path to EIAAdapter."
            )
        if not self.weather_csv_path.exists():
            raise FileNotFoundError(
                f"Weather CSV file not found at: {self.weather_csv_path}"
            )

        df = pd.read_csv(self.weather_csv_path)
        missing = sorted(self.WEATHER_REQUIRED_COLUMNS - set(df.columns))
        if missing:
            raise ValueError(
                f"Weather CSV is missing required columns: {missing}. "
                f"Found columns: {list(df.columns)}"
            )

        out = df.copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"])

        numeric_cols = [c for c in out.columns if c not in {"region_id", "date"}]
        for c in numeric_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")

        out = out.sort_values(["region_id", "date"]).reset_index(drop=True)
        self._weather_df_cache = out
        return out

    def _weather_timeseries(
        self,
        *,
        region_id: str,
        start: str,
        end: str,
        value_columns: list[str],
    ) -> pd.DataFrame:
        """
        Generic weather timeseries filter by region + inclusive date range.
        Returns date, region_id and requested value columns.
        """
        base = self._load_weather_csv()

        missing_cols = [c for c in value_columns if c not in base.columns]
        if missing_cols:
            raise ValueError(
                f"Requested weather columns not found in CSV: {missing_cols}. "
                f"Available columns: {list(base.columns)}"
            )

        available_regions = set(
            base["region_id"].dropna().astype(str).unique().tolist()
        )
        if region_id not in available_regions:
            raise ValueError(
                f"Requested weather region_id '{region_id}' not found. "
                f"Available region_id values: {sorted(available_regions)}"
            )

        start_ts = pd.to_datetime(start, errors="coerce")
        end_ts = pd.to_datetime(end, errors="coerce")
        if pd.isna(start_ts) or pd.isna(end_ts):
            raise ValueError(
                f"Invalid weather date window start='{start}' end='{end}'."
            )

        subset = base.loc[base["region_id"] == region_id].copy()
        subset = subset.loc[(subset["date"] >= start_ts) & (subset["date"] <= end_ts)]

        cols = ["date", "region_id", *value_columns]
        if subset.empty:
            return pd.DataFrame(columns=cols)

        out = subset[cols].copy()
        out = out.dropna(subset=value_columns, how="all")
        out = out.sort_values("date").reset_index(drop=True)
        return out

    def _canonical_weather_region(self, region: str) -> str:
        token = str(region or "").strip().lower()
        mapping = {
            "lower48": "lower_48",
            "lower_48": "lower_48",
            "national": "lower_48",
            "us": "lower_48",
            "u.s.": "lower_48",
            "united_states": "lower_48",
            "east": "east",
            "midwest": "midwest",
            "south": "south",
            "west": "west",
        }
        if token not in mapping:
            raise ValueError(
                f"Invalid weather region '{region}'. Expected one of: {sorted(mapping.keys())}"
            )
        return mapping[token]

    def _fetch_open_meteo_degree_day_forecast(self) -> tuple[pd.DataFrame, str]:
        today_utc = datetime.now(timezone.utc).date()
        end_utc = today_utc + timedelta(days=15)
        as_of = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        by_region: dict[str, pd.DataFrame] = {}
        for region_id, (lat, lon) in self.WEATHER_REGION_COORDS.items():
            df = self._fetch_region_degree_day_forecast_open_meteo(
                latitude=lat,
                longitude=lon,
                start=today_utc.isoformat(),
                end=end_utc.isoformat(),
            )
            if df.empty:
                continue
            df["region_id"] = region_id
            by_region[region_id] = df

        if not by_region:
            return pd.DataFrame(columns=["date", "region_id", "hdd_mean", "cdd_mean"]), as_of

        regional_frames = [v.copy() for v in by_region.values()]
        regional_df = pd.concat(regional_frames, ignore_index=True)

        national_rows: list[dict[str, Any]] = []
        all_dates = sorted(regional_df["date"].dropna().unique().tolist())
        for d in all_dates:
            day_slice = regional_df.loc[regional_df["date"] == d]
            hdd_total = 0.0
            cdd_total = 0.0
            weight_total = 0.0
            for region_id, weight in self.WEATHER_REGION_WEIGHTS.items():
                row = day_slice.loc[day_slice["region_id"] == region_id]
                if row.empty:
                    continue
                hdd_val = float(pd.to_numeric(row.iloc[0]["hdd_mean"], errors="coerce"))
                cdd_val = float(pd.to_numeric(row.iloc[0]["cdd_mean"], errors="coerce"))
                hdd_total += weight * hdd_val
                cdd_total += weight * cdd_val
                weight_total += weight
            if weight_total <= 0:
                continue
            national_rows.append(
                {
                    "date": d,
                    "region_id": "lower_48",
                    "hdd_mean": hdd_total / weight_total,
                    "cdd_mean": cdd_total / weight_total,
                }
            )

        national_df = pd.DataFrame(national_rows)
        merged = pd.concat([regional_df, national_df], ignore_index=True)
        merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
        merged = merged.dropna(subset=["date"]).sort_values(["region_id", "date"]).reset_index(drop=True)
        return merged, as_of

    def _fetch_region_degree_day_forecast_open_meteo(
        self,
        *,
        latitude: float,
        longitude: float,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max,temperature_2m_min",
            "temperature_unit": "fahrenheit",
            "timezone": "UTC",
            "start_date": start,
            "end_date": end,
        }
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
        daily = payload.get("daily") if isinstance(payload, dict) else None
        if not isinstance(daily, dict):
            return pd.DataFrame(columns=["date", "hdd_mean", "cdd_mean"])

        dates = daily.get("time") or []
        tmax = daily.get("temperature_2m_max") or []
        tmin = daily.get("temperature_2m_min") or []
        rows: list[dict[str, Any]] = []
        for d, max_f, min_f in zip(dates, tmax, tmin):
            try:
                max_val = float(max_f)
                min_val = float(min_f)
            except (TypeError, ValueError):
                continue
            tavg_f = (max_val + min_val) / 2.0
            rows.append(
                {
                    "date": d,
                    "hdd_mean": max(0.0, 65.0 - tavg_f),
                    "cdd_mean": max(0.0, tavg_f - 65.0),
                }
            )
        return pd.DataFrame(rows, columns=["date", "hdd_mean", "cdd_mean"])

    def _bucket_degree_days(
        self,
        df: pd.DataFrame,
        *,
        start_day: int,
        end_day: int,
    ) -> dict[str, Any] | None:
        if df is None or df.empty:
            return None
        ordered = df.copy()
        ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
        ordered["hdd_mean"] = pd.to_numeric(ordered["hdd_mean"], errors="coerce")
        ordered["cdd_mean"] = pd.to_numeric(ordered["cdd_mean"], errors="coerce")
        ordered = ordered.dropna(subset=["date", "hdd_mean", "cdd_mean"]).sort_values("date")
        if ordered.empty:
            return None
        sliced = ordered.iloc[start_day - 1 : end_day].copy()
        if sliced.empty:
            return None
        return {
            "dates": [d.date() for d in sliced["date"].tolist()],
            "hdd": float(sliced["hdd_mean"].sum()),
            "cdd": float(sliced["cdd_mean"].sum()),
        }

    def _shift_date_back_n_years(self, value: date, years_back: int) -> pd.Timestamp:
        shifted = pd.Timestamp(value) - pd.DateOffset(years=years_back)
        return pd.to_datetime(shifted, errors="coerce")

    def _estimate_gas_demand_delta_bcfd(
        self,
        *,
        delta_hdd: float,
        delta_cdd: float,
        days: int,
        region_id: str,
    ) -> float:
        region_scale = {
            "lower_48": 1.0,
            "east": 0.27,
            "midwest": 0.23,
            "south": 0.32,
            "west": 0.18,
        }.get(region_id, 1.0)
        if days <= 0:
            return 0.0
        # HDD sensitivity is generally stronger than CDD sensitivity for gas demand.
        hdd_sensitivity_bcfd_per_dd = 0.60 * region_scale
        cdd_sensitivity_bcfd_per_dd = 0.35 * region_scale
        avg_delta_hdd = delta_hdd / float(days)
        avg_delta_cdd = delta_cdd / float(days)
        return (hdd_sensitivity_bcfd_per_dd * avg_delta_hdd) + (
            cdd_sensitivity_bcfd_per_dd * avg_delta_cdd
        )

    def _call_library(
        self,
        *,
        fn: Callable[..., Any],
        fn_name: str,
        kwargs: dict,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Calls a library method and returns (df, meta).
        Meta can include units, frequency, series name, etc.
        """
        out = fn(**kwargs)

        # You may need to adapt this depending on how eia-ng-client returns data:
        # - DataFrame
        # - dict with keys {data, units, ...}
        # - list of rows
        meta: Dict[str, Any] = {"fn": fn_name}

        if isinstance(out, pd.DataFrame):
            return out, meta

        if isinstance(out, dict):
            # Common patterns: out["data"] is DataFrame or list
            if "units" in out:
                meta["units"] = out["units"]
            if "frequency" in out:
                meta["frequency"] = out["frequency"]

            data = out.get("data", out.get("df", out.get("rows")))
            if isinstance(data, pd.DataFrame):
                return data, meta
            if isinstance(data, list):
                return pd.DataFrame(data), meta

        if isinstance(out, list):
            return pd.DataFrame(out), meta

        raise TypeError(
            f"Unsupported return type from eia-ng-client call {fn_name}: {type(out)}"
        )

    def _fetch_ng_consumption_sector_history(
        self, *, sector: str, start: str, end: str
    ) -> pd.DataFrame:
        url = self.NG_CONSUMPTION_SECTOR_DNAV_URLS[sector]
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        tables = pd.read_html(StringIO(response.text))
        if not tables:
            return pd.DataFrame(columns=["date", "value", "series"])

        month_map = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
        }

        table = None
        year_col = None
        for candidate in tables:
            candidate = candidate.copy()
            candidate.columns = [str(col).strip() for col in candidate.columns]
            found_year_col = next(
                (col for col in candidate.columns if str(col).strip().lower() == "year"),
                None,
            )
            month_cols = {m for m in month_map if m in candidate.columns}
            if found_year_col is not None and len(month_cols) >= 3:
                table = candidate
                year_col = found_year_col
                break

        if table is None or year_col is None:
            return pd.DataFrame(columns=["date", "value", "series"])
        start_ts = pd.to_datetime(start, errors="coerce")
        end_ts = pd.to_datetime(end, errors="coerce")
        rows: list[dict[str, Any]] = []

        for _, row in table.iterrows():
            year = pd.to_numeric(row.get(year_col), errors="coerce")
            if pd.isna(year):
                continue
            year_int = int(year)
            for month_name, month_num in month_map.items():
                raw_value = row.get(month_name)
                if pd.isna(raw_value):
                    continue
                value = pd.to_numeric(
                    str(raw_value).replace(",", "").strip(), errors="coerce"
                )
                if pd.isna(value):
                    continue
                date = pd.Timestamp(year=year_int, month=month_num, day=1)
                if date < start_ts or date > end_ts:
                    continue
                rows.append(
                    {
                        "date": date,
                        "value": float(value),
                        "series": sector,
                    }
                )

        return pd.DataFrame(rows, columns=["date", "value", "series"])

    def _normalize_timeseries_df(
        self,
        df: pd.DataFrame,
        *,
        date_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """
        Normalize to columns: ['date', 'value'] (and preserve extras if present).
        """

        # If upstream returned no rows, avoid a confusing schema error downstream.
        if df is None or (df.empty and len(df.columns) == 0):
            return pd.DataFrame(columns=["date", "value"])
        if date_col not in df.columns:
            # common alternates
            for alt in ("period", "timestamp", "Date", "time"):
                if alt in df.columns:
                    df = df.rename(columns={alt: date_col})
                    break
        if value_col not in df.columns:
            for alt in (
                "value",
                "Value",
                "series",
                "data",
                "v",
                "generation",
                "production",
                "quantity",
                "amount",
            ):
                if alt in df.columns:
                    # keep if already correct; otherwise rename
                    if alt != value_col:
                        df = df.rename(columns={alt: value_col})
                    break

        if date_col not in df.columns or value_col not in df.columns:
            raise ValueError(
                f"Expected columns '{date_col}' and '{value_col}' in df. Got: {list(df.columns)}"
            )
        out = df.copy()
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col])
        out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
        out = out.sort_values(date_col)

        # final canonical rename
        out = out.rename(columns={date_col: "date", value_col: "value"})
        out = out.reset_index(drop=True)
        return out

    def _canonical_pipeline_dataset(self, dataset: str) -> str:
        if dataset not in self.PIPELINE_DATASET_FILES:
            raise ValueError(
                f"Invalid pipeline dataset '{dataset}'. Expected one of: "
                f"{sorted(set(self.PIPELINE_DATASET_FILES.keys()))}"
            )
        if dataset == "major_pipeline_sumamry":
            return "major_pipeline_summary"
        return dataset

    def _normalize_pipeline_df(
        self,
        df: pd.DataFrame,
        *,
        dataset: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        if dataset in self.PIPELINE_WIDE_DATASETS:
            out = self._normalize_pipeline_wide_year_df(
                df,
                id_columns=self.PIPELINE_WIDE_DATASETS[dataset],
            )
        elif dataset == "pipeline_state2_state_capacity":
            out = self._normalize_pipeline_capacity_df(df)
        elif dataset == "inflow_single_year":
            out = self._normalize_pipeline_single_year_df(df)
        elif dataset in self.PIPELINE_PROJECT_DATASETS:
            out = self._normalize_pipeline_project_df(df, dataset=dataset)
        else:
            out = df.copy()

        if "date" in out.columns:
            start_ts = pd.to_datetime(start, errors="coerce")
            end_ts = pd.to_datetime(end, errors="coerce")
            if pd.isna(start_ts) or pd.isna(end_ts):
                raise ValueError(
                    f"Invalid pipeline date window start='{start}' end='{end}'."
                )
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            out = out.dropna(subset=["date"])
            out = out.loc[(out["date"] >= start_ts) & (out["date"] <= end_ts)]
            out = out.sort_values("date").reset_index(drop=True)
        else:
            out = out.reset_index(drop=True)

        out["dataset"] = dataset
        return out

    def _normalize_pipeline_wide_year_df(
        self,
        df: pd.DataFrame,
        *,
        id_columns: list[str],
    ) -> pd.DataFrame:
        out = df.copy()
        year_columns = [c for c in out.columns if re.fullmatch(r"\d{4}", str(c))]
        for col in id_columns:
            if col in out.columns:
                cleaned = out[col].replace("", pd.NA)
                out[col] = cleaned.ffill()
        melted = out.melt(
            id_vars=[c for c in id_columns if c in out.columns],
            value_vars=year_columns,
            var_name="year",
            value_name="value",
        )
        melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
        melted = melted.dropna(subset=["value"])
        melted["date"] = pd.to_datetime(melted["year"] + "-01-01", errors="coerce")
        return melted.reset_index(drop=True)

    def _normalize_pipeline_capacity_df(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["year"] = pd.to_numeric(out.get("year"), errors="coerce").astype("Int64")
        out["value"] = pd.to_numeric(out.get("Capacity (mmcfd)"), errors="coerce")
        out["date"] = pd.to_datetime(
            out["year"].astype("string") + "-01-01",
            errors="coerce",
        )
        out = out.dropna(subset=["date", "value"])
        return out.reset_index(drop=True)

    def _normalize_pipeline_single_year_df(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if len(out.columns) >= 3:
            out = out.rename(
                columns={
                    out.columns[0]: "pipeline",
                    out.columns[1]: "location",
                    out.columns[2]: "value",
                }
            )
        out["value"] = pd.to_numeric(out.get("value"), errors="coerce")
        out = out.dropna(subset=["value"])
        out["date"] = pd.Timestamp(
            year=datetime.utcnow().year,
            month=1,
            day=1,
        )
        return out.reset_index(drop=True)

    def _normalize_pipeline_project_df(
        self,
        df: pd.DataFrame,
        *,
        dataset: str,
    ) -> pd.DataFrame:
        out = df.copy()
        if "Additional Capacity (MMcf/d)" in out.columns:
            out["value"] = pd.to_numeric(
                out["Additional Capacity (MMcf/d)"], errors="coerce"
            )
        elif "Cost (millions)" in out.columns:
            out["value"] = pd.to_numeric(out["Cost (millions)"], errors="coerce")

        date_series = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns]")
        if "Year In Service Date" in out.columns:
            year_values = out["Year In Service Date"].astype(str).str.extract(
                r"(?P<year>\d{4})"
            )["year"]
            year_dates = pd.to_datetime(year_values + "-01-01", errors="coerce")
            date_series = date_series.fillna(year_dates)
        if "Completed Date" in out.columns:
            date_series = date_series.fillna(
                pd.to_datetime(out["Completed Date"], errors="coerce")
            )
        if dataset == "natural_gas_pipeline_projects" and "Last Updated Date" in out.columns:
            date_series = date_series.fillna(
                pd.to_datetime(out["Last Updated Date"], errors="coerce")
            )
        out["date"] = date_series
        out = out.dropna(subset=["date"]).sort_values("date")
        return out.reset_index(drop=True)

    def _make_source(
        self, *, label: str, reference: str, parameters: dict
    ) -> SourceRef:
        return SourceRef(
            source_type="eia_api",
            label=label,
            reference=reference,
            parameters=parameters,
            retrieved_at=datetime.utcnow(),
        )

    # ---- subclass hooks ----

    def _fetch_timeseries(self, start: str, end: str, **kwargs) -> pd.DataFrame:
        which = kwargs.get("_fetch")

        if which == "henry_hub_spot":
            rows = self.client.natural_gas.spot_prices(start=start, end=end)
            if DEBUG_ENABLED:
                print(
                    f"[DEBUG] eia-ng henry_hub_spot {start}..{end} -> "
                    f"{0 if rows is None else len(rows)} rows"
                )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "lng_exports":
            region = kwargs.get("region", "united_states_pipeline_total")
            rows = self.client.natural_gas.exports(start=start, end=end, country=region)
            if DEBUG_ENABLED:
                print(
                    f"[DEBUG] eia-ng lng_exports {region} {start}..{end} -> "
                    f"{0 if rows is None else len(rows)} rows"
                )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_production":
            state = kwargs.get("state", "united_states_total")
            rows = self.client.natural_gas.production(
                start=start, end=end, state=state
            )
            if DEBUG_ENABLED:
                print(
                    f"[DEBUG] eia-ng production {state} {start}..{end} -> "
                    f"{0 if rows is None else len(rows)} rows"
                )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_consumption":
            state = kwargs.get("state", "united_states_total")
            rows = self.client.natural_gas.consumption(
                start=start, end=end, state=state
            )
            if DEBUG_ENABLED:
                print(
                    f"[DEBUG] eia-ng consumption {state} {start}..{end} -> "
                    f"{0 if rows is None else len(rows)} rows"
                )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_consumption_by_sector":
            frames: list[pd.DataFrame] = []
            for sector in self.NG_CONSUMPTION_SECTOR_DNAV_URLS:
                frame = self._fetch_ng_consumption_sector_history(
                    sector=sector, start=start, end=end
                )
                if frame.empty:
                    continue
                frames.append(frame)

            if not frames:
                return pd.DataFrame(columns=["date", "value", "series"])
            return pd.concat(frames, ignore_index=True)

        if which == "lng_imports":
            region = kwargs.get("region", "united_states_pipeline_total")
            rows = self.client.natural_gas.imports(start=start, end=end, country=region)
            if DEBUG_ENABLED:
                print(
                    f"[DEBUG] eia-ng lng_imports {region} {start}..{end} -> "
                    f"{0 if rows is None else len(rows)} rows"
                )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_electricity":
            rows = self.client.electricity.generation_natural_gas(start=start, end=end)
            if DEBUG_ENABLED:
                print(
                    f"[DEBUG] eia-ng electricity {start}..{end} -> "
                    f"{0 if rows is None else len(rows)} rows"
                )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_exploration_reserves":
            state = kwargs.get("state", "all")
            resource_category = kwargs.get(
                "resource_category", "proved_associated_gas"
            )
            rows = self.client.natural_gas.exploration_and_reserves(
                start=start,
                end=end,
                state=state,
                resource_category=resource_category,
            )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        # keep your other ones (storage, etc.)
        if which == "storage_working_gas":
            region = kwargs.get("region", "lower48")
            rows = self.client.natural_gas.storage(start=start, end=end, region=region)
            if DEBUG_ENABLED:
                print(
                    f"[DEBUG] eia-ng storage_working_gas {region} {start}..{end} -> "
                    f"{0 if rows is None else len(rows)} rows"
                )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        raise ValueError(f"Unknown fetch key: {which}")

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        out = self._normalize_timeseries_df(df, date_col="date", value_col="value")
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
        out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return out

    def _dedupe_cols(self, df: pd.DataFrame) -> list[str]:
        # For EIA, often date+series is the real unique key if multiple series exist
        if "series" in df.columns:
            return ["date", "series"]
        return ["date"]
