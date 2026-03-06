# atlas/tools/eia_adapter.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

import pandas as pd
from dotenv import load_dotenv
from eia_ng import EIAClient
from schemas.answer import SourceRef
from tools.cache_base import CacheBackedTimeseriesAdapterBase

load_dotenv()


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
        "tavg_f_mean",
        "tavg_f_median",
        "tavg_c_mean",
        "tavg_c_median",
    }

    def __init__(
        self,
        cache_dir: str = "data/cache/eia",
        api_key: str | None = None,
        weather_csv_path: str | Path | None = None,
    ):
        super().__init__(cache_dir=cache_dir, date_col="date")
        self.client = EIAClient(api_key=api_key or os.getenv("EIA_API_KEY"))
        self.weather_csv_path = (
            Path(weather_csv_path) if weather_csv_path is not None else None
        )
        self._weather_df_cache: pd.DataFrame | None = None

    # ----------------------------
    # Public methods (router calls these)
    # ----------------------------

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
        if region not in self.TRADE_REGIONS:
            raise ValueError(
                f"Invalid trade region '{region}'. Expected one of: {sorted(self.TRADE_REGIONS)}"
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
        if region not in self.TRADE_REGIONS:
            raise ValueError(
                f"Invalid trade region '{region}'. Expected one of: {sorted(self.TRADE_REGIONS)}"
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

    def ng_consumption_lower48(self, start: str, end: str) -> EIAResult:
        """
        NG Consumption (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="consumption",
            start=start,
            end=end,
            cache_key_parts={
                "region": "united_states_total"
            },  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "ng_consumption"},
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="M",
        )

        src = self._make_source(
            label="EIA Natural Gas: Consumption",
            reference="eia-ng-client:natural_gas.consumption",
            parameters={
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_production_lower48(self, start: str, end: str) -> EIAResult:
        """
        NG Production (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="ng_production",
            start=start,
            end=end,
            cache_key_parts={
                "region": "united_states_total"
            },  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "ng_production"},
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="M",
        )

        src = self._make_source(
            label="EIA Natural Gas: Production",
            reference="eia-ng-client:natural_gas.production",
            parameters={
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_exploration_reserves_lower48(self, start: str, end: str) -> EIAResult:
        """
        NG Exploration (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="ng_exploration_reserves",
            start=start,
            end=end,
            cache_key_parts={},  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "ng_exploration_reserves"},
            allow_internal_gap_fill_daily=False,  # set False if series is weekly/monthly
            expected_calendar="A",
        )

        src = self._make_source(
            label="EIA Natural Gas: Production",
            reference="eia-ng-client:natural_gas.exploration_and_reserves",
            parameters={
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
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
            print(
                f"[DEBUG] eia-ng lng_exports {region} {start}..{end} -> "
                f"{0 if rows is None else len(rows)} rows"
            )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_production":
            rows = self.client.natural_gas.production(start=start, end=end)
            print(
                f"[DEBUG] eia-ng production {start}..{end} -> "
                f"{0 if rows is None else len(rows)} rows"
            )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_consumption":
            rows = self.client.natural_gas.consumption(start=start, end=end)
            print(
                f"[DEBUG] eia-ng consumption {start}..{end} -> "
                f"{0 if rows is None else len(rows)} rows"
            )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "lng_imports":
            region = kwargs.get("region", "united_states_pipeline_total")
            rows = self.client.natural_gas.imports(start=start, end=end, country=region)
            print(
                f"[DEBUG] eia-ng lng_imports {region} {start}..{end} -> "
                f"{0 if rows is None else len(rows)} rows"
            )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_electricity":
            rows = self.client.electricity.generation_natural_gas(start=start, end=end)
            print(
                f"[DEBUG] eia-ng electricity {start}..{end} -> "
                f"{0 if rows is None else len(rows)} rows"
            )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        if which == "ng_exploration_reserves":
            rows = self.client.natural_gas.exploration_and_reserves(start=2000)
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        # keep your other ones (storage, etc.)
        if which == "storage_working_gas":
            region = kwargs.get("region", "lower48")
            rows = self.client.natural_gas.storage(start=start, end=end, region=region)
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
