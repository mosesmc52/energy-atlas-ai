# atlas/tools/eia_adapter.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
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

    def __init__(self, cache_dir: str = "data/cache/eia"):
        super().__init__(cache_dir=cache_dir, date_col="date")
        self.client = EIAClient(api_key=os.getenv("EIA_API_KEY"))

    # ----------------------------
    # Public methods (router calls these)
    # ----------------------------

    def ng_storage_working_gas_lower48(self, start: str, end: str) -> EIAResult:
        """
        Lower 48 working gas in storage (weekly).
        Cache-first: load CSV, fetch missing edges via eia-ng, save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="working_gas_storage_lower48",
            start=start,
            end=end,
            cache_key_parts={"region": "lower48"},
            fetch_ctx={"_fetch": "storage_working_gas", "region": "lower48"},
            allow_internal_gap_fill_daily=True,  # weekly series: edge fill is safer initially
            expected_calendar="B",
        )

        src = self._make_source(
            label="EIA Natural Gas Storage: Working Gas (Lower 48)",
            reference="eia-ng-client:natural_gas.storage",
            parameters={
                "region": "lower48",
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,  # <-- include cache behavior
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_henry_hub_spot(self, start: str, end: str) -> EIAResult:
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

    def ng_lng_exports(self, start: str, end: str) -> EIAResult:
        """
        LNG exports (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="lng_exports",
            start=start,
            end=end,
            cache_key_parts={},  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "lng_exports"},
            allow_internal_gap_fill_daily=True,  # set False if series is weekly/monthly
        )

        src = self._make_source(
            label="EIA Natural Gas: LNG Exports",
            reference="eia-ng-client:natural_gas.exports",
            parameters={
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return EIAResult(df=df, source=src, meta=meta)

    def ng_lng_imports(self, start: str, end: str) -> EIAResult:
        """
        LNG imports (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        df, cache_info = self._cached_timeseries(
            metric_key="lng_imports",
            start=start,
            end=end,
            cache_key_parts={},  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "lng_imports"},
            allow_internal_gap_fill_daily=True,  # set False if series is weekly/monthly
        )

        src = self._make_source(
            label="EIA Natural Gas: LNG Imports",
            reference="eia-ng-client:natural_gas.imports",
            parameters={
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
            allow_internal_gap_fill_daily=True,  # set False if series is weekly/monthly
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
            metric_key="ng_consumption",
            start=start,
            end=end,
            cache_key_parts={},  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "ng_consumption"},
            allow_internal_gap_fill_daily=True,  # set False if series is weekly/monthly
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
            cache_key_parts={},  # add facets if you later support export type/region
            fetch_ctx={"_fetch": "ng_production"},
            allow_internal_gap_fill_daily=True,  # set False if series is weekly/monthly
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
        pass

    def ng_pipelines_lower48(self, start: str, end: str) -> EIAResult:
        """
        NG Pipelines (canonical series).
        Cache-first: load CSV, fetch missing edges (and optionally internal daily gaps), save, return window.
        """
        pass

    # ----------------------------
    # Library calling + normalization helpers
    # ----------------------------

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
            for alt in ("value", "Value", "series", "data", "v"):
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
            rows = self.client.natural_gas.exports(start=start, end=end)
            print(
                f"[DEBUG] eia-ng lng_exports {start}..{end} -> "
                f"{0 if rows is None else len(rows)} rows"
            )
            if not rows:
                return pd.DataFrame(columns=["date", "value"])
            return pd.DataFrame(rows)

        # keep your other ones (storage, etc.)
        if which == "storage_working_gas":
            rows = self.client.natural_gas.storage(start=start, end=end)
            print(
                f"[DEBUG] eia-ng storage_working_gas {start}..{end} -> "
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
