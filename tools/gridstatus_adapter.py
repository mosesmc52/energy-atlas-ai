# atlas/tools/gridstatus_adapter.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from schemas.answer import SourceRef
from tools.cache_base import CacheBackedTimeseriesAdapterBase


@dataclass(frozen=True)
class GridStatusResult:
    df: pd.DataFrame
    source: SourceRef
    meta: Dict[str, Any] | None = None


class GridStatusAdapter(CacheBackedTimeseriesAdapterBase):
    """
    Adapter around the `gridstatus` library.

    Responsibilities:
      - Call gridstatus ISO methods
      - Normalize outputs to canonical ['date','value'] OR return a richer DF when appropriate
      - Cache-first window retrieval using CacheBackedTimeseriesAdapterBase
      - Attach provenance (SourceRef)

    Notes:
      - gridstatus fuel-mix and load are often sub-daily (5-min / 15-min / hourly) depending on ISO.
      - For v1, we normalize timestamps to pandas datetime and keep the raw cadence.
    """

    # Heat rate for converting MWh of gas generation to gas energy input.
    # Typical combined-cycle heat rate ballpark: 7–8 MMBtu/MWh.
    DEFAULT_HEAT_RATE_MMBTU_PER_MWH = 7.5

    def __init__(self, cache_dir: str = "data/cache/gridstatus"):
        # Store canonical time column as "date" like your EIA adapter.
        super().__init__(cache_dir=cache_dir, date_col="date")

        try:
            import gridstatus  # noqa: F401
        except Exception as e:
            raise ImportError(
                "gridstatus is not installed. Install with: pip install gridstatus"
            ) from e

    # ----------------------------
    # Public methods (router can call these)
    # ----------------------------

    def iso_fuel_mix(
        self,
        *,
        iso: str,
        start: str,
        end: str,
    ) -> GridStatusResult:
        """
        Fetch ISO fuel mix (often sub-daily). Returns a DataFrame with:
          - date (timestamp)
          - columns for each fuel type (e.g., gas, wind, solar, coal, nuclear, etc.)
          - plus derived columns we add: total_generation_mw, gas_share
        """
        df, cache_info = self._cached_timeseries(
            metric_key="iso_fuel_mix",
            start=start,
            end=end,
            cache_key_parts={"iso": iso.lower()},
            fetch_ctx={"_fetch": "iso_fuel_mix", "iso": iso},
            allow_internal_gap_fill_daily=False,  # do not fabricate sub-daily bars
            expected_calendar="H",  # "hourly-ish" expectation; base may treat as informational
        )

        # Enrich/derive:
        df = self._enrich_fuel_mix_df(df)

        src = self._make_source(
            label=f"GridStatus Fuel Mix: {iso.upper()}",
            reference="gridstatus:fuel_mix",
            parameters={
                "iso": iso,
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return GridStatusResult(df=df, source=src, meta=meta)

    def iso_load(
        self,
        *,
        iso: str,
        start: str,
        end: str,
    ) -> GridStatusResult:
        """
        Fetch ISO load/demand (often sub-daily). Returns canonical DF:
          - date
          - value  (load MW)
        """
        df, cache_info = self._cached_timeseries(
            metric_key="iso_load",
            start=start,
            end=end,
            cache_key_parts={"iso": iso.lower()},
            fetch_ctx={"_fetch": "iso_load", "iso": iso},
            allow_internal_gap_fill_daily=False,
            expected_calendar="H",
        )

        src = self._make_source(
            label=f"GridStatus Load: {iso.upper()}",
            reference="gridstatus:load",
            parameters={
                "iso": iso,
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        meta = {"cache": cache_info.__dict__}
        return GridStatusResult(df=df, source=src, meta=meta)

    def iso_gas_dependency(
        self,
        *,
        iso: str,
        start: str,
        end: str,
        heat_rate_mmbtu_per_mwh: float = DEFAULT_HEAT_RATE_MMBTU_PER_MWH,
        assume_fuel_mix_units: str = "MW",
    ) -> GridStatusResult:
        """
        v1 "natural gas dependency" metric derived from fuel mix.

        Output DF columns:
          - date
          - gas_generation (same units as mix, typically MW)
          - total_generation
          - gas_share
          - gas_burn_mmbtu_per_hour (approx; if fuel mix is MW)
          - gas_burn_mcf_per_hour (approx; using 1 MMBtu ≈ 1 Mcf)

        Assumptions:
          - fuel mix columns are in MW at each timestamp (common in ISOs).
          - MW at timestamp ~ MWh over the next hour only if cadence is hourly.
            If cadence is 5-min, this is "instantaneous" MW; you should integrate
            to get energy. For v1, treat this as a *proxy* / instantaneous burn rate.
        """
        mix_res = self.iso_fuel_mix(iso=iso, start=start, end=end)
        df = mix_res.df.copy()

        if "gas" not in df.columns:
            raise ValueError(
                f"Fuel mix for {iso} did not include a 'gas' column. "
                f"Columns={list(df.columns)}"
            )
        if "total_generation_mw" not in df.columns:
            df = self._enrich_fuel_mix_df(df)

        df["gas_generation"] = pd.to_numeric(df["gas"], errors="coerce")
        df["total_generation"] = pd.to_numeric(
            df["total_generation_mw"], errors="coerce"
        )
        df["gas_share"] = pd.to_numeric(df.get("gas_share"), errors="coerce")

        # Convert power to (approx) fuel burn rate
        # If MW and hourly cadence: MW ~= MWh per hour. If sub-hourly, treat as burn-rate proxy.
        # gas_burn_mmbtu_per_hour = gas_MWh_per_hour * heat_rate
        df["gas_burn_mmbtu_per_hour"] = df["gas_generation"] * float(
            heat_rate_mmbtu_per_mwh
        )

        # Very rough conversion often used in gas market heuristics:
        # 1 MMBtu ≈ 1 Mcf (depends on gas quality; close enough for v1)
        df["gas_burn_mcf_per_hour"] = df["gas_burn_mmbtu_per_hour"]

        # Keep output compact and consistent
        out_cols = [
            "date",
            "gas_generation",
            "total_generation",
            "gas_share",
            "gas_burn_mmbtu_per_hour",
            "gas_burn_mcf_per_hour",
        ]
        out = (
            df[out_cols]
            .dropna(subset=["date"])
            .sort_values("date")
            .reset_index(drop=True)
        )

        src = self._make_source(
            label=f"GridStatus Derived: {iso.upper()} Gas Dependency (from fuel mix)",
            reference="gridstatus:derived_gas_dependency",
            parameters={
                "iso": iso,
                "start": start,
                "end": end,
                "heat_rate_mmbtu_per_mwh": heat_rate_mmbtu_per_mwh,
                "assume_fuel_mix_units": assume_fuel_mix_units,
                "source_fuel_mix_reference": mix_res.source.reference,
            },
        )
        meta = {
            "heat_rate_mmbtu_per_mwh": heat_rate_mmbtu_per_mwh,
            "assume_fuel_mix_units": assume_fuel_mix_units,
            "note": "If fuel mix cadence is sub-hourly, values are instantaneous burn-rate proxies unless integrated.",
        }
        return GridStatusResult(df=out, source=src, meta=meta)

    def iso_renewables(
        self,
        *,
        iso: str,
        start: str,
        end: str,
    ) -> GridStatusResult:
        """
        v1 renewables metric derived from fuel mix.

        Important:
          - v1 renewables = wind + solar only
          - excludes hydro, batteries, geothermal, and nuclear
        """
        mix_res = self.iso_fuel_mix(iso=iso, start=start, end=end)
        df = mix_res.df.copy()

        wind_col = self._pick_first_existing_column(
            df,
            (
                "wind",
                "wind_generation",
                "wind generation",
                "wind_mw",
                "wind mw",
            ),
        )
        solar_col = self._pick_first_existing_column(
            df,
            (
                "solar",
                "solar_generation",
                "solar generation",
                "solar_mw",
                "solar mw",
                "solar_pv",
                "solar pv",
            ),
        )

        if wind_col is None and solar_col is None:
            raise ValueError(
                f"Fuel mix for {iso} did not include wind/solar columns. "
                f"Columns={list(df.columns)}"
            )

        total_col = self._pick_first_existing_column(
            df, ("total_generation_mw", "total_generation")
        )
        if total_col is None:
            df = self._enrich_fuel_mix_df(df)
            total_col = "total_generation_mw"

        wind = (
            pd.to_numeric(df[wind_col], errors="coerce")
            if wind_col is not None
            else pd.Series(0.0, index=df.index, dtype="float64")
        )
        solar = (
            pd.to_numeric(df[solar_col], errors="coerce")
            if solar_col is not None
            else pd.Series(0.0, index=df.index, dtype="float64")
        )
        total = pd.to_numeric(df[total_col], errors="coerce")

        out = pd.DataFrame(
            {
                "date": df["date"],
                "wind_generation": wind.fillna(0.0),
                "solar_generation": solar.fillna(0.0),
                "total_generation": total,
            }
        )
        out["renewable_generation"] = (
            out["wind_generation"] + out["solar_generation"]
        )
        out["renewable_share"] = out["renewable_generation"] / out[
            "total_generation"
        ].replace({0: pd.NA})

        out = (
            out[
                [
                    "date",
                    "wind_generation",
                    "solar_generation",
                    "renewable_generation",
                    "total_generation",
                    "renewable_share",
                ]
            ]
            .dropna(subset=["date"])
            .sort_values("date")
            .reset_index(drop=True)
        )

        src = self._make_source(
            label=f"GridStatus Derived: {iso.upper()} Renewables (Wind + Solar)",
            reference="gridstatus:derived_renewables_from_fuel_mix",
            parameters={
                "iso": iso,
                "start": start,
                "end": end,
                "source_fuel_mix_reference": mix_res.source.reference,
            },
        )
        meta = {
            "note": "v1 renewables include wind + solar only (exclude hydro, batteries, geothermal, nuclear)."
        }
        return GridStatusResult(df=out, source=src, meta=meta)

    # ----------------------------
    # Subclass hooks required by CacheBackedTimeseriesAdapterBase
    # ----------------------------

    def _fetch_timeseries(self, start: str, end: str, **kwargs) -> pd.DataFrame:
        """
        Fetch raw timeseries for the cache base.
        Must return a DataFrame that includes a datetime-like column 'date' and some values.
        """
        which = kwargs.get("_fetch")
        iso = kwargs.get("iso")
        if not iso:
            raise ValueError("GridStatusAdapter requires 'iso' in fetch_ctx")

        iso_obj = self._get_iso_client(iso)

        if which == "iso_fuel_mix":
            # gridstatus API typically supports ISO.get_fuel_mix(start=..., end=...)
            df = iso_obj.get_fuel_mix(start=start, end=end)
            return self._normalize_gridstatus_df(
                df, time_col_candidates=("time", "timestamp", "interval_start", "date")
            )

        if which == "iso_load":
            # gridstatus API varies; many ISOs provide get_load(...)
            # We normalize to ['date','value'] as MW load.
            df = iso_obj.get_load(start=start, end=end)
            df = self._normalize_gridstatus_df(
                df, time_col_candidates=("time", "timestamp", "interval_start", "date")
            )

            # Find a likely load value column
            value_col = self._pick_value_col(
                df, preferred=("load", "mw", "value", "Load")
            )
            if value_col is None:
                raise ValueError(
                    f"Could not find a load value column in ISO load df for {iso}. "
                    f"Columns={list(df.columns)}"
                )
            df = df.rename(columns={value_col: "value"})
            return df[["date", "value"]]

        raise ValueError(f"Unknown fetch key: {which}")

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Called by cache base after reading from cache and after fetching.
        We keep "date" normalized (timezone-naive) and sorted.
        """
        if df is None or (df.empty and len(df.columns) == 0):
            return pd.DataFrame(columns=["date", "value"])

        out = df.copy()

        # Ensure 'date'
        if "date" not in out.columns:
            out = self._normalize_gridstatus_df(
                out, time_col_candidates=("time", "timestamp", "interval_start", "Date")
            )

        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"])
        # Normalize timezone to naive for consistent caching/joins
        if getattr(out["date"].dt, "tz", None) is not None:
            out["date"] = out["date"].dt.tz_convert(None)
        else:
            # If tz-aware per row, this still works:
            try:
                out["date"] = out["date"].dt.tz_localize(None)
            except Exception:
                pass

        out = out.sort_values("date").reset_index(drop=True)
        return out

    def _dedupe_cols(self, df: pd.DataFrame) -> list[str]:
        # Dedupe on timestamp only; gridstatus fuel mix is one row per timestamp.
        return ["date"]

    # ----------------------------
    # Internal helpers
    # ----------------------------

    def _get_iso_client(self, iso: str):
        """
        Map string -> gridstatus ISO class instance.

        Supported aliases (v1):
          - "ercot"
          - "pjm"
          - "isone" / "iso-ne" / "iso_new_england"
          - "nyiso"
          - "caiso"
        """
        iso_key = iso.strip().lower().replace("_", "-")

        from gridstatus import CAISO, ISONE, NYISO, PJM, Ercot  # type: ignore

        if iso_key in ("ercot",):
            return Ercot()
        if iso_key in ("pjm",):
            return PJM()
        if iso_key in ("isone", "iso-ne", "iso new england", "iso-new-england"):
            return ISONE()
        if iso_key in ("nyiso",):
            return NYISO()
        if iso_key in ("caiso",):
            return CAISO()

        raise ValueError(
            f"Unsupported ISO '{iso}'. Add a mapping in GridStatusAdapter._get_iso_client()."
        )

    def _normalize_gridstatus_df(
        self,
        df: pd.DataFrame,
        *,
        time_col_candidates: Tuple[str, ...],
    ) -> pd.DataFrame:
        """
        Normalize a gridstatus df by ensuring a canonical 'date' column exists.
        Does NOT force down to ['date','value'] because fuel-mix is multi-column.
        """
        if df is None or (
            isinstance(df, pd.DataFrame) and df.empty and len(df.columns) == 0
        ):
            return pd.DataFrame(columns=["date"])

        out = df.copy()

        # If index is time-like, move to column
        if (
            out.index is not None
            and out.index.name is not None
            and out.index.name.lower()
            in (
                "time",
                "timestamp",
                "date",
            )
        ):
            out = out.reset_index()

        if "date" not in out.columns:
            for c in time_col_candidates:
                if c in out.columns:
                    out = out.rename(columns={c: "date"})
                    break

        if "date" not in out.columns:
            # common gridstatus patterns
            for alt in ("Interval Start", "interval_start", "start", "period"):
                if alt in out.columns:
                    out = out.rename(columns={alt: "date"})
                    break

        if "date" not in out.columns:
            raise ValueError(
                f"Expected a time column in gridstatus output. "
                f"Tried candidates={time_col_candidates}. Columns={list(out.columns)}"
            )

        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return out

    def _pick_value_col(
        self,
        df: pd.DataFrame,
        *,
        preferred: Tuple[str, ...],
    ) -> Optional[str]:
        cols = list(df.columns)
        for p in preferred:
            if p in cols:
                return p
            # case-insensitive match
            for c in cols:
                if c.lower() == p.lower():
                    return c
        return None

    def _pick_first_existing_column(
        self, df: pd.DataFrame, candidates: Tuple[str, ...]
    ) -> Optional[str]:
        cols = list(df.columns)
        if not cols:
            return None

        def _norm(s: str) -> str:
            return "".join(ch for ch in s.lower() if ch.isalnum())

        norm_to_col = {_norm(c): c for c in cols}

        for cand in candidates:
            if cand in cols:
                return cand
            for c in cols:
                if c.lower() == cand.lower():
                    return c

            n = _norm(cand)
            if n in norm_to_col:
                return norm_to_col[n]

        return None

    def _enrich_fuel_mix_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add:
          - total_generation_mw: sum of numeric fuel columns excluding 'date'
          - gas_share: gas / total_generation_mw (if gas column exists)
        """
        out = df.copy()
        if "date" not in out.columns:
            out = self._normalize_gridstatus_df(
                out, time_col_candidates=("time", "timestamp", "interval_start", "date")
            )

        # Identify numeric-ish fuel columns
        fuel_cols = [c for c in out.columns if c != "date"]
        # Coerce to numeric where possible
        for c in fuel_cols:
            out[c] = pd.to_numeric(out[c], errors="coerce")

        # total generation as sum across columns (ignoring NaNs)
        out["total_generation_mw"] = out[fuel_cols].sum(axis=1, skipna=True)

        if "gas" in out.columns:
            # Avoid divide-by-zero
            denom = out["total_generation_mw"].replace({0: pd.NA})
            out["gas_share"] = out["gas"] / denom

        return out

    def _make_source(
        self, *, label: str, reference: str, parameters: dict
    ) -> SourceRef:
        return SourceRef(
            source_type="gridstatus",
            label=label,
            reference=reference,
            parameters=parameters,
            retrieved_at=datetime.utcnow(),
        )
