# atlas/tools/eia_adapter.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from eia_ng import EIAClient
from schemas.answer import SourceRef

load_dotenv()


@dataclass(frozen=True)
class EIAResult:
    df: pd.DataFrame
    source: SourceRef
    meta: Dict[str, Any] | None = (
        None  # optional extra metadata (units, frequency, notes)
    )


class EIAAdapter:
    """
    Thin adapter around your eia-ng-client package.

    Responsibilities:
      - Call library methods
      - Normalize outputs to a consistent DataFrame shape
      - Attach provenance (SourceRef) for auditability
      - (Optional) attach meta like units/frequency
    """

    def __init__(self):
        self.client = EIAClient(api_key=os.getenv("EIA_API_KEY"))

    # ----------------------------
    # Public methods (router calls these)
    # ----------------------------

    def storage_working_gas_lower48(self, start: str, end: str) -> EIAResult:
        """
        Lower 48 working gas in storage (weekly).
        """

        rows = self.client.natural_gas.storage(start=start, end=end)
        df = pd.DataFrame(rows)

        df = self._normalize_timeseries_df(df, date_col="date", value_col="value")
        src = self._make_source(
            label="EIA Natural Gas Storage: Working Gas (Lower 48)",
            reference="eia-ng-client:storage.working_gas",
            parameters={"region": "lower48", "start": start, "end": end},
        )
        meta = {}
        return EIAResult(df=df, source=src, meta=meta)

    def henry_hub_spot(self, start: str, end: str) -> EIAResult:
        """
        Henry Hub spot price.
        """
        rows = self.client.natural_gas.spot_prices(start=start, end=end)
        df = pd.DataFrame(rows)

        df = self._normalize_timeseries_df(df, date_col="date", value_col="value")
        src = self._make_source(
            label="EIA Natural Gas Price: Henry Hub Spot",
            reference="eia-ng-client:prices.henry_hub_spot",
            parameters={"start": start, "end": end},
        )
        meta = {}

        return EIAResult(df=df, source=src, meta=meta)

    def lng_exports(self, start: str, end: str) -> EIAResult:
        """
        LNG exports (or whichever LNG series you choose as canonical).
        """
        rows = self.client.natural_gas.exports(start=start, end=end)
        df = pd.DataFrame(rows)
        df = self._normalize_timeseries_df(df, date_col="date", value_col="value")
        src = self._make_source(
            label="EIA Natural Gas: LNG Exports",
            reference="eia-ng-client:lng.exports",
            parameters={"start": start, "end": end},
        )
        meta = {}
        return EIAResult(df=df, source=src, meta=meta)

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
