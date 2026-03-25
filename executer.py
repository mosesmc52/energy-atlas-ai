from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
from schemas.answer import SourceRef
from tools.eia_adapter import EIAAdapter, EIAResult
from tools.gridstatus_adapter import GridStatusAdapter, GridStatusResult


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


class MetricExecutor:
    """
    Deterministic dispatcher: metric -> implementation.
    """

    def __init__(self, *, eia: EIAAdapter, grid: GridStatusAdapter):
        self.eia = eia
        self.grid = grid

        self._metric_to_handler = {
            # --- EIA ---
            "working_gas_storage_lower48": self._eia_storage_lower48,
            "working_gas_storage_change_weekly": self._eia_storage_change_weekly,
            "henry_hub_spot": self._eia_henry_hub_spot,
            "lng_exports": self._eia_lng_exports,
            "lng_imports": self._eia_lng_imports,
            "ng_electricity": self._eia_ng_electricity,
            "ng_consumption_lower48": self._eia_ng_consumption_lower48,
            "ng_consumption_by_sector": self._eia_ng_consumption_by_sector,
            "ng_production_lower48": self._eia_ng_production_lower48,
            "ng_exploration_reserves_lower48": self._eia_ng_exploration_reserves_lower48,
            # --- GridStatus (v1) ---
            "iso_fuel_mix": self._grid_iso_fuel_mix,
            "iso_load": self._grid_iso_load,
            "iso_gas_dependency": self._grid_iso_gas_dependency,
            "iso_renewables": self._grid_iso_renewables,
        }

    def execute(self, req: ExecuteRequest) -> MetricResult:
        if req.metric not in self._metric_to_handler:
            raise ValueError(f"Unsupported metric: {req.metric}")

        handler = self._metric_to_handler[req.metric]

        # ---- execute adapter handler ----
        res = handler(start=req.start, end=req.end, filters=req.filters or {})

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

    def _to_metric_result(self, res: Any) -> MetricResult:
        """
        Convert adapter-specific result types into a unified MetricResult.
        """
        if isinstance(res, MetricResult):
            return res
        if isinstance(res, (EIAResult, GridStatusResult)):
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
        region = str(filters.get("region") or "lower48")
        return self.eia.storage_working_gas(start=start, end=end, region=region)

    def _eia_storage_change_weekly(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        region = str(filters.get("region") or "lower48")
        return self.eia.storage_working_gas_change_weekly(
            start=start, end=end, region=region
        )

    def _eia_henry_hub_spot(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.henry_hub_spot(start=start, end=end)

    def _eia_lng_exports(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        region = str(filters.get("region") or "united_states_pipeline_total")
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
        return self.eia.ng_consumption_lower48(start=start, end=end)

    def _eia_ng_consumption_by_sector(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.ng_consumption_by_sector(start=start, end=end)

    def _eia_ng_production_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.ng_production_lower48(start=start, end=end)

    def _eia_ng_exploration_reserves_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.ng_exploration_reserves_lower48(start=start, end=end)

    # -----------------------
    # Metric handlers (GridStatus v1)
    # -----------------------

    def _grid_iso_fuel_mix(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> GridStatusResult:
        iso = str(filters.get("iso") or "ercot")
        return self.grid.iso_fuel_mix(iso=iso, start=start, end=end)

    def _grid_iso_load(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> GridStatusResult:
        iso = str(filters.get("iso") or "ercot")
        return self.grid.iso_load(iso=iso, start=start, end=end)

    def _grid_iso_gas_dependency(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> GridStatusResult:
        iso = str(filters.get("iso") or "ercot")
        heat_rate = float(filters.get("heat_rate_mmbtu_per_mwh", 7.5))
        return self.grid.iso_gas_dependency(
            iso=iso,
            start=start,
            end=end,
            heat_rate_mmbtu_per_mwh=heat_rate,
        )

    def _grid_iso_renewables(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> GridStatusResult:
        iso = str(filters.get("iso") or "ercot")
        return self.grid.iso_renewables(iso=iso, start=start, end=end)
