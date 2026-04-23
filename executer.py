from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
from schemas.answer import SourceRef
from tools.cftc_adapter import CFTCAdapter, CFTCResult
from tools.des_adapter import DESResult, DallasEnergySurveyAdapter
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

    def __init__(
        self,
        *,
        eia: EIAAdapter,
        grid: GridStatusAdapter,
        des: DallasEnergySurveyAdapter | None = None,
        cftc: CFTCAdapter | None = None,
    ):
        self.eia = eia
        self.grid = grid
        self.des = des or DallasEnergySurveyAdapter()
        self.cftc = cftc or CFTCAdapter()

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
            "ng_pipeline": self._eia_ng_pipeline,
            "weather_degree_days_forecast_vs_5y": self._eia_weather_degree_days_forecast_vs_5y,
            # --- Dallas Fed Energy Survey ---
            "des_business_activity_index": self._des_metric,
            "des_company_outlook_index": self._des_metric,
            "des_outlook_uncertainty_index": self._des_metric,
            "des_oil_production_index": self._des_metric,
            "des_gas_production_index": self._des_metric,
            "des_capex_index": self._des_metric,
            "des_employment_index": self._des_metric,
            "des_input_cost_index": self._des_metric,
            "des_finding_development_costs_index": self._des_metric,
            "des_lease_operating_expense_index": self._des_metric,
            "des_prices_received_services_index": self._des_metric,
            "des_equipment_utilization_index": self._des_metric,
            "des_operating_margin_index": self._des_metric,
            "des_wti_price_expectation_6m": self._des_metric,
            "des_wti_price_expectation_1y": self._des_metric,
            "des_wti_price_expectation_2y": self._des_metric,
            "des_wti_price_expectation_5y": self._des_metric,
            "des_hh_price_expectation_6m": self._des_metric,
            "des_hh_price_expectation_1y": self._des_metric,
            "des_hh_price_expectation_2y": self._des_metric,
            "des_hh_price_expectation_5y": self._des_metric,
            "des_breakeven_oil_us": self._des_metric,
            "des_breakeven_gas_us": self._des_metric,
            "des_breakeven_oil_permian": self._des_metric,
            "des_breakeven_oil_eagle_ford": self._des_metric,
            "des_special_questions_text": self._des_metric,
            "des_comments_text": self._des_metric,
            "des_report_summary_text": self._des_metric,
            # --- CFTC ---
            "managed_money_long": self._cftc_metric,
            "managed_money_short": self._cftc_metric,
            "managed_money_net": self._cftc_metric,
            "managed_money_net_percentile_156w": self._cftc_metric,
            "open_interest": self._cftc_metric,
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
        runtime_filters = dict(req.filters or {})
        if req.metric.startswith("des_"):
            runtime_filters["_metric"] = req.metric
        if req.metric in {
            "managed_money_long",
            "managed_money_short",
            "managed_money_net",
            "managed_money_net_percentile_156w",
            "open_interest",
        }:
            runtime_filters["_metric"] = req.metric
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

    def _to_metric_result(self, res: Any) -> MetricResult:
        """
        Convert adapter-specific result types into a unified MetricResult.
        """
        if isinstance(res, MetricResult):
            return res
        if isinstance(res, (EIAResult, GridStatusResult, DESResult, CFTCResult)):
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
        base = self.eia.storage_working_gas(start=start, end=end, region=region)
        if not bool(filters.get("include_weekly_change")):
            return base

        weekly_change = self.eia.storage_working_gas_change_weekly(
            start=start, end=end, region=region
        )
        left = base.df.copy()
        right = weekly_change.df.copy()
        if left is None or left.empty:
            merged = pd.DataFrame(columns=["date", "value", "weekly_change"])
        else:
            merged = left.copy()
            if right is not None and not right.empty:
                right = right.rename(columns={"value": "weekly_change"})
                merged = merged.merge(right[["date", "weekly_change"]], on="date", how="left")
            else:
                merged["weekly_change"] = pd.NA

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

    def _eia_storage_change_weekly(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        if str(filters.get("group_by") or "") == "region":
            regional_frames: list[pd.DataFrame] = []
            sources: list[str] = []
            region_meta: dict[str, Any] = {}
            for region in sorted(EIAAdapter.STORAGE_REGIONS - {"lower48"}):
                regional_result = self.eia.storage_working_gas_change_weekly(
                    start=start, end=end, region=region
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
                label="EIA Natural Gas Storage: Weekly Change by Region",
                reference="eia-ng-client:derived_natural_gas.storage_change_weekly_by_region",
                parameters={
                    "regions": sorted(EIAAdapter.STORAGE_REGIONS - {"lower48"}),
                    "start": start,
                    "end": end,
                    "group_by": "region",
                    "source_storage_references": sources,
                },
            )
            return EIAResult(
                df=df,
                source=src,
                meta={
                    "cache": {"regions": region_meta},
                    "note": "Derived as row-to-row weekly difference of working gas storage levels by EIA storage region.",
                },
            )

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
        state = str(filters.get("region") or "united_states_total")
        return self.eia.ng_consumption_lower48(start=start, end=end, state=state)

    def _eia_ng_consumption_by_sector(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.ng_consumption_by_sector(start=start, end=end)

    def _eia_ng_production_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        state = str(filters.get("region") or "united_states_total")
        return self.eia.ng_production_lower48(start=start, end=end, state=state)

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

    def _des_metric(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> DESResult:
        metric = str(filters.get("_metric"))
        return self.des.get_metric(metric, start_date=start, end_date=end)

    def _cftc_metric(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> CFTCResult:
        metric = str(filters.get("_metric"))
        return self.cftc.get_metric(metric, start=start, end=end)

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
