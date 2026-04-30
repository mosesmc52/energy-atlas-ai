from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
from schemas.answer import SourceRef
from agents.source_planner import SourcePlan
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
            "ng_supply_balance_regime": self._eia_ng_supply_balance_regime,
            "ng_exploration_reserves_lower48": self._eia_ng_exploration_reserves_lower48,
            "ng_pipeline": self._eia_ng_pipeline,
            "weather_degree_days_forecast_vs_5y": self._eia_weather_degree_days_forecast_vs_5y,
            "weather_regional_demand_drivers": self._eia_weather_regional_demand_drivers,
            "weekly_energy_atlas_summary": self._eia_weekly_energy_atlas_summary,
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

    def execute_plan(self, plan: SourcePlan, *, start: str, end: str) -> Dict[str, MetricResult]:
        results: Dict[str, MetricResult] = {}
        for call in plan.calls:
            req = ExecuteRequest(
                metric=call.metric,
                start=start,
                end=end,
                filters=call.filters or {},
            )
            results[call.metric] = self.execute(req)
        return results

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
            region="united_states_pipeline_total",
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
        try:
            return self.grid.iso_fuel_mix(iso=iso, start=start, end=end)
        except Exception as exc:  # noqa: BLE001
            return self._gridstatus_error_result(
                metric="iso_fuel_mix",
                iso=iso,
                start=start,
                end=end,
                error=exc,
            )

    def _grid_iso_load(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> GridStatusResult:
        iso = str(filters.get("iso") or "ercot")
        try:
            return self.grid.iso_load(iso=iso, start=start, end=end)
        except Exception as exc:  # noqa: BLE001
            return self._gridstatus_error_result(
                metric="iso_load",
                iso=iso,
                start=start,
                end=end,
                error=exc,
            )

    def _grid_iso_gas_dependency(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> GridStatusResult:
        iso = str(filters.get("iso") or "ercot")
        heat_rate = float(filters.get("heat_rate_mmbtu_per_mwh", 7.5))
        try:
            return self.grid.iso_gas_dependency(
                iso=iso,
                start=start,
                end=end,
                heat_rate_mmbtu_per_mwh=heat_rate,
            )
        except Exception as exc:  # noqa: BLE001
            return self._gridstatus_error_result(
                metric="iso_gas_dependency",
                iso=iso,
                start=start,
                end=end,
                error=exc,
                extra={"heat_rate_mmbtu_per_mwh": heat_rate},
            )

    def _grid_iso_renewables(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> GridStatusResult:
        iso = str(filters.get("iso") or "ercot")
        try:
            return self.grid.iso_renewables(iso=iso, start=start, end=end)
        except Exception as exc:  # noqa: BLE001
            return self._gridstatus_error_result(
                metric="iso_renewables",
                iso=iso,
                start=start,
                end=end,
                error=exc,
            )

    def _gridstatus_error_result(
        self,
        *,
        metric: str,
        iso: str,
        start: str,
        end: str,
        error: Exception,
        extra: Dict[str, Any] | None = None,
    ) -> GridStatusResult:
        details = str(error).strip() or repr(error)
        parameters: Dict[str, Any] = {
            "metric": metric,
            "iso": iso,
            "start": start,
            "end": end,
            "error": details,
        }
        if extra:
            parameters.update(extra)

        return GridStatusResult(
            df=pd.DataFrame(columns=["date", "value"]),
            source=SourceRef(
                source_type="gridstatus",
                label=f"GridStatus {metric} unavailable",
                reference=f"gridstatus:error_{metric}",
                parameters=parameters,
            ),
            meta={
                "error": details,
                "fallback": "empty_dataframe",
            },
        )
