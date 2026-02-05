from __future__ import annotations

from schemas.chart_spec import AxisSpec, ChartSpec, SeriesSpec  # wherever these live


def chart_policy(*, metric: str, mode: str, df) -> ChartSpec | None:
    if df is None or df.empty or len(df) < 2:
        return None

    # v0.1: simple “one series line chart” for the common metrics
    if metric == "henry_hub_spot":
        return ChartSpec(
            chart_type="line",
            title="Henry Hub Natural Gas Spot Price",
            x=AxisSpec(field="date", label="Date"),
            y=AxisSpec(field="value", label="$/MMBtu"),
            series=[
                SeriesSpec(
                    name="Henry Hub",
                    source="eia_api",
                    metric=metric,
                )
            ],
        )

    if metric == "working_gas_storage_lower48":
        return ChartSpec(
            chart_type="line",
            title="Working Gas in Storage (Lower 48)",
            x=AxisSpec(field="date", label="Date"),
            y=AxisSpec(field="value", label="Bcf"),
            series=[
                SeriesSpec(
                    name="Working Gas",
                    source="eia_api",
                    metric=metric,
                )
            ],
        )

    if metric == "lng_exports":
        return ChartSpec(
            chart_type="line",
            title="U.S. LNG Exports",
            x=AxisSpec(field="date", label="Date"),
            y=AxisSpec(field="value", label="Volume"),
            series=[
                SeriesSpec(
                    name="LNG Exports",
                    source="eia_api",
                    metric=metric,
                )
            ],
        )

    return None
