# atlas/answers/chart_defaults.py
from schemas.chart import ChartSpec


def default_chart_for_metric(metric: str) -> ChartSpec | None:
    if metric == "ng_henry_hub_spot":
        return ChartSpec(
            kind="line",
            title="Henry Hub Natural Gas Spot Price",
            x="date",
            y="value",
        )

    if metric == "ng_working_gas_storage_lower48":
        return ChartSpec(
            kind="line",
            title="Working Gas in Storage (Lower 48)",
            x="date",
            y="value",
        )

    if metric == "ng_lng_exports":
        return ChartSpec(
            kind="line",
            title="U.S. LNG Exports",
            x="date",
            y="value",
        )

    return None
