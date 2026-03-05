from __future__ import annotations

from typing import Any

from schemas.chart_spec import AxisSpec, ChartSpec, SeriesSpec  # wherever these live


def _infer_chart_intent(query: str) -> str:
    q = (query or "").lower()
    comparison_terms = (
        "compare",
        "comparison",
        "vs",
        "versus",
        "higher than",
        "lower than",
        "bigger",
        "smaller",
    )
    balance_terms = (
        "balance",
        "supply vs demand",
        "supply and demand",
        "tight",
        "oversupplied",
    )
    trend_terms = (
        "trend",
        "over time",
        "history",
        "historical",
        "changed",
        "rising",
        "falling",
        "increase",
        "decrease",
    )

    if any(t in q for t in balance_terms):
        return "balance"
    if any(t in q for t in comparison_terms):
        return "comparison"
    if any(t in q for t in trend_terms):
        return "trend"
    return "trend"


def _metric_config(metric: str) -> dict[str, Any] | None:
    config = {
        "henry_hub_spot": {
            "title": "Henry Hub Natural Gas Spot Price",
            "series": "Henry Hub",
            "y_label": "Price",
            "y_units": "$/MMBtu",
        },
        "working_gas_storage_lower48": {
            "title": "Working Gas in Storage (Lower 48)",
            "series": "Working Gas",
            "y_label": "Storage",
            "y_units": "Bcf",
        },
        "lng_exports": {
            "title": "U.S. LNG Exports",
            "series": "LNG Exports",
            "y_label": "Volume",
            "y_units": None,
        },
        "lng_imports": {
            "title": "U.S. LNG Imports",
            "series": "LNG Imports",
            "y_label": "Volume",
            "y_units": None,
        },
        "ng_electricity": {
            "title": "Natural Gas Electricity Generation",
            "series": "NG Electricity",
            "y_label": "Generation",
            "y_units": None,
        },
        "ng_consumption_lower48": {
            "title": "Natural Gas Consumption (Lower 48)",
            "series": "NG Consumption",
            "y_label": "Consumption",
            "y_units": None,
        },
        "ng_production_lower48": {
            "title": "Natural Gas Production (Lower 48)",
            "series": "NG Production",
            "y_label": "Production",
            "y_units": None,
        },
        "ng_exploration_reserves_lower48": {
            "title": "Natural Gas Reserves",
            "series": "NG Reserves",
            "y_label": "Reserves",
            "y_units": None,
        },
    }
    return config.get(metric)


def chart_policy(*, metric: str, mode: str, df, query: str = "") -> ChartSpec | None:
    if df is None or df.empty or len(df) < 2:
        return None

    cfg = _metric_config(metric)
    if cfg is None:
        return None

    intent = _infer_chart_intent(query=query)
    chart_type = "bar" if intent == "comparison" else "line"
    notes = None
    if intent == "balance":
        notes = "Balance view requested; showing observed trend for this metric."

    return ChartSpec(
        chart_type=chart_type,
        title=cfg["title"],
        x=AxisSpec(field="date", label="Date"),
        y=AxisSpec(field="value", label=cfg["y_label"], units=cfg["y_units"]),
        series=[
            SeriesSpec(
                name=cfg["series"],
                source="eia_api",
                metric=metric,
            )
        ],
        notes=notes,
    )
