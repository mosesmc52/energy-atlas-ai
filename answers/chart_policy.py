from __future__ import annotations

from typing import Iterable

import pandas as pd

from schemas.chart_spec import ChartSpec


TREND_TERMS = (
    "trend",
    "over time",
    "this week",
    "this month",
    "this year",
    "last 30",
    "last 90",
    "last 365",
    "since",
    "historically",
    "history",
    "time series",
    "trajectory",
)

COMPARISON_TERMS = (
    "year over year",
    "yoy",
    "month over month",
    "mom",
    "compared to last year",
    "vs",
    "versus",
    "compare",
)

VOLATILITY_TERMS = (
    "spike",
    "volatility",
    "unusual",
    "highest",
    "lowest",
    "record",
    "extreme",
)

COMPOSITION_TERMS = (
    "share",
    "mix",
    "breakdown",
    "by fuel",
)

RELATIONSHIP_TERMS = (
    "correlation",
    "relationship",
    "drives",
    "impact of",
)

MULTI_SERIES_TERMS = (
    "overlay",
    "together",
    "how does",
)

DEFINITION_TERMS = ("define", "definition", "explain")

SINGLE_VALUE_TERMS = (
    "latest",
    "current",
    "today",
    "right now",
    "how much is",
)


METRIC_TITLES = {
    "henry_hub_spot": "Henry Hub Natural Gas Spot Price",
    "working_gas_storage_lower48": "Working Gas in Storage",
    "working_gas_storage_change_weekly": "Weekly Change in Working Gas Storage",
    "lng_exports": "U.S. Natural Gas Exports",
    "lng_imports": "U.S. Natural Gas Imports",
    "ng_electricity": "Natural Gas Electricity Generation",
    "ng_consumption_lower48": "Natural Gas Consumption (Lower 48)",
    "ng_consumption_by_sector": "Natural Gas Consumption by Sector",
    "ng_production_lower48": "Natural Gas Production (Lower 48)",
    "iso_load": "ISO Load",
    "iso_gas_dependency": "ISO Gas Dependency",
    "iso_renewables": "ISO Renewables (Wind + Solar)",
    "iso_fuel_mix": "ISO Fuel Mix",
    "des_business_activity_index": "Dallas Fed Business Activity Index",
    "des_company_outlook_index": "Dallas Fed Company Outlook Index",
    "des_outlook_uncertainty_index": "Dallas Fed Outlook Uncertainty Index",
    "des_oil_production_index": "Dallas Fed Oil Production Index",
    "des_gas_production_index": "Dallas Fed Gas Production Index",
    "des_capex_index": "Dallas Fed Capital Expenditures Index",
    "des_wti_price_expectation_1y": "Dallas Fed WTI Price Expectations",
    "des_hh_price_expectation_1y": "Dallas Fed Henry Hub Price Expectations",
    "des_breakeven_oil_us": "Dallas Fed Break-even Oil Price",
    "des_breakeven_gas_us": "Dallas Fed Break-even Gas Price",
    "managed_money_long": "CFTC Managed Money Long",
    "managed_money_short": "CFTC Managed Money Short",
    "managed_money_net": "CFTC Managed Money Net",
    "managed_money_net_percentile_156w": "CFTC Managed Money Net Percentile",
    "open_interest": "CFTC Open Interest",
}

METRIC_Y_LABELS = {
    "henry_hub_spot": "$/MMBtu",
    "des_wti_price_expectation_1y": "$/bbl",
    "des_hh_price_expectation_1y": "$/MMBtu",
    "des_breakeven_oil_us": "$/bbl",
    "des_breakeven_gas_us": "$/MMBtu",
    "managed_money_long": "Contracts",
    "managed_money_short": "Contracts",
    "managed_money_net": "Contracts",
    "managed_money_net_percentile_156w": "Percentile",
    "open_interest": "Contracts",
}


def _has_any(text: str, terms: Iterable[str]) -> bool:
    return any(t in text for t in terms)


def _looks_like_definition_query(text: str) -> bool:
    q = text.strip().lower()
    if _has_any(q, DEFINITION_TERMS):
        return True

    if not (q.startswith("what is ") or q.startswith("what's ")):
        return False

    # If query includes clear data intent, this is not a pure definition ask.
    data_cues = (
        "today",
        "yesterday",
        "last ",
        "this ",
        "since",
        "over time",
        "trend",
        "price",
        "storage",
        "load",
        "mix",
        "dependency",
        "exports",
        "imports",
        "production",
        "consumption",
        "generation",
    )
    return not _has_any(q, data_cues)


def _numeric_columns(df) -> list[str]:
    out: list[str] = []
    for c in df.columns:
        if c == "date":
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            out.append(c)
    return out


def _top_fuels(df, limit: int = 5) -> list[str]:
    exclude = {
        "total_generation_mw",
        "total_generation",
        "gas_share",
        "gas_burn_mmbtu_per_hour",
        "gas_burn_mcf_per_hour",
        "value",
    }
    fuels = [c for c in _numeric_columns(df) if c not in exclude]
    if not fuels:
        return []

    means = df[fuels].apply(pd.to_numeric, errors="coerce").mean(numeric_only=True)
    ranked = means.sort_values(ascending=False)
    return [c for c in ranked.index[:limit].tolist() if c in df.columns]


def _default_y(metric: str, df) -> list[str]:
    defaults = {
        "henry_hub_spot": ["value"],
        "working_gas_storage_lower48": ["value"],
        "working_gas_storage_change_weekly": ["value"],
        "lng_exports": ["value"],
        "lng_imports": ["value"],
        "ng_electricity": ["value"],
        "ng_consumption_lower48": ["value"],
        "ng_consumption_by_sector": ["value"],
        "ng_production_lower48": ["value"],
        "iso_load": ["value"],
        "iso_gas_dependency": ["gas_share", "gas_generation"],
        "iso_renewables": ["renewable_generation"],
        "des_business_activity_index": ["value"],
        "des_company_outlook_index": ["value"],
        "des_outlook_uncertainty_index": ["value"],
        "des_oil_production_index": ["value"],
        "des_gas_production_index": ["value"],
        "des_capex_index": ["value"],
        "des_wti_price_expectation_1y": ["value"],
        "des_hh_price_expectation_1y": ["value"],
        "des_breakeven_oil_us": ["value"],
        "des_breakeven_gas_us": ["value"],
        "managed_money_long": ["value"],
        "managed_money_short": ["value"],
        "managed_money_net": ["value"],
        "managed_money_net_percentile_156w": ["value"],
        "open_interest": ["value"],
    }
    preferred = defaults.get(metric, ["value"])
    y = [c for c in preferred if c in df.columns]
    if y:
        return y

    nums = _numeric_columns(df)
    return nums[:1]


def _default_y_label(metric: str, y_fields: list[str]) -> str:
    if metric in METRIC_Y_LABELS:
        return METRIC_Y_LABELS[metric]
    if y_fields:
        return y_fields[0].replace("_", " ").title()
    return "Value"


def chart_policy(*, metric: str, mode: str, df, query: str = "") -> ChartSpec | None:
    if df is None or df.empty:
        return None

    q = (query or "").strip().lower()
    n_points = len(df)

    if _looks_like_definition_query(q):
        return None

    has_trend = _has_any(q, TREND_TERMS)
    has_comparison = _has_any(q, COMPARISON_TERMS)
    has_volatility = _has_any(q, VOLATILITY_TERMS)
    has_composition = _has_any(q, COMPOSITION_TERMS)
    has_relationship = _has_any(q, RELATIONSHIP_TERMS)
    has_multi_series = _has_any(q, MULTI_SERIES_TERMS)

    explicit_chart_intent = any(
        [
            has_trend,
            has_comparison,
            has_volatility,
            has_composition,
            has_relationship,
            has_multi_series,
        ]
    )

    # Single-scalar asks should not auto-chart unless user clearly asks for trend/change.
    if _has_any(q, SINGLE_VALUE_TERMS) and not explicit_chart_intent:
        return None

    # Very small windows should not chart except a comparison bar.
    if n_points < 3 and not has_comparison:
        return None

    title = METRIC_TITLES.get(metric, "Energy Metric")

    if has_relationship:
        nums = _numeric_columns(df)
        if len(nums) < 2:
            return None
        return ChartSpec(
            chart_type="scatter",
            title=f"{title}: Relationship View",
            x=nums[0],
            y=[nums[1]],
            x_label=nums[0].replace("_", " ").title(),
            y_label=nums[1].replace("_", " ").title(),
            notes="Scatter plot selected for variable relationship query.",
        )

    if has_volatility:
        y = _default_y(metric, df)
        if not y:
            return None
        return ChartSpec(
            chart_type="histogram",
            title=f"{title}: Distribution",
            x="date",
            y=y,
            x_label=y[0].replace("_", " ").title(),
            y_label="Frequency",
        )

    if metric == "iso_renewables":
        share_view = _has_any(q, ("share", "mix", "percent", "percentage"))
        breakdown_view = _has_any(
            q,
            (
                "wind and solar",
                "solar and wind",
                "wind solar",
                "breakdown",
                "by source",
                "split",
            ),
        )

        if share_view and "renewable_share" in df.columns:
            return ChartSpec(
                chart_type="line",
                title=f"{title}: Share",
                x="date",
                y=["renewable_share"],
                x_label="Date",
                y_label="Renewable Share",
                notes="v1 renewables include wind + solar only.",
            )

        if breakdown_view:
            ys = [c for c in ("wind_generation", "solar_generation") if c in df.columns]
            if ys:
                return ChartSpec(
                    chart_type="stacked_area",
                    title=f"{title}: Wind + Solar Breakdown",
                    x="date",
                    y=ys,
                    x_label="Date",
                    y_label="Generation (MW)",
                    notes="v1 renewables include wind + solar only.",
                )

        if "renewable_generation" in df.columns:
            return ChartSpec(
                chart_type="line",
                title=title,
                x="date",
                y=["renewable_generation"],
                x_label="Date",
                y_label="Renewable Generation",
                notes="v1 renewables include wind + solar only.",
            )

    if metric == "iso_fuel_mix" or has_composition:
        fuels = _top_fuels(df, limit=5)
        if not fuels:
            fuels = _default_y(metric, df)
        if not fuels:
            return None

        share_view = _has_any(q, ("share", "mix", "breakdown"))
        return ChartSpec(
            chart_type="stacked_area",
            title=f"{title}: {'Share' if share_view else 'Generation'}",
            x="date",
            y=fuels,
            x_label="Date",
            y_label="Share of Generation" if share_view else "Generation (MW)",
            groupnorm="fraction" if share_view else None,
        )

    if has_comparison:
        y = _default_y(metric, df)
        if not y:
            return None
        return ChartSpec(
            chart_type="bar",
            title=f"{title}: Period Comparison",
            x="date",
            y=y,
            x_label="Date",
            y_label=y[0].replace("_", " ").title(),
            aggregation="monthly" if n_points > 28 else "none",
        )

    if metric == "iso_gas_dependency" or has_multi_series:
        y = [
            c
            for c in ("gas_share", "gas_generation", "gas_burn_mmbtu_per_hour")
            if c in df.columns
        ]
        if len(y) >= 2:
            return ChartSpec(
                chart_type="line",
                title=title,
                x="date",
                y=y[:2],
                x_label="Date",
                y_label="Value",
                notes="Gas burn is a proxy using heat_rate=7.5 MMBtu/MWh.",
            )

    if has_trend or metric in {
        "henry_hub_spot",
        "working_gas_storage_lower48",
        "working_gas_storage_change_weekly",
        "lng_exports",
        "lng_imports",
        "ng_electricity",
        "ng_consumption_lower48",
        "ng_production_lower48",
        "iso_load",
        "iso_renewables",
    }:
        y = _default_y(metric, df)
        if not y:
            return None
        return ChartSpec(
            chart_type="line",
            title=title,
            x="date",
            y=y,
            x_label="Date",
            y_label=_default_y_label(metric, y),
        )

    return None
