from __future__ import annotations

from typing import Iterable

import pandas as pd


def quarter_over_quarter_change(
    df: pd.DataFrame,
    *,
    group_cols: Iterable[str] = ("metric", "region"),
    value_col: str = "value",
) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    sort_cols = [c for c in ["metric", "region", "date"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols)
    groups = [c for c in group_cols if c in out.columns]
    if groups:
        out["qoq_change"] = out.groupby(groups)[value_col].diff()
    else:
        out["qoq_change"] = out[value_col].diff()
    return out


def expectation_minus_spot(
    expectations: pd.DataFrame,
    spot: pd.DataFrame,
    *,
    expectation_value_col: str = "value",
    spot_value_col: str = "spot_value",
) -> pd.DataFrame:
    exp = expectations.copy()
    spot_df = spot.copy()
    exp["date"] = pd.to_datetime(exp["date"], errors="coerce")
    spot_df["date"] = pd.to_datetime(spot_df["date"], errors="coerce")
    exp[expectation_value_col] = pd.to_numeric(exp[expectation_value_col], errors="coerce")
    if "value" in spot_df.columns and spot_value_col not in spot_df.columns:
        spot_df = spot_df.rename(columns={"value": spot_value_col})
    spot_df[spot_value_col] = pd.to_numeric(spot_df[spot_value_col], errors="coerce")
    merged = exp.merge(spot_df[["date", spot_value_col]], on="date", how="left")
    merged["expectation_minus_spot"] = (
        merged[expectation_value_col] - merged[spot_value_col]
    )
    return merged


def price_minus_breakeven_margin(
    prices: pd.DataFrame,
    breakeven: pd.DataFrame,
    *,
    price_value_col: str = "value",
    breakeven_value_col: str = "breakeven_value",
) -> pd.DataFrame:
    price_df = prices.copy()
    be_df = breakeven.copy()
    price_df["date"] = pd.to_datetime(price_df["date"], errors="coerce")
    be_df["date"] = pd.to_datetime(be_df["date"], errors="coerce")
    price_df[price_value_col] = pd.to_numeric(price_df[price_value_col], errors="coerce")
    if "value" in be_df.columns and breakeven_value_col not in be_df.columns:
        be_df = be_df.rename(columns={"value": breakeven_value_col})
    be_df[breakeven_value_col] = pd.to_numeric(be_df[breakeven_value_col], errors="coerce")
    merged = price_df.merge(be_df[["date", breakeven_value_col]], on="date", how="left")
    merged["price_minus_breakeven_margin"] = (
        merged[price_value_col] - merged[breakeven_value_col]
    )
    return merged


def rolling_z_scores(
    df: pd.DataFrame,
    *,
    window: int = 4,
    group_cols: Iterable[str] = ("metric", "region"),
    value_col: str = "value",
) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    groups = [c for c in group_cols if c in out.columns]
    sort_cols = [c for c in [*groups, "date"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols)

    def _z(series: pd.Series) -> pd.Series:
        mean = series.rolling(window=window, min_periods=2).mean()
        std = series.rolling(window=window, min_periods=2).std(ddof=0)
        return (series - mean) / std.replace({0: pd.NA})

    if groups:
        out["rolling_zscore"] = out.groupby(groups)[value_col].transform(_z)
    else:
        out["rolling_zscore"] = _z(out[value_col])
    return out


def regime_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    value = pd.to_numeric(out.get("value"), errors="coerce")
    out["expansion"] = False
    out["contraction"] = False
    out["elevated_uncertainty"] = False
    out["margin_compression"] = False

    metric = out.get("metric")
    if metric is None:
        return out

    metric_series = metric.astype(str)
    out.loc[
        metric_series.eq("des_business_activity_index") & value.gt(0), "expansion"
    ] = True
    out.loc[
        metric_series.eq("des_business_activity_index") & value.lt(0), "contraction"
    ] = True
    out.loc[
        metric_series.eq("des_outlook_uncertainty_index") & value.ge(20),
        "elevated_uncertainty",
    ] = True
    out.loc[
        metric_series.eq("des_operating_margin_index") & value.lt(0),
        "margin_compression",
    ] = True
    return out
