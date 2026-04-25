from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from utils.query_intents import has_seasonal_norm_phrase


def ng_electricity_seasonal_norm_summary(
    *,
    df: pd.DataFrame,
    normal_years: int,
) -> Optional[dict[str, Any]]:
    if df is None or df.empty or "date" not in df.columns or "value" not in df.columns:
        return None

    ordered = df.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
    ordered = ordered.dropna(subset=["date", "value"]).sort_values("date")
    if ordered.empty:
        return None

    latest = ordered.iloc[-1]
    latest_date = pd.Timestamp(latest["date"])
    latest_value = float(latest["value"])

    hist = ordered[ordered["date"] < latest_date].copy()
    if hist.empty:
        return None

    cutoff = latest_date - pd.DateOffset(years=max(1, int(normal_years)))
    hist = hist[hist["date"] >= cutoff]
    hist = hist[hist["date"].dt.month == latest_date.month]
    if hist.empty:
        return None

    normal_value = float(hist["value"].mean())
    delta_vs_normal = latest_value - normal_value
    pct_vs_normal = None
    if normal_value != 0:
        pct_vs_normal = (delta_vs_normal / normal_value) * 100.0

    samples = int(len(hist))
    return {
        "latest_date": latest_date.date().isoformat(),
        "latest_value": latest_value,
        "normal_value": normal_value,
        "delta_vs_normal": delta_vs_normal,
        "pct_vs_normal": pct_vs_normal,
        "normal_years": max(1, int(normal_years)),
        "samples": samples,
    }


def should_compute_ng_electricity_seasonal_norm(query: str) -> bool:
    return has_seasonal_norm_phrase(query)
