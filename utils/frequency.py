from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FrequencyInfo:
    freq: str  # "daily", "weekly", "monthly", "irregular"
    pandas_freq: Optional[str]  # "D", "W", "MS", or None
    step_days: Optional[int]  # e.g. 1, 7, 30, or None
    confidence: float  # 0..1
    n_points: int


def infer_frequency_daily_base(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    min_points: int = 12,
) -> FrequencyInfo:
    """
    Infer timeseries cadence using daily as the base.
    Works best on normalized, sorted data (date normalized to midnight).
    """
    if df is None or df.empty or date_col not in df.columns:
        return FrequencyInfo("irregular", None, None, 0.0, 0)

    d = pd.to_datetime(df[date_col], errors="coerce").dropna().sort_values().unique()
    n = len(d)
    if n < 2:
        return FrequencyInfo("irregular", None, None, 0.0, n)
    if n < min_points:
        # still infer, but lower confidence
        small_sample_penalty = 0.25
    else:
        small_sample_penalty = 0.0

    # Differences in days
    diffs = np.diff(d).astype("timedelta64[D]").astype(int)
    diffs = diffs[diffs > 0]
    if len(diffs) == 0:
        return FrequencyInfo("irregular", None, None, 0.0, n)

    # Dominant step (mode)
    vals, counts = np.unique(diffs, return_counts=True)
    mode_step = int(vals[np.argmax(counts)])
    mode_share = float(np.max(counts) / np.sum(counts))

    # Daily-first classification
    # (We allow minor missing days; cadence is still daily if mode_step==1)
    if mode_step == 1:
        # Confidence high if most gaps are 1 day
        conf = max(0.0, min(1.0, mode_share - small_sample_penalty))
        return FrequencyInfo("daily", "D", 1, conf, n)

    # Weekly (mode 7) is common for storage-type series
    if mode_step == 7:
        conf = max(0.0, min(1.0, mode_share - small_sample_penalty))
        return FrequencyInfo("weekly", "W", 7, conf, n)

    # Monthly-ish: many EIA monthly series are irregular around 28–31 days
    # Detect if diffs cluster around ~30.
    if 28 <= mode_step <= 31:
        conf = max(0.0, min(1.0, mode_share - small_sample_penalty))
        return FrequencyInfo("monthly", "MS", 30, conf, n)

    # Otherwise irregular (could be business days, holidays, etc.)
    conf = max(0.0, min(1.0, mode_share - small_sample_penalty))
    return FrequencyInfo("irregular", None, mode_step, conf, n)
