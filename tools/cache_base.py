from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CacheFetchInfo:
    cache_path: str
    cache_hit: bool
    fetched_segments: List[Dict[str, Any]]
    inferred_freq: Optional[str] = None
    freq_confidence: Optional[float] = None


class CacheBackedTimeseriesAdapterBase:
    """
    Reusable CSV cache layer for time series adapters.

    Responsibilities (generic):
      - Load CSV cache
      - Normalize dates
      - Infer frequency (daily-first)
      - Compute missing segments (daily internal gaps; edge fill fallback)
      - Fetch missing segments via subclass-provided fetch function
      - Merge/dedupe/sort
      - Save to CSV atomically
      - Return requested window + cache metadata

    Subclasses provide:
      - _fetch_timeseries(...) -> pd.DataFrame
      - _normalize_df(...)
      - _dedupe_cols(...)
      - _cache_key(...)
    """

    def __init__(
        self,
        *,
        cache_dir: str | Path = "data/cache",
        date_col: str = "date",
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.date_col = date_col

    # -------------------------
    # Public entrypoint used by subclasses
    # -------------------------

    def _cached_timeseries(
        self,
        *,
        metric_key: str,
        start: str,
        end: str,
        cache_key_parts: Dict[str, Any] | None = None,
        fetch_ctx: Dict[str, Any] | None = None,
        allow_internal_gap_fill_daily: bool = True,
    ) -> tuple[pd.DataFrame, CacheFetchInfo]:
        """
        Generic cache-backed timeseries fetch.

        Returns:
          (df_window, CacheFetchInfo)
        """
        cache_key_parts = cache_key_parts or {}
        fetch_ctx = fetch_ctx or {}

        start_ts = self._norm_date(start)
        end_ts = self._norm_date(end)

        cache_path = self._cache_path(metric_key, cache_key_parts)
        df_cache = self._load_cache(cache_path)
        if df_cache is not None and not df_cache.empty:
            df_cache = self._normalize_df(df_cache)

        # Decide missing segments
        freq = (
            self._infer_frequency_daily_base(df_cache)
            if df_cache is not None and not df_cache.empty
            else None
        )
        missing = self._missing_segments(
            df_cache,
            start_ts,
            end_ts,
            freq=freq,
            allow_internal_gap_fill_daily=allow_internal_gap_fill_daily,
        )

        fetched_segments: List[Dict[str, Any]] = []
        df_merged = df_cache

        for seg_start, seg_end in missing:
            df_new = self._fetch_timeseries(
                start=seg_start.date().isoformat(),
                end=seg_end.date().isoformat(),
                **fetch_ctx,
            )
            df_new = self._normalize_df(df_new)

            fetched_segments.append(
                {
                    "start": seg_start.date().isoformat(),
                    "end": seg_end.date().isoformat(),
                    "rows": int(len(df_new)),
                }
            )
            df_merged = self._merge_cache(df_merged, df_new)

        # Persist if we had to fetch or cache didn't exist
        if fetched_segments or df_cache is None:
            if df_merged is None:
                df_merged = pd.DataFrame(columns=[self.date_col])
            self._save_cache(cache_path, df_merged)

        df_out = self._slice_window(df_merged, start_ts, end_ts)

        info = CacheFetchInfo(
            cache_path=str(cache_path),
            cache_hit=bool(
                df_cache is not None and not df_cache.empty and len(missing) == 0
            ),
            fetched_segments=fetched_segments,
            inferred_freq=(freq["freq"] if freq else None),
            freq_confidence=(freq["confidence"] if freq else None),
        )
        return df_out, info

    # -------------------------
    # Subclass hooks
    # -------------------------

    def _fetch_timeseries(self, start: str, end: str, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Must at minimum:
          - ensure self.date_col exists and is datetime64[ns]
          - normalize to midnight (.dt.normalize()) to support set operations
          - sort by date
        """
        raise NotImplementedError

    def _dedupe_cols(self, df: pd.DataFrame) -> List[str]:
        """
        Return columns used to drop duplicates in merged cache.
        Default: [date_col], but subclasses can add series_id etc.
        """
        return [self.date_col]

    # -------------------------
    # Cache I/O
    # -------------------------

    def _cache_path(self, metric_key: str, parts: Dict[str, Any]) -> Path:
        elems = [metric_key]
        for k in sorted(parts.keys()):
            elems.append(f"{k}={parts[k]}")
        safe = "__".join(elems).replace("/", "_").replace(" ", "_")
        return self.cache_dir / f"{safe}.csv"

    def _load_cache(self, path: Path) -> Optional[pd.DataFrame]:
        if not path.exists():
            return None
        try:
            return pd.read_csv(path)
        except Exception:
            return None

    def _save_cache(self, path: Path, df: pd.DataFrame) -> None:
        tmp = path.with_suffix(".tmp.csv")
        df.to_csv(tmp, index=False)
        tmp.replace(path)

    def _merge_cache(
        self, df_old: Optional[pd.DataFrame], df_new: pd.DataFrame
    ) -> pd.DataFrame:
        if df_old is None or df_old.empty:
            merged = df_new.copy()
        else:
            merged = pd.concat([df_old, df_new], ignore_index=True)

        dedupe_cols = [c for c in self._dedupe_cols(merged) if c in merged.columns]
        if not dedupe_cols:
            dedupe_cols = [self.date_col] if self.date_col in merged.columns else []

        if dedupe_cols:
            merged = merged.drop_duplicates(subset=dedupe_cols, keep="last")

        merged = merged.sort_values(self.date_col).reset_index(drop=True)
        return merged

    def _slice_window(
        self, df: Optional[pd.DataFrame], start: pd.Timestamp, end: pd.Timestamp
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        d = pd.to_datetime(df[self.date_col], errors="coerce").dropna()
        mask = (d >= start) & (d <= end)
        out = df.loc[mask].copy()
        out = out.sort_values(self.date_col).reset_index(drop=True)
        return out

    # -------------------------
    # Missing coverage logic
    # -------------------------

    def _missing_segments(
        self,
        df: Optional[pd.DataFrame],
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        freq: Optional[Dict[str, Any]],
        allow_internal_gap_fill_daily: bool,
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        if df is None or df.empty or self.date_col not in df.columns:
            return [(start, end)]

        d = pd.to_datetime(df[self.date_col], errors="coerce").dropna().dt.normalize()
        in_win = df[(d >= start) & (d <= end)]
        if in_win.empty:
            return [(start, end)]

        # If daily and allowed: fill internal gaps
        if allow_internal_gap_fill_daily and freq and freq.get("freq") == "daily":
            return self._missing_segments_daily(in_win, start, end)

        # Otherwise: edge fill only (safe default across unknown cadences)
        d2 = (
            pd.to_datetime(in_win[self.date_col], errors="coerce")
            .dropna()
            .dt.normalize()
        )
        min_d = d2.min()
        max_d = d2.max()

        segs: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        if start < min_d:
            segs.append((start, min_d))
        if end > max_d:
            segs.append((max_d, end))
        return segs

    def _missing_segments_daily(
        self, df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        observed = (
            pd.to_datetime(df[self.date_col], errors="coerce").dropna().dt.normalize()
        )
        observed = pd.DatetimeIndex(observed.unique())
        expected = pd.date_range(start=start, end=end, freq="D")
        missing = expected.difference(observed)
        return self._compress_dates_to_segments(missing)

    def _compress_dates_to_segments(
        self, missing: pd.DatetimeIndex
    ) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
        if len(missing) == 0:
            return []
        missing = missing.sort_values()
        segs: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
        seg_start = missing[0]
        prev = missing[0]
        for dt in missing[1:]:
            if (dt - prev).days == 1:
                prev = dt
                continue
            segs.append((seg_start, prev))
            seg_start = dt
            prev = dt
        segs.append((seg_start, prev))
        return segs

    # -------------------------
    # Frequency inference (daily-first)
    # -------------------------

    def _infer_frequency_daily_base(
        self,
        df: pd.DataFrame,
        *,
        min_points: int = 12,
    ) -> Dict[str, Any]:
        d = (
            pd.to_datetime(df[self.date_col], errors="coerce")
            .dropna()
            .dt.normalize()
            .sort_values()
            .unique()
        )
        n = len(d)
        if n < 2:
            return {"freq": "irregular", "confidence": 0.0, "n_points": n}

        diffs = np.diff(d).astype("timedelta64[D]").astype(int)
        diffs = diffs[diffs > 0]
        if len(diffs) == 0:
            return {"freq": "irregular", "confidence": 0.0, "n_points": n}

        vals, counts = np.unique(diffs, return_counts=True)
        mode_step = int(vals[np.argmax(counts)])
        mode_share = float(np.max(counts) / np.sum(counts))

        small_sample_penalty = 0.25 if n < min_points else 0.0
        conf = max(0.0, min(1.0, mode_share - small_sample_penalty))

        if mode_step == 1:
            return {"freq": "daily", "confidence": conf, "step_days": 1, "n_points": n}
        if mode_step == 7:
            return {"freq": "weekly", "confidence": conf, "step_days": 7, "n_points": n}
        if 28 <= mode_step <= 31:
            return {
                "freq": "monthly",
                "confidence": conf,
                "step_days": mode_step,
                "n_points": n,
            }

        return {
            "freq": "irregular",
            "confidence": conf,
            "step_days": mode_step,
            "n_points": n,
        }

    # -------------------------
    # Utils
    # -------------------------

    def _norm_date(self, x: str) -> pd.Timestamp:
        return pd.to_datetime(x).tz_localize(None).normalize()
