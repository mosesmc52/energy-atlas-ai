#!/usr/bin/env python3
"""
Download NOAA GHCND daily station CSVs from:
  https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/

Filter stations from an input CSV where region == ..., download station files,
then compute a region-level daily aggregation and save.

Expected station CSV columns (minimum):
  - ghcnd_station_id
  - region
Optional:
  - station_name
  - state
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

NOAA_GHCND_ACCESS_BASE = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
)


# -----------------------------
# Data structures ("items")
# -----------------------------


@dataclass(frozen=True)
class StationMetaItem:
    region: str
    ghcnd_station_id: str
    station_name: Optional[str] = None
    state: Optional[str] = None


# -----------------------------
# Helpers
# -----------------------------


def c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def compute_hdd_from_tavg_c(tavg_c: float, base_f: float = 65.0) -> float:
    """HDD = max(0, baseF - TavgF)."""
    tavg_f = c_to_f(tavg_c)
    return max(0.0, base_f - tavg_f)


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def download_station_csv(station_id: str, out_dir: str, timeout: int = 60) -> str:
    safe_mkdir(out_dir)
    url = f"{NOAA_GHCND_ACCESS_BASE}{station_id}.csv"
    out_path = os.path.join(out_dir, f"{station_id}.csv")

    # simple cache
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return out_path

    r = requests.get(url, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(
            f"Failed download {station_id}: HTTP {r.status_code} url={url}"
        )

    with open(out_path, "wb") as f:
        f.write(r.content)

    return out_path


def read_and_normalize_station_file(
    station_id: str,
    filepath: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> pd.DataFrame:
    """
    Normalize station data to:
      region-independent fields: station_id, date, tavg_c, tmin_c, tmax_c, hdd
    Temperatures are tenths of °C in the NOAA access files.
    """
    df = pd.read_csv(filepath)

    if "DATE" not in df.columns:
        raise ValueError(f"{station_id}: missing DATE column in {filepath}")

    cols = ["DATE"]
    for c in ("TAVG", "TMIN", "TMAX"):
        if c in df.columns:
            cols.append(c)
    df = df[cols].copy()
    df.rename(columns={"DATE": "date"}, inplace=True)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")

    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date)]

    # Convert tenths °C -> °C
    for col in ("TAVG", "TMIN", "TMAX"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 10.0

    # Prefer TAVG; fallback to mean(TMIN, TMAX)
    df["tavg_c"] = df["TAVG"] if "TAVG" in df.columns else pd.NA
    df["tmin_c"] = df["TMIN"] if "TMIN" in df.columns else pd.NA
    df["tmax_c"] = df["TMAX"] if "TMAX" in df.columns else pd.NA

    mask_fill = df["tavg_c"].isna() & df["tmin_c"].notna() & df["tmax_c"].notna()
    df.loc[mask_fill, "tavg_c"] = (
        df.loc[mask_fill, "tmin_c"] + df.loc[mask_fill, "tmax_c"]
    ) / 2.0

    def _hdd(x):
        if pd.isna(x):
            return pd.NA
        return compute_hdd_from_tavg_c(float(x), base_f=65.0)

    df["hdd"] = df["tavg_c"].apply(_hdd)

    df["ghcnd_station_id"] = station_id
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    return df[["ghcnd_station_id", "date", "tavg_c", "tmin_c", "tmax_c", "hdd"]].copy()


def aggregate_region_daily(df_all: pd.DataFrame, region_id: str) -> pd.DataFrame:
    """
    Aggregate across stations by day.
    Outputs both median and mean. Median is recommended for robustness.
    """
    df = df_all.copy()
    for col in ("tavg_c", "hdd"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df_valid = df.dropna(subset=["tavg_c"]).copy()

    g = df_valid.groupby("date", as_index=False)
    agg = g.agg(
        n_stations_used=("ghcnd_station_id", "nunique"),
        tavg_c_median=("tavg_c", "median"),
        tavg_c_mean=("tavg_c", "mean"),
        hdd_median=("hdd", "median"),
        hdd_mean=("hdd", "mean"),
    ).sort_values("date")

    agg["tavg_f_median"] = agg["tavg_c_median"].apply(
        lambda x: c_to_f(float(x)) if pd.notna(x) else pd.NA
    )
    agg["tavg_f_mean"] = agg["tavg_c_mean"].apply(
        lambda x: c_to_f(float(x)) if pd.notna(x) else pd.NA
    )

    agg.insert(0, "region_id", region_id)

    return agg[
        [
            "region_id",
            "date",
            "n_stations_used",
            "tavg_c_median",
            "tavg_f_median",
            "hdd_median",
            "tavg_c_mean",
            "tavg_f_mean",
            "hdd_mean",
        ]
    ].copy()


def load_station_meta(csv_path: str, region_filter: str) -> List[StationMetaItem]:
    df = pd.read_csv(csv_path)

    if "ghcnd_station_id" not in df.columns:
        raise ValueError("stations csv missing required column: 'ghcnd_station_id'")

    # Backward compatibility:
    # - prefer explicit 'region'
    # - fall back to legacy 'pipeline'
    # - if neither exists, allow lower_48/all-stations mode
    if "region" in df.columns:
        pass
    elif "pipeline" in df.columns:
        df = df.rename(columns={"pipeline": "region"})
    else:
        if region_filter.strip().lower() != "lower_48":
            raise ValueError(
                "stations csv missing 'region' column. "
                "Provide a stations CSV with a 'region' (or legacy 'pipeline') column "
                "for region-specific filtering."
            )
        # Default all listed stations to lower_48 when no region column exists.
        df = df.copy()
        df["region"] = "lower_48"

    df["region"] = df["region"].astype(str).str.strip().str.lower()
    df["ghcnd_station_id"] = df["ghcnd_station_id"].astype(str).str.strip()

    df = df[df["region"] == region_filter.strip().lower()].copy()
    if df.empty:
        raise ValueError(f"No stations found for region == {region_filter!r}")

    items: List[StationMetaItem] = []
    for _, r in df.iterrows():
        items.append(
            StationMetaItem(
                region=str(r["region"]),
                ghcnd_station_id=str(r["ghcnd_station_id"]),
                station_name=(
                    str(r["station_name"])
                    if "station_name" in df.columns and pd.notna(r.get("station_name"))
                    else None
                ),
                state=(
                    str(r["state"])
                    if "state" in df.columns and pd.notna(r.get("state"))
                    else None
                ),
            )
        )

    # De-duplicate station ids
    seen = set()
    out = []
    for it in items:
        if it.ghcnd_station_id not in seen:
            out.append(it)
            seen.add(it.ghcnd_station_id)
    return out


# -----------------------------
# Main
# -----------------------------


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--stations-csv",
        default="scripts/noaa/major_airports_by_state_ghcnd.csv",
        required=False,
        help="Path to stations CSV",
    )
    p.add_argument(
        "--region",
        default="lower_48",
        help="Region filter (default: lower_48)",
    )
    p.add_argument("--out-dir", default="data/raw/noaa", help="Output directory")
    p.add_argument("--start", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument(
        "--days_ago", type=int, default=0, help="The number of days ago to start"
    )
    p.add_argument("--end", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")

    args = p.parse_args()

    region_id = args.region.strip().lower()

    # 1) Load stations
    stations = load_station_meta(args.stations_csv, region_filter=region_id)

    # 2) Download + normalize each station file
    station_dir = os.path.join(args.out_dir, "stations", region_id)
    agg_dir = os.path.join(args.out_dir, "regional")
    safe_mkdir(station_dir)
    safe_mkdir(agg_dir)

    frames = []

    start_date = None
    if args.start:
        start_date = args.start
    elif args.days_ago > 0:
        start_date = (datetime.now() - timedelta(days=args.days_ago)).strftime(
            "%Y-%m-%d"
        )

    for st in stations:
        fp = download_station_csv(
            st.ghcnd_station_id, out_dir=station_dir, timeout=args.timeout
        )
        df_st = read_and_normalize_station_file(
            station_id=st.ghcnd_station_id,
            filepath=fp,
            start_date=start_date,
            end_date=args.end,
        )
        frames.append(df_st)

    df_all = pd.concat(frames, ignore_index=True)

    # 3) Aggregate daily region series
    df_region = aggregate_region_daily(df_all, region_id=region_id)

    # 4) Save outputs (CSV)
    station_norm_path = os.path.join(agg_dir, f"{region_id}_stations_normalized.csv")
    region_path = os.path.join(agg_dir, f"{region_id}_region_daily.csv")
    meta_path = os.path.join(agg_dir, f"{region_id}_region_daily.meta.json")

    df_all.to_csv(station_norm_path, index=False)
    df_region.to_csv(region_path, index=False)

    meta = {
        "region_id": region_id,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stations_count": len(stations),
        "stations": [asdict(s) for s in stations],
        "source_base_url": NOAA_GHCND_ACCESS_BASE,
        "station_files_dir": station_dir,
        "outputs": {
            "stations_normalized_csv": station_norm_path,
            "region_daily_csv": region_path,
        },
        "date_filter": {"start_date": start_date, "end_date": args.end},
        "aggregation": {
            "tavg": "median and mean across stations",
            "hdd": "computed from regional tavg using base 65F",
            "temp_units": "GHCND tenths of C converted to C; also saved F",
        },
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"Saved station-normalized: {station_norm_path}")
    print(f"Saved region daily:      {region_path}")
    print(f"Saved metadata:          {meta_path}")


if __name__ == "__main__":
    main()
