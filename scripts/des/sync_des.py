#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from tools.des_adapter import DallasEnergySurveyAdapter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Dallas Fed Energy Survey data.")
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--include-reports", action="store_true")
    parser.add_argument("--include-historical", action="store_true")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--out-dir", type=str, default="data/processed/des")
    parser.add_argument("--format", choices=["json", "parquet", "csv"], default="parquet")
    return parser


def _write_output(df, path: Path, fmt: str) -> None:
    if fmt == "parquet":
        df.to_parquet(path.with_suffix(".parquet"), index=False)
    elif fmt == "csv":
        df.to_csv(path.with_suffix(".csv"), index=False)
    else:
        df.to_json(path.with_suffix(".json"), orient="records", indent=2, date_format="iso")


def main() -> None:
    args = build_parser().parse_args()
    adapter = DallasEnergySurveyAdapter(processed_dir=args.out_dir)

    include_historical = args.include_historical or not args.include_reports
    if include_historical:
        historical = adapter.sync_historical(force_refresh=args.force_refresh)
        if args.start_date:
            historical = historical.loc[historical["date"] >= args.start_date]
        if args.end_date:
            historical = historical.loc[historical["date"] <= args.end_date]
        if historical.empty:
            raise SystemExit("DES historical sync returned no rows.")
        _write_output(historical, Path(args.out_dir) / "des_historical_export", args.format)
        print(f"Ingested {len(historical)} historical rows")

    if args.include_reports:
        reports = adapter.sync_reports(force_refresh=args.force_refresh)
        if args.start_date:
            reports = reports.loc[reports["report_date"] >= args.start_date]
        if args.end_date:
            reports = reports.loc[reports["report_date"] <= args.end_date]
        if reports.empty:
            raise SystemExit("DES report sync returned no rows.")
        _write_output(reports, Path(args.out_dir) / "des_reports_export", args.format)
        print(f"Ingested {len(reports)} report rows")


if __name__ == "__main__":
    main()
