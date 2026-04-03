from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from atlas.ingest.des_historical import (
    DEFAULT_PROCESSED_DIR,
    build_des_timeseries,
    fetch_des_breakeven_data,
    fetch_des_historical_all_data,
    fetch_des_historical_index_data,
    fetch_des_price_expectations,
)
from atlas.ingest.des_reports import (
    build_des_report_records,
    crawl_des_archive,
    parse_des_report,
)
from schemas.answer import SourceRef


DES_CANONICAL_METRICS: dict[str, dict[str, Any]] = {
    "des_business_activity_index": {"unit": "index", "label": "Business Activity Index"},
    "des_company_outlook_index": {"unit": "index", "label": "Company Outlook Index"},
    "des_outlook_uncertainty_index": {"unit": "index", "label": "Outlook Uncertainty Index"},
    "des_oil_production_index": {"unit": "index", "label": "Oil Production Index"},
    "des_gas_production_index": {"unit": "index", "label": "Gas Production Index"},
    "des_capex_index": {"unit": "index", "label": "Capital Expenditures Index"},
    "des_employment_index": {"unit": "index", "label": "Employment Index"},
    "des_input_cost_index": {"unit": "index", "label": "Input Cost Index"},
    "des_finding_development_costs_index": {"unit": "index", "label": "Finding and Development Costs Index"},
    "des_lease_operating_expense_index": {"unit": "index", "label": "Lease Operating Expense Index"},
    "des_prices_received_services_index": {"unit": "index", "label": "Prices Received for Services Index"},
    "des_equipment_utilization_index": {"unit": "index", "label": "Equipment Utilization Index"},
    "des_operating_margin_index": {"unit": "index", "label": "Operating Margin Index"},
    "des_wti_price_expectation_6m": {"unit": "$/bbl", "label": "WTI Price Expectation 6M"},
    "des_wti_price_expectation_1y": {"unit": "$/bbl", "label": "WTI Price Expectation 1Y"},
    "des_wti_price_expectation_2y": {"unit": "$/bbl", "label": "WTI Price Expectation 2Y"},
    "des_wti_price_expectation_5y": {"unit": "$/bbl", "label": "WTI Price Expectation 5Y"},
    "des_hh_price_expectation_6m": {"unit": "$/MMBtu", "label": "Henry Hub Price Expectation 6M"},
    "des_hh_price_expectation_1y": {"unit": "$/MMBtu", "label": "Henry Hub Price Expectation 1Y"},
    "des_hh_price_expectation_2y": {"unit": "$/MMBtu", "label": "Henry Hub Price Expectation 2Y"},
    "des_hh_price_expectation_5y": {"unit": "$/MMBtu", "label": "Henry Hub Price Expectation 5Y"},
    "des_breakeven_oil_us": {"unit": "$/bbl", "label": "Breakeven Oil Price U.S."},
    "des_breakeven_gas_us": {"unit": "$/MMBtu", "label": "Breakeven Gas Price U.S."},
    "des_breakeven_oil_permian": {"unit": "$/bbl", "label": "Breakeven Oil Price Permian"},
    "des_breakeven_oil_eagle_ford": {"unit": "$/bbl", "label": "Breakeven Oil Price Eagle Ford"},
    "des_special_questions_text": {"unit": "text", "label": "Special Questions"},
    "des_comments_text": {"unit": "text", "label": "Survey Comments"},
    "des_report_summary_text": {"unit": "text", "label": "Report Summary"},
}


@dataclass(frozen=True)
class DESResult:
    df: pd.DataFrame
    source: SourceRef
    meta: dict[str, Any] | None = None


class DallasEnergySurveyAdapter:
    def __init__(
        self,
        *,
        raw_dir: str | Path = "data/raw/des",
        processed_dir: str | Path = DEFAULT_PROCESSED_DIR,
    ) -> None:
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def sync_historical(self, *, force_refresh: bool = False) -> pd.DataFrame:
        processed_path = self.processed_dir / "des_historical"
        cached = self._load_table(processed_path)
        if cached is not None and not force_refresh:
            return cached

        frame = build_des_timeseries(
            [
                fetch_des_historical_index_data(raw_dir=self.raw_dir, force_refresh=force_refresh),
                fetch_des_historical_all_data(raw_dir=self.raw_dir, force_refresh=force_refresh),
                fetch_des_price_expectations(raw_dir=self.raw_dir, force_refresh=force_refresh),
                fetch_des_breakeven_data(raw_dir=self.raw_dir, force_refresh=force_refresh),
            ]
        )
        self._save_table(frame, processed_path)
        return frame

    def sync_reports(self, *, force_refresh: bool = False) -> pd.DataFrame:
        processed_path = self.processed_dir / "des_reports"
        cached = self._load_table(processed_path)
        if cached is not None and not force_refresh:
            return cached

        archive = crawl_des_archive(raw_dir=self.raw_dir, force_refresh=force_refresh)
        records = [
            parse_des_report(
                year=item["year"],
                quarter=item["quarter"],
                url=item["url"],
                raw_dir=self.raw_dir,
                force_refresh=force_refresh,
            )
            for item in archive
        ]
        frame = build_des_report_records(records)
        self._save_table(frame, processed_path)
        return frame

    def get_metric(
        self,
        metric_key: str,
        start_date: str | None = None,
        end_date: str | None = None,
        *,
        force_refresh: bool = False,
    ) -> DESResult:
        if metric_key not in DES_CANONICAL_METRICS:
            raise ValueError(f"Unsupported DES metric: {metric_key}")

        if metric_key.endswith("_text"):
            df = self._text_metric_frame(metric_key, force_refresh=force_refresh)
        else:
            historical = self.sync_historical(force_refresh=force_refresh)
            df = historical.loc[historical["metric"] == metric_key].copy()

        if start_date:
            df = df.loc[pd.to_datetime(df["date"], errors="coerce") >= pd.Timestamp(start_date)]
        if end_date:
            df = df.loc[pd.to_datetime(df["date"], errors="coerce") <= pd.Timestamp(end_date)]
        df = df.reset_index(drop=True)

        label = DES_CANONICAL_METRICS[metric_key]["label"]
        source = self._make_source(
            label=f"Dallas Fed Energy Survey: {label}",
            reference=f"dallasfed:des:{metric_key}",
            parameters={
                "metric": metric_key,
                "start_date": start_date,
                "end_date": end_date,
                "raw_dir": str(self.raw_dir),
                "processed_dir": str(self.processed_dir),
            },
        )
        return DESResult(
            df=df,
            source=source,
            meta={"metric": metric_key, "unit": DES_CANONICAL_METRICS[metric_key]["unit"]},
        )

    def get_latest(self, metric_key: str, *, force_refresh: bool = False) -> dict[str, Any] | None:
        result = self.get_metric(metric_key, force_refresh=force_refresh)
        if result.df.empty:
            return None
        row = result.df.sort_values("date").iloc[-1]
        return {col: row[col] for col in result.df.columns}

    def get_report(self, quarter_or_date: str, *, force_refresh: bool = False) -> dict[str, Any] | None:
        reports = self.sync_reports(force_refresh=force_refresh)
        key = str(quarter_or_date).strip().lower()
        if not key:
            return None
        mask = (
            reports["quarter"].astype(str).str.lower().eq(key)
            | reports["report_date"].astype(str).str.lower().eq(key)
        )
        if not mask.any():
            return None
        row = reports.loc[mask].sort_values("report_date").iloc[-1]
        return {col: row[col] for col in reports.columns}

    def search_report_text(
        self,
        query: str,
        start_date: str | None = None,
        *,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        reports = self.sync_reports(force_refresh=force_refresh).copy()
        if start_date:
            reports = reports.loc[reports["report_date"] >= pd.Timestamp(start_date)]
        haystack = (
            reports["summary"].fillna("")
            + "\n"
            + reports["special_questions_text"].fillna("")
            + "\n"
            + reports["comments_text"].fillna("")
            + "\n"
            + reports["price_forecasts_text"].fillna("")
        )
        mask = haystack.str.contains(str(query), case=False, na=False, regex=False)
        return reports.loc[mask].reset_index(drop=True)

    def get_special_questions(self, start_date: str | None = None, *, force_refresh: bool = False) -> pd.DataFrame:
        reports = self.sync_reports(force_refresh=force_refresh).copy()
        if start_date:
            reports = reports.loc[reports["report_date"] >= pd.Timestamp(start_date)]
        return reports.loc[
            reports["special_questions_text"].notna(),
            ["report_date", "year", "quarter", "title", "url", "special_questions_text"],
        ].reset_index(drop=True)

    def get_comments(self, start_date: str | None = None, *, force_refresh: bool = False) -> pd.DataFrame:
        reports = self.sync_reports(force_refresh=force_refresh).copy()
        if start_date:
            reports = reports.loc[reports["report_date"] >= pd.Timestamp(start_date)]
        return reports.loc[
            reports["comments_text"].notna(),
            ["report_date", "year", "quarter", "title", "url", "comments_text"],
        ].reset_index(drop=True)

    def _text_metric_frame(self, metric_key: str, *, force_refresh: bool = False) -> pd.DataFrame:
        reports = self.sync_reports(force_refresh=force_refresh).copy()
        column_map = {
            "des_special_questions_text": "special_questions_text",
            "des_comments_text": "comments_text",
            "des_report_summary_text": "summary",
        }
        source_col = column_map[metric_key]
        df = reports.loc[
            reports[source_col].notna(),
            ["report_date", "quarter", "year", "url", "title", source_col],
        ].copy()
        df = df.rename(columns={"report_date": "date", source_col: "value", "url": "source_url"})
        df["metric"] = metric_key
        df["unit"] = "text"
        df["region"] = "11th_district"
        df["frequency"] = "quarterly"
        df["source"] = "Dallas Fed"
        df["release_date"] = df["date"]
        df["vintage"] = datetime.utcnow().date().isoformat()
        df["file_name"] = None
        return df[
            [
                "date",
                "quarter",
                "metric",
                "value",
                "unit",
                "region",
                "frequency",
                "source",
                "source_url",
                "release_date",
                "vintage",
                "file_name",
                "year",
                "title",
            ]
        ]

    def _make_source(self, *, label: str, reference: str, parameters: dict[str, Any]) -> SourceRef:
        return SourceRef(
            source_type="dallasfed",
            label=label,
            reference=reference,
            parameters=parameters,
            retrieved_at=datetime.utcnow(),
        )

    def _load_table(self, base_path: Path) -> pd.DataFrame | None:
        parquet_path = base_path.with_suffix(".parquet")
        csv_path = base_path.with_suffix(".csv")
        if parquet_path.exists():
            return pd.read_parquet(parquet_path)
        if csv_path.exists():
            frame = pd.read_csv(csv_path)
            for col in ("date", "release_date", "report_date"):
                if col in frame.columns:
                    frame[col] = pd.to_datetime(frame[col], errors="coerce")
            return frame
        return None

    def _save_table(self, df: pd.DataFrame, base_path: Path) -> None:
        parquet_path = base_path.with_suffix(".parquet")
        csv_path = base_path.with_suffix(".csv")
        try:
            df.to_parquet(parquet_path, index=False)
            if csv_path.exists():
                csv_path.unlink()
        except Exception:
            df.to_csv(csv_path, index=False)
