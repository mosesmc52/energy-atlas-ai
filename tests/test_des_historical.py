from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from atlas.ingest.des_historical import _normalize_wide_sheet
from tools.des_adapter import DallasEnergySurveyAdapter


class TestDesHistoricalNormalization(unittest.TestCase):
    def test_normalize_index_sheet_maps_canonical_metrics(self) -> None:
        frame = pd.DataFrame(
            {
                "Quarter": ["2025 Q1", "2025 Q2"],
                "Business Activity": [12.4, 15.1],
                "Company Outlook": [-1.0, 2.5],
                "Oil Production": [4.2, 5.1],
            }
        )

        normalized = _normalize_wide_sheet(
            frame,
            category="index",
            source_url="https://example.com/des-index.xlsx",
            file_name="des-index.xlsx",
        )

        self.assertEqual(
            set(normalized["metric"]),
            {
                "des_business_activity_index",
                "des_company_outlook_index",
                "des_oil_production_index",
            },
        )
        self.assertEqual(list(normalized.columns[:5]), ["date", "quarter", "metric", "value", "unit"])
        self.assertEqual(normalized.loc[normalized["metric"] == "des_business_activity_index", "quarter"].iloc[0], "2025Q1")

    def test_adapter_get_metric_uses_cached_processed_table_and_date_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            processed_dir = Path(tmp) / "processed"
            raw_dir = Path(tmp) / "raw"
            processed_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                {
                    "date": pd.to_datetime(["2025-03-31", "2025-06-30"]),
                    "quarter": ["2025Q1", "2025Q2"],
                    "metric": ["des_business_activity_index", "des_business_activity_index"],
                    "value": [10.0, 14.0],
                    "unit": ["index", "index"],
                    "region": ["us", "us"],
                    "frequency": ["quarterly", "quarterly"],
                    "source": ["Dallas Fed", "Dallas Fed"],
                    "source_url": ["u1", "u2"],
                    "release_date": pd.to_datetime(["2025-03-31", "2025-06-30"]),
                    "vintage": ["2026-04-03", "2026-04-03"],
                    "file_name": ["a.xlsx", "a.xlsx"],
                }
            ).to_csv(processed_dir / "des_historical.csv", index=False)

            adapter = DallasEnergySurveyAdapter(raw_dir=raw_dir, processed_dir=processed_dir)
            result = adapter.get_metric("des_business_activity_index", start_date="2025-04-01")

            self.assertEqual(len(result.df), 1)
            self.assertEqual(float(result.df.iloc[0]["value"]), 14.0)


if __name__ == "__main__":
    unittest.main()
