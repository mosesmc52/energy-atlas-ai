from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from tools.eia_adapter import EIAAdapter


SAMPLE_CSV = """region_id,date,n_stations_used,tavg_c_median,tavg_f_median,hdd_median,tavg_c_mean,tavg_f_mean,hdd_mean
lower_48,2025-11-10,7,3.0,37.4,27.0,3.2,37.8,26.5
lower_48,2025-11-11,7,4.0,39.2,25.0,4.1,39.4,24.8
lower_48,2025-11-12,7,4.75,40.55,24.45,4.9714285714,40.9485714286,24.0514285714
east,2025-11-12,5,6.0,42.8,21.2,6.3,43.3,20.7
"""


class TestEIAWeatherCSV(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.csv_path = Path(self.tmp_dir.name) / "daily_region_weather.csv"
        self.csv_path.write_text(SAMPLE_CSV, encoding="utf-8")

        self.eia_client_patch = patch("tools.eia_adapter.EIAClient", return_value=object())
        self.eia_client_patch.start()

        self.adapter = EIAAdapter(weather_csv_path=self.csv_path)

    def tearDown(self) -> None:
        self.eia_client_patch.stop()
        self.tmp_dir.cleanup()

    def test_csv_loads_successfully(self) -> None:
        df = self.adapter._load_weather_csv()
        self.assertFalse(df.empty)
        self.assertIn("region_id", df.columns)

    def test_date_parsing_works(self) -> None:
        df = self.adapter._load_weather_csv()
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["date"]))

    def test_region_filtering_works(self) -> None:
        df = self.adapter._weather_timeseries(
            region_id="lower_48",
            start="2025-11-10",
            end="2025-11-12",
            value_columns=["hdd_mean"],
        )
        self.assertEqual(set(df["region_id"].unique()), {"lower_48"})

    def test_inclusive_start_end_filtering_works(self) -> None:
        df = self.adapter._weather_timeseries(
            region_id="lower_48",
            start="2025-11-10",
            end="2025-11-11",
            value_columns=["hdd_mean"],
        )
        self.assertEqual(df["date"].dt.strftime("%Y-%m-%d").tolist(), ["2025-11-10", "2025-11-11"])

    def test_get_weather_hdd_mean_maps_to_hdd_mean(self) -> None:
        df = self.adapter.get_weather_hdd(
            region_id="lower_48",
            start="2025-11-10",
            end="2025-11-12",
            method="mean",
        )
        self.assertEqual(list(df.columns), ["date", "region_id", "hdd"])
        self.assertAlmostEqual(float(df.iloc[0]["hdd"]), 26.5)

    def test_get_weather_hdd_median_maps_to_hdd_median(self) -> None:
        df = self.adapter.get_weather_hdd(
            region_id="lower_48",
            start="2025-11-10",
            end="2025-11-12",
            method="median",
        )
        self.assertAlmostEqual(float(df.iloc[0]["hdd"]), 27.0)

    def test_get_weather_tavg_f_mean_maps_correctly(self) -> None:
        df = self.adapter.get_weather_tavg(
            region_id="lower_48",
            start="2025-11-10",
            end="2025-11-12",
            unit="f",
            method="mean",
        )
        self.assertEqual(list(df.columns), ["date", "region_id", "tavg"])
        self.assertAlmostEqual(float(df.iloc[0]["tavg"]), 37.8)

    def test_empty_date_range_returns_expected_schema(self) -> None:
        df = self.adapter.get_weather_hdd(
            region_id="lower_48",
            start="2024-01-01",
            end="2024-01-31",
            method="mean",
        )
        self.assertTrue(df.empty)
        self.assertEqual(list(df.columns), ["date", "region_id", "hdd"])

    def test_invalid_region_raises_clear_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "region_id 'unknown_region' not found"):
            self.adapter.get_weather_hdd(
                region_id="unknown_region",
                start="2025-11-10",
                end="2025-11-12",
                method="mean",
            )

    def test_invalid_unit_or_method_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            self.adapter.get_weather_tavg(
                region_id="lower_48",
                start="2025-11-10",
                end="2025-11-12",
                unit="k",
                method="mean",
            )
        with self.assertRaises(ValueError):
            self.adapter.get_weather_hdd(
                region_id="lower_48",
                start="2025-11-10",
                end="2025-11-12",
                method="avg",
            )

    def test_missing_weather_csv_raises_clear_error(self) -> None:
        missing = Path(self.tmp_dir.name) / "missing.csv"
        adapter = EIAAdapter(weather_csv_path=missing)
        with self.assertRaisesRegex(FileNotFoundError, "Weather CSV file not found"):
            adapter._load_weather_csv()


if __name__ == "__main__":
    unittest.main()
