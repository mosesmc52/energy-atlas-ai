from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.eia_adapter import EIAAdapter


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def _make_weather_csv(path: Path) -> None:
    rows = ["region_id,date,n_stations_used,tavg_c_median,tavg_f_median,hdd_median,tavg_c_mean,tavg_f_mean,hdd_mean,cdd_median,cdd_mean"]
    for year in range(2021, 2026):
        for day in range(1, 16):
            date_text = f"{year}-01-{day:02d}"
            rows.append(f"lower_48,{date_text},10,5.0,41.0,24.0,5.0,41.0,24.0,0.0,0.0")
            rows.append(f"east,{date_text},4,4.0,39.2,25.8,4.0,39.2,25.8,0.0,0.0")
            rows.append(f"midwest,{date_text},3,3.0,37.4,27.6,3.0,37.4,27.6,0.0,0.0")
            rows.append(f"south,{date_text},2,9.0,48.2,16.8,9.0,48.2,16.8,0.0,0.0")
            rows.append(f"west,{date_text},2,10.0,50.0,15.0,10.0,50.0,15.0,0.0,0.0")
    path.write_text("\n".join(rows), encoding="utf-8")


class TestWeatherForecastVs5Y(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.csv_path = Path(self.tmp_dir.name) / "daily_region_weather.csv"
        _make_weather_csv(self.csv_path)
        self.adapter = EIAAdapter(weather_csv_path=self.csv_path)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    @patch("tools.eia_adapter.requests.get")
    def test_forecast_vs_5y_returns_bucket_rows(self, mock_get) -> None:
        def _fake_get(url, params=None, timeout=0):  # noqa: ANN001
            del url, timeout
            dates = [f"2026-01-{d:02d}" for d in range(1, 16)]
            # Slightly warmer forecast than historical baseline.
            payload = {
                "daily": {
                    "time": dates,
                    "temperature_2m_max": [55.0 + (i % 3) for i in range(15)],
                    "temperature_2m_min": [35.0 + (i % 2) for i in range(15)],
                }
            }
            self.assertIn("latitude", params)
            self.assertIn("longitude", params)
            return _FakeResponse(payload)

        mock_get.side_effect = _fake_get
        result = self.adapter.weather_degree_days_forecast_vs_5y(
            start="2026-01-01",
            end="2026-01-31",
            region="lower48",
            normal_years=3,
        )

        self.assertEqual(result.source.reference, "open-meteo:degree_days.forecast_vs_5y")
        self.assertEqual(len(result.df), 3)
        self.assertEqual(
            list(result.df["bucket"]),
            ["days_1_5", "days_6_10", "days_11_15"],
        )
        self.assertIn("demand_delta_bcfd", result.df.columns)
        self.assertTrue((result.df["normal_years"] == 3).all())


if __name__ == "__main__":
    unittest.main()
