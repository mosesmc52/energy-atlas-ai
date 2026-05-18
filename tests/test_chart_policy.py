from __future__ import annotations

import unittest

import pandas as pd

from answers.chart_policy import chart_policy


class TestChartPolicy(unittest.TestCase):
    def test_weather_forecast_prefers_demand_delta_bar_by_bucket(self) -> None:
        df = pd.DataFrame(
            [
                {"bucket": "days_1_5", "demand_delta_bcfd": 0.4},
                {"bucket": "days_6_10", "demand_delta_bcfd": -0.2},
                {"bucket": "days_11_15", "demand_delta_bcfd": 0.1},
            ]
        )
        spec = chart_policy(
            metric="weather_degree_days_forecast_vs_5y",
            mode="observed",
            df=df,
            query="How will weather impact natural gas demand over the next 7-14 days?",
        )
        self.assertIsNotNone(spec)
        self.assertEqual(spec.chart_type, "bar")
        self.assertEqual(spec.x, "bucket")
        self.assertEqual(spec.y, ["demand_delta_bcfd"])

    def test_weather_regional_drivers_uses_region_bar(self) -> None:
        df = pd.DataFrame(
            [
                {"region": "east", "demand_delta_bcfd": 0.5},
                {"region": "midwest", "demand_delta_bcfd": 0.1},
                {"region": "south", "demand_delta_bcfd": -0.4},
                {"region": "west", "demand_delta_bcfd": -0.2},
            ]
        )
        spec = chart_policy(
            metric="weather_regional_demand_drivers",
            mode="observed",
            df=df,
            query="Which regions are driving weather-related demand right now?",
        )
        self.assertIsNotNone(spec)
        self.assertEqual(spec.chart_type, "bar")
        self.assertEqual(spec.x, "region")
        self.assertEqual(spec.y, ["demand_delta_bcfd"])

    def test_storage_comparison_query_prefers_line_chart_over_bar(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2025-01-01", "value": 2800.0},
                {"date": "2025-06-01", "value": 2600.0},
                {"date": "2026-01-01", "value": 3000.0},
                {"date": "2026-05-01", "value": 2900.0},
            ]
        )
        spec = chart_policy(
            metric="working_gas_storage_lower48",
            mode="observed",
            df=df,
            query="How does current storage compare to the same week last year?",
        )
        self.assertIsNotNone(spec)
        self.assertEqual(spec.chart_type, "line")

    def test_production_mom_query_prefers_line_chart_over_bar(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-01", "value": 100100.0},
                {"date": "2026-02-01", "value": 100400.0},
                {"date": "2026-03-01", "value": 100250.0},
            ]
        )
        spec = chart_policy(
            metric="ng_production_lower48",
            mode="observed",
            df=df,
            query="Is production up or down month over month?",
        )
        self.assertIsNotNone(spec)
        self.assertEqual(spec.chart_type, "line")


if __name__ == "__main__":
    unittest.main()
