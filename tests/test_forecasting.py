from __future__ import annotations

import unittest

import pandas as pd

from executer import MetricResult
from schemas.answer import SourceRef
from tools.forecasting import ForecastErrorCode, TrendForecaster, forecast_linear_trend


class _StubExecutor:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def execute(self, _request):
        return MetricResult(
            df=self.df,
            source=SourceRef(
                source_type="manual",
                label="test-source",
                reference="unit-test",
            ),
            meta={},
        )


class TestForecasting(unittest.TestCase):
    def test_linear_forecast_uses_recent_window(self) -> None:
        dates = pd.date_range("2025-01-01", periods=35, freq="D")
        values = [100 + idx * 2 for idx in range(35)]
        df = pd.DataFrame({"date": dates, "value": values})

        result = forecast_linear_trend(
            df,
            metric="working_gas_storage_lower48",
            horizon_days=7,
            include_overlay=True,
        )

        self.assertIsNone(result.error_code)
        self.assertEqual(result.observations_used, 30)
        self.assertEqual(len(result.forecast_points), 7)
        self.assertEqual(result.forecast_points[0]["date"][:10], "2025-02-05")
        self.assertAlmostEqual(result.forecast_points[0]["value"], 170.0, places=3)
        self.assertIn("overlay", result.to_dict())

    def test_forecast_requires_sufficient_data(self) -> None:
        dates = pd.date_range("2025-01-01", periods=6, freq="D")
        df = pd.DataFrame({"date": dates, "value": [1, 2, 3, 4, 5, 6]})

        result = forecast_linear_trend(df, metric="henry_hub_spot", horizon_days=7)

        self.assertEqual(result.error_code, ForecastErrorCode.INSUFFICIENT_DATA)

    def test_forecaster_executor_wrapper(self) -> None:
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        df = pd.DataFrame({"date": dates, "value": list(range(30))})
        forecaster = TrendForecaster(executor=_StubExecutor(df))

        result = forecaster.forecast_metric("henry_hub_spot", horizon_days=7)

        self.assertIsNone(result.error_code)
        self.assertEqual(len(result.forecast_points), 7)

    def test_forecast_supports_non_value_metric_columns(self) -> None:
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        df = pd.DataFrame(
            {
                "date": dates,
                "gas_share": [0.30 + (idx * 0.001) for idx in range(30)],
                "gas_generation": [1000 + idx for idx in range(30)],
            }
        )

        result = forecast_linear_trend(
            df,
            metric="iso_gas_dependency",
            horizon_days=7,
        )

        self.assertIsNone(result.error_code)
        self.assertEqual(len(result.forecast_points), 7)
