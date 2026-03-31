from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd
from django.test import RequestFactory, SimpleTestCase

from alerts.views import forecast_metric_view
from executer import MetricResult
from schemas.answer import SourceRef
from tools.forecasting import ForecastErrorCode, TrendForecaster


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


class TrendForecasterAppTests(SimpleTestCase):
    def test_forecast_metric_view_returns_structured_payload(self):
        factory = RequestFactory()
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        df = pd.DataFrame({"date": dates, "value": list(range(30))})
        forecaster = TrendForecaster(executor=_StubExecutor(df))
        request = factory.post(
            "/alerts/forecast/",
            data=json.dumps(
                {
                    "metric": "working_gas_storage_lower48",
                    "horizon_days": 7,
                    "include_overlay": True,
                }
            ),
            content_type="application/json",
        )

        with patch("alerts.views.build_metric_forecaster", return_value=forecaster):
            response = forecast_metric_view(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload["metric"], "working_gas_storage_lower48")
        self.assertEqual(payload["horizon_days"], 7)
        self.assertIn("overlay", payload)

    def test_forecaster_invalid_horizon_returns_structured_error(self):
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        df = pd.DataFrame({"date": dates, "value": list(range(30))})
        forecaster = TrendForecaster(executor=_StubExecutor(df))

        result = forecaster.forecast_metric("working_gas_storage_lower48", horizon_days=30)

        self.assertEqual(result.error_code, ForecastErrorCode.INVALID_HORIZON)
