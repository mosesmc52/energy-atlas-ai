import unittest

import pandas as pd

from charts.plotly_renderer import render_plotly
from schemas.chart_spec import ChartSpec
from tools.forecasting import forecast_linear_trend


class TestPlotlyRenderer(unittest.TestCase):
    def test_datetime_bar_uses_category_labels(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2025-09-01", "2025-10-01", "2025-11-01", "2025-12-01"]
                ),
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        )
        spec = ChartSpec(
            chart_type="bar",
            title="Test",
            x="date",
            y=["value"],
            aggregation="monthly",
        )

        fig = render_plotly(spec, df)

        self.assertEqual(fig.layout.xaxis.type, "category")
        self.assertEqual(list(fig.data[0].x), ["2025-09", "2025-10", "2025-11", "2025-12"])
        self.assertFalse(bool(fig.layout.xaxis.rangeslider.visible))

    def test_line_chart_can_overlay_forecast_trace(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.date_range("2025-01-01", periods=30, freq="D"),
                "value": list(range(30)),
            }
        )
        spec = ChartSpec(chart_type="line", title="Test", x="date", y=["value"])
        forecast = forecast_linear_trend(
            df,
            metric="henry_hub_spot",
            horizon_days=7,
            include_overlay=True,
        )

        fig = render_plotly(spec, df, forecast_overlay=forecast)

        self.assertEqual(len(fig.data), 2)
        self.assertEqual(fig.data[-1].name, "Forecast")


if __name__ == "__main__":
    unittest.main()
