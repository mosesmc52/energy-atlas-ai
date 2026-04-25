import unittest

import pandas as pd

from charts.plotly_renderer import (
    compute_timeseries_summary_metrics,
    compute_storage_change_summary_metrics,
    render_plotly,
    should_render_timeseries_summary_cards,
)
from schemas.chart_spec import ChartSpec
from tools.forecasting import forecast_linear_trend


class TestPlotlyRenderer(unittest.TestCase):
    def test_categorical_bar_keeps_bucket_axis(self) -> None:
        df = pd.DataFrame(
            {
                "bucket": ["days_1_5", "days_6_10", "days_11_15"],
                "demand_delta_bcfd": [0.4, -0.2, 0.1],
            }
        )
        spec = ChartSpec(
            chart_type="bar",
            title="Weather-Driven Demand Impact by Forecast Window",
            x="bucket",
            y=["demand_delta_bcfd"],
        )

        fig = render_plotly(spec, df)
        self.assertEqual(list(fig.data[0].x), ["days 1-5", "days 6-10", "days 11-15"])

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

        self.assertEqual(len(fig.data), 3)
        self.assertEqual(fig.data[-1].name, "Forecast")
        self.assertFalse(bool(fig.layout.xaxis.rangeslider.visible))
        self.assertGreaterEqual(len(fig.layout.annotations), 1)

    def test_compute_storage_change_summary_metrics(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2026-02-28", "2026-03-07", "2026-03-14", "2026-03-21", "2026-03-28"]
                ),
                "value": [-120, -70, -40, 35, -48],
            }
        )

        metrics = compute_storage_change_summary_metrics(df)

        self.assertEqual(len(metrics), 4)
        self.assertEqual(metrics[0]["label"], "Latest weekly change")
        self.assertEqual(metrics[0]["value"], -48.0)
        self.assertEqual(metrics[1]["value"], 35.0)
        self.assertEqual(metrics[2]["value"], -30.75)
        self.assertEqual(metrics[3]["value"], -120.0)

    def test_compute_timeseries_summary_metrics(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2026-01-03", "2026-01-10", "2026-01-17", "2026-01-24"]
                ),
                "value": [2.1, 2.4, 2.2, 2.8],
            }
        )

        metrics = compute_timeseries_summary_metrics(df, unit="$/MMBtu")

        self.assertEqual(len(metrics), 4)
        self.assertEqual(metrics[0]["label"], "Latest reading")
        self.assertEqual(metrics[0]["value"], 2.8)
        self.assertEqual(metrics[1]["value"], 2.2)
        self.assertEqual(metrics[2]["value"], 2.1)
        self.assertEqual(metrics[3]["value"], 2.8)

    def test_storage_change_chart_gets_dashboard_styling(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2026-02-28", "2026-03-07", "2026-03-14", "2026-03-21", "2026-03-28"]
                ),
                "value": [-120, -70, -360, 35, -48],
            }
        )
        spec = ChartSpec(
            chart_type="line",
            title="Weekly Change in Working Gas Storage",
            x="date",
            y=["value"],
        )

        fig = render_plotly(spec, df)

        self.assertFalse(bool(fig.layout.xaxis.rangeslider.visible))
        self.assertGreaterEqual(len(fig.layout.annotations), 2)
        self.assertGreaterEqual(len(fig.layout.shapes), 2)

    def test_line_chart_splits_long_dataframe_by_region(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2026-03-07", "2026-03-14", "2026-03-07", "2026-03-14"]
                ),
                "region": ["east", "east", "midwest", "midwest"],
                "value": [15.0, 20.0, 10.0, 18.0],
            }
        )
        spec = ChartSpec(
            chart_type="line",
            title="Weekly Change in Working Gas Storage by Region",
            x="date",
            y=["value"],
        )

        fig = render_plotly(spec, df)

        self.assertEqual(len(fig.data), 2)
        self.assertEqual({trace.name for trace in fig.data}, {"east", "midwest"})
        self.assertFalse(should_render_timeseries_summary_cards(spec))

    def test_storage_level_and_change_chart_renders_two_series(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-07", "2026-03-14"]),
                "value": [800.0, 850.0],
                "weekly_change": [20.0, 50.0],
            }
        )
        spec = ChartSpec(
            chart_type="line",
            title="Working Gas in Storage and Weekly Change",
            x="date",
            y=["value", "weekly_change"],
        )

        fig = render_plotly(spec, df)

        self.assertEqual(len(fig.data), 2)
        self.assertEqual(fig.data[0].name, "value")
        self.assertEqual(fig.data[1].name, "weekly_change")


if __name__ == "__main__":
    unittest.main()
