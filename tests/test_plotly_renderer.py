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

    def test_storage_type_labels_remove_underscores_in_bar_and_legend(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-09-01", "2025-09-01"]),
                "value": [1.0, 2.0],
                "storage_type": ["salt_cavern", "depleted_field"],
            }
        )
        spec = ChartSpec(
            chart_type="bar",
            title="Base Gas by Storage Type",
            x="storage_type",
            y=["value"],
        )

        fig = render_plotly(spec, df)

        self.assertEqual(list(fig.data[0].x), ["salt cavern", "depleted field"])

    def test_storage_type_line_legend_removes_underscores(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-09-01", "2025-10-01", "2025-09-01", "2025-10-01"]),
                "value": [1.0, 1.5, 2.0, 2.5],
                "storage_type": ["salt_cavern", "salt_cavern", "depleted_field", "depleted_field"],
            }
        )
        spec = ChartSpec(
            chart_type="line",
            title="Working Gas by Storage Type",
            x="date",
            y=["value"],
        )

        fig = render_plotly(spec, df)

        self.assertEqual({trace.name for trace in fig.data}, {"salt cavern", "depleted field"})

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

    def test_compute_timeseries_summary_metrics_includes_five_year_avg_when_available(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2021-01-01",
                        "2021-02-01",
                        "2021-03-01",
                        "2021-04-01",
                        "2022-01-01",
                        "2022-02-01",
                        "2022-03-01",
                        "2022-04-01",
                        "2023-01-01",
                        "2023-02-01",
                        "2023-03-01",
                        "2023-04-01",
                        "2024-01-01",
                        "2024-02-01",
                        "2024-03-01",
                        "2024-04-01",
                        "2025-01-01",
                        "2025-02-01",
                        "2025-03-01",
                        "2025-04-01",
                        "2026-01-01",
                        "2026-02-01",
                        "2026-03-01",
                        "2026-04-01",
                    ]
                ),
                "value": [
                    10.0,
                    11.0,
                    12.0,
                    13.0,
                    20.0,
                    21.0,
                    22.0,
                    23.0,
                    30.0,
                    31.0,
                    32.0,
                    33.0,
                    40.0,
                    41.0,
                    42.0,
                    43.0,
                    50.0,
                    51.0,
                    52.0,
                    53.0,
                    60.0,
                    61.0,
                    62.0,
                    63.0,
                ],
            }
        )

        metrics = compute_timeseries_summary_metrics(df, unit="MMcf")
        labels = [m.get("label") for m in metrics]

        self.assertIn("5Y Avg", labels)

    def test_compute_timeseries_summary_metrics_compares_regions(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2021-01-01",
                        "2021-01-08",
                        "2021-01-01",
                        "2021-01-08",
                    ]
                ),
                "value": [100.0, 110.0, 200.0, 210.0],
                "region": ["east", "east", "midwest", "midwest"],
            }
        )

        metrics = compute_timeseries_summary_metrics(df, unit="Bcf")

        self.assertEqual(len(metrics), 3)
        self.assertEqual(
            [metric["label"] for metric in metrics],
            ["East Latest", "Midwest Latest", "Spread"],
        )
        self.assertEqual([metric["value"] for metric in metrics], [110.0, 210.0, 100.0])
        self.assertEqual(metrics[2]["subtitle"], "Midwest - East")

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
        self.assertEqual({trace.name for trace in fig.data}, {"East", "Midwest"})
        self.assertFalse(should_render_timeseries_summary_cards(spec))
        self.assertIn("Bcf", fig.data[0].hovertemplate)

    def test_storage_line_chart_keeps_three_region_traces(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2026-03-07",
                        "2026-03-14",
                        "2026-03-07",
                        "2026-03-14",
                        "2026-03-07",
                        "2026-03-14",
                    ]
                ),
                "region": ["east", "east", "midwest", "midwest", "pacific", "pacific"],
                "value": [15.0, 20.0, 10.0, 18.0, 12.0, 17.0],
            }
        )
        spec = ChartSpec(
            chart_type="line",
            title="Working Gas in Storage",
            x="date",
            y=["value"],
        )

        fig = render_plotly(spec, df)

        self.assertEqual(len(fig.data), 3)
        self.assertEqual(
            {trace.name for trace in fig.data},
            {"East", "Midwest", "Pacific"},
        )

    def test_storage_single_series_line_uses_bcf_hover(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-03-07", "2026-03-14"]),
                "value": [800.0, 850.0],
            }
        )
        spec = ChartSpec(
            chart_type="line",
            title="Working Gas in Storage",
            x="date",
            y=["value"],
            x_label="Date",
            y_label="Storage (Bcf)",
        )

        fig = render_plotly(spec, df)

        self.assertEqual(len(fig.data), 1)
        self.assertIn("Bcf", fig.data[0].hovertemplate)
        self.assertEqual(fig.layout.yaxis.title.text, "Storage (Bcf)")

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

    def test_storage_regional_bar_uses_latest_row_per_region(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-02", "2026-01-09"] * 2),
                "value": [800.0, 810.0, 900.0, 920.0],
                "region": ["east", "east", "midwest", "midwest"],
            }
        )
        spec = ChartSpec(
            chart_type="bar",
            title="Current Working Gas in Storage by Region",
            x="region",
            y=["value"],
            y_label="Bcf",
        )

        fig = render_plotly(spec, df)

        self.assertEqual(list(fig.data[0].x), ["midwest", "east"])
        self.assertEqual(list(fig.data[0].y), [920.0, 810.0])

    def test_storage_deviation_bar_preserves_rank_order_and_zero_line(self) -> None:
        df = pd.DataFrame(
            {
                "region": ["mountain", "pacific", "east"],
                "deviation_bcf": [-120.0, -85.0, -40.0],
            }
        )
        spec = ChartSpec(
            chart_type="bar",
            title="Storage Deviation from 5-Year Average by Region",
            x="region",
            y=["deviation_bcf"],
            x_label="Region",
            y_label="Deviation (Bcf)",
        )

        fig = render_plotly(spec, df)

        self.assertEqual(fig.data[0].orientation, "h")
        self.assertEqual(list(fig.data[0].x), [-120.0, -85.0, -40.0])
        self.assertEqual(list(fig.data[0].y), ["Mountain", "Pacific", "East"])
        self.assertGreaterEqual(len(fig.layout.shapes), 1)

    def test_storage_seasonal_line_renders_value_and_five_year_average(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-02", "2026-01-09"]),
                "value": [100.0, 110.0],
                "five_year_avg": [95.0, 102.0],
                "five_year_min": [90.0, 98.0],
                "five_year_max": [105.0, 115.0],
            }
        )
        spec = ChartSpec(
            chart_type="seasonal_line",
            title="Working Gas in Storage vs 5-Year Average",
            x="date",
            y=["value", "five_year_avg"],
        )

        fig = render_plotly(spec, df)

        self.assertIn("Storage", [trace.name for trace in fig.data])
        self.assertIn("5-year average", [trace.name for trace in fig.data])

    def test_market_pressure_dashboard_renders_four_components(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "date": "2026-04-24",
                    "weather_demand_delta_bcfd": -0.45,
                    "storage_surprise_bcf": 20.0,
                    "lng_delta_mmcf": 5.0,
                    "production_delta_mmcf": 300.0,
                    "price_delta_usd_mmbtu": 0.10,
                }
            ]
        )
        spec = ChartSpec(
            chart_type="bar",
            title="Market Pressure Dashboard",
            x="component",
            y=["score"],
            x_label="Driver",
            y_label="Pressure Score (Bullish + / Bearish -)",
        )

        fig = render_plotly(spec, df)

        self.assertEqual(list(fig.data[0].x), ["Weather", "Storage", "LNG / Supply", "Price"])
        self.assertEqual(len(fig.data[0].y), 4)


if __name__ == "__main__":
    unittest.main()
