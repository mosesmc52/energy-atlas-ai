import unittest
from unittest.mock import Mock

from executer import ExecuteRequest, MetricExecutor


class TestMetricExecutor(unittest.TestCase):
    def test_consumption_passes_state_filter_to_eia_adapter(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.ng_consumption_lower48.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="ng_consumption_lower48",
                start="2024-01-01",
                end="2024-12-31",
                filters={"region": "ca"},
            )
        )

        eia.ng_consumption_lower48.assert_called_once_with(
            start="2024-01-01",
            end="2024-12-31",
            state="ca",
        )

    def test_production_passes_state_filter_to_eia_adapter(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.ng_production_lower48.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="ng_production_lower48",
                start="2024-01-01",
                end="2024-12-31",
                filters={"region": "tx"},
            )
        )

        eia.ng_production_lower48.assert_called_once_with(
            start="2024-01-01",
            end="2024-12-31",
            state="tx",
        )

    def test_imports_passes_region_filter_to_eia_adapter(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.lng_imports.return_value = Mock(df=None, source=None, meta={"cache": {}})
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="lng_imports",
                start="2024-01-01",
                end="2024-12-31",
                filters={"region": "qatar"},
            )
        )

        eia.lng_imports.assert_called_once_with(
            start="2024-01-01",
            end="2024-12-31",
            region="qatar",
        )

    def test_exports_passes_region_filter_to_eia_adapter(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.lng_exports.return_value = Mock(df=None, source=None, meta={"cache": {}})
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="lng_exports",
                start="2024-01-01",
                end="2024-12-31",
                filters={"region": "japan"},
            )
        )

        eia.lng_exports.assert_called_once_with(
            start="2024-01-01",
            end="2024-12-31",
            region="japan",
        )

    def test_reserves_passes_state_and_resource_category_to_eia_adapter(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.ng_exploration_reserves_lower48.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="ng_exploration_reserves_lower48",
                start="2024-01-01",
                end="2024-12-31",
                filters={"region": "tx", "resource_category": "proved_ngl"},
            )
        )

        eia.ng_exploration_reserves_lower48.assert_called_once_with(
            start="2024-01-01",
            end="2024-12-31",
            state="tx",
            resource_category="proved_ngl",
        )

    def test_pipeline_passes_dataset_filter_to_eia_adapter(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.ng_pipeline.return_value = Mock(df=None, source=None, meta={})
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="ng_pipeline",
                start="2024-01-01",
                end="2024-12-31",
                filters={"dataset": "pipeline_state2_state_capacity"},
            )
        )

        eia.ng_pipeline.assert_called_once_with(
            start="2024-01-01",
            end="2024-12-31",
            dataset="pipeline_state2_state_capacity",
        )

    def test_storage_change_group_by_region_fetches_each_storage_region(self) -> None:
        eia = Mock()
        grid = Mock()

        def make_result(*, region: str, **kwargs):
            return Mock(
                df=__import__("pandas").DataFrame(
                    [{"date": "2024-01-05", "value": 10.0, "region": region}]
                ).drop(columns=["region"]),
                source=Mock(reference=f"ref:{region}"),
                meta={"cache": {"hit": True}},
            )

        eia.storage_working_gas_change_weekly.side_effect = make_result
        executor = MetricExecutor(eia=eia, grid=grid)

        result = executor.execute(
            ExecuteRequest(
                metric="working_gas_storage_change_weekly",
                start="2024-01-01",
                end="2024-12-31",
                filters={"group_by": "region"},
            )
        )

        self.assertEqual(eia.storage_working_gas_change_weekly.call_count, 5)
        self.assertEqual(set(result.df["region"]), {"east", "midwest", "south_central", "mountain", "pacific"})
        self.assertEqual(result.source.reference, "eia-ng-client:derived_natural_gas.storage_change_weekly_by_region")

    def test_storage_level_can_include_weekly_change(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.storage_working_gas.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"date": "2024-01-05", "value": 100.0},
                    {"date": "2024-01-12", "value": 110.0},
                ]
            ),
            source=Mock(reference="ref:storage"),
            meta={"cache": {"hit": True}},
        )
        eia.storage_working_gas_change_weekly.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [{"date": "2024-01-12", "value": 10.0}]
            ),
            source=Mock(reference="ref:change"),
            meta={"cache": {"hit": True}},
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        result = executor.execute(
            ExecuteRequest(
                metric="working_gas_storage_lower48",
                start="2024-01-01",
                end="2024-12-31",
                filters={"region": "east", "include_weekly_change": True},
            )
        )

        self.assertEqual(result.source.reference, "eia-ng-client:natural_gas.storage_with_weekly_change")
        self.assertIn("weekly_change", result.df.columns)
        self.assertEqual(result.df.iloc[-1]["weekly_change"], 10.0)

    def test_weather_forecast_metric_passes_region_filter(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.weather_degree_days_forecast_vs_5y.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="weather_degree_days_forecast_vs_5y",
                start="2026-01-01",
                end="2026-01-31",
                filters={"region": "east"},
            )
        )

        eia.weather_degree_days_forecast_vs_5y.assert_called_once_with(
            start="2026-01-01",
            end="2026-01-31",
            region="east",
            normal_years=5,
        )

    def test_weather_forecast_metric_passes_requested_normal_years(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.weather_degree_days_forecast_vs_5y.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        executor.execute(
            ExecuteRequest(
                metric="weather_degree_days_forecast_vs_5y",
                start="2026-01-01",
                end="2026-01-31",
                filters={"region": "west", "normal_years": 2},
            )
        )

        eia.weather_degree_days_forecast_vs_5y.assert_called_once_with(
            start="2026-01-01",
            end="2026-01-31",
            region="west",
            normal_years=2,
        )

    def test_weather_regional_demand_drivers_builds_ranked_regions(self) -> None:
        eia = Mock()
        grid = Mock()

        def make_regional_weather(*, region: str, **kwargs):  # noqa: ANN001
            del kwargs
            base = {"east": 0.6, "midwest": 0.2, "south": -0.4, "west": -0.1}[region]
            return Mock(
                df=__import__("pandas").DataFrame(
                    [
                        {
                            "bucket": "days_1_5",
                            "bucket_start_day": 1,
                            "delta_hdd": 1.0,
                            "delta_cdd": -0.5,
                            "demand_delta_bcfd": base,
                            "as_of": "2026-04-24T00:00:00Z",
                        },
                        {
                            "bucket": "days_6_10",
                            "bucket_start_day": 6,
                            "delta_hdd": 0.5,
                            "delta_cdd": -0.3,
                            "demand_delta_bcfd": base * 0.9,
                            "as_of": "2026-04-24T00:00:00Z",
                        },
                    ]
                ),
                source=Mock(reference=f"ref:weather:{region}"),
                meta={"cache": {"hit": True}},
            )

        eia.weather_degree_days_forecast_vs_5y.side_effect = make_regional_weather
        executor = MetricExecutor(eia=eia, grid=grid)

        result = executor.execute(
            ExecuteRequest(
                metric="weather_regional_demand_drivers",
                start="2026-04-01",
                end="2026-04-24",
                filters={"normal_years": 5},
            )
        )

        self.assertEqual(eia.weather_degree_days_forecast_vs_5y.call_count, 4)
        self.assertEqual(result.source.reference, "open-meteo:degree_days.regional_drivers")
        self.assertEqual(set(result.df["region"].tolist()), {"east", "midwest", "south", "west"})
        self.assertIn("demand_delta_bcfd", result.df.columns)

    def test_supply_balance_regime_metric_combines_component_signals(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.ng_production_lower48.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"date": "2026-03-01", "value": 100.0},
                    {"date": "2026-04-01", "value": 102.0},
                ]
            ),
            source=Mock(reference="ref:prod"),
            meta={"cache": {"hit": True}},
        )
        eia.storage_working_gas_change_weekly.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [{"date": "2026-04-17", "value": 80.0}]
            ),
            source=Mock(reference="ref:storage_change"),
            meta={"cache": {"hit": True}},
        )
        eia.weather_degree_days_forecast_vs_5y.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"bucket": "days_1_5", "demand_delta_bcfd": -0.5, "as_of": "2026-04-24T00:00:00Z"},
                    {"bucket": "days_6_10", "demand_delta_bcfd": -0.4, "as_of": "2026-04-24T00:00:00Z"},
                ]
            ),
            source=Mock(reference="ref:weather"),
            meta={"cache": {"hit": True}},
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        result = executor.execute(
            ExecuteRequest(
                metric="ng_supply_balance_regime",
                start="2025-01-01",
                end="2026-04-24",
                filters={"region": "united_states_total"},
            )
        )

        self.assertEqual(eia.ng_production_lower48.call_count, 1)
        self.assertEqual(eia.storage_working_gas_change_weekly.call_count, 1)
        self.assertEqual(eia.weather_degree_days_forecast_vs_5y.call_count, 1)
        self.assertEqual(result.source.reference, "eia-ng-client:derived_natural_gas.supply_balance_regime")
        self.assertGreater(len(result.df), 1)
        self.assertIn("regime", result.df.columns)
        self.assertEqual(result.df.iloc[-1]["regime"], "expanding")

    def test_iso_gas_dependency_gracefully_falls_back_on_gridstatus_error(self) -> None:
        eia = Mock()
        grid = Mock()
        grid.iso_gas_dependency.side_effect = StopIteration()
        executor = MetricExecutor(eia=eia, grid=grid)

        result = executor.execute(
            ExecuteRequest(
                metric="iso_gas_dependency",
                start="2026-01-01",
                end="2026-01-31",
                filters={"iso": "ercot"},
            )
        )

        self.assertEqual(result.source.reference, "gridstatus:error_iso_gas_dependency")
        self.assertEqual(len(result.df), 0)
        self.assertIn("error", result.source.parameters)

    def test_iso_renewables_gracefully_falls_back_on_gridstatus_error(self) -> None:
        eia = Mock()
        grid = Mock()
        grid.iso_renewables.side_effect = RuntimeError("service unavailable")
        executor = MetricExecutor(eia=eia, grid=grid)

        result = executor.execute(
            ExecuteRequest(
                metric="iso_renewables",
                start="2026-01-01",
                end="2026-01-31",
                filters={"iso": "ercot"},
            )
        )

        self.assertEqual(result.source.reference, "gridstatus:error_iso_renewables")
        self.assertEqual(len(result.df), 0)
        self.assertIn("service unavailable", str(result.source.parameters.get("error")))

    def test_weekly_energy_atlas_summary_combines_weather_storage_supply_and_price(self) -> None:
        eia = Mock()
        grid = Mock()
        eia.weather_degree_days_forecast_vs_5y.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"delta_hdd": -4.0, "delta_cdd": 2.0, "demand_delta_bcfd": -0.5, "as_of": "2026-04-24T00:00:00Z"},
                    {"delta_hdd": -3.0, "delta_cdd": 1.0, "demand_delta_bcfd": -0.4, "as_of": "2026-04-24T00:00:00Z"},
                ]
            ),
            source=Mock(reference="ref:weather"),
            meta={"cache": {"hit": True}},
        )
        eia.storage_working_gas_change_weekly.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"date": "2026-03-20", "value": 20.0},
                    {"date": "2026-03-27", "value": 25.0},
                    {"date": "2026-04-03", "value": 30.0},
                    {"date": "2026-04-10", "value": 35.0},
                    {"date": "2026-04-17", "value": 40.0},
                    {"date": "2026-04-24", "value": 50.0},
                ]
            ),
            source=Mock(reference="ref:storage"),
            meta={"cache": {"hit": True}},
        )
        eia.lng_exports.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"date": "2026-03-01", "value": 120.0},
                    {"date": "2026-04-01", "value": 125.0},
                ]
            ),
            source=Mock(reference="ref:lng"),
            meta={"cache": {"hit": True}},
        )
        eia.ng_production_lower48.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"date": "2026-03-01", "value": 104000.0},
                    {"date": "2026-04-01", "value": 104300.0},
                ]
            ),
            source=Mock(reference="ref:prod"),
            meta={"cache": {"hit": True}},
        )
        eia.henry_hub_spot.return_value = Mock(
            df=__import__("pandas").DataFrame(
                [
                    {"date": "2026-04-17", "value": 2.71},
                    {"date": "2026-04-24", "value": 2.81},
                ]
            ),
            source=Mock(reference="ref:price"),
            meta={"cache": {"hit": True}},
        )
        executor = MetricExecutor(eia=eia, grid=grid)

        result = executor.execute(
            ExecuteRequest(
                metric="weekly_energy_atlas_summary",
                start="2025-04-24",
                end="2026-04-24",
                filters=None,
            )
        )

        self.assertEqual(
            result.source.reference,
            "eia-ng-client:derived_natural_gas.weekly_energy_atlas_summary",
        )
        self.assertIn("weather_demand_delta_bcfd", result.df.columns)
        self.assertIn("storage_surprise_bcf", result.df.columns)
        self.assertIn("price_delta_usd_mmbtu", result.df.columns)
        self.assertEqual(eia.weather_degree_days_forecast_vs_5y.call_count, 1)
        self.assertEqual(eia.storage_working_gas_change_weekly.call_count, 1)
        self.assertEqual(eia.lng_exports.call_count, 1)
        self.assertEqual(eia.ng_production_lower48.call_count, 1)
        self.assertEqual(eia.henry_hub_spot.call_count, 1)


if __name__ == "__main__":
    unittest.main()
