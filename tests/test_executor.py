import unittest
from unittest.mock import Mock

import pandas as pd

from agents.router import EnergyRouteResult
from executer import ExecuteRequest, MetricExecutor
from schemas.answer import SourceRef
from tools.eia_adapter import EIAResult


def _storage_route(**overrides) -> EnergyRouteResult:
    values = {
        "domain": "storage",
        "analysis_type": "time_series",
        "primary_metric": "working_gas_storage_lower48",
        "metrics": ["working_gas_storage_lower48"],
        "storage_dataset": "weekly_working_gas",
        "storage_frequency": "weekly",
        "storage_metric_type": "working_gas",
        "storage_type": None,
        "storage_types_all": False,
        "regions": ["lower48"],
        "states": [],
        "states_all": False,
        "start_date": "2021-01-01",
        "end_date": "2026-06-01",
        "date_expression": None,
        "value_type": "level",
        "comparisons": ["none"],
        "ranking_basis": "current_storage",
        "chart_type": "line",
        "output_mode": "chart_and_answer",
        "filters": {"regions": ["lower48"]},
        "confidence": 0.9,
        "ambiguous": False,
        "reason": None,
        "normalized_query": "storage",
    }
    values.update(overrides)
    return EnergyRouteResult(**values)


def _storage_result(region: str, value: float = 10.0) -> EIAResult:
    return EIAResult(
        df=pd.DataFrame([{"date": "2024-01-05", "value": value}]),
        source=SourceRef(
            source_type="eia_api",
            label=f"Storage {region}",
            reference=f"ref:{region}",
            parameters={"region": region},
        ),
        meta={"cache": {"hit": True}},
    )


def _state_storage_result(state: str, value: float = 10.0) -> EIAResult:
    return EIAResult(
        df=pd.DataFrame([{"date": "2024-01-31", "value": value}]),
        source=SourceRef(
            source_type="eia_api",
            label=f"Storage {state}",
            reference=f"ref:{state}",
            parameters={"state": state},
        ),
        meta={"units": "MMcf"},
    )


def _storage_type_result(storage_type: str, value: float = 10.0) -> EIAResult:
    return EIAResult(
        df=pd.DataFrame([{"date": "2024-01-31", "value": value}]),
        source=SourceRef(
            source_type="eia_api",
            label=f"Storage {storage_type}",
            reference=f"ref:{storage_type}",
            parameters={"storage_type": storage_type, "eia_storage_type": storage_type},
        ),
        meta={"units": "MMcf"},
    )


def _geography_storage_result(geography: str, value: float = 10.0, units: str = "MMcf") -> EIAResult:
    return EIAResult(
        df=pd.DataFrame([{"date": "2024-01-31", "value": value, "geography": geography}]),
        source=SourceRef(
            source_type="eia_api",
            label=f"Storage {geography}",
            reference=f"ref:{geography}",
            parameters={"geography": geography},
        ),
        meta={"units": units},
    )


class TestMetricExecutor(unittest.TestCase):
    def test_consumption_passes_state_filter_to_eia_adapter(self) -> None:
        eia = Mock()
        eia.ng_consumption_lower48.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia)

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
        eia.ng_production_lower48.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia)

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
        eia.lng_imports.return_value = Mock(df=None, source=None, meta={"cache": {}})
        executor = MetricExecutor(eia=eia)

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
        eia.lng_exports.return_value = Mock(df=None, source=None, meta={"cache": {}})
        executor = MetricExecutor(eia=eia)

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
        eia.ng_exploration_reserves_lower48.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia)

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
        eia.ng_pipeline.return_value = Mock(df=None, source=None, meta={})
        executor = MetricExecutor(eia=eia)

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

    def test_storage_change_regions_fetch_each_storage_region(self) -> None:
        eia = Mock()

        def make_result(*, region: str, **kwargs):
            return _storage_result(region)

        eia.storage_working_gas_change_weekly.side_effect = make_result
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="working_gas_storage_change_weekly",
                start="2024-01-01",
                end="2024-12-31",
                filters={"regions": ["all"]},
            )
        )

        self.assertEqual(eia.storage_working_gas_change_weekly.call_count, 7)
        self.assertEqual(
            set(result.df["region"]),
            {
                "east",
                "midwest",
                "south_central",
                "south_central_salt",
                "south_central_nonsalt",
                "mountain",
                "pacific",
            },
        )
        self.assertEqual(result.source.reference, "eia-ng-client:natural_gas.storage_by_region")

    def test_storage_level_multiple_regions_concatenates_region_column(self) -> None:
        eia = Mock()

        def make_result(*, region: str, **kwargs):
            return _storage_result(region, value=20.0 if region == "east" else 30.0)

        eia.storage_working_gas.side_effect = make_result
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="working_gas_storage_lower48",
                start="2021-01-01",
                end="2026-06-01",
                filters={"regions": ["east", "midwest"]},
            )
        )

        self.assertEqual(eia.storage_working_gas.call_count, 2)
        self.assertEqual(set(result.df["region"]), {"east", "midwest"})
        self.assertEqual(list(result.df.columns), ["date", "value", "region"])

    def test_storage_level_single_region_keeps_region_column(self) -> None:
        eia = Mock()
        eia.storage_working_gas.side_effect = lambda region, **kwargs: _storage_result(region, value=20.0)
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="working_gas_storage_lower48",
                start="2021-01-01",
                end="2026-06-01",
                filters={"regions": ["east"]},
            )
        )

        self.assertEqual(eia.storage_working_gas.call_count, 1)
        self.assertEqual(set(result.df["region"]), {"east"})
        self.assertEqual(list(result.df.columns), ["date", "value", "region"])

    def test_execute_storage_route_attaches_route_metadata(self) -> None:
        eia = Mock()
        eia.storage_working_gas.side_effect = lambda region, **kwargs: _storage_result(region)
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            regions=["east", "midwest"],
            filters={"regions": ["east", "midwest"]},
        )

        result = executor.execute_storage_route(route)

        self.assertEqual(set(result.df["region"]), {"east", "midwest"})
        self.assertEqual(result.meta["domain"], "storage")
        self.assertEqual(result.meta["analysis_type"], "time_series")
        self.assertEqual(result.meta["regions"], ["east", "midwest"])
        self.assertEqual(result.meta["start_date"], "2021-01-01")

    def test_execute_storage_route_expands_fetch_window_for_baseline_queries(self) -> None:
        eia = Mock()
        eia.storage_working_gas.side_effect = lambda region, start, end: _storage_result(region)
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="seasonal_compare",
            regions=["lower48"],
            start_date="2026-05-01",
            end_date="2026-06-01",
            comparisons=["five_year_avg"],
            filters={"regions": ["lower48"]},
        )

        result = executor.execute_storage_route(route)

        eia.storage_working_gas.assert_called_once_with(
            start="2020-06-01",
            end="2026-06-01",
            region="lower48",
        )
        self.assertEqual(result.meta["requested_start_date"], "2026-05-01")
        self.assertEqual(result.meta["requested_end_date"], "2026-06-01")
        self.assertEqual(result.meta["fetch_start_date"], "2020-06-01")
        self.assertEqual(result.meta["fetch_end_date"], "2026-06-01")

    def test_execute_storage_route_expands_latest_all_operators_monthly_window(self) -> None:
        eia = Mock()
        eia.underground_storage_all_operators.side_effect = lambda state, **kwargs: _state_storage_result(state)
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="latest",
            primary_metric="underground_storage_base_gas_monthly",
            metrics=["underground_storage_base_gas_monthly"],
            storage_dataset="underground_storage_all_operators",
            storage_frequency="monthly",
            storage_metric_type="base_gas",
            regions=[],
            states=["la"],
            states_all=False,
            start_date=None,
            end_date="2026-06-07",
            chart_type="none",
            output_mode="answer",
            filters={
                "states": ["la"],
                "states_all": False,
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": "monthly",
                "storage_metric_type": "base_gas",
            },
        )

        result = executor.execute_storage_route(route)

        eia.underground_storage_all_operators.assert_called_once_with(
            start="2024-06-07",
            end="2026-06-07",
            state="la",
            metric_type="base_gas",
            frequency="monthly",
        )
        self.assertIsNone(result.meta["requested_start_date"])
        self.assertEqual(result.meta["requested_end_date"], "2026-06-07")
        self.assertEqual(result.meta["fetch_start_date"], "2024-06-07")
        self.assertEqual(result.meta["fetch_end_date"], "2026-06-07")

    def test_execute_storage_route_keeps_explicit_time_series_window_for_all_operators(self) -> None:
        eia = Mock()
        eia.underground_storage_all_operators.side_effect = lambda state, **kwargs: _state_storage_result(state)
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="time_series",
            primary_metric="underground_storage_working_gas_monthly",
            metrics=["underground_storage_working_gas_monthly"],
            storage_dataset="underground_storage_all_operators",
            storage_frequency="monthly",
            storage_metric_type="working_gas",
            regions=[],
            states=["tx"],
            states_all=False,
            start_date="2018-01-01",
            end_date="2026-06-07",
            chart_type="line",
            output_mode="chart_and_answer",
            filters={
                "states": ["tx"],
                "states_all": False,
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": "monthly",
                "storage_metric_type": "working_gas",
            },
        )

        executor.execute_storage_route(route)

        eia.underground_storage_all_operators.assert_called_once_with(
            start="2018-01-01",
            end="2026-06-07",
            state="tx",
            metric_type="working_gas",
            frequency="monthly",
        )

    def test_execute_storage_route_retries_empty_latest_all_operators_with_latest_available_window(self) -> None:
        eia = Mock()
        eia.underground_storage_all_operators.side_effect = [
            EIAResult(
                df=pd.DataFrame(columns=["date", "value", "state"]),
                source=SourceRef(
                    source_type="eia_api",
                    label="Storage la",
                    reference="ref:la-empty",
                    parameters={"state": "pa"},
                ),
                meta={"units": "MMcf"},
            ),
            _state_storage_result("pa", value=83.44),
        ]
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="latest",
            primary_metric="underground_storage_withdrawals_monthly",
            metrics=["underground_storage_withdrawals_monthly"],
            storage_dataset="underground_storage_all_operators",
            storage_frequency="monthly",
            storage_metric_type="withdrawals",
            regions=[],
            states=["pa"],
            states_all=False,
            start_date="2026-05-01",
            end_date="2026-05-31",
            normalized_query="what were withdrawals in 2026-05?",
            chart_type="none",
            output_mode="answer",
            filters={
                "states": ["pa"],
                "states_all": False,
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": "monthly",
                "storage_metric_type": "withdrawals",
            },
        )

        result = executor.execute_storage_route(route)

        self.assertEqual(eia.underground_storage_all_operators.call_count, 2)
        first_call = eia.underground_storage_all_operators.call_args_list[0]
        second_call = eia.underground_storage_all_operators.call_args_list[1]
        self.assertEqual(
            first_call.kwargs,
            {
                "start": "2026-05-01",
                "end": "2026-05-31",
                "state": "pa",
                "metric_type": "withdrawals",
                "frequency": "monthly",
            },
        )
        self.assertEqual(
            second_call.kwargs,
            {
                "start": "2024-05-31",
                "end": "2026-05-31",
                "state": "pa",
                "metric_type": "withdrawals",
                "frequency": "monthly",
            },
        )
        self.assertTrue(result.meta["latest_available_fallback"])
        self.assertEqual(result.meta["fallback_fetch_start_date"], "2024-05-31")
        self.assertEqual(result.meta["fallback_fetch_end_date"], "2026-05-31")
        self.assertEqual(result.meta["fetch_start_date"], "2024-05-31")
        self.assertEqual(result.meta["fetch_end_date"], "2026-05-31")

    def test_regional_compare_storage_route_expands_regions_without_lower48(self) -> None:
        eia = Mock()
        eia.storage_working_gas.side_effect = lambda region, **kwargs: _storage_result(region)
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="regional_compare",
            regions=["lower48"],
            chart_type="bar",
            filters={"regions": ["lower48"]},
        )

        result = executor.execute_storage_route(route)

        self.assertEqual(
            set(result.df["region"]),
            {
                "east",
                "midwest",
                "south_central",
                "south_central_salt",
                "south_central_nonsalt",
                "mountain",
                "pacific",
            },
        )
        self.assertNotIn("lower48", set(result.df["region"]))

    def test_storage_level_can_include_weekly_change(self) -> None:
        eia = Mock()
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
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="working_gas_storage_lower48",
                start="2024-01-01",
                end="2024-12-31",
                filters={"region": "east", "include_weekly_change": True},
            )
        )

        self.assertEqual(result.source.reference, "eia-ng-client:natural_gas.storage_with_weekly_change")

    def test_underground_storage_all_operators_multiple_states_concatenates_state_column(self) -> None:
        eia = Mock()

        def make_result(*, state: str, **kwargs):
            return _state_storage_result(state, value=20.0 if state == "tx" else 30.0)

        eia.underground_storage_all_operators.side_effect = make_result
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="underground_storage_injections_monthly",
                start="2020-01-01",
                end="2020-12-31",
                filters={
                    "states": ["tx", "la"],
                    "storage_frequency": "monthly",
                    "storage_metric_type": "injections",
                },
            )
        )

        self.assertEqual(eia.underground_storage_all_operators.call_count, 2)
        self.assertEqual(set(result.df["state"]), {"tx", "la"})
        self.assertEqual(list(result.df.columns), ["date", "value", "state"])

    def test_underground_storage_all_operators_states_all_expands_in_executor(self) -> None:
        eia = Mock()

        def make_result(*, state: str, **kwargs):
            return _state_storage_result(state, value=20.0)

        eia.underground_storage_all_operators.side_effect = make_result
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="underground_storage_working_gas_monthly",
                start="2020-01-01",
                end="2020-12-31",
                filters={
                    "states": [],
                    "states_all": True,
                    "storage_frequency": "monthly",
                    "storage_metric_type": "working_gas",
                },
            )
        )

        self.assertEqual(eia.underground_storage_all_operators.call_count, 30)
        self.assertNotIn("united_states_total", set(result.df["state"]))
        self.assertNotIn("nj", set(result.df["state"]))

    def test_execute_storage_route_for_state_storage_attaches_state_metadata(self) -> None:
        eia = Mock()
        eia.underground_storage_all_operators.side_effect = lambda state, **kwargs: _state_storage_result(state)
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            primary_metric="underground_storage_working_gas_monthly",
            metrics=["underground_storage_working_gas_monthly"],
            storage_dataset="underground_storage_all_operators",
            storage_frequency="monthly",
            storage_metric_type="working_gas",
            regions=[],
            states=["tx", "la"],
            states_all=False,
            filters={
                "states": ["tx", "la"],
                "states_all": False,
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": "monthly",
                "storage_metric_type": "working_gas",
            },
        )

        result = executor.execute_storage_route(route)

        self.assertEqual(set(result.df["state"]), {"tx", "la"})
        self.assertEqual(result.meta["storage_dataset"], "underground_storage_all_operators")
        self.assertEqual(result.meta["storage_frequency"], "monthly")
        self.assertEqual(result.meta["storage_metric_type"], "working_gas")
        self.assertEqual(result.meta["states"], ["tx", "la"])
        self.assertFalse(result.meta["states_all"])

    def test_execute_storage_route_for_storage_by_type_uses_storage_type_filters(self) -> None:
        eia = Mock()
        eia.underground_storage_by_type.return_value = _storage_type_result("salt_cavern")
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="latest",
            primary_metric="underground_storage_by_type_working_gas_monthly",
            metrics=["underground_storage_by_type_working_gas_monthly"],
            storage_dataset="underground_storage_by_type",
            storage_frequency="monthly",
            storage_metric_type="working_gas",
            storage_type="salt_cavern",
            storage_types_all=False,
            start_date=None,
            regions=[],
            states=[],
            filters={
                "storage_dataset": "underground_storage_by_type",
                "storage_frequency": "monthly",
                "storage_metric_type": "working_gas",
                "storage_type": "salt_cavern",
                "storage_types_all": False,
            },
        )

        result = executor.execute_storage_route(route)

        eia.underground_storage_by_type.assert_called_once_with(
            start="2024-06-01",
            end="2026-06-01",
            storage_type="salt_cavern",
            metric_type="working_gas",
            frequency="monthly",
        )
        self.assertEqual(result.df["storage_type"].tolist(), ["salt_cavern"])
        self.assertEqual(result.meta["storage_type"], "salt_cavern")
        self.assertFalse(result.meta["storage_types_all"])

    def test_storage_by_type_all_expands_in_executor(self) -> None:
        eia = Mock()
        eia.underground_storage_by_type.side_effect = lambda storage_type, **kwargs: _storage_type_result(storage_type)
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="regional_compare",
            primary_metric="underground_storage_by_type_working_gas_monthly",
            metrics=["underground_storage_by_type_working_gas_monthly"],
            storage_dataset="underground_storage_by_type",
            storage_frequency="monthly",
            storage_metric_type="working_gas",
            storage_type=None,
            storage_types_all=True,
            regions=[],
            states=[],
            filters={
                "storage_dataset": "underground_storage_by_type",
                "storage_frequency": "monthly",
                "storage_metric_type": "working_gas",
                "storage_type": None,
                "storage_types_all": True,
            },
        )

        result = executor.execute_storage_route(route)

        self.assertEqual(eia.underground_storage_by_type.call_count, 3)
        self.assertEqual(set(result.df["storage_type"]), {"salt_cavern", "depleted_field", "aquifer"})

    def test_storage_by_type_time_series_without_explicit_start_gets_history_window(self) -> None:
        eia = Mock()
        eia.underground_storage_by_type.return_value = _storage_type_result("salt_cavern")
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="time_series",
            primary_metric="underground_storage_by_type_working_gas_monthly",
            metrics=["underground_storage_by_type_working_gas_monthly"],
            storage_dataset="underground_storage_by_type",
            storage_frequency="monthly",
            storage_metric_type="working_gas",
            storage_type="salt_cavern",
            storage_types_all=False,
            start_date="2025-12-16",
            end_date="2026-06-16",
            regions=[],
            states=[],
            chart_type="line",
            output_mode="chart_and_answer",
            normalized_query="plot storage type history.",
            filters={
                "storage_dataset": "underground_storage_by_type",
                "storage_frequency": "monthly",
                "storage_metric_type": "working_gas",
                "storage_type": "salt_cavern",
                "storage_types_all": False,
            },
        )

        result = executor.execute_storage_route(route)

        eia.underground_storage_by_type.assert_called_once_with(
            start="2020-06-16",
            end="2026-06-16",
            storage_type="salt_cavern",
            metric_type="working_gas",
            frequency="monthly",
        )
        self.assertEqual(result.meta["fetch_start_date"], "2020-06-16")
        self.assertEqual(result.meta["fetch_end_date"], "2026-06-16")

    def test_storage_by_type_time_series_with_explicit_start_keeps_requested_window(self) -> None:
        eia = Mock()
        eia.underground_storage_by_type.return_value = _storage_type_result("salt_cavern")
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="time_series",
            primary_metric="underground_storage_by_type_working_gas_monthly",
            metrics=["underground_storage_by_type_working_gas_monthly"],
            storage_dataset="underground_storage_by_type",
            storage_frequency="monthly",
            storage_metric_type="working_gas",
            storage_type="salt_cavern",
            storage_types_all=False,
            start_date="2015-01-01",
            end_date="2026-06-16",
            regions=[],
            states=[],
            chart_type="line",
            output_mode="chart_and_answer",
            normalized_query="show salt cavern working gas storage since 2015.",
            filters={
                "storage_dataset": "underground_storage_by_type",
                "storage_frequency": "monthly",
                "storage_metric_type": "working_gas",
                "storage_type": "salt_cavern",
                "storage_types_all": False,
            },
        )

        result = executor.execute_storage_route(route)

        eia.underground_storage_by_type.assert_called_once_with(
            start="2015-01-01",
            end="2026-06-16",
            storage_type="salt_cavern",
            metric_type="working_gas",
            frequency="monthly",
        )
        self.assertEqual(result.meta["fetch_start_date"], "2015-01-01")
        self.assertEqual(result.meta["fetch_end_date"], "2026-06-16")

    def test_annual_storage_by_type_compare_without_explicit_date_expands_window(self) -> None:
        eia = Mock()
        eia.underground_storage_by_type.return_value = _storage_type_result("salt_cavern")
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="regional_compare",
            primary_metric="underground_storage_by_type_injections_annual",
            metrics=["underground_storage_by_type_injections_annual"],
            storage_dataset="underground_storage_by_type",
            storage_frequency="annual",
            storage_metric_type="injections",
            storage_type=None,
            storage_types_all=True,
            start_date="2025-12-16",
            end_date="2026-06-16",
            regions=[],
            states=[],
            chart_type="bar",
            output_mode="chart_and_answer",
            normalized_query="compare annual injections by storage type.",
            filters={
                "storage_dataset": "underground_storage_by_type",
                "storage_frequency": "annual",
                "storage_metric_type": "injections",
                "storage_type": None,
                "storage_types_all": True,
            },
        )

        executor.execute_storage_route(route)

        self.assertEqual(eia.underground_storage_by_type.call_count, 3)
        for call in eia.underground_storage_by_type.call_args_list:
            self.assertEqual(call.kwargs["start"], "2016-06-16")
            self.assertEqual(call.kwargs["end"], "2026-06-16")
            self.assertEqual(call.kwargs["metric_type"], "injections")
            self.assertEqual(call.kwargs["frequency"], "annual")

    def test_annual_storage_by_type_time_series_retries_with_latest_completed_year_when_empty(self) -> None:
        eia = Mock()
        eia.underground_storage_by_type.side_effect = [
            EIAResult(
                df=pd.DataFrame(columns=["date", "value", "storage_type"]),
                source=SourceRef(
                    source_type="eia_api",
                    label="Storage salt",
                    reference="ref:salt-empty",
                    parameters={"storage_type": "salt_cavern"},
                ),
                meta={"units": "MMcf"},
            ),
            _storage_type_result("salt_cavern", value=83.44),
        ]
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="time_series",
            primary_metric="underground_storage_by_type_working_gas_annual",
            metrics=["underground_storage_by_type_working_gas_annual"],
            storage_dataset="underground_storage_by_type",
            storage_frequency="annual",
            storage_metric_type="working_gas",
            storage_type="salt_cavern",
            storage_types_all=False,
            start_date="2025-12-16",
            end_date="2026-06-16",
            regions=[],
            states=[],
            chart_type="line",
            output_mode="chart_and_answer",
            normalized_query="show annual working gas storage by type.",
            filters={
                "storage_dataset": "underground_storage_by_type",
                "storage_frequency": "annual",
                "storage_metric_type": "working_gas",
                "storage_type": "salt_cavern",
                "storage_types_all": False,
            },
        )

        result = executor.execute_storage_route(route)

        self.assertEqual(eia.underground_storage_by_type.call_count, 2)
        first_call = eia.underground_storage_by_type.call_args_list[0]
        second_call = eia.underground_storage_by_type.call_args_list[1]
        self.assertEqual(
            first_call.kwargs,
            {
                "start": "2020-06-16",
                "end": "2026-06-16",
                "storage_type": "salt_cavern",
                "metric_type": "working_gas",
                "frequency": "annual",
            },
        )
        self.assertEqual(
            second_call.kwargs,
            {
                "start": "2019-12-31",
                "end": "2025-12-31",
                "storage_type": "salt_cavern",
                "metric_type": "working_gas",
                "frequency": "annual",
            },
        )
        self.assertTrue(result.meta["latest_available_fallback"])
        self.assertEqual(result.meta["fallback_fetch_start_date"], "2019-12-31")
        self.assertEqual(result.meta["fallback_fetch_end_date"], "2025-12-31")

    def test_underground_storage_capacity_routes_to_capacity_adapter(self) -> None:
        eia = Mock()
        eia.underground_storage_capacity.return_value = _geography_storage_result("tx", value=250.0)
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="underground_storage_working_gas_capacity_annual",
                start="2015-01-01",
                end="2015-12-31",
                filters={
                    "states": ["tx"],
                    "storage_frequency": "annual",
                    "storage_metric_type": "working_gas_capacity",
                },
            )
        )

        eia.underground_storage_capacity.assert_called_once_with(
            start="2015-01-01",
            end="2015-12-31",
            geography="tx",
            capacity_type="working_gas",
            frequency="annual",
        )
        self.assertEqual(result.df["state"].tolist(), ["tx"])
        self.assertEqual(result.df["geography"].tolist(), ["tx"])

    def test_underground_storage_count_routes_to_count_adapter(self) -> None:
        eia = Mock()
        eia.underground_storage_count.return_value = _geography_storage_result("lower48", value=398.0, units="count")
        executor = MetricExecutor(eia=eia)

        result = executor.execute(
            ExecuteRequest(
                metric="underground_storage_field_count_monthly",
                start="2020-01-01",
                end="2020-12-31",
                filters={
                    "regions": ["lower48"],
                    "storage_frequency": "monthly",
                    "storage_metric_type": "storage_field_count",
                },
            )
        )

        eia.underground_storage_count.assert_called_once_with(
            start="2020-01-01",
            end="2020-12-31",
            geography="lower48",
            frequency="monthly",
        )
        self.assertEqual(result.df["region"].tolist(), ["lower48"])
        self.assertEqual(result.df["geography"].tolist(), ["lower48"])

    def test_execute_storage_route_for_capacity_by_region_preserves_regions(self) -> None:
        eia = Mock()
        eia.underground_storage_capacity.side_effect = lambda geography, **kwargs: _geography_storage_result(
            geography,
            value=100.0 if geography == "east" else 200.0,
        )
        executor = MetricExecutor(eia=eia)
        route = _storage_route(
            analysis_type="regional_compare",
            primary_metric="underground_storage_total_capacity_monthly",
            metrics=["underground_storage_total_capacity_monthly"],
            storage_dataset="underground_storage_all_operators",
            storage_frequency="monthly",
            storage_metric_type="total_capacity",
            regions=["east", "midwest"],
            states=[],
            states_all=False,
            chart_type="bar",
            output_mode="chart_and_answer",
            filters={
                "regions": ["east", "midwest"],
                "states": [],
                "states_all": False,
                "storage_dataset": "underground_storage_all_operators",
                "storage_frequency": "monthly",
                "storage_metric_type": "total_capacity",
            },
        )

        result = executor.execute_storage_route(route)

        self.assertEqual(eia.underground_storage_capacity.call_count, 2)
        self.assertEqual(set(result.df["region"]), {"east", "midwest"})
        self.assertEqual(set(result.df["geography"]), {"east", "midwest"})

    def test_weather_forecast_metric_passes_region_filter(self) -> None:
        eia = Mock()
        eia.weather_degree_days_forecast_vs_5y.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia)

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
        eia.weather_degree_days_forecast_vs_5y.return_value = Mock(
            df=None, source=None, meta={"cache": {}}
        )
        executor = MetricExecutor(eia=eia)

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
        executor = MetricExecutor(eia=eia)

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
        executor = MetricExecutor(eia=eia)

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

    def test_weekly_energy_atlas_summary_combines_weather_storage_supply_and_price(self) -> None:
        eia = Mock()
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
        executor = MetricExecutor(eia=eia)

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
