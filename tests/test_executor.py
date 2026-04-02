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


if __name__ == "__main__":
    unittest.main()
