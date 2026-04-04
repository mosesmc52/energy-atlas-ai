from __future__ import annotations

import unittest
from unittest.mock import Mock

from agents.router import route_query
from executer import ExecuteRequest, MetricExecutor


class TestDesRouterAndExecutor(unittest.TestCase):
    def test_router_maps_business_activity_query(self) -> None:
        result = route_query("Show Dallas Fed business activity index since 2020")
        self.assertEqual(result.primary_metric, "des_business_activity_index")
        self.assertEqual(result.source, "rule")

    def test_router_maps_special_questions_query(self) -> None:
        result = route_query("Summarize Dallas Fed special questions from the latest survey")
        self.assertEqual(result.primary_metric, "des_special_questions_text")

    def test_executor_dispatches_des_metric(self) -> None:
        eia = Mock()
        grid = Mock()
        des = Mock()
        des.get_metric.return_value = Mock(df=None, source=None, meta={"cache": {}})
        executor = MetricExecutor(eia=eia, grid=grid, des=des)

        executor.execute(
            ExecuteRequest(
                metric="des_business_activity_index",
                start="2025-01-01",
                end="2025-12-31",
                filters={},
            )
        )

        des.get_metric.assert_called_once_with(
            "des_business_activity_index",
            start_date="2025-01-01",
            end_date="2025-12-31",
        )


if __name__ == "__main__":
    unittest.main()
