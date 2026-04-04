from __future__ import annotations

import unittest
from unittest.mock import Mock

from agents.router import route_query
from executer import ExecuteRequest, MetricExecutor


class TestCFTCRouting(unittest.TestCase):
    def test_router_maps_managed_money_net(self) -> None:
        result = route_query("Show CFTC managed money net in Henry Hub natural gas")
        self.assertEqual(result.primary_metric, "managed_money_net")

    def test_router_maps_open_interest(self) -> None:
        result = route_query("What is COT open interest in Henry Hub gas?")
        self.assertEqual(result.primary_metric, "open_interest")

    def test_executor_dispatches_cftc_metric(self) -> None:
        eia = Mock()
        grid = Mock()
        des = Mock()
        cftc = Mock()
        cftc.get_metric.return_value = Mock(df=None, source=None, meta={"cache": {}})
        executor = MetricExecutor(eia=eia, grid=grid, des=des, cftc=cftc)

        executor.execute(
            ExecuteRequest(
                metric="managed_money_net",
                start="2024-01-01",
                end="2024-12-31",
                filters={},
            )
        )

        cftc.get_metric.assert_called_once_with(
            "managed_money_net",
            start="2024-01-01",
            end="2024-12-31",
        )


if __name__ == "__main__":
    unittest.main()
