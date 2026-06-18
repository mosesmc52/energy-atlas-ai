from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from tools.eia_adapter import EIAAdapter


class TestEIAUndergroundStorageAdapter(unittest.TestCase):
    @patch("tools.eia_adapter.EIAClient")
    def test_underground_storage_all_operators_uses_eia_ng_method(self, mock_client_cls: Mock) -> None:
        client = Mock()
        client.natural_gas.underground_storage_all_operators.return_value = [
            {"period": "2024-01", "value": "123.4"},
            {"period": "2024-02", "value": "124.5"},
        ]
        mock_client_cls.return_value = client

        adapter = EIAAdapter()
        result = adapter.underground_storage_all_operators(
            start="2024-01-15",
            end="2024-12-20",
            state="tx",
            metric_type="working_gas",
            frequency="monthly",
        )

        client.natural_gas.underground_storage_all_operators.assert_called_once_with(
            start="2024-01",
            end="2024-12",
            geography="tx",
            metric_type="working_gas",
            frequency="monthly",
        )
        self.assertEqual(result.df["state"].tolist(), ["tx", "tx"])
        self.assertEqual(result.df["value"].tolist(), [123.4, 124.5])
        self.assertEqual(
            result.df["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2024-01-01", "2024-02-01"],
        )
        self.assertEqual(
            result.source.reference,
            "eia-ng-client:natural_gas.underground_storage_all_operators",
        )
        self.assertEqual(result.source.parameters["geography"], "tx")
        self.assertEqual(result.source.parameters["start"], "2024-01")
        self.assertEqual(result.source.parameters["end"], "2024-12")
        self.assertEqual(result.meta["units"], "MMcf")

    @patch("tools.eia_adapter.EIAClient")
    def test_underground_storage_all_operators_maps_united_states_total_to_us_total(
        self, mock_client_cls: Mock
    ) -> None:
        client = Mock()
        client.natural_gas.underground_storage_all_operators.return_value = [
            {"period": "2024", "value": "5.5"}
        ]
        mock_client_cls.return_value = client

        adapter = EIAAdapter()
        result = adapter.underground_storage_all_operators(
            start="2024-01-15",
            end="2024-12-20",
            state="united_states_total",
            metric_type="working_gas_yoy_pct_change",
            frequency="annual",
        )

        client.natural_gas.underground_storage_all_operators.assert_called_once_with(
            start="2024",
            end="2024",
            geography="us_total",
            metric_type="working_gas_yoy_pct_change",
            frequency="annual",
        )
        self.assertEqual(result.df["state"].tolist(), ["united_states_total"])
        self.assertEqual(
            result.df["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2024-01-01"],
        )
        self.assertEqual(result.meta["units"], "%")
        self.assertEqual(result.source.parameters["geography"], "us_total")
        self.assertEqual(result.source.parameters["start"], "2024")
        self.assertEqual(result.source.parameters["end"], "2024")

    @patch("tools.eia_adapter.EIAClient")
    def test_underground_storage_by_type_uses_eia_ng_method(self, mock_client_cls: Mock) -> None:
        client = Mock()
        client.natural_gas.underground_storage_type.return_value = [
            {"period": "2024-01", "value": "123.4"},
            {"period": "2024-02", "value": "124.5"},
        ]
        mock_client_cls.return_value = client

        adapter = EIAAdapter()
        result = adapter.underground_storage_by_type(
            start="2024-01-15",
            end="2024-12-20",
            storage_type="salt_cavern",
            metric_type="working_gas",
            frequency="monthly",
        )

        client.natural_gas.underground_storage_type.assert_called_once_with(
            start="2024-01",
            end="2024-12",
            storage_type="salt_working_gas",
            frequency="monthly",
        )
        self.assertEqual(result.df["storage_type"].tolist(), ["salt_cavern", "salt_cavern"])
        self.assertEqual(result.df["value"].tolist(), [123.4, 124.5])
        self.assertEqual(
            result.df["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2024-01-01", "2024-02-01"],
        )
        self.assertEqual(
            result.source.reference,
            "eia-ng-client:natural_gas.underground_storage_type",
        )
        self.assertEqual(result.source.parameters["storage_type"], "salt_cavern")
        self.assertEqual(result.source.parameters["eia_storage_type"], "salt_working_gas")
        self.assertEqual(result.source.parameters["start"], "2024-01")
        self.assertEqual(result.source.parameters["end"], "2024-12")
        self.assertEqual(result.meta["units"], "MMcf")

    @patch("tools.eia_adapter.EIAClient")
    def test_underground_storage_by_type_annual_falls_back_to_monthly_last_value_for_stock_metrics(
        self, mock_client_cls: Mock
    ) -> None:
        client = Mock()
        client.natural_gas.underground_storage_type.side_effect = [
            [],
            [
                {"period": "2024-01", "value": "100.0"},
                {"period": "2024-12", "value": "200.0"},
                {"period": "2025-01", "value": "300.0"},
                {"period": "2025-12", "value": "400.0"},
            ],
        ]
        mock_client_cls.return_value = client

        adapter = EIAAdapter()
        result = adapter.underground_storage_by_type(
            start="2024-01-15",
            end="2025-12-20",
            storage_type="salt_cavern",
            metric_type="working_gas",
            frequency="annual",
        )

        self.assertEqual(client.natural_gas.underground_storage_type.call_count, 2)
        first_call = client.natural_gas.underground_storage_type.call_args_list[0]
        second_call = client.natural_gas.underground_storage_type.call_args_list[1]
        self.assertEqual(
            first_call.kwargs,
            {
                "start": "2024",
                "end": "2025",
                "storage_type": "salt_working_gas",
                "frequency": "annual",
            },
        )
        self.assertEqual(
            second_call.kwargs,
            {
                "start": "2024-01",
                "end": "2025-12",
                "storage_type": "salt_working_gas",
                "frequency": "monthly",
            },
        )
        self.assertEqual(result.df["value"].tolist(), [200.0, 400.0])
        self.assertEqual(
            result.df["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2024-12-01", "2025-12-01"],
        )
        self.assertTrue(result.meta["derived_from_monthly"])

    @patch("tools.eia_adapter.EIAClient")
    def test_underground_storage_by_type_annual_falls_back_to_monthly_sum_for_flow_metrics(
        self, mock_client_cls: Mock
    ) -> None:
        client = Mock()
        client.natural_gas.underground_storage_type.side_effect = [
            [],
            [
                {"period": "2024-01", "value": "10.0"},
                {"period": "2024-12", "value": "20.0"},
                {"period": "2025-01", "value": "30.0"},
                {"period": "2025-12", "value": "40.0"},
            ],
        ]
        mock_client_cls.return_value = client

        adapter = EIAAdapter()
        result = adapter.underground_storage_by_type(
            start="2024-01-15",
            end="2025-12-20",
            storage_type="salt_cavern",
            metric_type="withdrawals",
            frequency="annual",
        )

        self.assertEqual(result.df["value"].tolist(), [30.0, 70.0])
        self.assertEqual(
            result.df["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2024-01-01", "2025-01-01"],
        )
        self.assertTrue(result.meta["derived_from_monthly"])

    @patch("tools.eia_adapter.EIAClient")
    def test_underground_storage_capacity_uses_eia_ng_method(self, mock_client_cls: Mock) -> None:
        client = Mock()
        client.natural_gas.underground_storage_capacity.return_value = [
            {"period": "2015", "value": "456.7"}
        ]
        mock_client_cls.return_value = client

        adapter = EIAAdapter()
        result = adapter.underground_storage_capacity(
            start="2015-01-15",
            end="2015-12-20",
            geography="tx",
            capacity_type="working_gas",
            frequency="annual",
        )

        client.natural_gas.underground_storage_capacity.assert_called_once_with(
            start="2015",
            end="2015",
            geography="tx",
            type="working_gas",
            frequency="annual",
        )
        self.assertEqual(result.df["geography"].tolist(), ["tx"])
        self.assertEqual(result.meta["units"], "MMcf")
        self.assertEqual(result.meta["capacity_type"], "working_gas")
        self.assertEqual(result.source.reference, "eia-ng-client:natural_gas.underground_storage_capacity")

    @patch("tools.eia_adapter.EIAClient")
    def test_underground_storage_count_uses_eia_ng_method(self, mock_client_cls: Mock) -> None:
        client = Mock()
        client.natural_gas.underground_storage_count.return_value = [
            {"period": "2020-01", "value": "398"}
        ]
        mock_client_cls.return_value = client

        adapter = EIAAdapter()
        result = adapter.underground_storage_count(
            start="2020-01-01",
            end="2020-12-31",
            geography="lower48",
            frequency="monthly",
        )

        client.natural_gas.underground_storage_count.assert_called_once_with(
            start="2020-01",
            end="2020-12",
            geography="lower48",
            frequency="monthly",
        )
        self.assertEqual(result.df["geography"].tolist(), ["lower48"])
        self.assertEqual(result.meta["units"], "count")
        self.assertEqual(result.meta["metric_type"], "storage_field_count")
        self.assertEqual(result.source.reference, "eia-ng-client:natural_gas.underground_storage_count")


if __name__ == "__main__":
    unittest.main()
