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


if __name__ == "__main__":
    unittest.main()
