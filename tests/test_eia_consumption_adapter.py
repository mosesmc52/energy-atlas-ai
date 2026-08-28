from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tools.eia_adapter import EIAAdapter


class TestEIAConsumptionAdapter(unittest.TestCase):
    @patch("tools.eia_adapter.EIAClient")
    def test_end_use_uses_updated_consumption_namespace(self, mock_client_cls: Mock) -> None:
        consumption = SimpleNamespace(
            end_use=Mock(return_value=[{"period": "2024", "value": "12.5"}])
        )
        client = SimpleNamespace(natural_gas=SimpleNamespace(consumption=consumption))
        mock_client_cls.return_value = client

        result = EIAAdapter().consumption_end_use(
            start="2024-01-01",
            end="2024-12-31",
            state="tx",
            type="industrial",
        )

        consumption.end_use.assert_called_once_with(
            start="2024",
            end="2024",
            state="tx",
            frequency="annual",
            type="industrial",
        )
        self.assertEqual(result.df["value"].tolist(), [12.5])
        self.assertEqual(result.df["state"].tolist(), ["tx"])
        self.assertEqual(result.df["type"].tolist(), ["industrial"])
        self.assertEqual(
            result.source.reference,
            "eia-ng-client:natural_gas.consumption.end_use",
        )

    @patch("tools.eia_adapter.EIAClient")
    def test_number_of_consumers_passes_sector_and_category(
        self, mock_client_cls: Mock
    ) -> None:
        consumption = SimpleNamespace(
            number_of_consumers=Mock(return_value=[{"period": "2024", "value": "42"}])
        )
        mock_client_cls.return_value = SimpleNamespace(
            natural_gas=SimpleNamespace(consumption=consumption)
        )

        result = EIAAdapter().consumption_number_of_consumers(
            start="2020",
            end="2024",
            state="us_total",
            sector="commercial",
            category="sales",
        )

        consumption.number_of_consumers.assert_called_once_with(
            start="2020",
            end="2024",
            state="us_total",
            frequency="annual",
            sector="commercial",
            category="sales",
        )
        self.assertEqual(result.df["value"].tolist(), [42.0])
        self.assertEqual(result.meta["sector"], "commercial")

    @patch("tools.eia_adapter.EIAClient")
    def test_end_use_maps_electric_power_for_client(self, mock_client_cls: Mock) -> None:
        consumption = SimpleNamespace(
            end_use=Mock(return_value=[{"period": "2024-01", "value": "12.5"}])
        )
        mock_client_cls.return_value = SimpleNamespace(
            natural_gas=SimpleNamespace(consumption=consumption)
        )

        EIAAdapter().consumption_end_use(
            start="2024-01-01",
            end="2024-12-31",
            state="fl",
            type="electric_power",
            frequency="monthly",
        )

        consumption.end_use.assert_called_once_with(
            start="2024-01",
            end="2024-12",
            state="fl",
            frequency="monthly",
            type="electric",
        )

    def test_consumption_parameter_validation(self) -> None:
        adapter = EIAAdapter.__new__(EIAAdapter)

        with self.assertRaises(ValueError):
            adapter.consumption_end_use(
                start="2024",
                end="2024",
                state="tx",
                type="transportation",
            )
        with self.assertRaises(ValueError):
            adapter.consumption_number_of_consumers(
                start="2024",
                end="2024",
                state="tx",
                sector="residential",
                category="customers",
            )


if __name__ == "__main__":
    unittest.main()
