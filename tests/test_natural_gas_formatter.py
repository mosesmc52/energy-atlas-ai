import unittest

from answers.response_formatters.natural_gas import (
    NaturalGasMetricSnapshot,
    format_natural_gas_commentary,
)


class TestNaturalGasFormatter(unittest.TestCase):
    def test_storage_above_normal_leans_loose(self) -> None:
        out = format_natural_gas_commentary(
            NaturalGasMetricSnapshot(
                metric_name="Natural gas storage",
                category="storage",
                subtype="storage_level",
                date="2026-05-08",
                current_value=2290,
                unit="Bcf",
                baseline_5y=2100,
                difference=190,
                percent_difference=9.0,
                range_5y_min=1800,
                range_5y_max=2300,
            )
        )
        self.assertEqual(out["market_signal"], "loose")
        self.assertIn("above seasonal norms", out["summary"])

    def test_storage_below_normal_leans_tight(self) -> None:
        out = format_natural_gas_commentary(
            NaturalGasMetricSnapshot(
                metric_name="Natural gas storage",
                category="storage",
                subtype="storage_level",
                date="2026-05-08",
                current_value=1800,
                unit="Bcf",
                baseline_5y=2100,
                difference=-300,
                percent_difference=-14.3,
                range_5y_min=1750,
                range_5y_max=2400,
            )
        )
        self.assertEqual(out["market_signal"], "tight")
        self.assertIn("below seasonal norms", out["summary"])

    def test_production_above_normal_commentary(self) -> None:
        out = format_natural_gas_commentary(
            NaturalGasMetricSnapshot(
                metric_name="Dry gas production",
                category="production",
                subtype="dry_gas",
                date="2026-02-01",
                current_value=3080138,
                unit="MMcf",
                baseline_5y=2950000,
                difference=130138,
                percent_difference=4.4,
            )
        )
        self.assertIn("near seasonal norms", out["summary"])

    def test_exports_elevated_commentary(self) -> None:
        out = format_natural_gas_commentary(
            NaturalGasMetricSnapshot(
                metric_name="LNG exports",
                category="exports",
                subtype="lng_exports",
                date="2026-05-10",
                current_value=15000,
                unit="MMcf",
                baseline_5y=12000,
                difference=3000,
                percent_difference=25.0,
            )
        )
        self.assertIn("leans tighter for the domestic market", out["summary"])

    def test_missing_baseline(self) -> None:
        out = format_natural_gas_commentary(
            NaturalGasMetricSnapshot(
                metric_name="Natural gas imports",
                category="imports",
                subtype="imports",
                date="2026-05-10",
                current_value=5000,
                unit="MMcf",
            )
        )
        self.assertIn("seasonal baseline was unavailable", out["summary"])

    def test_near_normal_value(self) -> None:
        out = format_natural_gas_commentary(
            NaturalGasMetricSnapshot(
                metric_name="Natural gas consumption",
                category="consumption",
                subtype="total",
                date="2026-05-10",
                current_value=1000,
                unit="MMcf",
                baseline_5y=980,
                difference=20,
                percent_difference=2.0,
            )
        )
        self.assertEqual(out["market_signal"], "neutral")
        self.assertIn("near seasonal norms", out["summary"])


if __name__ == "__main__":
    unittest.main()
