import unittest

from agents.metric_capabilities import get_metric_capability


class TestMetricCapabilities(unittest.TestCase):
    def test_iso_gas_dependency_has_proxy_fallback(self) -> None:
        capability = get_metric_capability("iso_gas_dependency")
        self.assertEqual(capability.fallback_metric, "ng_electricity")
        self.assertIsNotNone(capability.fallback_note)

    def test_unknown_metric_returns_default_capability(self) -> None:
        capability = get_metric_capability("unknown_metric")
        self.assertEqual(capability.metric, "unknown_metric")
        self.assertIsNone(capability.fallback_metric)

    def test_time_series_metrics_expose_default_lookback(self) -> None:
        self.assertEqual(
            get_metric_capability("ng_electricity").default_lookback_years, 2
        )
        self.assertEqual(
            get_metric_capability("ng_consumption_lower48").default_lookback_years, 2
        )


if __name__ == "__main__":
    unittest.main()
