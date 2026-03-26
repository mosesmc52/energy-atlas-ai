from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from agents.llm_router import llm_route_structured


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.output_text = json.dumps(payload)


class _FakeResponses:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def create(self, **_: object) -> _FakeResponse:
        return _FakeResponse(self._payload)


class _FakeClient:
    def __init__(self, payload: dict) -> None:
        self.responses = _FakeResponses(payload)


class TestLLMRouterStructured(unittest.TestCase):
    def _call_with_payload(self, payload: dict):
        with patch(
            "agents.llm_router._get_openai_client",
            return_value=_FakeClient(payload),
        ):
            return llm_route_structured(
                user_query="test",
                normalized_query="test",
            )

    def test_straightforward_single_metric(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "iso_load",
                "metrics": ["iso_load"],
                "filters": {"iso": "ercot"},
                "reason": "Load in ERCOT",
                "confidence": 0.92,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.intent, "single_metric")
        self.assertEqual(result.primary_metric, "iso_load")
        self.assertEqual(result.metrics, ["iso_load"])
        self.assertEqual(result.filters, {"iso": "ercot"})

    def test_ambiguous_forces_ambiguous_flag(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "ambiguous",
                "primary_metric": None,
                "metrics": [],
                "filters": None,
                "reason": "Could refer to demand or consumption",
                "confidence": 0.4,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.intent, "ambiguous")
        self.assertTrue(result.ambiguous)

    def test_compare_inserts_primary_into_metrics(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "compare",
                "primary_metric": "lng_exports",
                "metrics": ["lng_imports"],
                "filters": {"region": "canada_pipeline"},
                "reason": "Compare import and export flows",
                "confidence": 0.8,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.intent, "compare")
        self.assertEqual(result.metrics, ["lng_exports", "lng_imports"])

    def test_production_state_filter_is_preserved(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "ng_production_lower48",
                "metrics": ["ng_production_lower48"],
                "filters": {"region": "tx"},
                "reason": "Texas production",
                "confidence": 0.88,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.primary_metric, "ng_production_lower48")
        self.assertEqual(result.filters, {"region": "tx"})

    def test_consumption_state_filter_is_preserved(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "ng_consumption_lower48",
                "metrics": ["ng_consumption_lower48"],
                "filters": {"region": "ca"},
                "reason": "California consumption",
                "confidence": 0.88,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.primary_metric, "ng_consumption_lower48")
        self.assertEqual(result.filters, {"region": "ca"})

    def test_import_region_filter_is_preserved(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "lng_imports",
                "metrics": ["lng_imports"],
                "filters": {"region": "qatar"},
                "reason": "Qatar imports",
                "confidence": 0.88,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.primary_metric, "lng_imports")
        self.assertEqual(result.filters, {"region": "qatar"})

    def test_export_region_filter_is_preserved(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "lng_exports",
                "metrics": ["lng_exports"],
                "filters": {"region": "japan"},
                "reason": "Japan exports",
                "confidence": 0.88,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.primary_metric, "lng_exports")
        self.assertEqual(result.filters, {"region": "japan"})

    def test_import_compressed_region_filter_is_preserved(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "lng_imports",
                "metrics": ["lng_imports"],
                "filters": {"region": "united_states_compressed_total"},
                "reason": "Compressed imports total",
                "confidence": 0.88,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.primary_metric, "lng_imports")
        self.assertEqual(
            result.filters, {"region": "united_states_compressed_total"}
        )

    def test_export_truck_region_filter_is_preserved(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "lng_exports",
                "metrics": ["lng_exports"],
                "filters": {"region": "united_states_truck_total"},
                "reason": "Truck exports total",
                "confidence": 0.88,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.primary_metric, "lng_exports")
        self.assertEqual(result.filters, {"region": "united_states_truck_total"})

    def test_reserves_filters_are_preserved(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "single_metric",
                "primary_metric": "ng_exploration_reserves_lower48",
                "metrics": ["ng_exploration_reserves_lower48"],
                "filters": {
                    "region": "tx",
                    "resource_category": "proved_ngl",
                },
                "reason": "Texas proved ngl reserves",
                "confidence": 0.88,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.primary_metric, "ng_exploration_reserves_lower48")
        self.assertEqual(
            result.filters,
            {"region": "tx", "resource_category": "proved_ngl"},
        )

    def test_derived_clamps_confidence_and_filters(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "derived",
                "primary_metric": "working_gas_storage_change_weekly",
                "metrics": ["working_gas_storage_change_weekly"],
                "filters": {"region": "lower48", "iso": "not_real"},
                "reason": "Derived week-over-week storage tightness",
                "confidence": 1.7,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.intent, "derived")
        self.assertEqual(result.confidence, 1.0)
        self.assertEqual(result.filters, {"region": "lower48"})

    def test_unsupported_forces_null_primary_and_empty_metrics(self) -> None:
        result = self._call_with_payload(
            {
                "intent": "unsupported",
                "primary_metric": "iso_load",
                "metrics": ["iso_load"],
                "filters": {"iso": "ercot"},
                "reason": "Topic not covered",
                "confidence": 0.5,
                "ambiguous": False,
            }
        )
        self.assertEqual(result.intent, "unsupported")
        self.assertIsNone(result.primary_metric)
        self.assertEqual(result.metrics, [])


if __name__ == "__main__":
    unittest.main()
