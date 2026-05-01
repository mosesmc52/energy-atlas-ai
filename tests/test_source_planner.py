from __future__ import annotations

import unittest

from agents.llm_query_parser import LLMQueryParseOutput
from agents.source_planner import build_source_plan


class TestSourcePlanner(unittest.TestCase):
    def test_power_demand_translating_into_gas_usage_expands_multi_source(self) -> None:
        parsed = LLMQueryParseOutput(
            intent="derived",
            primary_metric="ng_electricity",
            metrics=["ng_electricity"],
            filters=None,
            time_window="this_week",
            comparison="none",
            calculation="summary",
            question_topics=["demand", "power"],
            requires_multiple_sources=True,
            reason="derived power-to-gas translation",
            confidence=0.82,
            ambiguous=False,
        )
        plan = build_source_plan(parsed)
        metrics = [c.metric for c in plan.calls]
        self.assertEqual(plan.intent, "derived")
        self.assertIn("ng_electricity", metrics)
        self.assertIn("iso_load", metrics)
        self.assertIn("weather_degree_days_forecast_vs_5y", metrics)
        self.assertTrue(plan.requires_multiple_sources)

    def test_storage_query_routes_storage_metric(self) -> None:
        parsed = LLMQueryParseOutput(
            intent="single_metric",
            primary_metric="working_gas_storage_lower48",
            metrics=["working_gas_storage_lower48"],
            filters={"region": "lower48"},
            time_window="latest",
            comparison="none",
            calculation="latest_value",
            question_topics=["storage"],
            requires_multiple_sources=False,
            reason=None,
            confidence=0.9,
            ambiguous=False,
        )
        plan = build_source_plan(parsed)
        self.assertEqual([c.metric for c in plan.calls], ["working_gas_storage_lower48"])

    def test_lng_exports_week_query_keeps_exports_with_comparison(self) -> None:
        parsed = LLMQueryParseOutput(
            intent="single_metric",
            primary_metric="lng_exports",
            metrics=["lng_exports"],
            filters={"region": "united_states_pipeline_total"},
            time_window="this_week",
            comparison="week_over_week",
            calculation="change",
            question_topics=["trade"],
            requires_multiple_sources=False,
            reason=None,
            confidence=0.87,
            ambiguous=False,
        )
        plan = build_source_plan(parsed)
        self.assertEqual(plan.calls[0].metric, "lng_exports")
        self.assertEqual(plan.comparison, "week_over_week")

    def test_price_because_of_storage_adds_price_and_storage_metrics(self) -> None:
        parsed = LLMQueryParseOutput(
            intent="explain",
            primary_metric="henry_hub_spot",
            metrics=["henry_hub_spot"],
            filters=None,
            time_window="last_30_days",
            comparison="none",
            calculation="summary",
            question_topics=["price", "storage"],
            requires_multiple_sources=True,
            reason=None,
            confidence=0.79,
            ambiguous=False,
        )
        plan = build_source_plan(parsed)
        metrics = [c.metric for c in plan.calls]
        self.assertIn("henry_hub_spot", metrics)
        self.assertIn("working_gas_storage_lower48", metrics)

    def test_ercot_gas_burn_includes_iso_dependency_with_filter(self) -> None:
        parsed = LLMQueryParseOutput(
            intent="single_metric",
            primary_metric="iso_gas_dependency",
            metrics=["iso_gas_dependency"],
            filters={"iso": "ercot"},
            time_window="this_week",
            comparison="none",
            calculation="summary",
            question_topics=["power", "demand"],
            requires_multiple_sources=False,
            reason=None,
            confidence=0.84,
            ambiguous=False,
        )
        plan = build_source_plan(parsed)
        self.assertEqual(plan.calls[0].metric, "iso_gas_dependency")
        self.assertEqual(plan.calls[0].filters, {"iso": "ercot"})


if __name__ == "__main__":
    unittest.main()
