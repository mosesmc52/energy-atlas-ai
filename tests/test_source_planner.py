from __future__ import annotations

import unittest

from agents.llm_query_parser import parse_energy_query
from agents.source_planner import build_source_plan


class TestSourcePlanner(unittest.TestCase):
    def test_storage_level_query_routes_to_eia_storage_metric(self) -> None:
        parsed = parse_energy_query(
            user_query="What is current working gas in storage?",
            normalized_query="what is current working gas in storage?",
        )

        plan = build_source_plan(parsed)

        self.assertEqual(plan.intent, "latest")
        self.assertEqual([call.metric for call in plan.calls], ["working_gas_storage_lower48"])
        self.assertEqual(plan.calls[0].adapter, "eia")

    def test_storage_weekly_change_routes_to_eia_change_metric(self) -> None:
        parsed = parse_energy_query(
            user_query="Are injections accelerating?",
            normalized_query="are injections accelerating?",
        )

        plan = build_source_plan(parsed)

        self.assertEqual(plan.intent, "weekly_change")
        self.assertEqual(
            [call.metric for call in plan.calls],
            ["working_gas_storage_change_weekly"],
        )
        self.assertEqual(plan.comparison, "prior_week")

    def test_route_dates_are_attached_to_source_call(self) -> None:
        from datetime import date

        from agents.router import route_query

        route = route_query("Compare East and Midwest storage since 2021")
        plan = build_source_plan(route)

        self.assertEqual(plan.time_window, "custom")
        self.assertEqual(plan.calls[0].start_date, "2021-01-01")
        self.assertEqual(plan.calls[0].end_date, date.today().isoformat())


if __name__ == "__main__":
    unittest.main()
