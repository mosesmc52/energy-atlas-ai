import unittest
from unittest.mock import patch

from agents.router import LLMRouteOutput
from agents.router import route_query


class TestRouter(unittest.TestCase):
    def test_production_growth_query_stays_on_rule_route(self) -> None:
        result = route_query("Is production growing year over year?")
        self.assertEqual(result.intent, "single_metric")
        self.assertEqual(result.primary_metric, "ng_production_lower48")
        self.assertEqual(result.metrics, ["ng_production_lower48"])
        self.assertEqual(result.source, "rule")

    @patch("agents.router.llm_route_structured")
    def test_ambiguous_query_can_still_fall_back_to_llm(self, mock_llm) -> None:
        mock_llm.return_value = LLMRouteOutput(
            intent="ambiguous",
            primary_metric=None,
            metrics=[],
            filters=None,
            reason="Need clarification",
            confidence=0.2,
            ambiguous=True,
        )

        result = route_query("How is gas doing?")
        self.assertEqual(result.intent, "unsupported")
        self.assertTrue(result.ambiguous)

    def test_reserves_query_uses_long_default_window(self) -> None:
        result = route_query("Are reserves increasing or decreasing?")
        self.assertEqual(result.primary_metric, "ng_exploration_reserves_lower48")
        self.assertEqual(result.start, "2000-01-01")

    def test_reserves_query_preserves_explicit_date_window(self) -> None:
        result = route_query("Are reserves increasing or decreasing this year?")
        self.assertEqual(result.primary_metric, "ng_exploration_reserves_lower48")
        self.assertNotEqual(result.start, "2000-01-01")

    def test_sector_consumption_ranking_uses_rule_route(self) -> None:
        result = route_query(
            "Which sector consumes the most gas (power, residential, industrial)?"
        )
        self.assertEqual(result.intent, "ranking")
        self.assertEqual(result.primary_metric, "ng_consumption_by_sector")
        self.assertEqual(result.metrics, ["ng_consumption_by_sector"])
        self.assertEqual(result.source, "rule")


if __name__ == "__main__":
    unittest.main()
