from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import Mock

from agents.agent_policy import load_agent_policy
from agents.energy_atlas_agent import EnergyAtlasAgent
from agents.router import EnergyRouteResult


def _route(**overrides) -> EnergyRouteResult:
    values = {
        "domain": "storage",
        "analysis_type": "latest",
        "primary_metric": "working_gas_storage_lower48",
        "metrics": ["working_gas_storage_lower48"],
        "regions": ["lower48"],
        "start_date": "2026-01-01",
        "end_date": "2026-04-24",
        "date_expression": None,
        "value_type": "level",
        "comparisons": ["none"],
        "chart_type": "none",
        "output_mode": "answer",
        "filters": {"regions": ["lower48"]},
        "confidence": 0.9,
        "ambiguous": False,
        "reason": None,
        "normalized_query": "storage",
    }
    values.update(overrides)
    return EnergyRouteResult(**values)


class TestEnergyAtlasAgent(unittest.TestCase):
    def test_load_agent_policy_reads_json_file(self) -> None:
        with NamedTemporaryFile("w", suffix=".json", encoding="utf-8") as handle:
            handle.write(
                """
{
  "answer_model": "gpt-5.4",
  "enable_forecast": true,
  "default_forecast_horizon_days": 9,
  "max_forecast_horizon_days": 12,
  "disable_forecast_metrics": ["working_gas_storage_change_weekly"],
  "force_forecast_metrics": ["working_gas_storage_lower48"]
}
                """.strip()
            )
            handle.flush()
            policy = load_agent_policy(handle.name)

        self.assertEqual(policy.answer_model, "gpt-5.4")
        self.assertTrue(policy.enable_forecast)
        self.assertEqual(policy.default_forecast_horizon_days, 9)
        self.assertEqual(policy.max_forecast_horizon_days, 12)
        self.assertIn("working_gas_storage_change_weekly", policy.disable_forecast_metrics)
        self.assertIn("working_gas_storage_lower48", policy.force_forecast_metrics)

    def test_load_agent_policy_missing_file_uses_defaults(self) -> None:
        policy = load_agent_policy(Path("/tmp/does-not-exist-agent-policy.json"))
        self.assertEqual(policy.answer_model, "gpt-5.2")
        self.assertTrue(policy.enable_forecast)

    def test_returns_early_for_unsupported_route(self) -> None:
        executor = Mock()
        route_fn = Mock(
            return_value=_route(
                domain="unsupported",
                analysis_type="unsupported",
                primary_metric=None,
                metrics=[],
                regions=[],
            )
        )
        agent = EnergyAtlasAgent(
            executor=executor,
            route_fn=route_fn,
            answer_builder_fn=Mock(),
        )

        outcome = agent.run(user_query="What is Henry Hub price?")

        self.assertEqual(outcome.route.domain, "unsupported")
        self.assertIsNone(outcome.result)
        self.assertIsNone(outcome.payload)
        executor.execute.assert_not_called()

    def test_runs_route_execute_and_answer_builder(self) -> None:
        executor = Mock()
        metric_result = Mock(
            df=Mock(),
            source=Mock(reference="ref:test"),
            meta={},
        )
        executor.execute_plan.return_value = {"working_gas_storage_lower48": metric_result}
        route_fn = Mock(return_value=_route())
        payload = Mock()
        answer_builder_fn = Mock(return_value=payload)
        agent = EnergyAtlasAgent(
            executor=executor,
            model="gpt-5.2",
            route_fn=route_fn,
            answer_builder_fn=answer_builder_fn,
        )

        outcome = agent.run(user_query="What is current working gas in storage?")

        self.assertIs(outcome.result, metric_result)
        self.assertIs(outcome.payload, payload)
        executor.execute_plan.assert_called_once()
        answer_builder_fn.assert_called_once_with(
            query="What is current working gas in storage?",
            result=metric_result,
            mode="observed",
            model="gpt-5.2",
        )


if __name__ == "__main__":
    unittest.main()
