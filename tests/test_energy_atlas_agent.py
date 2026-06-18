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
        "storage_dataset": "weekly_working_gas",
        "storage_frequency": "weekly",
        "storage_metric_type": "working_gas",
        "storage_type": None,
        "storage_types_all": False,
        "regions": ["lower48"],
        "states": [],
        "states_all": False,
        "start_date": "2026-01-01",
        "end_date": "2026-04-24",
        "date_expression": None,
        "value_type": "level",
        "comparisons": ["none"],
        "ranking_basis": "current_storage",
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
        executor.execute_storage_route.assert_not_called()

    def test_runs_route_execute_and_answer_builder(self) -> None:
        executor = Mock()
        metric_result = Mock(
            df=Mock(),
            source=Mock(reference="ref:test"),
            meta={},
        )
        executor.execute_storage_route.return_value = metric_result
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
        executor.execute_storage_route.assert_called_once()
        answer_builder_fn.assert_called_once_with(
            query="What is current working gas in storage?",
            result=metric_result,
            route=route_fn.return_value,
            mode="observed",
            model="gpt-5.2",
        )

    def test_storage_route_preserves_regions_list_into_executor(self) -> None:
        executor = Mock()
        metric_result = Mock(df=Mock(), source=Mock(reference="ref:test"), meta={})
        executor.execute_storage_route.return_value = metric_result
        route_fn = Mock(
            return_value=_route(
                analysis_type="time_series",
                chart_type="line",
                output_mode="chart_and_answer",
                regions=["east", "midwest"],
                filters={"regions": ["east", "midwest"]},
            )
        )
        agent = EnergyAtlasAgent(
            executor=executor,
            route_fn=route_fn,
            answer_builder_fn=Mock(return_value=Mock()),
        )

        agent.run(user_query="Compare East and Midwest storage since 2021")

        routed = executor.execute_storage_route.call_args.args[0]
        self.assertEqual(routed.regions, ["east", "midwest"])
        self.assertEqual(routed.filters["regions"], ["east", "midwest"])

    def test_storage_route_passes_storage_type_filters_into_executor(self) -> None:
        executor = Mock()
        metric_result = Mock(df=Mock(), source=Mock(reference="ref:test"), meta={})
        executor.execute_storage_route.return_value = metric_result
        route_fn = Mock(
            return_value=_route(
                primary_metric="underground_storage_by_type_working_gas_monthly",
                storage_dataset="underground_storage_by_type",
                storage_frequency="monthly",
                storage_metric_type="working_gas",
                storage_type="salt_cavern",
                storage_types_all=False,
                regions=[],
                filters={
                    "storage_dataset": "underground_storage_by_type",
                    "storage_frequency": "monthly",
                    "storage_metric_type": "working_gas",
                    "storage_type": "salt_cavern",
                    "storage_types_all": False,
                },
            )
        )
        agent = EnergyAtlasAgent(
            executor=executor,
            route_fn=route_fn,
            answer_builder_fn=Mock(return_value=Mock()),
        )

        agent.run(user_query="What is working gas storage in salt caverns?")

        routed = executor.execute_storage_route.call_args.args[0]
        self.assertEqual(routed.filters["storage_type"], "salt_cavern")
        self.assertFalse(routed.filters["storage_types_all"])

    def test_storage_route_preserves_capacity_regions_into_executor(self) -> None:
        executor = Mock()
        metric_result = Mock(df=Mock(), source=Mock(reference="ref:test"), meta={})
        executor.execute_storage_route.return_value = metric_result
        route_fn = Mock(
            return_value=_route(
                primary_metric="underground_storage_total_capacity_monthly",
                storage_dataset="underground_storage_all_operators",
                storage_frequency="monthly",
                storage_metric_type="total_capacity",
                analysis_type="regional_compare",
                chart_type="bar",
                output_mode="chart_and_answer",
                regions=["east", "midwest"],
                states=[],
                filters={
                    "regions": ["east", "midwest"],
                    "states": [],
                    "states_all": False,
                    "storage_dataset": "underground_storage_all_operators",
                    "storage_frequency": "monthly",
                    "storage_metric_type": "total_capacity",
                },
            )
        )
        agent = EnergyAtlasAgent(
            executor=executor,
            route_fn=route_fn,
            answer_builder_fn=Mock(return_value=Mock()),
        )

        agent.run(user_query="Compare storage capacity by region.")

        routed = executor.execute_storage_route.call_args.args[0]
        self.assertEqual(routed.filters["regions"], ["east", "midwest"])
        self.assertEqual(routed.filters["storage_metric_type"], "total_capacity")


if __name__ == "__main__":
    unittest.main()
