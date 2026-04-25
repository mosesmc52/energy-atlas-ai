import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import Mock
import pandas as pd

from agents.agent_policy import load_agent_policy
from agents.energy_atlas_agent import EnergyAtlasAgent
from agents.router import HybridRouteResult


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
  "disable_forecast_metrics": ["ng_supply_balance_regime"],
  "force_forecast_metrics": ["henry_hub_spot"]
}
                """.strip()
            )
            handle.flush()
            policy = load_agent_policy(handle.name)

        self.assertEqual(policy.answer_model, "gpt-5.4")
        self.assertTrue(policy.enable_forecast)
        self.assertEqual(policy.default_forecast_horizon_days, 9)
        self.assertEqual(policy.max_forecast_horizon_days, 12)
        self.assertIn("ng_supply_balance_regime", policy.disable_forecast_metrics)
        self.assertIn("henry_hub_spot", policy.force_forecast_metrics)

    def test_load_agent_policy_missing_file_uses_defaults(self) -> None:
        policy = load_agent_policy(Path("/tmp/does-not-exist-agent-policy.json"))
        self.assertEqual(policy.answer_model, "gpt-5.2")
        self.assertTrue(policy.enable_forecast)

    def test_returns_early_for_unsupported_route(self) -> None:
        executor = Mock()
        route_fn = Mock(
            return_value=HybridRouteResult(
                intent="unsupported",
                primary_metric=None,
                metrics=[],
                start="2026-01-01",
                end="2026-04-24",
                filters=None,
                confidence=0.0,
                ambiguous=False,
                source="rule",
            )
        )
        agent = EnergyAtlasAgent(
            executor=executor, route_fn=route_fn, answer_builder_fn=Mock()
        )

        outcome = agent.run(user_query="unknown question")

        self.assertEqual(outcome.route.intent, "unsupported")
        self.assertIsNone(outcome.result)
        self.assertIsNone(outcome.payload)
        self.assertIsNone(outcome.forecast)
        executor.execute.assert_not_called()

    def test_runs_route_execute_and_answer_builder(self) -> None:
        executor = Mock()
        metric_result = Mock(
            df=Mock(),
            source=Mock(reference="ref:test"),
            meta={},
        )
        executor.execute.return_value = metric_result
        route_fn = Mock(
            return_value=HybridRouteResult(
                intent="single_metric",
                primary_metric="henry_hub_spot",
                metrics=["henry_hub_spot"],
                start="2026-01-01",
                end="2026-04-24",
                filters={"region": "united_states_total"},
                confidence=0.9,
                ambiguous=False,
                source="rule",
            )
        )
        payload = Mock()
        answer_builder_fn = Mock(return_value=payload)
        agent = EnergyAtlasAgent(
            executor=executor,
            model="gpt-5.2",
            route_fn=route_fn,
            answer_builder_fn=answer_builder_fn,
        )

        outcome = agent.run(user_query="What is Henry Hub price?")

        self.assertIs(outcome.result, metric_result)
        self.assertIs(outcome.payload, payload)
        executor.execute.assert_called_once()
        answer_builder_fn.assert_called_once_with(
            query="What is Henry Hub price?",
            result=metric_result,
            mode="observed",
            model="gpt-5.2",
        )

    def test_includes_forecast_when_requested(self) -> None:
        executor = Mock()
        df_obj = Mock()
        metric_result = Mock(
            df=df_obj,
            source=Mock(reference="ref:test"),
            meta={},
        )
        executor.execute.return_value = metric_result
        route_fn = Mock(
            return_value=HybridRouteResult(
                intent="single_metric",
                primary_metric="henry_hub_spot",
                metrics=["henry_hub_spot"],
                start="2026-01-01",
                end="2026-04-24",
                filters=None,
                confidence=0.9,
                ambiguous=False,
                source="rule",
                include_forecast=True,
                forecast_horizon_days=14,
            )
        )
        forecast_obj = Mock()
        forecaster = Mock()
        forecaster.forecast_dataframe.return_value = forecast_obj
        agent = EnergyAtlasAgent(
            executor=executor,
            route_fn=route_fn,
            answer_builder_fn=Mock(return_value=Mock()),
        )

        outcome = agent.run(user_query="Forecast Henry Hub", forecaster=forecaster)

        self.assertIs(outcome.forecast, forecast_obj)
        forecaster.forecast_dataframe.assert_called_once_with(
            df_obj,
            metric="henry_hub_spot",
            horizon_days=14,
            include_overlay=True,
            source_reference="ref:test",
        )

    def test_forecast_horizon_is_capped_by_policy(self) -> None:
        executor = Mock()
        metric_result = Mock(
            df=Mock(),
            source=Mock(reference="ref:test"),
            meta={},
        )
        executor.execute.return_value = metric_result
        route_fn = Mock(
            return_value=HybridRouteResult(
                intent="single_metric",
                primary_metric="henry_hub_spot",
                metrics=["henry_hub_spot"],
                start="2026-01-01",
                end="2026-04-24",
                filters=None,
                confidence=0.9,
                ambiguous=False,
                source="rule",
                include_forecast=True,
                forecast_horizon_days=30,
            )
        )
        forecaster = Mock()
        forecaster.forecast_dataframe.return_value = Mock()
        agent = EnergyAtlasAgent(
            executor=executor,
            route_fn=route_fn,
            answer_builder_fn=Mock(return_value=Mock()),
            policy_path="config/agent_policy.json",
        )

        agent.run(user_query="Forecast Henry Hub", forecaster=forecaster)
        _, kwargs = forecaster.forecast_dataframe.call_args
        self.assertEqual(kwargs["horizon_days"], 14)

    def test_iso_gas_dependency_falls_back_to_ng_electricity_when_empty(self) -> None:
        executor = Mock()
        empty_iso_result = Mock(
            df=pd.DataFrame(columns=["date", "gas_share"]),
            source=Mock(reference="ref:iso"),
            meta={"metric": "iso_gas_dependency"},
        )
        proxy_result = Mock(
            df=pd.DataFrame([{"date": "2026-04-01", "value": 100.0}]),
            source=Mock(reference="ref:ng_electricity"),
            meta={"metric": "ng_electricity"},
        )
        executor.execute.side_effect = [empty_iso_result, proxy_result]
        route_fn = Mock(
            return_value=HybridRouteResult(
                intent="single_metric",
                primary_metric="iso_gas_dependency",
                metrics=["iso_gas_dependency"],
                start="2026-01-01",
                end="2026-04-24",
                filters={"iso": "ercot"},
                confidence=0.9,
                ambiguous=False,
                source="rule",
            )
        )
        payload = Mock()
        answer_builder_fn = Mock(return_value=payload)
        agent = EnergyAtlasAgent(
            executor=executor,
            route_fn=route_fn,
            answer_builder_fn=answer_builder_fn,
        )

        outcome = agent.run(
            user_query="What percentage of electricity generation is coming from natural gas?"
        )

        self.assertIs(outcome.payload, payload)
        self.assertEqual(executor.execute.call_count, 2)
        proxy_used = answer_builder_fn.call_args.kwargs["result"]
        self.assertEqual(proxy_used.meta.get("metric"), "ng_electricity")
        self.assertEqual(proxy_used.meta.get("proxy_for_metric"), "iso_gas_dependency")


if __name__ == "__main__":
    unittest.main()
