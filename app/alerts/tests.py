from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from django.contrib.auth import get_user_model
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone

from alerts.models import AlertOperator, AlertRule, AlertTriggerType, AlertValueMode, SharedAnswer
from alerts.services import SignalErrorCode, SignalEvaluation, SignalEvaluator, should_trigger_alert
from alerts.tasks import evaluate_alert_rule_now
from alerts.views import forecast_metric_view
from executer import MetricResult
from schemas.answer import SourceRef
from tools.forecasting import ForecastErrorCode, TrendForecaster


class _StubExecutor:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def execute(self, _request):
        return MetricResult(
            df=self.df,
            source=SourceRef(
                source_type="manual",
                label="test-source",
                reference="unit-test",
            ),
            meta={},
        )


class _MappingExecutor:
    def __init__(self, mapping: dict[str, pd.DataFrame]):
        self.mapping = mapping

    def execute(self, req):
        return SimpleNamespace(
            df=self.mapping[req.metric].copy(),
            source=SimpleNamespace(reference=f"test:{req.metric}"),
            meta={},
        )


class TrendForecasterAppTests(SimpleTestCase):
    def test_forecast_metric_view_returns_structured_payload(self):
        factory = RequestFactory()
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        df = pd.DataFrame({"date": dates, "value": list(range(30))})
        forecaster = TrendForecaster(executor=_StubExecutor(df))
        request = factory.post(
            "/alerts/forecast/",
            data=json.dumps(
                {
                    "metric": "working_gas_storage_lower48",
                    "horizon_days": 7,
                    "include_overlay": True,
                }
            ),
            content_type="application/json",
        )

        with patch("alerts.views.build_metric_forecaster", return_value=forecaster):
            response = forecast_metric_view(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload["metric"], "working_gas_storage_lower48")
        self.assertEqual(payload["horizon_days"], 7)
        self.assertIn("overlay", payload)

    def test_forecaster_invalid_horizon_returns_structured_error(self):
        dates = pd.date_range("2025-01-01", periods=30, freq="D")
        df = pd.DataFrame({"date": dates, "value": list(range(30))})
        forecaster = TrendForecaster(executor=_StubExecutor(df))

        result = forecaster.forecast_metric("working_gas_storage_lower48", horizon_days=30)

        self.assertEqual(result.error_code, ForecastErrorCode.INVALID_HORIZON)


class TriggerBehaviorTests(SimpleTestCase):
    def test_condition_true_only_on_false_to_true_transition(self):
        self.assertTrue(
            should_trigger_alert(
                previous_result=False,
                new_result=True,
                trigger_type=AlertTriggerType.CONDITION_TRUE,
            )
        )
        self.assertFalse(
            should_trigger_alert(
                previous_result=True,
                new_result=True,
                trigger_type=AlertTriggerType.CONDITION_TRUE,
            )
        )

    def test_condition_always_triggers_when_true(self):
        self.assertTrue(
            should_trigger_alert(
                previous_result=True,
                new_result=True,
                trigger_type=AlertTriggerType.CONDITION_ALWAYS,
            )
        )
        self.assertFalse(
            should_trigger_alert(
                previous_result=False,
                new_result=False,
                trigger_type=AlertTriggerType.CONDITION_ALWAYS,
            )
        )

    def test_condition_false_only_on_true_to_false_transition(self):
        self.assertTrue(
            should_trigger_alert(
                previous_result=True,
                new_result=False,
                trigger_type=AlertTriggerType.CONDITION_FALSE,
            )
        )
        self.assertFalse(
            should_trigger_alert(
                previous_result=False,
                new_result=False,
                trigger_type=AlertTriggerType.CONDITION_FALSE,
            )
        )

    def test_return_answer_triggers_unless_error(self):
        self.assertTrue(
            should_trigger_alert(
                previous_result=None,
                new_result=None,
                trigger_type=AlertTriggerType.RETURN_ANSWER,
                error_code=None,
            )
        )
        self.assertFalse(
            should_trigger_alert(
                previous_result=None,
                new_result=None,
                trigger_type=AlertTriggerType.RETURN_ANSWER,
                error_code="UNSUPPORTED_SIGNAL",
            )
        )


class StructuredEvaluationTests(SimpleTestCase):
    def _evaluator(self, mapping: dict[str, pd.DataFrame]) -> SignalEvaluator:
        return SignalEvaluator(executor=_MappingExecutor(mapping), eia=SimpleNamespace())

    def test_raw_mode_evaluation(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-01", "2026-04-02"]),
                "value": [2.0, 3.5],
            }
        )
        evaluator = self._evaluator({"henry_hub_spot_price": df})
        rule = SimpleNamespace(
            question="Henry Hub raw check",
            metric="henry_hub_spot_price",
            value_mode=AlertValueMode.RAW,
            operator=AlertOperator.GT,
            threshold=3.0,
        )

        evaluation = evaluator.evaluate_rule(rule)

        self.assertEqual(evaluation.error_code, None)
        self.assertTrue(evaluation.result)
        self.assertEqual(evaluation.values["raw_value"], 3.5)
        self.assertEqual(evaluation.values["evaluated_value"], 3.5)

    def test_zscore_mode_evaluation(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2026-01-01", periods=6, freq="D"),
                "value": [10, 11, 9, 10, 10, 14],
            }
        )
        evaluator = self._evaluator({"henry_hub_spot_price": df})
        rule = SimpleNamespace(
            question="Henry Hub z-score check",
            metric="henry_hub_spot_price",
            value_mode=AlertValueMode.ZSCORE,
            operator=AlertOperator.GT,
            threshold=1.5,
        )

        evaluation = evaluator.evaluate_rule(rule)

        self.assertIsNone(evaluation.error_code)
        self.assertTrue(evaluation.result)
        self.assertGreater(float(evaluation.values["evaluated_value"]), 1.5)

    def test_unsupported_metric_fails_gracefully(self):
        evaluator = self._evaluator({})
        rule = SimpleNamespace(
            question="Unsupported metric",
            metric="nonexistent_metric",
            value_mode=AlertValueMode.RAW,
            operator=AlertOperator.GT,
            threshold=1.0,
        )
        evaluation = evaluator.evaluate_rule(rule)
        self.assertEqual(evaluation.error_code, SignalErrorCode.UNSUPPORTED_SIGNAL)
        self.assertIsNone(evaluation.result)

    def test_unsupported_zscore_mode_fails_gracefully(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-01", "2026-04-02"]),
                "value": [1, 2],
            }
        )
        evaluator = self._evaluator({"market_supply_regime": df})
        rule = SimpleNamespace(
            question="Supply regime z-score check",
            metric="market_supply_regime",
            value_mode=AlertValueMode.ZSCORE,
            operator=AlertOperator.GT,
            threshold=1.0,
        )
        evaluation = evaluator.evaluate_rule(rule)
        self.assertEqual(evaluation.error_code, SignalErrorCode.UNSUPPORTED_SIGNAL)
        self.assertIsNone(evaluation.result)


class AlertUiAndApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="tester@example.com",
            email="tester@example.com",
            password="secret123",
        )
        self.client.force_login(self.user)

    def _base_payload(self) -> dict:
        return {
            "name": "Henry Hub breakout",
            "question": "Notify me when Henry Hub breaks above threshold.",
            "metric": "henry_hub_spot_price",
            "value_mode": AlertValueMode.RAW,
            "operator": AlertOperator.GT,
            "threshold": "3.0",
            "frequency": "hourly",
            "trigger_type": AlertTriggerType.CONDITION_TRUE,
            "cooldown_hours": "0",
        }

    def test_create_form_requires_structured_fields(self):
        response = self.client.get(reverse("alerts:create"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'name="metric"')
        self.assertContains(response, 'name="value_mode"')
        self.assertContains(response, 'name="operator"')
        self.assertContains(response, 'name="threshold"')

        payload = self._base_payload()
        payload.pop("metric")
        with patch("alerts.views.can_create_alert", return_value=(True, None)):
            response = self.client.post(reverse("alerts:create"), data=payload)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "metric is required")

    def test_api_create_read_update_rule(self):
        evaluation = SignalEvaluation(
            question="test",
            result=True,
            explanation="deterministic",
            values={
                "raw_value": 3.4,
                "evaluated_value": 3.4,
                "condition_result": True,
            },
            metric="henry_hub_spot_price",
        )
        with (
            patch("alerts.views.can_create_alert", return_value=(True, None)),
            patch("alerts.views.build_signal_evaluator") as mock_build_evaluator,
        ):
            mock_build_evaluator.return_value.evaluate_rule.return_value = evaluation
            response = self.client.post(
                reverse("alerts:create_rule"),
                data=json.dumps(self._base_payload()),
                content_type="application/json",
            )
        self.assertEqual(response.status_code, 201)
        created_payload = response.json()
        alert_rule_id = created_payload["alert_rule_id"]
        self.assertEqual(created_payload["metric"], "henry_hub_spot_price")
        self.assertEqual(created_payload["value_mode"], AlertValueMode.RAW)
        self.assertEqual(created_payload["operator"], AlertOperator.GT)
        self.assertEqual(created_payload["threshold"], 3.0)

        read_response = self.client.get(reverse("alerts:rule_api", args=[alert_rule_id]))
        self.assertEqual(read_response.status_code, 200)
        self.assertEqual(read_response.json()["metric"], "henry_hub_spot_price")

        update_payload = self._base_payload()
        update_payload["threshold"] = 3.8
        update_payload["operator"] = AlertOperator.GTE
        with patch("alerts.views.build_signal_evaluator") as mock_build_evaluator:
            mock_build_evaluator.return_value.evaluate_rule.return_value = SignalEvaluation(
                question="test",
                result=False,
                explanation="deterministic",
                values={
                    "raw_value": 3.4,
                    "evaluated_value": 3.4,
                    "condition_result": False,
                },
                metric="henry_hub_spot_price",
            )
            update_response = self.client.put(
                reverse("alerts:rule_api", args=[alert_rule_id]),
                data=json.dumps(update_payload),
                content_type="application/json",
            )
        self.assertEqual(update_response.status_code, 200)
        self.assertEqual(update_response.json()["operator"], AlertOperator.GTE)
        self.assertEqual(update_response.json()["threshold"], 3.8)

    def test_import_metric_requires_country_code(self):
        payload = self._base_payload()
        payload["metric"] = "import"
        with patch("alerts.views.can_create_alert", return_value=(True, None)):
            response = self.client.post(reverse("alerts:create"), data=payload)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "country_code is required")

    def test_production_metric_accepts_state_selection(self):
        payload = self._base_payload()
        payload["metric"] = "production"
        payload["geography_type"] = "state"
        payload["state_code"] = "tx"
        evaluation = SignalEvaluation(
            question="test",
            result=True,
            explanation="deterministic",
            values={
                "raw_value": 3.4,
                "evaluated_value": 3.4,
                "condition_result": True,
                "region": "tx",
            },
            metric="ng_production_lower48",
        )
        with (
            patch("alerts.views.can_create_alert", return_value=(True, None)),
            patch("alerts.views.build_signal_evaluator") as mock_build_evaluator,
        ):
            mock_build_evaluator.return_value.evaluate_rule.return_value = evaluation
            response = self.client.post(reverse("alerts:create"), data=payload)

        self.assertRedirects(response, reverse("alerts:list"))
        rule = AlertRule.objects.get(user=self.user, metric="production")
        self.assertEqual(rule.region, "tx")


class CooldownBehaviorTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="cooldown@example.com",
            email="cooldown@example.com",
            password="secret123",
        )

    def test_cooldown_blocks_second_notification(self):
        rule = AlertRule.objects.create(
            user=self.user,
            name="Cooldown rule",
            question="Condition always true",
            signal_id="structured_condition",
            metric="henry_hub_spot_price",
            value_mode=AlertValueMode.RAW,
            operator=AlertOperator.GT,
            threshold=1.0,
            trigger_type=AlertTriggerType.CONDITION_ALWAYS,
            cooldown_hours=24,
            delivery_channels=["email"],
        )
        evaluation = SignalEvaluation(
            question=rule.question,
            result=True,
            explanation="Always true",
            values={
                "raw_value": 4.0,
                "evaluated_value": 4.0,
                "condition_result": True,
            },
            metric=rule.metric,
        )

        with patch("alerts.tasks.build_signal_evaluator") as mock_build_evaluator:
            mock_build_evaluator.return_value.evaluate_rule.return_value = evaluation
            first = evaluate_alert_rule_now(rule.id)
            second = evaluate_alert_rule_now(rule.id)

        self.assertTrue(first["notification_sent"])
        self.assertFalse(second["notification_sent"])
        rule.refresh_from_db()
        self.assertIsNotNone(rule.last_notified_at)
        self.assertGreaterEqual(rule.last_notified_at, timezone.now() - timedelta(minutes=1))


class SharedAnswerTests(TestCase):
    def test_create_shared_answer_returns_public_url_and_persists_payload(self):
        response = self.client.post(
            reverse("create-shared-answer"),
            data=json.dumps(
                {
                    "question": "Are exports higher than last year?",
                    "response_json": {
                        "answer": "Exports remain above last year.",
                        "signal": {"status": "bullish", "confidence": 0.84},
                        "summary": "Exports remain above last year.",
                        "drivers": ["LNG feedgas demand stayed firm."],
                        "data_points": [
                            {"metric": "lng_exports", "value": 14.2, "unit": "bcf/d"}
                        ],
                        "forecast": {
                            "direction": "up",
                            "reasoning": "Current utilization remains elevated.",
                        },
                        "suggested_alerts": [],
                        "alerts": [],
                        "sources": [
                            {"title": "EIA LNG Export Feedgas", "date": "2026-04-07"}
                        ],
                    },
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        payload = json.loads(response.content)
        shared_answer = SharedAnswer.objects.get(share_id=payload["share_id"])
        self.assertTrue(payload["url"].endswith(payload["path"]))
        self.assertEqual(shared_answer.question, "Are exports higher than last year?")
        self.assertEqual(shared_answer.response_json["signal"]["status"], "bullish")
