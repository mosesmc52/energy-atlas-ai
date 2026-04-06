from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd
from django.contrib.auth import get_user_model
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.urls import reverse

from alerts.models import AlertRule, AlertTriggerType
from alerts.services import ParsedSignal, SignalEvaluation, should_trigger_alert
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


class AnswerMonitorTriggerTests(SimpleTestCase):
    def test_should_trigger_alert_returns_true_for_successful_answer_monitor_evaluation(self):
        self.assertTrue(
            should_trigger_alert(
                previous_result=None,
                new_result=None,
                trigger_type=AlertTriggerType.EVERY_ANSWER,
                error_code=None,
            )
        )

    def test_should_trigger_alert_returns_false_for_failed_answer_monitor_evaluation(self):
        self.assertFalse(
            should_trigger_alert(
                previous_result=None,
                new_result=None,
                trigger_type=AlertTriggerType.EVERY_ANSWER,
                error_code="UNSUPPORTED_SIGNAL",
            )
        )


class AlertRuleAnswerMonitorFlowTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="tester@example.com",
            email="tester@example.com",
            password="secret123",
        )
        self.client.force_login(self.user)

    def test_create_alert_persists_answer_monitor_trigger_and_marks_initial_event_triggered(self):
        parsed = ParsedSignal(
            signal_id="routed_metric_query",
            question="What is the current Henry Hub price?",
            metric="henry_hub_spot_price",
        )
        evaluation = SignalEvaluation(
            question=parsed.question,
            result=None,
            explanation="As of 2026-04-05, Henry Hub is 2.91.",
            values={"latest_value": 2.91, "latest_date": "2026-04-05"},
            as_of="2026-04-05",
            metric=parsed.metric,
        )

        with (
            patch("alerts.views.can_create_alert", return_value=(True, None)),
            patch("alerts.views.parse_signal_question", return_value=parsed),
            patch("alerts.views.build_signal_evaluator") as mock_build_evaluator,
        ):
            mock_build_evaluator.return_value.evaluate.return_value = evaluation
            response = self.client.post(
                reverse("alerts:create"),
                data={
                    "name": "Henry Hub answer monitor",
                    "question": parsed.question,
                    "frequency": "hourly",
                    "trigger_type": AlertTriggerType.EVERY_ANSWER,
                    "cooldown_hours": "0",
                    "action": "create",
                },
            )

        self.assertRedirects(response, reverse("alerts:list"))
        rule = AlertRule.objects.get(user=self.user)
        self.assertEqual(rule.trigger_type, AlertTriggerType.EVERY_ANSWER)
        self.assertIsNone(rule.last_result)
        self.assertEqual(rule.last_explanation, evaluation.explanation)
        self.assertTrue(rule.events.get().was_triggered)

    def test_scheduled_evaluation_sends_notification_for_answer_monitor_without_boolean_result(self):
        rule = AlertRule.objects.create(
            user=self.user,
            name="Storage answer monitor",
            question="How much gas is currently in storage?",
            signal_id="routed_metric_query",
            metric="working_gas_storage_lower48",
            config_json={"filters": {}, "route_intent": "latest_value"},
            trigger_type=AlertTriggerType.EVERY_ANSWER,
            cooldown_hours=0,
            delivery_channels=["email"],
        )
        evaluation = SignalEvaluation(
            question=rule.question,
            result=None,
            explanation="As of 2026-04-05, storage is 1,820.",
            values={"latest_value": 1820.0, "latest_date": "2026-04-05"},
            as_of="2026-04-05",
            metric=rule.metric,
        )

        with patch("alerts.tasks.build_signal_evaluator") as mock_build_evaluator:
            mock_build_evaluator.return_value.evaluate.return_value = evaluation
            result = evaluate_alert_rule_now(rule.id)

        rule.refresh_from_db()
        event = rule.events.get()
        self.assertTrue(result["was_triggered"])
        self.assertTrue(result["notification_sent"])
        self.assertTrue(event.was_triggered)
        self.assertTrue(event.notification_sent)
        self.assertEqual(rule.last_explanation, evaluation.explanation)
