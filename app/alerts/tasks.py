from __future__ import annotations

from datetime import timedelta

try:
    from celery import shared_task

    CELERY_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover
    CELERY_AVAILABLE = False

    def shared_task(*_args, **_kwargs):  # type: ignore[misc]
        def decorator(func):
            return func

        return decorator

from django.db import transaction
from django.utils import timezone

from alerts.models import AlertEvent, AlertFrequency, AlertRule
from alerts.services import (
    build_signal_evaluator,
    should_trigger_alert,
)


def _frequency_interval(frequency: str) -> timedelta:
    mapping = {
        AlertFrequency.HOURLY: timedelta(hours=1),
        AlertFrequency.DAILY: timedelta(days=1),
        AlertFrequency.WEEKLY: timedelta(days=7),
    }
    return mapping.get(frequency, timedelta(days=1))


def _is_due(rule: AlertRule, now) -> bool:
    if not rule.is_enabled:
        return False
    if rule.last_evaluated_at is None:
        return True
    return now - rule.last_evaluated_at >= _frequency_interval(rule.frequency)


def evaluate_alert_rule_now(alert_rule_id: int) -> dict:
    evaluator = build_signal_evaluator()
    now = timezone.now()

    with transaction.atomic():
        rule = (
            AlertRule.objects.select_for_update()
            .select_related("user")
            .get(id=alert_rule_id)
        )

        evaluation = evaluator.evaluate_rule(rule)
        was_triggered = should_trigger_alert(
            previous_result=rule.last_condition_result,
            new_result=evaluation.result,
            trigger_type=rule.trigger_type,
            error_code=evaluation.error_code,
        )
        notification_sent = False

        if was_triggered and not rule.in_cooldown(now=now):
            notification_sent = True

        event = AlertEvent.objects.create(
            alert_rule=rule,
            evaluated_at=now,
            result=evaluation.result,
            explanation=evaluation.explanation,
            values_json=evaluation.values,
            error_code=evaluation.error_code or "",
            error_message="" if evaluation.error_code is None else evaluation.explanation,
            was_triggered=was_triggered,
            notification_sent=notification_sent,
            notification_sent_at=now if notification_sent else None,
        )

        rule.last_result = evaluation.result
        rule.last_raw_value = evaluation.values.get("raw_value")
        rule.last_evaluated_value = evaluation.values.get("evaluated_value")
        rule.last_condition_result = evaluation.values.get("condition_result")
        rule.last_evaluated_at = now
        rule.last_explanation = evaluation.explanation
        rule.last_values_json = evaluation.values
        if was_triggered:
            rule.last_triggered_at = now
        if notification_sent:
            rule.last_notified_at = now
        rule.save(
            update_fields=[
                "last_result",
                "last_raw_value",
                "last_evaluated_value",
                "last_condition_result",
                "last_evaluated_at",
                "last_explanation",
                "last_values_json",
                "last_triggered_at",
                "last_notified_at",
                "updated_at",
            ]
        )

    return {
        "alert_rule_id": rule.id,
        "alert_event_id": event.id,
        "signal_id": rule.signal_id,
        "result": evaluation.result,
        "was_triggered": was_triggered,
        "notification_sent": notification_sent,
        "error_code": evaluation.error_code,
    }


def get_due_alert_rule_ids(now=None) -> list[int]:
    now = now or timezone.now()
    return [rule.id for rule in AlertRule.objects.select_related("user") if _is_due(rule, now)]


def evaluate_due_alert_rules_now() -> dict:
    due_rule_ids = get_due_alert_rule_ids()
    results = [evaluate_alert_rule_now(rule_id) for rule_id in due_rule_ids]
    return {
        "evaluated_count": len(results),
        "results": results,
    }


@shared_task(name="alerts.evaluate_alert_rule")
def evaluate_alert_rule(alert_rule_id: int) -> dict:
    return evaluate_alert_rule_now(alert_rule_id)


@shared_task(name="alerts.evaluate_due_alert_rules")
def evaluate_due_alert_rules() -> dict:
    now = timezone.now()
    due_rule_ids = get_due_alert_rule_ids(now=now)

    evaluated = []
    for rule_id in due_rule_ids:
        if not CELERY_AVAILABLE:
            raise RuntimeError(
                "Celery is not installed in the current environment. "
                "Install dependencies with `poetry install` before using async alert tasks."
            )
        evaluated.append(evaluate_alert_rule.delay(rule_id).id)

    return {
        "queued_count": len(due_rule_ids),
        "task_ids": evaluated,
    }
