from __future__ import annotations

import json

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from alerts.models import AlertDeliveryChannel, AlertEvent, AlertRule
from alerts.services import (
    build_metric_forecaster,
    build_signal_evaluator,
    is_builtin_signal_id,
    parsed_signal_from_rule,
    parsed_signal_from_signal_id,
    parse_signal_question,
    should_trigger_alert,
)


def _request_payload(request) -> dict:
    if request.content_type == "application/json":
        try:
            return json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return {}
    return request.POST.dict()


def _delivery_channels_from_request(request) -> list[str]:
    if request.content_type == "application/json":
        payload = _request_payload(request)
        value = payload.get("delivery_channels") or []
        return value if isinstance(value, list) else []
    return [item for item in request.POST.getlist("delivery_channels") if item]


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


UNSUPPORTED_ALERT_QUESTION_MESSAGE = (
    "This question could not be mapped to a supported Energy Atlas metric.\n"
    "Try a question the assistant already supports, such as:\n"
    "- What is the current Henry Hub price?\n"
    "- Are exports higher than last year?\n"
    "- Is production growing year over year?\n"
    "- How much gas is currently in storage?\n"
    "- Which sector consumes the most gas?"
)

ALERT_LIST_SORT_OPTIONS = {
    "created": {
        "label": "Created",
        "order_by": "-created_at",
    },
    "title": {
        "label": "Title",
        "order_by": "name",
    },
    "last_sent": {
        "label": "Last sent",
        "order_by": "-last_notified_at",
    },
}


def _create_rule_and_initial_event(*, user, payload: dict) -> tuple[AlertRule | None, dict | None, str | None]:
    question = (payload.get("question") or "").strip()
    if not question:
        return None, None, "question is required"

    parsed = parse_signal_question(question)
    if parsed is None:
        return None, None, UNSUPPORTED_ALERT_QUESTION_MESSAGE

    evaluator = build_signal_evaluator()
    evaluation = evaluator.evaluate(parsed)

    rule = AlertRule.objects.create(
        user=user,
        name=(payload.get("name") or question[:120]).strip() or question[:120],
        question=question,
        signal_id=parsed.signal_id,
        metric=parsed.metric,
        region=str(parsed.filters.get("region") or ""),
        config_json={
            "filters": parsed.filters,
            **parsed.config,
        },
        frequency=payload.get("frequency") or AlertRule._meta.get_field("frequency").default,
        trigger_type=payload.get("trigger_type") or AlertRule._meta.get_field("trigger_type").default,
        cooldown_hours=int(
            payload.get("cooldown_hours") or AlertRule._meta.get_field("cooldown_hours").default
        ),
        delivery_channels=payload.get("delivery_channels") or [],
    )

    was_triggered = should_trigger_alert(
        previous_result=rule.last_result,
        new_result=evaluation.result,
        trigger_type=rule.trigger_type,
    )
    AlertEvent.objects.create(
        alert_rule=rule,
        result=evaluation.result,
        explanation=evaluation.explanation,
        values_json=evaluation.values,
        error_code=evaluation.error_code or "",
        error_message="" if evaluation.error_code is None else evaluation.explanation,
        was_triggered=was_triggered,
    )

    rule.last_result = evaluation.result
    rule.last_evaluated_at = timezone.now()
    rule.last_explanation = evaluation.explanation
    rule.last_values_json = evaluation.values
    if was_triggered:
        rule.last_triggered_at = timezone.now()
    rule.save(
        update_fields=[
            "last_result",
            "last_evaluated_at",
            "last_explanation",
            "last_values_json",
            "last_triggered_at",
            "updated_at",
        ]
    )

    return rule, evaluation.to_dict(), None


def create_builtin_alert_rule(
    *,
    user,
    signal_id: str,
    title: str,
    delivery_channels: list[str] | None = None,
) -> tuple[AlertRule | None, dict | None, str | None]:
    if not is_builtin_signal_id(signal_id):
        return None, None, "unknown signal_id"

    parsed = parsed_signal_from_signal_id(signal_id)
    if parsed is None:
        return None, None, "unknown signal_id"

    payload = {
        "name": (title or parsed.question[:120]).strip() or parsed.question[:120],
        "question": parsed.question,
        "frequency": AlertRule._meta.get_field("frequency").default,
        "trigger_type": AlertRule._meta.get_field("trigger_type").default,
        "cooldown_hours": AlertRule._meta.get_field("cooldown_hours").default,
        "delivery_channels": delivery_channels or [AlertDeliveryChannel.EMAIL],
    }
    return _create_rule_and_initial_event(user=user, payload=payload)


@login_required
@require_http_methods(["GET"])
def alert_list_view(request):
    sort = str(request.GET.get("sort") or "created").strip().lower()
    sort_config = ALERT_LIST_SORT_OPTIONS.get(sort, ALERT_LIST_SORT_OPTIONS["created"])
    alert_rules = (
        AlertRule.objects.filter(user=request.user)
        .prefetch_related("events")
        .order_by(sort_config["order_by"], "-created_at")
    )
    return render(
        request,
        "alerts/list.html",
        {
            "alert_rules": alert_rules,
            "sort": sort if sort in ALERT_LIST_SORT_OPTIONS else "created",
            "sort_options": [
                {"value": value, "label": config["label"]}
                for value, config in ALERT_LIST_SORT_OPTIONS.items()
            ],
        },
    )


@login_required
@require_http_methods(["GET", "POST"])
def alert_create_view(request):
    evaluation: dict | None = None
    initial_name = ""
    initial_question = ""

    if request.method == "GET":
        signal_id = str(request.GET.get("signal_id") or "").strip()
        title = str(request.GET.get("title") or "").strip()
        if signal_id and is_builtin_signal_id(signal_id):
            parsed = parsed_signal_from_signal_id(signal_id)
            if parsed is not None:
                initial_question = parsed.question
                initial_name = title or parsed.question[:120]
        else:
            initial_name = str(request.GET.get("name") or "").strip()
            initial_question = str(request.GET.get("question") or "").strip()

    if request.method == "POST":
        action = request.POST.get("action", "create")
        payload = request.POST.dict()
        payload["delivery_channels"] = [AlertDeliveryChannel.EMAIL]
        if action == "test":
            question = (payload.get("question") or "").strip()
            if not question:
                messages.error(request, "question is required")
            else:
                evaluator = build_signal_evaluator()
                evaluation_obj = evaluator.evaluate_question(question)
                evaluation = evaluation_obj.to_dict()
                if evaluation_obj.error_code:
                    messages.error(request, evaluation_obj.explanation)
                else:
                    messages.success(request, "Alert test completed.")
        else:
            rule, evaluation, error = _create_rule_and_initial_event(
                user=request.user,
                payload=payload,
            )
            if error:
                messages.error(request, error)
            else:
                messages.success(request, "Alert created successfully.")
                return redirect("alerts:list")

    return render(
        request,
        "alerts/create.html",
        {
            "evaluation": evaluation,
            "initial_name": initial_name,
            "initial_question": initial_question,
            "frequency_choices": AlertRule._meta.get_field("frequency").choices,
            "trigger_type_choices": AlertRule._meta.get_field("trigger_type").choices,
        },
    )


@login_required
@require_http_methods(["GET", "POST"])
def alert_detail_view(request, alert_rule_id: int):
    alert_rule = get_object_or_404(
        AlertRule.objects.prefetch_related("events"),
        id=alert_rule_id,
        user=request.user,
    )

    if request.method == "POST":
        evaluator = build_signal_evaluator()
        evaluation = evaluator.evaluate(parsed_signal_from_rule(alert_rule))
        was_triggered = should_trigger_alert(
            previous_result=alert_rule.last_result,
            new_result=evaluation.result,
            trigger_type=alert_rule.trigger_type,
        )
        AlertEvent.objects.create(
            alert_rule=alert_rule,
            result=evaluation.result,
            explanation=evaluation.explanation,
            values_json=evaluation.values,
            error_code=evaluation.error_code or "",
            error_message="" if evaluation.error_code is None else evaluation.explanation,
            was_triggered=was_triggered,
        )
        alert_rule.last_result = evaluation.result
        alert_rule.last_evaluated_at = timezone.now()
        alert_rule.last_explanation = evaluation.explanation
        alert_rule.last_values_json = evaluation.values
        if was_triggered:
            alert_rule.last_triggered_at = timezone.now()
        alert_rule.save(
            update_fields=[
                "last_result",
                "last_evaluated_at",
                "last_explanation",
                "last_values_json",
                "last_triggered_at",
                "updated_at",
            ]
        )
        messages.success(request, "Alert evaluated successfully.")
        return redirect("alerts:detail", alert_rule_id=alert_rule.id)

    return render(
        request,
        "alerts/detail.html",
        {
            "alert_rule": alert_rule,
            "events": alert_rule.events.all()[:20],
        },
    )


@require_http_methods(["POST"])
def evaluate_signal_view(request):
    payload = _request_payload(request)
    question = (payload.get("question") or "").strip()
    if not question:
        return JsonResponse(
            {"error": "question is required"},
            status=400,
        )

    evaluator = build_signal_evaluator()
    evaluation = evaluator.evaluate_question(question)
    status_code = 200 if evaluation.error_code is None else 422
    return JsonResponse(evaluation.to_dict(), status=status_code)


@require_http_methods(["POST"])
def forecast_metric_view(request):
    payload = _request_payload(request)
    metric = str(payload.get("metric") or "").strip()
    if not metric:
        return JsonResponse({"error": "metric is required"}, status=400)

    try:
        horizon_days = int(payload.get("horizon_days") or 7)
        lookback_observations = int(payload.get("lookback_observations") or 30)
    except (TypeError, ValueError):
        return JsonResponse({"error": "invalid forecast parameters"}, status=400)

    filters = payload.get("filters") if isinstance(payload.get("filters"), dict) else {}
    include_overlay = _coerce_bool(payload.get("include_overlay"))
    forecaster = build_metric_forecaster()
    forecast = forecaster.forecast_metric(
        metric,
        start=payload.get("start"),
        end=payload.get("end"),
        filters=filters,
        horizon_days=horizon_days,
        lookback_observations=lookback_observations,
        include_overlay=include_overlay,
    )
    status_code = 200 if forecast.error_code is None else 422
    return JsonResponse(forecast.to_dict(), status=status_code)


@require_http_methods(["POST"])
def create_alert_rule_view(request):
    if not request.user.is_authenticated:
        return JsonResponse({"error": "authentication required"}, status=401)

    payload = _request_payload(request)
    payload["delivery_channels"] = _delivery_channels_from_request(request)
    rule, evaluation, error = _create_rule_and_initial_event(
        user=request.user,
        payload=payload,
    )
    if error:
        status = 400 if error == "question is required" else 422
        return JsonResponse({"error": error}, status=status)

    return JsonResponse(
        {
            "alert_rule_id": rule.id,
            "signal_id": rule.signal_id,
            "evaluation": evaluation,
        },
        status=201,
    )
