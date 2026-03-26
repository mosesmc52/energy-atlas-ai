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
    build_signal_evaluator,
    parsed_signal_from_rule,
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


def _create_rule_and_initial_event(*, user, payload: dict) -> tuple[AlertRule | None, dict | None, str | None]:
    question = (payload.get("question") or "").strip()
    if not question:
        return None, None, "question is required"

    parsed = parse_signal_question(question)
    if parsed is None:
        return None, None, "unsupported alert question"

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


@login_required
@require_http_methods(["GET"])
def alert_list_view(request):
    alert_rules = (
        AlertRule.objects.filter(user=request.user)
        .prefetch_related("events")
        .order_by("-created_at")
    )
    return render(
        request,
        "alerts/list.html",
        {
            "alert_rules": alert_rules,
        },
    )


@login_required
@require_http_methods(["GET", "POST"])
def alert_sandbox_view(request):
    evaluation: dict | None = None
    selected_delivery_channels: list[str] = []

    if request.method == "POST":
        action = request.POST.get("action", "test")
        payload = request.POST.dict()
        payload["delivery_channels"] = _delivery_channels_from_request(request)
        selected_delivery_channels = list(payload["delivery_channels"])
        question = (payload.get("question") or "").strip()

        if action == "save":
            rule, evaluation, error = _create_rule_and_initial_event(
                user=request.user,
                payload=payload,
            )
            if error:
                messages.error(request, error)
            else:
                messages.success(request, "Alert created successfully.")
                return redirect("alerts:detail", alert_rule_id=rule.id)
        else:
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

    return render(
        request,
        "alerts/sandbox.html",
        {
            "evaluation": evaluation,
            "frequency_choices": AlertRule._meta.get_field("frequency").choices,
            "trigger_type_choices": AlertRule._meta.get_field("trigger_type").choices,
            "delivery_channel_choices": AlertDeliveryChannel.choices,
            "selected_delivery_channels": selected_delivery_channels,
        },
    )


@login_required
@require_http_methods(["GET", "POST"])
def alert_create_view(request):
    selected_delivery_channels: list[str] = []
    if request.method == "POST":
        payload = request.POST.dict()
        payload["delivery_channels"] = _delivery_channels_from_request(request)
        selected_delivery_channels = list(payload["delivery_channels"])
        rule, evaluation, error = _create_rule_and_initial_event(
            user=request.user,
            payload=payload,
        )
        if error:
            messages.error(request, error)
        else:
            messages.success(request, "Alert created successfully.")
            return redirect("alerts:detail", alert_rule_id=rule.id)

    return render(
        request,
        "alerts/create.html",
        {
            "frequency_choices": AlertRule._meta.get_field("frequency").choices,
            "trigger_type_choices": AlertRule._meta.get_field("trigger_type").choices,
            "delivery_channel_choices": AlertDeliveryChannel.choices,
            "selected_delivery_channels": selected_delivery_channels,
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
