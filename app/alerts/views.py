from __future__ import annotations

import json

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from alerts.models import (
    AlertDeliveryChannel,
    AlertEvent,
    AlertRule,
    AlertStatus,
    AlertTriggerType,
    SharedAnswer,
)
from alerts.services import (
    build_metric_forecaster,
    build_signal_evaluator,
    is_builtin_signal_id,
    parsed_signal_from_rule,
    parsed_signal_from_signal_id,
    parse_signal_question,
    should_trigger_alert,
)
from billing.services import can_create_alert
from schemas.answer import StructuredAnswer


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


def _shared_answer_context(shared_answer: SharedAnswer) -> dict:
    response = (
        shared_answer.response_json if isinstance(shared_answer.response_json, dict) else {}
    )
    signal = response.get("signal") if isinstance(response.get("signal"), dict) else {}
    status = str(signal.get("status") or "").lower()
    signal_labels = {
        "bullish": "Bullish",
        "bearish": "Bearish",
        "neutral": "Neutral",
    }
    signal_styles = {
        "bullish": "bg-emerald-100 text-emerald-700 ring-emerald-200",
        "bearish": "bg-rose-100 text-rose-700 ring-rose-200",
        "neutral": "bg-amber-100 text-amber-700 ring-amber-200",
    }
    try:
        confidence_pct = round(float(signal.get("confidence") or 0) * 100)
    except (TypeError, ValueError):
        confidence_pct = 0

    drivers = [str(item).strip() for item in (response.get("drivers") or []) if str(item).strip()]
    sources = []
    for source in response.get("sources") or []:
        if not isinstance(source, dict):
            continue
        title = str(source.get("title") or "").strip()
        if not title:
            continue
        sources.append(
            {
                "title": title,
                "date": str(source.get("date") or "").strip(),
            }
        )

    datapoints = []
    for datapoint in response.get("data_points") or []:
        if not isinstance(datapoint, dict):
            continue
        metric = str(datapoint.get("metric") or "").strip()
        value = datapoint.get("value")
        unit = str(datapoint.get("unit") or "").strip()
        if not metric or value in (None, ""):
            continue
        datapoints.append(
            {
                "metric": metric.replace("_", " ").title(),
                "value": value,
                "unit": unit,
            }
        )

    forecast = response.get("forecast") if isinstance(response.get("forecast"), dict) else {}
    return {
        "shared_answer": shared_answer,
        "signal_label": signal_labels.get(status, "Insight"),
        "signal_style": signal_styles.get(status, "bg-slate-100 text-slate-700 ring-slate-200"),
        "confidence_pct": confidence_pct,
        "summary": str(response.get("summary") or response.get("answer") or "").strip(),
        "drivers": drivers,
        "forecast_direction": str(forecast.get("direction") or "").strip(),
        "forecast_reasoning": str(forecast.get("reasoning") or "").strip(),
        "sources": sources,
        "datapoints": datapoints,
    }


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


def _alert_form_context(
    *,
    evaluation: dict | None,
    initial_name: str,
    initial_question: str,
    initial_frequency: str,
    initial_trigger_type: str,
    initial_cooldown_hours: int,
    action_url: str,
    submit_label: str,
    eyebrow: str,
    title: str,
    description: str,
    back_url: str,
    form_id: str,
):
    return {
        "evaluation": evaluation,
        "initial_name": initial_name,
        "initial_question": initial_question,
        "initial_frequency": initial_frequency,
        "initial_trigger_type": initial_trigger_type,
        "initial_cooldown_hours": initial_cooldown_hours,
        "frequency_choices": AlertRule._meta.get_field("frequency").choices,
        "trigger_type_choices": AlertRule._meta.get_field("trigger_type").choices,
        "action_url": action_url,
        "submit_label": submit_label,
        "eyebrow": eyebrow,
        "page_title": title,
        "page_description": description,
        "back_url": back_url,
        "form_id": form_id,
        "answer_trigger_type_value": AlertTriggerType.EVERY_ANSWER,
    }


def _create_rule_and_initial_event(*, user, payload: dict) -> tuple[AlertRule | None, dict | None, str | None]:
    question = (payload.get("question") or "").strip()
    if not question:
        return None, None, "question is required"

    current_active_alert_count = AlertRule.objects.filter(
        user=user,
        is_active=True,
    ).count()
    is_allowed, limit_error = can_create_alert(
        user=user,
        current_active_alert_count=current_active_alert_count,
    )
    if not is_allowed:
        return None, None, limit_error

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
        error_code=evaluation.error_code,
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


def _update_rule_from_payload(*, alert_rule: AlertRule, payload: dict) -> tuple[dict | None, str | None]:
    question = (payload.get("question") or "").strip()
    if not question:
        return None, "question is required"

    parsed = parse_signal_question(question)
    if parsed is None:
        return None, UNSUPPORTED_ALERT_QUESTION_MESSAGE

    evaluator = build_signal_evaluator()
    evaluation = evaluator.evaluate(parsed)

    alert_rule.name = (payload.get("name") or question[:120]).strip() or question[:120]
    alert_rule.question = question
    alert_rule.signal_id = parsed.signal_id
    alert_rule.metric = parsed.metric
    alert_rule.region = str(parsed.filters.get("region") or "")
    alert_rule.config_json = {
        "filters": parsed.filters,
        **parsed.config,
    }
    alert_rule.frequency = payload.get("frequency") or alert_rule.frequency
    alert_rule.trigger_type = payload.get("trigger_type") or alert_rule.trigger_type
    alert_rule.cooldown_hours = int(
        payload.get("cooldown_hours") or alert_rule.cooldown_hours
    )
    alert_rule.delivery_channels = payload.get("delivery_channels") or alert_rule.delivery_channels
    alert_rule.save(
        update_fields=[
            "name",
            "question",
            "signal_id",
            "metric",
            "region",
            "config_json",
            "frequency",
            "trigger_type",
            "cooldown_hours",
            "delivery_channels",
            "updated_at",
        ]
    )
    return evaluation.to_dict(), None


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
    checkout_status = str(request.GET.get("checkout") or "").strip().lower()
    if checkout_status in {"success", "cancel"}:
        if checkout_status == "success":
            messages.success(request, "Checkout completed successfully.")
        else:
            messages.warning(request, "Checkout was canceled.")
        redirect_query = request.GET.copy()
        redirect_query.pop("checkout", None)
        redirect_url = reverse("alerts:list")
        if redirect_query:
            redirect_url = f"{redirect_url}?{redirect_query.urlencode()}"
        return redirect(redirect_url)

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
        _alert_form_context(
            evaluation=evaluation,
            initial_name=initial_name,
            initial_question=initial_question,
            initial_frequency=request.POST.get("frequency") or AlertRule._meta.get_field("frequency").default,
            initial_trigger_type=request.POST.get("trigger_type") or AlertRule._meta.get_field("trigger_type").default,
            initial_cooldown_hours=int(request.POST.get("cooldown_hours") or AlertRule._meta.get_field("cooldown_hours").default),
            action_url=reverse("alerts:create"),
            submit_label="Create alert",
            eyebrow="Create",
            title="Define a new alert rule",
            description="Write the question, choose the trigger behavior, test the result, then save the rule when the explanation looks right.",
            back_url=reverse("alerts:list"),
            form_id="alert-create-form",
        ),
    )


@login_required
@require_http_methods(["GET", "POST"])
def alert_edit_view(request, alert_rule_id: int):
    alert_rule = get_object_or_404(
        AlertRule,
        id=alert_rule_id,
        user=request.user,
    )
    evaluation: dict | None = None

    if request.method == "POST":
        action = request.POST.get("action", "save")
        payload = request.POST.dict()
        payload["delivery_channels"] = alert_rule.delivery_channels or [AlertDeliveryChannel.EMAIL]

        question = (payload.get("question") or "").strip()
        if action == "test":
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
            evaluation, error = _update_rule_from_payload(
                alert_rule=alert_rule,
                payload=payload,
            )
            if error:
                messages.error(request, error)
            else:
                messages.success(request, "Alert updated successfully.")
                return redirect("alerts:detail", alert_rule_id=alert_rule.id)

    return render(
        request,
        "alerts/edit.html",
        _alert_form_context(
            evaluation=evaluation,
            initial_name=request.POST.get("name") or alert_rule.name,
            initial_question=request.POST.get("question") or alert_rule.question,
            initial_frequency=request.POST.get("frequency") or alert_rule.frequency,
            initial_trigger_type=request.POST.get("trigger_type") or alert_rule.trigger_type,
            initial_cooldown_hours=int(request.POST.get("cooldown_hours") or alert_rule.cooldown_hours),
            action_url=reverse("alerts:edit", args=[alert_rule.id]),
            submit_label="Save changes",
            eyebrow="Edit",
            title="Update alert rule",
            description="Adjust the question or alert settings, test the result, then save the updated rule.",
            back_url=reverse("alerts:detail", args=[alert_rule.id]),
            form_id="alert-edit-form",
        ),
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
            error_code=evaluation.error_code,
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
            "answer_trigger_type_value": AlertTriggerType.EVERY_ANSWER,
        },
    )


@login_required
@require_http_methods(["POST"])
def alert_toggle_view(request, alert_rule_id: int):
    alert_rule = get_object_or_404(
        AlertRule,
        id=alert_rule_id,
        user=request.user,
    )

    if alert_rule.is_enabled:
        alert_rule.is_active = False
        alert_rule.status = AlertStatus.DISABLED
        messages.success(request, "Alert disabled.")
    else:
        alert_rule.is_active = True
        alert_rule.status = AlertStatus.ACTIVE
        messages.success(request, "Alert enabled.")

    alert_rule.save(update_fields=["is_active", "status", "updated_at"])
    return redirect("alerts:detail", alert_rule_id=alert_rule.id)


@login_required
@require_http_methods(["POST"])
def alert_delete_view(request, alert_rule_id: int):
    alert_rule = get_object_or_404(
        AlertRule,
        id=alert_rule_id,
        user=request.user,
    )
    alert_name = alert_rule.name
    alert_rule.delete()
    messages.success(request, f'Alert "{alert_name}" deleted.')
    return redirect("alerts:list")


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


@csrf_exempt
@require_http_methods(["POST"])
def create_shared_answer_view(request):
    payload = _request_payload(request)
    question = str(payload.get("question") or "").strip()
    response_json = payload.get("response_json")

    if not question:
        return JsonResponse({"error": "question is required"}, status=400)
    if not isinstance(response_json, dict):
        return JsonResponse({"error": "response_json must be an object"}, status=400)

    try:
        structured_response = StructuredAnswer.model_validate(response_json)
    except Exception:
        return JsonResponse({"error": "response_json is invalid"}, status=400)

    shared_answer = SharedAnswer.objects.create(
        question=question,
        response_json=structured_response.model_dump(mode="json"),
    )
    share_path = reverse("shared-answer-detail", args=[shared_answer.share_id])
    app_url = str(getattr(settings, "APP_URL", "") or "").strip().rstrip("/")
    share_url = f"{app_url}{share_path}" if app_url else request.build_absolute_uri(share_path)
    return JsonResponse(
        {
            "share_id": shared_answer.share_id,
            "url": share_url,
            "path": share_path,
        },
        status=201,
    )


@require_http_methods(["GET"])
def shared_answer_detail_view(request, share_id: str):
    shared_answer = get_object_or_404(SharedAnswer, share_id=share_id)
    return render(
        request,
        "shared/detail.html",
        _shared_answer_context(shared_answer),
    )
