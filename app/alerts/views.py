from __future__ import annotations

import json
import logging
from types import SimpleNamespace

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
    AlertOperator,
    AlertRule,
    AlertStatus,
    AlertTriggerType,
    AlertValueMode,
    SharedAnswer,
)
from alerts.services import (
    build_metric_forecaster,
    build_signal_evaluator,
    get_metric_registry,
    is_builtin_signal_id,
    parsed_signal_from_signal_id,
    should_trigger_alert,
)
from alerts.schemas import AlertRulePayload
from billing.services import can_create_alert
from main.analytics import queue_analytics_event
from schemas.answer import StructuredAnswer

logger = logging.getLogger(__name__)

US_STATE_OPTIONS = [
    ("united_states_total", "National (United States)"),
    ("al", "Alabama"),
    ("ak", "Alaska"),
    ("az", "Arizona"),
    ("ar", "Arkansas"),
    ("ca", "California"),
    ("co", "Colorado"),
    ("ct", "Connecticut"),
    ("de", "Delaware"),
    ("fl", "Florida"),
    ("ga", "Georgia"),
    ("hi", "Hawaii"),
    ("id", "Idaho"),
    ("il", "Illinois"),
    ("in", "Indiana"),
    ("ia", "Iowa"),
    ("ks", "Kansas"),
    ("ky", "Kentucky"),
    ("la", "Louisiana"),
    ("me", "Maine"),
    ("md", "Maryland"),
    ("ma", "Massachusetts"),
    ("mi", "Michigan"),
    ("mn", "Minnesota"),
    ("ms", "Mississippi"),
    ("mo", "Missouri"),
    ("mt", "Montana"),
    ("ne", "Nebraska"),
    ("nv", "Nevada"),
    ("nh", "New Hampshire"),
    ("nj", "New Jersey"),
    ("nm", "New Mexico"),
    ("ny", "New York"),
    ("nc", "North Carolina"),
    ("nd", "North Dakota"),
    ("oh", "Ohio"),
    ("ok", "Oklahoma"),
    ("or", "Oregon"),
    ("pa", "Pennsylvania"),
    ("ri", "Rhode Island"),
    ("sc", "South Carolina"),
    ("sd", "South Dakota"),
    ("tn", "Tennessee"),
    ("tx", "Texas"),
    ("ut", "Utah"),
    ("vt", "Vermont"),
    ("va", "Virginia"),
    ("wa", "Washington"),
    ("wv", "West Virginia"),
    ("wi", "Wisconsin"),
    ("wy", "Wyoming"),
]


def _request_payload(request) -> dict:
    if str(request.content_type or "").startswith("application/json"):
        try:
            return json.loads(request.body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return {}
    return request.POST.dict()


def _delivery_channels_from_request(request) -> list[str]:
    if str(request.content_type or "").startswith("application/json"):
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
    initial_metric: str,
    initial_value_mode: str,
    initial_operator: str,
    initial_threshold: str,
    initial_geography_type: str,
    initial_state_code: str,
    initial_country_code: str,
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
        "initial_metric": initial_metric,
        "initial_value_mode": initial_value_mode,
        "initial_operator": initial_operator,
        "initial_threshold": initial_threshold,
        "initial_geography_type": initial_geography_type,
        "initial_state_code": initial_state_code,
        "initial_country_code": initial_country_code,
        "initial_frequency": initial_frequency,
        "initial_trigger_type": initial_trigger_type,
        "initial_cooldown_hours": initial_cooldown_hours,
        "frequency_choices": AlertRule._meta.get_field("frequency").choices,
        "trigger_type_choices": AlertRule._meta.get_field("trigger_type").choices,
        "value_mode_choices": AlertRule._meta.get_field("value_mode").choices,
        "operator_choices": AlertRule._meta.get_field("operator").choices,
        "metric_choices": [
            {
                "value": metric_id,
                "label": str(config.get("label") or metric_id.replace("_", " ").title()),
                "zscore_supported": bool(config.get("zscore_supported", False)),
                "geography": str(config.get("geography") or "none"),
            }
            for metric_id, config in get_metric_registry().items()
        ],
        "state_options": US_STATE_OPTIONS,
        "action_url": action_url,
        "submit_label": submit_label,
        "eyebrow": eyebrow,
        "page_title": title,
        "page_description": description,
        "back_url": back_url,
        "form_id": form_id,
        "answer_trigger_type_value": AlertTriggerType.RETURN_ANSWER,
    }


def _validate_rule_payload(payload: dict) -> tuple[dict | None, str | None]:
    try:
        validated = AlertRulePayload.model_validate(payload)
    except Exception as exc:  # noqa: BLE001
        if hasattr(exc, "errors"):
            for err in exc.errors():  # type: ignore[attr-defined]
                loc = err.get("loc") or []
                field = str(loc[-1]) if loc else ""
                err_type = str(err.get("type") or "")
                if err_type == "missing" and field:
                    return None, f"{field} is required"
                if field == "threshold":
                    return None, "threshold must be a numeric value"
                if field == "cooldown_hours":
                    if err_type == "greater_than_equal":
                        return None, "cooldown_hours must be greater than or equal to 0"
                    return None, "cooldown_hours must be an integer"
        return None, "invalid alert payload"

    metric = validated.metric.strip()
    metric_registry = get_metric_registry()
    if metric not in metric_registry:
        return None, f"metric '{metric}' is not supported"
    metric_config = metric_registry[metric]
    resolved_metric = str(metric_config.get("target_metric") or metric).strip()

    value_mode = validated.value_mode.strip()
    valid_value_modes = {choice[0] for choice in AlertValueMode.choices}
    if value_mode not in valid_value_modes:
        return None, f"value_mode '{value_mode}' is not supported"

    if value_mode == AlertValueMode.ZSCORE and not bool(metric_config.get("zscore_supported", False)):
        return None, f"zscore mode is not supported for metric '{metric}'"

    operator = validated.operator.strip()
    valid_operators = {choice[0] for choice in AlertOperator.choices}
    if operator not in valid_operators:
        return None, f"operator '{operator}' is not supported"

    valid_frequencies = {choice[0] for choice in AlertRule._meta.get_field("frequency").choices}
    frequency = validated.frequency.strip()
    if frequency not in valid_frequencies:
        return None, f"frequency '{frequency}' is not supported"

    valid_trigger_types = {choice[0] for choice in AlertTriggerType.choices}
    trigger_type = validated.trigger_type.strip()
    if trigger_type not in valid_trigger_types:
        return None, f"trigger_type '{trigger_type}' is not supported"

    threshold = float(validated.threshold)
    cooldown_hours = int(validated.cooldown_hours)

    geography_mode = str(metric_config.get("geography") or "none")
    geography_type = str(validated.geography_type or "").strip().lower()
    state_code = str(validated.state_code or "").strip().lower()
    country_code = str(validated.country_code or "").strip()
    region = ""

    if geography_mode == "country_only":
        if not country_code:
            return None, "country_code is required"
        region = country_code
        geography_type = "country"
    elif geography_mode == "state_or_national":
        if geography_type not in {"national", "state"}:
            geography_type = "national"
        if geography_type == "state":
            if not state_code:
                return None, "state_code is required"
            region = state_code
        else:
            region = "united_states_total"
    elif geography_mode == "national_only":
        geography_type = "national"
        region = "lower48" if resolved_metric == "working_gas_storage_lower48" else "united_states_total"

    return (
        {
            "name": validated.name.strip(),
            "question": validated.question.strip(),
            "metric": metric,
            "resolved_metric": resolved_metric,
            "value_mode": value_mode,
            "operator": operator,
            "threshold": threshold,
            "frequency": frequency,
            "trigger_type": trigger_type,
            "cooldown_hours": cooldown_hours,
            "delivery_channels": payload.get("delivery_channels") or [],
            "region": region,
            "geography_type": geography_type,
            "state_code": state_code,
            "country_code": country_code,
        },
        None,
    )


def _create_rule_and_initial_event(*, user, payload: dict) -> tuple[AlertRule | None, dict | None, str | None]:
    normalized_payload, error = _validate_rule_payload(payload)
    if error:
        return None, None, error
    assert normalized_payload is not None

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

    evaluator = build_signal_evaluator()
    rule = AlertRule.objects.create(
        user=user,
        name=normalized_payload["name"],
        question=normalized_payload["question"],
        signal_id="structured_condition",
        metric=normalized_payload["metric"],
        value_mode=normalized_payload["value_mode"],
        operator=normalized_payload["operator"],
        threshold=normalized_payload["threshold"],
        region=normalized_payload["region"],
        config_json={
            "geography_type": normalized_payload["geography_type"],
            "state_code": normalized_payload["state_code"],
            "country_code": normalized_payload["country_code"],
            "resolved_metric": normalized_payload["resolved_metric"],
        },
        frequency=normalized_payload["frequency"],
        trigger_type=normalized_payload["trigger_type"],
        cooldown_hours=normalized_payload["cooldown_hours"],
        delivery_channels=normalized_payload["delivery_channels"],
    )
    evaluation = evaluator.evaluate_rule(rule)

    was_triggered = should_trigger_alert(
        previous_result=rule.last_condition_result,
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
    rule.last_raw_value = evaluation.values.get("raw_value")
    rule.last_evaluated_value = evaluation.values.get("evaluated_value")
    rule.last_condition_result = evaluation.values.get("condition_result")
    rule.last_evaluated_at = timezone.now()
    rule.last_explanation = evaluation.explanation
    rule.last_values_json = evaluation.values
    if was_triggered:
        rule.last_triggered_at = timezone.now()
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
            "updated_at",
        ]
    )

    return rule, evaluation.to_dict(), None


def _evaluate_payload(payload: dict) -> tuple[dict | None, str | None]:
    normalized_payload, error = _validate_rule_payload(payload)
    if error:
        return None, error
    assert normalized_payload is not None

    evaluator = build_signal_evaluator()
    evaluation = evaluator.evaluate_rule(SimpleNamespace(**normalized_payload))
    return evaluation.to_dict(), None


def _update_rule_from_payload(*, alert_rule: AlertRule, payload: dict) -> tuple[dict | None, str | None]:
    normalized_payload, error = _validate_rule_payload(payload)
    if error:
        return None, error
    assert normalized_payload is not None

    evaluator = build_signal_evaluator()
    alert_rule.name = normalized_payload["name"]
    alert_rule.question = normalized_payload["question"]
    alert_rule.signal_id = "structured_condition"
    alert_rule.metric = normalized_payload["metric"]
    alert_rule.value_mode = normalized_payload["value_mode"]
    alert_rule.operator = normalized_payload["operator"]
    alert_rule.threshold = normalized_payload["threshold"]
    alert_rule.region = normalized_payload["region"]
    alert_rule.config_json = {
        "geography_type": normalized_payload["geography_type"],
        "state_code": normalized_payload["state_code"],
        "country_code": normalized_payload["country_code"],
        "resolved_metric": normalized_payload["resolved_metric"],
    }
    alert_rule.frequency = normalized_payload["frequency"]
    alert_rule.trigger_type = normalized_payload["trigger_type"]
    alert_rule.cooldown_hours = normalized_payload["cooldown_hours"]
    alert_rule.delivery_channels = normalized_payload["delivery_channels"]
    evaluation = evaluator.evaluate_rule(alert_rule)

    alert_rule.save(
        update_fields=[
            "name",
            "question",
            "signal_id",
            "metric",
            "value_mode",
            "operator",
            "threshold",
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
        "metric": parsed.metric,
        "value_mode": AlertValueMode.RAW,
        "operator": AlertOperator.GTE,
        "threshold": 0,
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
            checkout_analytics = request.session.pop("pending_checkout_analytics", {})
            queue_analytics_event(
                request,
                "subscription_completed",
                plan=checkout_analytics.get("plan", ""),
                billing_interval=checkout_analytics.get("billing_interval", ""),
                value=checkout_analytics.get("value", ""),
                currency=checkout_analytics.get("currency", ""),
                app_surface="django",
            )
            messages.success(request, "Checkout completed successfully.")
        else:
            request.session.pop("pending_checkout_analytics", None)
            messages.warning(request, "Checkout was canceled.")
        redirect_query = request.GET.copy()
        redirect_query.pop("checkout", None)
        redirect_url = reverse("alerts:list")
        if redirect_query:
            redirect_url = f"{redirect_url}?{redirect_query.urlencode()}"
        return redirect(redirect_url)

    sort = str(request.GET.get("sort") or "title").strip().lower()
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
    initial_metric = ""
    initial_value_mode = AlertValueMode.RAW
    initial_operator = AlertOperator.GTE
    initial_threshold = "0"
    initial_geography_type = "national"
    initial_state_code = "united_states_total"
    initial_country_code = ""

    if request.method == "GET":
        signal_id = str(request.GET.get("signal_id") or "").strip()
        title = str(request.GET.get("title") or "").strip()
        if signal_id and is_builtin_signal_id(signal_id):
            parsed = parsed_signal_from_signal_id(signal_id)
            if parsed is not None:
                initial_question = parsed.question
                initial_name = title or parsed.question[:120]
                initial_metric = parsed.metric
                if "threshold" in parsed.config:
                    initial_threshold = str(parsed.config.get("threshold"))
        else:
            initial_name = str(request.GET.get("name") or "").strip()
            initial_question = str(request.GET.get("question") or "").strip()
            initial_metric = str(request.GET.get("metric") or "").strip()
            initial_geography_type = str(request.GET.get("geography_type") or "national").strip()
            initial_state_code = str(request.GET.get("state_code") or "united_states_total").strip()
            initial_country_code = str(request.GET.get("country_code") or "").strip()

    if request.method == "POST":
        action = request.POST.get("action", "create")
        payload = request.POST.dict()
        payload["delivery_channels"] = [AlertDeliveryChannel.EMAIL]
        if action == "test":
            evaluation, error = _evaluate_payload(payload)
            if error:
                messages.error(request, error)
            else:
                if evaluation.get("error_code"):
                    messages.error(request, str(evaluation.get("explanation") or "Alert test failed."))
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
                queue_analytics_event(
                    request,
                    "alert_created",
                    signal_id=rule.signal_id,
                    source="manual",
                    app_surface="django",
                )
                messages.success(request, "Alert created successfully.")
                return redirect("alerts:list")

    return render(
        request,
        "alerts/create.html",
        _alert_form_context(
            evaluation=evaluation,
            initial_name=initial_name,
            initial_question=initial_question,
            initial_metric=initial_metric,
            initial_value_mode=request.POST.get("value_mode") or initial_value_mode,
            initial_operator=request.POST.get("operator") or initial_operator,
            initial_threshold=request.POST.get("threshold") or initial_threshold,
            initial_geography_type=request.POST.get("geography_type") or initial_geography_type,
            initial_state_code=request.POST.get("state_code") or initial_state_code,
            initial_country_code=request.POST.get("country_code") or initial_country_code,
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

        if action == "test":
            evaluation, error = _evaluate_payload(payload)
            if error:
                messages.error(request, error)
            else:
                if evaluation.get("error_code"):
                    messages.error(request, str(evaluation.get("explanation") or "Alert test failed."))
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
            initial_metric=request.POST.get("metric") or alert_rule.metric,
            initial_value_mode=request.POST.get("value_mode") or alert_rule.value_mode,
            initial_operator=request.POST.get("operator") or alert_rule.operator,
            initial_threshold=request.POST.get("threshold") or str(alert_rule.threshold),
            initial_geography_type=request.POST.get("geography_type") or str((alert_rule.config_json or {}).get("geography_type") or "national"),
            initial_state_code=request.POST.get("state_code") or str((alert_rule.config_json or {}).get("state_code") or "united_states_total"),
            initial_country_code=request.POST.get("country_code") or str((alert_rule.config_json or {}).get("country_code") or ""),
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
        evaluation = evaluator.evaluate_rule(alert_rule)
        was_triggered = should_trigger_alert(
            previous_result=alert_rule.last_condition_result,
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
        alert_rule.last_raw_value = evaluation.values.get("raw_value")
        alert_rule.last_evaluated_value = evaluation.values.get("evaluated_value")
        alert_rule.last_condition_result = evaluation.values.get("condition_result")
        alert_rule.last_evaluated_at = timezone.now()
        alert_rule.last_explanation = evaluation.explanation
        alert_rule.last_values_json = evaluation.values
        if was_triggered:
            alert_rule.last_triggered_at = timezone.now()
        alert_rule.save(
            update_fields=[
                "last_result",
                "last_raw_value",
                "last_evaluated_value",
                "last_condition_result",
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
            "answer_trigger_type_value": AlertTriggerType.RETURN_ANSWER,
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
        status = 400 if ("is required" in error or error == "invalid alert payload") else 422
        return JsonResponse({"error": error}, status=status)

    return JsonResponse(
        {
            "alert_rule_id": rule.id,
            "signal_id": rule.signal_id,
            "metric": rule.metric,
            "value_mode": rule.value_mode,
            "operator": rule.operator,
            "threshold": rule.threshold,
            "region": rule.region,
            "evaluation": evaluation,
        },
        status=201,
    )


@require_http_methods(["GET", "PUT"])
def alert_rule_api_view(request, alert_rule_id: int):
    if not request.user.is_authenticated:
        return JsonResponse({"error": "authentication required"}, status=401)

    rule = get_object_or_404(AlertRule, id=alert_rule_id, user=request.user)
    if request.method == "GET":
        return JsonResponse(
            {
                "id": rule.id,
                "name": rule.name,
                "question": rule.question,
                "metric": rule.metric,
                "value_mode": rule.value_mode,
                "operator": rule.operator,
                "threshold": rule.threshold,
                "frequency": rule.frequency,
                "trigger_type": rule.trigger_type,
                "cooldown_hours": rule.cooldown_hours,
                "region": rule.region,
                "geography_type": str((rule.config_json or {}).get("geography_type") or ""),
                "state_code": str((rule.config_json or {}).get("state_code") or ""),
                "country_code": str((rule.config_json or {}).get("country_code") or ""),
                "last_raw_value": rule.last_raw_value,
                "last_evaluated_value": rule.last_evaluated_value,
                "last_condition_result": rule.last_condition_result,
            },
            status=200,
        )

    payload = _request_payload(request)
    payload["delivery_channels"] = rule.delivery_channels or [AlertDeliveryChannel.EMAIL]
    evaluation, error = _update_rule_from_payload(
        alert_rule=rule,
        payload=payload,
    )
    if error:
        return JsonResponse({"error": error}, status=400)
    return JsonResponse(
        {
            "id": rule.id,
            "metric": rule.metric,
            "value_mode": rule.value_mode,
            "operator": rule.operator,
            "threshold": rule.threshold,
            "evaluation": evaluation,
        },
        status=200,
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
    except Exception as exc:
        logger.warning(
            "Shared answer validation failed: %s payload_keys=%s payload=%s",
            exc,
            sorted(response_json.keys()) if isinstance(response_json, dict) else None,
            response_json,
            exc_info=True,
        )
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
    context = _shared_answer_context(shared_answer)
    app_url = str(getattr(settings, "APP_URL", "") or "").strip().rstrip("/")
    share_path = reverse("shared-answer-detail", args=[shared_answer.share_id])
    share_url = f"{app_url}{share_path}" if app_url else request.build_absolute_uri(share_path)
    share_image_path = "/public/images/social-card-compact.png"
    share_image_url = (
        f"{app_url}{share_image_path}"
        if app_url
        else request.build_absolute_uri(share_image_path)
    )
    context.update(
        {
            "share_url": share_url,
            "share_image_url": share_image_url,
        }
    )
    return render(
        request,
        "shared/detail.html",
        context,
    )
