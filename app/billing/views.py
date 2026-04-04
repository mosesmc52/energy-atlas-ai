from __future__ import annotations

import json
import logging
from datetime import datetime
from datetime import timezone as dt_timezone

import stripe
from billing.models import (
    BillingInterval,
    PlanPrice,
    SubscriptionPlan,
    SubscriptionStatus,
    UserSubscription,
    WebhookEvent,
)
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import HttpRequest, HttpResponse, HttpResponseBadRequest, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

logger = logging.getLogger(__name__)
HANDLED_STRIPE_EVENT_TYPES = {
    "checkout.session.completed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
}


def _stripe_secret_key() -> str:
    if getattr(settings, "STRIPE_LIVE_MODE", False):
        return str(getattr(settings, "STRIPE_LIVE_SECRET_KEY", "") or "").strip()
    return str(getattr(settings, "STRIPE_TEST_SECRET_KEY", "") or "").strip()


def _stripe_webhook_secret() -> str:
    return str(getattr(settings, "STRIPE_WEBHOOK_SECRET", "") or "").strip()


def _ts_to_dt(value):
    if not value:
        return None
    return datetime.fromtimestamp(int(value), tz=dt_timezone.utc)


def _json_ready(value):
    if hasattr(value, "to_dict_recursive"):
        return value.to_dict_recursive()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    return value


def _safe_json_value(value):
    prepared = _json_ready(value)
    return json.loads(json.dumps(prepared, default=str))


def _subscription_status(value: str | None) -> str:
    allowed = {choice for choice, _ in SubscriptionStatus.choices}
    status = str(value or "").strip().lower()
    return status if status in allowed else SubscriptionStatus.INCOMPLETE


def _cancel_at_period_end(subscription_obj: dict) -> bool:
    if bool(subscription_obj.get("cancel_at_period_end", False)):
        return True

    cancel_at = subscription_obj.get("cancel_at")
    current_period_end = subscription_obj.get("current_period_end")
    cancellation_reason = str(
        ((subscription_obj.get("cancellation_details") or {}).get("reason") or "")
    ).strip()

    if cancel_at and current_period_end and int(cancel_at) == int(current_period_end):
        return True
    if cancellation_reason == "cancellation_requested" and cancel_at:
        return True
    return False


def _cancel_reason(subscription_obj: dict) -> str:
    details = subscription_obj.get("cancellation_details") or {}
    reason = str(details.get("reason") or "").strip()
    feedback = str(details.get("feedback") or "").strip()
    comment = str(details.get("comment") or "").strip()

    parts = []
    if reason:
        parts.append(f"reason={reason}")
    if feedback:
        parts.append(f"feedback={feedback}")
    if comment and comment != ".":
        parts.append(f"comment={comment}")
    return "; ".join(parts)


def _cancellation_feedback(subscription_obj: dict) -> str:
    details = subscription_obj.get("cancellation_details") or {}
    return str(details.get("feedback") or "").strip()


def _cancellation_comment(subscription_obj: dict) -> str:
    details = subscription_obj.get("cancellation_details") or {}
    return str(details.get("comment") or "").strip()


def _plan_from_price_id(price_id: str):
    if not price_id:
        return None
    plan_price = (
        PlanPrice.objects.select_related("plan")
        .filter(stripe_price_id=price_id, is_active=True, plan__is_active=True)
        .first()
    )
    return plan_price.plan if plan_price else None


def _user_from_metadata(metadata: dict | None):
    user_id = str((metadata or {}).get("user_id") or "").strip()
    if not user_id:
        return None
    User = get_user_model()
    try:
        return User.objects.get(pk=int(user_id))
    except Exception:
        logger.warning("Stripe metadata referenced missing user_id=%s", user_id)
        return None


def _checkout_plan_price() -> PlanPrice | None:
    monthly_price = (
        PlanPrice.objects.select_related("plan")
        .filter(
            plan__key="pro",
            is_active=True,
            plan__is_active=True,
            interval=BillingInterval.MONTH,
        )
        .order_by("created_at")
        .first()
    )
    if monthly_price is not None:
        return monthly_price
    return (
        PlanPrice.objects.select_related("plan")
        .filter(plan__key="pro", is_active=True, plan__is_active=True)
        .order_by("created_at")
        .first()
    )


def _format_plan_price(plan_price: PlanPrice | None) -> tuple[str, str]:
    if plan_price is None:
        return ("Custom", "")

    metadata = plan_price.metadata_json or {}
    currency = str(plan_price.currency or metadata.get("currency") or "usd").upper()
    unit_amount = (
        plan_price.unit_amount_cents
        if plan_price.unit_amount_cents is not None
        else metadata.get("unit_amount")
    )
    display_amount = str(
        plan_price.display_price or metadata.get("display_amount") or ""
    ).strip()
    interval_label = {
        BillingInterval.MONTH: "per month",
        BillingInterval.YEAR: "per year",
        BillingInterval.ONE_TIME: "one-time",
    }.get(plan_price.interval, plan_price.interval.replace("_", " "))

    if display_amount:
        return (display_amount, interval_label)

    if unit_amount not in (None, ""):
        try:
            amount = int(unit_amount) / 100
            amount_text = (
                f"${amount:,.0f}" if float(amount).is_integer() else f"${amount:,.2f}"
            )
            return (amount_text, interval_label)
        except (TypeError, ValueError):
            pass

    if currency == "USD":
        return ("Configured in Stripe", interval_label)
    return (f"Configured in {currency}", interval_label)


def _plan_features(plan, plan_price: PlanPrice | None) -> list[str]:
    features = (
        plan.features_json.get("pricing_features")
        or plan.features_json.get("features")
        or []
    )
    normalized = [str(item).strip() for item in features if str(item).strip()]
    if normalized:
        return normalized

    fallback = [f"{plan.active_alert_limit} active alerts"]
    if plan.key == "free":
        fallback.extend(
            [
                "Daily signal evaluation",
                "Core natural gas signals",
                "Basic chat access",
                "Email notifications",
            ]
        )
        return fallback
    if plan.key == "pro":
        fallback.extend(
            [
                "Full signal coverage",
                "Report-based explanations",
                "Alert history and status tracking",
                "Priority access to new features",
            ]
        )
        return fallback
    interval = plan_price.interval if plan_price is not None else "custom"
    fallback.extend(
        [
            f"{plan.name} plan access",
            f"{interval.replace('_', ' ')} billing",
        ]
    )
    return fallback


def _plan_card(plan) -> dict:
    plan_price = (
        PlanPrice.objects.filter(plan=plan, is_active=True)
        .order_by("interval", "created_at")
        .first()
    )
    price_text, interval_text = _format_plan_price(plan_price)
    return {
        "key": plan.key,
        "name": plan.name,
        "description": plan.description,
        "active_alert_limit": plan.active_alert_limit,
        "price_text": price_text if plan.key != "free" else "$0",
        "interval_text": interval_text if plan.key != "free" else "forever",
        "features": _plan_features(plan, plan_price),
        "cta_label": "Get started free" if plan.key == "free" else f"Start {plan.name}",
    }


def pricing_page(request: HttpRequest) -> HttpResponse:
    plans = {
        plan.key: plan
        for plan in SubscriptionPlan.objects.filter(is_active=True).order_by("name")
    }
    free_plan = plans.get("free")
    pro_plan = plans.get("pro")
    plan_cards = []
    if free_plan is not None:
        plan_cards.append(_plan_card(free_plan))
    if pro_plan is not None:
        plan_cards.append(_plan_card(pro_plan))
    for key, plan in plans.items():
        if key not in {"free", "pro"}:
            plan_cards.append(_plan_card(plan))

    return render(
        request,
        "billing/pricing.html",
        {
            "plan_cards": plan_cards,
            "free_plan_card": next(
                (card for card in plan_cards if card["key"] == "free"), None
            ),
            "pro_plan_card": next(
                (card for card in plan_cards if card["key"] == "pro"), None
            ),
        },
    )


@login_required
def manage_subscription(request: HttpRequest) -> HttpResponse:
    subscription = (
        UserSubscription.objects.filter(user=request.user)
        .select_related("plan")
        .first()
    )
    subscription = _refresh_local_subscription(subscription)
    plan_price = None
    if subscription is not None:
        plan_price = (
            PlanPrice.objects.filter(plan=subscription.plan, is_active=True)
            .order_by("interval", "created_at")
            .first()
        )
    price_text, interval_text = _format_plan_price(plan_price)
    return render(
        request,
        "billing/manage_subscription.html",
        {
            "subscription": subscription,
            "subscription_price_text": price_text,
            "subscription_interval_text": interval_text,
        },
    )


@login_required
def billing_settings(request: HttpRequest) -> HttpResponse:
    subscription = (
        UserSubscription.objects.filter(user=request.user)
        .select_related("plan")
        .first()
    )
    if subscription is None or not subscription.stripe_customer_id:
        messages.error(request, "No Stripe billing profile was found for this account.")
        return redirect("billing:manage")

    secret_key = _stripe_secret_key()
    if not secret_key:
        messages.error(request, "Stripe is not configured yet.")
        return redirect("billing:manage")

    stripe.api_key = secret_key
    try:
        session = stripe.billing_portal.Session.create(
            customer=subscription.stripe_customer_id,
            return_url=request.build_absolute_uri(reverse("billing:manage")),
        )
    except Exception:
        logger.exception(
            "Stripe billing portal session creation failed for user_id=%s customer_id=%s",
            request.user.pk,
            subscription.stripe_customer_id,
        )
        messages.error(request, "Unable to open Stripe billing settings right now.")
        return redirect("billing:manage")

    return redirect(session.url, permanent=False)


def _price_id_from_subscription_object(subscription_obj: dict) -> str:
    try:
        return str(subscription_obj["items"]["data"][0]["price"]["id"] or "").strip()
    except Exception:
        return ""


@transaction.atomic
def _upsert_user_subscription(
    *,
    user,
    plan,
    stripe_customer_id: str,
    stripe_subscription_id: str,
    subscription_obj: dict,
):
    if user is None or plan is None:
        return None

    subscription, _ = UserSubscription.objects.update_or_create(
        user=user,
        defaults={
            "plan": plan,
            "status": _subscription_status(subscription_obj.get("status")),
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "current_period_start": _ts_to_dt(
                subscription_obj.get("current_period_start")
            ),
            "current_period_end": _ts_to_dt(subscription_obj.get("current_period_end")),
            "cancel_at_period_end": _cancel_at_period_end(subscription_obj),
            "cancel_reason": _cancel_reason(subscription_obj),
            "cancellation_feedback": _cancellation_feedback(subscription_obj),
            "cancellation_comment": _cancellation_comment(subscription_obj),
            "raw_payload_json": _safe_json_value(subscription_obj),
        },
    )
    return subscription


def _refresh_local_subscription(subscription: UserSubscription | None) -> UserSubscription | None:
    if (
        subscription is None
        or not subscription.stripe_subscription_id
        or not _stripe_secret_key()
    ):
        return subscription

    stripe.api_key = _stripe_secret_key()
    try:
        stripe_subscription = _safe_json_value(
            stripe.Subscription.retrieve(subscription.stripe_subscription_id)
        )
    except Exception:
        logger.exception(
            "Stripe subscription refresh failed for user_id=%s subscription_id=%s",
            subscription.user_id,
            subscription.stripe_subscription_id,
        )
        return subscription

    refreshed_plan = (
        _plan_from_price_id(_price_id_from_subscription_object(stripe_subscription))
        or subscription.plan
    )
    return _upsert_user_subscription(
        user=subscription.user,
        plan=refreshed_plan,
        stripe_customer_id=str(subscription.stripe_customer_id or "").strip(),
        stripe_subscription_id=str(subscription.stripe_subscription_id or "").strip(),
        subscription_obj=stripe_subscription,
    )


def _handle_checkout_completed(event_obj: dict):
    session = event_obj
    user_id = str(
        session.get("client_reference_id")
        or (session.get("metadata") or {}).get("user_id")
        or ""
    ).strip()
    price_id = str((session.get("metadata") or {}).get("price_id") or "").strip()
    stripe_customer_id = str(session.get("customer") or "").strip()
    stripe_subscription_id = str(session.get("subscription") or "").strip()
    if not user_id or not stripe_subscription_id:
        return

    user = _user_from_metadata({"user_id": user_id})
    if user is None:
        return

    subscription_obj = None
    stripe.api_key = _stripe_secret_key()
    try:
        subscription_obj = _safe_json_value(
            stripe.Subscription.retrieve(stripe_subscription_id)
        )
    except Exception:
        logger.exception(
            "Stripe checkout completed but subscription retrieve failed. subscription_id=%s",
            stripe_subscription_id,
        )

    resolved_price_id = price_id
    if subscription_obj is not None:
        resolved_price_id = resolved_price_id or _price_id_from_subscription_object(
            subscription_obj
        )
    plan = _plan_from_price_id(resolved_price_id)
    if plan is None:
        logger.warning(
            "Stripe checkout completed but no active plan matched price_id=%s for user_id=%s",
            resolved_price_id,
            user_id,
        )
        return
    if subscription_obj is None:
        subscription_obj = {
            "id": stripe_subscription_id,
            "customer": stripe_customer_id,
            "status": SubscriptionStatus.INCOMPLETE,
            "cancel_at_period_end": False,
            "metadata": {
                "user_id": str(user.pk),
                "price_id": resolved_price_id,
            },
        }
    _upsert_user_subscription(
        user=user,
        plan=plan,
        stripe_customer_id=stripe_customer_id,
        stripe_subscription_id=stripe_subscription_id,
        subscription_obj=subscription_obj,
    )


def _handle_subscription_event(event_obj: dict):
    subscription_obj = event_obj
    stripe_customer_id = str(subscription_obj.get("customer") or "").strip()
    stripe_subscription_id = str(subscription_obj.get("id") or "").strip()
    if not stripe_customer_id or not stripe_subscription_id:
        return

    user_subscription = (
        UserSubscription.objects.filter(stripe_customer_id=stripe_customer_id)
        .select_related("user")
        .first()
    )
    if user_subscription is None:
        user_subscription = (
            UserSubscription.objects.filter(
                stripe_subscription_id=stripe_subscription_id
            )
            .select_related("user")
            .first()
        )
    if user_subscription is None:
        metadata = subscription_obj.get("metadata") or {}
        user = _user_from_metadata(metadata)
        plan = _plan_from_price_id(
            str(metadata.get("price_id") or "").strip()
            or _price_id_from_subscription_object(subscription_obj)
        )
        if user is not None and plan is not None:
            _upsert_user_subscription(
                user=user,
                plan=plan,
                stripe_customer_id=stripe_customer_id,
                stripe_subscription_id=stripe_subscription_id,
                subscription_obj=subscription_obj,
            )
            return
        logger.warning(
            "Stripe subscription event could not be matched to a local subscription. customer=%s subscription=%s",
            stripe_customer_id,
            stripe_subscription_id,
        )
        return

    plan = (
        _plan_from_price_id(_price_id_from_subscription_object(subscription_obj))
        or user_subscription.plan
    )
    _upsert_user_subscription(
        user=user_subscription.user,
        plan=plan,
        stripe_customer_id=stripe_customer_id,
        stripe_subscription_id=stripe_subscription_id,
        subscription_obj=subscription_obj,
    )


@login_required
def start_checkout(request: HttpRequest) -> HttpResponse:
    secret_key = _stripe_secret_key()
    plan_price = _checkout_plan_price()

    if not secret_key:
        messages.error(request, "Stripe is not configured yet.")
        return redirect("pricing")

    if plan_price is None:
        messages.error(request, "No active Pro plan price is configured in billing.")
        return redirect("pricing")

    price_id = plan_price.stripe_price_id

    stripe.api_key = secret_key
    success_url = (
        request.build_absolute_uri(reverse("alerts:list")) + "?checkout=success"
    )
    cancel_url = request.build_absolute_uri(reverse("pricing")) + "?checkout=cancel"

    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        customer_email=request.user.email or None,
        client_reference_id=str(request.user.pk),
        metadata={
            "user_id": str(request.user.pk),
            "price_id": price_id,
        },
        subscription_data={
            "metadata": {
                "user_id": str(request.user.pk),
                "price_id": price_id,
            }
        },
        success_url=success_url,
        cancel_url=cancel_url,
    )
    return redirect(session.url, permanent=False)


@csrf_exempt
@require_POST
def stripe_webhook(request: HttpRequest) -> HttpResponse:
    event_type = "unknown"
    try:
        secret = _stripe_webhook_secret()
        if not secret:
            return HttpResponseBadRequest("STRIPE_WEBHOOK_SECRET is not configured.")

        payload = request.body
        signature = request.META.get("HTTP_STRIPE_SIGNATURE", "")
        try:
            event = stripe.Webhook.construct_event(payload, signature, secret)
        except ValueError:
            return HttpResponseBadRequest("Invalid payload.")
        except stripe.error.SignatureVerificationError:
            return HttpResponseBadRequest("Invalid signature.")

        event = _safe_json_value(event)
        event_id = str(event.get("id") or "").strip()
        event_type = str(event.get("type") or "").strip()
        event_obj = event.get("data", {}).get("object", {}) or {}
        if not event_id:
            return HttpResponseBadRequest("Missing event id.")

        webhook_event, created = WebhookEvent.objects.get_or_create(
            provider="stripe",
            event_id=event_id,
            defaults={
                "event_type": event_type,
                "livemode": bool(event.get("livemode", False)),
                "payload_json": event,
            },
        )
        if not created and webhook_event.processed_at is not None:
            return JsonResponse({"status": "already_processed"})

        webhook_event.event_type = event_type
        webhook_event.livemode = bool(event.get("livemode", False))
        webhook_event.payload_json = event

        if event_type not in HANDLED_STRIPE_EVENT_TYPES:
            webhook_event.processed_at = timezone.now()
            webhook_event.save(
                update_fields=[
                    "event_type",
                    "livemode",
                    "payload_json",
                    "processed_at",
                ]
            )
            return JsonResponse({"status": "ignored", "event_type": event_type})

        if event_type == "checkout.session.completed":
            _handle_checkout_completed(event_obj)
        else:
            _handle_subscription_event(event_obj)

        webhook_event.processed_at = timezone.now()
        webhook_event.save(
            update_fields=[
                "event_type",
                "livemode",
                "payload_json",
                "processed_at",
            ]
        )
    except Exception:
        logger.exception("Stripe webhook processing failed unexpectedly")
        return JsonResponse({"status": "error_ignored", "event_type": event_type})

    return JsonResponse({"status": "ok"})
