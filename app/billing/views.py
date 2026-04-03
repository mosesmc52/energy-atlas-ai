from __future__ import annotations

import stripe
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.urls import reverse


def _stripe_secret_key() -> str:
    if getattr(settings, "STRIPE_LIVE_MODE", False):
        return str(getattr(settings, "STRIPE_LIVE_SECRET_KEY", "") or "").strip()
    return str(getattr(settings, "STRIPE_TEST_SECRET_KEY", "") or "").strip()


@login_required
def start_checkout(request: HttpRequest) -> HttpResponse:
    secret_key = _stripe_secret_key()
    price_id = str(getattr(settings, "STRIPE_PRO_PRICE_ID", "") or "").strip()

    if not secret_key:
        messages.error(request, "Stripe is not configured yet.")
        return redirect("pricing")

    if not price_id:
        messages.error(request, "Stripe price ID is not configured yet.")
        return redirect("pricing")

    stripe.api_key = secret_key
    success_url = request.build_absolute_uri(reverse("pricing")) + "?checkout=success"
    cancel_url = request.build_absolute_uri(reverse("pricing")) + "?checkout=cancel"

    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        customer_email=request.user.email or None,
        success_url=success_url,
        cancel_url=cancel_url,
    )
    return redirect(session.url, permanent=False)
