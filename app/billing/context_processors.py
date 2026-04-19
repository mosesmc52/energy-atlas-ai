from __future__ import annotations

from django.conf import settings

from billing.services import get_active_subscription, has_free_full_alert_access


def billing_subscription(request):
    user = getattr(request, "user", None)
    if user is None or not getattr(user, "is_authenticated", False):
        return {
            "active_subscription": None,
            "has_active_subscription": False,
            "has_free_full_alert_access": False,
            "chat_view_url": str(getattr(settings, "APP_URL", "") or "").rstrip("/") or "/",
        }

    subscription = get_active_subscription(user)
    return {
        "active_subscription": subscription,
        "has_active_subscription": subscription is not None,
        "has_free_full_alert_access": has_free_full_alert_access(user),
        "chat_view_url": str(getattr(settings, "APP_URL", "") or "").rstrip("/") or "/",
    }
