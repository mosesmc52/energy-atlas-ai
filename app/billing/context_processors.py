from __future__ import annotations

from billing.services import get_active_subscription


def billing_subscription(request):
    user = getattr(request, "user", None)
    if user is None or not getattr(user, "is_authenticated", False):
        return {
            "active_subscription": None,
            "has_active_subscription": False,
        }

    subscription = get_active_subscription(user)
    return {
        "active_subscription": subscription,
        "has_active_subscription": subscription is not None,
    }
