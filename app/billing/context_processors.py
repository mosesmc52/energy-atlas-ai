from __future__ import annotations

from django.conf import settings
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from billing.services import get_active_subscription, has_free_full_alert_access


def _chat_view_url(*, include_django_auth_hint: bool) -> str:
    base_url = str(getattr(settings, "APP_URL", "") or "").rstrip("/") or "/"
    if not include_django_auth_hint:
        return base_url

    split = urlsplit(base_url)
    query = dict(parse_qsl(split.query, keep_blank_values=True))
    query["ea_from_django"] = "1"
    return urlunsplit(
        (
            split.scheme,
            split.netloc,
            split.path,
            urlencode(query),
            split.fragment,
        )
    )


def billing_subscription(request):
    user = getattr(request, "user", None)
    if user is None or not getattr(user, "is_authenticated", False):
        return {
            "active_subscription": None,
            "has_active_subscription": False,
            "has_free_full_alert_access": False,
            "chat_view_url": _chat_view_url(include_django_auth_hint=False),
        }

    subscription = get_active_subscription(user)
    return {
        "active_subscription": subscription,
        "has_active_subscription": subscription is not None,
        "has_free_full_alert_access": has_free_full_alert_access(user),
        "chat_view_url": _chat_view_url(include_django_auth_hint=True),
    }
