from __future__ import annotations

import os
from typing import Optional

try:
    import sentry_sdk
except Exception:  # pragma: no cover
    sentry_sdk = None


_INITIALIZED = False


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def init_sentry(*, service_name: str, use_django_integration: bool = False) -> bool:
    """Initialize Sentry once per process.

    Uses a single DSN env var for all app processes and tags each event with
    a service name so Django and Chainlit can be filtered separately.
    """

    global _INITIALIZED

    if _INITIALIZED or sentry_sdk is None:
        return False

    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        return False

    environment = os.getenv("SENTRY_ENVIRONMENT", os.getenv("ENV", "development")).strip() or "development"
    release = os.getenv("SENTRY_RELEASE", "").strip() or None
    traces_sample_rate = float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0"))

    integrations = []
    if use_django_integration:
        try:
            from sentry_sdk.integrations.django import DjangoIntegration

            integrations.append(DjangoIntegration())
        except Exception:
            pass

    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        release=release,
        traces_sample_rate=traces_sample_rate,
        send_default_pii=_as_bool(os.getenv("SENTRY_SEND_DEFAULT_PII"), False),
        integrations=integrations,
    )
    sentry_sdk.set_tag("service", service_name)

    _INITIALIZED = True
    return True
