from __future__ import annotations

from typing import Any


SESSION_KEY = "_analytics_events"


def _clean_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def queue_analytics_event(request, event: str, **params: Any) -> None:
    event_name = str(event or "").strip()
    if not event_name or not hasattr(request, "session"):
        return

    payload = {"event": event_name}
    payload.update({key: _clean_value(value) for key, value in params.items()})

    events = list(request.session.get(SESSION_KEY) or [])
    events.append(payload)
    request.session[SESSION_KEY] = events[-20:]
    request.session.modified = True


def analytics_events(request) -> dict[str, list[dict[str, Any]]]:
    if not hasattr(request, "session"):
        return {"analytics_events": []}

    events = request.session.pop(SESSION_KEY, [])
    if events:
        request.session.modified = True
    return {"analytics_events": events if isinstance(events, list) else []}
