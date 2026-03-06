from __future__ import annotations

import json
import logging
import os
import time
from typing import TYPE_CHECKING, Any, Dict, Final, Optional, Tuple

if TYPE_CHECKING:
    from agents.router import LLMRouteOutput

logger = logging.getLogger(__name__)

INTENTS: Final[tuple[str, ...]] = (
    "single_metric",
    "compare",
    "ranking",
    "derived",
    "explain",
    "ambiguous",
    "unsupported",
)

METRICS: Final[tuple[str, ...]] = (
    "iso_gas_dependency",
    "iso_renewables",
    "iso_fuel_mix",
    "iso_load",
    "working_gas_storage_change_weekly",
    "working_gas_storage_lower48",
    "henry_hub_spot",
    "lng_exports",
    "lng_imports",
    "ng_consumption_lower48",
    "ng_electricity",
    "ng_production_lower48",
    "ng_exploration_reserves_lower48",
)

ISO_FILTERS: Final[tuple[str, ...]] = (
    "ercot",
    "pjm",
    "isone",
    "nyiso",
    "caiso",
)

REGION_FILTERS: Final[tuple[str, ...]] = (
    "lower48",
    "east",
    "midwest",
    "south_central",
    "mountain",
    "pacific",
    "united_states_pipeline_total",
    "canada_pipeline",
    "mexico_pipeline",
)

METRIC_DESCRIPTIONS: Final[Dict[str, str]] = {
    "iso_gas_dependency": "ISO electricity generation share from natural gas.",
    "iso_renewables": "ISO renewable electricity generation or renewable share.",
    "iso_fuel_mix": "ISO generation mix by fuel categories.",
    "iso_load": "ISO grid/system electricity demand (load).",
    "working_gas_storage_change_weekly": "Weekly change in underground working gas storage.",
    "working_gas_storage_lower48": "Total underground working gas storage inventory for lower 48/regions.",
    "henry_hub_spot": "Henry Hub natural gas spot benchmark price.",
    "lng_exports": "Natural gas exports flows; includes LNG export framing in this app.",
    "lng_imports": "Natural gas imports flows; includes LNG import framing in this app.",
    "ng_consumption_lower48": "Total lower 48 natural gas consumption/use.",
    "ng_electricity": "Natural gas consumed by electric power sector.",
    "ng_production_lower48": "Lower 48 dry natural gas production/supply.",
    "ng_exploration_reserves_lower48": "Natural gas exploration/proved reserves in lower 48 context.",
}

_ALLOWED_INTENTS = set(INTENTS)
_ALLOWED_METRICS = set(METRICS)
_ALLOWED_ISOS = set(ISO_FILTERS)
_ALLOWED_REGIONS = set(REGION_FILTERS)


class LLMRouterError(RuntimeError):
    """Raised when structured LLM routing cannot be completed safely."""


def _build_route_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "intent": {"type": "string", "enum": list(INTENTS)},
            "primary_metric": {
                "anyOf": [
                    {"type": "string", "enum": list(METRICS)},
                    {"type": "null"},
                ]
            },
            "metrics": {
                "type": "array",
                "items": {"type": "string", "enum": list(METRICS)},
                "uniqueItems": True,
            },
            "filters": {
                "anyOf": [
                    {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "iso": {"type": "string", "enum": list(ISO_FILTERS)},
                            "region": {
                                "type": "string",
                                "enum": list(REGION_FILTERS),
                            },
                        },
                    },
                    {"type": "null"},
                ]
            },
            "reason": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "ambiguous": {"type": "boolean"},
        },
        "required": [
            "intent",
            "primary_metric",
            "metrics",
            "filters",
            "reason",
            "confidence",
            "ambiguous",
        ],
    }


def _build_prompts(user_query: str, normalized_query: str) -> Tuple[str, str]:
    metric_lines = "\n".join(
        f"- {metric}: {desc}" for metric, desc in METRIC_DESCRIPTIONS.items()
    )

    system_prompt = (
        "You are a strict routing classifier for an energy analytics backend. "
        "Return only valid JSON that matches the schema exactly. "
        "Do not invent metrics, filters, or intents. "
        "If uncertain, prefer intent='ambiguous' instead of guessing. "
        "If the request is unsupported by listed metrics, return "
        "intent='unsupported', primary_metric=null, metrics=[]. "
        "Allowed intents: "
        + ", ".join(INTENTS)
        + "\nAllowed metrics with compact definitions:\n"
        + metric_lines
        + "\nAllowed filter.iso values: "
        + ", ".join(ISO_FILTERS)
        + "\nAllowed filter.region values: "
        + ", ".join(REGION_FILTERS)
    )

    user_prompt = (
        f"Original user query: {user_query}\n"
        f"Normalized query: {normalized_query}\n"
        "Task: classify intent, choose metric(s), and optional filters from allowed enums only."
    )
    return system_prompt, user_prompt


def _get_openai_client() -> Any:
    from openai import OpenAI

    return OpenAI()


def _extract_json_text(response: Any) -> str:
    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = getattr(response, "output", None)
    if isinstance(output, list):
        for item in output:
            content = getattr(item, "content", None)
            if not isinstance(content, list):
                continue
            for chunk in content:
                text_value = getattr(chunk, "text", None)
                if isinstance(text_value, str) and text_value.strip():
                    return text_value

    raise LLMRouterError("Responses API returned no parseable text payload")


def _request_structured_route(
    user_query: str,
    normalized_query: str,
    model: str,
) -> Dict[str, Any]:
    system_prompt, user_prompt = _build_prompts(
        user_query=user_query,
        normalized_query=normalized_query,
    )
    client = _get_openai_client()
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "llm_route_output",
                "strict": True,
                "schema": _build_route_schema(),
            }
        },
    )
    raw_json = _extract_json_text(response)
    parsed = json.loads(raw_json)
    if not isinstance(parsed, dict):
        raise LLMRouterError("Structured route payload must be a JSON object")
    return parsed


def _clamp_confidence(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, parsed))


def _normalize_filters(filters: Any) -> Optional[Dict[str, str]]:
    if not isinstance(filters, dict):
        return None
    out: Dict[str, str] = {}
    iso = filters.get("iso")
    region = filters.get("region")
    if isinstance(iso, str) and iso in _ALLOWED_ISOS:
        out["iso"] = iso
    if isinstance(region, str) and region in _ALLOWED_REGIONS:
        out["region"] = region
    return out or None


def _sanitize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    intent_value = payload.get("intent")
    intent = intent_value if isinstance(intent_value, str) and intent_value in _ALLOWED_INTENTS else "unsupported"

    primary_metric_value = payload.get("primary_metric")
    primary_metric = (
        primary_metric_value
        if isinstance(primary_metric_value, str) and primary_metric_value in _ALLOWED_METRICS
        else None
    )

    metrics: list[str] = []
    raw_metrics = payload.get("metrics")
    if isinstance(raw_metrics, list):
        for metric in raw_metrics:
            if isinstance(metric, str) and metric in _ALLOWED_METRICS and metric not in metrics:
                metrics.append(metric)

    if primary_metric and primary_metric not in metrics:
        metrics.insert(0, primary_metric)

    if intent == "unsupported":
        primary_metric = None
        metrics = []

    ambiguous = bool(payload.get("ambiguous", False))
    if intent == "ambiguous":
        ambiguous = True

    reason_value = payload.get("reason")
    reason = str(reason_value)[:240] if reason_value is not None else None

    return {
        "intent": intent,
        "primary_metric": primary_metric,
        "metrics": metrics,
        "filters": _normalize_filters(payload.get("filters")),
        "reason": reason,
        "confidence": _clamp_confidence(payload.get("confidence")),
        "ambiguous": ambiguous,
    }


def _to_llm_route_output(data: Dict[str, Any]) -> "LLMRouteOutput":
    from agents.router import LLMRouteOutput

    return LLMRouteOutput(
        intent=data["intent"],
        primary_metric=data["primary_metric"],
        metrics=data["metrics"],
        filters=data["filters"],
        reason=data["reason"],
        confidence=data["confidence"],
        ambiguous=data["ambiguous"],
    )


def _is_transient_error(error: Exception) -> bool:
    transient_names = {
        "APIConnectionError",
        "APITimeoutError",
        "RateLimitError",
        "InternalServerError",
    }
    return error.__class__.__name__ in transient_names or isinstance(
        error,
        (ConnectionError, TimeoutError),
    )


def llm_route_structured(user_query: str, normalized_query: str) -> "LLMRouteOutput":
    model = os.getenv("ATLAS_ROUTER_MODEL", "gpt-5")
    max_attempts = 3
    backoff_seconds = (0.25, 0.75)

    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            raw_payload = _request_structured_route(
                user_query=user_query,
                normalized_query=normalized_query,
                model=model,
            )
            sanitized = _sanitize_payload(raw_payload)
            return _to_llm_route_output(sanitized)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if _is_transient_error(exc) and attempt < max_attempts:
                logger.warning(
                    "Transient LLM router error on attempt %s/%s (%s)",
                    attempt,
                    max_attempts,
                    exc.__class__.__name__,
                )
                time.sleep(backoff_seconds[min(attempt - 1, len(backoff_seconds) - 1)])
                continue
            break

    raise LLMRouterError(
        f"Failed to route with model={model} after {max_attempts} attempts"
    ) from last_error
