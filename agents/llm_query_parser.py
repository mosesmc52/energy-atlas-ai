from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Final, Optional, Tuple

from agents.llm_router import (
    DATASET_FILTERS,
    ISO_FILTERS,
    INTENTS,
    METRICS,
    REGION_FILTERS,
    RESOURCE_CATEGORY_FILTERS,
)

logger = logging.getLogger(__name__)

TIME_WINDOWS: Final[tuple[str, ...]] = (
    "latest",
    "today",
    "this_week",
    "prior_week",
    "last_7_days",
    "last_30_days",
    "month_to_date",
    "year_to_date",
    "custom",
    "unknown",
)

COMPARISONS: Final[tuple[str, ...]] = (
    "prior_period",
    "week_over_week",
    "month_over_month",
    "year_over_year",
    "five_year_normal",
    "regional_comparison",
    "sector_comparison",
    "none",
)

CALCULATIONS: Final[tuple[str, ...]] = (
    "latest_value",
    "change",
    "percent_change",
    "spread",
    "z_score",
    "percentile",
    "correlation",
    "summary",
    "none",
)


@dataclass(frozen=True)
class LLMQueryParseOutput:
    intent: str
    primary_metric: Optional[str]
    metrics: list[str]
    filters: Optional[dict]
    time_window: Optional[str]
    comparison: Optional[str]
    calculation: Optional[str]
    question_topics: list[str]
    requires_multiple_sources: bool
    reason: Optional[str]
    confidence: float
    ambiguous: bool


class LLMQueryParserError(RuntimeError):
    """Raised when structured LLM parsing cannot be completed safely."""


_ALLOWED_INTENTS = set(INTENTS)
_ALLOWED_METRICS = set(METRICS)
_ALLOWED_ISOS = set(ISO_FILTERS)
_ALLOWED_REGIONS = set(REGION_FILTERS)
_ALLOWED_TIME_WINDOWS = set(TIME_WINDOWS)
_ALLOWED_COMPARISONS = set(COMPARISONS)
_ALLOWED_CALCULATIONS = set(CALCULATIONS)


def _build_parse_schema() -> Dict[str, Any]:
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
            },
            "filters": {
                "anyOf": [
                    {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "iso": {
                                "anyOf": [
                                    {"type": "string", "enum": list(ISO_FILTERS)},
                                    {"type": "null"},
                                ]
                            },
                            "region": {
                                "anyOf": [
                                    {"type": "string", "enum": list(REGION_FILTERS)},
                                    {"type": "null"},
                                ]
                            },
                            "resource_category": {
                                "anyOf": [
                                    {
                                        "type": "string",
                                        "enum": list(RESOURCE_CATEGORY_FILTERS),
                                    },
                                    {"type": "null"},
                                ]
                            },
                            "dataset": {
                                "anyOf": [
                                    {"type": "string", "enum": list(DATASET_FILTERS)},
                                    {"type": "null"},
                                ]
                            },
                            "normal_years": {
                                "anyOf": [
                                    {"type": "integer", "enum": [1, 2, 3, 5]},
                                    {"type": "null"},
                                ]
                            },
                        },
                        "required": [
                            "iso",
                            "region",
                            "resource_category",
                            "dataset",
                            "normal_years",
                        ],
                    },
                    {"type": "null"},
                ]
            },
            "time_window": {
                "anyOf": [
                    {"type": "string", "enum": list(TIME_WINDOWS)},
                    {"type": "null"},
                ]
            },
            "comparison": {
                "anyOf": [
                    {"type": "string", "enum": list(COMPARISONS)},
                    {"type": "null"},
                ]
            },
            "calculation": {
                "anyOf": [
                    {"type": "string", "enum": list(CALCULATIONS)},
                    {"type": "null"},
                ]
            },
            "question_topics": {
                "type": "array",
                "items": {"type": "string"},
            },
            "requires_multiple_sources": {"type": "boolean"},
            "reason": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "ambiguous": {"type": "boolean"},
        },
        "required": [
            "intent",
            "primary_metric",
            "metrics",
            "filters",
            "time_window",
            "comparison",
            "calculation",
            "question_topics",
            "requires_multiple_sources",
            "reason",
            "confidence",
            "ambiguous",
        ],
    }


def _build_prompts(user_query: str, normalized_query: str) -> Tuple[str, str]:
    system_prompt = (
        "You are a strict query parser for an energy analytics backend. "
        "Your job is to convert the user’s question into structured JSON.\n\n"
        "Do NOT select source adapters.\n"
        "Do NOT execute queries.\n"
        "Do NOT invent metrics or filters.\n\n"
        "Use only allowed enum values.\n\n"
        "If multiple datasets are required, set requires_multiple_sources=true.\n"
        "If uncertain, set intent='ambiguous'.\n"
        "If unsupported, set intent='unsupported', primary_metric=null, metrics=[]."
    )

    user_prompt = (
        f"Original user query: {user_query}\n"
        f"Normalized query: {normalized_query}\n"
        "Task: parse question into intent, metric(s), filters, time_window, comparison, calculation, and topics using allowed enums only."
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

    raise LLMQueryParserError("Responses API returned no parseable text payload")


def _request_structured_parse(
    user_query: str,
    normalized_query: str,
    model: str,
) -> Dict[str, Any]:
    system_prompt, user_prompt = _build_prompts(user_query=user_query, normalized_query=normalized_query)
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
                "name": "llm_query_parse_output",
                "strict": True,
                "schema": _build_parse_schema(),
            }
        },
    )
    raw_json = _extract_json_text(response)
    parsed = json.loads(raw_json)
    if not isinstance(parsed, dict):
        raise LLMQueryParserError("Structured parse payload must be a JSON object")
    return parsed


def _clamp_confidence(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, parsed))


def _normalize_filters(filters: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(filters, dict):
        return None
    out: Dict[str, Any] = {}
    iso = filters.get("iso")
    region = filters.get("region")
    resource_category = filters.get("resource_category")
    dataset = filters.get("dataset")
    if isinstance(iso, str) and iso in _ALLOWED_ISOS:
        out["iso"] = iso
    if isinstance(region, str) and region in _ALLOWED_REGIONS:
        out["region"] = region
    if isinstance(resource_category, str) and resource_category in RESOURCE_CATEGORY_FILTERS:
        out["resource_category"] = resource_category
    if isinstance(dataset, str) and dataset in DATASET_FILTERS:
        out["dataset"] = dataset
    normal_years = filters.get("normal_years")
    if isinstance(normal_years, int) and normal_years in {1, 2, 3, 5}:
        out["normal_years"] = normal_years
    return out or None


def _sanitize_payload(payload: Dict[str, Any]) -> LLMQueryParseOutput:
    intent_value = payload.get("intent")
    intent = intent_value if isinstance(intent_value, str) and intent_value in _ALLOWED_INTENTS else "unsupported"

    primary_metric_value = payload.get("primary_metric")
    primary_metric = (
        primary_metric_value if isinstance(primary_metric_value, str) and primary_metric_value in _ALLOWED_METRICS else None
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

    time_window = payload.get("time_window")
    if not isinstance(time_window, str) or time_window not in _ALLOWED_TIME_WINDOWS:
        time_window = "unknown"

    comparison = payload.get("comparison")
    if not isinstance(comparison, str) or comparison not in _ALLOWED_COMPARISONS:
        comparison = "none"

    calculation = payload.get("calculation")
    if not isinstance(calculation, str) or calculation not in _ALLOWED_CALCULATIONS:
        calculation = "none"

    question_topics: list[str] = []
    raw_topics = payload.get("question_topics")
    if isinstance(raw_topics, list):
        for topic in raw_topics:
            if isinstance(topic, str):
                normalized = topic.strip().lower()
                if normalized and normalized not in question_topics:
                    question_topics.append(normalized)

    requires_multiple_sources = bool(payload.get("requires_multiple_sources", False))
    if len(metrics) >= 2:
        requires_multiple_sources = True

    ambiguous = bool(payload.get("ambiguous", False))
    confidence = _clamp_confidence(payload.get("confidence"))
    if intent == "ambiguous" or confidence < 0.55:
        ambiguous = True

    reason_value = payload.get("reason")
    reason = str(reason_value)[:240] if reason_value is not None else None

    return LLMQueryParseOutput(
        intent=intent,
        primary_metric=primary_metric,
        metrics=metrics,
        filters=_normalize_filters(payload.get("filters")),
        time_window=time_window,
        comparison=comparison,
        calculation=calculation,
        question_topics=question_topics,
        requires_multiple_sources=requires_multiple_sources,
        reason=reason,
        confidence=confidence,
        ambiguous=ambiguous,
    )


def _is_transient_error(error: Exception) -> bool:
    transient_names = {
        "APIConnectionError",
        "APITimeoutError",
        "RateLimitError",
        "InternalServerError",
    }
    return error.__class__.__name__ in transient_names or isinstance(error, (ConnectionError, TimeoutError))


def llm_parse_query(user_query: str, normalized_query: str) -> LLMQueryParseOutput:
    model = os.getenv("ATLAS_QUERY_PARSER_MODEL", os.getenv("ATLAS_ROUTER_MODEL", "gpt-5"))
    max_attempts = 3
    backoff_seconds = (0.25, 0.75)

    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        try:
            raw_payload = _request_structured_parse(
                user_query=user_query,
                normalized_query=normalized_query,
                model=model,
            )
            return _sanitize_payload(raw_payload)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if _is_transient_error(exc) and attempt < max_attempts:
                logger.warning(
                    "Transient LLM query parser error on attempt %s/%s (%s)",
                    attempt,
                    max_attempts,
                    exc.__class__.__name__,
                )
                time.sleep(backoff_seconds[min(attempt - 1, len(backoff_seconds) - 1)])
                continue
            break

    raise LLMQueryParserError(
        f"Failed to parse query with model={model} after {max_attempts} attempts"
    ) from last_error
