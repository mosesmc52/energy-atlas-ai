from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

import pandas as pd

from agents.filter_resolvers import FilterResolverDeps
from agents.filter_resolvers import build_filters as build_metric_filters
from agents.llm_router import LLMRouterError
from agents.llm_router import llm_route_structured as llm_route_structured_impl
from agents.metric_capabilities import get_metric_capability
from agents.router_data import (
    BONUS_TERMS,
    COMPARE_PATTERNS,
    DERIVED_PATTERNS,
    EXPLAIN_PATTERNS,
    FORECAST_PATTERNS,
    NORMALIZE_PATTERNS,
    RANK_PATTERNS,
    ROUTE_MAP,
)
from agents.router_keywords import (
    ALLOWED_CONSUMPTION_STATES,
    ALLOWED_EXPORT_REGIONS,
    ALLOWED_IMPORT_REGIONS,
    ALLOWED_ISOS,
    ALLOWED_PIPELINE_DATASETS,
    ALLOWED_PRODUCTION_STATES,
    ALLOWED_RESERVES_RESOURCE_CATEGORIES,
    ALLOWED_RESERVES_STATES,
    ALLOWED_STORAGE_REGIONS,
    ALLOWED_TRADE_REGIONS,
    ALLOWED_WEATHER_NORMAL_YEARS,
    ALLOWED_WEATHER_REGIONS,
    CONSUMPTION_STATE_KEYWORDS,
    EXPORT_REGION_KEYWORDS,
    IMPORT_REGION_KEYWORDS,
    ISO_KEYWORDS,
    PIPELINE_DATASET_KEYWORDS,
    PRODUCTION_STATE_KEYWORDS,
    REGIONAL_GROUP_TERMS,
    RESERVES_RESOURCE_CATEGORY_KEYWORDS,
    RESERVES_STATE_KEYWORDS,
    STORAGE_COMPARE_TERMS,
    STORAGE_REGION_KEYWORDS,
    TRADE_REGION_KEYWORDS,
    WEATHER_REGION_KEYWORDS,
)
from agents.scoring_policy import ScoreAdjustmentDeps, apply_metric_score_adjustments
from agents.window_policy import WindowPolicyDeps
from agents.window_policy import (
    resolve_metric_lookback_years as resolve_window_lookback_years,
)
from agents.window_policy import (
    resolved_normal_years_for_query as resolve_window_normal_years,
)
from utils.dates import has_explicit_date_reference, resolve_date_range
from utils.helpers import contains_any
from utils.query_intents import has_seasonal_norm_phrase
from utils.query_intents import (
    is_current_like_without_explicit_window,
    is_iso_gas_share_question,
    is_power_burn_seasonal_question,
    is_renewables_power_sector_demand_question,
)

ALLOWED_METRICS = set(ROUTE_MAP.keys())

@dataclass(frozen=True)
class RouteCandidate:
    metric: str
    score: float
    matched_terms: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class HybridRouteResult:
    intent: Literal[
        "single_metric",
        "compare",
        "ranking",
        "derived",
        "explain",
        "ambiguous",
        "unsupported",
    ]
    primary_metric: Optional[str]
    metrics: List[str]
    start: str
    end: str
    filters: Optional[Dict[str, Any]] = None
    confidence: float = 0.0
    ambiguous: bool = False
    candidates: List[RouteCandidate] = field(default_factory=list)
    source: Literal["rule", "llm"] = "rule"
    reason: Optional[str] = None
    normalized_query: Optional[str] = None
    include_forecast: bool = False
    forecast_horizon_days: Optional[int] = None


# ----------------------------
# Helpers
# ----------------------------
def normalize_query(user_query: str) -> str:
    q = user_query.lower().strip()
    for pattern, replacement in NORMALIZE_PATTERNS:
        q = re.sub(pattern, replacement, q, flags=re.IGNORECASE)
    q = re.sub(r"\s+", " ", q).strip()
    return q


def route_iso(q: str) -> str | None:
    q = q.lower()
    for iso, keys in ISO_KEYWORDS.items():
        if contains_any(keys, q):
            return iso
    return None


def route_storage_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in STORAGE_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_weather_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in WEATHER_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_weather_normal_years(q: str) -> int | None:
    q = q.lower()
    direct = re.search(r"\b([12345])\s*[- ]?years?\b", q)
    if direct:
        return int(direct.group(1))
    word_map = {
        "one year": 1,
        "two year": 2,
        "three year": 3,
        "four year": 4,
        "five year": 5,
    }
    for phrase, value in word_map.items():
        if phrase in q:
            return value
    return None


def wants_regional_grouping(q: str) -> bool:
    q = q.lower()
    return contains_any(REGIONAL_GROUP_TERMS, q)


def wants_storage_ranking_by_region(q: str) -> bool:
    q = q.lower()
    return any(term in q for term in ("withdrawal", "injection", "build")) and any(
        term in q for term in ("where", "fastest", "largest", "biggest", "most")
    )


def wants_storage_level_and_change(q: str) -> bool:
    q = q.lower()
    return "storage" in q and contains_any(STORAGE_COMPARE_TERMS, q) and any(
        term in q for term in ("together", "compare")
    )


def wants_seasonal_norm_comparison(q: str) -> bool:
    return has_seasonal_norm_phrase(q)


def is_weekly_energy_atlas_summary_query(q: str) -> bool:
    q = q.lower().replace("’", "'")
    direct_phrases = (
        "week in energy atlas",
        "energy atlas weekly summary",
        "weekly energy atlas recap",
        "weekly natural gas wrap-up",
        "weekly natural gas wrap up",
        "weekly wrap-up",
        "weekly wrap up",
    )
    if any(phrase in q for phrase in direct_phrases):
        return True

    recap_cues = ("weekly summary", "weekly recap", "this week's summary")
    has_recap = any(cue in q for cue in recap_cues)
    if not has_recap and "energy atlas summary" in q and ("this week" in q or "weekly" in q):
        has_recap = True
    has_energy_atlas = "energy atlas" in q
    if has_recap and has_energy_atlas:
        return True
    has_weather = "weather" in q
    has_storage = "storage" in q
    has_lng_or_supply = ("lng" in q) or ("supply" in q)
    has_price = ("price" in q) or ("henry hub" in q)
    return has_recap and has_weather and has_storage and has_lng_or_supply and has_price


def route_trade_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in TRADE_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_import_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in IMPORT_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_export_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in EXPORT_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_production_state(q: str) -> str | None:
    q = q.lower()
    for state, keys in PRODUCTION_STATE_KEYWORDS.items():
        long_keys = [key for key in keys if len(key) > 2]
        if long_keys and contains_any(long_keys, q):
            return state

    safe_abbrev_states = ALLOWED_PRODUCTION_STATES - {"ar", "in", "or", "united_states_total"}
    tokens = set(re.findall(r"\b[a-z]{2}\b", q))
    for state in safe_abbrev_states:
        if state in tokens:
            return state

    return None


def route_consumption_state(q: str) -> str | None:
    q = q.lower()
    for state, keys in CONSUMPTION_STATE_KEYWORDS.items():
        long_keys = [key for key in keys if len(key) > 2]
        if long_keys and contains_any(long_keys, q):
            return state

    safe_abbrev_states = ALLOWED_CONSUMPTION_STATES - {
        "ar",
        "de",
        "hi",
        "id",
        "in",
        "me",
        "or",
        "united_states_total",
    }
    tokens = set(re.findall(r"\b[a-z]{2}\b", q))
    for state in safe_abbrev_states:
        if state in tokens:
            return state

    return None


def route_reserves_state(q: str) -> str | None:
    q = q.lower()
    for state, keys in RESERVES_STATE_KEYWORDS.items():
        long_keys = [key for key in keys if len(key) > 2]
        if long_keys and contains_any(long_keys, q):
            return state

    safe_abbrev_states = ALLOWED_RESERVES_STATES - {"al", "ar", "oh", "ok", "all", "us"}
    tokens = set(re.findall(r"\b[a-z]{2}\b", q))
    for state in safe_abbrev_states:
        if state in tokens:
            return state

    return None


def route_reserves_resource_category(q: str) -> str | None:
    q = q.lower()
    for category, keys in RESERVES_RESOURCE_CATEGORY_KEYWORDS.items():
        if contains_any(keys, q):
            return category
    return None


def route_pipeline_dataset(q: str) -> str | None:
    q = q.lower()
    for dataset, keys in PIPELINE_DATASET_KEYWORDS.items():
        if contains_any(keys, q):
            return dataset
    return None


def detect_intent(q: str) -> str:
    if any(re.search(p, q) for p in COMPARE_PATTERNS):
        return "compare"
    if any(re.search(p, q) for p in RANK_PATTERNS):
        return "ranking"
    if any(re.search(p, q) for p in DERIVED_PATTERNS):
        return "derived"
    if any(re.search(p, q) for p in EXPLAIN_PATTERNS):
        return "explain"
    return "single_metric"


def detect_forecast_request(q: str) -> bool:
    return any(re.search(pattern, q) for pattern in FORECAST_PATTERNS)


def detect_forecast_horizon_days(q: str) -> Optional[int]:
    if not detect_forecast_request(q):
        return None
    if re.search(r"\b(14|fourteen)\s*day", q) or re.search(r"\b(two|2)\s+weeks?\b", q):
        return 14
    return 7


def score_metric(q: str, metric: str, keywords: List[str]) -> RouteCandidate:
    score = 0.0
    matched_terms: List[str] = []

    for kw in keywords:
        if kw in q:
            score += 3.0 if len(kw.split()) > 1 else 1.5
            matched_terms.append(kw)

    for bonus in BONUS_TERMS.get(metric, []):
        if bonus in q and bonus not in matched_terms:
            score += 1.0
            matched_terms.append(bonus)

    score = apply_metric_score_adjustments(
        metric=metric,
        q=q,
        score=score,
        deps=SCORE_ADJUSTMENT_DEPS,
    )

    return RouteCandidate(
        metric=metric, score=max(score, 0.0), matched_terms=matched_terms
    )


def score_routes(q: str) -> List[RouteCandidate]:
    candidates = [
        score_metric(q, metric, keywords) for metric, keywords in ROUTE_MAP.items()
    ]
    candidates = [c for c in candidates if c.score > 0]
    candidates.sort(key=lambda x: x.score, reverse=True)
    return candidates


WINDOW_POLICY_DEPS = WindowPolicyDeps(
    get_metric_capability=get_metric_capability,
    wants_seasonal_norm_comparison=wants_seasonal_norm_comparison,
    route_weather_normal_years=route_weather_normal_years,
    allowed_weather_normal_years=ALLOWED_WEATHER_NORMAL_YEARS,
)


FILTER_RESOLVER_DEPS = FilterResolverDeps(
    route_iso=route_iso,
    route_storage_region=route_storage_region,
    wants_storage_level_and_change=wants_storage_level_and_change,
    wants_regional_grouping=wants_regional_grouping,
    wants_storage_ranking_by_region=wants_storage_ranking_by_region,
    route_export_region=route_export_region,
    route_import_region=route_import_region,
    route_consumption_state=route_consumption_state,
    route_production_state=route_production_state,
    resolve_ng_electricity_normal_years=lambda q: resolve_window_normal_years(
        metric="ng_electricity",
        q=q,
        deps=WINDOW_POLICY_DEPS,
    ),
    route_reserves_state=route_reserves_state,
    route_reserves_resource_category=route_reserves_resource_category,
    route_pipeline_dataset=route_pipeline_dataset,
    route_weather_region=route_weather_region,
    route_weather_normal_years=route_weather_normal_years,
    allowed_weather_normal_years=ALLOWED_WEATHER_NORMAL_YEARS,
)

SCORE_ADJUSTMENT_DEPS = ScoreAdjustmentDeps(
    route_consumption_state=route_consumption_state,
    route_production_state=route_production_state,
    route_reserves_state=route_reserves_state,
    route_reserves_resource_category=route_reserves_resource_category,
    route_import_region=route_import_region,
    route_export_region=route_export_region,
)


def build_filters(metric: str, q: str, confidence: float) -> Optional[Dict[str, Any]]:
    return build_metric_filters(
        metric=metric,
        q=q,
        confidence=confidence,
        deps=FILTER_RESOLVER_DEPS,
    )


def candidate_confidence(candidates: List[RouteCandidate]) -> float:
    if not candidates:
        return 0.0
    top = candidates[0].score
    second = candidates[1].score if len(candidates) > 1 else 0.0

    # simple heuristic: strong top score + clear gap improves confidence
    conf = min(0.98, 0.15 * top + 0.08 * max(top - second, 0))
    return round(conf, 3)


def is_ambiguous(candidates: List[RouteCandidate]) -> bool:
    if not candidates:
        return True
    if len(candidates) == 1:
        # A lone metric hit should not require LLM fallback unless the match is
        # extremely weak. Otherwise common single-term questions like
        # "Is production growing year over year?" can be misrouted as unsupported.
        return candidates[0].score < 1.5

    top = candidates[0].score
    second = candidates[1].score

    # ambiguous if the best and second-best are very close
    return (top < 3.0) or ((top - second) <= 1.0)


# ----------------------------
# LLM hook contract
# ----------------------------
@dataclass(frozen=True)
class LLMRouteOutput:
    intent: str
    primary_metric: Optional[str]
    metrics: List[str]
    filters: Optional[Dict[str, Any]]
    reason: Optional[str]
    confidence: float
    ambiguous: bool


def llm_route_structured(user_query: str, normalized_query: str) -> LLMRouteOutput:

    try:
        return llm_route_structured_impl(
            user_query=user_query, normalized_query=normalized_query
        )
    except LLMRouterError as err:
        return LLMRouteOutput(
            intent="unsupported",
            primary_metric=None,
            metrics=[],
            filters=None,
            reason=f"LLM router error: {err}",
            confidence=0.0,
            ambiguous=False,
        )


def validate_llm_route(
    llm: LLMRouteOutput,
    start: str,
    end: str,
    normalized_query: str,
) -> HybridRouteResult:
    metrics = [m for m in llm.metrics if m in ALLOWED_METRICS]

    primary_metric = (
        llm.primary_metric if llm.primary_metric in ALLOWED_METRICS else None
    )
    if primary_metric and primary_metric not in metrics:
        metrics = [primary_metric] + metrics

    filters = dict(llm.filters or {})

    if "iso" in filters and filters["iso"] not in ALLOWED_ISOS:
        filters.pop("iso")
    if "region" in filters:
        region = filters["region"]
        if primary_metric and primary_metric.startswith("iso_"):
            filters.pop("region", None)
        elif primary_metric in {
            "working_gas_storage_lower48",
            "working_gas_storage_change_weekly",
        }:
            if region not in ALLOWED_STORAGE_REGIONS:
                filters.pop("region", None)
        elif primary_metric == "lng_exports":
            if region not in ALLOWED_EXPORT_REGIONS:
                filters.pop("region", None)
        elif primary_metric == "lng_imports":
            if region not in ALLOWED_IMPORT_REGIONS:
                filters.pop("region", None)
        elif primary_metric == "ng_consumption_lower48":
            if region not in ALLOWED_CONSUMPTION_STATES:
                filters.pop("region", None)
        elif primary_metric == "ng_production_lower48":
            if region not in ALLOWED_PRODUCTION_STATES:
                filters.pop("region", None)
        elif primary_metric == "ng_exploration_reserves_lower48":
            if region not in ALLOWED_RESERVES_STATES:
                filters.pop("region", None)
        elif primary_metric == "weather_degree_days_forecast_vs_5y":
            if region not in ALLOWED_WEATHER_REGIONS:
                filters.pop("region", None)
        else:
            filters.pop("region", None)
    if "resource_category" in filters:
        resource_category = filters["resource_category"]
        if primary_metric != "ng_exploration_reserves_lower48":
            filters.pop("resource_category", None)
        elif resource_category not in ALLOWED_RESERVES_RESOURCE_CATEGORIES:
            filters.pop("resource_category", None)
    if "dataset" in filters:
        dataset = filters["dataset"]
        if primary_metric != "ng_pipeline":
            filters.pop("dataset", None)
        elif dataset not in ALLOWED_PIPELINE_DATASETS:
            filters.pop("dataset", None)
    if "normal_years" in filters:
        normal_years = filters["normal_years"]
        if primary_metric != "weather_degree_days_forecast_vs_5y":
            filters.pop("normal_years", None)
        else:
            try:
                parsed_years = int(normal_years)
            except (TypeError, ValueError):
                filters.pop("normal_years", None)
            else:
                if parsed_years in ALLOWED_WEATHER_NORMAL_YEARS:
                    filters["normal_years"] = parsed_years
                else:
                    filters.pop("normal_years", None)

    if llm.intent not in {
        "single_metric",
        "compare",
        "ranking",
        "derived",
        "explain",
        "ambiguous",
        "unsupported",
    }:
        intent = "unsupported"
    else:
        intent = llm.intent

    if not metrics and primary_metric is None:
        intent = "unsupported"

    return HybridRouteResult(
        intent=intent,
        primary_metric=primary_metric,
        metrics=metrics,
        start=start,
        end=end,
        filters=filters or None,
        confidence=max(0.0, min(1.0, float(llm.confidence))),
        ambiguous=bool(llm.ambiguous),
        candidates=[],
        source="llm",
        reason=llm.reason,
        normalized_query=normalized_query,
        include_forecast=detect_forecast_request(normalized_query),
        forecast_horizon_days=detect_forecast_horizon_days(normalized_query),
    )


# ----------------------------
# Main hybrid router
# ----------------------------
def route_query(user_query: str) -> HybridRouteResult:

    normalized = normalize_query(user_query)
    start, end = resolve_date_range(user_query)
    has_explicit_dates = has_explicit_date_reference(user_query)
    intent = detect_intent(normalized)
    include_forecast = detect_forecast_request(normalized)
    forecast_horizon_days = detect_forecast_horizon_days(normalized)
    current_like_only = is_current_like_without_explicit_window(normalized)

    candidates = score_routes(normalized)
    confidence = candidate_confidence(candidates)
    ambiguous = is_ambiguous(candidates)

    has_import = bool(re.search(r"\bimports?\b", normalized))
    has_export = bool(re.search(r"\bexports?\b", normalized))
    has_reserves = bool(re.search(r"\breserves?\b", normalized))

    if has_reserves:
        metric = "ng_exploration_reserves_lower48"
        reserves_start = "2000-01-01" if not has_explicit_dates else start
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=reserves_start,
            end=end,
            filters=build_filters(metric, normalized, 1.0),
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for implied natural-gas reserves query",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if has_import and not has_export:
        metric = "lng_imports"
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=start,
            end=end,
            filters=build_filters(metric, normalized, 1.0),
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for implied natural-gas imports query",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if has_export and not has_import:
        metric = "lng_exports"
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=start,
            end=end,
            filters=build_filters(metric, normalized, 1.0),
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for implied natural-gas exports query",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if is_weekly_energy_atlas_summary_query(normalized):
        summary_start = start
        if not has_explicit_dates:
            summary_start = (
                pd.Timestamp(end) - pd.DateOffset(years=1)
            ).date().isoformat()
        metric = "weekly_energy_atlas_summary"
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=summary_start,
            end=end,
            filters=None,
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for Energy Atlas weekly multi-factor summary",
            normalized_query=normalized,
            include_forecast=False,
            forecast_horizon_days=None,
        )

    # Deterministic domain fast-paths for frequent electricity + gas asks.
    if is_power_burn_seasonal_question(normalized):
        metric = "ng_electricity"
        fast_start = start
        lookback_years = resolve_window_lookback_years(
            metric=metric,
            q=normalized,
            has_explicit_dates=has_explicit_dates,
            current_like_only=current_like_only,
            deps=WINDOW_POLICY_DEPS,
        )
        if lookback_years is not None:
            fast_start = (
                pd.Timestamp(end) - pd.DateOffset(years=lookback_years)
            ).date().isoformat()
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=fast_start,
            end=end,
            filters=build_filters(metric, normalized, 1.0),
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for natural gas power burn seasonal comparison",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if is_iso_gas_share_question(normalized) or is_renewables_power_sector_demand_question(normalized):
        metric = "iso_gas_dependency"
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=start,
            end=end,
            filters=build_filters(metric, normalized, 1.0),
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for electricity generation gas-share dependency",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    # No rule candidate -> LLM fallback
    if not candidates:
        llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
        return validate_llm_route(
            llm, start=start, end=end, normalized_query=normalized
        )

    top = candidates[0]
    if not has_explicit_dates and top.metric == "ng_exploration_reserves_lower48":
        start = "2000-01-01"
    lookback_years = resolve_window_lookback_years(
        metric=top.metric,
        q=normalized,
        has_explicit_dates=has_explicit_dates,
        current_like_only=current_like_only,
        deps=WINDOW_POLICY_DEPS,
    )
    if lookback_years is not None:
        start = (pd.Timestamp(end) - pd.DateOffset(years=lookback_years)).date().isoformat()
    filters = build_filters(top.metric, normalized, confidence)

    if (
        top.metric in {"working_gas_storage_lower48", "working_gas_storage_change_weekly"}
        and filters
        and (
            filters.get("region") in ALLOWED_STORAGE_REGIONS
            or
            filters.get("group_by") == "region"
            or filters.get("include_weekly_change") is True
        )
    ):
        return HybridRouteResult(
            intent="single_metric" if filters.get("include_weekly_change") else intent,
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"Storage rule route on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if top.metric == "weather_degree_days_forecast_vs_5y" and confidence >= 0.6:
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"Weather degree-day rule route on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=True,
            forecast_horizon_days=15,
        )

    # Fast path: for single-metric questions, stay on rules unless the match is ambiguous.
    if intent == "single_metric" and not ambiguous:
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"Strong rule match on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if intent == "ranking" and top.metric == "ng_consumption_by_sector" and not ambiguous:
        return HybridRouteResult(
            intent="ranking",
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"Strong rule match on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    # If a non-single intent still has a very strong single top metric, keep rule routing.
    if intent in {"compare", "derived", "explain"} and not ambiguous and confidence >= 0.8:
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"High-confidence rule match on {top.metric} for {intent} phrasing",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    # Multi-metric or advanced intent -> LLM assist
    if intent in {"compare", "ranking", "derived", "explain"}:
        llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
        return validate_llm_route(
            llm, start=start, end=end, normalized_query=normalized
        )

    # Ambiguous rule route -> LLM assist
    if ambiguous:
        llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
        return validate_llm_route(
            llm, start=start, end=end, normalized_query=normalized
        )

    # Fallback deterministic return
    return HybridRouteResult(
        intent="single_metric",
        primary_metric=top.metric,
        metrics=[top.metric],
        start=start,
        end=end,
        filters=filters,
        confidence=confidence,
        ambiguous=ambiguous,
        candidates=candidates[:3],
        source="rule",
        reason=f"Fallback rule route to {top.metric}",
        normalized_query=normalized,
        include_forecast=include_forecast,
        forecast_horizon_days=forecast_horizon_days,
    )
