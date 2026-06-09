from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from agents.llm_query_parser import llm_parse_query
from agents.llm_router import (
    STORAGE_DATASETS,
    STORAGE_FREQUENCIES,
    STORAGE_METRIC_BY_VALUE_TYPE,
    STORAGE_METRIC_TYPES,
    STORAGE_REGIONS,
    STORAGE_TYPES,
    UNDERGROUND_STORAGE_BY_TYPE_METRIC_BY_TYPE_AND_FREQUENCY,
    UNDERGROUND_STORAGE_METRIC_BY_TYPE_AND_FREQUENCY,
    UNDERGROUND_STORAGE_STATES,
)
from utils.dates import resolve_date_range

NORMAL_DEVIATION_TERMS = (
    "deficit",
    "surplus",
    "above normal",
    "below normal",
    "above average",
    "below average",
    "tighter",
    "tight",
    "loose",
    "looser",
    "tightening",
    "loosening",
    "vs normal",
    "versus normal",
    "compared to normal",
    "storage gap",
    "inventory gap",
)

WEEKLY_CHANGE_TERMS = (
    "injection",
    "injections",
    "injected",
    "withdrawal",
    "withdrawals",
    "withdrawn",
    "build",
    "builds",
    "draw",
    "draws",
    "weekly change",
    "net change",
)

CHANGE_DIRECTION_TERMS = (
    "accelerating",
    "acceleration",
    "slowing",
    "decelerating",
    "shrinking",
    "widening",
    "growing",
    "increasing",
    "decreasing",
    "improving",
    "worsening",
)

TIME_SERIES_INFERENCE_TERMS = (
    "how has",
    "changed since",
    "over the last",
    "since",
    "trend",
    "history",
    "historical",
    "over time",
)

YOY_CUES = (
    "year ago",
    "from year ago",
    "year-over-year",
    "year over year",
    "yoy",
)

YOY_PERCENT_CUES = (
    "percent",
    "percentage",
    "pct",
    "%",
    "percent change",
    "percentage change",
    "pct change",
    "% change",
    "percent increase",
    "percentage increase",
    "pct increase",
    "% increase",
    "percent decrease",
    "percentage decrease",
    "pct decrease",
    "% decrease",
)

YOY_VOLUME_CHANGE_CUES = (
    "volume change",
    "change from year ago",
    "working gas change",
    "year-over-year increase",
    "year-over-year decrease",
    "year over year increase",
    "year over year decrease",
    "yoy increase",
    "yoy decrease",
    "increase from year ago",
    "decrease from year ago",
    "larger than year ago",
    "lower than year ago",
)

YOY_LATEST_CUES = (
    "what is",
    "current",
    "latest",
    "how much",
    "what was",
)

YOY_RANKING_CUES = (
    "which state",
    "rank states",
    "largest",
    "highest",
    "most",
    "biggest",
)

NORMAL_RANKING_TERMS = (
    "below normal",
    "above normal",
    "deficit",
    "surplus",
    "tight",
    "tightest",
    "loose",
    "loosest",
    "storage deficit",
    "storage surplus",
    "inventory deficit",
    "inventory surplus",
)

RANKING_INTENT_TERMS = (
    "which region",
    "which state",
    "rank",
    "ranking",
    "by region",
    "by state",
)

NATIONAL_STORAGE_TERMS = (
    "united states",
    "u.s.",
    "us total",
    "u.s. total",
    "national",
    "nationwide",
)

MONTHLY_STORAGE_TERMS = ("monthly", "month", "by month")
ANNUAL_STORAGE_TERMS = ("annual", "yearly", "by year")
ALL_OPERATORS_STORAGE_TERMS = (
    "monthly",
    "annual",
    "state",
    "by state",
    "compare states",
    "which state",
    "rank states",
    "base gas",
    "cushion gas",
    "total natural gas in storage",
    "total gas in storage",
    "total storage",
    "all operators",
    "net withdrawals",
    "net withdrawal",
    "net withdrawls",
    "withdrawals",
    "withdrawls",
    "withdrawal",
    "injections",
    "injected",
    "injection",
    "working gas volume change from year ago",
    "change from year ago",
    "year ago volume change",
    "yoy volume change",
    "working gas percent change from year ago",
    "working gas % change from year ago",
    "percent change from year ago",
    "% change from year ago",
    "yoy percent change",
    "yoy pct change",
)

BY_TYPE_STORAGE_TERMS = (
    "by type",
    "storage type",
    "storage types",
    "compare storage types",
    "rank storage types",
    "underground storage by type",
    "salt cavern",
    "salt-cavern",
    "salt storage",
    "depleted field",
    "depleted reservoir",
    "aquifer",
)

UNDERGROUND_STATE_ALIASES = {
    "al": ("alabama",),
    "ak": ("alaska",),
    "az": ("arizona",),
    "ar": ("arkansas",),
    "ca": ("california",),
    "co": ("colorado",),
    "ct": ("connecticut",),
    "de": ("delaware",),
    "fl": ("florida",),
    "ga": ("georgia",),
    "ia": ("iowa",),
    "id": ("idaho",),
    "il": ("illinois",),
    "in": ("indiana",),
    "ks": ("kansas",),
    "ky": ("kentucky",),
    "la": ("louisiana",),
    "ma": ("massachusetts",),
    "md": ("maryland",),
    "mi": ("michigan",),
    "mn": ("minnesota",),
    "ms": ("mississippi",),
    "mo": ("missouri",),
    "mt": ("montana",),
    "ne": ("nebraska",),
    "nv": ("nevada",),
    "nj": ("new jersey",),
    "nm": ("new mexico",),
    "ny": ("new york",),
    "oh": ("ohio",),
    "ok": ("oklahoma",),
    "or": ("oregon",),
    "pa": ("pennsylvania",),
    "tx": ("texas",),
    "ut": ("utah",),
    "va": ("virginia",),
    "wa": ("washington",),
    "wv": ("west virginia",),
    "wy": ("wyoming",),
    "united_states_total": ("united states", "u.s.", " us ", "u.s. total", "national"),
}


@dataclass(frozen=True)
class EnergyRouteResult:
    domain: str
    analysis_type: str
    primary_metric: Optional[str]
    metrics: list[str]
    storage_dataset: str
    storage_frequency: str
    storage_metric_type: str
    storage_type: Optional[str]
    storage_types_all: bool
    regions: list[str]
    states: list[str]
    states_all: bool
    start_date: Optional[str]
    end_date: Optional[str]
    date_expression: Optional[str]
    value_type: str
    comparisons: list[str]
    ranking_basis: str
    chart_type: str
    output_mode: str
    filters: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    ambiguous: bool = False
    reason: Optional[str] = None
    normalized_query: Optional[str] = None


def normalize_query(user_query: str) -> str:
    q = user_query.lower().strip()
    q = q.replace("’", "'").replace("–", "-").replace("—", "-")
    q = re.sub(r"\s+", " ", q).strip()
    return q


def _has_term(normalized_query: str, terms: tuple[str, ...]) -> bool:
    return any(term in normalized_query for term in terms)


def _append_comparison(comparisons: list[str], comparison: str) -> list[str]:
    cleaned = [comp for comp in comparisons if comp and comp != "none"]
    if comparison == "five_year_avg":
        cleaned = [comp for comp in cleaned if comp != "seasonal_normal"]
    if comparison not in cleaned:
        cleaned.append(comparison)
    return cleaned or ["none"]


def _is_national_storage_series(state: str) -> bool:
    return str(state or "").strip().lower() == "united_states_total"


def _has_national_storage_request(normalized_query: str) -> bool:
    return any(term in normalized_query for term in NATIONAL_STORAGE_TERMS)


def _parse_storage_frequency_from_text(normalized_query: str, parsed_frequency: str) -> str:
    if _has_term(normalized_query, ANNUAL_STORAGE_TERMS):
        return "annual"
    if _has_term(normalized_query, MONTHLY_STORAGE_TERMS):
        return "monthly"
    return parsed_frequency or "weekly"


def _parse_storage_type_from_text(normalized_query: str) -> tuple[Optional[str], bool]:
    if any(term in normalized_query for term in ("compare storage types", "rank storage types")):
        return None, True
    if any(
        term in normalized_query
        for term in ("by type", "storage type", "storage types", "underground storage by type")
    ):
        return None, True
    if any(term in normalized_query for term in ("salt cavern", "salt-cavern", "salt storage", "salt")):
        return "salt_cavern", False
    if any(term in normalized_query for term in ("depleted field", "depleted reservoir", "depleted")):
        return "depleted_field", False
    if "aquifer" in normalized_query:
        return "aquifer", False
    return None, False


def _parse_storage_metric_type_from_text(
    normalized_query: str,
    parsed_metric_type: str,
) -> str:
    if any(
        term in normalized_query
        for term in (
            "working gas percent change from year ago",
            "working gas % change from year ago",
            "percent change from year ago",
            "% change from year ago",
            "yoy percent change",
            "yoy pct change",
        )
    ):
        return "working_gas_yoy_pct_change"
    if any(
        term in normalized_query
        for term in (
            "working gas volume change from year ago",
            "change from year ago",
            "year ago volume change",
            "yoy volume change",
        )
    ):
        return "working_gas_yoy_volume_change"
    if any(term in normalized_query for term in ("net withdrawals", "net withdrawal", "net withdrawls")):
        return "net_withdrawals"
    if any(term in normalized_query for term in ("withdrawals", "withdrawls", "withdrawn", "withdrawal")):
        return "withdrawals"
    if any(term in normalized_query for term in ("injections", "injected", "injection")):
        return "injections"
    if any(term in normalized_query for term in ("base gas", "cushion gas")):
        return "base_gas"
    if any(term in normalized_query for term in ("natural gas in storage", "total gas in storage", "total storage")):
        return "total_gas"
    return parsed_metric_type or "working_gas"


def _has_explicit_time_series_request(normalized_query: str) -> bool:
    if _has_term(normalized_query, TIME_SERIES_INFERENCE_TERMS):
        return True
    if re.search(r"\bfrom\s+(?:[a-z]+\s+)?20\d{2}\b", normalized_query):
        return True
    return bool(re.search(r"\b20\d{2}\b", normalized_query))


def infer_underground_storage_yoy_metric_type(
    normalized_query: str,
    storage_dataset: str,
    current_metric_type: str,
) -> str:
    if storage_dataset != "underground_storage_all_operators":
        return current_metric_type

    has_yoy_cue = _has_term(normalized_query, YOY_CUES)
    has_percent_cue = _has_term(normalized_query, YOY_PERCENT_CUES)
    has_volume_change_cue = _has_term(normalized_query, YOY_VOLUME_CHANGE_CUES)
    has_working_gas = "working gas" in normalized_query
    has_ranking_cue = _has_term(normalized_query, YOY_RANKING_CUES)
    has_percent_increase_decrease = has_percent_cue and any(
        term in normalized_query for term in ("increase", "decrease", "change")
    )

    if has_percent_cue and has_yoy_cue:
        return "working_gas_yoy_pct_change"
    if has_working_gas and has_percent_increase_decrease and has_ranking_cue:
        return "working_gas_yoy_pct_change"
    if has_yoy_cue and has_volume_change_cue and not has_percent_cue:
        return "working_gas_yoy_volume_change"
    if has_working_gas and has_yoy_cue and not has_percent_cue:
        return "working_gas_yoy_volume_change"
    return current_metric_type


def _is_yoy_storage_metric(storage_metric_type: str) -> bool:
    return storage_metric_type in {
        "working_gas_yoy_volume_change",
        "working_gas_yoy_pct_change",
    }


def _parse_states_from_text(normalized_query: str) -> list[str]:
    if _has_national_storage_request(normalized_query):
        return ["united_states_total"]
    padded = f" {normalized_query} "
    matches: list[tuple[int, str]] = []
    for state, aliases in UNDERGROUND_STATE_ALIASES.items():
        positions = [padded.find(alias) for alias in aliases if alias in padded]
        if positions and state in UNDERGROUND_STORAGE_STATES:
            matches.append((min(positions), state))
    return [state for _, state in sorted(matches, key=lambda item: item[0])]


def _resolve_storage_dataset(
    normalized_query: str,
    *,
    parsed_dataset: str,
    storage_frequency: str,
    storage_metric_type: str,
    states: list[str],
    states_all: bool,
    regions: list[str],
) -> str:
    has_explicit_weekly = any(
        term in normalized_query
        for term in ("this week", "weekly", "weekly storage report", "weekly report")
    )
    has_weekly_change_language = _has_term(normalized_query, WEEKLY_CHANGE_TERMS)
    has_change_direction = _has_term(normalized_query, CHANGE_DIRECTION_TERMS)
    if states or states_all:
        return "underground_storage_all_operators"
    if _has_term(normalized_query, BY_TYPE_STORAGE_TERMS):
        return "underground_storage_by_type"
    if has_weekly_change_language and (
        has_explicit_weekly
        or has_change_direction
        or "by region" in normalized_query
        or any(region in regions for region in STORAGE_REGIONS if region != "lower48")
    ):
        return "weekly_working_gas"
    if has_explicit_weekly and storage_frequency == "weekly":
        return "weekly_working_gas"
    if storage_frequency in {"monthly", "annual"}:
        return "underground_storage_all_operators"
    if storage_metric_type != "working_gas":
        return "weekly_working_gas" if has_explicit_weekly else "underground_storage_all_operators"
    if _has_term(normalized_query, ALL_OPERATORS_STORAGE_TERMS):
        if not any(region in regions for region in STORAGE_REGIONS if region != "lower48"):
            return "underground_storage_all_operators"
    if parsed_dataset in STORAGE_DATASETS:
        return parsed_dataset
    return "weekly_working_gas"


def infer_storage_analysis_type_from_text(
    normalized_query: str,
    current_analysis_type: str,
    value_type: str,
    comparisons: list[str],
) -> tuple[str, str, list[str], str, Optional[str], Optional[str]]:
    analysis_type = current_analysis_type
    resolved_value_type = value_type
    resolved_comparisons = list(comparisons or ["none"])
    ranking_basis = "current_storage"
    resolved_chart_type: Optional[str] = None
    resolved_output_mode: Optional[str] = None

    has_normal_deviation = _has_term(normalized_query, NORMAL_DEVIATION_TERMS)
    has_normal_ranking = _has_term(normalized_query, NORMAL_RANKING_TERMS)
    has_ranking_intent = _has_term(normalized_query, RANKING_INTENT_TERMS)
    has_weekly_change = _has_term(normalized_query, WEEKLY_CHANGE_TERMS)
    has_change_direction = _has_term(normalized_query, CHANGE_DIRECTION_TERMS)
    has_time_series_inference = _has_explicit_time_series_request(normalized_query)

    if analysis_type == "ranking" and has_normal_ranking and has_ranking_intent:
        resolved_comparisons = _append_comparison(resolved_comparisons, "five_year_avg")
        ranking_basis = "deviation_from_normal"
        resolved_chart_type = "bar"
        resolved_output_mode = "chart_and_answer"
        return (
            analysis_type,
            resolved_value_type,
            resolved_comparisons,
            ranking_basis,
            resolved_chart_type,
            resolved_output_mode,
        )

    if analysis_type == "ranking":
        return (
            analysis_type,
            resolved_value_type,
            resolved_comparisons,
            ranking_basis,
            resolved_chart_type,
            resolved_output_mode,
        )

    if has_weekly_change and has_change_direction:
        analysis_type = "weekly_change"
        resolved_value_type = "weekly_change"
        resolved_comparisons = _append_comparison(resolved_comparisons, "prior_week")
        resolved_chart_type = "line"
        resolved_output_mode = "chart_and_answer"
        return (
            analysis_type,
            resolved_value_type,
            resolved_comparisons,
            ranking_basis,
            resolved_chart_type,
            resolved_output_mode,
        )

    if has_normal_deviation:
        analysis_type = "deviation_from_normal"
        if has_weekly_change:
            resolved_value_type = "weekly_change"
        resolved_comparisons = _append_comparison(resolved_comparisons, "five_year_avg")

    if has_normal_ranking and has_ranking_intent:
        analysis_type = "ranking"
        ranking_basis = "deviation_from_normal"
        resolved_comparisons = _append_comparison(resolved_comparisons, "five_year_avg")
        resolved_chart_type = "bar"
        resolved_output_mode = "chart_and_answer"

    if has_time_series_inference and not has_weekly_change:
        resolved_value_type = "level"

    if (
        resolved_value_type == "level"
        and has_time_series_inference
        and analysis_type in {"latest", "time_series", "unsupported"}
    ):
        analysis_type = "time_series"
        resolved_chart_type = "line"
        resolved_output_mode = "chart_and_answer"

    return (
        analysis_type,
        resolved_value_type,
        resolved_comparisons,
        ranking_basis,
        resolved_chart_type,
        resolved_output_mode,
    )


def _metrics_for_route(domain: str, value_type: str) -> tuple[Optional[str], list[str]]:
    if domain != "storage":
        return None, []
    metric = STORAGE_METRIC_BY_VALUE_TYPE.get(value_type)
    return metric, [metric] if metric else []


def _storage_metrics_for_route(
    *,
    storage_dataset: str,
    storage_frequency: str,
    storage_metric_type: str,
    value_type: str,
) -> tuple[Optional[str], list[str]]:
    if storage_dataset == "underground_storage_all_operators":
        metric = UNDERGROUND_STORAGE_METRIC_BY_TYPE_AND_FREQUENCY.get(
            (storage_metric_type, storage_frequency)
        )
        return metric, [metric] if metric else []
    if storage_dataset == "underground_storage_by_type":
        metric = UNDERGROUND_STORAGE_BY_TYPE_METRIC_BY_TYPE_AND_FREQUENCY.get(
            (storage_metric_type, storage_frequency)
        )
        return metric, [metric] if metric else []
    metric = STORAGE_METRIC_BY_VALUE_TYPE.get(value_type)
    return metric, [metric] if metric else []


def _filters_for_route(
    domain: str,
    *,
    storage_dataset: str,
    storage_frequency: str,
    storage_metric_type: str,
    storage_type: str | None,
    storage_types_all: bool,
    regions: list[str],
    states: list[str],
    states_all: bool,
) -> dict[str, Any]:
    if domain != "storage":
        return {}
    if storage_dataset == "underground_storage_by_type":
        return {
            "storage_dataset": storage_dataset,
            "storage_frequency": storage_frequency,
            "storage_metric_type": storage_metric_type,
            "storage_type": storage_type,
            "storage_types_all": storage_types_all,
        }
    if storage_dataset == "underground_storage_all_operators":
        return {
            "states": states,
            "states_all": states_all,
            "storage_dataset": storage_dataset,
            "storage_frequency": storage_frequency,
            "storage_metric_type": storage_metric_type,
        }
    return {
        "regions": regions,
        "storage_dataset": storage_dataset,
        "storage_frequency": storage_frequency,
        "storage_metric_type": storage_metric_type,
    }


def route_query(user_query: str) -> EnergyRouteResult:
    normalized = normalize_query(user_query)
    national_storage_request = _has_national_storage_request(normalized)
    start_date, end_date = resolve_date_range(user_query)
    parsed = llm_parse_query(user_query=user_query, normalized_query=normalized)

    regions = list(parsed.regions or [])
    states = [state for state in list(getattr(parsed, "states", []) or []) if state in UNDERGROUND_STORAGE_STATES]
    states_all = bool(getattr(parsed, "states_all", False))
    storage_type = getattr(parsed, "storage_type", None)
    if storage_type not in STORAGE_TYPES:
        storage_type = None
    storage_types_all = bool(getattr(parsed, "storage_types_all", False))
    storage_frequency = _parse_storage_frequency_from_text(
        normalized, getattr(parsed, "storage_frequency", "weekly")
    )
    storage_metric_type = _parse_storage_metric_type_from_text(
        normalized, getattr(parsed, "storage_metric_type", "working_gas")
    )
    states_from_text = _parse_states_from_text(normalized)
    if states_from_text:
        states = states_from_text
        states_all = False
    if parsed.domain == "storage":
        regions = [region for region in regions if region in STORAGE_REGIONS]
        storage_type_from_text, storage_types_all_from_text = _parse_storage_type_from_text(normalized)
        if storage_type_from_text in STORAGE_TYPES:
            storage_type = storage_type_from_text
            storage_types_all = False
        elif storage_types_all_from_text:
            storage_type = None
            storage_types_all = True
        storage_dataset = _resolve_storage_dataset(
            normalized,
            parsed_dataset=getattr(parsed, "storage_dataset", "weekly_working_gas"),
            storage_frequency=storage_frequency,
            storage_metric_type=storage_metric_type,
            states=states,
            states_all=states_all,
            regions=regions,
        )
        if storage_dataset == "underground_storage_all_operators":
            regions = []
            storage_type = None
            storage_types_all = False
            if storage_frequency not in {"monthly", "annual"}:
                storage_frequency = "monthly"
            if national_storage_request:
                states = ["united_states_total"]
                states_all = False
            elif not states and any(
                term in normalized for term in ("by state", "compare states", "rank states", "which state", "all states")
            ):
                states_all = True
            storage_metric_type = infer_underground_storage_yoy_metric_type(
                normalized,
                storage_dataset,
                storage_metric_type,
            )
        elif storage_dataset == "underground_storage_by_type":
            regions = []
            states = []
            states_all = False
            if storage_frequency not in {"monthly", "annual"}:
                storage_frequency = "monthly"
        else:
            storage_dataset = "weekly_working_gas"
            storage_frequency = "weekly"
            storage_metric_type = "working_gas"
            storage_type = None
            storage_types_all = False
            states = []
            states_all = False
            if not regions:
                regions = ["lower48"]
            regions = [region for region in regions if region in STORAGE_REGIONS] or ["lower48"]
    else:
        storage_dataset = "weekly_working_gas"
        storage_frequency = "weekly"
        storage_metric_type = "working_gas"
        storage_type = None
        storage_types_all = False

    analysis_type = parsed.analysis_type
    value_type = parsed.value_type
    comparisons = list(parsed.comparisons or ["none"])
    chart_type = parsed.chart_type
    output_mode = parsed.output_mode
    ranking_basis = "current_storage"
    if parsed.domain == "storage":
        if storage_dataset == "weekly_working_gas":
            (
                analysis_type,
                value_type,
                comparisons,
                ranking_basis,
                inferred_chart_type,
                inferred_output_mode,
            ) = infer_storage_analysis_type_from_text(
                normalized_query=normalized,
                current_analysis_type=analysis_type,
                value_type=value_type,
                comparisons=comparisons,
            )
            if inferred_chart_type:
                chart_type = inferred_chart_type
            if inferred_output_mode:
                output_mode = inferred_output_mode
            if analysis_type in {"ranking", "regional_compare"} and (
                not regions
                or regions == ["lower48"]
                or regions == list(STORAGE_REGIONS[:1])
            ):
                if _has_term(normalized, RANKING_INTENT_TERMS):
                    regions = list(STORAGE_REGIONS)
        else:
            value_type = "level"
            if storage_dataset == "underground_storage_by_type" and _is_yoy_storage_metric(storage_metric_type):
                comparisons = ["none"]
                analysis_type = "unsupported"
                chart_type = "none"
                output_mode = "answer"
            elif _is_yoy_storage_metric(storage_metric_type):
                comparisons = ["none"]
                has_yoy_ranking = _has_term(normalized, YOY_RANKING_CUES)
                has_yoy_time_series = _has_explicit_time_series_request(normalized)
                has_yoy_latest = _has_term(normalized, YOY_LATEST_CUES)
                if has_yoy_ranking:
                    analysis_type = "ranking"
                    chart_type = "bar"
                    output_mode = "chart_and_answer"
                    states_all = True
                    states = []
                elif has_yoy_time_series:
                    analysis_type = "time_series"
                    chart_type = "line"
                    output_mode = "chart_and_answer"
                elif has_yoy_latest:
                    analysis_type = "latest"
                    chart_type = "none"
                    output_mode = "answer"
            if (
                storage_dataset == "underground_storage_all_operators"
                and analysis_type in {"ranking", "regional_compare"}
                and not states
                and not national_storage_request
            ):
                states_all = True
            if national_storage_request:
                states = ["united_states_total"]
                states_all = False
            has_explicit_time_series = _has_explicit_time_series_request(normalized)
            if storage_dataset == "underground_storage_by_type" and _is_yoy_storage_metric(storage_metric_type):
                pass
            elif _is_yoy_storage_metric(storage_metric_type):
                pass
            elif has_explicit_time_series:
                analysis_type = "time_series"
                chart_type = "line"
                output_mode = "chart_and_answer"
            elif analysis_type in {"ranking", "regional_compare"}:
                chart_type = "bar"
                output_mode = "chart_and_answer"
            elif chart_type == "bar":
                output_mode = "chart_and_answer"
        if analysis_type in {"ranking", "regional_compare"} and chart_type == "bar":
            output_mode = "chart_and_answer"
        if (
            storage_dataset == "underground_storage_all_operators"
            and analysis_type in {"ranking", "regional_compare"}
            and not national_storage_request
        ):
            states = [state for state in states if not _is_national_storage_series(state)]
            if not states:
                states_all = True
        if storage_dataset == "underground_storage_by_type":
            comparisons = ["none"]
            if analysis_type == "latest":
                chart_type = "none"
                output_mode = "answer"
            elif analysis_type in {"ranking", "regional_compare"}:
                chart_type = "bar"
                output_mode = "chart_and_answer"
            elif analysis_type == "time_series":
                chart_type = "line"
                output_mode = "chart_and_answer"

    primary_metric, metrics = _storage_metrics_for_route(
        storage_dataset=storage_dataset,
        storage_frequency=storage_frequency,
        storage_metric_type=storage_metric_type,
        value_type=value_type,
    ) if parsed.domain == "storage" else _metrics_for_route(parsed.domain, value_type)

    if parsed.domain == "storage" and analysis_type == "unsupported":
        primary_metric = None
        metrics = []

    return EnergyRouteResult(
        domain=parsed.domain,
        analysis_type=analysis_type,
        primary_metric=primary_metric,
        metrics=metrics,
        storage_dataset=storage_dataset,
        storage_frequency=storage_frequency,
        storage_metric_type=storage_metric_type,
        storage_type=storage_type,
        storage_types_all=storage_types_all,
        regions=regions,
        states=states,
        states_all=states_all,
        start_date=start_date,
        end_date=end_date,
        date_expression=parsed.date_expression,
        value_type=value_type,
        comparisons=comparisons,
        ranking_basis=ranking_basis,
        chart_type=chart_type,
        output_mode=output_mode,
        filters=_filters_for_route(
            parsed.domain,
            storage_dataset=storage_dataset,
            storage_frequency=storage_frequency,
            storage_metric_type=storage_metric_type,
            storage_type=storage_type,
            storage_types_all=storage_types_all,
            regions=regions,
            states=states,
            states_all=states_all,
        ),
        confidence=parsed.confidence,
        ambiguous=parsed.ambiguous,
        reason=parsed.reason,
        normalized_query=normalized,
    )
