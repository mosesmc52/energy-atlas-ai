from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from agents.llm_router import (
    CHART_TYPES,
    COMPARISONS,
    OUTPUT_MODES,
    STORAGE_ANALYSIS_TYPES,
    STORAGE_DATASETS,
    STORAGE_FREQUENCIES,
    STORAGE_METRIC_TYPES,
    STORAGE_REGIONS,
    UNDERGROUND_STORAGE_STATES,
    VALUE_TYPES,
)


@dataclass(frozen=True)
class EnergyQueryParse:
    domain: str
    analysis_type: str
    storage_dataset: str = "weekly_working_gas"
    storage_frequency: str = "weekly"
    storage_metric_type: str = "working_gas"
    regions: list[str] = field(default_factory=list)
    states: list[str] = field(default_factory=list)
    states_all: bool = False
    value_type: str = "level"
    comparisons: list[str] = field(default_factory=lambda: ["none"])
    chart_type: str = "none"
    output_mode: str = "answer"
    date_expression: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    filters: dict = field(default_factory=dict)
    confidence: float = 0.0
    ambiguous: bool = False
    reason: Optional[str] = None


STORAGE_TERMS = (
    "storage",
    "working gas",
    "inventor",
    "inventories",
)

NON_STORAGE_NATGAS_TERMS = (
    "henry hub",
    "price",
    "production",
    "output",
    "lng",
    "export",
    "exports",
    "import",
    "imports",
    "consumption",
    "demand",
    "weather",
    "hdd",
    "cdd",
    "power",
)

WEEKLY_CHANGE_TERMS = (
    "injection",
    "injections",
    "injected",
    "withdrawal",
    "withdrawals",
    "withdrew",
    "build",
    "draw",
    "change",
)

REGION_ALIASES = {
    "lower48": ("lower 48", "lower48", "u.s.", "us ", "united states"),
    "east": ("east", "eastern"),
    "midwest": ("midwest", "mid-west"),
    "mountain": ("mountain",),
    "pacific": ("pacific",),
    "south_central": ("south central", "south-central"),
    "south_central_salt": ("salt", "salt cavern", "south central salt"),
    "south_central_nonsalt": ("nonsalt", "non-salt", "non salt", "south central nonsalt"),
}

STATE_ALIASES = {
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
    "united_states_total": ("united states", "u.s.", " us ", "national", "u.s. total"),
}

MONTHLY_FREQUENCY_TERMS = ("monthly", "month", "by month")
ANNUAL_FREQUENCY_TERMS = ("annual", "yearly", "by year")

WEEKLY_STORAGE_TERMS = (
    "weekly storage",
    "working gas weekly",
    "weekly injection",
    "weekly withdrawal",
    "weekly storage report",
    "weekly report",
    "storage regions",
)

ALL_OPERATORS_TERMS = (
    "monthly",
    "annual",
    "state",
    "by state",
    "compare states",
    "rank states",
    "which state",
    "base gas",
    "cushion gas",
    "total natural gas in storage",
    "natural gas in storage",
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
    "year ago",
    "yoy",
    "percent change",
    "% change",
)


def _contains_any(q: str, terms: tuple[str, ...]) -> bool:
    return any(term in q for term in terms)


def _classify_domain(q: str) -> tuple[str, str, float]:
    if _contains_any(q, STORAGE_TERMS):
        return "storage", "Storage language detected.", 0.9
    if _contains_any(q, WEEKLY_CHANGE_TERMS):
        return "storage", "Storage weekly-change language detected.", 0.78
    if "gas" in q and ("region" in q or _contains_any(q, tuple(alias for aliases in REGION_ALIASES.values() for alias in aliases))):
        return "storage", "Storage region language detected.", 0.74
    if "region" in q and any(term in q for term in ("normal", "above", "below")):
        return "storage", "Regional storage comparison language detected.", 0.72
    if any(term in q for term in ("seasonal average", "seasonal normal", "versus the seasonal", "vs the seasonal")):
        return "storage", "Storage seasonal comparison language detected.", 0.72
    if _contains_any(q, NON_STORAGE_NATGAS_TERMS):
        return "unsupported", "Only the storage domain is active right now.", 0.9
    return "unsupported", "No supported storage domain language detected.", 0.7


def _parse_regions(q: str) -> list[str]:
    if _asks_all_regions(q):
        return list(STORAGE_REGIONS)

    regions: list[str] = []
    padded = f" {q} "
    for region, aliases in REGION_ALIASES.items():
        if any(alias in padded for alias in aliases) and region not in regions:
            regions.append(region)
    return regions


def _parse_states(q: str) -> list[str]:
    matches: list[tuple[int, str]] = []
    padded = f" {q} "
    for state, aliases in STATE_ALIASES.items():
        positions = [padded.find(alias) for alias in aliases if alias in padded]
        if positions:
            matches.append((min(positions), state))
    return [state for _, state in sorted(matches, key=lambda item: item[0])]


def _asks_all_regions(q: str) -> bool:
    all_region_phrases = (
        "by region",
        "all regions",
        "across regions",
        "compare regions",
        "which region",
        "rank",
        "ranking",
    )
    return any(phrase in q for phrase in all_region_phrases)


def _asks_all_states(q: str) -> bool:
    phrases = (
        "by state",
        "all states",
        "across states",
        "compare states",
        "which state",
        "rank states",
        "ranking states",
    )
    return any(phrase in q for phrase in phrases)


def _parse_storage_frequency(q: str) -> str:
    if _contains_any(q, ANNUAL_FREQUENCY_TERMS):
        return "annual"
    if _contains_any(q, MONTHLY_FREQUENCY_TERMS):
        return "monthly"
    return "weekly"


def _parse_storage_metric_type(q: str) -> str:
    if any(term in q for term in ("working gas percent change from year ago", "working gas % change from year ago", "percent change from year ago", "% change from year ago", "yoy percent change", "yoy pct change")):
        return "working_gas_yoy_pct_change"
    if any(term in q for term in ("working gas volume change from year ago", "change from year ago", "year ago volume change", "yoy volume change")):
        return "working_gas_yoy_volume_change"
    if any(term in q for term in ("net withdrawals", "net withdrawal", "net withdrawls")):
        return "net_withdrawals"
    if any(term in q for term in ("withdrawals", "withdrawls", "withdrawn", "withdrawal")):
        return "withdrawals"
    if any(term in q for term in ("injections", "injected", "injection")):
        return "injections"
    if any(term in q for term in ("base gas", "cushion gas")):
        return "base_gas"
    if any(term in q for term in ("natural gas in storage", "total gas in storage", "total storage")):
        return "total_gas"
    return "working_gas"


def _parse_storage_dataset(q: str, *, frequency: str, states: list[str], regions: list[str], metric_type: str) -> str:
    has_weekly_terms = _contains_any(q, WEEKLY_STORAGE_TERMS)
    has_all_operator_terms = _contains_any(q, ALL_OPERATORS_TERMS)
    has_state_terms = bool(states) or _asks_all_states(q)
    has_weekly_regions = bool(regions) and any(region != "lower48" for region in regions)

    if has_state_terms:
        return "underground_storage_all_operators"
    if frequency in {"monthly", "annual"}:
        return "underground_storage_all_operators"
    if metric_type != "working_gas":
        return "underground_storage_all_operators"
    if has_all_operator_terms and not has_weekly_terms:
        return "underground_storage_all_operators"
    if has_weekly_regions:
        return "weekly_working_gas"
    return "weekly_working_gas"


def _parse_value_type(q: str) -> str:
    if _contains_any(q, WEEKLY_CHANGE_TERMS):
        return "weekly_change"
    return "level"


def _parse_comparisons(q: str) -> list[str]:
    comparisons: list[str] = []
    if any(term in q for term in ("prior week", "previous week", "previous report", "last report", "accelerating")):
        comparisons.append("prior_week")
    if any(term in q for term in ("last year", "year ago", "same week last year")):
        comparisons.append("last_year")
    if any(term in q for term in ("5-year average", "5 year average", "five-year average", "five year average", "seasonal average")):
        comparisons.append("five_year_avg")
    if any(term in q for term in ("range", "band", "min/max", "min max", "five-year range", "5-year range")):
        comparisons.append("five_year_range")
    if any(term in q for term in ("normal", "seasonal", "same week history")) and "five_year_avg" not in comparisons:
        comparisons.append("seasonal_normal")
    return comparisons or ["none"]


def _parse_analysis_type(
    q: str,
    value_type: str,
    comparisons: list[str],
    regions: list[str],
    states: list[str],
    storage_dataset: str,
) -> str:
    if any(term in q for term in ("which region", "which state", "rank", "ranking", "most above", "most below")):
        return "ranking"
    if _asks_all_regions(q) or _asks_all_states(q):
        return "regional_compare"
    if len(regions) > 1 or len(states) > 1:
        return "time_series"
    if any(comp in comparisons for comp in ("five_year_avg", "five_year_range", "seasonal_normal", "last_year")):
        return "seasonal_compare"
    if any(term in q for term in ("plot", "chart", "trend", "over time", "since", "from ")) or re.search(r"\b20\d{2}\b", q):
        return "time_series"
    if storage_dataset == "weekly_working_gas" and value_type == "weekly_change":
        return "weekly_change"
    if any(term in q for term in ("why", "explain", "driver", "driving")):
        return "explain"
    return "latest"


def _parse_chart_type(q: str, analysis_type: str) -> str:
    explicit_chart = any(term in q for term in ("plot", "chart", "graph", "visualize"))
    if analysis_type == "time_series":
        return "line"
    if analysis_type in {"regional_compare", "ranking"}:
        return "bar"
    if analysis_type == "seasonal_compare":
        return "seasonal_line"
    if analysis_type == "weekly_change":
        return "line"
    if explicit_chart:
        return "line"
    return "none"


def _parse_output_mode(q: str, chart_type: str) -> str:
    if any(term in q for term in ("plot", "chart", "graph", "visualize")):
        return "chart_and_answer"
    if chart_type != "none" and any(term in q for term in ("show", "compare", "trend")):
        return "chart_and_answer"
    return "answer"


def _parse_date_expression(q: str) -> Optional[str]:
    match = re.search(r"\bsince\s+20\d{2}\b", q)
    if match:
        return match.group(0)

    match = re.search(r"\bfrom\s+20\d{2}\s+(?:to|through|-)\s+20\d{2}\b", q)
    if match:
        return match.group(0)

    match = re.search(r"\b(?:latest|current|this week|this year|ytd|year to date)\b", q)
    if match:
        return match.group(0)

    return None


def _sanitize(value: str, allowed: tuple[str, ...], default: str) -> str:
    return value if value in allowed else default


def parse_energy_query(user_query: str, normalized_query: str) -> EnergyQueryParse:
    q = normalized_query or user_query.lower().strip()
    domain, reason, confidence = _classify_domain(q)
    if domain != "storage":
        return EnergyQueryParse(
            domain="unsupported",
            analysis_type="unsupported",
            regions=[],
            value_type="level",
            comparisons=["none"],
            chart_type="none",
            output_mode="answer",
            date_expression=_parse_date_expression(q),
            confidence=confidence,
            ambiguous=False,
            reason=reason,
        )

    regions = _parse_regions(q) or ["lower48"]
    states = _parse_states(q)
    states_all = False
    storage_frequency = _parse_storage_frequency(q)
    storage_metric_type = _parse_storage_metric_type(q)
    storage_dataset = _parse_storage_dataset(
        q,
        frequency=storage_frequency,
        states=states,
        regions=regions,
        metric_type=storage_metric_type,
    )
    if storage_dataset == "underground_storage_all_operators":
        regions = []
        if storage_frequency == "weekly":
            storage_frequency = "monthly"
        if not states and _asks_all_states(q):
            states_all = True
    else:
        states = []
        states_all = False
        storage_frequency = "weekly"
        storage_metric_type = "working_gas"
    value_type = _parse_value_type(q)
    comparisons = _parse_comparisons(q)
    analysis_type = _parse_analysis_type(
        q,
        value_type,
        comparisons,
        regions,
        states,
        storage_dataset,
    )
    chart_type = _parse_chart_type(q, analysis_type)
    output_mode = _parse_output_mode(q, chart_type)
    date_expression = _parse_date_expression(q)

    analysis_type = _sanitize(analysis_type, STORAGE_ANALYSIS_TYPES, "unsupported")
    storage_dataset = _sanitize(storage_dataset, STORAGE_DATASETS, "weekly_working_gas")
    storage_frequency = _sanitize(storage_frequency, STORAGE_FREQUENCIES, "weekly")
    storage_metric_type = _sanitize(storage_metric_type, STORAGE_METRIC_TYPES, "working_gas")
    value_type = _sanitize(value_type, VALUE_TYPES, "level")
    chart_type = _sanitize(chart_type, CHART_TYPES, "none")
    output_mode = _sanitize(output_mode, OUTPUT_MODES, "answer")
    comparisons = [comp for comp in comparisons if comp in COMPARISONS] or ["none"]

    return EnergyQueryParse(
        domain="storage",
        analysis_type=analysis_type,
        storage_dataset=storage_dataset,
        storage_frequency=storage_frequency,
        storage_metric_type=storage_metric_type,
        regions=regions,
        states=states,
        states_all=states_all,
        value_type=value_type,
        comparisons=comparisons,
        chart_type=chart_type,
        output_mode=output_mode,
        date_expression=date_expression,
        filters={
            "regions": regions,
            "states": states,
            "states_all": states_all,
            "storage_dataset": storage_dataset,
            "storage_frequency": storage_frequency,
            "storage_metric_type": storage_metric_type,
        },
        confidence=confidence,
        ambiguous=False,
        reason=reason,
    )


def llm_parse_query(user_query: str, normalized_query: str) -> EnergyQueryParse:
    return parse_energy_query(user_query=user_query, normalized_query=normalized_query)
