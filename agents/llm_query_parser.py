from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from agents.llm_router import (
    CHART_TYPES,
    COMPARISONS,
    LNG_STORAGE_METRIC_BY_TYPE_AND_FREQUENCY,
    OUTPUT_MODES,
    STORAGE_ANALYSIS_TYPES,
    STORAGE_DATASETS,
    STORAGE_FREQUENCIES,
    STORAGE_INSIGHT_TYPES,
    STORAGE_METRIC_TYPES,
    STORAGE_REGIONS,
    STORAGE_TYPES,
    UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS,
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
    storage_type: str | None = None
    storage_types_all: bool = False
    storage_insight_type: str | None = None
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
    "stored in",
    "storage type",
    "storage types",
    "aquifer",
    "aquifers",
    "salt cavern",
    "salt caverns",
    "salt storage",
    "depleted field",
    "depleted fields",
    "depleted reservoir",
    "depleted reservoirs",
    "capacity",
    "storage capacity",
    "field count",
    "storage field count",
    "lng storage",
    "liquefied natural gas storage",
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

STORAGE_TYPE_ALL_TERMS = (
    "by type",
    "storage type",
    "storage types",
    "compare storage types",
    "rank storage types",
    "underground storage by type",
)

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
    "capacity",
    "storage capacity",
    "field count",
    "storage field count",
    "number of storage fields",
    "how many storage fields",
    "count of storage fields",
    "underground storage count",
)

LNG_STORAGE_TERMS = (
    "lng storage",
    "liquefied natural gas storage",
    "lng storage additions",
    "lng storage withdrawals",
    "lng storage withdrawls",
    "lng storage net withdrawals",
    "lng storage net withdrawls",
)

STORAGE_INSIGHT_TERMS = (
    "how full",
    "full is storage",
    "storage utilization",
    "utilization rate",
    "percent full",
    "% full",
    "remaining capacity",
    "spare capacity",
    "unused capacity",
    "storage space remaining",
    "available capacity",
    "least remaining capacity",
    "most remaining capacity",
    "capacity per field",
    "average storage field size",
    "average capacity per field",
    "storage capacity per field",
    "capacity most concentrated",
    "historical maximum",
    "historical max",
    "record high",
    "all-time high",
    "near its max",
    "near maximum",
    "when was storage last this high",
    "weekly storage report",
    "storage report analysis",
    "this week's storage report",
    "latest storage report",
    "weekly report card",
    "storage summary",
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


def _parse_storage_type(q: str) -> tuple[str | None, bool]:
    if any(term in q for term in STORAGE_TYPE_ALL_TERMS):
        return None, True
    if any(term in q for term in ("salt cavern", "salt-cavern", "salt storage", "salt")):
        return "salt_cavern", False
    if any(term in q for term in ("depleted field", "depleted reservoir", "depleted")):
        return "depleted_field", False
    if "aquifer" in q:
        return "aquifer", False
    return None, False


def _parse_storage_insight_type(q: str) -> str | None:
    if any(term in q for term in ("how full", "full is storage", "storage utilization", "utilization rate", "percent full", "% full")):
        return "storage_utilization"
    if any(
        term in q
        for term in (
            "remaining capacity",
            "remaining storage capacity",
            "spare capacity",
            "unused capacity",
            "storage space remaining",
            "available capacity",
            "least remaining capacity",
            "least remaining storage capacity",
            "most remaining capacity",
            "most remaining storage capacity",
        )
    ):
        return "remaining_capacity"
    if any(
        term in q
        for term in (
            "capacity per field",
            "average storage field size",
            "average capacity per field",
            "storage capacity per field",
            "capacity most concentrated",
        )
    ):
        return "capacity_per_field"
    if any(
        term in q
        for term in (
            "historical maximum",
            "historical max",
            "record high",
            "all-time high",
            "near its max",
            "near maximum",
            "when was storage last this high",
        )
    ):
        return "historical_max_compare"
    if any(
        term in q
        for term in (
            "weekly storage report",
            "storage report analysis",
            "this week's storage report",
            "latest storage report",
            "weekly report card",
            "storage summary",
        )
    ):
        return "weekly_report_card"
    return None


def _has_explicit_time_series_request(q: str) -> bool:
    if any(term in q for term in ("plot", "chart", "trend", "over time", "history", "historical", "over the last", "since")):
        return True
    if re.search(r"\bfrom\s+(?:[a-z]+\s+)?20\d{2}\b", q):
        return True
    return bool(re.search(r"\b20\d{2}\b", q))


def _parse_storage_metric_type(q: str) -> str:
    # Storage metric guidance:
    # - "working gas in storage" -> working_gas
    # - "working gas volume change from year ago" -> working_gas_yoy_volume_change
    # - "working gas percent change from year ago" -> working_gas_yoy_pct_change
    # - "year-over-year increase/decrease in working gas" -> volume change unless percent/pct/% is explicit
    # - For underground_storage_all_operators, do not treat "year ago" as seasonal_compare intent
    if any(
        term in q
        for term in (
            "lng storage net withdrawals",
            "lng storage net withdrawls",
            "lng net withdrawals",
            "lng net withdrawls",
            "net withdrawals from lng storage",
            "net withdrawls from lng storage",
        )
    ):
        return "lng_storage_net_withdrawals"
    if any(
        term in q
        for term in (
            "lng storage withdrawals",
            "lng storage withdrawls",
            "lng withdrawals",
            "lng withdrawls",
            "withdrawals from lng storage",
            "withdrawls from lng storage",
        )
    ):
        return "lng_storage_withdrawals"
    if any(
        term in q
        for term in (
            "lng storage additions",
            "lng additions",
            "additions to lng storage",
            "lng storage injected",
            "lng storage injection",
        )
    ):
        return "lng_storage_additions"
    if any(
        term in q
        for term in (
            "storage field count",
            "field count",
            "storage fields",
            "number of storage fields",
            "how many storage fields",
            "count of storage fields",
            "underground storage count",
        )
    ):
        return "storage_field_count"
    if any(
        term in q
        for term in (
            "working gas capacity",
            "working capacity",
            "working gas storage capacity",
        )
    ):
        return "working_gas_capacity"
    if any(
        term in q
        for term in (
            "total capacity",
            "storage capacity",
            "underground storage capacity",
            "natural gas storage capacity",
            "capacity",
        )
    ):
        return "total_capacity"
    has_yoy_cue = any(term in q for term in YOY_CUES)
    has_percent_cue = any(term in q for term in YOY_PERCENT_CUES)
    has_volume_change_cue = any(term in q for term in YOY_VOLUME_CHANGE_CUES)
    has_working_gas = "working gas" in q
    has_ranking_state_cue = any(term in q for term in ("which state", "rank states", "largest", "highest", "most", "biggest"))

    if has_percent_cue and has_yoy_cue:
        return "working_gas_yoy_pct_change"
    if has_working_gas and has_percent_cue and has_ranking_state_cue and any(
        term in q for term in ("increase", "decrease", "change")
    ):
        return "working_gas_yoy_pct_change"
    if has_yoy_cue and has_volume_change_cue and not has_percent_cue:
        return "working_gas_yoy_volume_change"
    if has_working_gas and has_yoy_cue and not has_percent_cue:
        return "working_gas_yoy_volume_change"
    if any(term in q for term in ("working gas percent change from year ago", "working gas % change from year ago", "percent change from year ago", "% change from year ago", "yoy percent change", "yoy pct change")):
        return "working_gas_yoy_pct_change"
    if any(term in q for term in ("working gas volume change from year ago", "change from year ago", "year ago volume change", "yoy volume change")):
        return "working_gas_yoy_volume_change"
    if any(term in q for term in ("net withdrawals", "net withdrawal", "net withdrawls")):
        return "net_withdrawals"
    if any(term in q for term in ("withdrawals", "withdrawls", "withdrawn", "withdrawal", "withdrew")):
        return "withdrawals"
    if any(term in q for term in ("injections", "injected", "injection")):
        return "injections"
    if any(term in q for term in ("base gas", "cushion gas")):
        return "base_gas"
    if any(term in q for term in ("natural gas in storage", "total gas in storage", "total storage")):
        return "total_gas"
    return "working_gas"


def _is_capacity_count_storage_metric(metric_type: str) -> bool:
    return metric_type in {
        "total_capacity",
        "working_gas_capacity",
        "storage_field_count",
    }


def _capacity_count_regions() -> list[str]:
    return list(UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS)


def _parse_storage_dataset(q: str, *, frequency: str, states: list[str], regions: list[str], metric_type: str) -> str:
    if metric_type in {
        "lng_storage_additions",
        "lng_storage_withdrawals",
        "lng_storage_net_withdrawals",
    } or _contains_any(q, LNG_STORAGE_TERMS):
        return "lng_storage"
    has_by_type_terms = any(
        term in q
        for term in (
            "by type",
            "storage type",
            "storage types",
            "salt cavern",
            "salt-cavern",
            "salt storage",
            "depleted field",
            "depleted reservoir",
            "aquifer",
        )
    )
    has_weekly_terms = _contains_any(q, WEEKLY_STORAGE_TERMS)
    has_all_operator_terms = _contains_any(q, ALL_OPERATORS_TERMS)
    has_state_terms = bool(states) or _asks_all_states(q)
    has_weekly_regions = bool(regions) and any(region != "lower48" for region in regions)

    if _is_capacity_count_storage_metric(metric_type):
        return "underground_storage_all_operators"
    if has_state_terms:
        return "underground_storage_all_operators"
    if has_by_type_terms:
        return "underground_storage_by_type"
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


def _is_yoy_storage_metric(metric_type: str) -> bool:
    return metric_type in {
        "working_gas_yoy_volume_change",
        "working_gas_yoy_pct_change",
    }


def _parse_analysis_type(
    q: str,
    value_type: str,
    comparisons: list[str],
    regions: list[str],
    states: list[str],
    storage_dataset: str,
    storage_type: str | None,
    storage_types_all: bool,
) -> str:
    if storage_dataset == "underground_storage_by_type":
        if any(term in q for term in ("rank", "ranking", "rank storage types", "which storage type")):
            return "ranking"
        if _has_explicit_time_series_request(q):
            return "time_series"
        if (
            storage_types_all
            and not any(term in q for term in ("compare", "which storage type"))
            and _contains_any(q, MONTHLY_FREQUENCY_TERMS + ANNUAL_FREQUENCY_TERMS)
        ):
            return "time_series"
        if storage_types_all and any(term in q for term in ("compare", "by type", "storage type", "storage types")):
            return "regional_compare"
        if storage_types_all:
            return "regional_compare"
        if storage_type:
            return "latest"
    if any(term in q for term in ("which region", "which state", "rank", "ranking", "most above", "most below")):
        return "ranking"
    if _asks_all_regions(q) or _asks_all_states(q):
        return "regional_compare"
    if len(regions) > 1 or len(states) > 1:
        return "time_series"
    if any(comp in comparisons for comp in ("five_year_avg", "five_year_range", "seasonal_normal", "last_year")):
        return "seasonal_compare"
    if _has_explicit_time_series_request(q):
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
    storage_insight_type = _parse_storage_insight_type(q)
    metric_is_capacity_count = _is_capacity_count_storage_metric(storage_metric_type)
    storage_type, storage_types_all = _parse_storage_type(q)
    storage_dataset = _parse_storage_dataset(
        q,
        frequency=storage_frequency,
        states=states,
        regions=regions,
        metric_type=storage_metric_type,
    )
    if storage_dataset == "underground_storage_all_operators":
        if storage_frequency == "weekly":
            storage_frequency = "monthly"
        if metric_is_capacity_count and _asks_all_regions(q):
            regions = _capacity_count_regions()
        if metric_is_capacity_count and not states and not _asks_all_regions(q) and "lower 48" not in q and "lower48" not in q:
            regions = []
        elif not metric_is_capacity_count:
            regions = []
        if not states and _asks_all_states(q):
            states_all = True
        storage_type = None
        storage_types_all = False
    elif storage_dataset == "lng_storage":
        regions = []
        if storage_frequency != "monthly":
            storage_frequency = "monthly"
        if not states:
            states = ["united_states_total"]
        storage_type = None
        storage_types_all = False
    elif storage_dataset == "underground_storage_by_type":
        regions = []
        states = []
        states_all = False
        if storage_frequency not in {"monthly", "annual"}:
            storage_frequency = "monthly"
    else:
        states = []
        states_all = False
        storage_frequency = "weekly"
        storage_metric_type = "working_gas"
        storage_type = None
        storage_types_all = False
    if metric_is_capacity_count and not states and not _asks_all_regions(q):
        regions = [region for region in regions if region != "lower48"]
    value_type = _parse_value_type(q)
    comparisons = _parse_comparisons(q)
    if storage_dataset in {"underground_storage_all_operators", "underground_storage_by_type"} and _is_yoy_storage_metric(storage_metric_type):
        comparisons = ["none"]
    analysis_type = _parse_analysis_type(
        q,
        value_type,
        comparisons,
        regions,
        states,
        storage_dataset,
        storage_type,
        storage_types_all,
    )
    chart_type = _parse_chart_type(q, analysis_type)
    output_mode = _parse_output_mode(q, chart_type)
    date_expression = _parse_date_expression(q)

    if storage_insight_type in STORAGE_INSIGHT_TYPES:
        analysis_type = "explain"
        if storage_insight_type == "weekly_report_card":
            storage_dataset = "weekly_working_gas"
            storage_frequency = "weekly"
            storage_metric_type = "working_gas"
            regions = regions or ["lower48"]
            states = []
            states_all = False
            chart_type = "table"
            output_mode = "answer"
        elif storage_insight_type == "historical_max_compare":
            if states:
                storage_dataset = "underground_storage_all_operators"
                storage_frequency = "monthly" if storage_frequency == "weekly" else storage_frequency
            else:
                storage_dataset = "weekly_working_gas"
                storage_frequency = "weekly"
                regions = regions or ["lower48"]
                states = []
                states_all = False
            chart_type = "line" if _has_explicit_time_series_request(q) or "near" in q or "last this high" in q else "none"
            output_mode = "chart_and_answer" if chart_type != "none" else "answer"
        else:
            storage_dataset = "underground_storage_all_operators"
            storage_frequency = "monthly" if storage_frequency == "weekly" else storage_frequency
            if storage_insight_type == "storage_utilization" and not (states_all or len(states) > 1 or regions):
                chart_type = "none"
                output_mode = "answer"
            else:
                chart_type = "bar"
                output_mode = "chart_and_answer"
            if storage_insight_type in {"storage_utilization", "remaining_capacity"} and "which region" in q:
                regions = _capacity_count_regions()
                states = []
                states_all = False
            if storage_insight_type == "capacity_per_field" and not states and not regions and any(
                term in q for term in ("which state", "rank states", "largest", "most concentrated", "by state")
            ):
                states_all = True

    analysis_type = _sanitize(analysis_type, STORAGE_ANALYSIS_TYPES, "unsupported")
    storage_dataset = _sanitize(storage_dataset, STORAGE_DATASETS, "weekly_working_gas")
    storage_frequency = _sanitize(storage_frequency, STORAGE_FREQUENCIES, "weekly")
    storage_metric_type = _sanitize(storage_metric_type, STORAGE_METRIC_TYPES, "working_gas")
    storage_type = storage_type if storage_type in STORAGE_TYPES else None
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
        storage_type=storage_type,
        storage_types_all=storage_types_all,
        storage_insight_type=storage_insight_type,
        regions=regions,
        states=states,
        states_all=states_all,
        value_type=value_type,
        comparisons=comparisons,
        chart_type=chart_type,
        output_mode=output_mode,
        date_expression=date_expression,
        filters={
            "storage_dataset": storage_dataset,
            "storage_frequency": storage_frequency,
            "storage_metric_type": storage_metric_type,
            "storage_type": storage_type,
            "storage_types_all": storage_types_all,
            "storage_insight_type": storage_insight_type,
            "regions": regions,
            "states": states,
            "states_all": states_all,
        },
        confidence=confidence,
        ambiguous=False,
        reason=reason,
    )


def llm_parse_query(user_query: str, normalized_query: str) -> EnergyQueryParse:
    return parse_energy_query(user_query=user_query, normalized_query=normalized_query)
