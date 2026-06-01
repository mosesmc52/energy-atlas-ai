from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from agents.llm_router import (
    CHART_TYPES,
    COMPARISONS,
    OUTPUT_MODES,
    STORAGE_ANALYSIS_TYPES,
    STORAGE_REGIONS,
    VALUE_TYPES,
)


@dataclass(frozen=True)
class EnergyQueryParse:
    domain: str
    analysis_type: str
    regions: list[str] = field(default_factory=list)
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


def _contains_any(q: str, terms: tuple[str, ...]) -> bool:
    return any(term in q for term in terms)


def _classify_domain(q: str) -> tuple[str, str, float]:
    if _contains_any(q, STORAGE_TERMS):
        return "storage", "Storage language detected.", 0.9
    if _contains_any(q, WEEKLY_CHANGE_TERMS):
        return "storage", "Storage weekly-change language detected.", 0.78
    if "region" in q and any(term in q for term in ("normal", "above", "below")):
        return "storage", "Regional storage comparison language detected.", 0.72
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
    if any(term in q for term in ("5-year average", "5 year average", "five-year average", "five year average")):
        comparisons.append("five_year_avg")
    if any(term in q for term in ("range", "band", "min/max", "min max", "five-year range", "5-year range")):
        comparisons.append("five_year_range")
    if any(term in q for term in ("normal", "seasonal", "same week history", "above normal", "below normal")):
        comparisons.append("seasonal_normal")
    return comparisons or ["none"]


def _parse_analysis_type(q: str, value_type: str, comparisons: list[str], regions: list[str]) -> str:
    if any(term in q for term in ("which region", "rank", "ranking", "most above", "most below")):
        return "ranking"
    if _asks_all_regions(q):
        return "regional_compare"
    if len(regions) > 1:
        return "time_series"
    if any(comp in comparisons for comp in ("five_year_avg", "five_year_range", "seasonal_normal", "last_year")):
        return "seasonal_compare"
    if any(term in q for term in ("plot", "chart", "trend", "over time", "since", "from ")) or re.search(r"\b20\d{2}\b", q):
        return "time_series"
    if value_type == "weekly_change":
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
    value_type = _parse_value_type(q)
    comparisons = _parse_comparisons(q)
    analysis_type = _parse_analysis_type(q, value_type, comparisons, regions)
    chart_type = _parse_chart_type(q, analysis_type)
    output_mode = _parse_output_mode(q, chart_type)
    date_expression = _parse_date_expression(q)

    analysis_type = _sanitize(analysis_type, STORAGE_ANALYSIS_TYPES, "unsupported")
    value_type = _sanitize(value_type, VALUE_TYPES, "level")
    chart_type = _sanitize(chart_type, CHART_TYPES, "none")
    output_mode = _sanitize(output_mode, OUTPUT_MODES, "answer")
    comparisons = [comp for comp in comparisons if comp in COMPARISONS] or ["none"]

    return EnergyQueryParse(
        domain="storage",
        analysis_type=analysis_type,
        regions=regions,
        value_type=value_type,
        comparisons=comparisons,
        chart_type=chart_type,
        output_mode=output_mode,
        date_expression=date_expression,
        filters={"regions": regions},
        confidence=confidence,
        ambiguous=False,
        reason=reason,
    )


def llm_parse_query(user_query: str, normalized_query: str) -> EnergyQueryParse:
    return parse_energy_query(user_query=user_query, normalized_query=normalized_query)
