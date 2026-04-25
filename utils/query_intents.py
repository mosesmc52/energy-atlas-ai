from __future__ import annotations

import re

SEASONAL_NORM_PHRASES = (
    "seasonal norm",
    "seasonal norms",
    "seasonal normal",
    "seasonal normals",
    "versus normal",
    "vs normal",
    "compared to normal",
)

ISO_GAS_SHARE_PHRASES = (
    "percentage of electricity generation",
    "percent of electricity generation",
    "what percentage of electricity generation",
    "electricity generation from natural gas",
    "share of electricity from natural gas",
)

CURRENT_LIKE_TOKENS = ("current", "latest", "right now", "today")

EXPLICIT_WINDOW_PATTERN = (
    r"(20\d{2})-(\d{2})|(?:last|past)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+"
    r"(day|days|week|weeks|month|months|year|years)|ytd|year to date|this year|last year|past year|last month|past month|last week|past week"
)


def has_seasonal_norm_phrase(text: str) -> bool:
    q = (text or "").lower()
    return any(phrase in q for phrase in SEASONAL_NORM_PHRASES)


def is_current_like_without_explicit_window(text: str) -> bool:
    q = (text or "").lower()
    return any(token in q for token in CURRENT_LIKE_TOKENS) and not re.search(
        EXPLICIT_WINDOW_PATTERN,
        q,
    )


def is_power_burn_seasonal_question(text: str) -> bool:
    q = (text or "").lower()
    return (
        "power burn" in q
        and "natural gas" in q
        and any(term in q for term in ("seasonal norm", "seasonal norms", "seasonal"))
    )


def is_iso_gas_share_question(text: str) -> bool:
    q = (text or "").lower()
    return any(phrase in q for phrase in ISO_GAS_SHARE_PHRASES)


def is_renewables_power_sector_demand_question(text: str) -> bool:
    q = (text or "").lower()
    return "renewables" in q and "power sector" in q and "demand" in q
