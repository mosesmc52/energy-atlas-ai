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
    has_power_context = any(
        phrase in q
        for phrase in (
            "power burn",
            "power demand",
            "electric demand",
            "electricity demand",
            "power load",
        )
    )
    has_gas_usage_context = any(
        phrase in q
        for phrase in (
            "natural gas",
            "gas usage",
            "gas use",
            "gas consumption",
            "gas burn",
        )
    )
    has_baseline_context = any(
        phrase in q
        for phrase in (
            "seasonal norm",
            "seasonal norms",
            "seasonal average",
            "seasonal demand",
            "historical seasonal",
            "5-year average",
            "5 year average",
            "five-year average",
            "five year average",
            "compared to",
            "versus",
            "vs",
        )
    )
    return has_power_context and has_baseline_context and (
        has_gas_usage_context or ("seasonal demand" in q or "historical seasonal" in q)
    )
