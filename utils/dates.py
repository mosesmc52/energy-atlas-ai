import re
from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd

_NUMBER_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
}


def _parse_count(token: str) -> Optional[int]:
    t = str(token or "").strip().lower()
    if not t:
        return None
    if t.isdigit():
        return int(t)
    return _NUMBER_WORDS.get(t)


def has_explicit_date_reference(query: str) -> bool:
    q = query.lower()

    if re.search(r"(20\d{2})-(\d{2})", q):
        return True
    if re.search(r"last\s+(\d+)\s+(day|days|month|months|year|years)", q):
        return True
    if re.search(
        r"(last|past)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+"
        r"(day|days|week|weeks|month|months|year|years)",
        q,
    ):
        return True
    if any(
        token in q
        for token in (
            "last year",
            "past year",
            "last month",
            "past month",
            "last week",
            "past week",
        )
    ):
        return True
    if "ytd" in q or "year to date" in q:
        return True
    if "this year" in q:
        return True
    if "latest" in q or "current" in q:
        return True

    return False


def resolve_date_range(query: str) -> Tuple[str, str]:
    """
    Resolve date range from natural language.
    Defaults to last 6 months if nothing found.
    """
    q = query.lower()
    today = date.today()

    # ---- explicit YYYY or YYYY-MM ----
    m = re.search(r"(20\d{2})-(\d{2})", q)
    if m:
        start = pd.Timestamp(f"{m.group(1)}-{m.group(2)}-01")
        end = start + pd.offsets.MonthEnd(1)
        return start.date().isoformat(), end.date().isoformat()

    # ---- last/past N days / months / years ----
    m = re.search(
        r"(?:last|past)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+"
        r"(day|days|week|weeks|month|months|year|years)",
        q,
    )
    if m:
        parsed = _parse_count(m.group(1))
        if parsed is None:
            parsed = 1
        n = parsed
        unit = m.group(2)
        if "day" in unit:
            start = today - timedelta(days=n)
        elif "week" in unit:
            start = today - timedelta(weeks=n)
        elif "month" in unit:
            start = today - pd.DateOffset(months=n)
        else:
            start = today - pd.DateOffset(years=n)
        if hasattr(start, "date"):
            return start.date().isoformat(), today.isoformat()
        return start.isoformat(), today.isoformat()

    if "last year" in q or "past year" in q:
        start = today - pd.DateOffset(years=1)
        return start.date().isoformat(), today.isoformat()

    if "last month" in q or "past month" in q:
        current_month_start = pd.Timestamp(today).replace(day=1)
        last_month_start = current_month_start - pd.DateOffset(months=1)
        last_month_end = last_month_start + pd.offsets.MonthEnd(1)
        return last_month_start.date().isoformat(), last_month_end.date().isoformat()

    if "last week" in q or "past week" in q:
        start = today - timedelta(weeks=1)
        return start.isoformat(), today.isoformat()

    # ---- keywords ----
    if "ytd" in q or "year to date" in q:
        start = date(today.year, 1, 1)
        return start.isoformat(), today.isoformat()

    if "this year" in q:
        start = date(today.year, 1, 1)
        end = date(today.year, 12, 31)
        return start.isoformat(), end.isoformat()

    if "latest" in q or "current" in q:
        # Small lookback so adapters can fetch latest observation
        start = today - timedelta(days=30)
        return start.isoformat(), today.isoformat()

    # ---- default fallback ----
    start = today - pd.DateOffset(months=6)
    return start.date().isoformat(), today.isoformat()
