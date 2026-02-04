import re
from datetime import date, timedelta
from typing import Tuple

import pandas as pd


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

    # ---- last N days / months / years ----
    m = re.search(r"last\s+(\d+)\s+(day|days|month|months|year|years)", q)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if "day" in unit:
            start = today - timedelta(days=n)
        elif "month" in unit:
            start = today - pd.DateOffset(months=n)
        else:
            start = today - pd.DateOffset(years=n)
        return start.date().isoformat(), today.isoformat()

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
