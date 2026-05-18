from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


QUESTION_TYPE_LATEST = "latest"
QUESTION_TYPE_MOM = "mom"
QUESTION_TYPE_YOY = "yoy"
QUESTION_TYPE_FIVE_YEAR_RANGE = "five_year_range"
QUESTION_TYPE_RANKING = "ranking"
QUESTION_TYPE_REGIONAL_RANKING = "regional_ranking"
QUESTION_TYPE_AVERAGE_N_DAYS = "average_n_days"
QUESTION_TYPE_UNKNOWN = "unknown"


@dataclass(frozen=True)
class ClassifiedQuery:
    question_type: str
    params: dict[str, Any]


def classify_query(normalized_query: str) -> ClassifiedQuery:
    q = (normalized_query or "").lower().strip()

    average_match = re.search(r"average .* last (?P<days>\d+)\s+days?", q)
    if average_match:
        return ClassifiedQuery(
            question_type=QUESTION_TYPE_AVERAGE_N_DAYS,
            params={"days": int(average_match.group("days"))},
        )

    if any(token in q for token in ("month over month", "mom")):
        return ClassifiedQuery(question_type=QUESTION_TYPE_MOM, params={})

    if any(token in q for token in ("year over year", "yoy", "same week last year", "vs last year")):
        return ClassifiedQuery(question_type=QUESTION_TYPE_YOY, params={})

    if (
        ("five-year range" in q or "5-year range" in q)
        or ("five-year" in q and "range" in q)
    ):
        return ClassifiedQuery(question_type=QUESTION_TYPE_FIVE_YEAR_RANGE, params={})

    if any(token in q for token in ("latest", "current", "right now", "most recent")):
        return ClassifiedQuery(question_type=QUESTION_TYPE_LATEST, params={})

    has_ranking_word = any(token in q for token in ("largest", "biggest", "most", "top", "rank"))
    has_region_word = any(token in q for token in ("region", "regional", "by region", "which region"))
    if has_ranking_word and has_region_word:
        return ClassifiedQuery(question_type=QUESTION_TYPE_REGIONAL_RANKING, params={})
    if has_ranking_word:
        return ClassifiedQuery(question_type=QUESTION_TYPE_RANKING, params={})

    return ClassifiedQuery(question_type=QUESTION_TYPE_UNKNOWN, params={})

