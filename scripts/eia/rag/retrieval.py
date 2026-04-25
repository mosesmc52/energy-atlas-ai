from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

_TOKEN_RE = re.compile(r"[a-z0-9]{2,}")
_RECENCY_WINDOW_DAYS = 120
_RAG_KEYWORDS = {
    "why",
    "driver",
    "drivers",
    "drove",
    "drive",
    "driven",
    "narrative",
    "report",
    "reports",
    "outlook",
    "explain",
    "movement",
    "move",
    "moved",
    "recent",
    "context",
    "market",
    "tightening",
    "loosening",
    "said",
}


def load_report_chunks(path: str) -> list[dict]:
    chunk_path = Path(path)
    if not chunk_path.exists() or not chunk_path.is_file():
        return []

    rows: list[dict] = []
    with chunk_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                rows.append(parsed)
    return rows


def should_use_report_rag(question: str) -> bool:
    query_tokens = set(_tokenize(question))
    if not query_tokens:
        return False
    return any(keyword in query_tokens for keyword in _RAG_KEYWORDS)


def search_report_chunks(query: str, chunks: list[dict], top_k: int = 5) -> list[dict]:
    query_tokens = set(_tokenize(query))
    if not chunks or not query_tokens:
        return []

    scored: list[tuple[float, dict[str, Any]]] = []
    for chunk in chunks:
        score = _score_chunk(query_tokens, chunk)
        if score <= 0:
            continue
        scored.append((score, chunk))

    scored.sort(
        key=lambda item: (
            item[0],
            _parse_date(item[1]).toordinal() if _parse_date(item[1]) else 0,
        ),
        reverse=True,
    )
    return [chunk for _, chunk in scored[:top_k]]


def _score_chunk(query_tokens: set[str], chunk: dict[str, Any]) -> float:
    title_tokens = set(_tokenize(chunk.get("title", "")))
    text_tokens = set(_tokenize(chunk.get("text", "")))
    type_tokens = set(_tokenize(chunk.get("report_type", "")))
    topic_tokens = set()
    for topic in chunk.get("topics") or []:
        topic_tokens.update(_tokenize(str(topic)))

    overlap_title = len(query_tokens & title_tokens)
    overlap_text = len(query_tokens & text_tokens)
    overlap_type = len(query_tokens & type_tokens)
    overlap_topics = len(query_tokens & topic_tokens)

    score = (
        (overlap_text * 1.0)
        + (overlap_title * 2.5)
        + (overlap_topics * 2.0)
        + (overlap_type * 1.5)
    )
    if score <= 0:
        return 0.0

    recency_date = _parse_date(chunk)
    if recency_date is not None:
        age_days = max((date.today() - recency_date).days, 0)
        recency_boost = max(0.0, (_RECENCY_WINDOW_DAYS - age_days) / _RECENCY_WINDOW_DAYS)
        score += round(recency_boost, 3)

    return score


def _parse_date(chunk: dict[str, Any]) -> date | None:
    for key in ("published_date", "release_date", "period_ending"):
        raw = chunk.get(key)
        if not raw:
            continue
        try:
            return datetime.fromisoformat(str(raw)[:10]).date()
        except ValueError:
            continue
    return None


def _tokenize(text: Any) -> list[str]:
    return _TOKEN_RE.findall(str(text).lower())
