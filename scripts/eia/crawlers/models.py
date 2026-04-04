from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

@dataclass
class ReportRecord:
    source: str
    report_type: str
    title: str
    url: str
    published_date: str | None = None
    release_date: str | None = None
    period_ending: str | None = None
    summary_text: str = ""
    body_text: str = ""
    topics: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

@dataclass
class ReportChunk:
    doc_id: str
    chunk_id: str
    source: str
    report_type: str
    title: str
    url: str
    chunk_index: int
    text: str
    published_date: str | None = None
    release_date: str | None = None
    period_ending: str | None = None
    topics: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
