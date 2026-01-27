# schemas/answer.py
from __future__ import annotations

from datetime import datetime
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field
from schemas.chart_spec import ChartSpec  # <-- import the real class


class DataPreview(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    units: Optional[dict] = None


class SourceRef(BaseModel):
    source_type: Literal[
        "eia_api",
        "aeo_table",
        "aeo_document",
        "ferc_form",
        "cftc",
        "newsletter",
        "simulation",
        "manual",
    ]
    label: str
    reference: str
    parameters: Optional[dict] = None
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)


class AnswerPayload(BaseModel):
    query: str
    mode: Literal["observed", "published_projection", "simulation", "mixed"]
    answer_text: str

    data_preview: Optional[DataPreview] = None
    chart_spec: Optional[ChartSpec] = None  # <-- no quotes

    sources: List[SourceRef]
    warnings: Optional[List[str]] = None
    generated_at: datetime = Field(default_factory=datetime.utcnow)
