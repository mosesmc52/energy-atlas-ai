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
        "gridstatus",
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


class SignalSummary(BaseModel):
    status: Literal["bullish", "bearish", "neutral"]
    confidence: float


class AnswerDataPoint(BaseModel):
    metric: str
    value: Any
    unit: str = ""


class AnswerForecast(BaseModel):
    direction: str
    reasoning: str


class AnswerAlert(BaseModel):
    name: str
    status: bool


class AnswerSourceSummary(BaseModel):
    title: str
    date: Optional[str] = None


class SuggestedAlert(BaseModel):
    title: str
    reason: str
    signal_id: str
    priority: str


class StructuredAnswer(BaseModel):
    answer: str
    signal: SignalSummary
    summary: str
    drivers: List[str] = Field(default_factory=list)
    data_points: List[AnswerDataPoint] = Field(default_factory=list)
    forecast: AnswerForecast
    suggested_alerts: List[SuggestedAlert] = Field(default_factory=list)
    alerts: List[AnswerAlert] = Field(default_factory=list)
    sources: List[AnswerSourceSummary] = Field(default_factory=list)


class AnswerPayload(BaseModel):
    query: str
    mode: Literal["observed", "published_projection", "simulation", "mixed"]
    answer_text: str

    structured_response: Optional[StructuredAnswer] = None
    report_context_used: bool = False
    report_context_reason: Optional[str] = None
    report_context_sources: List[AnswerSourceSummary] = Field(default_factory=list)
    data_preview: Optional[DataPreview] = None
    chart_spec: Optional[ChartSpec] = None  # <-- no quotes

    sources: List[SourceRef]
    warnings: Optional[List[str]] = None
    generated_at: datetime = Field(default_factory=datetime.utcnow)
