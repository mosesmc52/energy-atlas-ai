from __future__ import annotations

from pydantic import BaseModel, Field


class AlertRulePayload(BaseModel):
    name: str = Field(min_length=1)
    question: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    value_mode: str = Field(min_length=1)
    operator: str = Field(min_length=1)
    threshold: float
    frequency: str = Field(min_length=1)
    trigger_type: str = Field(min_length=1)
    cooldown_hours: int = Field(ge=0)
    geography_type: str | None = None
    state_code: str | None = None
    country_code: str | None = None
