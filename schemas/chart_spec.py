from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class SeriesSpec(BaseModel):
    """One plotted series (legacy-compatible)."""

    name: str = Field(..., description="Legend label")
    source: Literal[
        "eia_api",
        "aeo_projection",
        "ferc",
        "cftc",
        "simulation",
        "gridstatus",
    ]
    metric: str = Field(..., description="Canonical metric name")
    filters: Optional[dict] = Field(
        default=None, description="Facets, regions, cases, etc."
    )
    aggregation: Optional[
        Literal["daily", "weekly", "monthly", "annual", "average", "sum"]
    ] = None
    color: Optional[str] = Field(default=None, description="Optional hex color")


class AxisSpec(BaseModel):
    field: str
    label: str
    units: Optional[str] = None


class ChartSpec(BaseModel):
    """Deterministic chart instruction."""

    chart_type: Literal[
        "line",
        "bar",
        "area",
        "stacked_area",
        "histogram",
        "box",
        "scatter",
        "heatmap",
    ]
    title: str

    # Preferred v2 fields
    x: str | AxisSpec = "date"
    y: List[str] | str | AxisSpec = "value"
    x_label: Optional[str] = None
    y_label: Optional[str] = None
    aggregation: Optional[Literal["none", "daily", "weekly", "monthly"]] = None
    notes: Optional[str] = Field(default=None, description="Caption or interpretation")
    groupnorm: Optional[Literal["fraction"]] = None

    # Legacy-compatible fields
    series: List[SeriesSpec] = Field(default_factory=list)
    start: Optional[str] = Field(default=None, description="ISO date or year")
    end: Optional[str] = Field(default=None, description="ISO date or year")
