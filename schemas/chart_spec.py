# atlas/schemas/chart_spec.py
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class SeriesSpec(BaseModel):
    """
    One plotted series.
    """

    name: str = Field(..., description="Legend label")

    source: Literal["eia_api", "aeo_projection", "ferc", "cftc", "simulation"]

    metric: str = Field(..., description="Canonical metric name from metrics.yaml")

    filters: Optional[dict] = Field(
        default=None, description="Facets, regions, cases, etc."
    )

    aggregation: Optional[
        Literal["daily", "weekly", "monthly", "annual", "average", "sum"]
    ] = None

    color: Optional[str] = Field(
        default=None, description="Optional hex color override"
    )


class AxisSpec(BaseModel):
    field: str
    label: str
    units: Optional[str] = None


class ChartSpec(BaseModel):
    """
    Deterministic chart instruction.
    """

    chart_type: Literal["line", "bar", "area", "stacked_area"]

    title: str

    x: AxisSpec
    y: AxisSpec

    series: List[SeriesSpec]

    start: Optional[str] = Field(default=None, description="ISO date or year")

    end: Optional[str] = Field(default=None, description="ISO date or year")

    notes: Optional[str] = Field(
        default=None, description="Caption or interpretation note"
    )
