from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from schemas.chart_spec import ChartSpec


def render_plotly(spec: ChartSpec, df: pd.DataFrame) -> go.Figure:
    if df is None or df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data to chart")
        return fig

    d = df.copy()

    x_field = spec.x.field
    y_field = spec.series[0].field if spec.series else spec.y.field

    # Ensure datetime x if possible
    if x_field in d.columns:
        d[x_field] = pd.to_datetime(d[x_field], errors="coerce")

    if spec.chart_type in ("line", "area", "stacked_area"):
        fig = px.line(d, x=x_field, y=y_field, title=spec.title)
    elif spec.chart_type == "bar":
        fig = px.bar(d, x=x_field, y=y_field, title=spec.title)
    else:
        raise ValueError(f"Unsupported chart_type: {spec.chart_type}")

    fig.update_layout(margin=dict(l=20, r=20, t=50, b=20))
    return fig
