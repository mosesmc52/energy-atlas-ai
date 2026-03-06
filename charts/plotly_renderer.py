from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from schemas.chart_spec import AxisSpec, ChartSpec


def _axis_field(
    axis: str | AxisSpec, default_label: str
) -> tuple[str, str, str | None]:
    if isinstance(axis, AxisSpec):
        return axis.field, axis.label, axis.units
    return axis, default_label, None


def _y_fields(y: list[str] | str | AxisSpec) -> list[str]:
    if isinstance(y, AxisSpec):
        return [y.field]
    if isinstance(y, str):
        return [y]
    return y


def render_plotly(spec: ChartSpec, df: pd.DataFrame) -> go.Figure:
    if df is None or df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data to chart")
        return fig

    d = df.copy()

    x_field, x_label, _ = _axis_field(spec.x, "Date")
    y_fields = [y for y in _y_fields(spec.y) if y in d.columns]

    y_units = None
    if isinstance(spec.y, AxisSpec):
        y_units = spec.y.units

    x_is_datetime = False
    if x_field in d.columns:
        d[x_field] = pd.to_datetime(d[x_field], errors="coerce")
        x_is_datetime = pd.api.types.is_datetime64_any_dtype(d[x_field])
        if x_is_datetime:
            d = d.dropna(subset=[x_field])

    chart_type = spec.chart_type
    template = "plotly_white"

    if chart_type == "stacked_area":
        fig = go.Figure()
        for y in y_fields:
            trace_kwargs = {
                "x": d[x_field],
                "y": d[y],
                "mode": "lines",
                "name": y,
                "stackgroup": "one",
            }
            if spec.groupnorm:
                trace_kwargs["groupnorm"] = spec.groupnorm
            fig.add_trace(go.Scatter(**trace_kwargs))
        fig.update_layout(title=spec.title, template=template)

    elif chart_type in ("line", "area"):
        fig = px.line(d, x=x_field, y=y_fields, title=spec.title, template=template)
        fig.update_traces(line=dict(width=3))

        if chart_type == "area":
            fig.update_traces(fill="tozeroy")

    elif chart_type == "bar":
        fig = px.bar(d, x=x_field, y=y_fields, title=spec.title, template=template)

    elif chart_type == "histogram":
        if not y_fields:
            raise ValueError("Histogram requires at least one y field")
        fig = px.histogram(d, x=y_fields[0], title=spec.title, template=template)

    elif chart_type == "box":
        if not y_fields:
            raise ValueError("Box chart requires at least one y field")
        fig = px.box(d, y=y_fields[0], title=spec.title, template=template)

    elif chart_type == "scatter":
        if not y_fields:
            raise ValueError("Scatter requires y fields")

        scatter_x = x_field if x_field in d.columns and x_field != "date" else None
        scatter_y = y_fields[0]

        if scatter_x is None and len(y_fields) >= 2:
            scatter_x = y_fields[0]
            scatter_y = y_fields[1]
        elif scatter_x is None:
            nums = [
                c
                for c in d.columns
                if c != y_fields[0] and pd.api.types.is_numeric_dtype(d[c])
            ]
            if not nums:
                raise ValueError("Scatter requires two numeric columns")
            scatter_x = nums[0]

        fig = px.scatter(
            d, x=scatter_x, y=scatter_y, title=spec.title, template=template
        )

    elif chart_type == "heatmap":
        if len(y_fields) >= 2:
            fig = px.density_heatmap(
                d, x=y_fields[0], y=y_fields[1], title=spec.title, template=template
            )
        elif y_fields and x_field in d.columns:
            fig = px.density_heatmap(
                d, x=x_field, y=y_fields[0], title=spec.title, template=template
            )
        else:
            raise ValueError("Heatmap requires at least two dimensions")

    else:
        raise ValueError(f"Unsupported chart_type: {chart_type}")

    fig.update_layout(
        margin=dict(l=30, r=20, t=120, b=40),  # increased top margin
        height=650 if chart_type in {"line", "area", "stacked_area"} else 500,
        font=dict(size=14),
        xaxis_title=spec.x_label or x_label,
        yaxis_title=spec.y_label or (y_fields[0] if y_fields else "Value"),
        title=dict(
            x=0.02,
            xanchor="left",
            font=dict(size=20),
            pad=dict(b=18),  # adds spacing between title and buttons
        ),
        hovermode=(
            "x unified"
            if x_is_datetime and chart_type in {"line", "area", "stacked_area", "bar"}
            else "closest"
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0.02,
        ),
    )

    if len(fig.data) <= 1:
        fig.update_layout(showlegend=False)

    fig.update_xaxes(
        showgrid=False,
        showline=True,
        linewidth=1,
        linecolor="rgba(0,0,0,0.35)",
        ticks="outside",
        ticklen=6,
    )

    if x_is_datetime:
        fig.update_xaxes(tickformat="%Y-%m")

    fig.update_yaxes(
        showgrid=True,
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False,
        showline=True,
        linewidth=1,
        linecolor="rgba(0,0,0,0.35)",
        ticks="outside",
        ticklen=6,
    )

    if spec.series:
        for i, tr in enumerate(fig.data):
            if i < len(spec.series):
                tr.name = spec.series[i].name

    for tr in fig.data:
        if chart_type in {"histogram", "heatmap"}:
            continue
        name_line = "<br>%{fullData.name}" if len(fig.data) > 1 else ""
        x_fmt = "%{x|%Y-%m-%d}" if x_is_datetime else "%{x}"
        y_suffix = f" {y_units}" if y_units else ""
        tr.hovertemplate = f"{x_fmt}{name_line}<br>%{{y:,.2f}}{y_suffix}<extra></extra>"

    if x_is_datetime and chart_type in {"line", "area", "stacked_area", "bar"}:
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                x=0,
                xanchor="left",
                y=1.15,  # move selector above chart
                yanchor="top",
                bgcolor="rgba(240,240,240,0.9)",
                activecolor="rgba(200,200,200,0.9)",
                buttons=[
                    dict(count=1, label="1M", step="month", stepmode="backward"),
                    dict(count=6, label="6M", step="month", stepmode="backward"),
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(step="all", label="All"),
                ],
            ),
        )

    return fig
