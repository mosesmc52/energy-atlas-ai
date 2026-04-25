from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from schemas.chart_spec import AxisSpec, ChartSpec
from tools.forecasting import ForecastResult

CHART_BG = "#FFFFFF"


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


def _bar_datetime_label(series: pd.Series, aggregation: str | None) -> pd.Series:
    if aggregation == "monthly":
        return series.dt.strftime("%Y-%m")
    if aggregation == "weekly":
        return series.dt.strftime("%Y-%m-%d")
    return series.dt.strftime("%Y-%m-%d")


def _bucket_label(series: pd.Series) -> pd.Series:
    text = series.astype(str)
    # days_11_15 -> days 11-15
    text = text.str.replace(r"^days_(\d+)_(\d+)$", r"days \1-\2", regex=True)
    return text.str.replace("_", " ", regex=False)


def compute_storage_change_summary_metrics(
    df: pd.DataFrame,
    *,
    date_field: str = "date",
    value_field: str = "value",
) -> list[dict[str, float | str | None]]:
    if (
        df is None
        or df.empty
        or date_field not in df.columns
        or value_field not in df.columns
    ):
        return []

    d = df[[date_field, value_field]].copy()
    d[date_field] = pd.to_datetime(d[date_field], errors="coerce")
    d[value_field] = pd.to_numeric(d[value_field], errors="coerce")
    d = d.dropna(subset=[date_field, value_field]).sort_values(date_field)
    if d.empty:
        return []

    latest = float(d.iloc[-1][value_field])
    previous = float(d.iloc[-2][value_field]) if len(d) >= 2 else None
    avg_4w = float(d.tail(4)[value_field].mean())
    deepest = float(d[value_field].min())

    return [
        {
            "label": "Latest weekly change",
            "value": latest,
            "unit": "Bcf",
            "subtitle": _flow_subtitle(latest),
        },
        {
            "label": "Previous week",
            "value": previous,
            "unit": "Bcf",
            "subtitle": "vs prior week" if previous is not None else None,
        },
        {
            "label": "4-week average",
            "value": avg_4w,
            "unit": "Bcf",
            "subtitle": "recent average",
        },
        {
            "label": "Deepest withdrawal",
            "value": deepest,
            "unit": "Bcf",
            "subtitle": "displayed period",
        },
    ]


def compute_timeseries_summary_metrics(
    df: pd.DataFrame,
    *,
    date_field: str = "date",
    value_field: str = "value",
    unit: str | None = None,
) -> list[dict[str, float | str | None]]:
    if (
        df is None
        or df.empty
        or date_field not in df.columns
        or value_field not in df.columns
    ):
        return []

    d = df[[date_field, value_field]].copy()
    d[date_field] = pd.to_datetime(d[date_field], errors="coerce")
    d[value_field] = pd.to_numeric(d[value_field], errors="coerce")
    d = d.dropna(subset=[date_field, value_field]).sort_values(date_field)
    if d.empty:
        return []

    latest = float(d.iloc[-1][value_field])
    previous = float(d.iloc[-2][value_field]) if len(d) >= 2 else None
    period_low = float(d[value_field].min())
    period_high = float(d[value_field].max())
    latest_date = d.iloc[-1][date_field].date().isoformat()

    return [
        {
            "label": "Latest reading",
            "value": latest,
            "unit": unit or "",
            "subtitle": latest_date,
        },
        {
            "label": "Previous period",
            "value": previous,
            "unit": unit or "",
            "subtitle": "prior observation" if previous is not None else None,
        },
        {
            "label": "Period low",
            "value": period_low,
            "unit": unit or "",
            "subtitle": "displayed period",
        },
        {
            "label": "Period high",
            "value": period_high,
            "unit": unit or "",
            "subtitle": "displayed period",
        },
    ]


def _flow_subtitle(value: float | None) -> str | None:
    if value is None:
        return None
    if value < 0:
        return "withdrawal"
    if value > 0:
        return "injection"
    return "flat"


def _is_storage_change_chart(spec: ChartSpec, y_fields: list[str]) -> bool:
    return (
        spec.title == "Weekly Change in Working Gas Storage"
        and spec.chart_type in {"line", "area"}
        and y_fields == ["value"]
    )


def should_render_storage_change_summary_cards(spec: ChartSpec) -> bool:
    return (
        spec.title == "Weekly Change in Working Gas Storage"
        and spec.chart_type in {"line", "area"}
    )


def should_render_timeseries_summary_cards(spec: ChartSpec) -> bool:
    if "by region" in spec.title.lower():
        return False
    if spec.chart_type not in {"line", "area"}:
        return False
    if isinstance(spec.y, AxisSpec):
        return True
    if isinstance(spec.y, str):
        return True
    return len(spec.y) == 1


def _format_bcf(value: float) -> str:
    rounded = round(float(value))
    return f"{rounded:,.0f} Bcf"


def _apply_timeseries_dashboard_style(
    fig: go.Figure,
    d: pd.DataFrame,
    *,
    x_field: str,
    y_fields: list[str],
    y_label: str,
    y_units: str | None,
) -> None:
    fig.update_layout(
        title=dict(
            x=0.5,
            xanchor="center",
            font=dict(size=28, color="#111827"),
            pad=dict(b=18),
        ),
        height=720,
        margin=dict(l=70, r=60, t=90, b=70),
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_BG,
        hovermode="x unified",
    )
    fig.update_xaxes(
        showgrid=False,
        rangeslider_visible=False,
        rangeselector=None,
        tickformat="%b %Y",
        title_text="Date",
    )
    fig.update_yaxes(
        title_text=y_label,
        showgrid=True,
        gridcolor="rgba(148,163,184,0.22)",
        zeroline=False,
    )

    for index, trace in enumerate(fig.data):
        if getattr(trace, "mode", "") == "lines":
            trace.line.width = 4 if index == 0 else 3

    if len(y_fields) != 1 or y_fields[0] not in d.columns or x_field not in d.columns:
        return

    series = d[[x_field, y_fields[0]]].copy()
    series[x_field] = pd.to_datetime(series[x_field], errors="coerce")
    series[y_fields[0]] = pd.to_numeric(series[y_fields[0]], errors="coerce")
    series = series.dropna(subset=[x_field, y_fields[0]]).sort_values(x_field)
    if series.empty:
        return

    latest = series.iloc[-1]
    marker_color = "#ff7f0e"
    fig.add_trace(
        go.Scatter(
            x=[latest[x_field]],
            y=[latest[y_fields[0]]],
            mode="markers",
            name="Latest",
            marker=dict(size=10, color=marker_color),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f}<extra></extra>",
            showlegend=False,
        )
    )

    y_suffix = f" {y_units}" if y_units else ""
    latest_value = f"{float(latest[y_fields[0]]):,.2f}{y_suffix}".strip()
    fig.add_annotation(
        x=latest[x_field],
        y=latest[y_fields[0]],
        xanchor="right",
        xshift=-18,
        yshift=-6,
        align="left",
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=1.5,
        arrowcolor="#111827",
        bgcolor="rgba(255,255,255,0.96)",
        bordercolor="rgba(15,23,42,0.25)",
        borderwidth=1,
        borderpad=6,
        font=dict(size=13, color="#1f2937"),
        text=f"Latest: {latest_value}<br>{latest[x_field].date().isoformat()}",
    )


def _apply_storage_change_dashboard_style(
    fig: go.Figure, d: pd.DataFrame, *, x_field: str, y_field: str
) -> None:
    if d.empty:
        return

    series = d[[x_field, y_field]].dropna().sort_values(x_field).copy()
    if series.empty:
        return

    latest = series.iloc[-1]
    deepest = series.loc[series[y_field].idxmin()]

    fig.update_traces(
        line=dict(color="#2C7BB6", width=4),
        fill="tozeroy",
        fillcolor="rgba(44,123,182,0.15)",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f} Bcf<extra></extra>",
    )

    fig.update_layout(
        title=dict(
            x=0.5,
            xanchor="center",
            font=dict(size=34, color="#111827"),
            pad=dict(b=18),
        ),
        height=720,
        margin=dict(l=70, r=60, t=90, b=70),
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_BG,
    )
    fig.update_xaxes(
        title_text="Report Week",
        showgrid=False,
        rangeslider_visible=False,
        rangeselector=None,
        tickformat="%b %Y",
    )
    fig.update_yaxes(
        title_text="Bcf",
        showgrid=True,
        gridcolor="rgba(148,163,184,0.22)",
        zeroline=False,
    )

    fig.add_hline(line_width=2, y=0, line_color="rgba(59,130,246,0.8)")

    deepest_idx = series.index.get_loc(deepest.name)
    start_idx = max(0, deepest_idx - 1)
    end_idx = min(len(series) - 1, deepest_idx + 1)
    fig.add_vrect(
        x0=series.iloc[start_idx][x_field],
        x1=series.iloc[end_idx][x_field],
        fillcolor="rgba(148, 163, 184, 0.18)",
        line_width=0,
        layer="below",
    )

    fig.add_trace(
        go.Scatter(
            x=[deepest[x_field]],
            y=[deepest[y_field]],
            mode="markers",
            name="Deepest withdrawal",
            marker=dict(size=10, color="#2ca02c"),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f} Bcf<extra></extra>",
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[latest[x_field]],
            y=[latest[y_field]],
            mode="markers",
            name="Latest",
            marker=dict(size=10, color="#ff7f0e"),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.0f} Bcf<extra></extra>",
            showlegend=False,
        )
    )

    fig.add_annotation(
        x=latest[x_field],
        y=latest[y_field],
        xanchor="right",
        xshift=-18,
        yshift=-6,
        align="left",
        showarrow=True,
        arrowhead=2,
        arrowsize=1,
        arrowwidth=1.5,
        arrowcolor="#111827",
        bgcolor="rgba(255,255,255,0.96)",
        bordercolor="rgba(15,23,42,0.25)",
        borderwidth=1,
        borderpad=6,
        font=dict(size=13, color="#1f2937"),
        text=f"Latest: {_format_bcf(latest[y_field])}<br>{latest[x_field].date().isoformat()}",
    )
    fig.add_annotation(
        x=deepest[x_field],
        y=deepest[y_field],
        xanchor="left",
        yanchor="top",
        xshift=-10,
        yshift=-34,
        showarrow=False,
        bgcolor="rgba(255,255,255,0.96)",
        bordercolor="rgba(15,23,42,0.25)",
        borderwidth=1,
        borderpad=5,
        font=dict(size=12, color="#1f2937"),
        text=f"Deepest withdrawal: {_format_bcf(deepest[y_field])}",
    )


def render_plotly(
    spec: ChartSpec,
    df: pd.DataFrame,
    forecast_overlay: ForecastResult | dict | None = None,
) -> go.Figure:
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
        x_series = d[x_field]
        should_parse_datetime = (
            pd.api.types.is_datetime64_any_dtype(x_series)
            or x_field == "date"
            or str(x_field).endswith("_date")
        )
        if should_parse_datetime:
            d[x_field] = pd.to_datetime(x_series, errors="coerce")
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
        color_field = None
        if y_fields == ["value"]:
            for candidate in ("region", "series"):
                if candidate in d.columns:
                    color_field = candidate
                    break
        fig = px.line(
            d,
            x=x_field,
            y=y_fields[0] if color_field else y_fields,
            color=color_field,
            title=spec.title,
            template=template,
        )
        fig.update_traces(line=dict(width=3))

        if chart_type == "area":
            fig.update_traces(fill="tozeroy")

    elif chart_type == "bar":
        bar_x_field = x_field
        if x_is_datetime:
            # Datetime bars with month-formatted ticks can show repeated labels for
            # distinct dates within the same month. Use explicit category labels.
            bar_x_field = "__bar_x_label"
            d[bar_x_field] = _bar_datetime_label(d[x_field], spec.aggregation)
        elif x_field == "bucket":
            bar_x_field = "__bar_x_label"
            d[bar_x_field] = _bucket_label(d[x_field])

        fig = px.bar(d, x=bar_x_field, y=y_fields, title=spec.title, template=template)
        if x_is_datetime:
            fig.update_xaxes(type="category", categoryorder="array", categoryarray=d[bar_x_field].tolist())

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

    if x_is_datetime and chart_type != "bar":
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
        x_fmt = "%{x|%Y-%m-%d}" if x_is_datetime and chart_type != "bar" else "%{x}"
        y_suffix = f" {y_units}" if y_units else ""
        tr.hovertemplate = f"{x_fmt}{name_line}<br>%{{y:,.2f}}{y_suffix}<extra></extra>"

    if x_is_datetime and chart_type in {"line", "area", "stacked_area"}:
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

    if x_is_datetime and chart_type in {"line", "area"} and "by region" not in spec.title.lower():
        _apply_timeseries_dashboard_style(
            fig,
            d,
            x_field=x_field,
            y_fields=y_fields,
            y_label=spec.y_label or (y_fields[0] if y_fields else "Value"),
            y_units=y_units,
        )

    if _is_storage_change_chart(spec, y_fields) and x_field in d.columns:
        _apply_storage_change_dashboard_style(fig, d, x_field=x_field, y_field="value")

    overlay_dict = (
        forecast_overlay.to_dict()
        if isinstance(forecast_overlay, ForecastResult)
        else forecast_overlay
    )
    overlay_points = (((overlay_dict or {}).get("overlay") or {}).get("forecast") or [])
    if overlay_points and chart_type in {"line", "area", "stacked_area"}:
        overlay_df = pd.DataFrame(overlay_points)
        if not overlay_df.empty and {"date", "value"}.issubset(overlay_df.columns):
            overlay_df["date"] = pd.to_datetime(overlay_df["date"], errors="coerce")
            overlay_df["value"] = pd.to_numeric(overlay_df["value"], errors="coerce")
            overlay_df = overlay_df.dropna(subset=["date", "value"]).sort_values("date")
            if not overlay_df.empty:
                fig.add_trace(
                    go.Scatter(
                        x=overlay_df["date"],
                        y=overlay_df["value"],
                        mode="lines",
                        name="Forecast",
                        line=dict(width=3, dash="dash", color="#f97316"),
                        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f}<br>Forecast<extra></extra>",
                    )
                )
                fig.update_layout(showlegend=True)

    return fig
