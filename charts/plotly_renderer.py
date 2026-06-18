from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from schemas.chart_spec import AxisSpec, ChartSpec
from tools.forecasting import ForecastResult

CHART_BG = "#FFFFFF"


def _sign(value: float | None) -> float:
    if value is None:
        return 0.0
    if value > 0:
        return 1.0
    if value < 0:
        return -1.0
    return 0.0


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


def _category_label(series: pd.Series) -> pd.Series:
    return series.astype(str).str.replace("_", " ", regex=False)


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

    selected_columns = [date_field, value_field]
    if "region" in df.columns:
        selected_columns.append("region")
    d = df[selected_columns].copy()
    d[date_field] = pd.to_datetime(d[date_field], errors="coerce")
    d[value_field] = pd.to_numeric(d[value_field], errors="coerce")
    d = d.dropna(subset=[date_field, value_field]).sort_values(date_field)
    if d.empty:
        return []

    if "region" in d.columns:
        d["region"] = d["region"].astype(str)
        unique_regions = [region for region in d["region"].dropna().unique().tolist() if region]
        if len(unique_regions) > 1:
            latest_by_region = (
                d.sort_values(["region", date_field])
                .groupby("region", as_index=False, sort=False)
                .tail(1)
                .sort_values("region")
            )
            metrics: list[dict[str, float | str | None]] = []
            for _, row in latest_by_region.iterrows():
                region_label = str(row["region"]).replace("_", " ").title()
                metrics.append(
                    {
                        "label": f"{region_label} Latest" if len(unique_regions) == 2 else region_label,
                        "value": float(row[value_field]),
                        "unit": unit or "",
                        "subtitle": row[date_field].date().isoformat(),
                    }
                )
            if len(unique_regions) == 2 and len(latest_by_region) == 2:
                left = latest_by_region.iloc[0]
                right = latest_by_region.iloc[1]
                left_label = str(left["region"]).replace("_", " ").title()
                right_label = str(right["region"]).replace("_", " ").title()
                metrics.append(
                    {
                        "label": "Spread",
                        "value": float(right[value_field]) - float(left[value_field]),
                        "unit": unit or "",
                        "subtitle": f"{right_label} - {left_label}",
                    }
                )
            return metrics

    latest = float(d.iloc[-1][value_field])
    previous = float(d.iloc[-2][value_field]) if len(d) >= 2 else None
    period_low = float(d[value_field].min())
    period_high = float(d[value_field].max())
    latest_date = d.iloc[-1][date_field].date().isoformat()
    five_year_avg = _same_time_five_year_average(d, date_field=date_field, value_field=value_field)

    metrics = [
        {
            "label": "Latest reading",
            "value": latest,
            "unit": unit or "",
            "subtitle": latest_date,
        },
        {
            "label": "Previous",
            "value": previous,
            "unit": unit or "",
            "subtitle": "prior observation" if previous is not None else None,
        },
        {
            "label": "Low",
            "value": period_low,
            "unit": unit or "",
            "subtitle": "displayed period",
        },
        {
            "label": "High",
            "value": period_high,
            "unit": unit or "",
            "subtitle": "displayed period",
        },
    ]
    if five_year_avg is not None:
        metrics.append(
            {
                "label": "5Y Avg",
                "value": float(five_year_avg),
                "unit": unit or "",
                "subtitle": "same-time baseline",
            }
        )
    return metrics


def _same_time_five_year_average(
    df: pd.DataFrame,
    *,
    date_field: str,
    value_field: str,
) -> float | None:
    if df is None or df.empty or len(df) < 6:
        return None
    latest = df.iloc[-1]
    latest_ts = pd.Timestamp(latest[date_field])
    latest_year = int(latest_ts.year)
    deltas = df[date_field].diff().dropna().dt.total_seconds() / 86400.0
    spacing = float(deltas.median()) if not deltas.empty else 30.0

    if spacing <= 10.0:
        scoped = df.copy()
        scoped["iso_week"] = scoped[date_field].dt.isocalendar().week.astype(int)
        hist = scoped.loc[
            (scoped["iso_week"] == int(latest_ts.isocalendar().week))
            & (scoped[date_field].dt.year >= latest_year - 5)
            & (scoped[date_field].dt.year < latest_year)
        ]
    elif spacing <= 45.0:
        hist = df.loc[
            (df[date_field].dt.month == latest_ts.month)
            & (df[date_field].dt.year >= latest_year - 5)
            & (df[date_field].dt.year < latest_year)
        ]
    else:
        doy = int(latest_ts.dayofyear)
        hist = df.loc[
            (df[date_field].dt.year >= latest_year - 5)
            & (df[date_field].dt.year < latest_year)
            & ((df[date_field].dt.dayofyear - doy).abs() <= 3)
        ]

    values = pd.to_numeric(hist[value_field], errors="coerce").dropna()
    if len(values) < 3:
        return None
    return float(values.mean())


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


def _add_latest_annotation_only(
    fig: go.Figure,
    d: pd.DataFrame,
    *,
    x_field: str,
    y_field: str,
    y_units: str | None,
) -> None:
    series = d[[x_field, y_field]].copy()
    series[x_field] = pd.to_datetime(series[x_field], errors="coerce")
    series[y_field] = pd.to_numeric(series[y_field], errors="coerce")
    series = series.dropna(subset=[x_field, y_field]).sort_values(x_field)
    if series.empty:
        return
    latest = series.iloc[-1]
    y_suffix = f" {y_units}" if y_units else ""
    latest_value = f"{float(latest[y_field]):,.2f}{y_suffix}".strip()
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
        text=f"Latest: {latest_value}<br>{latest[x_field].date().isoformat()}",
    )


def _storage_latest_by_region(d: pd.DataFrame, value_field: str) -> pd.DataFrame:
    scoped = d.copy()
    if "date" in scoped.columns:
        scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
        scoped = scoped.dropna(subset=["date"])
        group_field = "region" if "region" in scoped.columns else "state"
        scoped = scoped.sort_values([group_field, "date"])
    scoped[value_field] = pd.to_numeric(scoped[value_field], errors="coerce")
    group_field = "region" if "region" in scoped.columns else "state"
    scoped = scoped.dropna(subset=[group_field, value_field])
    if scoped.empty:
        return scoped
    if "date" not in scoped.columns:
        return scoped.sort_values(value_field, ascending=False)
    return scoped.groupby(group_field, as_index=False, sort=False).tail(1).reset_index(drop=True)


def _render_storage_timeseries(spec: ChartSpec, d: pd.DataFrame) -> go.Figure | None:
    if spec.chart_type != "line" or not {"date", "value"}.issubset(d.columns):
        return None
    if len(_y_fields(spec.y)) != 1 or _y_fields(spec.y) != ["value"]:
        return None

    scoped = d.copy()
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    scoped["value"] = pd.to_numeric(scoped["value"], errors="coerce")
    required = ["date", "value"]
    color_field = None
    if "region" in scoped.columns:
        required.append("region")
        color_field = "region"
    elif "state" in scoped.columns:
        required.append("state")
        color_field = "state"
    elif "storage_type" in scoped.columns:
        required.append("storage_type")
        color_field = "storage_type"
    scoped = scoped.dropna(subset=required)
    sort_cols = ["date"]
    if color_field:
        sort_cols = [color_field, "date"]
    scoped = scoped.sort_values(sort_cols)
    if scoped.empty:
        return None

    fig = px.line(
        scoped,
        x="date",
        y="value",
        color=color_field,
        title=spec.title,
        template="plotly_white",
    )
    unit = "%" if "%" in str(spec.y_label or "") else "MMcf" if "MMcf" in str(spec.y_label or "") else "Bcf"
    hover_template = f"%{{x|%Y-%m-%d}}<br>%{{y:,.0f}} {unit}<extra></extra>"
    if color_field:
        hover_template = f"%{{x|%Y-%m-%d}}<br>%{{fullData.name}}<br>%{{y:,.0f}} {unit}<extra></extra>"
    fig.update_traces(line=dict(width=3), hovertemplate=hover_template)
    if color_field:
        for trace in fig.data:
            if color_field == "state":
                trace.name = "U.S." if str(trace.name) == "united_states_total" else str(trace.name).upper()
            elif color_field == "storage_type":
                trace.name = str(trace.name).replace("_", " ")
            else:
                trace.name = str(trace.name).replace("_", " ").title()
    fig.update_layout(
        height=650,
        margin=dict(l=30, r=20, t=120, b=40),
        xaxis_title=spec.x_label or "Date",
        yaxis_title=spec.y_label or "Storage (Bcf)",
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.02),
        title=dict(x=0.02, xanchor="left", font=dict(size=20), pad=dict(b=18)),
    )
    if _is_storage_change_chart(spec, ["value"]):
        _apply_storage_change_dashboard_style(fig, scoped, x_field="date", y_field="value")
    return fig


def _render_storage_region_bar(spec: ChartSpec, d: pd.DataFrame) -> go.Figure | None:
    group_field = "region" if "region" in d.columns else "state" if "state" in d.columns else None
    if spec.chart_type != "bar" or group_field is None or "value" not in d.columns:
        return None
    scoped = _storage_latest_by_region(d, "value").sort_values("value", ascending=False)
    if scoped.empty:
        return None
    unit = "%" if "%" in str(spec.y_label or "") else "MMcf" if "MMcf" in str(spec.y_label or "") else "Bcf"
    display_field = group_field
    scoped[display_field] = scoped[display_field].astype(str)
    if group_field == "region":
        scoped[display_field] = scoped[display_field].str.replace("_", " ", regex=False)
    else:
        scoped[display_field] = scoped[display_field].replace({"united_states_total": "U.S."}).str.upper()
        scoped.loc[scoped[display_field] == "U.S.", display_field] = "U.S."

    horizontal = len(scoped) > 5
    if horizontal:
        scoped = scoped.sort_values("value", ascending=True)
        fig = go.Figure(
            data=[
                go.Bar(
                    x=scoped["value"],
                    y=scoped[display_field],
                    orientation="h",
                    hovertemplate=f"%{{y}}<br>%{{x:,.0f}} {unit}<extra></extra>",
                )
            ]
        )
        x_title, y_title = spec.y_label or unit, spec.x_label or ("State" if group_field == "state" else "Region")
    else:
        fig = go.Figure(
            data=[
                go.Bar(
                    x=scoped[display_field],
                    y=scoped["value"],
                    hovertemplate=f"%{{x}}<br>%{{y:,.0f}} {unit}<extra></extra>",
                )
            ]
        )
        x_title, y_title = spec.x_label or ("State" if group_field == "state" else "Region"), spec.y_label or unit
    fig.update_layout(
        title=spec.title,
        template="plotly_white",
        height=500,
        margin=dict(l=70 if horizontal else 30, r=20, t=120, b=40),
        xaxis_title=x_title,
        yaxis_title=y_title,
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
    )
    return fig


def _render_storage_deviation_bar(spec: ChartSpec, d: pd.DataFrame) -> go.Figure | None:
    if spec.chart_type != "bar" or not {"region", "deviation_bcf"}.issubset(d.columns):
        return None
    scoped = d.copy()
    scoped["deviation_bcf"] = pd.to_numeric(scoped["deviation_bcf"], errors="coerce")
    scoped = scoped.dropna(subset=["region", "deviation_bcf"])
    if scoped.empty:
        return None
    scoped["region"] = scoped["region"].astype(str).str.replace("_", " ").str.title()
    colors = ["#d62728" if v < 0 else "#2ca02c" for v in scoped["deviation_bcf"]]
    fig = go.Figure(
        data=[
            go.Bar(
                x=scoped["deviation_bcf"],
                y=scoped["region"],
                orientation="h",
                marker_color=colors,
                hovertemplate="%{y}<br>%{x:+,.0f} Bcf vs 5Y Avg<extra></extra>",
            )
        ]
    )
    fig.add_vline(x=0, line_width=1.5, line_color="rgba(0,0,0,0.45)")
    fig.update_layout(
        title=spec.title,
        template="plotly_white",
        height=500,
        margin=dict(l=80, r=20, t=120, b=40),
        xaxis_title=spec.y_label or "Bcf vs 5Y Avg",
        yaxis_title=spec.x_label or "Region",
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
    )
    return fig


def _render_storage_seasonal_line(spec: ChartSpec, d: pd.DataFrame) -> go.Figure | None:
    if spec.chart_type != "seasonal_line" or not {"date", "value", "five_year_avg"}.issubset(d.columns):
        return None
    scoped = d.copy()
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    for col in ("value", "five_year_avg", "five_year_min", "five_year_max"):
        if col in scoped.columns:
            scoped[col] = pd.to_numeric(scoped[col], errors="coerce")
    scoped = scoped.dropna(subset=["date", "value", "five_year_avg"]).sort_values("date")
    if scoped.empty:
        return None

    fig = go.Figure()
    if {"five_year_min", "five_year_max"}.issubset(scoped.columns):
        band = scoped.dropna(subset=["five_year_min", "five_year_max"])
        if not band.empty:
            fig.add_trace(
                go.Scatter(
                    x=band["date"],
                    y=band["five_year_max"],
                    mode="lines",
                    line=dict(width=0),
                    name="5Y max",
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=band["date"],
                    y=band["five_year_min"],
                    mode="lines",
                    fill="tonexty",
                    fillcolor="rgba(148,163,184,0.25)",
                    line=dict(width=0),
                    name="5Y range",
                    hovertemplate="%{x|%Y-%m-%d}<br>5Y range<extra></extra>",
                )
            )
    fig.add_trace(
        go.Scatter(
            x=scoped["date"],
            y=scoped["five_year_avg"],
            mode="lines",
            name="5-year average",
            line=dict(width=3, dash="dash", color="#64748b"),
            hovertemplate="%{x|%Y-%m-%d}<br>5-year avg: %{y:,.0f} Bcf<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=scoped["date"],
            y=scoped["value"],
            mode="lines",
            name="Storage",
            line=dict(width=3, color="#2563eb"),
            hovertemplate="%{x|%Y-%m-%d}<br>Storage: %{y:,.0f} Bcf<extra></extra>",
        )
    )
    fig.update_layout(
        title=spec.title,
        template="plotly_white",
        height=650,
        margin=dict(l=30, r=20, t=120, b=40),
        xaxis_title=spec.x_label or "Date",
        yaxis_title=spec.y_label or "Bcf",
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.02),
    )
    return fig


def _render_storage_chart(spec: ChartSpec, d: pd.DataFrame) -> go.Figure | None:
    for renderer in (
        _render_storage_seasonal_line,
        _render_storage_deviation_bar,
        _render_storage_region_bar,
        _render_storage_timeseries,
    ):
        fig = renderer(spec, d)
        if fig is not None:
            return fig
    return None


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
    storage_fig = _render_storage_chart(spec, d)
    if storage_fig is not None:
        overlay_dict = (
            forecast_overlay.to_dict()
            if isinstance(forecast_overlay, ForecastResult)
            else forecast_overlay
        )
        overlay = (overlay_dict or {}).get("overlay") or {}
        historical_points = overlay.get("historical") or []
        forecast_points = overlay.get("forecast") or []
        if historical_points and spec.chart_type in {"line", "area", "stacked_area"}:
            historical_df = pd.DataFrame(historical_points)
            if not historical_df.empty and {"date", "value"}.issubset(historical_df.columns):
                historical_df["date"] = pd.to_datetime(historical_df["date"], errors="coerce")
                historical_df["value"] = pd.to_numeric(historical_df["value"], errors="coerce")
                historical_df = historical_df.dropna(subset=["date", "value"]).sort_values("date")
                if not historical_df.empty:
                    storage_fig.add_trace(
                        go.Scatter(
                            x=historical_df["date"],
                            y=historical_df["value"],
                            mode="lines",
                            name="Historical trend",
                            line=dict(width=2, dash="dot", color="#94a3b8"),
                            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f}<br>Historical trend<extra></extra>",
                        )
                    )
        if forecast_points and spec.chart_type in {"line", "area", "stacked_area"}:
            overlay_df = pd.DataFrame(forecast_points)
            if not overlay_df.empty and {"date", "value"}.issubset(overlay_df.columns):
                overlay_df["date"] = pd.to_datetime(overlay_df["date"], errors="coerce")
                overlay_df["value"] = pd.to_numeric(overlay_df["value"], errors="coerce")
                overlay_df = overlay_df.dropna(subset=["date", "value"]).sort_values("date")
                if not overlay_df.empty:
                    storage_fig.add_trace(
                        go.Scatter(
                            x=overlay_df["date"],
                            y=overlay_df["value"],
                            mode="lines",
                            name="Forecast",
                            line=dict(width=3, dash="dash", color="#f97316"),
                            hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f}<br>Forecast<extra></extra>",
                        )
                    )
                    storage_fig.update_layout(showlegend=True)
        return storage_fig

    if (
        spec.title == "Market Pressure Dashboard"
        and {
            "weather_demand_delta_bcfd",
            "storage_surprise_bcf",
            "lng_delta_mmcf",
            "production_delta_mmcf",
            "price_delta_usd_mmbtu",
        }.issubset(d.columns)
    ):
        row = d.iloc[-1]
        weather_delta = pd.to_numeric(row.get("weather_demand_delta_bcfd"), errors="coerce")
        storage_surprise = pd.to_numeric(row.get("storage_surprise_bcf"), errors="coerce")
        lng_delta = pd.to_numeric(row.get("lng_delta_mmcf"), errors="coerce")
        production_delta = pd.to_numeric(row.get("production_delta_mmcf"), errors="coerce")
        price_delta = pd.to_numeric(row.get("price_delta_usd_mmbtu"), errors="coerce")

        components = pd.DataFrame(
            [
                {
                    "component": "Weather",
                    "score": _sign(None if pd.isna(weather_delta) else float(weather_delta)),
                    "detail": (
                        f"{float(weather_delta):+,.2f} Bcf/d"
                        if not pd.isna(weather_delta)
                        else "n/a"
                    ),
                },
                {
                    "component": "Storage",
                    "score": _sign(None if pd.isna(storage_surprise) else -float(storage_surprise)),
                    "detail": (
                        f"{float(storage_surprise):+,.1f} Bcf surprise"
                        if not pd.isna(storage_surprise)
                        else "n/a"
                    ),
                },
                {
                    "component": "LNG / Supply",
                    "score": _sign(
                        None
                        if pd.isna(lng_delta) and pd.isna(production_delta)
                        else -float((0.0 if pd.isna(lng_delta) else lng_delta) + (0.0 if pd.isna(production_delta) else production_delta))
                    ),
                    "detail": (
                        f"LNG {float(lng_delta):+,.0f}, Prod {float(production_delta):+,.0f} MMcf"
                        if (not pd.isna(lng_delta) or not pd.isna(production_delta))
                        else "n/a"
                    ),
                },
                {
                    "component": "Price",
                    "score": _sign(None if pd.isna(price_delta) else float(price_delta)),
                    "detail": (
                        f"{float(price_delta):+,.2f} $/MMBtu"
                        if not pd.isna(price_delta)
                        else "n/a"
                    ),
                },
            ]
        )

        components["color"] = components["score"].map(
            lambda s: "#2ca02c" if s > 0 else "#d62728" if s < 0 else "#9ca3af"
        )

        fig = go.Figure(
            data=[
                go.Bar(
                    x=components["component"],
                    y=components["score"],
                    marker_color=components["color"],
                    text=components["detail"],
                    textposition="outside",
                    hovertemplate="%{x}<br>Score: %{y:+.0f}<br>%{text}<extra></extra>",
                )
            ]
        )
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=30, r=20, t=120, b=40),
            height=500,
            xaxis_title=spec.x_label or "Driver",
            yaxis_title=spec.y_label or "Pressure Score",
            title=dict(x=0.02, xanchor="left", font=dict(size=20), pad=dict(b=18)),
            plot_bgcolor="white",
            paper_bgcolor="white",
            showlegend=False,
        )
        fig.update_yaxes(
            range=[-1.4, 1.4],
            tickmode="array",
            tickvals=[-1, 0, 1],
            ticktext=["Bearish", "Neutral", "Bullish"],
            showgrid=True,
            gridcolor="rgba(0,0,0,0.08)",
            zeroline=True,
            zerolinecolor="rgba(0,0,0,0.35)",
        )
        fig.update_xaxes(showgrid=False)
        return fig

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
            for candidate in ("region", "series", "storage_type"):
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
        elif x_field in d.columns and pd.api.types.is_object_dtype(d[x_field]):
            bar_x_field = "__bar_x_label"
            d[bar_x_field] = _category_label(d[x_field])

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
    historical_points = (((overlay_dict or {}).get("overlay") or {}).get("historical") or [])
    if historical_points and chart_type in {"line", "area", "stacked_area"}:
        historical_df = pd.DataFrame(historical_points)
        if not historical_df.empty and {"date", "value"}.issubset(historical_df.columns):
            historical_df["date"] = pd.to_datetime(historical_df["date"], errors="coerce")
            historical_df["value"] = pd.to_numeric(historical_df["value"], errors="coerce")
            historical_df = historical_df.dropna(subset=["date", "value"]).sort_values("date")
            if not historical_df.empty:
                fig.add_trace(
                    go.Scatter(
                        x=historical_df["date"],
                        y=historical_df["value"],
                        mode="lines",
                        name="Historical trend",
                        line=dict(width=2, dash="dot", color="#94a3b8"),
                        hovertemplate="%{x|%Y-%m-%d}<br>%{y:,.2f}<br>Historical trend<extra></extra>",
                    )
                )
                fig.update_layout(showlegend=True)
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

    if (
        x_is_datetime
        and chart_type in {"line", "area"}
        and "by region" not in spec.title.lower()
        and len(fig.layout.annotations or ()) == 0
    ):
        _add_latest_annotation_only(
            fig,
            d,
            x_field=x_field,
            y_field=y_fields[0] if y_fields else "value",
            y_units=y_units,
        )

    return fig
