# atlas/answer_builder.py
# atlas/answer_builder.py (or wherever _make_preview lives)
from __future__ import annotations

from functools import lru_cache
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from answers.chart_policy import chart_policy
from answers.features import (
    ng_electricity_seasonal_norm_summary as compute_ng_electricity_seasonal_norm_summary,
)
from answers.features import should_compute_ng_electricity_seasonal_norm
from answers.response_formatters.natural_gas import (
    NaturalGasMetricSnapshot,
    format_directional_change,
    format_date_month_d_year,
    format_period_comparison,
    format_natural_gas_commentary,
)
try:
    from alerts.services import get_builtin_signal_registry, is_builtin_signal_id
except Exception as exc:
    if not isinstance(exc, (ImportError, ModuleNotFoundError)) and exc.__class__.__name__ != "ImproperlyConfigured":
        raise

    def get_builtin_signal_registry() -> dict:
        return {}

    def is_builtin_signal_id(signal_id: str) -> bool:
        return False
from openai import OpenAI
from schemas.answer import (
    AnswerAlert,
    AnswerDataPoint,
    AnswerForecast,
    AnswerPayload,
    AnswerSourceSummary,
    DataPreview,
    SignalSummary,
    SuggestedAlert,
    StructuredAnswer,
)
from schemas.chart_spec import ChartSpec
from scripts.eia.rag.prompt_context import format_report_context
from scripts.eia.rag.retrieval import (
    load_report_chunks,
    search_report_chunks,
    should_use_report_rag,
)
from tools.eia_adapter import EIAResult

SYSTEM_INSTRUCTIONS = (
    "You are Energy Atlas AI. Return a compact analyst briefing as valid JSON.\n"
    "Use Structured Facts as the authoritative source for current values and computed facts.\n"
    "Use Report Context only for narrative background, drivers, and recent report framing.\n"
    "If Report Context conflicts with Structured Facts, prefer Structured Facts.\n"
    "Do not invent report content that is not present in the retrieved chunks.\n"
    "Cite report titles and dates in the prose when useful.\n"
    "Required keys: answer, signal, summary, drivers, data_points, forecast, suggested_alerts, alerts, sources.\n"
    "signal.status must be bullish, bearish, or neutral.\n"
    "signal.confidence must be a float between 0 and 1.\n"
    "drivers, data_points, suggested_alerts, alerts, and sources must be arrays.\n"
    "Suggested alerts must only be included when monitoring would help a future decision.\n"
    "Suggested alerts must contain title, reason, signal_id, and priority.\n"
    "Return 1 to 3 suggested alerts at most, or an empty array if no alert is useful.\n"
    "Keep summary concise and scannable."
)

client = OpenAI()  # expects OPENAI_API_KEY in env
logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parent

METRIC_UNITS = {
    "henry_hub_spot": "$/MMBtu",
    "working_gas_storage_lower48": "Bcf",
    "working_gas_storage_change_weekly": "Bcf",
    "lng_exports": "MMcf",
    "lng_imports": "MMcf",
    "ng_electricity": "MMcf",
    "ng_consumption_lower48": "MMcf",
    "ng_consumption_by_sector": "MMcf",
    "ng_production_lower48": "MMcf",
    "ng_supply_balance_regime": "index",
    "weather_degree_days_forecast_vs_5y": "degree-days",
    "weather_regional_demand_drivers": "Bcf/d",
    "weekly_energy_atlas_summary": "index",
}

SECTOR_LABELS = {
    "residential": "residential",
    "commercial": "commercial",
    "industrial": "industrial",
    "electric_power": "power",
}


def _report_chunks_candidates() -> list[Path]:
    env_path = os.getenv("REPORT_CHUNKS_PATH", "").strip()
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(
        REPO_ROOT
        / "data"
        / "processed"
        / "eia"
        / "ng"
        / "crawlers"
        / "report_chunks.jsonl"
    )
    candidates.append(
        REPO_ROOT
        / "scripts"
        / "eia"
        / "crawlers"
        / "output"
        / "report_chunks.jsonl"
    )
    return candidates


def _resolve_report_chunks_path() -> Path:
    for candidate in _report_chunks_candidates():
        if candidate.exists():
            return candidate
    return _report_chunks_candidates()[0]


def _dedupe_report_sources(chunks: list[dict]) -> list[AnswerSourceSummary]:
    seen: set[tuple[str, Optional[str]]] = set()
    sources: list[AnswerSourceSummary] = []
    for chunk in chunks:
        title = str(chunk.get("title") or "").strip()
        date_value = (
            str(
                chunk.get("published_date")
                or chunk.get("release_date")
                or chunk.get("period_ending")
                or ""
            ).strip()
            or None
        )
        key = (title, date_value)
        if not title or key in seen:
            continue
        seen.add(key)
        sources.append(AnswerSourceSummary(title=title, date=date_value))
    return sources


def _is_report_narrative_query(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    cues = (
        "report",
        "tell us",
        "what does",
        "market balance",
        "fundamentals",
        "tightening",
        "loosening",
        "implication",
    )
    return any(cue in q for cue in cues)


def _build_report_rag_context(
    query: str, *, top_k: int = 4
) -> tuple[str, list[AnswerSourceSummary], bool, str]:
    if not should_use_report_rag(query):
        logger.info("report_rag used=false reason=heuristic query=%r", query)
        return "", [], False, "heuristic_not_triggered"

    report_chunks_path = _resolve_report_chunks_path()
    chunks = _cached_report_chunks(str(report_chunks_path))
    if not chunks:
        logger.info(
            "report_rag used=false reason=missing_or_empty path=%s query=%r",
            report_chunks_path,
            query,
        )
        return "", [], False, "missing_or_empty_chunk_file"

    matches = search_report_chunks(query, chunks, top_k=top_k)
    if not matches:
        logger.info(
            "report_rag used=false reason=no_matches path=%s query=%r",
            report_chunks_path,
            query,
        )
        return "", [], False, "no_retrieval_matches"

    sources = _dedupe_report_sources(matches)
    logger.info(
        "report_rag used=true chunks=%d titles=%s",
        len(matches),
        [source.title for source in sources[:3]],
    )
    return format_report_context(matches), sources, True, "retrieval_matches_found"


@lru_cache(maxsize=4)
def _cached_report_chunks(report_chunks_path: str) -> tuple[dict, ...]:
    chunks = load_report_chunks(report_chunks_path)
    return tuple(chunks)


def _json_safe(v: Any) -> Any:
    # pandas Timestamp / datetime64
    if isinstance(v, pd.Timestamp):
        # Date-only series: keep it clean
        return v.date().isoformat() if v.tzinfo is None else v.isoformat()

    # pandas missing datetime
    if v is pd.NaT:
        return None

    # numpy scalars
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        x = float(v)
        if np.isnan(x) or np.isinf(x):
            return None
        return x

    # python float NaN/inf
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None

    return v


def _make_preview(df: pd.DataFrame, n: int | None = 10) -> DataPreview:
    preview_df = df if n is None else df.tail(n)

    # convert the preview to python objects (keeps Timestamps as pd.Timestamp)
    rows = preview_df.to_numpy(dtype=object).tolist()
    rows = [[_json_safe(v) for v in row] for row in rows]

    return DataPreview(
        columns=list(preview_df.columns),
        rows=rows,
        units=None,
    )


def _make_chart_preview(df: pd.DataFrame | None) -> DataPreview | None:
    if df is None:
        return None
    return _make_preview(df, n=None)


def _should_include_data_preview() -> bool:
    explicit = os.getenv("ATLAS_INCLUDE_DATA_PREVIEW", "").strip().lower()
    if explicit in {"1", "true", "yes", "on"}:
        return True
    if explicit in {"0", "false", "no", "off"}:
        return False
    response_mode = os.getenv("ATLAS_RESPONSE_MODE", "fast").strip().lower()
    return response_mode in {"analysis", "detailed"}


def _maybe_data_preview(df: pd.DataFrame | None) -> DataPreview | None:
    if df is None or not _should_include_data_preview():
        return None
    return _make_preview(df)


def _safe_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except Exception:
        return None


def _pick_fact_value_col(df: pd.DataFrame, metric: str) -> Optional[str]:
    if "value" in df.columns:
        return "value"

    metric_defaults = {
        "iso_load": "value",
        "iso_gas_dependency": "gas_share",
        "iso_fuel_mix": "total_generation_mw",
    }
    preferred = metric_defaults.get(metric)
    if preferred and preferred in df.columns:
        return preferred

    numeric_cols = [
        c for c in df.columns if c != "date" and pd.api.types.is_numeric_dtype(df[c])
    ]
    if not numeric_cols:
        return None
    return numeric_cols[0]


def _format_number(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    abs_value = abs(value)
    if abs_value >= 100:
        return f"{value:,.0f}"
    if abs_value >= 10:
        return f"{value:,.1f}"
    return f"{value:,.2f}"


def _format_delta(delta: Optional[float], unit: Optional[str]) -> Optional[str]:
    if delta is None:
        return None
    if delta == 0:
        return "remained relatively stable versus the prior reporting period"
    unit_suffix = f" {unit}" if unit else ""
    direction, phrase = format_directional_change("consumption", delta, None)
    if direction == "up":
        return f"{phrase} by {_format_number(abs(delta))}{unit_suffix} {format_period_comparison()}"
    return f"{phrase} by {_format_number(abs(delta))}{unit_suffix} {format_period_comparison()}"


def _titleize_metric(metric: str) -> str:
    text = (metric or "").replace("_", " ").strip()
    acronyms = {"lng": "LNG", "ng": "Natural Gas", "hub": "Hub"}
    parts = []
    for part in text.split():
        parts.append(acronyms.get(part.lower(), part.capitalize()))
    return " ".join(parts) or "Metric"


def _signal_from_delta(delta: Optional[float]) -> tuple[str, float]:
    if delta is None:
        return "neutral", 0.5
    if delta > 0:
        return "bullish", 0.82
    if delta < 0:
        return "bearish", 0.82
    return "neutral", 0.68


def _forecast_direction_from_delta(delta: Optional[float]) -> str:
    if delta is None or delta == 0:
        return "flat"
    return "up" if delta > 0 else "down"


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
    elif isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        text = " ".join(parts)
    else:
        text = str(value).strip()

    lowered = text.lower()
    if lowered.startswith("summary:"):
        text = text[len("summary:") :].strip()
    return text


def _suggested_alert_catalog_text() -> str:
    registry = get_builtin_signal_registry()
    lines = []
    for signal_id, config in registry.items():
        lines.append(
            f"- {signal_id}: {config['title']} | question: {config['question']}"
        )
    return "\n".join(lines)


def _is_suggested_alert_relevant(*, signal_id: str, metric: str, query: str) -> bool:
    q = (query or "").strip().lower()
    if signal_id == "routed_metric_query":
        return False

    # Snapshot prompts should not propose monitoring-heavy alerts unless user asks
    # for comparative or forward-looking context.
    snapshot_prompt = any(token in q for token in ("right now", "current", "latest", "today"))

    signal_query_terms = {
        "storage_below_five_year_average_pct": (
            "5-year",
            "five-year",
            "average",
            "normal",
            "deficit",
            "below",
            "tight",
        ),
        "storage_deficit_widening_wow": (
            "widening",
            "week-over-week",
            "wow",
            "trend",
            "deficit",
        ),
        "hdd_above_normal_this_week": (
            "hdd",
            "heating degree",
            "weather",
            "colder",
            "cold",
            "normal",
        ),
        "production_below_30d_average": (
            "production",
            "output",
            "supply",
            "30d",
            "30-day",
            "average",
        ),
        "supply_constrained_regime": (
            "supply",
            "constrained",
            "tight",
            "balance",
            "risk",
        ),
    }
    terms = signal_query_terms.get(signal_id, ())
    if terms and not any(term in q for term in terms):
        return False

    if signal_id.startswith("storage_"):
        if metric not in {"working_gas_storage_lower48", "working_gas_storage_change_weekly"}:
            return False
        if snapshot_prompt and signal_id != "storage_below_five_year_average_pct":
            return False
    if signal_id == "hdd_above_normal_this_week" and metric != "weather_degree_days_forecast_vs_5y":
        return False
    if signal_id == "production_below_30d_average" and metric != "ng_production_lower48":
        return False

    return True


def _normalize_structured_response(payload: dict[str, Any], *, metric: str, query: str) -> StructuredAnswer:
    signal = payload.get("signal") or {}
    if not isinstance(signal, dict):
        signal = {}
    forecast = payload.get("forecast") or {}
    if not isinstance(forecast, dict):
        forecast = {}
    suggested_alerts = []
    suggested_alert_items = payload.get("suggested_alerts")
    if not isinstance(suggested_alert_items, list):
        suggested_alert_items = []
    for item in suggested_alert_items:
        if not isinstance(item, dict):
            continue
        signal_id = str(item.get("signal_id") or "").strip()
        if not is_builtin_signal_id(signal_id):
            continue
        if not _is_suggested_alert_relevant(signal_id=signal_id, metric=metric, query=query):
            continue
        title = _coerce_text(item.get("title"))
        reason = _coerce_text(item.get("reason"))
        if not title or not reason:
            continue
        suggested_alerts.append(
            SuggestedAlert(
                title=title,
                reason=reason,
                signal_id=signal_id,
                priority=str(item.get("priority") or "medium").strip() or "medium",
            )
        )

    drivers_items = payload.get("drivers")
    if not isinstance(drivers_items, list):
        drivers_items = []
    data_point_items = payload.get("data_points")
    if not isinstance(data_point_items, list):
        data_point_items = []
    alerts_items = payload.get("alerts")
    if not isinstance(alerts_items, list):
        alerts_items = []
    source_items = payload.get("sources")
    if not isinstance(source_items, list):
        source_items = []

    return StructuredAnswer(
        answer=_coerce_text(payload.get("answer")),
        signal=SignalSummary(
            status=str(signal.get("status") or "neutral").lower(),
            confidence=max(0.0, min(1.0, float(signal.get("confidence") or 0.5))),
        ),
        summary=_coerce_text(payload.get("summary")),
        drivers=[
            _coerce_text(driver)
            for driver in drivers_items
            if _coerce_text(driver)
        ],
        data_points=[
            AnswerDataPoint(
                metric=str(item.get("metric") or "").strip(),
                value=item.get("value"),
                unit=str(item.get("unit") or "").strip(),
            )
            for item in data_point_items
            if isinstance(item, dict)
        ],
        forecast=AnswerForecast(
            direction=str(forecast.get("direction") or "flat").strip(),
            reasoning=str(forecast.get("reasoning") or "").strip(),
        ),
        suggested_alerts=suggested_alerts,
        alerts=[
            AnswerAlert(
                name=str(item.get("name") or "").strip(),
                status=_coerce_bool(item.get("status")),
            )
            for item in alerts_items
            if isinstance(item, dict) and str(item.get("name") or "").strip()
        ],
        sources=[
            AnswerSourceSummary(
                title=str(item.get("title") or "").strip(),
                date=str(item.get("date") or "").strip() or None,
            )
            for item in source_items
            if isinstance(item, dict) and str(item.get("title") or "").strip()
        ],
    )


def _is_low_value_no_context_text(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    bad_phrases = (
        "driver attribution not supported",
        "no narrative",
        "no fundamental context",
        "context retrieved",
        "cannot attribute",
    )
    return any(phrase in t for phrase in bad_phrases)


def _data_only_driver(metric: str, facts: dict[str, Any]) -> str:
    latest_value = facts.get("latest_value")
    delta = facts.get("delta")
    unit = METRIC_UNITS.get(metric, "")
    latest_value_text = _format_number(latest_value)
    delta_text = _format_delta(delta, unit)
    unit_suffix = f" {unit}" if unit else ""
    if latest_value is None:
        return "No narrative report context was retrieved; this answer uses observed market data only."
    if delta_text:
        return f"Observed move only: latest value is {latest_value_text}{unit_suffix}, {delta_text}."
    return f"Observed move only: latest value is {latest_value_text}{unit_suffix}."


def _improve_no_context_language(
    *,
    structured_response: StructuredAnswer,
    metric: str,
    facts: dict[str, Any],
    report_context_used: bool,
) -> StructuredAnswer:
    if report_context_used:
        return structured_response

    cleaned_drivers = [
        d for d in structured_response.drivers if not _is_low_value_no_context_text(d)
    ]
    if not cleaned_drivers:
        cleaned_drivers = [_data_only_driver(metric, facts)]
    elif all(_is_low_value_no_context_text(d) for d in structured_response.drivers):
        cleaned_drivers = [_data_only_driver(metric, facts)]

    summary = structured_response.summary
    if _is_low_value_no_context_text(summary):
        summary = (
            f"{_coerce_text(structured_response.answer)} "
            "Narrative report context was not retrieved, so this is based on observed market data."
        ).strip()

    answer = structured_response.answer
    if _is_low_value_no_context_text(answer):
        answer = (
            f"{_coerce_text(structured_response.summary)} "
            "Narrative report context was not retrieved, so this is based on observed market data."
        ).strip()

    return structured_response.model_copy(
        update={
            "drivers": cleaned_drivers,
            "summary": summary,
            "answer": answer,
        }
    )


def _build_structured_answer(
    *,
    metric: str,
    query: str,
    df: pd.DataFrame | None,
    facts: dict[str, Any],
    mode: str,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    latest_date = facts.get("latest_date")
    latest_value = facts.get("latest_value")
    delta = facts.get("delta")
    unit = METRIC_UNITS.get(metric, "")
    metric_label = _titleize_metric(metric)
    status, confidence = _signal_from_delta(delta)
    direction = _forecast_direction_from_delta(delta)

    if facts.get("n_points", 0) == 0 or latest_date is None or latest_value is None:
        return StructuredAnswer(
            answer="No data was returned for the requested period.",
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No usable observations were returned for the requested period.",
            drivers=["The query executed successfully but returned no observations."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no observations were returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    latest_value_text = _format_number(latest_value)
    summary = _deterministic_answer_text(
        metric=metric,
        query=query,
        facts=facts,
        mode=mode,
        df=df,
    )
    drivers = [f"Latest {metric_label} reading was {latest_value_text} {unit}".strip()]
    if delta is not None:
        drivers.append(_format_delta(delta, unit) or "No change from the previous observation.")
    else:
        drivers.append("No prior observation was available for a delta comparison.")

    alerts = []
    if delta is not None:
        alerts.append(
            AnswerAlert(
                name=f"{metric_label} {'increased' if delta > 0 else 'decreased' if delta < 0 else 'was unchanged'}",
                status=delta != 0,
            )
        )

    return StructuredAnswer(
        answer=summary,
        signal=SignalSummary(status=status, confidence=confidence),
        summary=summary,
        drivers=drivers,
        data_points=[
            AnswerDataPoint(metric=metric_label, value=_json_safe(latest_value), unit=unit)
        ],
        forecast=AnswerForecast(
            direction=direction,
            reasoning=(
                "Near-term direction is inferred from the latest observation delta."
                if delta is not None
                else "Near-term direction is flat because only one observation was available."
            ),
        ),
        suggested_alerts=[],
        alerts=alerts,
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _extract_json_object(text: str) -> Optional[dict[str, Any]]:
    candidate = (text or "").strip()
    if not candidate:
        return None
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", candidate, re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def _deterministic_sector_consumption_answer(query: str, df: pd.DataFrame) -> str:
    if df is None or df.empty or "date" not in df.columns or "value" not in df.columns:
        return "No data was returned for the requested period."

    latest_date = pd.to_datetime(df["date"], errors="coerce").max()
    if pd.isna(latest_date):
        return "No data was returned for the requested period."

    latest_rows = df.loc[pd.to_datetime(df["date"], errors="coerce") == latest_date].copy()
    if latest_rows.empty or "series" not in latest_rows.columns:
        return "No data was returned for the requested period."

    requested_sectors: list[str] = []
    q = (query or "").lower()
    sector_terms = {
        "electric_power": ("power", "electric power"),
        "residential": ("residential",),
        "industrial": ("industrial",),
        "commercial": ("commercial",),
    }
    for sector, terms in sector_terms.items():
        if any(term in q for term in terms):
            requested_sectors.append(sector)

    latest_rows["series"] = latest_rows["series"].astype(str)
    if requested_sectors:
        latest_rows = latest_rows[latest_rows["series"].isin(requested_sectors)]

    latest_rows = latest_rows.dropna(subset=["value"])
    if latest_rows.empty:
        return "No data was returned for the requested period."

    latest_rows["value"] = pd.to_numeric(latest_rows["value"], errors="coerce")
    latest_rows = latest_rows.dropna(subset=["value"]).sort_values("value", ascending=False)
    if latest_rows.empty:
        return "No data was returned for the requested period."

    leader = latest_rows.iloc[0]
    ranking = ", ".join(
        f"{SECTOR_LABELS.get(str(row['series']), str(row['series']))} ({_format_number(float(row['value']))} MMcf)"
        for _, row in latest_rows.iterrows()
    )
    leader_label = SECTOR_LABELS.get(str(leader["series"]), str(leader["series"]))
    return (
        f"As of {latest_date.date().isoformat()}, the {leader_label} sector consumed the most gas "
        f"at {_format_number(float(leader['value']))} MMcf. "
        f"Ranking: {ranking}."
    )


def _sector_consumption_chart_df(query: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or not {"date", "value", "series"}.issubset(df.columns):
        return pd.DataFrame(columns=["sector", "value"])

    scoped = df.copy()
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    scoped["value"] = pd.to_numeric(scoped["value"], errors="coerce")
    scoped["series"] = scoped["series"].astype(str)
    scoped = scoped.dropna(subset=["date", "value", "series"])
    if scoped.empty:
        return pd.DataFrame(columns=["sector", "value"])

    latest_date = scoped["date"].max()
    latest_rows = scoped.loc[scoped["date"] == latest_date].copy()
    q = (query or "").lower()
    sector_terms = {
        "electric_power": ("power", "electric power"),
        "residential": ("residential",),
        "industrial": ("industrial",),
        "commercial": ("commercial",),
    }
    requested_sectors = [
        sector
        for sector, terms in sector_terms.items()
        if any(term in q for term in terms)
    ]
    if requested_sectors:
        latest_rows = latest_rows.loc[latest_rows["series"].isin(requested_sectors)].copy()

    if latest_rows.empty:
        return pd.DataFrame(columns=["sector", "value"])

    latest_rows["sector"] = latest_rows["series"].apply(
        lambda s: SECTOR_LABELS.get(str(s), str(s)).replace("_", " ").title()
    )
    out = latest_rows[["sector", "value"]].sort_values("value", ascending=False).reset_index(drop=True)
    return out


def _deterministic_sector_structured_answer(
    *,
    query: str,
    df: pd.DataFrame,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _deterministic_sector_consumption_answer(query=query, df=df)
    if answer == "No data was returned for the requested period.":
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No sector consumption data was returned for the requested period.",
            drivers=["The dataset did not include any usable sector observations."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no observations were returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    latest_date = pd.to_datetime(df["date"], errors="coerce").max()
    latest_rows = df.loc[pd.to_datetime(df["date"], errors="coerce") == latest_date].copy()
    latest_rows["value"] = pd.to_numeric(latest_rows["value"], errors="coerce")
    latest_rows = latest_rows.dropna(subset=["value"]).sort_values("value", ascending=False)
    leader = latest_rows.iloc[0]
    leader_label = SECTOR_LABELS.get(str(leader["series"]), str(leader["series"]))
    data_points = [
        AnswerDataPoint(
            metric=SECTOR_LABELS.get(str(row["series"]), str(row["series"])).replace("_", " ").title(),
            value=_json_safe(float(row["value"])),
            unit="MMcf",
        )
        for _, row in latest_rows.iterrows()
    ]
    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status="neutral", confidence=0.74),
        summary=answer,
        drivers=[
            f"{leader_label.capitalize()} led sector gas consumption on {latest_date.date().isoformat()}.",
            "Sector ranking is based on the latest available observation.",
        ],
        data_points=data_points,
        forecast=AnswerForecast(
            direction="flat",
            reasoning="This response ranks sectors using the latest observation and does not infer a directional forecast.",
        ),
        suggested_alerts=[],
        alerts=[AnswerAlert(name=f"{leader_label.capitalize()} Sector Leads Consumption", status=True)],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _power_sector_proxy_answer(df: pd.DataFrame) -> str:
    if df is None or df.empty or not {"date", "value", "series"}.issubset(df.columns):
        return "No data was returned for the requested period."
    scoped = df.copy()
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    scoped["value"] = pd.to_numeric(scoped["value"], errors="coerce")
    scoped["series"] = scoped["series"].astype(str)
    scoped = scoped.dropna(subset=["date", "value", "series"])
    scoped = scoped.loc[scoped["series"].isin({"electric_power", "power"})].sort_values("date")
    if scoped.empty:
        return "No power-sector observations were returned for the requested period."

    latest = scoped.iloc[-1]
    latest_date = latest["date"]
    latest_value = float(latest["value"])
    month_label = latest_date.strftime("%B %Y")
    return (
        f"Power-sector natural gas use (proxy from sector consumption) was "
        f"{_format_number(latest_value)} MMcf in {month_label}."
    )


def _power_sector_proxy_structured_answer(
    *,
    df: pd.DataFrame,
    source_label: str,
    source_date: Optional[str],
    proxy_note: str,
) -> StructuredAnswer:
    answer = _power_sector_proxy_answer(df)
    if answer in {
        "No data was returned for the requested period.",
        "No power-sector observations were returned for the requested period.",
    }:
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary=answer,
            drivers=[
                proxy_note or "Proxy mode was requested, but no power-sector rows were available.",
            ],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no power-sector proxy observations were returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    scoped = df.copy()
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    scoped["value"] = pd.to_numeric(scoped["value"], errors="coerce")
    scoped["series"] = scoped["series"].astype(str)
    scoped = scoped.dropna(subset=["date", "value", "series"])
    scoped = scoped.loc[scoped["series"].isin({"electric_power", "power"})].sort_values("date")
    latest = scoped.iloc[-1]
    prior_value = float(scoped.iloc[-2]["value"]) if len(scoped) >= 2 else None
    latest_value = float(latest["value"])
    delta = (latest_value - prior_value) if prior_value is not None else None

    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(
            status="bullish" if (delta or 0.0) > 0 else "bearish" if (delta or 0.0) < 0 else "neutral",
            confidence=0.72,
        ),
        summary=answer,
        drivers=[
            proxy_note or "Used power-sector rows from consumption-by-sector as a proxy for ng_electricity.",
            "Latest proxy value comes from the electric power sector series.",
        ],
        data_points=[
            AnswerDataPoint(metric="Power-Sector Gas Use", value=_json_safe(latest_value), unit="MMcf"),
            AnswerDataPoint(metric="Change vs Prior", value=_json_safe(delta), unit="MMcf"),
        ],
        forecast=AnswerForecast(
            direction="up" if (delta or 0.0) > 0 else "down" if (delta or 0.0) < 0 else "flat",
            reasoning="Direction reflects change in latest proxy observation versus prior month.",
        ),
        suggested_alerts=[],
        alerts=[],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _is_regional_storage_change_view(metric: str, df: pd.DataFrame | None) -> bool:
    return bool(
        metric == "working_gas_storage_change_weekly"
        and df is not None
        and not df.empty
        and {"date", "value", "region"}.issubset(df.columns)
    )


def _is_storage_level_and_change_view(metric: str, df: pd.DataFrame | None) -> bool:
    return bool(
        metric == "working_gas_storage_lower48"
        and df is not None
        and not df.empty
        and {"date", "value"}.issubset(df.columns)
    )


def _is_weather_degree_day_forecast_view(metric: str, df: pd.DataFrame | None) -> bool:
    return bool(
        metric == "weather_degree_days_forecast_vs_5y"
        and df is not None
        and not df.empty
        and {
            "bucket",
            "forecast_hdd",
            "normal_hdd_5y",
            "delta_hdd",
            "forecast_cdd",
            "normal_cdd_5y",
            "delta_cdd",
            "demand_delta_bcfd",
            "as_of",
        }.issubset(df.columns)
    )


def _is_weather_regional_demand_drivers_view(metric: str, df: pd.DataFrame | None) -> bool:
    return bool(
        metric == "weather_regional_demand_drivers"
        and df is not None
        and not df.empty
        and {
            "region",
            "demand_delta_bcfd",
            "total_delta_hdd",
            "total_delta_cdd",
            "date",
        }.issubset(df.columns)
    )


def _is_supply_balance_regime_view(metric: str, df: pd.DataFrame | None) -> bool:
    return bool(
        metric == "ng_supply_balance_regime"
        and df is not None
        and not df.empty
        and {
            "regime",
            "score",
            "production_delta_pct",
            "storage_weekly_change",
            "weather_demand_delta_bcfd",
            "date",
        }.issubset(df.columns)
    )


def _is_weekly_energy_atlas_summary_view(metric: str, df: pd.DataFrame | None) -> bool:
    return bool(
        metric == "weekly_energy_atlas_summary"
        and df is not None
        and not df.empty
        and {
            "date",
            "weather_demand_delta_bcfd",
            "storage_surprise_bcf",
            "lng_delta_mmcf",
            "production_delta_mmcf",
            "price_latest_usd_mmbtu",
            "price_delta_usd_mmbtu",
        }.issubset(df.columns)
    )


def _is_regional_production_change_view(metric: str, df: pd.DataFrame | None) -> bool:
    return bool(
        metric == "ng_production_lower48"
        and df is not None
        and not df.empty
        and {"date", "value", "region"}.issubset(df.columns)
    )


def _regional_production_change_answer(df: pd.DataFrame) -> str:
    if not _is_regional_production_change_view("ng_production_lower48", df):
        return "No data was returned for the requested period."

    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["region"] = d["region"].astype(str)
    d = d.dropna(subset=["date", "value", "region"]).sort_values(["region", "date"])
    if d.empty:
        return "No data was returned for the requested period."

    rows: list[dict[str, Any]] = []
    for region, g in d.groupby("region"):
        g = g.sort_values("date")
        if len(g) < 2:
            continue
        latest = g.iloc[-1]
        prior = g.iloc[-2]
        delta = float(latest["value"]) - float(prior["value"])
        rows.append(
            {
                "region": region,
                "date": latest["date"],
                "latest": float(latest["value"]),
                "delta": delta,
            }
        )
    if not rows:
        return "Not enough regional production history was returned to compute a latest period change ranking."

    ranked = pd.DataFrame(rows)
    ranked["abs_delta"] = ranked["delta"].abs()
    ranked = ranked.sort_values("abs_delta", ascending=False)
    top = ranked.iloc[0]
    as_of = pd.to_datetime(top["date"]).date().isoformat()
    top_region = str(top["region"]).replace("_", " ").upper() if len(str(top["region"])) == 2 else str(top["region"]).replace("_", " ").title()
    top_delta = float(top["delta"])
    direction = "increase" if top_delta > 0 else "decrease" if top_delta < 0 else "no change"
    ranking = ", ".join(
        f"{(str(row['region']).replace('_', ' ').upper() if len(str(row['region'])) == 2 else str(row['region']).replace('_', ' ').title())} ({_format_number(float(row['delta']))} MMcf)"
        for _, row in ranked.iterrows()
    )
    return (
        f"As of {as_of}, {top_region} contributed most to the latest production change "
        f"with a {direction} of {_format_number(top_delta)} MMcf. Ranking by absolute change: {ranking}."
    )


def _regional_production_change_structured_answer(
    *,
    df: pd.DataFrame,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _regional_production_change_answer(df)
    if answer.startswith("No data was returned") or answer.startswith("Not enough regional production history"):
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary=answer,
            drivers=["Regional production series did not include enough observations to rank latest change contributions."],
            data_points=[],
            forecast=AnswerForecast(direction="flat", reasoning="Ranking unavailable because regional history was insufficient."),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="Insufficient Regional Production History", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["region"] = d["region"].astype(str)
    d = d.dropna(subset=["date", "value", "region"]).sort_values(["region", "date"])
    rows: list[dict[str, Any]] = []
    for region, g in d.groupby("region"):
        g = g.sort_values("date")
        if len(g) < 2:
            continue
        latest = g.iloc[-1]
        prior = g.iloc[-2]
        rows.append(
            {
                "region": region,
                "delta": float(latest["value"]) - float(prior["value"]),
            }
        )
    ranked = pd.DataFrame(rows)
    ranked["abs_delta"] = ranked["delta"].abs()
    ranked = ranked.sort_values("abs_delta", ascending=False)
    data_points = [
        AnswerDataPoint(
            metric=(str(row["region"]).replace("_", " ").upper() if len(str(row["region"])) == 2 else str(row["region"]).replace("_", " ").title()),
            value=round(float(row["delta"]), 3),
            unit="MMcf",
        )
        for _, row in ranked.iterrows()
    ]
    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status="neutral", confidence=0.78),
        summary=answer,
        drivers=[
            "Contribution is ranked by absolute latest period-over-period production change across reported regions/states.",
            "Positive deltas indicate increases; negative deltas indicate decreases.",
        ],
        data_points=data_points,
        forecast=AnswerForecast(direction="flat", reasoning="This is a latest-period contribution ranking, not a directional forecast."),
        suggested_alerts=[],
        alerts=[],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _regional_production_change_chart_df(df: pd.DataFrame) -> pd.DataFrame:
    if not _is_regional_production_change_view("ng_production_lower48", df):
        return pd.DataFrame(columns=["region", "delta"])
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d["region"] = d["region"].astype(str)
    d = d.dropna(subset=["date", "value", "region"]).sort_values(["region", "date"])
    rows: list[dict[str, Any]] = []
    for region, g in d.groupby("region"):
        g = g.sort_values("date")
        if len(g) < 2:
            continue
        latest = g.iloc[-1]
        prior = g.iloc[-2]
        delta = float(latest["value"]) - float(prior["value"])
        label = (
            str(region).replace("_", " ").upper()
            if len(str(region)) == 2
            else str(region).replace("_", " ").title()
        )
        rows.append({"region": label, "delta": delta})
    if not rows:
        return pd.DataFrame(columns=["region", "delta"])
    out = pd.DataFrame(rows)
    out["abs_delta"] = out["delta"].abs()
    out = out.sort_values("abs_delta", ascending=False).drop(columns=["abs_delta"])
    return out.reset_index(drop=True)


def _weather_normal_years(df: pd.DataFrame) -> int:
    if "normal_years" not in df.columns or df.empty:
        return 5
    val = pd.to_numeric(df["normal_years"], errors="coerce").dropna()
    if val.empty:
        return 5
    try:
        parsed = int(val.iloc[-1])
    except (TypeError, ValueError):
        return 5
    return parsed if parsed in {1, 2, 3, 4, 5} else 5


def _format_as_of_date(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return "n/a"
    parsed = pd.to_datetime(raw, errors="coerce", utc=True)
    if pd.isna(parsed):
        return raw
    ts = parsed.to_pydatetime()
    return f"{ts.strftime('%B')} {ts.day}, {ts.year}"


def _weather_degree_day_forecast_answer(df: pd.DataFrame, *, query: str = "") -> str:
    if not _is_weather_degree_day_forecast_view("weather_degree_days_forecast_vs_5y", df):
        return "No data was returned for the requested period."
    ordered = df.copy()
    ordered["bucket_start_day"] = pd.to_numeric(ordered["bucket_start_day"], errors="coerce")
    ordered = ordered.sort_values("bucket_start_day")
    if ordered.empty:
        return "No data was returned for the requested period."
    total_delta_hdd = float(pd.to_numeric(ordered["delta_hdd"], errors="coerce").sum())
    total_delta_cdd = float(pd.to_numeric(ordered["delta_cdd"], errors="coerce").sum())
    avg_demand_delta = float(pd.to_numeric(ordered["demand_delta_bcfd"], errors="coerce").mean())
    normal_years = _weather_normal_years(ordered)
    as_of = _format_as_of_date(ordered.iloc[-1].get("as_of"))
    q = (query or "").strip().lower().replace("–", "-").replace("—", "-")
    heating_direction = "above" if total_delta_hdd > 0 else "below" if total_delta_hdd < 0 else "near"
    cooling_direction = "above" if total_delta_cdd > 0 else "below" if total_delta_cdd < 0 else "near"
    demand_direction = "higher" if avg_demand_delta > 0 else "lower" if avg_demand_delta < 0 else "about the same"
    demand_move = "increase" if avg_demand_delta > 0 else "decrease" if avg_demand_delta < 0 else "little change"
    weather_takeaway = (
        "overall cooler than normal"
        if total_delta_hdd > 0 and total_delta_cdd <= 0
        else "overall warmer than normal"
        if total_delta_hdd < 0 and total_delta_cdd >= 0
        else "mixed versus normal"
    )
    if ("which regions" in q and "driving" in q) or ("regions" in q and "weather-related demand" in q):
        return (
            f"As of {as_of}, this weather view is a Lower 48 aggregate and does not split demand impact by region. "
            f"At the aggregate level, weather is {weather_takeaway} versus the {normal_years}-year normal, "
            f"implying demand {demand_direction} than normal by about {abs(avg_demand_delta):.2f} Bcf/d. "
            "For regional drivers, ask the same question by region (East, Midwest, South, or West)."
        )

    asks_7_14 = bool(re.search(r"\b7\s*-\s*14\b", q)) or ("7 to 14" in q)
    if asks_7_14:
        mid_range = ordered.loc[ordered["bucket_start_day"] >= 6].copy()
        mid_range_delta = float(
            pd.to_numeric(mid_range["demand_delta_bcfd"], errors="coerce").mean()
        ) if not mid_range.empty else avg_demand_delta
        mid_direction = (
            "higher"
            if mid_range_delta > 0
            else "lower"
            if mid_range_delta < 0
            else "about the same"
        )
        return (
            f"As of {as_of}, weather over roughly days 7-14 (proxied with forecast buckets days 6-15) "
            f"is likely to keep natural gas demand {mid_direction} than normal by about "
            f"{abs(mid_range_delta):.2f} Bcf/d."
        )

    asks_bull_bear = ("bullish" in q or "bearish" in q or "compared to last week" in q)
    if asks_bull_bear:
        stance = "bullish" if avg_demand_delta > 0 else "bearish" if avg_demand_delta < 0 else "neutral"
        return (
            f"As of {as_of}, the current weather signal is {stance} for gas demand "
            f"({demand_direction} than normal by about {abs(avg_demand_delta):.2f} Bcf/d). "
            "A strict week-over-week forecast comparison is not available from the current weather snapshot alone."
        )

    return (
        f"As of {as_of}, the next 15 days look {weather_takeaway} versus the {normal_years}-year average. "
        f"Heating need is {abs(total_delta_hdd):.1f} degree-days {heating_direction} normal, "
        f"and cooling demand is {abs(total_delta_cdd):.1f} degree-days {cooling_direction} normal. "
        f"For natural gas, this weather setup points to a {demand_move} of about {abs(avg_demand_delta):.2f} Bcf/d, "
        f"so demand is expected to be {demand_direction} than normal."
    )


def _weather_degree_day_forecast_structured_answer(
    *,
    df: pd.DataFrame,
    query: str,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _weather_degree_day_forecast_answer(df, query=query)
    if answer == "No data was returned for the requested period.":
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No weather forecast degree-day anomaly data was returned for the requested period.",
            drivers=["The weather forecast pipeline did not return usable HDD/CDD bucket data."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no weather bucket data was returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    ordered = df.copy().sort_values("bucket_start_day")
    total_delta_hdd = float(pd.to_numeric(ordered["delta_hdd"], errors="coerce").sum())
    total_delta_cdd = float(pd.to_numeric(ordered["delta_cdd"], errors="coerce").sum())
    avg_demand_delta = float(pd.to_numeric(ordered["demand_delta_bcfd"], errors="coerce").mean())
    normal_years = _weather_normal_years(ordered)
    signal_status = "bullish" if avg_demand_delta > 0 else "bearish" if avg_demand_delta < 0 else "neutral"
    direction = "up" if avg_demand_delta > 0 else "down" if avg_demand_delta < 0 else "flat"
    confidence = 0.78
    data_points = [
        AnswerDataPoint(metric="Total HDD Delta (1-15d)", value=round(total_delta_hdd, 2), unit="degree-days"),
        AnswerDataPoint(metric="Total CDD Delta (1-15d)", value=round(total_delta_cdd, 2), unit="degree-days"),
        AnswerDataPoint(metric="Estimated Demand Delta", value=round(avg_demand_delta, 3), unit="Bcf/d"),
    ]
    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status=signal_status, confidence=confidence),
        summary=answer,
        drivers=[
            "Higher HDD usually means colder weather and more gas used for heating homes and businesses.",
            "Higher CDD usually means hotter weather and more power demand for air conditioning, which can raise gas burn.",
            "Demand delta is an estimate from weather effects only, not a full supply-demand balance model.",
        ],
        data_points=data_points,
        forecast=AnswerForecast(
            direction=direction,
            reasoning=f"Direction reflects average weather-driven demand delta versus a rolling {normal_years}-year normal across forecast buckets.",
        ),
        suggested_alerts=[],
        alerts=[],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _weather_regional_demand_drivers_answer(df: pd.DataFrame) -> str:
    if not _is_weather_regional_demand_drivers_view("weather_regional_demand_drivers", df):
        return "No data was returned for the requested period."
    ordered = df.copy()
    ordered["demand_delta_bcfd"] = pd.to_numeric(ordered["demand_delta_bcfd"], errors="coerce")
    ordered["total_delta_hdd"] = pd.to_numeric(ordered["total_delta_hdd"], errors="coerce")
    ordered["total_delta_cdd"] = pd.to_numeric(ordered["total_delta_cdd"], errors="coerce")
    ordered = ordered.dropna(subset=["demand_delta_bcfd"]).copy()
    if ordered.empty:
        return "No data was returned for the requested period."
    ordered["abs_demand_delta_bcfd"] = ordered["demand_delta_bcfd"].abs()
    ordered = ordered.sort_values("abs_demand_delta_bcfd", ascending=False)
    top = ordered.iloc[0]
    as_of = _format_as_of_date(top.get("date"))
    top_region = str(top["region"]).replace("_", " ").title()
    top_delta = float(top["demand_delta_bcfd"])
    direction = "higher" if top_delta > 0 else "lower" if top_delta < 0 else "flat"

    lines = []
    for _, row in ordered.iterrows():
        region = str(row["region"]).replace("_", " ").title()
        delta = float(row["demand_delta_bcfd"])
        region_dir = "up" if delta > 0 else "down" if delta < 0 else "flat"
        lines.append(f"{region} ({region_dir} {abs(delta):.2f} Bcf/d)")

    return (
        f"As of {as_of}, {top_region} is the largest weather-driven demand driver "
        f"({direction} by about {abs(top_delta):.2f} Bcf/d versus normal). "
        f"Regional ranking: {', '.join(lines)}."
    )


def _weather_regional_demand_drivers_structured_answer(
    *,
    df: pd.DataFrame,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _weather_regional_demand_drivers_answer(df)
    if answer == "No data was returned for the requested period.":
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No regional weather-demand driver data was returned for the requested period.",
            drivers=["Regional weather-demand ranking could not be computed from the available forecast data."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no regional weather-demand data was returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    ordered = df.copy()
    ordered["demand_delta_bcfd"] = pd.to_numeric(ordered["demand_delta_bcfd"], errors="coerce")
    ordered = ordered.dropna(subset=["demand_delta_bcfd"])
    ordered["abs_demand_delta_bcfd"] = ordered["demand_delta_bcfd"].abs()
    ordered = ordered.sort_values("abs_demand_delta_bcfd", ascending=False)
    avg_delta = float(ordered["demand_delta_bcfd"].mean()) if not ordered.empty else 0.0
    signal_status = "bullish" if avg_delta > 0 else "bearish" if avg_delta < 0 else "neutral"
    direction = "up" if avg_delta > 0 else "down" if avg_delta < 0 else "flat"

    data_points = [
        AnswerDataPoint(
            metric=str(row["region"]).replace("_", " ").title(),
            value=round(float(row["demand_delta_bcfd"]), 3),
            unit="Bcf/d",
        )
        for _, row in ordered.iterrows()
    ]
    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status=signal_status, confidence=0.8),
        summary=answer,
        drivers=[
            "Ranking reflects average regional weather-demand delta versus normal across forecast buckets.",
            "Positive values indicate weather that tends to raise gas demand; negative values indicate softer demand pressure.",
        ],
        data_points=data_points,
        forecast=AnswerForecast(
            direction=direction,
            reasoning="Direction reflects the average of regional weather-demand deltas versus normal.",
        ),
        suggested_alerts=[],
        alerts=[],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _supply_balance_regime_answer(df: pd.DataFrame) -> str:
    if not _is_supply_balance_regime_view("ng_supply_balance_regime", df):
        return "No data was returned for the requested period."

    row = df.iloc[-1]
    as_of = _format_as_of_date(row.get("date"))
    regime = str(row.get("regime") or "mixed").strip().lower()
    if regime not in {"expanding", "tightening", "mixed"}:
        regime = "mixed"
    production_delta_pct = float(pd.to_numeric(row.get("production_delta_pct"), errors="coerce") or 0.0)
    storage_weekly_change = float(pd.to_numeric(row.get("storage_weekly_change"), errors="coerce") or 0.0)
    weather_demand_delta = float(pd.to_numeric(row.get("weather_demand_delta_bcfd"), errors="coerce") or 0.0)
    direction_text = {
        "expanding": "supply appears to be expanding overall",
        "tightening": "supply appears to be tightening overall",
        "mixed": "signals are mixed, so supply is not clearly expanding or tightening",
    }[regime]

    return (
        f"As of {as_of}, {direction_text}. "
        f"Production changed {production_delta_pct:+.2f}% versus the prior reading, "
        f"latest weekly storage change was {storage_weekly_change:+.1f} Bcf, "
        f"and weather-driven demand pressure is {weather_demand_delta:+.2f} Bcf/d versus normal."
    )


def _supply_balance_regime_structured_answer(
    *,
    df: pd.DataFrame,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _supply_balance_regime_answer(df)
    if answer == "No data was returned for the requested period.":
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No supply-balance regime data was returned for the requested period.",
            drivers=["The derived supply-balance pipeline did not return usable component metrics."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no observations were returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    row = df.iloc[-1]
    regime = str(row.get("regime") or "mixed").strip().lower()
    score = float(pd.to_numeric(row.get("score"), errors="coerce") or 0.0)
    production_delta_pct = float(pd.to_numeric(row.get("production_delta_pct"), errors="coerce") or 0.0)
    storage_weekly_change = float(pd.to_numeric(row.get("storage_weekly_change"), errors="coerce") or 0.0)
    weather_demand_delta = float(pd.to_numeric(row.get("weather_demand_delta_bcfd"), errors="coerce") or 0.0)

    signal_status = "bullish" if regime == "tightening" else "bearish" if regime == "expanding" else "neutral"
    confidence = max(0.55, min(0.9, 0.55 + (abs(score) * 0.1)))
    direction = "up" if regime == "tightening" else "down" if regime == "expanding" else "flat"

    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status=signal_status, confidence=round(confidence, 2)),
        summary=answer,
        drivers=[
            "Production trend indicates whether upstream supply is growing or contracting.",
            "Weekly storage change captures near-term inventory build or withdrawal pressure.",
            "Weather demand delta approximates demand pull versus normal conditions.",
        ],
        data_points=[
            AnswerDataPoint(metric="Regime Score", value=round(score, 3), unit="index"),
            AnswerDataPoint(metric="Production Delta", value=round(production_delta_pct, 3), unit="%"),
            AnswerDataPoint(metric="Latest Storage Change", value=round(storage_weekly_change, 3), unit="Bcf"),
            AnswerDataPoint(metric="Weather Demand Delta", value=round(weather_demand_delta, 3), unit="Bcf/d"),
        ],
        forecast=AnswerForecast(
            direction=direction,
            reasoning="Direction reflects the combined production, storage, and weather-demand signals.",
        ),
        suggested_alerts=[],
        alerts=[],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _weekly_energy_atlas_summary_answer(df: pd.DataFrame) -> str:
    if not _is_weekly_energy_atlas_summary_view("weekly_energy_atlas_summary", df):
        return "No data was returned for the requested period."

    row = df.iloc[-1]
    as_of = _format_as_of_date(row.get("date"))
    weather_delta = float(pd.to_numeric(row.get("weather_demand_delta_bcfd"), errors="coerce") or 0.0)
    storage_latest = pd.to_numeric(row.get("storage_latest_bcf"), errors="coerce")
    storage_expected = pd.to_numeric(row.get("storage_expected_bcf"), errors="coerce")
    storage_surprise = pd.to_numeric(row.get("storage_surprise_bcf"), errors="coerce")
    lng_delta = float(pd.to_numeric(row.get("lng_delta_mmcf"), errors="coerce") or 0.0)
    production_delta = float(pd.to_numeric(row.get("production_delta_mmcf"), errors="coerce") or 0.0)
    price_latest = pd.to_numeric(row.get("price_latest_usd_mmbtu"), errors="coerce")
    price_delta = pd.to_numeric(row.get("price_delta_usd_mmbtu"), errors="coerce")

    weather_direction = (
        "higher"
        if weather_delta > 0
        else "lower"
        if weather_delta < 0
        else "about unchanged"
    )
    weather_line = (
        f"**Weather:** Weather is pushing gas demand {weather_direction} than normal by about "
        f"{abs(weather_delta):.2f} Bcf/d."
    )

    if pd.notna(storage_latest) and pd.notna(storage_expected) and pd.notna(storage_surprise):
        storage_shape = (
            "a larger-than-expected build"
            if float(storage_surprise) > 0
            else "a smaller-than-expected build / tighter read"
            if float(storage_surprise) < 0
            else "in line with recent expectations"
        )
        storage_line = (
            f"**Storage:** Latest weekly change was {_format_number(float(storage_latest))} Bcf versus "
            f"about {_format_number(float(storage_expected))} Bcf expected (recent 5-week average), "
            f"a surprise of {_format_number(float(storage_surprise))} Bcf ({storage_shape})."
        )
    else:
        storage_line = "**Storage:** Not enough recent history to estimate surprise versus expectations."

    lng_dir = "up" if lng_delta > 0 else "down" if lng_delta < 0 else "flat"
    prod_dir = "up" if production_delta > 0 else "down" if production_delta < 0 else "flat"
    lng_supply_line = (
        "**LNG / Supply:** LNG exports are "
        f"{lng_dir} {_format_number(abs(lng_delta))} MMcf versus the prior point, and dry gas production is "
        f"{prod_dir} {_format_number(abs(production_delta))} MMcf."
    )

    if pd.notna(price_latest) and pd.notna(price_delta):
        price_line = (
            f"**Price:** Henry Hub moved {_format_delta(float(price_delta), '$/MMBtu')} "
            f"to ${float(price_latest):.2f}/MMBtu."
        )
    elif pd.notna(price_latest):
        price_line = f"**Price:** Henry Hub is currently ${float(price_latest):.2f}/MMBtu."
    else:
        price_line = "**Price:** No latest Henry Hub price observation was available."

    return f"As of {as_of}, weekly Energy Atlas summary:\n{weather_line}\n{storage_line}\n{lng_supply_line}\n{price_line}"


def _weekly_energy_atlas_summary_structured_answer(
    *,
    df: pd.DataFrame,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _weekly_energy_atlas_summary_answer(df)
    if answer == "No data was returned for the requested period.":
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No weekly recap data was returned for the requested period.",
            drivers=["The weekly summary pipeline did not return usable component metrics."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no observations were returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    row = df.iloc[-1]
    weather_delta = float(pd.to_numeric(row.get("weather_demand_delta_bcfd"), errors="coerce") or 0.0)
    storage_surprise = float(pd.to_numeric(row.get("storage_surprise_bcf"), errors="coerce") or 0.0)
    production_delta = float(pd.to_numeric(row.get("production_delta_mmcf"), errors="coerce") or 0.0)
    price_delta = float(pd.to_numeric(row.get("price_delta_usd_mmbtu"), errors="coerce") or 0.0)

    pressure_score = 0.0
    pressure_score += 1.0 if weather_delta > 0 else -1.0 if weather_delta < 0 else 0.0
    pressure_score += 1.0 if storage_surprise < 0 else -1.0 if storage_surprise > 0 else 0.0
    pressure_score += 1.0 if production_delta < 0 else -1.0 if production_delta > 0 else 0.0
    pressure_score += 1.0 if price_delta > 0 else -1.0 if price_delta < 0 else 0.0
    signal_status = "bullish" if pressure_score >= 1 else "bearish" if pressure_score <= -1 else "neutral"
    direction = "up" if pressure_score >= 1 else "down" if pressure_score <= -1 else "flat"

    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status=signal_status, confidence=0.76),
        summary=answer,
        drivers=[
            "Weather contribution is the forecast demand shift versus normal (Bcf/d).",
            "Storage surprise uses latest weekly change versus a recent 5-week average as an expectation proxy.",
            "LNG/supply combines export and production moves versus prior observations.",
            "Price result is the latest Henry Hub move versus prior point.",
        ],
        data_points=[
            AnswerDataPoint(metric="Weather Demand Impact", value=round(weather_delta, 3), unit="Bcf/d"),
            AnswerDataPoint(metric="Storage Surprise", value=round(storage_surprise, 3), unit="Bcf"),
            AnswerDataPoint(
                metric="LNG Export Change",
                value=round(float(pd.to_numeric(row.get("lng_delta_mmcf"), errors="coerce") or 0.0), 3),
                unit="MMcf",
            ),
            AnswerDataPoint(metric="Production Change", value=round(production_delta, 3), unit="MMcf"),
            AnswerDataPoint(metric="Henry Hub Weekly Move", value=round(price_delta, 3), unit="$/MMBtu"),
        ],
        forecast=AnswerForecast(
            direction=direction,
            reasoning="Direction reflects the combined weekly pressure from weather demand, storage surprise, supply changes, and price action.",
        ),
        suggested_alerts=[],
        alerts=[],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _regional_storage_change_answer(df: pd.DataFrame, query: str = "") -> str:
    if not _is_regional_storage_change_view("working_gas_storage_change_weekly", df):
        return "No data was returned for the requested period."

    ordered = df.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
    ordered = ordered.dropna(subset=["date", "value", "region"])
    if ordered.empty:
        return "No data was returned for the requested period."

    latest_date = ordered["date"].max()
    latest_rows = ordered.loc[ordered["date"] == latest_date].copy()
    lowered_query = (query or "").lower()
    rank_withdrawals = "withdrawal" in lowered_query or "fastest" in lowered_query
    latest_rows = latest_rows.sort_values("value", ascending=rank_withdrawals)
    if latest_rows.empty:
        return "No data was returned for the requested period."

    leader = latest_rows.iloc[0]
    leader_region = str(leader["region"]).replace("_", " ").title()
    leader_value = float(leader["value"])
    ranking = ", ".join(
        f"{str(row['region']).replace('_', ' ').title()} ({_format_number(float(row['value']))} Bcf)"
        for _, row in latest_rows.iterrows()
    )
    if "largest weekly storage change" in lowered_query:
        descriptor = "the largest weekly storage change"
        return (
            f"As of {latest_date.date().isoformat()}, {leader_region} had {descriptor} "
            f"at {_format_number(leader_value)} Bcf. Ranking: {ranking}."
        )
    if rank_withdrawals:
        descriptor = "the fastest storage withdrawal"
    else:
        descriptor = (
            "the largest storage build"
            if leader_value > 0
            else "the largest storage withdrawal"
            if leader_value < 0
            else "the largest storage change"
        )
    return (
        f"As of {latest_date.date().isoformat()}, {leader_region} posted {descriptor} "
        f"at {_format_number(leader_value)} Bcf. Ranking: {ranking}."
    )


def _regional_storage_change_structured_answer(
    *,
    df: pd.DataFrame,
    query: str,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _regional_storage_change_answer(df, query=query)
    if answer == "No data was returned for the requested period.":
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No regional storage change data was returned for the requested period.",
            drivers=["The dataset did not include any usable regional storage observations."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no observations were returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    ordered = df.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
    ordered = ordered.dropna(subset=["date", "value", "region"])
    latest_date = ordered["date"].max()
    latest_rows = ordered.loc[ordered["date"] == latest_date].copy()
    latest_rows = latest_rows.sort_values("value", ascending=False)
    leader = latest_rows.iloc[0]
    leader_region = str(leader["region"]).replace("_", " ").title()
    data_points = [
        AnswerDataPoint(
            metric=str(row["region"]).replace("_", " ").title(),
            value=_json_safe(float(row["value"])),
            unit="Bcf",
        )
        for _, row in latest_rows.iterrows()
    ]
    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status="neutral", confidence=0.78),
        summary=answer,
        drivers=[
            f"{leader_region} led the latest weekly storage change on {latest_date.date().isoformat()}.",
            "Regional ranking is based on the latest available EIA storage week.",
        ],
        data_points=data_points,
        forecast=AnswerForecast(
            direction="flat",
            reasoning="This response ranks regions using the latest observation and does not infer a directional forecast.",
        ),
        suggested_alerts=[],
        alerts=[AnswerAlert(name=f"{leader_region} Leads Weekly Storage Change", status=True)],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _storage_level_and_change_answer(
    df: pd.DataFrame, *, region_label: str = "Selected region", query: str = ""
) -> str:
    if not _is_storage_level_and_change_view("working_gas_storage_lower48", df):
        return "No data was returned for the requested period."

    ordered = df.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
    if "weekly_change" in ordered.columns:
        ordered["weekly_change"] = pd.to_numeric(ordered["weekly_change"], errors="coerce")
    else:
        ordered["weekly_change"] = np.nan
    ordered = ordered.dropna(subset=["date", "value"]).sort_values("date")
    if ordered.empty:
        return "No data was returned for the requested period."

    latest = ordered.iloc[-1]
    lowered_query = (query or "").lower().replace("–", "-").replace("—", "-")
    latest_date = latest["date"].date().isoformat()
    latest_value = float(latest["value"])
    storage_text = _format_number(latest_value)

    comparison_df = _storage_same_week_yearly_comparison_df(ordered)
    same_week_history = comparison_df.copy()
    if not same_week_history.empty:
        same_week_history["comparison_year"] = pd.to_numeric(
            same_week_history["comparison_year"], errors="coerce"
        )
        latest_year = int(pd.Timestamp(latest["date"]).year)
        same_week_history = same_week_history.loc[
            same_week_history["comparison_year"] < latest_year
        ].copy()

    if (
        "same week last year" in lowered_query
        or "year over year" in lowered_query
        or "yoy" in lowered_query
    ):
        target_last_year = latest["date"] - pd.DateOffset(years=1)
        last_year_window = ordered.loc[
            (ordered["date"] >= (target_last_year - pd.Timedelta(days=10)))
            & (ordered["date"] <= (target_last_year + pd.Timedelta(days=10)))
            & (ordered["date"] < latest["date"])
        ].copy()
        if last_year_window.empty:
            return (
                f"As of {latest_date}, {region_label} storage was {storage_text} Bcf. "
                "I could not find a matching same-week observation from last year in the returned history."
            )
        last_year_window["days_from_target"] = (last_year_window["date"] - target_last_year).abs().dt.days
        prior = last_year_window.sort_values(["days_from_target", "date"]).iloc[0]
        prior_value = float(prior["value"])
        delta = latest_value - prior_value
        pct = (delta / prior_value * 100.0) if prior_value != 0 else None
        pct_text = f" ({pct:+.1f}%)" if pct is not None else ""
        return (
            f"As of {latest_date}, {region_label} storage was {storage_text} Bcf, "
            f"vs {_format_number(prior_value)} Bcf in the same reporting week last year "
            f"({prior['date'].date().isoformat()}), a change of {_format_number(delta)} Bcf{pct_text}."
        )

    asks_five_year_average = ("five-year" in lowered_query or "5-year" in lowered_query) and "average" in lowered_query
    asks_five_year_range = ("five-year" in lowered_query or "5-year" in lowered_query) and "range" in lowered_query
    asks_tight_loose = any(term in lowered_query for term in ("tight", "loose", "neutral")) and (
        "five-year range" in lowered_query or "5-year range" in lowered_query
    )

    if asks_five_year_average or asks_five_year_range or asks_tight_loose:
        if same_week_history.empty:
            return (
                f"As of {latest_date}, {region_label} storage was {storage_text} Bcf. "
                "Not enough same-week history was returned to compute a reliable five-year baseline."
            )
        five_year_avg = float(pd.to_numeric(same_week_history["value"], errors="coerce").mean())
        five_year_min = float(pd.to_numeric(same_week_history["value"], errors="coerce").min())
        five_year_max = float(pd.to_numeric(same_week_history["value"], errors="coerce").max())
        delta_avg = latest_value - five_year_avg
        pct_avg = (delta_avg / five_year_avg * 100.0) if five_year_avg != 0 else None
        pct_avg_text = f" ({pct_avg:+.1f}%)" if pct_avg is not None else ""
        if latest_value < five_year_min:
            regime = "tight"
        elif latest_value > five_year_max:
            regime = "loose"
        else:
            regime = "neutral"

        if asks_tight_loose:
            return (
                f"As of {latest_date}, inventories are {regime} versus the five-year range "
                f"for this week: current {storage_text} Bcf vs range "
                f"{_format_number(five_year_min)} to {_format_number(five_year_max)} Bcf."
            )
        if asks_five_year_range:
            return (
                f"As of {latest_date}, {region_label} storage was {storage_text} Bcf. "
                f"For this same week, the five-year range is {_format_number(five_year_min)} to "
                f"{_format_number(five_year_max)} Bcf, and the five-year average is "
                f"{_format_number(five_year_avg)} Bcf."
            )
        return (
            f"As of {latest_date}, {region_label} storage was {storage_text} Bcf versus a "
            f"five-year same-week average of {_format_number(five_year_avg)} Bcf, "
            f"a difference of {_format_number(delta_avg)} Bcf{pct_avg_text}."
        )

    if pd.notna(latest["weekly_change"]):
        change_value = float(latest["weekly_change"])
        flow_word = "injection" if change_value > 0 else "withdrawal" if change_value < 0 else "change"
        change_text = _format_number(change_value)
        return (
            f"As of {latest_date}, {region_label} storage was {storage_text} Bcf and the latest weekly change "
            f"was {change_text} Bcf ({flow_word})."
        )
    return f"As of {latest_date}, {region_label} storage was {storage_text} Bcf."


def _is_storage_five_year_comparison_query(query: str) -> bool:
    lowered = (query or "").lower().replace("–", "-").replace("—", "-")
    has_five_year = "five-year" in lowered or "5-year" in lowered
    has_baseline = "average" in lowered or "range" in lowered
    return has_five_year and has_baseline


def _is_same_time_five_year_comparison_query(query: str) -> bool:
    lowered = (query or "").lower()
    normalized = re.sub(r"[\u2010-\u2015\u2212]", "-", lowered)
    has_five_year = bool(re.search(r"(?:five|5)\s*-?\s*year", normalized))
    has_baseline = any(
        token in normalized for token in ("average", "range", "seasonal", "historical", "history", "norm", "normal")
    )
    same_time_hint = bool(
        re.search(r"same[\s-]*time|same[\s-]*week|this[\s-]*week|this[\s-]*time", normalized)
    )
    return has_five_year and has_baseline and same_time_hint


def _five_year_same_time_baseline(series_df: pd.DataFrame) -> tuple[float, float, float] | None:
    scoped = series_df.copy()
    if not {"date", "value"}.issubset(scoped.columns):
        return None
    scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
    scoped["value"] = pd.to_numeric(scoped["value"], errors="coerce")
    scoped = scoped.dropna(subset=["date", "value"]).sort_values("date")
    if len(scoped) < 6:
        return None
    latest_row = scoped.iloc[-1]
    latest_ts = pd.Timestamp(latest_row["date"])
    latest_year = int(latest_ts.year)
    deltas = scoped["date"].diff().dropna().dt.total_seconds() / 86400.0
    spacing = float(deltas.median()) if not deltas.empty else 30.0

    if spacing <= 10.0:
        scoped["iso_week"] = scoped["date"].dt.isocalendar().week.astype(int)
        hist = scoped.loc[
            (scoped["iso_week"] == int(latest_ts.isocalendar().week))
            & (scoped["date"].dt.year >= latest_year - 5)
            & (scoped["date"].dt.year < latest_year)
        ]
    elif spacing <= 45.0:
        hist = scoped.loc[
            (scoped["date"].dt.month == latest_ts.month)
            & (scoped["date"].dt.year >= latest_year - 5)
            & (scoped["date"].dt.year < latest_year)
        ]
    else:
        doy = int(latest_ts.dayofyear)
        hist = scoped.loc[
            (scoped["date"].dt.year >= latest_year - 5)
            & (scoped["date"].dt.year < latest_year)
            & ((scoped["date"].dt.dayofyear - doy).abs() <= 3)
        ]

    hist_vals = pd.to_numeric(hist["value"], errors="coerce").dropna()
    if len(hist_vals) < 3:
        return None
    return float(hist_vals.mean()), float(hist_vals.min()), float(hist_vals.max())


def _series_with_five_year_average_line(
    df: pd.DataFrame,
    *,
    baseline_mode: str = "average",
) -> pd.DataFrame:
    if df is None or df.empty or not {"date", "value"}.issubset(df.columns):
        return pd.DataFrame()
    baseline = _five_year_same_time_baseline(df)
    if baseline is None:
        return pd.DataFrame()
    avg_5y, _, _ = baseline
    chart_df = df.copy()
    chart_df["date"] = pd.to_datetime(chart_df["date"], errors="coerce")
    chart_df["value"] = pd.to_numeric(chart_df["value"], errors="coerce")
    chart_df = chart_df.dropna(subset=["date", "value"]).sort_values("date").reset_index(drop=True)
    if chart_df.empty:
        return pd.DataFrame()
    baseline_value = float(avg_5y)
    if baseline_mode == "median":
        history = chart_df.copy()
        latest_ts = pd.Timestamp(history.iloc[-1]["date"])
        latest_year = int(latest_ts.year)
        deltas = history["date"].diff().dropna().dt.total_seconds() / 86400.0
        spacing = float(deltas.median()) if not deltas.empty else 30.0
        if spacing <= 10.0:
            history["iso_week"] = history["date"].dt.isocalendar().week.astype(int)
            hist = history.loc[
                (history["iso_week"] == int(latest_ts.isocalendar().week))
                & (history["date"].dt.year >= latest_year - 5)
                & (history["date"].dt.year < latest_year)
            ]
        elif spacing <= 45.0:
            hist = history.loc[
                (history["date"].dt.month == latest_ts.month)
                & (history["date"].dt.year >= latest_year - 5)
                & (history["date"].dt.year < latest_year)
            ]
        else:
            doy = int(latest_ts.dayofyear)
            hist = history.loc[
                (history["date"].dt.year >= latest_year - 5)
                & (history["date"].dt.year < latest_year)
                & ((history["date"].dt.dayofyear - doy).abs() <= 3)
            ]
        hist_vals = pd.to_numeric(hist["value"], errors="coerce").dropna()
        if len(hist_vals) >= 3:
            baseline_value = float(hist_vals.median())
    chart_df["five_year_baseline"] = baseline_value
    return chart_df


def _storage_same_week_yearly_comparison_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or not {"date", "value"}.issubset(df.columns):
        return pd.DataFrame(columns=["comparison_year", "value", "date"])
    ordered = df.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
    ordered = ordered.dropna(subset=["date", "value"]).sort_values("date")
    if ordered.empty:
        return pd.DataFrame(columns=["comparison_year", "value", "date"])

    latest = ordered.iloc[-1]
    latest_date = latest["date"]
    latest_year = int(latest_date.year)
    rows: list[dict[str, Any]] = []

    for year in range(latest_year - 5, latest_year + 1):
        if year == latest_year:
            rows.append(
                {
                    "comparison_year": str(year),
                    "value": float(latest["value"]),
                    "date": latest_date,
                }
            )
            continue
        target = latest_date - pd.DateOffset(years=(latest_year - year))
        candidates = ordered.loc[
            (ordered["date"] >= (target - pd.Timedelta(days=10)))
            & (ordered["date"] <= (target + pd.Timedelta(days=10)))
            & (ordered["date"] < latest_date)
        ].copy()
        if candidates.empty:
            continue
        candidates["days_from_target"] = (candidates["date"] - target).abs().dt.days
        pick = candidates.sort_values(["days_from_target", "date"]).iloc[0]
        rows.append(
            {
                "comparison_year": str(year),
                "value": float(pick["value"]),
                "date": pick["date"],
            }
        )

    if not rows:
        return pd.DataFrame(columns=["comparison_year", "value", "date"])
    return pd.DataFrame(rows).sort_values("comparison_year").reset_index(drop=True)


def _route_domain(route) -> str:
    return str(getattr(route, "domain", "") or "")


def _route_analysis_type(route) -> str:
    return str(getattr(route, "analysis_type", "") or "")


def _route_value_type(route) -> str:
    return str(getattr(route, "value_type", "") or "")


def _route_comparisons(route) -> list[str]:
    return list(getattr(route, "comparisons", []) or [])


def _route_regions(route) -> list[str]:
    return list(getattr(route, "regions", []) or [])


def _route_states(route) -> list[str]:
    return list(getattr(route, "states", []) or [])


def _route_states_all(route) -> bool:
    return bool(getattr(route, "states_all", False))


def _route_chart_type(route) -> str:
    return str(getattr(route, "chart_type", "") or "")


def _route_output_mode(route) -> str:
    return str(getattr(route, "output_mode", "") or "")


def _route_ranking_basis(route) -> str:
    return str(getattr(route, "ranking_basis", "") or "current_storage")


def _route_storage_dataset(route) -> str:
    return str(getattr(route, "storage_dataset", "") or "weekly_working_gas")


def _route_storage_frequency(route) -> str:
    return str(getattr(route, "storage_frequency", "") or "weekly")


def _route_storage_metric_type(route) -> str:
    return str(getattr(route, "storage_metric_type", "") or "working_gas")


def _route_storage_type(route) -> str | None:
    value = getattr(route, "storage_type", None)
    return str(value) if value else None


def _route_storage_types_all(route) -> bool:
    return bool(getattr(route, "storage_types_all", False))


def _storage_region_label(region: str | None) -> str:
    label = str(region or "lower48").replace("_", " ").title()
    return "Lower 48" if label == "Lower48" else label


def _storage_state_label(state: str | None) -> str:
    value = str(state or "united_states_total").strip().lower()
    if value == "united_states_total":
        return "United States Total"
    if len(value) == 2:
        return value.upper()
    return value.replace("_", " ").title()


def _storage_type_label(storage_type: str | None) -> str:
    value = str(storage_type or "").strip().lower()
    if not value:
        return "Storage Type"
    return value.replace("_", " ").title()


def _storage_geography_label(geography: str | None) -> str:
    value = str(geography or "").strip().lower()
    if not value:
        return "United States Total"
    if value in {"united_states_total", "us_total"}:
        return "United States Total"
    if len(value) == 2:
        return value.upper()
    return value.replace("_", " ").title()


def _storage_metric_label(metric_type: str) -> str:
    return {
        "total_gas": "Natural Gas in Storage",
        "base_gas": "Base Gas in Storage",
        "working_gas": "Working Gas in Storage",
        "total_capacity": "Total Underground Storage Capacity",
        "working_gas_capacity": "Working Gas Storage Capacity",
        "storage_field_count": "Underground Storage Field Count",
        "net_withdrawals": "Net Withdrawals",
        "injections": "Injections",
        "withdrawals": "Withdrawals",
        "working_gas_yoy_volume_change": "Working Gas Change from Year Ago",
        "working_gas_yoy_pct_change": "Working Gas % Change from Year Ago",
    }.get(metric_type, "Working Gas in Storage")


def _storage_metric_unit(metric_type: str) -> str:
    if metric_type == "storage_field_count":
        return "fields"
    return "%" if metric_type == "working_gas_yoy_pct_change" else "MMcf"


def _underground_storage_metric_label(metric_type: str) -> str:
    return _storage_metric_label(metric_type)


def _underground_storage_unit(metric_type: str) -> str:
    return _storage_metric_unit(metric_type)


def _storage_prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or not {"date", "value"}.issubset(df.columns):
        return pd.DataFrame(columns=["date", "value"])
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    d = d.dropna(subset=["date", "value"])
    if "state" in d.columns:
        d["state"] = d["state"].astype(str)
    sort_cols = ["date"]
    if "state" in d.columns:
        sort_cols = ["state", "date"]
    if "region" in d.columns:
        d["region"] = d["region"].astype(str)
        sort_cols = ["region", "date"]
    return d.sort_values(sort_cols).reset_index(drop=True)


def _underground_storage_prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    d = _storage_prepare_df(df)
    if d.empty:
        return pd.DataFrame(columns=["date", "value", "state", "region", "geography", "storage_type"])
    if "state" not in d.columns:
        if "geography" in d.columns:
            d["geography"] = d["geography"].astype(str)
        elif "storage_type" not in d.columns:
            d["state"] = "united_states_total"
    if "state" in d.columns:
        d["state"] = d["state"].astype(str)
        return d.sort_values(["state", "date"]).reset_index(drop=True)
    if "region" in d.columns:
        d["region"] = d["region"].astype(str)
        return d.sort_values(["region", "date"]).reset_index(drop=True)
    if "geography" in d.columns:
        d["geography"] = d["geography"].astype(str)
        return d.sort_values(["geography", "date"]).reset_index(drop=True)
    d["storage_type"] = d["storage_type"].astype(str)
    return d.sort_values(["storage_type", "date"]).reset_index(drop=True)


def _underground_storage_group_column(df: pd.DataFrame) -> str:
    if "state" in df.columns:
        return "state"
    if "region" in df.columns:
        return "region"
    if "geography" in df.columns:
        return "geography"
    return "state"


def _underground_storage_geography_label(column: str, value: str | None) -> str:
    if column == "state":
        return _storage_state_label(value)
    if column == "region":
        return _storage_region_label(value)
    return _storage_geography_label(value)


def _storage_latest_by_region_df(df: pd.DataFrame) -> pd.DataFrame:
    d = _storage_prepare_df(df)
    if d.empty:
        return d
    if "region" not in d.columns:
        if "state" in d.columns:
            return (
                d.sort_values(["state", "date"])
                .groupby("state", as_index=False, sort=False)
                .tail(1)
                .reset_index(drop=True)
            )
        return d.tail(1).reset_index(drop=True)
    return (
        d.sort_values(["region", "date"])
        .groupby("region", as_index=False, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )


def _underground_storage_latest_by_state_df(df: pd.DataFrame) -> pd.DataFrame:
    d = _underground_storage_prepare_df(df)
    if d.empty:
        return d
    group_column = _underground_storage_group_column(d)
    return (
        d.sort_values([group_column, "date"])
        .groupby(group_column, as_index=False, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )


def _underground_storage_latest_by_type_df(df: pd.DataFrame) -> pd.DataFrame:
    d = _underground_storage_prepare_df(df)
    if d.empty or "storage_type" not in d.columns:
        return d
    return (
        d.sort_values(["storage_type", "date"])
        .groupby("storage_type", as_index=False, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )


def _storage_prior_year_same_week_samples(
    group: pd.DataFrame,
    *,
    target_date: pd.Timestamp,
    normal_years: int = 5,
    tolerance_days: int = 10,
) -> pd.Series:
    latest_year = int(target_date.year)
    samples: list[float] = []
    for year in range(latest_year - normal_years, latest_year):
        shifted = target_date - pd.DateOffset(years=(latest_year - year))
        candidates = group.loc[
            (group["date"].dt.year == year)
            & (group["date"] >= (shifted - pd.Timedelta(days=tolerance_days)))
            & (group["date"] <= (shifted + pd.Timedelta(days=tolerance_days)))
        ].copy()
        if candidates.empty:
            continue
        candidates["days_from_target"] = (candidates["date"] - shifted).abs().dt.days
        picked = candidates.sort_values(["days_from_target", "date"]).iloc[0]
        value = _safe_float(picked["value"])
        if value is not None:
            samples.append(value)
    return pd.Series(samples, dtype="float64")


def _storage_current_year_vs_baseline_series(
    df: pd.DataFrame,
    *,
    normal_years: int = 5,
) -> pd.DataFrame:
    d = _storage_prepare_df(df)
    if d.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "value",
                "five_year_avg",
                "five_year_min",
                "five_year_max",
                "deviation_bcf",
                "deviation_pct",
                "sample_count",
            ]
        )

    if "region" not in d.columns:
        d["region"] = "lower48"
        added_region = True
    else:
        added_region = False

    rows: list[dict[str, Any]] = []
    for region, group in d.groupby("region", sort=False):
        group = group.sort_values("date").copy()
        latest_year = int(group["date"].dt.year.max())
        current_year = group.loc[group["date"].dt.year == latest_year].copy()
        for _, row in current_year.iterrows():
            samples = _storage_prior_year_same_week_samples(
                group,
                target_date=pd.Timestamp(row["date"]),
                normal_years=normal_years,
            )
            out = {
                "date": row["date"],
                "value": float(row["value"]),
                "region": region,
                "sample_count": int(len(samples)),
            }
            if len(samples) >= 3:
                avg = float(samples.mean())
                min_value = float(samples.min())
                max_value = float(samples.max())
                deviation_bcf = float(row["value"]) - avg
                deviation_pct = (deviation_bcf / avg * 100.0) if avg else np.nan
                out.update(
                    {
                        "five_year_avg": avg,
                        "five_year_min": min_value,
                        "five_year_max": max_value,
                        "deviation_bcf": deviation_bcf,
                        "deviation_pct": deviation_pct,
                    }
                )
            else:
                out.update(
                    {
                        "five_year_avg": np.nan,
                        "five_year_min": np.nan,
                        "five_year_max": np.nan,
                        "deviation_bcf": np.nan,
                        "deviation_pct": np.nan,
                    }
                )
            rows.append(out)

    result = pd.DataFrame(rows).sort_values(["region", "date"]).reset_index(drop=True)
    if added_region and "region" in result.columns:
        result = result.drop(columns=["region"])
    return result


def _storage_current_vs_five_year_baseline(
    df: pd.DataFrame,
    *,
    normal_years: int = 5,
) -> dict | None:
    series_df = _storage_current_year_vs_baseline_series(df, normal_years=normal_years)
    if series_df.empty:
        return None
    latest = _storage_latest_by_region_df(series_df).dropna(subset=["five_year_avg"])
    if latest.empty:
        return None
    latest = latest.copy()
    latest["latest_date"] = latest["date"].dt.date.astype(str)
    latest["current_value"] = pd.to_numeric(latest["value"], errors="coerce")
    if "region" not in latest.columns:
        row = latest.iloc[0]
        return {
            "latest_date": row["latest_date"],
            "current_value": float(row["current_value"]),
            "five_year_avg": float(row["five_year_avg"]),
            "five_year_min": float(row["five_year_min"]),
            "five_year_max": float(row["five_year_max"]),
            "deviation_bcf": float(row["deviation_bcf"]),
            "deviation_pct": float(row["deviation_pct"]),
            "sample_count": int(row["sample_count"]),
        }
    return {"rows": latest.to_dict(orient="records")}


def _storage_same_week_baseline_by_region(
    df: pd.DataFrame,
    *,
    normal_years: int = 5,
) -> pd.DataFrame:
    return _storage_current_year_vs_baseline_series(df, normal_years=normal_years)


def _storage_deviation_from_normal_df(df: pd.DataFrame) -> pd.DataFrame:
    d = _storage_current_year_vs_baseline_series(df)
    if d.empty or "five_year_avg" not in d.columns:
        return pd.DataFrame(columns=["date", "value", "deviation_bcf"])
    return d.dropna(
        subset=["date", "value", "five_year_avg", "deviation_bcf"]
    ).reset_index(drop=True)


def _storage_region_deviation_ranking_df(
    df: pd.DataFrame,
) -> pd.DataFrame:
    baseline = _storage_current_vs_five_year_baseline(df)
    if baseline is None or "rows" not in baseline:
        return pd.DataFrame(
            columns=[
                "region",
                "latest_date",
                "current_storage_bcf",
                "five_year_avg_bcf",
                "deviation_bcf",
                "deviation_pct",
            ]
        )
    ranking_df = pd.DataFrame(baseline["rows"]).copy()
    if ranking_df.empty:
        return ranking_df
    ranking_df = ranking_df.rename(
        columns={
            "current_value": "current_storage_bcf",
            "five_year_avg": "five_year_avg_bcf",
        }
    )
    keep = [
        "region",
        "latest_date",
        "current_storage_bcf",
        "five_year_avg_bcf",
        "deviation_bcf",
        "deviation_pct",
    ]
    return ranking_df[keep].dropna(subset=["deviation_bcf"]).reset_index(drop=True)


def _storage_period_change_summary(df: pd.DataFrame) -> dict:
    d = _storage_prepare_df(df)
    if d.empty:
        return {}
    if "region" in d.columns and d["region"].nunique() > 1:
        latest_rows = _storage_latest_by_region_df(d)
        return {
            "latest_by_region": latest_rows,
            "latest_date": latest_rows["date"].max().date().isoformat(),
        }
    if "state" in d.columns and d["state"].nunique() > 1:
        latest_rows = _storage_latest_by_region_df(d)
        return {
            "latest_by_region": latest_rows,
            "latest_date": latest_rows["date"].max().date().isoformat(),
        }
    if "region" in d.columns:
        d = d.drop(columns=["region"])
    if "state" in d.columns:
        d = d.drop(columns=["state"])
    start = d.iloc[0]
    latest = d.iloc[-1]
    start_value = _safe_float(start["value"])
    latest_value = _safe_float(latest["value"])
    return {
        "start_date": start["date"].date().isoformat(),
        "start_value": start_value,
        "latest_date": latest["date"].date().isoformat(),
        "latest_value": latest_value,
        "net_change": (
            latest_value - start_value
            if latest_value is not None and start_value is not None
            else None
        ),
    }


def _underground_storage_period_change_summary(df: pd.DataFrame) -> dict:
    d = _underground_storage_prepare_df(df)
    if d.empty:
        return {}
    group_column = _underground_storage_group_column(d)
    if group_column in d.columns and d[group_column].nunique() > 1:
        latest_rows = _underground_storage_latest_by_state_df(d)
        return {
            "latest_by_geography": latest_rows,
            "latest_date": latest_rows["date"].max().date().isoformat(),
        }
    for column in ("state", "region", "geography"):
        if column in d.columns:
            d = d.drop(columns=[column])
    start = d.iloc[0]
    latest = d.iloc[-1]
    start_value = _safe_float(start["value"])
    latest_value = _safe_float(latest["value"])
    return {
        "start_date": start["date"].date().isoformat(),
        "start_value": start_value,
        "latest_date": latest["date"].date().isoformat(),
        "latest_value": latest_value,
        "net_change": (
            latest_value - start_value
            if latest_value is not None and start_value is not None
            else None
        ),
    }


def _storage_chart_spec_from_route(
    route: Any,
    df: pd.DataFrame,
) -> ChartSpec | None:
    if _route_output_mode(route) == "answer" or _route_chart_type(route) == "none":
        return None

    analysis_type = _route_analysis_type(route)
    chart_type = _route_chart_type(route)
    storage_dataset = _route_storage_dataset(route)
    metric_type = _route_storage_metric_type(route)
    metric_label = _storage_metric_label(metric_type)
    metric_unit = _storage_metric_unit(metric_type)
    if not chart_type:
        chart_type = {
            "time_series": "line",
            "regional_compare": "bar",
            "ranking": "bar",
            "weekly_change": "line",
            "seasonal_compare": "seasonal_line",
        }.get(analysis_type, "line")

    if analysis_type == "seasonal_compare":
        return ChartSpec(
            chart_type="seasonal_line",
            title="Working Gas in Storage vs 5-Year Average",
            x="date",
            y=["value", "five_year_avg"],
            x_label="Date",
            y_label="Bcf",
        )

    if analysis_type in {"regional_compare", "ranking"}:
        y_field = "deviation_bcf" if "deviation_bcf" in df.columns else "value"
        x_field = "state" if "state" in df.columns and "region" not in df.columns else "region"
        if y_field == "deviation_bcf":
            title = "Storage Deviation from 5-Year Average by Region"
        elif _route_value_type(route) == "weekly_change":
            title = "Weekly Change in Working Gas Storage by Region"
        elif storage_dataset == "underground_storage_all_operators":
            title = f"{metric_label} by State"
        else:
            title = "Current Working Gas in Storage by Region"
        return ChartSpec(
            chart_type="bar",
            title=title,
            x=x_field,
            y=[y_field],
            x_label="State" if x_field == "state" else "Region",
            y_label="Deviation (Bcf)" if y_field == "deviation_bcf" else metric_unit,
        )

    if analysis_type == "weekly_change":
        return ChartSpec(
            chart_type="line" if chart_type != "bar" else "bar",
            title="Weekly Change in Working Gas Storage",
            x="date" if chart_type != "bar" else "region",
            y=["value"],
            x_label="Date" if chart_type != "bar" else "Region",
            y_label="Bcf",
        )

    if analysis_type == "deviation_from_normal" and "deviation_bcf" in df.columns:
        return ChartSpec(
            chart_type="line" if chart_type != "bar" else "bar",
            title="Storage Deviation from 5-Year Average",
            x="date" if chart_type != "bar" else "region",
            y=["deviation_bcf"],
            x_label="Date" if chart_type != "bar" else "Region",
            y_label="Bcf vs 5Y Avg",
        )

    if analysis_type == "time_series" and chart_type == "line":
        return ChartSpec(
            chart_type="line",
            title=metric_label if storage_dataset == "underground_storage_all_operators" else "Working Gas in Storage",
            x="date",
            y=["value"],
            x_label="Date",
            y_label=metric_unit if storage_dataset == "underground_storage_all_operators" else "Storage (Bcf)",
        )

    return ChartSpec(
        chart_type="line",
        title=metric_label if storage_dataset == "underground_storage_all_operators" else "Working Gas in Storage",
        x="date",
        y=["value"],
        x_label="Date",
        y_label=metric_unit if storage_dataset == "underground_storage_all_operators" else "Storage (Bcf)",
    )


def _storage_payload(
    *,
    query: str,
    result: EIAResult,
    mode: str,
    answer_text: str,
    chart_df: pd.DataFrame,
    chart_spec: ChartSpec | None,
    source_date: str | None,
) -> AnswerPayload:
    return AnswerPayload(
        query=query,
        mode=mode,
        answer_text=answer_text,
        structured_response=None,
        report_context_used=False,
        report_context_reason="storage_route",
        report_context_sources=[AnswerSourceSummary(title=result.source.label, date=source_date)],
        data_preview=_maybe_data_preview(chart_df),
        chart_data_preview=_make_chart_preview(chart_df),
        chart_spec=chart_spec,
        sources=[result.source],
    )


def _storage_single_latest_answer(df: pd.DataFrame, route: Any) -> str:
    latest = _storage_latest_by_region_df(df)
    if latest.empty:
        return "No data was returned for the requested period."
    row = latest.iloc[0]
    date = row["date"].date().isoformat()
    region = _storage_region_label(row.get("region") or (_route_regions(route) or ["lower48"])[0])
    value = float(row["value"])
    if _route_value_type(route) == "weekly_change":
        flow = "injection" if value > 0 else "withdrawal" if value < 0 else "change"
        return f"As of {date}, {region} posted a weekly {flow} of {_format_number(abs(value))} Bcf."
    return f"As of {date}, {region} working gas in storage was {_format_number(value)} Bcf."


def _underground_storage_latest_answer(df: pd.DataFrame, route: Any) -> str:
    latest = _underground_storage_latest_by_state_df(df)
    if latest.empty:
        return "No data was returned for the requested period."
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))
    group_column = _underground_storage_group_column(latest)
    if group_column in latest.columns and latest[group_column].nunique() > 1:
        ranked = latest.sort_values("value", ascending=False).reset_index(drop=True)
        top = ranked.iloc[0]
        date = top["date"].date().isoformat()
        answer = (
            f"As of {date}, {_underground_storage_geography_label(group_column, top[group_column])} had the highest {metric_label.lower()} "
            f"at {_format_number(float(top['value']))} {unit}."
        )
        if len(ranked) > 1:
            second = ranked.iloc[1]
            answer += (
                f" {_underground_storage_geography_label(group_column, second[group_column])} followed at "
                f"{_format_number(float(second['value']))} {unit}."
            )
        return answer
    row = latest.iloc[0]
    date = row["date"].date().isoformat()
    geography_value = row.get(group_column)
    if geography_value is None:
        geography_value = (_route_states(route) or _route_regions(route) or ["united_states_total"])[0]
    geography = _underground_storage_geography_label(group_column, geography_value)
    return f"As of {date}, {geography} {metric_label.lower()} was {_format_number(float(row['value']))} {unit}."


def _underground_storage_time_series_answer(df: pd.DataFrame, route: Any) -> str:
    d = _underground_storage_prepare_df(df)
    if d.empty:
        return "No data was returned for the requested period."
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))
    group_column = _underground_storage_group_column(d)
    if group_column in d.columns and d[group_column].nunique() > 1:
        latest = _underground_storage_latest_by_state_df(d).sort_values("value", ascending=False)
        date = latest["date"].max().date().isoformat()
        pairs = [
            f"{_underground_storage_geography_label(group_column, row[group_column])}: {_format_number(float(row['value']))} {unit}"
            for _, row in latest.iterrows()
        ]
        return f"As of {date}, latest geography {metric_label.lower()} was " + "; ".join(pairs) + "."

    summary = _underground_storage_period_change_summary(d)
    geography = _underground_storage_geography_label(
        group_column,
        d.iloc[0][group_column] if group_column in d.columns and not d.empty else ((_route_states(route) or _route_regions(route) or ["united_states_total"])[0]),
    )
    return (
        f"From {summary.get('start_date')} to {summary.get('latest_date')}, {geography} {metric_label.lower()} moved "
        f"from {_format_number(summary.get('start_value'))} {unit} to {_format_number(summary.get('latest_value'))} {unit}, "
        f"a net change of {_format_number(summary.get('net_change'))} {unit}."
    )


def _underground_storage_ranking_answer(df: pd.DataFrame, route: Any) -> str:
    latest = _underground_storage_latest_by_state_df(df)
    if latest.empty:
        return "No data was returned for the requested period."
    group_column = _underground_storage_group_column(latest)
    if group_column not in latest.columns:
        return "No data was returned for the requested period."
    ranked = latest.dropna(subset=["value"]).sort_values("value", ascending=False)
    if ranked.empty:
        return "No data was returned for the requested period."
    top = ranked.iloc[0]
    date = top["date"].date().isoformat()
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))
    return (
        f"As of {date}, {_underground_storage_geography_label(group_column, top[group_column])} ranked highest for {metric_label.lower()} "
        f"at {_format_number(float(top['value']))} {unit}."
    )


def _underground_storage_by_type_latest_answer(df: pd.DataFrame, route: Any) -> str:
    latest = _underground_storage_latest_by_type_df(df)
    if latest.empty:
        return "No data was returned for the requested period."
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))
    if "storage_type" in latest.columns and latest["storage_type"].nunique() > 1:
        ranked = latest.sort_values("value", ascending=False).reset_index(drop=True)
        top = ranked.iloc[0]
        date = top["date"].date().isoformat()
        return (
            f"As of {date}, {_storage_type_label(top['storage_type'])} had the highest {metric_label.lower()} "
            f"at {_format_number(float(top['value']))} {unit}."
        )
    row = latest.iloc[0]
    date = row["date"].date().isoformat()
    storage_type = _storage_type_label(row.get("storage_type") or _route_storage_type(route))
    return f"As of {date}, {storage_type} {metric_label.lower()} was {_format_number(float(row['value']))} {unit}."


def _underground_storage_by_type_time_series_answer(df: pd.DataFrame, route: Any) -> str:
    d = _underground_storage_prepare_df(df)
    if d.empty:
        return "No data was returned for the requested period."
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))
    if "storage_type" in d.columns and d["storage_type"].nunique() > 1:
        latest = _underground_storage_latest_by_type_df(d).sort_values("value", ascending=False)
        date = latest["date"].max().date().isoformat()
        pairs = [
            f"{_storage_type_label(row['storage_type'])}: {_format_number(float(row['value']))} {unit}"
            for _, row in latest.iterrows()
        ]
        return f"As of {date}, latest storage type {metric_label.lower()} was " + "; ".join(pairs) + "."

    summary = _underground_storage_period_change_summary(d)
    storage_type = _storage_type_label(
        d.iloc[0]["storage_type"] if "storage_type" in d.columns and not d.empty else _route_storage_type(route)
    )
    return (
        f"From {summary.get('start_date')} to {summary.get('latest_date')}, {storage_type} {metric_label.lower()} moved "
        f"from {_format_number(summary.get('start_value'))} {unit} to {_format_number(summary.get('latest_value'))} {unit}, "
        f"a net change of {_format_number(summary.get('net_change'))} {unit}."
    )


def _underground_storage_by_type_ranking_answer(df: pd.DataFrame, route: Any) -> str:
    latest = _underground_storage_latest_by_type_df(df)
    if latest.empty or "storage_type" not in latest.columns:
        return "No data was returned for the requested period."
    ranked = latest.dropna(subset=["value"]).sort_values("value", ascending=False)
    if ranked.empty:
        return "No data was returned for the requested period."
    top = ranked.iloc[0]
    date = top["date"].date().isoformat()
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))
    return (
        f"As of {date}, {_storage_type_label(top['storage_type'])} ranked highest for {metric_label.lower()} "
        f"at {_format_number(float(top['value']))} {unit}."
    )


def _underground_storage_chart_spec_from_route(
    route: Any,
    df: pd.DataFrame,
) -> ChartSpec | None:
    if _route_output_mode(route) == "answer" or _route_chart_type(route) == "none":
        return None

    analysis_type = _route_analysis_type(route)
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))

    if analysis_type == "time_series":
        return ChartSpec(
            chart_type="line",
            title=f"{metric_label} by Geography",
            x="date",
            y=["value"],
            x_label="Date",
            y_label=unit,
        )

    if analysis_type in {"ranking", "regional_compare"} or (
        analysis_type == "latest" and _route_chart_type(route) == "bar"
    ):
        x_field = "state" if "state" in df.columns else "region" if "region" in df.columns else "geography"
        x_label = "State" if x_field == "state" else "Region" if x_field == "region" else "Geography"
        return ChartSpec(
            chart_type="bar",
            title=f"{metric_label} by {x_label}",
            x=x_field,
            y=["value"],
            x_label=x_label,
            y_label=unit,
        )

    return None


def _underground_storage_by_type_chart_spec_from_route(
    route: Any,
    df: pd.DataFrame,
) -> ChartSpec | None:
    if _route_output_mode(route) == "answer" or _route_chart_type(route) == "none":
        return None

    analysis_type = _route_analysis_type(route)
    metric_label = _underground_storage_metric_label(_route_storage_metric_type(route))
    unit = _underground_storage_unit(_route_storage_metric_type(route))

    if analysis_type == "time_series":
        return ChartSpec(
            chart_type="line",
            title=f"{metric_label} by Storage Type",
            x="date",
            y=["value"],
            x_label="Date",
            y_label=unit,
        )

    if analysis_type in {"ranking", "regional_compare"} or (
        analysis_type == "latest" and _route_chart_type(route) == "bar"
    ):
        return ChartSpec(
            chart_type="bar",
            title=f"{metric_label} by Storage Type",
            x="storage_type",
            y=["value"],
            x_label="Storage Type",
            y_label=unit,
        )

    return None


def _storage_time_series_answer(df: pd.DataFrame, route: Any) -> str:
    d = _storage_prepare_df(df)
    if d.empty:
        return "No data was returned for the requested period."
    if "region" in d.columns and d["region"].nunique() > 1:
        latest = _storage_latest_by_region_df(d).sort_values("value", ascending=False)
        date = latest["date"].max().date().isoformat()
        pairs = [
            f"{_storage_region_label(row['region'])}: {_format_number(float(row['value']))} Bcf"
            for _, row in latest.iterrows()
        ]
        return f"As of {date}, latest regional storage was " + "; ".join(pairs) + "."

    summary = _storage_period_change_summary(d)
    region = _storage_region_label((_route_regions(route) or ["lower48"])[0])
    return (
        f"From {summary.get('start_date')} to {summary.get('latest_date')}, {region} storage moved "
        f"from {_format_number(summary.get('start_value'))} Bcf to {_format_number(summary.get('latest_value'))} Bcf, "
        f"a net change of {_format_number(summary.get('net_change'))} Bcf."
    )


def _storage_regional_rank_answer(df: pd.DataFrame, *, value_field: str = "value") -> str:
    latest = _storage_latest_by_region_df(df)
    if latest.empty or "region" not in latest.columns or value_field not in latest.columns:
        return "No data was returned for the requested period."
    ranked = latest.dropna(subset=[value_field]).sort_values(value_field, ascending=False)
    if ranked.empty:
        return "No data was returned for the requested period."
    top = ranked.iloc[0]
    date = top["date"].date().isoformat()
    unit = "Bcf" if value_field == "value" else "Bcf versus the five-year average"
    return (
        f"As of {date}, {_storage_region_label(top['region'])} ranked highest at "
        f"{_format_number(float(top[value_field]))} {unit}."
    )


def _storage_deviation_sort_ascending(route: Any) -> bool:
    query = str(getattr(route, "normalized_query", "") or "")
    descending_terms = ("above normal", "surplus", "loosest", "loose")
    ascending_terms = ("below normal", "deficit", "tight", "tightest")
    if any(term in query for term in descending_terms):
        return False
    if any(term in query for term in ascending_terms):
        return True
    return False


def _storage_deviation_ranking_answer(df: pd.DataFrame, route: Any) -> str:
    if df.empty:
        return "No data was returned for the requested period."
    ascending = _storage_deviation_sort_ascending(route)
    ranked = df.sort_values("deviation_bcf", ascending=ascending).reset_index(drop=True)
    top = ranked.iloc[0]
    latest_date = str(top["latest_date"])
    top_region = _storage_region_label(top["region"])
    top_deviation = float(top["deviation_bcf"])
    direction = "below normal" if top_deviation < 0 else "above normal"
    if len(ranked) == 1:
        return (
            f"As of {latest_date}, {top_region} storage was the furthest {direction} at "
            f"{_format_number(abs(top_deviation))} Bcf {direction} versus its same-week five-year average."
        )
    second = ranked.iloc[1]
    second_region = _storage_region_label(second["region"])
    second_deviation = float(second["deviation_bcf"])
    if top_deviation < 0:
        return (
            f"As of {latest_date}, {top_region} storage was the furthest below normal at "
            f"{_format_number(abs(top_deviation))} Bcf below its same-week five-year average. "
            f"{second_region} ranked second at {_format_number(abs(second_deviation))} Bcf below normal."
        )
    return (
        f"As of {latest_date}, {top_region} storage was the furthest above normal at "
        f"{_format_number(abs(top_deviation))} Bcf above its same-week five-year average. "
        f"{second_region} ranked second at {_format_number(abs(second_deviation))} Bcf above normal."
    )


def _storage_weekly_change_answer(df: pd.DataFrame) -> str:
    d = _storage_prepare_df(df)
    if d.empty:
        return "No data was returned for the requested period."
    if "region" in d.columns and d["region"].nunique() > 1:
        return _storage_regional_rank_answer(_storage_latest_by_region_df(d), value_field="value")
    if len(d) == 1:
        value = float(d.iloc[-1]["value"])
        flow = "injection" if value > 0 else "withdrawal" if value < 0 else "change"
        return f"As of {d.iloc[-1]['date'].date().isoformat()}, the latest weekly {flow} was {_format_number(abs(value))} Bcf."
    latest = float(d.iloc[-1]["value"])
    previous = float(d.iloc[-2]["value"])
    latest_flow = "injection" if latest > 0 else "withdrawal" if latest < 0 else "change"
    if latest > previous:
        direction = "accelerating"
    elif latest < previous:
        direction = "slowing"
    else:
        direction = "unchanged"
    return (
        f"As of {d.iloc[-1]['date'].date().isoformat()}, the latest weekly {latest_flow} was "
        f"{_format_number(abs(latest))} Bcf versus {_format_number(abs(previous))} Bcf the prior week, "
        f"so weekly changes are {direction}."
    )


def _storage_seasonal_compare_answer(df: pd.DataFrame, route: Any) -> str:
    baseline = _storage_current_vs_five_year_baseline(df)
    if baseline is None:
        return "Not enough same-week history was returned to compute a five-year average."
    if "rows" in baseline:
        latest = pd.DataFrame(baseline["rows"]).iloc[0]
        region = _storage_region_label(latest.get("region") or (_route_regions(route) or ["lower48"])[0])
        latest_date = str(latest["latest_date"])
        value = float(latest["current_value"])
        avg = float(latest["five_year_avg"])
        min_value = float(latest["five_year_min"])
        max_value = float(latest["five_year_max"])
        diff = float(latest["deviation_bcf"])
        pct = float(latest["deviation_pct"]) if pd.notna(latest["deviation_pct"]) else None
    else:
        region = _storage_region_label((_route_regions(route) or ["lower48"])[0])
        latest_date = str(baseline["latest_date"])
        value = float(baseline["current_value"])
        avg = float(baseline["five_year_avg"])
        min_value = float(baseline["five_year_min"])
        max_value = float(baseline["five_year_max"])
        diff = float(baseline["deviation_bcf"])
        pct = float(baseline["deviation_pct"]) if pd.notna(baseline["deviation_pct"]) else None
    direction = "above normal" if diff > 0 else "below normal" if diff < 0 else "in line with normal"
    pct_text = f", or {_format_number(abs(pct))}% {direction.replace(' normal', '')}" if pct is not None and diff != 0 else ""
    comparisons = set(_route_comparisons(route))
    if "five_year_range" in comparisons:
        return (
            f"As of {latest_date}, {region} storage was {_format_number(value)} Bcf versus a "
            f"five-year same-week range of {_format_number(min_value)} to {_format_number(max_value)} Bcf."
        )
    return (
        f"As of {latest_date}, {region} working gas in storage was {_format_number(value)} Bcf versus a "
        f"same-week five-year average of {_format_number(avg)} Bcf. Storage was "
        f"{_format_number(abs(diff))} Bcf {'above' if diff > 0 else 'below' if diff < 0 else 'in line with'} normal{pct_text}."
    )


def _storage_deviation_answer(df: pd.DataFrame) -> str:
    d = _storage_deviation_from_normal_df(df)
    if d.empty:
        return "Not enough same-week history was returned to compute storage deviation from normal."
    if "region" in d.columns and d["region"].nunique() > 1:
        return _storage_regional_rank_answer(d, value_field="deviation_bcf")
    if len(d) == 1:
        row = d.iloc[-1]
        avg = float(row["five_year_avg"])
        diff = float(row["deviation_bcf"])
        return (
            f"As of {row['date'].date().isoformat()}, {_storage_region_label(row.get('region')) if 'region' in row else 'storage'} "
            f"was {_format_number(float(row['value']))} Bcf, which was "
            f"{_format_number(abs(diff))} Bcf {'above' if diff > 0 else 'below' if diff < 0 else 'in line with'} "
            f"the same-week five-year average of {_format_number(avg)} Bcf."
        )
    latest = d.iloc[-1]
    previous = d.iloc[-2]
    latest_dev = float(latest["deviation_bcf"])
    previous_dev = float(previous["deviation_bcf"])
    status = "shrinking" if abs(latest_dev) < abs(previous_dev) else "widening" if abs(latest_dev) > abs(previous_dev) else "unchanged"
    noun = "deficit" if latest_dev < 0 else "surplus"
    return (
        f"The storage {noun} is {status} because the deviation moved from "
        f"{_format_number(previous_dev)} Bcf to {_format_number(latest_dev)} Bcf."
    )


def _build_underground_storage_all_operators_payload(
    *,
    query: str,
    result: EIAResult,
    route: Any,
    mode: str,
    source_date: str | None,
) -> AnswerPayload:
    df = _underground_storage_prepare_df(result.df)
    analysis_type = _route_analysis_type(route)

    if analysis_type == "time_series":
        group_column = _underground_storage_group_column(df) if not df.empty else "state"
        chart_df = df.sort_values([group_column, "date"]) if not df.empty and group_column in df.columns else df
        answer_text = _underground_storage_time_series_answer(df, route)
    elif analysis_type in {"ranking", "regional_compare"}:
        chart_df = _underground_storage_latest_by_state_df(df).sort_values(
            "value", ascending=False
        ).reset_index(drop=True)
        answer_text = _underground_storage_ranking_answer(chart_df, route)
    elif analysis_type == "latest":
        chart_df = _underground_storage_latest_by_state_df(df)
        answer_text = _underground_storage_latest_answer(df, route)
    else:
        chart_df = _underground_storage_latest_by_state_df(df)
        answer_text = _underground_storage_latest_answer(df, route)

    if not chart_df.empty and ("state" in chart_df.columns or "region" in chart_df.columns or "geography" in chart_df.columns):
        label_column = "state" if "state" in chart_df.columns else "region" if "region" in chart_df.columns else "geography"
        logger.info(
            "underground_storage_answer chart_geographies=%s rows=%s analysis=%s states_all=%s",
            sorted(chart_df[label_column].dropna().astype(str).unique().tolist()),
            len(chart_df),
            analysis_type,
            _route_states_all(route),
        )

    chart_spec = _underground_storage_chart_spec_from_route(route, chart_df)
    return AnswerPayload(
        query=query,
        mode=mode,
        answer_text=answer_text,
        structured_response=None,
        report_context_used=False,
        report_context_reason="underground_storage_all_operators_route",
        report_context_sources=[AnswerSourceSummary(title=result.source.label, date=source_date)],
        data_preview=_maybe_data_preview(chart_df),
        chart_data_preview=_make_chart_preview(chart_df),
        chart_spec=chart_spec,
        sources=[result.source],
        warnings=None,
        generated_at=datetime.utcnow(),
    )


def _build_underground_storage_by_type_payload(
    *,
    query: str,
    result: EIAResult,
    route: Any,
    mode: str,
    source_date: str | None,
) -> AnswerPayload:
    df = _underground_storage_prepare_df(result.df)
    analysis_type = _route_analysis_type(route)

    if analysis_type == "time_series":
        chart_df = df.sort_values(["storage_type", "date"]) if not df.empty else df
        answer_text = _underground_storage_by_type_time_series_answer(df, route)
    elif analysis_type in {"ranking", "regional_compare"}:
        chart_df = _underground_storage_latest_by_type_df(df).sort_values(
            "value", ascending=False
        ).reset_index(drop=True)
        answer_text = _underground_storage_by_type_ranking_answer(chart_df, route)
    elif analysis_type == "latest":
        chart_df = _underground_storage_latest_by_type_df(df)
        answer_text = _underground_storage_by_type_latest_answer(df, route)
    else:
        chart_df = _underground_storage_latest_by_type_df(df)
        answer_text = _underground_storage_by_type_latest_answer(df, route)

    chart_spec = _underground_storage_by_type_chart_spec_from_route(route, chart_df)
    return AnswerPayload(
        query=query,
        mode=mode,
        answer_text=answer_text,
        structured_response=None,
        report_context_used=False,
        report_context_reason="underground_storage_by_type_route",
        report_context_sources=[AnswerSourceSummary(title=result.source.label, date=source_date)],
        data_preview=_maybe_data_preview(chart_df),
        chart_data_preview=_make_chart_preview(chart_df),
        chart_spec=chart_spec,
        sources=[result.source],
        warnings=None,
        generated_at=datetime.utcnow(),
    )


def _build_storage_answer_payload(
    *,
    query: str,
    result: EIAResult,
    route: Any,
    mode: str,
    source_date: str | None,
) -> AnswerPayload:
    storage_dataset = _route_storage_dataset(route)

    if storage_dataset == "underground_storage_all_operators":
        return _build_underground_storage_all_operators_payload(
            query=query,
            result=result,
            route=route,
            mode=mode,
            source_date=source_date,
        )
    if storage_dataset == "underground_storage_by_type":
        return _build_underground_storage_by_type_payload(
            query=query,
            result=result,
            route=route,
            mode=mode,
            source_date=source_date,
        )
    if storage_dataset != "weekly_working_gas":
        return _storage_payload(
            query=query,
            result=result,
            mode=mode,
            answer_text="No data was returned for the requested storage dataset.",
            chart_df=pd.DataFrame(),
            chart_spec=None,
            source_date=source_date,
        )

    df = _storage_prepare_df(result.df)
    analysis_type = _route_analysis_type(route)
    comparisons = _route_comparisons(route)
    chart_df = df
    if analysis_type in {"regional_compare"}:
        chart_df = _storage_latest_by_region_df(df).sort_values("value", ascending=False)
        answer_text = _storage_regional_rank_answer(chart_df)
    elif analysis_type == "ranking":
        if _route_ranking_basis(route) == "deviation_from_normal" or any(
            c in comparisons for c in {"five_year_avg", "seasonal_normal"}
        ):
            chart_df = _storage_region_deviation_ranking_df(df)
            chart_df = chart_df.sort_values(
                "deviation_bcf",
                ascending=_storage_deviation_sort_ascending(route),
            ).reset_index(drop=True)
            answer_text = _storage_deviation_ranking_answer(chart_df, route)
        else:
            chart_df = _storage_latest_by_region_df(df).sort_values("value", ascending=False)
            answer_text = _storage_regional_rank_answer(chart_df)
    elif analysis_type == "weekly_change":
        if _route_chart_type(route) == "bar" and "region" in df.columns:
            chart_df = _storage_latest_by_region_df(df).sort_values("value", ascending=False)
        answer_text = _storage_weekly_change_answer(df)
    elif analysis_type == "seasonal_compare":
        chart_df = _storage_same_week_baseline_by_region(df).dropna(subset=["five_year_avg"])
        answer_text = _storage_seasonal_compare_answer(df, route)
    elif analysis_type == "deviation_from_normal":
        chart_df = _storage_deviation_from_normal_df(df)
        answer_text = _storage_deviation_answer(df)
    elif analysis_type == "time_series":
        chart_df = df.sort_values(["region", "date"] if "region" in df.columns else ["date"])
        answer_text = _storage_time_series_answer(df, route)
    else:
        chart_df = _storage_latest_by_region_df(df)
        answer_text = _storage_single_latest_answer(df, route)

    if not chart_df.empty and "region" in chart_df.columns:
        logger.info(
            "storage_answer chart_regions=%s rows=%s analysis=%s",
            sorted(chart_df["region"].dropna().astype(str).unique().tolist()),
            len(chart_df),
            analysis_type,
        )
    if not chart_df.empty and "state" in chart_df.columns:
        logger.info(
            "storage_answer chart_states=%s rows=%s analysis=%s",
            sorted(chart_df["state"].dropna().astype(str).unique().tolist()),
            len(chart_df),
            analysis_type,
        )

    chart_spec = _storage_chart_spec_from_route(route, chart_df)
    return _storage_payload(
        query=query,
        result=result,
        mode=mode,
        answer_text=answer_text,
        chart_df=chart_df,
        chart_spec=chart_spec,
        source_date=source_date,
    )


def _storage_level_and_change_structured_answer(
    *,
    df: pd.DataFrame,
    region_label: str,
    query: str,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _storage_level_and_change_answer(df, region_label=region_label, query=query)
    if answer == "No data was returned for the requested period.":
        return StructuredAnswer(
            answer=answer,
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No storage level and weekly change data was returned for the requested period.",
            drivers=["The dataset did not include usable storage observations for this combined view."],
            data_points=[],
            forecast=AnswerForecast(
                direction="flat",
                reasoning="Forecast unavailable because no observations were returned.",
            ),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No Data Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    ordered = df.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
    if "weekly_change" in ordered.columns:
        ordered["weekly_change"] = pd.to_numeric(ordered["weekly_change"], errors="coerce")
    else:
        ordered["weekly_change"] = np.nan
    ordered = ordered.dropna(subset=["date", "value"]).sort_values("date")
    latest = ordered.iloc[-1]
    data_points = [
        AnswerDataPoint(metric="Storage", value=_json_safe(float(latest["value"])), unit="Bcf")
    ]
    if pd.notna(latest["weekly_change"]):
        data_points.append(
            AnswerDataPoint(
                metric="Weekly Change",
                value=_json_safe(float(latest["weekly_change"])),
                unit="Bcf",
            )
        )
    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status="neutral", confidence=0.76),
        summary=answer,
        drivers=[
            "This combined view shows storage level and derived week-over-week change for the same region.",
            "Weekly change is computed from row-to-row differences in the storage series.",
        ],
        data_points=data_points,
        forecast=AnswerForecast(
            direction="flat",
            reasoning="This response combines latest storage level and weekly change rather than inferring a forecast.",
        ),
        suggested_alerts=[],
        alerts=[],
        sources=[AnswerSourceSummary(title=source_label, date=source_date)],
    )


def _deterministic_answer_text(
    *, metric: str, query: str, facts: dict[str, Any], mode: str, df: pd.DataFrame | None = None
) -> str:
    latest_date = facts.get("latest_date")
    latest_value = facts.get("latest_value")
    prior_value = facts.get("prior_value")
    delta = facts.get("delta")
    n_points = int(facts.get("n_points") or 0)
    unit = METRIC_UNITS.get(metric)
    unit_suffix = f" {unit}" if unit else ""

    if n_points == 0 or latest_date is None or latest_value is None:
        return "No data was returned for the requested period."

    latest_value_text = _format_number(latest_value)
    delta_text = _format_delta(delta, unit)

    average_match = re.search(r"average .* last (\d+)\s+days?", (query or "").lower())
    if metric == "henry_hub_spot" and average_match and df is not None and not df.empty:
        days = int(average_match.group(1))
        scoped = df.copy()
        if {"date", "value"}.issubset(scoped.columns) and days > 0:
            scoped["date"] = pd.to_datetime(scoped["date"], errors="coerce")
            scoped["value"] = pd.to_numeric(scoped["value"], errors="coerce")
            scoped = scoped.dropna(subset=["date", "value"]).sort_values("date")
            if not scoped.empty:
                end_ts = scoped["date"].max()
                start_ts = end_ts - pd.Timedelta(days=max(0, days - 1))
                window = scoped.loc[scoped["date"] >= start_ts].copy()
                if not window.empty:
                    avg_value = float(window["value"].mean())
                    return (
                        f"Over the last {days} days ({start_ts.date().isoformat()} to {end_ts.date().isoformat()}), "
                        f"the average Henry Hub price was {_format_number(avg_value)} $/MMBtu."
                    )

    def _format_interpretive_baseline_answer(
        *,
        latest_date_str: str,
        latest_num: float,
        prior_num: float | None,
        avg_5y: float,
        min_5y: float,
        max_5y: float,
    ) -> str:
        diff = latest_num - avg_5y
        pct = ((diff / avg_5y) * 100.0) if avg_5y != 0 else None
        prior_diff = (latest_num - prior_num) if prior_num is not None else None
        category_map = {
            "working_gas_storage_lower48": ("storage", "storage_level"),
            "working_gas_storage_change_weekly": ("storage", "storage_change"),
            "weather_degree_days_forecast_vs_5y": ("weather", "demand_weather"),
            "ng_production_lower48": ("production", "dry_gas"),
            "lng_exports": ("exports", "lng_exports"),
            "lng_imports": ("imports", "imports"),
            "ng_electricity": ("consumption", "power_burn"),
            "ng_consumption_lower48": ("consumption", "total"),
            "henry_hub_spot": ("price", "spot"),
        }
        category, subtype = category_map.get(metric, ("consumption", None))
        metric_label = {
            "working_gas_storage_lower48": "Natural gas storage",
            "working_gas_storage_change_weekly": "Weekly storage change",
            "weather_degree_days_forecast_vs_5y": "Weather-driven gas demand",
            "ng_production_lower48": "Dry gas production",
            "lng_exports": "LNG exports",
            "lng_imports": "Natural gas imports",
            "ng_electricity": "Power-sector gas demand",
            "ng_consumption_lower48": "Natural gas consumption",
            "henry_hub_spot": "Henry Hub price",
        }.get(metric, "Natural gas metric")
        out = format_natural_gas_commentary(
            NaturalGasMetricSnapshot(
                metric_name=metric_label,
                category=category,
                subtype=subtype,
                date=str(latest_date_str),
                current_value=float(latest_num),
                unit=unit or "",
                baseline_5y=float(avg_5y),
                baseline_type="average",
                difference=float(diff),
                percent_difference=float(pct) if pct is not None else None,
                prior_value=float(prior_num) if prior_num is not None else None,
                prior_difference=float(prior_diff) if prior_diff is not None else None,
                range_5y_min=float(min_5y),
                range_5y_max=float(max_5y),
            )
        )
        return str(out.get("summary") or "")

    def _should_auto_add_five_year_context() -> bool:
        q = (query or "").lower()
        has_five_year = bool(re.search(r"(?:five|5)\s*-?\s*year", q))
        if has_five_year or any(token in q for token in ("yoy", "year over year", "same week last year")):
            return False
        return any(token in q for token in ("latest", "current", "right now", "what is", "how much is"))

    def _asks_explicit_five_year_comparison() -> bool:
        q = (query or "").lower()
        has_five_year = bool(re.search(r"(?:five|5)\s*-?\s*year", q))
        has_baseline = any(
            token in q for token in ("average", "range", "seasonal", "historical", "history", "norm", "normal")
        )
        return has_five_year and has_baseline

    if prior_value is None or delta_text is None:
        if df is not None and (_should_auto_add_five_year_context() or _asks_explicit_five_year_comparison()):
            baseline = _five_year_same_time_baseline(df)
            if baseline is not None:
                avg_5y, min_5y, max_5y = baseline
                return _format_interpretive_baseline_answer(
                    latest_date_str=str(latest_date),
                    latest_num=float(latest_value),
                    prior_num=_safe_float(prior_value),
                    avg_5y=avg_5y,
                    min_5y=min_5y,
                    max_5y=max_5y,
                )
            if _asks_explicit_five_year_comparison():
                return (
                    f"As of {format_date_month_d_year(str(latest_date))}, the reading came in at {latest_value_text}{unit_suffix}. "
                    "Not enough same-time history was returned to compute a reliable five-year baseline."
                )
        return f"As of {format_date_month_d_year(str(latest_date))}, the reading came in at {latest_value_text}{unit_suffix}."

    if mode == "observed":
        base = f"As of {format_date_month_d_year(str(latest_date))}, the reading came in at {latest_value_text}{unit_suffix} and {delta_text}."
        if df is not None and (_should_auto_add_five_year_context() or _asks_explicit_five_year_comparison()):
            baseline = _five_year_same_time_baseline(df)
            if baseline is not None:
                avg_5y, min_5y, max_5y = baseline
                return _format_interpretive_baseline_answer(
                    latest_date_str=str(latest_date),
                    latest_num=float(latest_value),
                    prior_num=_safe_float(prior_value),
                    avg_5y=avg_5y,
                    min_5y=min_5y,
                    max_5y=max_5y,
                )
            if _asks_explicit_five_year_comparison():
                return f"{base} Not enough same-time history was returned to compute a reliable five-year baseline."
        return base

    return (
        f"The latest observed reading came in at {latest_value_text}{unit_suffix} on {format_date_month_d_year(str(latest_date))}, "
        f"{delta_text}."
    )


def _ng_electricity_seasonal_norm_summary(
    *,
    df: pd.DataFrame,
    normal_years: int,
) -> Optional[dict[str, Any]]:
    summary = compute_ng_electricity_seasonal_norm_summary(
        df=df,
        normal_years=normal_years,
    )
    if summary is None:
        return None

    latest_date = pd.Timestamp(summary["latest_date"])
    latest_value = float(summary["latest_value"])
    normal_value = float(summary["normal_value"])
    delta_vs_normal = float(summary["delta_vs_normal"])
    pct_vs_normal = summary.get("pct_vs_normal")
    samples = int(summary["samples"])
    direction = "above" if delta_vs_normal > 0 else "below" if delta_vs_normal < 0 else "in line with"
    month_label = latest_date.strftime("%B %Y")
    normal_label = f"{max(1, int(normal_years))}-year seasonal norm"

    if direction == "in line with":
        text = (
            f"As of {latest_date.date().isoformat()}, natural gas power burn was "
            f"{_format_number(latest_value)} MMcf, essentially in line with its {normal_label} "
            f"for {month_label} ({_format_number(normal_value)} MMcf)."
        )
    else:
        pct_text = (
            f" ({abs(pct_vs_normal):.1f}% {direction} normal)"
            if pct_vs_normal is not None
            else ""
        )
        text = (
            f"As of {latest_date.date().isoformat()}, natural gas power burn was "
            f"{_format_number(latest_value)} MMcf, {direction} the {normal_label} for {month_label} "
            f"({_format_number(normal_value)} MMcf) by {_format_number(abs(delta_vs_normal))} MMcf{pct_text}."
        )

    return {
        "text": text,
        **summary,
    }


def build_answer_with_openai(
    *,
    query: str,
    result: EIAResult,
    route: Any | None = None,
    mode: str = "observed",
    model: str = "gpt-5.2",
    **_: Any,
) -> AnswerPayload:
    df = result.df
    src = result.source

    metric = result.meta.get("metric", "") if result.meta else ""
    proxy_for_metric = str((result.meta or {}).get("proxy_for_metric") or "")
    proxy_note = str((result.meta or {}).get("proxy_note") or "").strip()
    raw_region_label = str(((result.meta or {}).get("filters") or {}).get("region") or "lower48")
    region_label = raw_region_label.replace("_", " ").title()
    if region_label == "Lower48":
        region_label = "Lower 48"
    source_date = src.retrieved_at.date().isoformat() if src.retrieved_at else None

    if route is not None and _route_domain(route) == "storage":
        return _build_storage_answer_payload(
            query=query,
            result=result,
            route=route,
            mode=mode,
            source_date=source_date,
        )

    report_context_text = ""
    report_context_sources: list[AnswerSourceSummary] = []
    report_context_used = False
    report_context_reason = "llm_narration_disabled"
    response_mode = os.getenv("ATLAS_RESPONSE_MODE", "fast").strip().lower()
    narration_enabled = (
        os.getenv("ATLAS_USE_LLM_NARRATION", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    use_llm_narration = narration_enabled and response_mode in {"analysis", "detailed"}
    prefer_report_narration = (
        use_llm_narration
        and _is_report_narrative_query(query)
        and metric
        in {
            "working_gas_storage_lower48",
            "working_gas_storage_change_weekly",
        }
    )

    if metric == "ng_consumption_by_sector":
        if proxy_for_metric == "ng_electricity":
            answer_text = _power_sector_proxy_answer(df)
            chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
            payload = AnswerPayload(
                query=query,
                mode=mode,
                answer_text=answer_text,
                structured_response=_power_sector_proxy_structured_answer(
                    df=df,
                    source_label=src.label,
                    source_date=source_date,
                    proxy_note=proxy_note,
                ),
                report_context_used=False,
                report_context_reason="power_sector_proxy_no_rag",
                report_context_sources=[],
                data_preview=_maybe_data_preview(df),
                chart_spec=chart_spec,
                sources=[src],
            )
            return payload

        answer_text = _deterministic_sector_consumption_answer(query=query, df=df)
        chart_df = _sector_consumption_chart_df(query=query, df=df)
        if not chart_df.empty:
            chart_spec = ChartSpec(
                chart_type="bar",
                title="Natural Gas Consumption by Sector (Latest)",
                x="sector",
                y=["value"],
                x_label="Sector",
                y_label="MMcf",
            )
        else:
            if df is not None and "date" in df.columns:
                df = df.sort_values("date").reset_index(drop=True)
            chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_deterministic_sector_structured_answer(
                query=query,
                df=df,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="ranking_response_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(chart_df if not chart_df.empty else df),
            chart_data_preview=_make_chart_preview(chart_df) if not chart_df.empty else None,
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_regional_storage_change_view(metric, df) and not prefer_report_narration:
        answer_text = _regional_storage_change_answer(df, query=query)
        if df is not None and "date" in df.columns:
            df = df.sort_values(["date", "region"]).reset_index(drop=True)
        chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_regional_storage_change_structured_answer(
                df=df,
                query=query,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="regional_storage_change_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_storage_level_and_change_view(metric, df) and not prefer_report_narration:
        answer_text = _storage_level_and_change_answer(df, region_label=region_label, query=query)
        chart_df = df
        if df is not None and "date" in df.columns:
            chart_df = df.sort_values("date").reset_index(drop=True)
        if _is_storage_five_year_comparison_query(query):
            comparison_df = _storage_same_week_yearly_comparison_df(chart_df)
            if not comparison_df.empty:
                chart_df = comparison_df
                chart_spec = ChartSpec(
                    chart_type="line",
                    title="Working Gas in Storage: Same-Week Comparison (5Y + Current)",
                    x="comparison_year",
                    y=["value"],
                    x_label="Year",
                    y_label="Storage (Bcf)",
                    notes="Line shows storage for approximately the same reporting week across the prior five years and current year.",
                )
            else:
                chart_spec = chart_policy(metric=metric, mode=mode, df=chart_df, query=query)
        else:
            chart_spec = chart_policy(metric=metric, mode=mode, df=chart_df, query=query)
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_storage_level_and_change_structured_answer(
                df=df,
                region_label=region_label,
                query=query,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="storage_level_and_change_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(chart_df),
            chart_data_preview=_make_chart_preview(chart_df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_weather_degree_day_forecast_view(metric, df):
        answer_text = _weather_degree_day_forecast_answer(df, query=query)
        if df is not None and "bucket_start_day" in df.columns:
            df = df.sort_values("bucket_start_day").reset_index(drop=True)
        chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_weather_degree_day_forecast_structured_answer(
                df=df,
                query=query,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="weather_degree_day_forecast_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_weather_regional_demand_drivers_view(metric, df):
        answer_text = _weather_regional_demand_drivers_answer(df)
        chart_spec = ChartSpec(
            chart_type="bar",
            title="Regional Weather-Driven Gas Demand",
            x="region",
            y=["demand_delta_bcfd"],
            x_label="Region",
            y_label="Demand Delta (Bcf/d)",
        )
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_weather_regional_demand_drivers_structured_answer(
                df=df,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="weather_regional_demand_drivers_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_regional_production_change_view(metric, df):
        answer_text = _regional_production_change_answer(df)
        chart_df = _regional_production_change_chart_df(df)
        chart_spec = (
            ChartSpec(
                chart_type="bar",
                title="Regional Contribution to Latest Production Change",
                x="region",
                y=["delta"],
                x_label="Region / State",
                y_label="Latest Change (MMcf)",
            )
            if not chart_df.empty
            else None
        )
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_regional_production_change_structured_answer(
                df=df,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="regional_production_change_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(chart_df if not chart_df.empty else df),
            chart_data_preview=_make_chart_preview(chart_df) if not chart_df.empty else None,
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_supply_balance_regime_view(metric, df) and not prefer_report_narration:
        answer_text = _supply_balance_regime_answer(df)
        chart_spec = None
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_supply_balance_regime_structured_answer(
                df=df,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="supply_balance_regime_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_weekly_energy_atlas_summary_view(metric, df):
        answer_text = _weekly_energy_atlas_summary_answer(df)
        chart_spec = ChartSpec(
            chart_type="bar",
            title="Market Pressure Dashboard",
            x="component",
            y=["score"],
            x_label="Driver",
            y_label="Pressure Score (Bullish + / Bearish -)",
            notes="Scorecard based on weekly direction of weather demand, storage surprise, LNG/supply, and price.",
        )
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_weekly_energy_atlas_summary_structured_answer(
                df=df,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="weekly_energy_atlas_summary_no_rag",
            report_context_sources=[],
            data_preview=_maybe_data_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    # 1) Deterministic facts
    if df is None or len(df) == 0:
        facts = {
            "latest_date": None,
            "latest_value": None,
            "prior_value": None,
            "delta": None,
            "n_points": 0,
            "columns": list(df.columns) if df is not None else [],
            "value_column": None,
        }
    else:
        latest = df.iloc[-1]
        latest_date = pd.to_datetime(latest["date"]).date().isoformat()
        value_col = _pick_fact_value_col(df, metric)
        latest_value = _safe_float(latest[value_col]) if value_col else None

        prior_value: Optional[float] = None
        delta: Optional[float] = None
        if len(df) >= 2 and value_col:
            prior_value = _safe_float(df.iloc[-2][value_col])
            if latest_value is not None and prior_value is not None:
                delta = latest_value - prior_value

        facts = {
            "latest_date": latest_date,
            "latest_value": latest_value,
            "prior_value": prior_value,
            "delta": delta,
            "n_points": int(len(df)),
            "columns": list(df.columns),
            "value_column": value_col,
        }

    # 2) Prefer deterministic narration for routine latest/delta summaries.
    structured_response = _build_structured_answer(
        metric=metric,
        query=query,
        df=df,
        facts=facts,
        mode=mode,
        source_label=src.label,
        source_date=source_date,
    )
    seasonal_summary: Optional[dict[str, Any]] = None
    if metric == "ng_electricity" and should_compute_ng_electricity_seasonal_norm(query):
        normal_years = int((((result.meta or {}).get("filters") or {}).get("normal_years") or 5))
        seasonal_summary = _ng_electricity_seasonal_norm_summary(
            df=df,
            normal_years=normal_years,
        )

    if use_llm_narration:
        (
            report_context_text,
            report_context_sources,
            report_context_used,
            report_context_reason,
        ) = (
            _build_report_rag_context(query)
        )
        report_context_block = report_context_text or "Report Context:\nNone retrieved."
        user_text = (
            f"User query: {query}\n\n"
            f"Metric: {metric}\n\n"
            f"Structured Facts:\n{json.dumps(facts, ensure_ascii=False)}\n\n"
            f"{report_context_block}\n\n"
            f"Valid built-in alert signal IDs:\n{_suggested_alert_catalog_text()}\n\n"
            "Instructions:\n"
            "- Use Structured Facts as the source of truth for current numbers.\n"
            "- Use Report Context only for narrative explanation and background.\n"
            "- If report context is missing, answer from Structured Facts only.\n"
            "- Suggest 1 to 3 alerts only if future monitoring would help with a decision.\n"
            "- Good alert themes include storage, production, HDD/weather, and supply-demand tightening.\n"
            "- Use only the listed built-in signal IDs for suggested_alerts.\n"
            "- If no useful built-in alert applies, return suggested_alerts as an empty array.\n"
        )

        resp = client.responses.create(
            model=model,
            instructions=SYSTEM_INSTRUCTIONS,
            input=user_text,
        )

        llm_payload = _extract_json_object(resp.output_text or "")
        if llm_payload is not None:
            structured_response = _normalize_structured_response(
                llm_payload, metric=metric, query=query
            )
            structured_response = _improve_no_context_language(
                structured_response=structured_response,
                metric=metric,
                facts=facts,
                report_context_used=report_context_used,
            )
        answer_text = structured_response.answer or structured_response.summary
    else:
        answer_text = (
            str(seasonal_summary["text"])
            if seasonal_summary is not None
            else _deterministic_answer_text(
                metric=metric,
                query=query,
                facts=facts,
                mode=mode,
                df=df,
            )
        )

    if seasonal_summary is not None and structured_response is not None:
        delta_vs_normal = float(seasonal_summary["delta_vs_normal"])
        normal_value = float(seasonal_summary["normal_value"])
        latest_value = float(seasonal_summary["latest_value"])
        direction = "above" if delta_vs_normal > 0 else "below" if delta_vs_normal < 0 else "in line with"
        samples = int(seasonal_summary["samples"])
        normal_years = int(seasonal_summary["normal_years"])
        drivers = [
            f"Seasonal baseline uses same-calendar-month history over {normal_years} years ({samples} historical observations).",
            f"Latest power burn: {_format_number(latest_value)} MMcf vs seasonal norm {_format_number(normal_value)} MMcf.",
            f"Current level is {direction} normal by {_format_number(abs(delta_vs_normal))} MMcf.",
        ]
        structured_response = structured_response.model_copy(
            update={
                "answer": answer_text,
                "summary": answer_text,
                "drivers": drivers,
                "data_points": [
                    AnswerDataPoint(metric="Natural Gas Power Burn", value=_json_safe(latest_value), unit="MMcf"),
                    AnswerDataPoint(metric="Seasonal Norm", value=_json_safe(normal_value), unit="MMcf"),
                    AnswerDataPoint(metric="Difference vs Norm", value=_json_safe(delta_vs_normal), unit="MMcf"),
                ],
            }
        )

    if proxy_for_metric == "iso_gas_dependency":
        percent_like_query = any(
            token in (query or "").lower()
            for token in ("percentage", "percent", "share", "%")
        )
        proxy_prefix = (
            "Direct ISO fuel-mix share data was unavailable, so this uses natural gas power-burn trend as a proxy. "
            "An exact electricity-generation percentage from gas could not be computed from available sources."
            if percent_like_query
            else "Direct ISO fuel-mix share data was unavailable, so this uses natural gas power-burn trend as a proxy."
        )
        answer_text = f"{proxy_prefix} {answer_text}".strip()
        if structured_response is not None:
            proxy_driver = proxy_note or "Proxy mode: answered from EIA natural gas power-burn series."
            updated_drivers = [proxy_driver] + list(structured_response.drivers or [])
            structured_response = structured_response.model_copy(
                update={"answer": answer_text, "summary": answer_text, "drivers": updated_drivers}
            )

    # 3) Assemble AnswerPayload
    if df is not None and "date" in df.columns:
        df = df.sort_values("date").reset_index(drop=True)
    chart_df = df
    chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
    if _is_same_time_five_year_comparison_query(query):
        baseline_mode = "median" if any(t in (query or "").lower() for t in ("median", "medium")) else "average"
        comparison_chart_df = _series_with_five_year_average_line(
            df, baseline_mode=baseline_mode
        ) if df is not None else pd.DataFrame()
        if not comparison_chart_df.empty:
            chart_df = comparison_chart_df
            baseline_label = "Median" if baseline_mode == "median" else "Average"
            chart_spec = ChartSpec(
                chart_type="line",
                title="Period Comparison with Same-Time 5-Year Baseline",
                x="date",
                y=["value", "five_year_baseline"],
                x_label="Date",
                y_label=METRIC_UNITS.get(metric, "Value"),
                notes=f"Horizontal line represents the same-time 5-year {baseline_label.lower()} benchmark.",
            )
    payload = AnswerPayload(
        query=query,
        mode=mode,
        answer_text=answer_text,
        structured_response=structured_response,
        report_context_used=report_context_used,
        report_context_reason=report_context_reason,
        report_context_sources=report_context_sources,
        data_preview=_maybe_data_preview(chart_df),
        chart_data_preview=_make_chart_preview(chart_df),
        chart_spec=chart_spec,
        sources=[src],
        warnings=None,
        generated_at=datetime.utcnow(),
    )

    return payload
