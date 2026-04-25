# atlas/answer_builder.py
# atlas/answer_builder.py (or wherever _make_preview lives)
from __future__ import annotations

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
from alerts.services import get_builtin_signal_registry, is_builtin_signal_id
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
    "des_business_activity_index": "index",
    "des_company_outlook_index": "index",
    "des_outlook_uncertainty_index": "index",
    "des_oil_production_index": "index",
    "des_gas_production_index": "index",
    "des_capex_index": "index",
    "des_employment_index": "index",
    "des_input_cost_index": "index",
    "des_finding_development_costs_index": "index",
    "des_lease_operating_expense_index": "index",
    "des_prices_received_services_index": "index",
    "des_equipment_utilization_index": "index",
    "des_operating_margin_index": "index",
    "des_wti_price_expectation_6m": "$/bbl",
    "des_wti_price_expectation_1y": "$/bbl",
    "des_wti_price_expectation_2y": "$/bbl",
    "des_wti_price_expectation_5y": "$/bbl",
    "des_hh_price_expectation_6m": "$/MMBtu",
    "des_hh_price_expectation_1y": "$/MMBtu",
    "des_hh_price_expectation_2y": "$/MMBtu",
    "des_hh_price_expectation_5y": "$/MMBtu",
    "des_breakeven_oil_us": "$/bbl",
    "des_breakeven_gas_us": "$/MMBtu",
    "des_breakeven_oil_permian": "$/bbl",
    "des_breakeven_oil_eagle_ford": "$/bbl",
    "managed_money_long": "contracts",
    "managed_money_short": "contracts",
    "managed_money_net": "contracts",
    "managed_money_net_percentile_156w": "percentile",
    "open_interest": "contracts",
    "weather_degree_days_forecast_vs_5y": "degree-days",
    "weather_regional_demand_drivers": "Bcf/d",
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
    chunks = load_report_chunks(str(report_chunks_path))
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


def _make_preview(df: pd.DataFrame, n: int = 10) -> DataPreview:
    tail = df.tail(n)

    # convert the tail to python objects (keeps Timestamps as pd.Timestamp)
    rows = tail.to_numpy(dtype=object).tolist()
    rows = [[_json_safe(v) for v in row] for row in rows]

    return DataPreview(
        columns=list(tail.columns),
        rows=rows,
        units=None,
    )


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


def _is_text_metric(metric: str, df: pd.DataFrame) -> bool:
    if metric.endswith("_text"):
        return True
    if "value" not in df.columns:
        return False
    return not pd.api.types.is_numeric_dtype(df["value"])


def _format_delta(delta: Optional[float], unit: Optional[str]) -> Optional[str]:
    if delta is None:
        return None
    direction = "up" if delta > 0 else "down" if delta < 0 else "unchanged"
    if direction == "unchanged":
        return "unchanged from the previous observation"
    unit_suffix = f" {unit}" if unit else ""
    return f"{direction} {_format_number(abs(delta))}{unit_suffix} from the previous observation"


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
    forecast = payload.get("forecast") or {}
    suggested_alerts = []
    for item in (payload.get("suggested_alerts") or []):
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
    return StructuredAnswer(
        answer=_coerce_text(payload.get("answer")),
        signal=SignalSummary(
            status=str(signal.get("status") or "neutral").lower(),
            confidence=max(0.0, min(1.0, float(signal.get("confidence") or 0.5))),
        ),
        summary=_coerce_text(payload.get("summary")),
        drivers=[
            _coerce_text(driver)
            for driver in (payload.get("drivers") or [])
            if _coerce_text(driver)
        ],
        data_points=[
            AnswerDataPoint(
                metric=str(item.get("metric") or "").strip(),
                value=item.get("value"),
                unit=str(item.get("unit") or "").strip(),
            )
            for item in (payload.get("data_points") or [])
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
            for item in (payload.get("alerts") or [])
            if isinstance(item, dict) and str(item.get("name") or "").strip()
        ],
        sources=[
            AnswerSourceSummary(
                title=str(item.get("title") or "").strip(),
                date=str(item.get("date") or "").strip() or None,
            )
            for item in (payload.get("sources") or [])
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


def _build_text_structured_answer(
    *,
    metric: str,
    df: pd.DataFrame,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    if df is None or df.empty:
        return StructuredAnswer(
            answer="No report text was returned for the requested period.",
            signal=SignalSummary(status="neutral", confidence=0.5),
            summary="No report text was returned for the requested period.",
            drivers=["The DES qualitative dataset returned no records."],
            data_points=[],
            forecast=AnswerForecast(direction="flat", reasoning="Text metrics do not provide a numeric forecast."),
            suggested_alerts=[],
            alerts=[AnswerAlert(name="No DES Text Returned", status=True)],
            sources=[AnswerSourceSummary(title=source_label, date=source_date)],
        )

    ordered = df.copy()
    if "date" in ordered.columns:
        ordered = ordered.sort_values("date")
    latest = ordered.iloc[-1]
    latest_date = pd.to_datetime(latest.get("date"), errors="coerce")
    latest_text = _coerce_text(latest.get("value"))
    snippet = latest_text[:280].strip()
    if len(latest_text) > 280:
        snippet = snippet + "..."
    metric_label = _titleize_metric(metric)
    date_label = latest_date.date().isoformat() if not pd.isna(latest_date) else source_date
    answer = f"{metric_label} for {date_label}: {snippet}" if snippet else f"No text content was available for {metric_label.lower()}."
    return StructuredAnswer(
        answer=answer,
        signal=SignalSummary(status="neutral", confidence=0.74),
        summary=answer,
        drivers=[
            f"Latest DES text record was captured for {date_label}." if date_label else "Latest DES text record was captured.",
            "Raw source text is preserved for downstream summarization.",
        ],
        data_points=[AnswerDataPoint(metric=metric_label, value=snippet, unit="text")] if snippet else [],
        forecast=AnswerForecast(direction="flat", reasoning="Text metrics are descriptive and do not imply a numeric forecast."),
        suggested_alerts=[],
        alerts=[],
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
        and {"date", "value", "weekly_change"}.issubset(df.columns)
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
    rank_withdrawals = "withdrawal" in (query or "").lower() or "fastest" in (query or "").lower()
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
    df: pd.DataFrame, *, region_label: str = "Selected region"
) -> str:
    if not _is_storage_level_and_change_view("working_gas_storage_lower48", df):
        return "No data was returned for the requested period."

    ordered = df.copy()
    ordered["date"] = pd.to_datetime(ordered["date"], errors="coerce")
    ordered["value"] = pd.to_numeric(ordered["value"], errors="coerce")
    ordered["weekly_change"] = pd.to_numeric(ordered["weekly_change"], errors="coerce")
    ordered = ordered.dropna(subset=["date", "value"]).sort_values("date")
    if ordered.empty:
        return "No data was returned for the requested period."

    latest = ordered.iloc[-1]
    latest_date = latest["date"].date().isoformat()
    storage_text = _format_number(float(latest["value"]))
    if pd.notna(latest["weekly_change"]):
        change_value = float(latest["weekly_change"])
        flow_word = "injection" if change_value > 0 else "withdrawal" if change_value < 0 else "change"
        change_text = _format_number(change_value)
        return (
            f"As of {latest_date}, {region_label} storage was {storage_text} Bcf and the latest weekly change "
            f"was {change_text} Bcf ({flow_word})."
        )
    return f"As of {latest_date}, {region_label} storage was {storage_text} Bcf."


def _storage_level_and_change_structured_answer(
    *,
    df: pd.DataFrame,
    region_label: str,
    source_label: str,
    source_date: Optional[str],
) -> StructuredAnswer:
    answer = _storage_level_and_change_answer(df, region_label=region_label)
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
    ordered["weekly_change"] = pd.to_numeric(ordered["weekly_change"], errors="coerce")
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
    *, metric: str, query: str, facts: dict[str, Any], mode: str
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

    if prior_value is None or delta_text is None:
        return f"As of {latest_date}, the latest value is {latest_value_text}{unit_suffix}."

    if mode == "observed":
        return (
            f"As of {latest_date}, the latest value is {latest_value_text}{unit_suffix}, "
            f"{delta_text}."
        )

    return (
        f"The latest observed value is {latest_value_text}{unit_suffix} on {latest_date}, "
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
    mode: str = "observed",
    model: str = "gpt-5.2",
) -> AnswerPayload:
    df = result.df
    src = result.source

    metric = result.meta.get("metric", "") if result.meta else ""
    proxy_for_metric = str((result.meta or {}).get("proxy_for_metric") or "")
    proxy_note = str((result.meta or {}).get("proxy_note") or "").strip()
    region_label = (
        str(((result.meta or {}).get("filters") or {}).get("region") or "Selected region")
        .replace("_", " ")
        .title()
    )
    source_date = src.retrieved_at.date().isoformat() if src.retrieved_at else None
    report_context_text = ""
    report_context_sources: list[AnswerSourceSummary] = []
    report_context_used = False
    report_context_reason = "llm_narration_disabled"
    use_llm_narration = (
        os.getenv("ATLAS_USE_LLM_NARRATION", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )
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
        answer_text = _deterministic_sector_consumption_answer(query=query, df=df)
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
            data_preview=_make_preview(df),
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
            data_preview=_make_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_storage_level_and_change_view(metric, df) and not prefer_report_narration:
        answer_text = _storage_level_and_change_answer(df, region_label=region_label)
        if df is not None and "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=answer_text,
            structured_response=_storage_level_and_change_structured_answer(
                df=df,
                region_label=region_label,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="storage_level_and_change_no_rag",
            report_context_sources=[],
            data_preview=_make_preview(df),
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
            data_preview=_make_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if _is_weather_regional_demand_drivers_view(metric, df):
        answer_text = _weather_regional_demand_drivers_answer(df)
        chart_spec = None
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
            data_preview=_make_preview(df),
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
            data_preview=_make_preview(df),
            chart_spec=chart_spec,
            sources=[src],
        )
        return payload

    if df is not None and not df.empty and _is_text_metric(metric, df):
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        payload = AnswerPayload(
            query=query,
            mode=mode,
            answer_text=_build_text_structured_answer(
                metric=metric,
                df=df,
                source_label=src.label,
                source_date=source_date,
            ).answer,
            structured_response=_build_text_structured_answer(
                metric=metric,
                df=df,
                source_label=src.label,
                source_date=source_date,
            ),
            report_context_used=False,
            report_context_reason="des_text_metric",
            report_context_sources=[],
            data_preview=_make_preview(df),
            chart_spec=None,
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
    chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
    payload = AnswerPayload(
        query=query,
        mode=mode,
        answer_text=answer_text,
        structured_response=structured_response,
        report_context_used=report_context_used,
        report_context_reason=report_context_reason,
        report_context_sources=report_context_sources,
        data_preview=_make_preview(df) if df is not None else None,
        chart_spec=chart_spec,
        sources=[src],
        warnings=None,
        generated_at=datetime.utcnow(),
    )

    return payload
