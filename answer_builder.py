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
from openai import OpenAI
from schemas.answer import (
    AnswerAlert,
    AnswerDataPoint,
    AnswerForecast,
    AnswerPayload,
    AnswerSourceSummary,
    DataPreview,
    SignalSummary,
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
    "Required keys: answer, signal, summary, drivers, data_points, forecast, alerts, sources.\n"
    "signal.status must be bullish, bearish, or neutral.\n"
    "signal.confidence must be a float between 0 and 1.\n"
    "drivers, data_points, alerts, and sources must be arrays.\n"
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


def _normalize_structured_response(payload: dict[str, Any]) -> StructuredAnswer:
    signal = payload.get("signal") or {}
    forecast = payload.get("forecast") or {}
    return StructuredAnswer(
        answer=str(payload.get("answer") or "").strip(),
        signal=SignalSummary(
            status=str(signal.get("status") or "neutral").lower(),
            confidence=max(0.0, min(1.0, float(signal.get("confidence") or 0.5))),
        ),
        summary=str(payload.get("summary") or "").strip(),
        drivers=[
            str(driver).strip()
            for driver in (payload.get("drivers") or [])
            if str(driver).strip()
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
        alerts=[AnswerAlert(name=f"{leader_label.capitalize()} Sector Leads Consumption", status=True)],
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
    source_date = src.retrieved_at.date().isoformat() if src.retrieved_at else None
    report_context_text = ""
    report_context_sources: list[AnswerSourceSummary] = []
    report_context_used = False
    report_context_reason = "llm_narration_disabled"

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
    use_llm_narration = (
        os.getenv("ATLAS_USE_LLM_NARRATION", "").strip().lower()
        in {"1", "true", "yes", "on"}
    )

    structured_response = _build_structured_answer(
        metric=metric,
        query=query,
        facts=facts,
        mode=mode,
        source_label=src.label,
        source_date=source_date,
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
            "Instructions:\n"
            "- Use Structured Facts as the source of truth for current numbers.\n"
            "- Use Report Context only for narrative explanation and background.\n"
            "- If report context is missing, answer from Structured Facts only.\n"
        )

        resp = client.responses.create(
            model=model,
            instructions=SYSTEM_INSTRUCTIONS,
            input=user_text,
        )

        llm_payload = _extract_json_object(resp.output_text or "")
        if llm_payload is not None:
            structured_response = _normalize_structured_response(llm_payload)
        answer_text = structured_response.answer or structured_response.summary
    else:
        answer_text = _deterministic_answer_text(
            metric=metric,
            query=query,
            facts=facts,
            mode=mode,
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
