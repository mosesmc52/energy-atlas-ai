# atlas/answer_builder.py
# atlas/answer_builder.py (or wherever _make_preview lives)
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Optional

import numpy as np
import pandas as pd
from answers.chart_policy import chart_policy
from openai import OpenAI
from schemas.answer import AnswerPayload, DataPreview
from tools.eia_adapter import EIAResult

SYSTEM_INSTRUCTIONS = (
    "You are Energy Atlas AI. Write concise, analyst-grade answers.\n"
    "Use ONLY the provided facts JSON. Do not invent numbers.\n"
    "If delta is present, mention it. If not, just state the latest value.\n"
    "Keep it to 2–4 sentences."
)

client = OpenAI()  # expects OPENAI_API_KEY in env

METRIC_UNITS = {
    "henry_hub_spot": "$/MMBtu",
    "working_gas_storage_lower48": "Bcf",
    "working_gas_storage_change_weekly": "Bcf",
    "lng_exports": "MMcf",
    "lng_imports": "MMcf",
    "ng_electricity": "MMcf",
    "ng_consumption_lower48": "MMcf",
    "ng_production_lower48": "MMcf",
}


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

    if use_llm_narration:
        user_text = (
            f"User query: {query}\n\n"
            f"Facts JSON:\n{json.dumps(facts, ensure_ascii=False)}"
        )

        resp = client.responses.create(
            model=model,
            instructions=SYSTEM_INSTRUCTIONS,
            input=user_text,
        )

        answer_text = (resp.output_text or "").strip()
    else:
        answer_text = _deterministic_answer_text(
            metric=metric,
            query=query,
            facts=facts,
            mode=mode,
        )

    # 3) Assemble AnswerPayload
    df = df.sort_values("date").reset_index(drop=True)
    chart_spec = chart_policy(metric=metric, mode=mode, df=df, query=query)
    payload = AnswerPayload(
        query=query,
        mode=mode,
        answer_text=answer_text,
        data_preview=_make_preview(df) if df is not None else None,
        chart_spec=chart_spec,
        sources=[src],
        warnings=None,
        generated_at=datetime.utcnow(),
    )

    return payload
