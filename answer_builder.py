# atlas/answer_builder.py
# atlas/answer_builder.py (or wherever _make_preview lives)
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

import numpy as np
import pandas as pd
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


def build_answer_with_openai(
    *,
    query: str,
    result: EIAResult,
    mode: str = "observed",
    model: str = "gpt-5.2",
) -> AnswerPayload:
    df = result.df
    src = result.source

    # 1) Deterministic facts
    if df is None or len(df) == 0:
        facts = {
            "latest_date": None,
            "latest_value": None,
            "prior_value": None,
            "delta": None,
            "n_points": 0,
            "columns": list(df.columns) if df is not None else [],
        }
    else:
        latest = df.iloc[-1]
        latest_date = pd.to_datetime(latest["date"]).date().isoformat()
        latest_value = _safe_float(latest["value"])

        prior_value: Optional[float] = None
        delta: Optional[float] = None
        if len(df) >= 2:
            prior_value = _safe_float(df.iloc[-2]["value"])
            if latest_value is not None and prior_value is not None:
                delta = latest_value - prior_value

        facts = {
            "latest_date": latest_date,
            "latest_value": latest_value,
            "prior_value": prior_value,
            "delta": delta,
            "n_points": int(len(df)),
            "columns": list(df.columns),
        }

    # 2) OpenAI narration (text only)
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

    # 3) Assemble AnswerPayload
    payload = AnswerPayload(
        query=query,
        mode=mode,
        answer_text=answer_text,
        data_preview=_make_preview(df) if df is not None else None,
        chart_spec=None,
        sources=[src],
        warnings=None,
        generated_at=datetime.utcnow(),
    )

    return payload
