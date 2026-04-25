from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class AgentPolicy:
    answer_model: str = "gpt-5.2"
    enable_forecast: bool = True
    default_forecast_horizon_days: int = 7
    max_forecast_horizon_days: int = 14
    disable_forecast_metrics: list[str] = field(default_factory=list)
    force_forecast_metrics: list[str] = field(default_factory=list)


def load_agent_policy(path: str | Path | None) -> AgentPolicy:
    if path is None:
        return AgentPolicy()

    candidate = Path(path)
    if not candidate.exists() or not candidate.is_file():
        return AgentPolicy()

    try:
        raw = json.loads(candidate.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return AgentPolicy()
    if not isinstance(raw, dict):
        return AgentPolicy()

    def _to_bool(v: Any, default: bool) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            t = v.strip().lower()
            if t in {"1", "true", "yes", "on"}:
                return True
            if t in {"0", "false", "no", "off"}:
                return False
        return default

    def _to_int(v: Any, default: int) -> int:
        try:
            parsed = int(v)
            return parsed if parsed > 0 else default
        except (TypeError, ValueError):
            return default

    def _to_str_list(v: Any) -> list[str]:
        if not isinstance(v, list):
            return []
        out: list[str] = []
        for item in v:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return out

    return AgentPolicy(
        answer_model=str(raw.get("answer_model") or "gpt-5.2").strip() or "gpt-5.2",
        enable_forecast=_to_bool(raw.get("enable_forecast"), True),
        default_forecast_horizon_days=_to_int(raw.get("default_forecast_horizon_days"), 7),
        max_forecast_horizon_days=_to_int(raw.get("max_forecast_horizon_days"), 14),
        disable_forecast_metrics=_to_str_list(raw.get("disable_forecast_metrics")),
        force_forecast_metrics=_to_str_list(raw.get("force_forecast_metrics")),
    )
