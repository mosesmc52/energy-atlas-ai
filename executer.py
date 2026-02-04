from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from tools.eia_adapter import EIAAdapter, EIAResult


@dataclass(frozen=True)
class ExecuteRequest:
    """
    The normalized request produced by the router (or later, by an agent).
    """

    metric: str
    start: str
    end: str
    filters: Dict[str, Any] | None = None


# assumes:
# - ExecuteRequest has: metric: str, start: str, end: str, filters: dict|None
# - EIAResult has: df, source, meta (dict or None)
# - EIAAdapter methods accept (start, end, **filters) or (start, end, filters=...)


class MetricExecutor:
    """
    Deterministic dispatcher: metric -> implementation.
    """

    def __init__(self, *, eia: EIAAdapter):
        self.eia = eia

        self._metric_to_handler = {
            "working_gas_storage_lower48": self._eia_storage_lower48,
            "henry_hub_spot": self._eia_henry_hub_spot,
            "lng_exports": self._eia_lng_exports,
        }

    def execute(self, req: ExecuteRequest) -> EIAResult:
        if req.metric not in self._metric_to_handler:
            raise ValueError(f"Unsupported metric: {req.metric}")

        handler = self._metric_to_handler[req.metric]

        # ---- execute adapter handler ----
        result = handler(start=req.start, end=req.end, filters=req.filters or {})

        # ---- attach execution context (KEY FIX) ----
        if result.meta is None:
            result.meta = {}

        result.meta.update(
            {
                "metric": req.metric,
                "start": req.start,
                "end": req.end,
                "filters": req.filters or {},
                "executed_at": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            }
        )

        return result

    # -----------------------
    # Metric handlers (EIA v0.1)
    # -----------------------

    def _eia_storage_lower48(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        # filters reserved for future facets (region variants, etc.)
        return self.eia.storage_working_gas_lower48(start=start, end=end)

    def _eia_henry_hub_spot(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.henry_hub_spot(start=start, end=end)

    def _eia_lng_exports(
        self, *, start: str, end: str, filters: Dict[str, Any]
    ) -> EIAResult:
        return self.eia.lng_exports(start=start, end=end)
