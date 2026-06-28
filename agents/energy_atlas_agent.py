from __future__ import annotations

from dataclasses import dataclass, replace
import logging
import os
from time import perf_counter
from typing import Callable, Optional

from agents.agent_policy import AgentPolicy, load_agent_policy
from agents.router import EnergyRouteResult, RouteContext, context_from_route, route_query
from executer import MetricExecutor, MetricResult
from schemas.answer import AnswerPayload

logger = logging.getLogger(__name__)
DEBUG_ENABLED = os.getenv("ATLAS_DEBUG", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


@dataclass(frozen=True)
class AgentTimings:
    route_ms: float
    execute_ms: float
    answer_ms: float


@dataclass(frozen=True)
class AgentOutcome:
    route: EnergyRouteResult
    result: Optional[MetricResult]
    payload: Optional[AnswerPayload]
    forecast: Optional[object]
    timings: AgentTimings


class EnergyAtlasAgent:
    def __init__(
        self,
        *,
        executor: MetricExecutor,
        model: Optional[str] = None,
        route_fn: Callable[..., EnergyRouteResult] = route_query,
        answer_builder_fn: Optional[Callable[..., AnswerPayload]] = None,
        policy: Optional[AgentPolicy] = None,
        policy_path: Optional[str] = None,
    ):
        if answer_builder_fn is None:
            from answer_builder import build_answer_with_openai

            answer_builder_fn = build_answer_with_openai
        resolved_policy = policy or load_agent_policy(
            policy_path
            or os.getenv("ATLAS_AGENT_POLICY_PATH")
            or "config/agent_policy.json"
        )
        self.executor = executor
        self.policy = resolved_policy
        self.model = model or resolved_policy.answer_model
        self._route_fn = route_fn
        self._answer_builder_fn = answer_builder_fn
        self._last_route_context: RouteContext | None = None

    def _execute_storage_route(self, route: EnergyRouteResult) -> MetricResult:
        filters = dict(route.filters or {})
        filters["regions"] = list(route.regions or filters.get("regions") or [])
        filters["states"] = list(route.states or filters.get("states") or [])
        filters["storage_dataset"] = route.storage_dataset
        filters["storage_frequency"] = route.storage_frequency
        filters["storage_metric_type"] = route.storage_metric_type
        filters["storage_type"] = route.storage_type
        filters["storage_types_all"] = route.storage_types_all
        filters["storage_insight_type"] = route.storage_insight_type
        prepared_route = replace(route, filters=filters)
        if DEBUG_ENABLED:
            logger.info(
                "agent_run storage route dataset=%s frequency=%s metric_type=%s regions=%s states=%s",
                route.storage_dataset,
                route.storage_frequency,
                route.storage_metric_type,
                list(prepared_route.regions or []),
                list(prepared_route.states or []),
            )
        return self.executor.execute_storage_route(prepared_route)

    def _unsupported_outcome(self, *, route: EnergyRouteResult, route_ms: float) -> AgentOutcome:
        return AgentOutcome(
            route=route,
            result=None,
            payload=None,
            forecast=None,
            timings=AgentTimings(route_ms=route_ms, execute_ms=0.0, answer_ms=0.0),
        )

    def run(
        self,
        *,
        user_query: str,
        previous_route_context: RouteContext | dict | None = None,
        forecaster=None,
    ) -> AgentOutcome:
        resolved_previous_context = previous_route_context
        if isinstance(previous_route_context, dict):
            resolved_previous_context = RouteContext(**previous_route_context)
        if resolved_previous_context is None:
            resolved_previous_context = self._last_route_context
        route_started = perf_counter()
        route = self._route_fn(
            user_query,
            previous_context=resolved_previous_context,
        )
        route_ms = (perf_counter() - route_started) * 1000
        if DEBUG_ENABLED:
            logger.info(
                "agent_run route domain=%s analysis=%s primary=%s start=%s end=%s",
                route.domain,
                route.analysis_type,
                route.primary_metric,
                route.start_date,
                route.end_date,
            )

        if route.domain == "unsupported" or route.analysis_type == "unsupported":
            return self._unsupported_outcome(route=route, route_ms=route_ms)
        if route.domain != "storage" or route.primary_metric is None:
            return self._unsupported_outcome(route=route, route_ms=route_ms)

        execute_started = perf_counter()
        result = self._execute_storage_route(route)
        execute_ms = (perf_counter() - execute_started) * 1000
        if DEBUG_ENABLED:
            df_obj = getattr(result, "df", None)
            try:
                row_count = 0 if df_obj is None else len(df_obj)
            except Exception:
                row_count = 0
            logger.info(
                "agent_run execute done domain=%s rows=%s metric=%s",
                route.domain,
                row_count,
                str((result.meta or {}).get("metric") or route.primary_metric or ""),
            )

        forecast = None
        metric = str((result.meta or {}).get("metric") or route.primary_metric or "")
        forecast_requested = metric in self.policy.force_forecast_metrics
        forecast_allowed = (
            self.policy.enable_forecast and metric not in self.policy.disable_forecast_metrics
        )
        if forecast_requested and forecast_allowed and forecaster is not None:
            requested_horizon = self.policy.default_forecast_horizon_days
            horizon_days = max(1, min(int(requested_horizon), int(self.policy.max_forecast_horizon_days)))
            forecast = forecaster.forecast_dataframe(
                result.df,
                metric=metric,
                horizon_days=horizon_days,
                include_overlay=True,
                source_reference=result.source.reference,
            )

        answer_started = perf_counter()
        payload = self._answer_builder_fn(
            query=user_query,
            result=result,
            route=route,
            mode="observed",
            model=self.model,
        )
        answer_ms = (perf_counter() - answer_started) * 1000
        if DEBUG_ENABLED:
            logger.info(
                "agent_run answer built structured=%s answer_len=%s",
                payload.structured_response is not None,
                len(str(payload.answer_text or "")),
            )
        if route.domain == "storage" and route.analysis_type != "unsupported" and route.primary_metric is not None:
            self._last_route_context = context_from_route(route)

        return AgentOutcome(
            route=route,
            result=result,
            payload=payload,
            forecast=forecast,
            timings=AgentTimings(
                route_ms=route_ms,
                execute_ms=execute_ms,
                answer_ms=answer_ms,
            ),
        )
