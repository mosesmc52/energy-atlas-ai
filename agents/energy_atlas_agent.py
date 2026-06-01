from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from time import perf_counter
from typing import Callable, Optional

from agents.agent_policy import AgentPolicy, load_agent_policy
from agents.metric_capabilities import get_metric_capability
from agents.router import EnergyRouteResult, route_query
from agents.source_planner import SourceCall, SourcePlan
from executer import ExecuteRequest, MetricExecutor, MetricResult
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
        route_fn: Callable[[str], EnergyRouteResult] = route_query,
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

    def _route_to_source_plan(self, route: EnergyRouteResult) -> SourcePlan:
        calls: list[SourceCall] = []
        metrics = list(route.metrics or [])
        if route.primary_metric and route.primary_metric not in metrics:
            metrics.insert(0, route.primary_metric)
        for metric in metrics:
            calls.append(
                SourceCall(
                    adapter="route",
                    metric=metric,
                    filters=dict(route.filters or {}),
                    calculation=None,
                    start_date=route.start_date,
                    end_date=route.end_date,
                )
            )
        return SourcePlan(
            intent=route.analysis_type,
            calls=calls,
            comparison=None if route.comparisons == ["none"] else ",".join(route.comparisons),
            time_window=route.analysis_type,
            requires_multiple_sources=len(calls) > 1,
            ambiguous=route.ambiguous,
            reason=route.reason,
        )

    def _execute_with_fallback(self, *, route: EnergyRouteResult) -> MetricResult:
        req = ExecuteRequest(
            metric=route.primary_metric or "",
            start=route.start_date or "",
            end=route.end_date or "",
            filters=route.filters,
        )
        result = self.executor.execute(req)

        metric = str(route.primary_metric or "")
        capability = get_metric_capability(metric)
        if (
            capability.fallback_metric
            and (result.df is None or len(result.df) == 0)
        ):
            proxy_req = ExecuteRequest(
                metric=capability.fallback_metric,
                start=route.start_date or "",
                end=route.end_date or "",
                filters={},
            )
            proxy_result = self.executor.execute(proxy_req)
            if proxy_result.meta is None:
                proxy_result.meta = {}
            proxy_result.meta.update(
                {
                    "proxy_for_metric": metric,
                    "proxy_note": capability.fallback_note
                    or f"Primary metric '{metric}' unavailable; using '{capability.fallback_metric}' as proxy.",
                }
            )
            return proxy_result

        return result

    def run(
        self,
        *,
        user_query: str,
        forecaster=None,
    ) -> AgentOutcome:
        route_started = perf_counter()
        route = self._route_fn(user_query)
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

        if route.domain == "unsupported" or route.analysis_type == "unsupported" or route.primary_metric is None:
            return AgentOutcome(
                route=route,
                result=None,
                payload=None,
                forecast=None,
                timings=AgentTimings(route_ms=route_ms, execute_ms=0.0, answer_ms=0.0),
            )

        execute_started = perf_counter()
        result: MetricResult
        used_plan_execution = False
        try:
            plan = self._route_to_source_plan(route)
            if DEBUG_ENABLED:
                logger.info(
                    "agent_run plan intent=%s calls=%s multi=%s ambiguous=%s",
                    plan.intent,
                    [c.metric for c in plan.calls],
                    plan.requires_multiple_sources,
                    plan.ambiguous,
                )
            plan_results = self.executor.execute_plan(
                plan,
                start=route.start_date or "",
                end=route.end_date or "",
            )
            if isinstance(plan_results, dict):
                route_metric = str(route.primary_metric or "")
                if route_metric and route_metric in plan_results:
                    result = plan_results[route_metric]
                    used_plan_execution = True
                elif plan.calls and plan.calls[0].metric in plan_results:
                    result = plan_results[plan.calls[0].metric]
                    used_plan_execution = True
                else:
                    result = self._execute_with_fallback(route=route)
            else:
                result = self._execute_with_fallback(route=route)
        except Exception as exc:
            if DEBUG_ENABLED:
                logger.exception("agent_run plan execution failed; falling back: %s", exc)
            result = self._execute_with_fallback(route=route)
        execute_ms = (perf_counter() - execute_started) * 1000
        if DEBUG_ENABLED:
            df_obj = getattr(result, "df", None)
            try:
                row_count = 0 if df_obj is None else len(df_obj)
            except Exception:
                row_count = 0
            logger.info(
                "agent_run execute done mode=%s rows=%s metric=%s",
                "source_plan" if used_plan_execution else "fallback",
                row_count,
                str((result.meta or {}).get("metric") or route.primary_metric or ""),
            )
        if used_plan_execution and result.meta is not None:
            result.meta["execution_mode"] = "source_plan"

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
