from __future__ import annotations

from dataclasses import dataclass
import os
from time import perf_counter
from typing import Callable, Optional

from agents.agent_policy import AgentPolicy, load_agent_policy
from agents.llm_query_parser import llm_parse_query
from agents.metric_capabilities import get_metric_capability
from agents.router import HybridRouteResult, route_query
from agents.source_planner import build_source_plan
from executer import ExecuteRequest, MetricExecutor, MetricResult
from schemas.answer import AnswerPayload


@dataclass(frozen=True)
class AgentTimings:
    route_ms: float
    execute_ms: float
    answer_ms: float


@dataclass(frozen=True)
class AgentOutcome:
    route: HybridRouteResult
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
        route_fn: Callable[[str], HybridRouteResult] = route_query,
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

    def _execute_with_fallback(self, *, route: HybridRouteResult) -> MetricResult:
        req = ExecuteRequest(
            metric=route.primary_metric or "",
            start=route.start,
            end=route.end,
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
                start=route.start,
                end=route.end,
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

        if route.intent in {"ambiguous", "unsupported"} or route.primary_metric is None:
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
            parsed = llm_parse_query(
                user_query=user_query,
                normalized_query=(route.normalized_query or user_query).strip().lower(),
            )
            plan = build_source_plan(parsed)
            plan_results = self.executor.execute_plan(plan, start=route.start, end=route.end)
            route_metric = str(route.primary_metric or "")
            if route_metric and route_metric in plan_results:
                result = plan_results[route_metric]
                used_plan_execution = True
            elif plan.calls:
                result = plan_results[plan.calls[0].metric]
                used_plan_execution = True
            else:
                result = self._execute_with_fallback(route=route)
        except Exception:
            result = self._execute_with_fallback(route=route)
        execute_ms = (perf_counter() - execute_started) * 1000
        if used_plan_execution and result.meta is not None:
            result.meta["execution_mode"] = "source_plan"

        forecast = None
        metric = str((result.meta or {}).get("metric") or route.primary_metric or "")
        forecast_requested = bool(route.include_forecast) or (
            metric in self.policy.force_forecast_metrics
        )
        forecast_allowed = (
            self.policy.enable_forecast and metric not in self.policy.disable_forecast_metrics
        )
        if forecast_requested and forecast_allowed and forecaster is not None:
            requested_horizon = route.forecast_horizon_days or self.policy.default_forecast_horizon_days
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
