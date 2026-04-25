from __future__ import annotations

from dataclasses import dataclass
import os
from time import perf_counter
from typing import Callable, Optional

from agents.agent_policy import AgentPolicy, load_agent_policy
from agents.router import HybridRouteResult, route_query
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
        req = ExecuteRequest(
            metric=route.primary_metric,
            start=route.start,
            end=route.end,
            filters=route.filters,
        )
        result = self.executor.execute(req)

        # Fallback: if ISO gas-share data is unavailable, use EIA power-burn trend as proxy.
        if route.primary_metric == "iso_gas_dependency" and (result.df is None or len(result.df) == 0):
            proxy_req = ExecuteRequest(
                metric="ng_electricity",
                start=route.start,
                end=route.end,
                filters={},
            )
            proxy_result = self.executor.execute(proxy_req)
            if proxy_result.meta is None:
                proxy_result.meta = {}
            proxy_result.meta.update(
                {
                    "proxy_for_metric": "iso_gas_dependency",
                    "proxy_note": "Direct ISO gas-share data unavailable; using natural gas power-burn trend as proxy.",
                }
            )
            result = proxy_result
        execute_ms = (perf_counter() - execute_started) * 1000

        forecast = None
        metric = str(route.primary_metric or "")
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
                metric=route.primary_metric,
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
