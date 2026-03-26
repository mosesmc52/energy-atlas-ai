# apps/chainlit/app.py
from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import sys
import tempfile
from time import perf_counter

cwd = pathlib.Path.cwd()
if cwd.name == "notebooks":
    proj_root = cwd.parent
else:
    proj_root = cwd  # if you launched from project root
if str(proj_root) not in sys.path:
    sys.path.insert(0, str(proj_root))
app_root = proj_root / "app"
if app_root.exists() and str(app_root) not in sys.path:
    sys.path.insert(0, str(app_root))


import chainlit as cl
from agents.router import route_query
from alerts.services import build_signal_evaluator, evaluation_as_json, parse_signal_question
from answer_builder import build_answer_with_openai
from charts.plotly_renderer import render_plotly
from executer import ExecuteRequest, MetricExecutor
from tools.eia_adapter import EIAAdapter
from tools.gridstatus_adapter import GridStatusAdapter
from utils.sheets_logger import GoogleSheetsQuestionLogger

logger = logging.getLogger(__name__)
DEBUG_ENABLED = os.getenv("ATLAS_DEBUG", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


async def _append_question_async(qlog, *, question: str, session_id: str) -> None:
    try:
        await asyncio.to_thread(
            qlog.append_question,
            question=question,
            session_id=session_id,
            tags=["energy-atlas-ai"],
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Sheets logging failed asynchronously: %s", e)


# -------------------------
# 1) Dependency wiring (startup)
# -------------------------


def build_container():
    cache_root = pathlib.Path(
        os.getenv(
            "ATLAS_CACHE_ROOT",
            str(pathlib.Path(tempfile.gettempdir()) / "energy-atlas-ai-cache"),
        )
    )
    eia_adapter = EIAAdapter(cache_dir=cache_root / "eia")
    grid_adapter = GridStatusAdapter(cache_dir=str(cache_root / "gridstatus"))
    executor = MetricExecutor(eia=eia_adapter, grid=grid_adapter)
    signal_evaluator = build_signal_evaluator()

    return {"executor": executor, "signal_evaluator": signal_evaluator}


@cl.set_starters
async def set_starters():
    return [
        cl.Starter(
            label="Price of henry hub",
            message="What is the current Henry Hub price?",
            icon="/public/icons/dollar.svg",
        ),
        cl.Starter(
            label="Production",
            message="Is production growing year over year?",
            icon="/public/icons/gas-plant.svg",
        ),
        cl.Starter(
            label="Electricity",
            message="How much natural gas did power plants use last month?",
            icon="/public/icons/electricity.svg",
        ),
        cl.Starter(
            label="Consumption",
            message="Which sector consumes the most gas (power, residential, industrial)?",
            icon="/public/icons/gas.svg",
        ),
        cl.Starter(
            label="Storage",
            message="How much gas is currently in storage?",
            icon="/public/icons/storage-tank.svg",
        ),
        cl.Starter(
            label="Exploration & Reserves",
            message="Are reserves increasing or decreasing?",
            icon="/public/icons/reserves.svg",
        ),
        cl.Starter(
            label="Import",
            message="Are imports rising or falling?",
            icon="/public/icons/tanker-import.svg",
        ),
        cl.Starter(
            label="Export",
            message="Are exports higher than last year?",
            icon="/public/icons/tanker-export.svg",
        ),
    ]


# Store dependencies in Chainlit session
@cl.on_chat_start
async def on_chat_start():
    # Core deps
    cl.user_session.set("deps", build_container())

    # Optional: Google Sheets question logging (best-effort)
    try:
        qlog = GoogleSheetsQuestionLogger(sheet_name="Questions")
        qlog.ensure_header()  # idempotent
        cl.user_session.set("qlog", qlog)
    except Exception as e:
        cl.user_session.set("qlog_error", str(e))
        print(f"[WARN] Sheets logger disabled: {e}")

    # Optional welcome message
    # await cl.Message(
    #     content="Energy Atlas AI (v0.1). Ask about natural gas storage, Henry Hub, or LNG exports."
    # ).send()


@cl.on_message
async def on_message(message: cl.Message):
    request_started = perf_counter() if DEBUG_ENABLED else 0.0
    deps = cl.user_session.get("deps") or {}
    executor: MetricExecutor = deps.get("executor")  # type: ignore[assignment]
    signal_evaluator = deps.get("signal_evaluator")

    user_query = (message.content or "").strip()
    if not user_query:
        await cl.Message(content="Please enter a question.").send()
        return

    parsed_signal = parse_signal_question(user_query)
    if parsed_signal is not None and signal_evaluator is not None:
        evaluation = signal_evaluator.evaluate(parsed_signal)
        await cl.Message(
            content=f"```json\n{evaluation_as_json(evaluation)}\n```"
        ).send()
        return

    # Best-effort question logging
    qlog_started = perf_counter() if DEBUG_ENABLED else 0.0
    qlog = cl.user_session.get("qlog")
    if qlog is not None:
        asyncio.create_task(
            _append_question_async(
                qlog,
                question=user_query,
                session_id=str(cl.user_session.get("id", "")),
            )
        )
    else:
        err = cl.user_session.get("qlog_error")
        if err:
            print(f"[INFO] Sheets disabled (init error): {err}")
    qlog_elapsed_ms = (perf_counter() - qlog_started) * 1000 if DEBUG_ENABLED else 0.0

    try:
        # (A) Route the query -> metric + params
        route_started = perf_counter() if DEBUG_ENABLED else 0.0
        route = route_query(user_query)
        route_elapsed_ms = (
            (perf_counter() - route_started) * 1000 if DEBUG_ENABLED else 0.0
        )
        if route.intent == "ambiguous":
            await cl.Message(
                content=(
                    "That question is ambiguous. Try naming the metric explicitly, "
                    "for example: 'Is Lower 48 dry gas production growing year over year?'"
                )
            ).send()
            return
        if route.intent == "unsupported" or route.primary_metric is None:
            fallback_reason = (
                route.reason
                or "This question did not map cleanly to a supported metric."
            )
            await cl.Message(
                content=(
                    "I couldn't map that question to a supported metric yet. "
                    f"{fallback_reason}"
                )
            ).send()
            return
        if route.primary_metric is None:
            raise ValueError(f"Unable to determine a metric for intent: {route.intent}")

        # (B) Execute -> fetch data (df + SourceRef)
        req = ExecuteRequest(
            metric=route.primary_metric,
            start=route.start,
            end=route.end,
            filters=route.filters,
        )
        execute_started = perf_counter() if DEBUG_ENABLED else 0.0
        result = executor.execute(req)
        execute_elapsed_ms = (
            (perf_counter() - execute_started) * 1000 if DEBUG_ENABLED else 0.0
        )
        cache_meta = ((result.meta or {}).get("cache") or {}) if result.meta else {}
        cache_timings = cache_meta.get("timings_ms") or {}
        fetched_segments = cache_meta.get("fetched_segments") or []
        background_refresh_scheduled = bool(
            cache_meta.get("background_refresh_scheduled", False)
        )

        # (C) Build AnswerPayload (OpenAI writes narrative; you keep facts/sources)
        answer_started = perf_counter() if DEBUG_ENABLED else 0.0
        payload = build_answer_with_openai(
            query=user_query,
            result=result,
            mode="observed",
            model=os.getenv("OPENAI_MODEL", "gpt-5.2"),
        )
        answer_elapsed_ms = (
            (perf_counter() - answer_started) * 1000 if DEBUG_ENABLED else 0.0
        )

        message_started = perf_counter() if DEBUG_ENABLED else 0.0
        msg = await cl.Message(content=payload.answer_text).send()
        message_elapsed_ms = (
            (perf_counter() - message_started) * 1000 if DEBUG_ENABLED else 0.0
        )

        chart_elapsed_ms = 0.0
        if payload.chart_spec is not None:
            chart_started = perf_counter() if DEBUG_ENABLED else 0.0
            fig = render_plotly(payload.chart_spec, result.df)

            await cl.Plotly(name=payload.chart_spec.title, figure=fig).send(
                for_id=msg.id
            )
            if DEBUG_ENABLED:
                chart_elapsed_ms = (perf_counter() - chart_started) * 1000

        # sources
        sources_elapsed_ms = 0.0
        if payload.sources:
            sources_started = perf_counter() if DEBUG_ENABLED else 0.0
            lines = [f"• {s.label}" for s in payload.sources]

            await cl.Message(content="**Sources**\n" + "\n".join(lines)).send()
            if DEBUG_ENABLED:
                sources_elapsed_ms = (perf_counter() - sources_started) * 1000

        if DEBUG_ENABLED:
            total_elapsed_ms = (perf_counter() - request_started) * 1000
            logger.info(
                "request_timing total=%.1fms qlog=%.1fms route=%.1fms execute=%.1fms cache_load=%.1fms cache_missing=%.1fms cache_fetch=%.1fms cache_normalize=%.1fms cache_merge=%.1fms cache_save=%.1fms cache_slice=%.1fms cache_background=%s cache_segments=%s answer=%.1fms message=%.1fms chart=%.1fms sources=%.1fms metric=%s route_source=%s query=%r",
                total_elapsed_ms,
                qlog_elapsed_ms,
                route_elapsed_ms,
                execute_elapsed_ms,
                float(cache_timings.get("load", 0.0)),
                float(cache_timings.get("missing", 0.0)),
                float(cache_timings.get("fetch", 0.0)),
                float(cache_timings.get("normalize", 0.0)),
                float(cache_timings.get("merge", 0.0)),
                float(cache_timings.get("save", 0.0)),
                float(cache_timings.get("slice", 0.0)),
                background_refresh_scheduled,
                fetched_segments,
                answer_elapsed_ms,
                message_elapsed_ms,
                chart_elapsed_ms,
                sources_elapsed_ms,
                route.primary_metric,
                route.source,
                user_query,
            )

    except Exception as e:
        if DEBUG_ENABLED:
            total_elapsed_ms = (perf_counter() - request_started) * 1000
            logger.exception(
                "request_failed total=%.1fms qlog=%.1fms query=%r",
                total_elapsed_ms,
                qlog_elapsed_ms,
                user_query,
            )

        # Keep errors visible during development
        await cl.Message(content=f"Error: {e}").send()
