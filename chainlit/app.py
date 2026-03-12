# apps/chainlit/app.py
from __future__ import annotations

import os
import pathlib
import sys

cwd = pathlib.Path.cwd()
if cwd.name == "notebooks":
    proj_root = cwd.parent
else:
    proj_root = cwd  # if you launched from project root
if str(proj_root) not in sys.path:
    sys.path.insert(0, str(proj_root))


import chainlit as cl
from agents.router import route_query
from answer_builder import build_answer_with_openai
from charts.plotly_renderer import render_plotly
from executer import ExecuteRequest, MetricExecutor
from tools.eia_adapter import EIAAdapter
from tools.gridstatus_adapter import GridStatusAdapter
from utils.sheets_logger import GoogleSheetsQuestionLogger

# -------------------------
# 1) Dependency wiring (startup)
# -------------------------


def build_container():
    eia_adapter = EIAAdapter()
    grid_adapter = GridStatusAdapter()
    executor = MetricExecutor(eia=eia_adapter, grid=grid_adapter)

    return {"executor": executor}


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
    deps = cl.user_session.get("deps") or {}
    executor: MetricExecutor = deps.get("executor")  # type: ignore[assignment]

    user_query = (message.content or "").strip()
    if not user_query:
        await cl.Message(content="Please enter a question.").send()
        return

    # Best-effort question logging
    qlog = cl.user_session.get("qlog")
    if qlog is not None:
        try:
            qlog.append_question(
                question=user_query,
                session_id=str(cl.user_session.get("id", "")),
                tags=["energy-atlas-ai"],
            )
        except Exception as e:
            print(f"[WARN] Sheets logging failed: {e}")
    else:
        err = cl.user_session.get("qlog_error")
        if err:
            print(f"[INFO] Sheets disabled (init error): {err}")

    try:
        # (A) Route the query -> metric + params

        route = route_query(user_query)
        if route.primary_metric is None:
            raise ValueError(f"Unable to determine a metric for intent: {route.intent}")

        # (B) Execute -> fetch data (df + SourceRef)
        req = ExecuteRequest(
            metric=route.primary_metric,
            start=route.start,
            end=route.end,
            filters=route.filters,
        )
        result = executor.execute(req)

        # (C) Build AnswerPayload (OpenAI writes narrative; you keep facts/sources)
        payload = build_answer_with_openai(
            query=user_query,
            result=result,
            mode="observed",
            model=os.getenv("OPENAI_MODEL", "gpt-5.2"),
        )

        msg = await cl.Message(content=payload.answer_text).send()

        if payload.chart_spec is not None:
            fig = render_plotly(payload.chart_spec, result.df)

            await cl.Plotly(name=payload.chart_spec.title, figure=fig).send(
                for_id=msg.id
            )

        # sources
        if payload.sources:
            lines = [f"• {s.label}" for s in payload.sources]

            await cl.Message(content="**Sources**\n" + "\n".join(lines)).send()

    except Exception as e:

        # Keep errors visible during development
        await cl.Message(content=f"Error: {e}").send()
