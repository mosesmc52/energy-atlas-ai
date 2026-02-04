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
from executer import ExecuteRequest, MetricExecutor
from tools.eia_adapter import EIAAdapter
from utils.sheets_logger import GoogleSheetsQuestionLogger

# -------------------------
# 1) Dependency wiring (startup)
# -------------------------


def build_container():
    """
    Build and return the app dependencies once.
    This is essentially your "DI container".
    """
    # Replace with your actual eia-ng-client initialization
    # Example (placeholder):
    # from eia_ng_client import EIAClient
    # eia_client = EIAClient(api_key=os.environ["EIA_API_KEY"])
    eia_client = None  # TODO: set this

    eia_adapter = EIAAdapter()
    executor = MetricExecutor(eia=eia_adapter)

    # OpenAI client is created inside build_answer_with_openai in the earlier example,
    # but you can also inject it if you prefer.
    return {"executor": executor}


@cl.set_starters
async def set_starters():
    return [
        cl.Starter(
            label="Morning routine ideation",
            message="Can you help me create a personalized morning routine that would help increase my productivity throughout the day? Start by asking me about my current habits and what activities energize me in the morning.",
            icon="/public/idea.svg",
        ),
        cl.Starter(
            label="Explain superconductors",
            message="Explain superconductors like I'm five years old.",
            icon="/public/learn.svg",
        ),
        cl.Starter(
            label="Python script for daily email reports",
            message="Write a script to automate sending daily email reports in Python, and walk me through how I would set it up.",
            icon="/public/terminal.svg",
            command="code",
        ),
        cl.Starter(
            label="Text inviting friend to wedding",
            message="Write a text asking a friend to be my plus-one at a wedding next month. I want to keep it super short and casual, and offer an out.",
            icon="/public/write.svg",
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

        # (B) Execute -> fetch data (df + SourceRef)
        req = ExecuteRequest(
            metric=route.metric,
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

        # (D) Render in Chainlit
        await cl.Message(content=payload.answer_text).send()

        if payload.data_preview:
            # render preview as markdown table (simple)
            cols = payload.data_preview.columns
            rows = payload.data_preview.rows

            # Build a small markdown table
            header = "| " + " | ".join(cols) + " |"
            sep = "| " + " | ".join(["---"] * len(cols)) + " |"
            body = "\n".join("| " + " | ".join(str(x) for x in r) + " |" for r in rows)
            table_md = "\n".join([header, sep, body])

            await cl.Message(content=f"**Data (preview)**\n\n{table_md}").send()

        # sources
        if payload.sources:
            src_lines = []
            for s in payload.sources:
                src_lines.append(
                    f"- **{s.label}**\n"
                    f"  - type: `{s.source_type}`\n"
                    f"  - ref: `{s.reference}`\n"
                    f"  - params: `{s.parameters}`\n"
                    f"  - retrieved_at: `{s.retrieved_at}`"
                )
            await cl.Message(content="**Sources**\n" + "\n".join(src_lines)).send()

        # chart (later): payload.chart_spec -> renderer -> cl.Plotly(...)
        # if payload.chart_spec:
        #     fig = render_chart(payload.chart_spec)
        #     await cl.Plotly(name=payload.chart_spec.title, figure=fig).send()

    except Exception as e:
        # Keep errors visible during development
        await cl.Message(content=f"Error: {e}").send()
