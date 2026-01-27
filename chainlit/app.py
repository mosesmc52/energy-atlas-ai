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


# Store dependencies in Chainlit session
@cl.on_chat_start
async def on_chat_start():
    deps = build_container()
    cl.user_session.set("deps", deps)

    await cl.Message(
        content="Energy Atlas AI (v0.1). Ask about natural gas storage, Henry Hub, or LNG exports."
    ).send()


# -------------------------
# 2) Message handler (per user query)
# -------------------------


@cl.on_message
async def on_message(message: cl.Message):
    deps = cl.user_session.get("deps")
    executor: MetricExecutor = deps["executor"]

    user_query = (message.content or "").strip()
    if not user_query:
        await cl.Message(content="Please enter a question.").send()
        return

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
