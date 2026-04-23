# apps/chainlit/app.py
from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import requests
import sys
import tempfile
import urllib.parse
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

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "main.settings")
os.environ.setdefault(
    "DJANGO_CONFIGURATION",
    "Production" if os.getenv("DJANGO_DEBUG", "").strip().lower() == "false" else "Development",
)

from configurations.importer import install

install()

import django
from openai import OpenAI

django.setup()

import chainlit as cl
from django.conf import settings
from agents.router import route_query
from agents.guardrails import (
    OUT_OF_SCOPE_MESSAGE,
    is_natural_gas_question,
    looks_like_general_energy_question,
)
from alerts.services import (
    build_signal_evaluator,
    is_builtin_signal_id,
    parse_signal_question,
)
from answer_builder import build_answer_with_openai
from charts.plotly_renderer import (
    compute_timeseries_summary_metrics,
    compute_storage_change_summary_metrics,
    render_plotly,
    should_render_storage_change_summary_cards,
    should_render_timeseries_summary_cards,
)
from executer import ExecuteRequest, MetricExecutor
from schemas.answer import StructuredAnswer
from tools.cftc_adapter import CFTCAdapter
from tools.des_adapter import DallasEnergySurveyAdapter
from tools.forecasting import TrendForecaster
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
SHARE_ACTION_NAME = "share_structured_answer"
ANALYTICS_MESSAGE_TYPE = "energy_atlas_analytics"


async def _track_chainlit_event(event: str, **params: object) -> None:
    payload = {
        "type": ANALYTICS_MESSAGE_TYPE,
        "event": event,
        "app_surface": "chainlit",
        **params,
    }
    try:
        await cl.send_window_message(payload)
    except Exception as e:  # noqa: BLE001
        logger.debug("Unable to send Chainlit analytics event %s: %s", event, e)


def _general_energy_answer(question: str, previous_context: str = "") -> str:
    client = OpenAI()
    user_content = question
    if previous_context:
        user_content = (
            f"Previous energy question context: {previous_context}\n"
            f"Current user question: {question}"
        )
    response = client.responses.create(
        model=os.getenv("OPENAI_MODEL", "gpt-5.2"),
        input=[
            {
                "role": "system",
                "content": (
                    "You are Energy Atlas AI. Answer only natural-gas-market questions "
                    "that do not require the app's structured datasets. Refuse any "
                    "question that is outside natural gas. Keep the answer "
                    "concise and useful. If the question asks for rankings, state what "
                    "metric and timeframe you are assuming. If exact current data may "
                    "have changed, say so briefly instead of overstating precision. "
                    "Do not fabricate live values or citations."
                ),
            },
            {"role": "user", "content": user_content},
        ],
    )
    answer = str(getattr(response, "output_text", "") or "").strip()
    if not answer:
        raise ValueError("OpenAI returned an empty general answer")
    return answer


def _share_api_base_url() -> str:
    explicit_base = os.getenv("ATLAS_BACKEND_URL", "").strip().rstrip("/")
    if explicit_base:
        return explicit_base

    app_url = str(getattr(settings, "APP_URL", "") or "").strip().rstrip("/")
    if app_url:
        return app_url

    return "http://127.0.0.1:8000"


def _create_shared_answer(question: str, response_json: dict) -> str:
    response = requests.post(
        f"{_share_api_base_url()}/api/shared-answers/",
        json={
            "question": question,
            "response_json": response_json,
        },
        timeout=15,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError:
        logger.warning(
            "Share API request failed: status=%s body=%s payload=%s",
            response.status_code,
            response.text[:2000],
            response_json,
        )
        raise
    payload = response.json()
    share_url = str(payload.get("url") or "").strip()
    if not share_url:
        raise ValueError("Share API returned no URL")
    return share_url


def _display_metric_name(metric: str) -> str:
    text = (metric or "").strip()
    if "_" in text:
        return text.replace("_", " ").title()
    return text or "Metric"


def _alert_create_url(signal_id: str, title: str) -> str:
    query = urllib.parse.urlencode({"signal_id": signal_id, "title": title})
    return f"/alerts/new/?{query}"


def _forecast_direction_from_result(forecast) -> str:
    slope = ((forecast.metadata or {}).get("slope_per_day") if forecast else None)
    try:
        slope_value = float(slope)
    except (TypeError, ValueError):
        return "flat"
    if slope_value > 0:
        return "up"
    if slope_value < 0:
        return "down"
    return "flat"


def _format_card_value(value: object, unit: str) -> str:
    if value is None:
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return ""
    if numeric.is_integer():
        number = f"{int(numeric):,}"
    else:
        number = f"{numeric:,.1f}"
    return f"{number} {unit}".strip()


def format_summary_cards(metrics: list[dict]) -> str:
    if not metrics:
        return ""

    labels = []
    values = []
    subtitles = []
    for metric in metrics:
        raw_value = metric.get("value")
        value_text = _format_card_value(raw_value, str(metric.get("unit") or ""))
        if not value_text:
            continue
        labels.append(str(metric.get("label") or "").strip() or "Metric")
        values.append(f"**{value_text}**")
        subtitles.append(str(metric.get("subtitle") or "").strip() or " ")

    if not labels:
        return ""

    divider = "| " + " | ".join(["---"] * len(labels)) + " |"
    label_row = "| " + " | ".join(labels) + " |"
    value_row = "| " + " | ".join(values) + " |"
    subtitle_row = "| " + " | ".join(subtitles) + " |"
    return "\n".join([label_row, divider, value_row, subtitle_row])


def format_response(data: StructuredAnswer | dict) -> str:
    if hasattr(data, "model_dump"):
        data = data.model_dump()

    signal_map = {
        "bullish": "🟢 Bullish",
        "bearish": "🔴 Bearish",
        "neutral": "🟡 Neutral",
    }

    signal_data = data.get("signal") or {}
    signal = signal_map.get(str(signal_data.get("status") or "").lower(), "⚪ Unknown")
    confidence = int(float(signal_data.get("confidence") or 0) * 100)
    sections = [f"**{signal}**", f"Confidence: {confidence}%"]

    summary = str(data.get("summary") or data.get("answer") or "").strip()
    if summary:
        sections.append(f"**Summary**\n{summary}")

    drivers_list = [
        str(driver).strip() for driver in (data.get("drivers") or []) if str(driver).strip()
    ]
    if drivers_list:
        sections.append("**Drivers**\n" + "\n".join(f"- {driver}" for driver in drivers_list))

    forecast = data.get("forecast") or {}
    forecast_direction = str(forecast.get("direction") or "").strip()
    forecast_reasoning = str(forecast.get("reasoning") or "").strip()
    if forecast_direction or forecast_reasoning:
        forecast_lines = []
        if forecast_direction:
            forecast_lines.append(f"Direction: {forecast_direction}")
        if forecast_reasoning:
            forecast_lines.append(forecast_reasoning)
        sections.append("**Forecast**\n" + "  \n".join(forecast_lines))

    suggestions = _validated_suggested_alerts(data)
    if suggestions:
        sections.append(
            "**Suggested Alerts**\n"
            + "\n".join(
                f"- **[{item['title']}]({_alert_create_url(item['signal_id'], item['title'])})**  \n  {item['reason']}"
                for item in suggestions
            )
        )

    source_lines = [
        (
            f"- {source.get('title')} ({source.get('date')})"
            if source.get("date")
            else f"- {source.get('title')}"
        )
        for source in (data.get("sources") or [])
        if str(source.get("title") or "").strip()
    ]
    if source_lines:
        sections.append("**Sources**\n" + "\n".join(source_lines))

    return "\n\n".join(sections)


def _validated_suggested_alerts(data: StructuredAnswer | dict | None) -> list[dict[str, str]]:
    if data is None:
        return []
    if hasattr(data, "model_dump"):
        data = data.model_dump()

    suggestions = []
    for item in (data.get("suggested_alerts") or []):
        if not isinstance(item, dict):
            continue
        signal_id = str(item.get("signal_id") or "").strip()
        title = str(item.get("title") or "").strip()
        reason = str(item.get("reason") or "").strip()
        priority = str(item.get("priority") or "").strip() or "medium"
        if not title or not reason or not is_builtin_signal_id(signal_id):
            continue
        suggestions.append(
            {
                "signal_id": signal_id,
                "title": title,
                "reason": reason,
                "priority": priority,
            }
        )
    return suggestions


def _format_signal_evaluation(evaluation) -> str:
    signal = "⚪ Unknown"
    if evaluation.result is True:
        signal = "🟢 Bullish"
    elif evaluation.result is False:
        signal = "🔴 Bearish"

    sections = [f"**{signal}**"]
    if evaluation.explanation:
        sections.append(f"**Summary**\n{evaluation.explanation}")

    value_lines = []
    for key, value in (evaluation.values or {}).items():
        if value in (None, "", {}, []):
            continue
        label = key.replace("_", " ").title()
        value_lines.append(f"- {label}: **{value}**")
    if value_lines:
        sections.append("**Drivers**\n" + "\n".join(value_lines))

    sections.append(
        "**Forecast**\nDirection: flat  \nAlert signal evaluations do not include a forward forecast."
    )
    if evaluation.metric or evaluation.as_of:
        source_line = evaluation.metric or "Built-in signal engine"
        if evaluation.as_of:
            source_line = f"{source_line} ({evaluation.as_of})"
        sections.append("**Sources**\n- " + source_line)
    return "\n\n".join(sections)

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
    weather_csv_path = os.getenv("ATLAS_WEATHER_CSV_PATH")
    if not weather_csv_path:
        weather_csv_path = str(
            (proj_root / "data" / "raw" / "noaa" / "regional" / "daily_region_weather.csv")
        )
    eia_adapter = EIAAdapter(
        cache_dir=cache_root / "eia",
        weather_csv_path=weather_csv_path,
    )
    grid_adapter = GridStatusAdapter(cache_dir=str(cache_root / "gridstatus"))
    des_adapter = DallasEnergySurveyAdapter(
        raw_dir=cache_root / "des" / "raw",
        processed_dir=cache_root / "des" / "processed",
    )
    cftc_adapter = CFTCAdapter(cache_dir=cache_root / "cftc")
    executor = MetricExecutor(
        eia=eia_adapter,
        grid=grid_adapter,
        des=des_adapter,
        cftc=cftc_adapter,
    )
    signal_evaluator = build_signal_evaluator()
    forecaster = TrendForecaster(executor=executor)

    return {
        "executor": executor,
        "signal_evaluator": signal_evaluator,
        "forecaster": forecaster,
    }


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
    cl.user_session.set("shareable_answers", {})
    await _track_chainlit_event(
        "chat_started",
        is_authenticated=bool(cl.user_session.get("user")),
    )

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
    forecaster: TrendForecaster | None = deps.get("forecaster")  # type: ignore[assignment]

    user_query = (message.content or "").strip()
    if not user_query:
        await cl.Message(content="Please enter a question.").send()
        return
    previous_energy_context = str(cl.user_session.get("last_energy_question") or "")
    if not is_natural_gas_question(user_query, previous_energy_context):
        await cl.Message(content=OUT_OF_SCOPE_MESSAGE).send()
        return
    await _track_chainlit_event(
        "question_submitted",
        is_authenticated=bool(cl.user_session.get("user")),
    )

    parsed_signal = parse_signal_question(user_query)
    if (
        parsed_signal is not None
        and signal_evaluator is not None
        and parsed_signal.signal_id != "routed_metric_query"
    ):
        evaluation = signal_evaluator.evaluate(parsed_signal)
        await cl.Message(content=_format_signal_evaluation(evaluation)).send()
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
            if looks_like_general_energy_question(user_query, previous_energy_context):
                try:
                    answer = await asyncio.to_thread(
                        _general_energy_answer,
                        user_query,
                        previous_energy_context,
                    )
                except Exception as e:
                    logger.warning("General energy fallback failed: %s", e)
                else:
                    cl.user_session.set("last_energy_question", user_query)
                    await cl.Message(content=answer).send()
                    await _track_chainlit_event(
                        "answer_rendered",
                        has_chart=False,
                        has_forecast=False,
                        is_authenticated=bool(cl.user_session.get("user")),
                        route_source="general_llm",
                    )
                    return

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
        cl.user_session.set("last_energy_question", user_query)

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
        forecast = None
        if route.include_forecast and forecaster is not None:
            forecast = forecaster.forecast_dataframe(
                result.df,
                metric=route.primary_metric,
                horizon_days=route.forecast_horizon_days or 7,
                include_overlay=True,
                source_reference=result.source.reference,
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
        if payload.report_context_used:
            logger.info(
                "report_rag active=true reason=%s sources=%s query=%r",
                payload.report_context_reason,
                [source.title for source in payload.report_context_sources],
                user_query,
            )
        else:
            logger.info(
                "report_rag active=false reason=%s query=%r",
                payload.report_context_reason,
                user_query,
            )
        if forecast is not None and payload.structured_response is not None:
            payload.structured_response.forecast.direction = _forecast_direction_from_result(
                forecast
            )
            payload.structured_response.forecast.reasoning = forecast.explanation
        elif forecast is not None:
            payload.answer_text = f"{payload.answer_text}\n\nForecast: {forecast.explanation}"

        message_started = perf_counter() if DEBUG_ENABLED else 0.0
        rendered_content = (
            format_response(payload.structured_response)
            if payload.structured_response is not None
            else payload.answer_text
        )
        msg = await cl.Message(content=rendered_content).send()
        if payload.structured_response is not None:
            shareable_answers = dict(cl.user_session.get("shareable_answers") or {})
            shareable_answers[msg.id] = {
                "question": user_query,
                "response_json": payload.structured_response.model_dump(mode="json"),
            }
            cl.user_session.set("shareable_answers", shareable_answers)
            msg.actions = [
                cl.Action(
                    name=SHARE_ACTION_NAME,
                    label="Share",
                    tooltip="Create an unlisted share link for this answer",
                    icon="share",
                    payload={"message_id": msg.id},
                )
            ]
            await msg.update()
        message_elapsed_ms = (
            (perf_counter() - message_started) * 1000 if DEBUG_ENABLED else 0.0
        )

        if payload.report_context_used and payload.report_context_sources:
            rag_lines = [
                f"- {source.title} ({source.date})"
                if source.date
                else f"- {source.title}"
                for source in payload.report_context_sources
            ]
            await cl.Message(
                content="**Report Context Used**\n" + "\n".join(rag_lines)
            ).send()

        chart_elapsed_ms = 0.0
        if payload.chart_spec is not None:
            chart_started = perf_counter() if DEBUG_ENABLED else 0.0
            fig = render_plotly(payload.chart_spec, result.df, forecast_overlay=forecast)

            summary_metrics = []
            if should_render_storage_change_summary_cards(payload.chart_spec):
                summary_metrics = compute_storage_change_summary_metrics(result.df)
            elif should_render_timeseries_summary_cards(payload.chart_spec):
                summary_metrics = compute_timeseries_summary_metrics(
                    result.df,
                    unit=getattr(payload.chart_spec.y, "units", None),
                )

            if summary_metrics:
                summary_cards = format_summary_cards(summary_metrics)
                if summary_cards:
                    await cl.Message(content=summary_cards).send()

            await cl.Plotly(name=payload.chart_spec.title, figure=fig).send(
                for_id=msg.id
            )
            if DEBUG_ENABLED:
                chart_elapsed_ms = (perf_counter() - chart_started) * 1000

        # sources
        sources_elapsed_ms = 0.0
        if payload.sources and payload.structured_response is None:
            sources_started = perf_counter() if DEBUG_ENABLED else 0.0
            lines = [f"• {s.label}" for s in payload.sources]

            await cl.Message(content="**Sources**\n" + "\n".join(lines)).send()
            if DEBUG_ENABLED:
                sources_elapsed_ms = (perf_counter() - sources_started) * 1000

        await _track_chainlit_event(
            "answer_rendered",
            has_chart=payload.chart_spec is not None,
            has_forecast=forecast is not None,
            is_authenticated=bool(cl.user_session.get("user")),
        )

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


@cl.action_callback(SHARE_ACTION_NAME)
async def share_structured_answer(action: cl.Action):
    message_id = str((action.payload or {}).get("message_id") or "").strip()
    shareable_answers = cl.user_session.get("shareable_answers") or {}
    answer_payload = shareable_answers.get(message_id) if isinstance(shareable_answers, dict) else None
    if not answer_payload:
        await cl.Message(
            content="This answer is no longer available to share in the current session."
        ).send()
        return

    try:
        share_url = await asyncio.to_thread(
            _create_shared_answer,
            str(answer_payload.get("question") or ""),
            dict(answer_payload.get("response_json") or {}),
        )
    except Exception as e:
        logger.warning("Share link creation failed: %s", e)
        await cl.Message(content="Unable to create a share link right now.").send()
        return

    await cl.Message(
        content=(
            "**Share Link**\n"
            f"Unlisted link for this answer only: {share_url}"
        )
    ).send()
    await _track_chainlit_event(
        "shared_answer_created",
        is_authenticated=bool(cl.user_session.get("user")),
    )
