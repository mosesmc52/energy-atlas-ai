from __future__ import annotations

import json
import os
import pathlib
import re
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Any, Optional

import pandas as pd

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.router import route_query
from executer import ExecuteRequest, MetricExecutor
from tools.forecasting import TrendForecaster
from tools.eia_adapter import EIAAdapter
from tools.gridstatus_adapter import GridStatusAdapter


class SignalErrorCode:
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    UNSUPPORTED_SIGNAL = "UNSUPPORTED_SIGNAL"
    EVALUATION_ERROR = "EVALUATION_ERROR"


@dataclass(frozen=True)
class ParsedSignal:
    signal_id: str
    question: str
    metric: str
    filters: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SignalEvaluation:
    question: str
    result: Optional[bool]
    explanation: str
    values: dict[str, Any] = field(default_factory=dict)
    as_of: Optional[str] = None
    metric: Optional[str] = None
    error_code: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "question": self.question,
            "result": self.result,
            "explanation": self.explanation,
        }
        if self.values:
            payload["values"] = self.values
        if self.as_of:
            payload["as_of"] = self.as_of
        if self.metric:
            payload["metric"] = self.metric
        if self.error_code:
            payload["error_code"] = self.error_code
        if self.metadata:
            payload["metadata"] = self.metadata
        return payload


def build_signal_evaluator() -> "SignalEvaluator":
    cache_root = pathlib.Path(
        os.getenv(
            "ATLAS_CACHE_ROOT",
            str(pathlib.Path(tempfile.gettempdir()) / "energy-atlas-ai-cache"),
        )
    )
    weather_csv_path = os.getenv("ATLAS_WEATHER_CSV_PATH")
    eia_adapter = EIAAdapter(
        cache_dir=cache_root / "eia",
        weather_csv_path=weather_csv_path,
    )
    grid_adapter = GridStatusAdapter(cache_dir=str(cache_root / "gridstatus"))
    executor = MetricExecutor(eia=eia_adapter, grid=grid_adapter)
    return SignalEvaluator(executor=executor, eia=eia_adapter)


def build_metric_forecaster() -> TrendForecaster:
    cache_root = pathlib.Path(
        os.getenv(
            "ATLAS_CACHE_ROOT",
            str(pathlib.Path(tempfile.gettempdir()) / "energy-atlas-ai-cache"),
        )
    )
    weather_csv_path = os.getenv("ATLAS_WEATHER_CSV_PATH")
    eia_adapter = EIAAdapter(
        cache_dir=cache_root / "eia",
        weather_csv_path=weather_csv_path,
    )
    grid_adapter = GridStatusAdapter(cache_dir=str(cache_root / "gridstatus"))
    executor = MetricExecutor(eia=eia_adapter, grid=grid_adapter)
    return TrendForecaster(executor=executor)


def parse_signal_question(question: str) -> Optional[ParsedSignal]:
    normalized = (question or "").strip().lower()
    if not normalized:
        return None

    match = re.search(
        r"storage .*more than (?P<threshold>\d+(?:\.\d+)?)% below the 5[- ]year average",
        normalized,
    )
    if match:
        threshold = float(match.group("threshold"))
        return ParsedSignal(
            signal_id="storage_below_five_year_average_pct",
            question=question,
            metric="working_gas_storage_lower48",
            config={"threshold": -threshold},
        )

    if "storage deficit widening week-over-week" in normalized:
        return ParsedSignal(
            signal_id="storage_deficit_widening_wow",
            question=question,
            metric="working_gas_storage_lower48",
        )

    if "hdd above normal this week" in normalized:
        return ParsedSignal(
            signal_id="hdd_above_normal_this_week",
            question=question,
            metric="weather_hdd_lower_48",
            filters={"region_id": "lower_48"},
        )

    if "supply-constrained regime" in normalized:
        return ParsedSignal(
            signal_id="supply_constrained_regime",
            question=question,
            metric="market_supply_regime",
            filters={"region_id": "lower_48"},
        )

    if re.search(r"production .*below .*30[- ]day average", normalized):
        return ParsedSignal(
            signal_id="production_below_30d_average",
            question=question,
            metric="ng_production_lower48",
        )

    route = route_query(question)
    if route.intent not in {"unsupported", "ambiguous"} and route.primary_metric:
        return ParsedSignal(
            signal_id="routed_metric_query",
            question=question,
            metric=route.primary_metric,
            filters=route.filters or {},
            config={
                "route_intent": route.intent,
                "route_source": route.source,
            },
        )

    return None


def parsed_signal_from_rule(rule) -> ParsedSignal:
    config_json = dict(rule.config_json or {})
    filters = dict(config_json.pop("filters", {}) or {})
    if rule.region and "region" not in filters:
        filters["region"] = rule.region
    return ParsedSignal(
        signal_id=rule.signal_id,
        question=rule.question,
        metric=rule.metric,
        filters=filters,
        config=config_json,
    )


def should_trigger_alert(previous_result: Optional[bool], new_result: Optional[bool], trigger_type: str) -> bool:
    if new_result is None:
        return False
    if trigger_type == "every_true":
        return new_result is True
    if trigger_type == "on_false_transition":
        return previous_result is True and new_result is False
    return previous_result is not True and new_result is True


class SignalEvaluator:
    def __init__(self, *, executor: MetricExecutor, eia: EIAAdapter):
        self.executor = executor
        self.eia = eia

    @staticmethod
    def _evaluated_at() -> str:
        return datetime.now(dt_timezone.utc).isoformat()

    def evaluate_question(self, question: str) -> SignalEvaluation:
        parsed = parse_signal_question(question)
        if parsed is None:
            return SignalEvaluation(
                question=question,
                result=None,
                explanation="This question does not map to a supported alert signal yet.",
                error_code=SignalErrorCode.UNSUPPORTED_SIGNAL,
            )
        return self.evaluate(parsed)

    def evaluate(self, parsed: ParsedSignal) -> SignalEvaluation:
        try:
            if parsed.signal_id == "storage_below_five_year_average_pct":
                return self._evaluate_storage_below_five_year_average(parsed)
            if parsed.signal_id == "storage_deficit_widening_wow":
                return self._evaluate_storage_deficit_widening(parsed)
            if parsed.signal_id == "hdd_above_normal_this_week":
                return self._evaluate_hdd_above_normal(parsed)
            if parsed.signal_id == "supply_constrained_regime":
                return self._evaluate_supply_constrained_regime(parsed)
            if parsed.signal_id == "production_below_30d_average":
                return self._evaluate_production_below_30d_average(parsed)
            if parsed.signal_id == "routed_metric_query":
                return self._evaluate_routed_metric_query(parsed)
        except Exception as exc:  # noqa: BLE001
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation=f"Signal evaluation failed: {exc}",
                metric=parsed.metric,
                error_code=SignalErrorCode.EVALUATION_ERROR,
            )

        return SignalEvaluation(
            question=parsed.question,
            result=None,
            explanation="This signal is not implemented.",
            metric=parsed.metric,
            error_code=SignalErrorCode.UNSUPPORTED_SIGNAL,
        )

    def _execute_metric(self, metric: str, start: str, end: str, filters: Optional[dict[str, Any]] = None):
        return self.executor.execute(
            ExecuteRequest(metric=metric, start=start, end=end, filters=filters)
        )

    @staticmethod
    def _pick_value_column(df: pd.DataFrame, metric: str) -> Optional[str]:
        if "value" in df.columns:
            return "value"
        if metric == "iso_gas_dependency" and "gas_share" in df.columns:
            return "gas_share"
        numeric_cols = [
            c for c in df.columns if c != "date" and pd.api.types.is_numeric_dtype(df[c])
        ]
        return numeric_cols[0] if numeric_cols else None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None or pd.isna(value):
                return None
            return float(value)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _format_number(value: Optional[float]) -> str:
        if value is None:
            return "n/a"
        abs_value = abs(value)
        if abs_value >= 100:
            return f"{value:,.0f}"
        if abs_value >= 10:
            return f"{value:,.1f}"
        return f"{value:,.2f}"

    @staticmethod
    def _titleize_metric(metric: str) -> str:
        text = (metric or "").replace("_", " ").strip()
        acronyms = {"lng": "LNG", "ng": "Natural Gas", "iso": "ISO"}
        return " ".join(acronyms.get(part.lower(), part.capitalize()) for part in text.split()) or "Metric"

    def _sector_ranking_summary(self, df: pd.DataFrame) -> Optional[str]:
        if df.empty or "date" not in df.columns or "value" not in df.columns or "series" not in df.columns:
            return None
        latest_date = pd.to_datetime(df["date"], errors="coerce").max()
        if pd.isna(latest_date):
            return None
        latest_rows = df.loc[pd.to_datetime(df["date"], errors="coerce") == latest_date].copy()
        latest_rows["value"] = pd.to_numeric(latest_rows["value"], errors="coerce")
        latest_rows = latest_rows.dropna(subset=["value"]).sort_values("value", ascending=False)
        if latest_rows.empty:
            return None
        leader = latest_rows.iloc[0]
        ranking = ", ".join(
            f"{str(row['series']).replace('_', ' ')} ({self._format_number(float(row['value']))})"
            for _, row in latest_rows.iterrows()
        )
        return (
            f"As of {latest_date.date().isoformat()}, {str(leader['series']).replace('_', ' ')} "
            f"led with {self._format_number(float(leader['value']))}. Ranking: {ranking}."
        )

    @staticmethod
    def _infer_boolean_result(
        question: str,
        *,
        latest_value: Optional[float],
        prior_value: Optional[float],
        delta: Optional[float],
    ) -> Optional[bool]:
        q = (question or "").strip().lower()
        if not q:
            return None

        if not re.match(r"^(is|are|was|were|do|does|did|has|have|had|can)\b", q):
            return None

        if "higher than last year" in q or "higher than" in q or "rising" in q or "growing" in q or "increasing" in q:
            return None if delta is None else delta > 0
        if "lower than" in q or "falling" in q or "decreasing" in q or "declining" in q:
            return None if delta is None else delta < 0
        if "above" in q:
            return None if delta is None else delta > 0
        if "below" in q:
            return None if delta is None else delta < 0
        if "current" in q or q.startswith("what "):
            return None

        if latest_value is not None and prior_value is not None:
            return latest_value > prior_value
        return None

    def _evaluate_routed_metric_query(self, parsed: ParsedSignal) -> SignalEvaluation:
        route = route_query(parsed.question)
        if route.intent in {"unsupported", "ambiguous"} or route.primary_metric is None:
            reason = route.reason or "This question did not map to a supported metric route."
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation=reason,
                metric=parsed.metric,
                error_code=SignalErrorCode.UNSUPPORTED_SIGNAL,
            )

        result = self._execute_metric(
            route.primary_metric,
            start=route.start,
            end=route.end,
            filters=route.filters,
        )
        df = result.df.copy()
        if df.empty:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="No data was returned for the requested period.",
                metric=route.primary_metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        if route.primary_metric == "ng_consumption_by_sector":
            ranking_summary = self._sector_ranking_summary(df)
            if ranking_summary is None:
                return SignalEvaluation(
                    question=parsed.question,
                    result=None,
                    explanation="No data was returned for the requested period.",
                    metric=route.primary_metric,
                    error_code=SignalErrorCode.INSUFFICIENT_DATA,
                )
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation=ranking_summary,
                metric=route.primary_metric,
                metadata={
                    "route_intent": route.intent,
                    "route_source": route.source,
                    "filters": route.filters or {},
                    "metric_source": result.source.reference,
                    "evaluated_at": self._evaluated_at(),
                },
            )

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date")
        value_col = self._pick_value_column(df, route.primary_metric)
        if value_col is None:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="The query returned data, but no numeric value column was available to summarize it.",
                metric=route.primary_metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        df = df.dropna(subset=[value_col])
        if df.empty:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="The query returned data, but no usable numeric observations were available.",
                metric=route.primary_metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        latest = df.iloc[-1]
        latest_value = self._safe_float(latest[value_col])
        prior_value = self._safe_float(df.iloc[-2][value_col]) if len(df) >= 2 else None
        delta = (
            None
            if latest_value is None or prior_value is None
            else latest_value - prior_value
        )
        latest_date_value = latest["date"] if "date" in latest else None
        latest_date = (
            latest_date_value.date().isoformat()
            if isinstance(latest_date_value, pd.Timestamp)
            else None
        )
        metric_label = self._titleize_metric(route.primary_metric)
        latest_text = self._format_number(latest_value)
        delta_text = (
            f", {'up' if delta > 0 else 'down' if delta < 0 else 'unchanged'} "
            f"{self._format_number(abs(delta))} from the prior observation"
            if delta is not None
            else ""
        )
        explanation = (
            f"As of {latest_date}, {metric_label} is {latest_text}{delta_text}."
            if latest_date
            else f"Latest {metric_label} reading is {latest_text}{delta_text}."
        )

        inferred_result = self._infer_boolean_result(
            parsed.question,
            latest_value=latest_value,
            prior_value=prior_value,
            delta=delta,
        )
        if inferred_result is None and re.match(r"^(is|are|was|were|do|does|did|has|have|had|can)\b", parsed.question.strip().lower()):
            explanation = (
                f"{explanation} This question was supported for data retrieval, "
                "but it does not map to a strict boolean alert condition yet."
            )

        values: dict[str, Any] = {
            "latest_value": latest_value,
            "prior_value": prior_value,
            "delta": delta,
            "value_column": value_col,
        }
        if latest_date:
            values["latest_date"] = latest_date

        return SignalEvaluation(
            question=parsed.question,
            result=inferred_result,
            explanation=explanation,
            values=values,
            as_of=latest_date,
            metric=route.primary_metric,
            metadata={
                "route_intent": route.intent,
                "route_source": route.source,
                "filters": route.filters or {},
                "metric_source": result.source.reference,
                "evaluated_at": self._evaluated_at(),
            },
        )

    def _historical_comparison_values(
        self,
        df: pd.DataFrame,
        target_date: pd.Timestamp,
        *,
        years: int = 5,
        tolerance_days: int = 21,
    ) -> list[float]:
        out: list[float] = []
        series = df.copy()
        series["date"] = pd.to_datetime(series["date"], errors="coerce")
        series["value"] = pd.to_numeric(series["value"], errors="coerce")
        series = series.dropna(subset=["date", "value"]).sort_values("date")
        if series.empty:
            return out

        tolerance = pd.Timedelta(days=tolerance_days)
        for year in range(1, years + 1):
            historical_target = target_date - pd.DateOffset(years=year)
            distances = (series["date"] - historical_target).abs()
            idx = distances.idxmin()
            if pd.isna(idx):
                continue
            if distances.loc[idx] <= tolerance:
                out.append(float(series.loc[idx, "value"]))
        return out

    def _evaluate_storage_below_five_year_average(self, parsed: ParsedSignal) -> SignalEvaluation:
        end = date.today().isoformat()
        start = (date.today() - timedelta(days=365 * 6 + 30)).isoformat()
        result = self._execute_metric(parsed.metric, start=start, end=end)
        df = result.df.copy()
        if df.empty:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough storage data to evaluate this signal.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date")
        latest = df.iloc[-1]
        comparison_values = self._historical_comparison_values(df, latest["date"])
        if len(comparison_values) < 3:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough historical storage data to compute a reliable 5-year average.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        five_year_average = sum(comparison_values) / len(comparison_values)
        pct_diff = ((float(latest["value"]) / five_year_average) - 1.0) * 100.0
        threshold = float(parsed.config.get("threshold", -10.0))
        is_true = pct_diff < threshold
        return SignalEvaluation(
            question=parsed.question,
            result=is_true,
            explanation=(
                f"Current storage is {pct_diff:.1f}% below the 5-year average, "
                f"which is {'below' if is_true else 'not below'} the {threshold:.1f}% threshold."
            ),
            values={
                "current_storage": round(float(latest["value"]), 2),
                "five_year_average": round(five_year_average, 2),
                "pct_diff": round(pct_diff, 1),
                "threshold": round(threshold, 1),
            },
            as_of=latest["date"].date().isoformat(),
            metric=parsed.metric,
            metadata={
                "metric_source": result.source.reference,
                "evaluated_at": self._evaluated_at(),
            },
        )

    def _evaluate_storage_deficit_widening(self, parsed: ParsedSignal) -> SignalEvaluation:
        end = date.today().isoformat()
        start = (date.today() - timedelta(days=365 * 6 + 60)).isoformat()
        result = self._execute_metric(parsed.metric, start=start, end=end)
        df = result.df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date")
        if len(df) < 2:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough storage history to compare week-over-week deficit changes.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        latest = df.iloc[-1]
        prior = df.iloc[-2]
        latest_hist = self._historical_comparison_values(df, latest["date"])
        prior_hist = self._historical_comparison_values(df, prior["date"])
        if len(latest_hist) < 3 or len(prior_hist) < 3:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough historical storage data to compute week-over-week deficit changes.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        latest_pct = ((float(latest["value"]) / (sum(latest_hist) / len(latest_hist))) - 1.0) * 100.0
        prior_pct = ((float(prior["value"]) / (sum(prior_hist) / len(prior_hist))) - 1.0) * 100.0
        widening = latest_pct < prior_pct
        return SignalEvaluation(
            question=parsed.question,
            result=widening,
            explanation=(
                f"The storage deficit moved from {prior_pct:.1f}% to {latest_pct:.1f}% versus the 5-year average, "
                f"so it is {'widening' if widening else 'not widening'} week-over-week."
            ),
            values={
                "current_pct_diff": round(latest_pct, 1),
                "previous_pct_diff": round(prior_pct, 1),
                "change_pct_points": round(latest_pct - prior_pct, 1),
            },
            as_of=latest["date"].date().isoformat(),
            metric=parsed.metric,
            metadata={
                "metric_source": result.source.reference,
                "evaluated_at": self._evaluated_at(),
            },
        )

    def _evaluate_hdd_above_normal(self, parsed: ParsedSignal) -> SignalEvaluation:
        end_date = date.today()
        start_date = end_date - timedelta(days=6)
        try:
            current_df = self.eia.get_weather_hdd(
                region_id=str(parsed.filters.get("region_id", "lower_48")),
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                method="mean",
            )
        except FileNotFoundError:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Weather history is not configured, so HDD signals cannot be evaluated.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )
        if current_df.empty:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough recent HDD data to evaluate this signal.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        current_week_hdd = float(pd.to_numeric(current_df["hdd"], errors="coerce").mean())
        historical_means: list[float] = []
        for year in range(1, 6):
            hist_start = start_date.replace(year=max(start_date.year - year, 1))
            hist_end = end_date.replace(year=max(end_date.year - year, 1))
            hist_df = self.eia.get_weather_hdd(
                region_id=str(parsed.filters.get("region_id", "lower_48")),
                start=hist_start.isoformat(),
                end=hist_end.isoformat(),
                method="mean",
            )
            if hist_df.empty:
                continue
            historical_means.append(
                float(pd.to_numeric(hist_df["hdd"], errors="coerce").mean())
            )

        if len(historical_means) < 2:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough historical HDD data to compute normal conditions.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        normal_hdd = sum(historical_means) / len(historical_means)
        is_true = current_week_hdd > normal_hdd
        return SignalEvaluation(
            question=parsed.question,
            result=is_true,
            explanation=(
                f"This week's HDD average is {current_week_hdd:.1f} versus a normal of {normal_hdd:.1f}, "
                f"so HDD are {'above' if is_true else 'not above'} normal."
            ),
            values={
                "current_week_hdd": round(current_week_hdd, 1),
                "normal_hdd": round(normal_hdd, 1),
                "difference": round(current_week_hdd - normal_hdd, 1),
            },
            as_of=end_date.isoformat(),
            metric=parsed.metric,
            metadata={"evaluated_at": self._evaluated_at()},
        )

    def _evaluate_supply_constrained_regime(self, parsed: ParsedSignal) -> SignalEvaluation:
        storage_eval = self._evaluate_storage_deficit_widening(
            ParsedSignal(
                signal_id="storage_deficit_widening_wow",
                question=parsed.question,
                metric="working_gas_storage_lower48",
            )
        )
        hdd_eval = self._evaluate_hdd_above_normal(
            ParsedSignal(
                signal_id="hdd_above_normal_this_week",
                question=parsed.question,
                metric="weather_hdd_lower_48",
                filters=parsed.filters,
            )
        )
        if storage_eval.result is None or hdd_eval.result is None:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough data to determine whether the market is supply-constrained.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        is_true = bool(storage_eval.result and hdd_eval.result)
        return SignalEvaluation(
            question=parsed.question,
            result=is_true,
            explanation=(
                "The market is in a supply-constrained regime because storage deficits are widening "
                "and HDD are above normal."
                if is_true
                else "The market is not currently in a supply-constrained regime because one or more required conditions are not met."
            ),
            values={
                "storage_deficit_widening": storage_eval.result,
                "hdd_above_normal": hdd_eval.result,
                "storage_signal_values": storage_eval.values,
                "weather_signal_values": hdd_eval.values,
            },
            as_of=storage_eval.as_of or hdd_eval.as_of,
            metric=parsed.metric,
            metadata={"evaluated_at": self._evaluated_at()},
        )

    def _evaluate_production_below_30d_average(self, parsed: ParsedSignal) -> SignalEvaluation:
        end = date.today().isoformat()
        start = (date.today() - timedelta(days=120)).isoformat()
        result = self._execute_metric(parsed.metric, start=start, end=end)
        df = result.df.copy()
        if df.empty:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough recent production data to evaluate this signal.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["date", "value"]).sort_values("date")
        if len(df) < 30:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough recent production data to evaluate this signal.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        median_spacing_days = (
            df["date"].diff().dropna().dt.total_seconds().median() / 86400.0
        )
        if median_spacing_days > 7:
            return SignalEvaluation(
                question=parsed.question,
                result=None,
                explanation="Not enough recent production data to evaluate this signal.",
                metric=parsed.metric,
                error_code=SignalErrorCode.INSUFFICIENT_DATA,
            )

        latest = df.iloc[-1]
        trailing_avg = float(df.iloc[-30:]["value"].mean())
        is_true = float(latest["value"]) < trailing_avg
        return SignalEvaluation(
            question=parsed.question,
            result=is_true,
            explanation=(
                f"Latest production is {float(latest['value']):.2f} versus a 30-day average of {trailing_avg:.2f}."
            ),
            values={
                "current_production": round(float(latest["value"]), 2),
                "thirty_day_average": round(trailing_avg, 2),
            },
            as_of=latest["date"].date().isoformat(),
            metric=parsed.metric,
            metadata={
                "metric_source": result.source.reference,
                "evaluated_at": self._evaluated_at(),
            },
        )


def evaluation_as_json(evaluation: SignalEvaluation) -> str:
    return json.dumps(evaluation.to_dict(), indent=2, default=str)
