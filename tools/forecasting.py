from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, Optional

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from executer import MetricExecutor


class ForecastErrorCode:
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    INVALID_HORIZON = "INVALID_HORIZON"
    FORECAST_ERROR = "FORECAST_ERROR"


@dataclass(frozen=True)
class ForecastResult:
    metric: str
    horizon_days: int
    explanation: str
    forecast_points: list[dict[str, Any]] = field(default_factory=list)
    observations_used: int = 0
    lookback_observations: int = 30
    as_of: Optional[str] = None
    error_code: Optional[str] = None
    overlay: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "metric": self.metric,
            "horizon_days": self.horizon_days,
            "explanation": self.explanation,
            "observations_used": self.observations_used,
            "lookback_observations": self.lookback_observations,
            "forecast_points": self.forecast_points,
        }
        if self.as_of:
            payload["as_of"] = self.as_of
        if self.error_code:
            payload["error_code"] = self.error_code
        if self.overlay:
            payload["overlay"] = self.overlay
        if self.metadata:
            payload["metadata"] = self.metadata
        return payload


class TrendForecaster:
    def __init__(self, *, executor: MetricExecutor | None):
        self.executor = executor

    def forecast_metric(
        self,
        metric: str,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        horizon_days: int = 7,
        lookback_observations: int = 30,
        min_observations: int = 10,
        include_overlay: bool = False,
    ) -> ForecastResult:
        if self.executor is None:
            return ForecastResult(
                metric=metric,
                horizon_days=horizon_days,
                explanation="Forecast execution is not configured.",
                lookback_observations=lookback_observations,
                error_code=ForecastErrorCode.FORECAST_ERROR,
            )

        end_date = end or date.today().isoformat()
        start_date = start or (date.today() - timedelta(days=365)).isoformat()
        from executer import ExecuteRequest

        try:
            result = self.executor.execute(
                ExecuteRequest(
                    metric=metric,
                    start=start_date,
                    end=end_date,
                    filters=filters or {},
                )
            )
        except Exception as exc:  # noqa: BLE001
            return ForecastResult(
                metric=metric,
                horizon_days=horizon_days,
                explanation=f"Forecast generation failed: {exc}",
                lookback_observations=lookback_observations,
                error_code=ForecastErrorCode.FORECAST_ERROR,
            )

        return self.forecast_dataframe(
            result.df,
            metric=metric,
            horizon_days=horizon_days,
            lookback_observations=lookback_observations,
            min_observations=min_observations,
            include_overlay=include_overlay,
            source_reference=result.source.reference,
        )

    def forecast_dataframe(
        self,
        df: pd.DataFrame,
        *,
        metric: str,
        horizon_days: int = 7,
        lookback_observations: int = 30,
        min_observations: int = 10,
        include_overlay: bool = False,
        source_reference: Optional[str] = None,
    ) -> ForecastResult:
        if horizon_days < 7 or horizon_days > 14:
            return ForecastResult(
                metric=metric,
                horizon_days=horizon_days,
                explanation="Forecast horizon must be between 7 and 14 days.",
                lookback_observations=lookback_observations,
                error_code=ForecastErrorCode.INVALID_HORIZON,
            )
        try:
            cleaned = self._clean_timeseries(df)
            if len(cleaned) < min_observations:
                return ForecastResult(
                    metric=metric,
                    horizon_days=horizon_days,
                    explanation="Not enough recent data to produce a forecast.",
                    observations_used=len(cleaned),
                    lookback_observations=lookback_observations,
                    error_code=ForecastErrorCode.INSUFFICIENT_DATA,
                )

            window = cleaned.tail(min(lookback_observations, len(cleaned))).reset_index(drop=True)
            spacing_days = self._infer_spacing_days(window)
            if spacing_days is None:
                return ForecastResult(
                    metric=metric,
                    horizon_days=horizon_days,
                    explanation="Not enough date spacing information to produce a forecast.",
                    observations_used=len(window),
                    lookback_observations=lookback_observations,
                    error_code=ForecastErrorCode.INSUFFICIENT_DATA,
                )

            forecast_points, slope = self._project_linear_trend(
                window,
                horizon_days=horizon_days,
                spacing_days=spacing_days,
            )
            explanation = (
                f"Linear trend projection using the most recent {len(window)} observations "
                f"for the next {horizon_days} days."
            )
            metadata: dict[str, Any] = {
                "inferred_frequency_days": round(spacing_days, 2),
                "slope_per_day": round(slope, 6),
                "chart_overlay_available": include_overlay,
            }
            if source_reference:
                metadata["metric_source"] = source_reference

            overlay: dict[str, Any] = {}
            if include_overlay:
                overlay = {
                    "historical": self._serialize_points(window),
                    "forecast": forecast_points,
                }

            return ForecastResult(
                metric=metric,
                horizon_days=horizon_days,
                explanation=explanation,
                forecast_points=forecast_points,
                observations_used=len(window),
                lookback_observations=lookback_observations,
                as_of=window.iloc[-1]["date"].date().isoformat(),
                overlay=overlay,
                metadata=metadata,
            )
        except Exception as exc:  # noqa: BLE001
            return ForecastResult(
                metric=metric,
                horizon_days=horizon_days,
                explanation=f"Forecast generation failed: {exc}",
                lookback_observations=lookback_observations,
                error_code=ForecastErrorCode.FORECAST_ERROR,
            )

    @staticmethod
    def _clean_timeseries(df: pd.DataFrame) -> pd.DataFrame:
        cleaned = df.copy()
        cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
        cleaned["value"] = pd.to_numeric(cleaned["value"], errors="coerce")
        cleaned = cleaned.dropna(subset=["date", "value"]).sort_values("date")
        return cleaned[["date", "value"]]

    @staticmethod
    def _infer_spacing_days(df: pd.DataFrame) -> Optional[float]:
        deltas = df["date"].diff().dropna()
        if deltas.empty:
            return None
        spacing_days = deltas.dt.total_seconds().median() / 86400.0
        if pd.isna(spacing_days) or spacing_days <= 0:
            return None
        return max(float(spacing_days), 1.0)

    @staticmethod
    def _project_linear_trend(
        df: pd.DataFrame,
        *,
        horizon_days: int,
        spacing_days: float,
    ) -> tuple[list[dict[str, Any]], float]:
        base_date = df.iloc[0]["date"]
        x = ((df["date"] - base_date).dt.total_seconds() / 86400.0).to_numpy(dtype=float)
        y = df["value"].to_numpy(dtype=float)
        slope, intercept = np.polyfit(x, y, 1)

        future_steps = max(1, int(np.ceil(horizon_days / spacing_days)))
        last_x = float(x[-1])
        last_date = df.iloc[-1]["date"]
        offsets = np.arange(1, future_steps + 1, dtype=float) * spacing_days

        points: list[dict[str, Any]] = []
        for offset in offsets:
            forecast_value = float(intercept + slope * (last_x + offset))
            forecast_date = last_date + pd.to_timedelta(offset, unit="D")
            points.append(
                {
                    "date": forecast_date.isoformat(),
                    "value": round(forecast_value, 4),
                }
            )
        return points, float(slope)

    @staticmethod
    def _serialize_points(df: pd.DataFrame) -> list[dict[str, Any]]:
        return [
            {
                "date": row.date.isoformat(),
                "value": round(float(row.value), 4),
            }
            for row in df.itertuples(index=False)
        ]


def forecast_linear_trend(
    df: pd.DataFrame,
    *,
    metric: str,
    horizon_days: int = 7,
    lookback_observations: int = 30,
    min_observations: int = 10,
    include_overlay: bool = False,
    source_reference: Optional[str] = None,
) -> ForecastResult:
    return TrendForecaster(executor=None).forecast_dataframe(
        df,
        metric=metric,
        horizon_days=horizon_days,
        lookback_observations=lookback_observations,
        min_observations=min_observations,
        include_overlay=include_overlay,
        source_reference=source_reference,
    )
