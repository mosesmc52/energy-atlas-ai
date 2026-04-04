from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from alerts.services import ParsedSignal, SignalErrorCode, SignalEvaluator, parse_signal_question


class _FakeExecutor:
    def __init__(self, mapping: dict[str, pd.DataFrame]):
        self.mapping = mapping

    def execute(self, req):
        return SimpleNamespace(
            df=self.mapping[req.metric].copy(),
            source=SimpleNamespace(reference=f"test:{req.metric}"),
            meta={"metric": req.metric},
        )


class _FakeEIA:
    def __init__(self, current_df: pd.DataFrame, historical_frames: list[pd.DataFrame]):
        self.current_df = current_df
        self.historical_frames = historical_frames
        self.call_count = 0

    def get_weather_hdd(self, *, region_id: str, start: str, end: str, method: str = "mean"):
        self.call_count += 1
        if self.call_count == 1:
            return self.current_df.copy()
        idx = self.call_count - 2
        if idx < len(self.historical_frames):
            return self.historical_frames[idx].copy()
        return pd.DataFrame(columns=["date", "region_id", "hdd"])


class TestSignalAlerts(unittest.TestCase):
    def test_parse_storage_signal_question(self) -> None:
        parsed = parse_signal_question(
            "Is current natural gas storage more than 10% below the 5-year average?"
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.signal_id, "storage_below_five_year_average_pct")
        self.assertEqual(parsed.config["threshold"], -10.0)

    def test_storage_signal_returns_true_with_values(self) -> None:
        dates = pd.to_datetime(
            [
                "2021-03-25",
                "2022-03-25",
                "2023-03-25",
                "2024-03-25",
                "2025-03-25",
                "2026-03-25",
            ]
        )
        df = pd.DataFrame(
            {
                "date": dates,
                "value": [2100, 2050, 2000, 2020, 1970, 1789],
            }
        )
        evaluator = SignalEvaluator(
            executor=_FakeExecutor({"working_gas_storage_lower48": df}),
            eia=_FakeEIA(pd.DataFrame(), []),
        )
        evaluation = evaluator.evaluate(
            ParsedSignal(
                signal_id="storage_below_five_year_average_pct",
                question="Is current natural gas storage more than 10% below the 5-year average?",
                metric="working_gas_storage_lower48",
                config={"threshold": -10.0},
            )
        )
        self.assertTrue(evaluation.result)
        self.assertEqual(evaluation.metric, "working_gas_storage_lower48")
        self.assertIn("pct_diff", evaluation.values)

    def test_production_signal_returns_insufficient_data(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
                "value": [100.0, 99.0, 98.0],
            }
        )
        evaluator = SignalEvaluator(
            executor=_FakeExecutor({"ng_production_lower48": df}),
            eia=_FakeEIA(pd.DataFrame(), []),
        )
        evaluation = evaluator.evaluate(
            ParsedSignal(
                signal_id="production_below_30d_average",
                question="Is production below its 30-day average?",
                metric="ng_production_lower48",
            )
        )
        self.assertIsNone(evaluation.result)
        self.assertEqual(evaluation.error_code, SignalErrorCode.INSUFFICIENT_DATA)


if __name__ == "__main__":
    unittest.main()
