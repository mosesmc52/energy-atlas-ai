from __future__ import annotations

import unittest
from datetime import datetime
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pandas as pd

from answer_builder import build_answer_with_openai
from schemas.answer import SourceRef
from tools.eia_adapter import EIAResult


class TestAnswerBuilder(unittest.TestCase):
    def test_sector_consumption_empty_dataframe_does_not_crash(self) -> None:
        result = EIAResult(
            df=pd.DataFrame(columns=["date", "value", "series"]),
            source=SourceRef(
                source_type="eia_api",
                label="Test",
                reference="test",
                retrieved_at=datetime.utcnow(),
            ),
            meta={"metric": "ng_consumption_by_sector"},
        )

        payload = build_answer_with_openai(
            query="Which sector consumes the most gas (power, residential, industrial)?",
            result=result,
        )

        self.assertEqual(
            payload.answer_text, "No data was returned for the requested period."
        )
        self.assertIsNotNone(payload.structured_response)
        self.assertEqual(payload.structured_response.signal.status, "neutral")

    def test_structured_response_is_built_for_standard_metric(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-01", "value": 13.5},
                {"date": "2026-01-08", "value": 14.1},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Natural Gas Weekly Update",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "lng_exports"},
        )

        payload = build_answer_with_openai(
            query="Are LNG exports rising?",
            result=result,
        )

        self.assertIsNotNone(payload.structured_response)
        structured = payload.structured_response
        self.assertEqual(structured.signal.status, "bullish")
        self.assertEqual(structured.signal.confidence, 0.82)
        self.assertEqual(structured.data_points[0].metric, "LNG Exports")
        self.assertEqual(structured.data_points[0].value, 14.1)
        self.assertEqual(structured.sources[0].title, "Natural Gas Weekly Update")
        self.assertEqual(structured.sources[0].date, "2026-01-22")

    def test_llm_path_can_include_report_context_sources(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-01", "value": 13.5},
                {"date": "2026-01-08", "value": 14.1},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Natural Gas Weekly Update",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "lng_exports"},
        )

        with NamedTemporaryFile("w", suffix=".jsonl", encoding="utf-8") as handle:
            handle.write(
                '{"title":"Today in Energy","report_type":"analysis","text":"Recent reports said LNG exports stayed strong because winter demand improved.","published_date":"2026-01-18","topics":["lng"]}\n'
            )
            handle.flush()

            class _FakeResponse:
                output_text = (
                    '{"answer":"Market tightening","signal":{"status":"bullish","confidence":0.82},"summary":"Recent reports highlighted strong LNG exports.","drivers":["High LNG exports"],"data_points":[{"metric":"LNG Exports","value":14.1,"unit":"MMcf"}],"forecast":{"direction":"up","reasoning":"Demand expected to remain elevated"},"alerts":[{"name":"High LNG Exports","status":true}],"sources":[{"title":"Today in Energy","date":"2026-01-18"}]}'
                )

            with patch.dict(
                "os.environ",
                {
                    "ATLAS_USE_LLM_NARRATION": "true",
                    "REPORT_CHUNKS_PATH": handle.name,
                },
                clear=False,
            ), patch(
                "answer_builder.client.responses.create",
                return_value=_FakeResponse(),
            ):
                payload = build_answer_with_openai(
                    query="Why are LNG exports supporting the market?",
                    result=result,
                )

        self.assertTrue(payload.report_context_used)
        self.assertEqual(len(payload.report_context_sources), 1)
        self.assertEqual(payload.report_context_sources[0].title, "Today in Energy")

    def test_regional_storage_change_builds_structured_ranking_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-08", "region": "east", "value": 40.0},
                {"date": "2026-01-08", "region": "midwest", "value": 55.0},
                {"date": "2026-01-08", "region": "pacific", "value": 12.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Regional Storage Change",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_change_weekly"},
        )

        payload = build_answer_with_openai(
            query="Show storage build by region.",
            result=result,
        )

        self.assertIn("Midwest posted the largest storage build", payload.answer_text)
        self.assertIsNotNone(payload.structured_response)
        self.assertEqual(payload.structured_response.data_points[0].metric, "Midwest")
        self.assertEqual(
            payload.chart_spec.title, "Weekly Change in Working Gas Storage by Region"
        )

    def test_regional_storage_change_can_rank_withdrawals(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-08", "region": "east", "value": -20.0},
                {"date": "2026-01-08", "region": "midwest", "value": -55.0},
                {"date": "2026-01-08", "region": "pacific", "value": -12.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Regional Storage Change",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_change_weekly"},
        )

        payload = build_answer_with_openai(
            query="Where are withdrawals happening fastest?",
            result=result,
        )

        self.assertIn("Midwest posted the fastest storage withdrawal", payload.answer_text)

    def test_storage_level_and_change_builds_combined_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-01", "value": 800.0, "weekly_change": None},
                {"date": "2026-01-08", "value": 850.0, "weekly_change": 50.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="East Storage Combined",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_lower48", "filters": {"region": "east"}},
        )

        payload = build_answer_with_openai(
            query="Compare East storage and weekly change together.",
            result=result,
        )

        self.assertIn("East storage was 850 Bcf", payload.answer_text)
        self.assertEqual(payload.chart_spec.title, "Working Gas in Storage and Weekly Change")


if __name__ == "__main__":
    unittest.main()
