from __future__ import annotations

import unittest
from datetime import datetime
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pandas as pd

from answer_builder import _is_suggested_alert_relevant, build_answer_with_openai
from schemas.answer import SourceRef
from tools.eia_adapter import EIAResult


class TestAnswerBuilder(unittest.TestCase):
    def test_storage_snapshot_query_rejects_deficit_widening_suggestion(self) -> None:
        self.assertFalse(
            _is_suggested_alert_relevant(
                signal_id="storage_deficit_widening_wow",
                metric="working_gas_storage_lower48",
                query="What is Lower 48 working gas storage right now?",
            )
        )

    def test_storage_deficit_query_allows_deficit_widening_suggestion(self) -> None:
        self.assertTrue(
            _is_suggested_alert_relevant(
                signal_id="storage_deficit_widening_wow",
                metric="working_gas_storage_lower48",
                query="Is the storage deficit widening week-over-week?",
            )
        )

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

    def test_sector_consumption_question_renders_latest_sector_bar_chart(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-01", "series": "commercial", "value": 200.0},
                {"date": "2026-01-01", "series": "residential", "value": 300.0},
                {"date": "2026-01-01", "series": "industrial", "value": 250.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Consumption by Sector",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "ng_consumption_by_sector"},
        )

        payload = build_answer_with_openai(
            query="Which sector consumes the most gas (commercial, residential, industrial)?",
            result=result,
        )
        self.assertIsNotNone(payload.chart_spec)
        self.assertEqual(payload.chart_spec.chart_type, "bar")
        self.assertEqual(payload.chart_spec.x, "sector")

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

    def test_regional_production_change_builds_contribution_ranking_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-04-01", "region": "tx", "value": 1000.0},
                {"date": "2026-05-01", "region": "tx", "value": 1060.0},
                {"date": "2026-04-01", "region": "la", "value": 800.0},
                {"date": "2026-05-01", "region": "la", "value": 790.0},
                {"date": "2026-04-01", "region": "pa", "value": 900.0},
                {"date": "2026-05-01", "region": "pa", "value": 920.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Production by Region",
                reference="test",
                retrieved_at=datetime(2026, 5, 22),
            ),
            meta={"metric": "ng_production_lower48", "filters": {"group_by": "region"}},
        )

        payload = build_answer_with_openai(
            query="Which state or region contributed most to the production change?",
            result=result,
        )

        self.assertIn("contributed most to the latest production change", payload.answer_text)
        self.assertIn("TX", payload.answer_text)
        self.assertIsNotNone(payload.chart_spec)
        self.assertEqual(payload.chart_spec.chart_type, "bar")
        self.assertEqual(payload.chart_spec.x, "region")

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

    def test_storage_same_week_last_year_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2025-01-09", "value": 2700.0, "weekly_change": 90.0},
                {"date": "2026-01-08", "value": 2850.0, "weekly_change": 80.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Lower 48 Storage Combined",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_lower48", "filters": {"region": "lower48"}},
        )
        payload = build_answer_with_openai(
            query="How does current storage compare to the same week last year?",
            result=result,
        )
        self.assertIn("same reporting week last year", payload.answer_text)
        self.assertIn("a change of 150 Bcf", payload.answer_text)

    def test_storage_default_region_label_is_lower48(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-01", "value": 2800.0},
                {"date": "2026-01-08", "value": 2850.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Lower 48 Storage",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_lower48"},
        )
        payload = build_answer_with_openai(
            query="How much storage is there?",
            result=result,
        )
        self.assertIn("Lower 48 storage was 2,850 Bcf", payload.answer_text)

    def test_storage_vs_five_year_average_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2021-01-07", "value": 3300.0, "weekly_change": 70.0},
                {"date": "2022-01-06", "value": 3200.0, "weekly_change": 60.0},
                {"date": "2023-01-05", "value": 3100.0, "weekly_change": 55.0},
                {"date": "2024-01-04", "value": 3000.0, "weekly_change": 50.0},
                {"date": "2025-01-09", "value": 2900.0, "weekly_change": 45.0},
                {"date": "2026-01-08", "value": 2800.0, "weekly_change": 40.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Lower 48 Storage Combined",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_lower48", "filters": {"region": "lower48"}},
        )
        payload = build_answer_with_openai(
            query="How does current storage compare to the five-year average?",
            result=result,
        )
        self.assertIn("five-year same-week average", payload.answer_text)
        self.assertIsNotNone(payload.chart_spec)
        self.assertEqual(payload.chart_spec.chart_type, "line")
        self.assertEqual(
            payload.chart_spec.title,
            "Working Gas in Storage: Same-Week Comparison (5Y + Current)",
        )

    def test_storage_tight_loose_neutral_vs_five_year_range_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2021-01-07", "value": 3300.0, "weekly_change": 70.0},
                {"date": "2022-01-06", "value": 3200.0, "weekly_change": 60.0},
                {"date": "2023-01-05", "value": 3100.0, "weekly_change": 55.0},
                {"date": "2024-01-04", "value": 3000.0, "weekly_change": 50.0},
                {"date": "2025-01-09", "value": 2900.0, "weekly_change": 45.0},
                {"date": "2026-01-08", "value": 2800.0, "weekly_change": 40.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Lower 48 Storage Combined",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_lower48", "filters": {"region": "lower48"}},
        )
        payload = build_answer_with_openai(
            query="Are inventories currently tight, loose, or neutral versus the five-year range?",
            result=result,
        )
        self.assertIn("inventories are tight", payload.answer_text)

    def test_storage_comparison_answer_works_without_weekly_change_column(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2021-01-07", "value": 3300.0},
                {"date": "2022-01-06", "value": 3200.0},
                {"date": "2023-01-05", "value": 3100.0},
                {"date": "2024-01-04", "value": 3000.0},
                {"date": "2025-01-09", "value": 2900.0},
                {"date": "2026-01-08", "value": 2800.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Lower 48 Storage",
                reference="test",
                retrieved_at=datetime(2026, 1, 22),
            ),
            meta={"metric": "working_gas_storage_lower48", "filters": {"region": "lower48"}},
        )
        payload = build_answer_with_openai(
            query="How does current storage compare to the five-year average?",
            result=result,
        )
        self.assertIn("five-year same-week average", payload.answer_text)

    def test_weather_answer_formats_as_of_date_human_readable(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "bucket": "days_1_5",
                    "bucket_start_day": 1,
                    "forecast_hdd": 12.0,
                    "normal_hdd_5y": 16.0,
                    "delta_hdd": -4.0,
                    "forecast_cdd": 5.0,
                    "normal_cdd_5y": 3.0,
                    "delta_cdd": 2.0,
                    "demand_delta_bcfd": -0.4,
                    "as_of": "2026-04-23T15:28:23Z",
                    "normal_years": 5,
                },
                {
                    "bucket": "days_6_10",
                    "bucket_start_day": 6,
                    "forecast_hdd": 11.0,
                    "normal_hdd_5y": 15.0,
                    "delta_hdd": -4.0,
                    "forecast_cdd": 6.0,
                    "normal_cdd_5y": 4.0,
                    "delta_cdd": 2.0,
                    "demand_delta_bcfd": -0.5,
                    "as_of": "2026-04-23T15:28:23Z",
                    "normal_years": 5,
                },
                {
                    "bucket": "days_11_15",
                    "bucket_start_day": 11,
                    "forecast_hdd": 10.0,
                    "normal_hdd_5y": 14.0,
                    "delta_hdd": -4.0,
                    "forecast_cdd": 7.0,
                    "normal_cdd_5y": 5.0,
                    "delta_cdd": 2.0,
                    "demand_delta_bcfd": -0.6,
                    "as_of": "2026-04-23T15:28:23Z",
                    "normal_years": 5,
                },
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Weather Degree Days Forecast vs 5-Year Normal (Lower 48)",
                reference="test",
                retrieved_at=datetime(2026, 4, 23),
            ),
            meta={"metric": "weather_degree_days_forecast_vs_5y"},
        )

        payload = build_answer_with_openai(
            query="How do current cooling/heating degree day forecasts compare to the 5-year average?",
            result=result,
        )

        self.assertIn("As of April 23, 2026", payload.answer_text)

    def test_weather_regional_drivers_question_renders_region_bar_chart(self) -> None:
        df = pd.DataFrame(
            [
                {"region": "east", "demand_delta_bcfd": 0.5, "total_delta_hdd": 10, "total_delta_cdd": -1, "date": "2026-05-10"},
                {"region": "midwest", "demand_delta_bcfd": 0.2, "total_delta_hdd": 5, "total_delta_cdd": 0, "date": "2026-05-10"},
                {"region": "south", "demand_delta_bcfd": -0.1, "total_delta_hdd": -2, "total_delta_cdd": 3, "date": "2026-05-10"},
                {"region": "west", "demand_delta_bcfd": -0.3, "total_delta_hdd": -4, "total_delta_cdd": 2, "date": "2026-05-10"},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Regional Weather Drivers",
                reference="test",
                retrieved_at=datetime(2026, 5, 10),
            ),
            meta={"metric": "weather_regional_demand_drivers"},
        )

        payload = build_answer_with_openai(
            query="Which regions are driving weather-related demand right now?",
            result=result,
        )
        self.assertIsNotNone(payload.chart_spec)
        self.assertEqual(payload.chart_spec.chart_type, "bar")
        self.assertEqual(payload.chart_spec.x, "region")
        self.assertEqual(payload.chart_spec.y, ["demand_delta_bcfd"])

    def test_henry_hub_average_last_7_days_answer(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-05-06", "value": 2.80},
                {"date": "2026-05-07", "value": 2.82},
                {"date": "2026-05-08", "value": 2.85},
                {"date": "2026-05-09", "value": 2.88},
                {"date": "2026-05-10", "value": 2.90},
                {"date": "2026-05-11", "value": 2.92},
                {"date": "2026-05-12", "value": 2.91},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Henry Hub Spot",
                reference="test",
                retrieved_at=datetime(2026, 5, 12),
            ),
            meta={"metric": "henry_hub_spot"},
        )

        payload = build_answer_with_openai(
            query="What was the average Henry Hub price over the last 7 days?",
            result=result,
        )
        self.assertIn("average Henry Hub price was", payload.answer_text)
        self.assertIn("Over the last 7 days", payload.answer_text)

    def test_latest_production_answer_auto_includes_five_year_context(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2021-05-01", "value": 98000.0},
                {"date": "2022-05-01", "value": 99000.0},
                {"date": "2023-05-01", "value": 100000.0},
                {"date": "2024-05-01", "value": 101000.0},
                {"date": "2025-05-01", "value": 102000.0},
                {"date": "2026-05-01", "value": 103000.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Production",
                reference="test",
                retrieved_at=datetime(2026, 5, 12),
            ),
            meta={"metric": "ng_production_lower48"},
        )
        payload = build_answer_with_openai(
            query="What is the latest U.S. marketed natural gas production?",
            result=result,
        )
        self.assertIn("5-year average", payload.answer_text)
        self.assertIn("5-year range", payload.answer_text)

    def test_latest_marketed_production_five_year_query_adds_average_line_chart(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2021-02-01", "value": 3000000.0},
                {"date": "2021-03-01", "value": 3020000.0},
                {"date": "2021-04-01", "value": 3040000.0},
                {"date": "2021-05-01", "value": 3060000.0},
                {"date": "2022-02-01", "value": 3010000.0},
                {"date": "2022-03-01", "value": 3030000.0},
                {"date": "2022-04-01", "value": 3050000.0},
                {"date": "2022-05-01", "value": 3070000.0},
                {"date": "2023-02-01", "value": 3020000.0},
                {"date": "2023-03-01", "value": 3040000.0},
                {"date": "2023-04-01", "value": 3060000.0},
                {"date": "2023-05-01", "value": 3080000.0},
                {"date": "2024-02-01", "value": 3030000.0},
                {"date": "2024-03-01", "value": 3050000.0},
                {"date": "2024-04-01", "value": 3070000.0},
                {"date": "2024-05-01", "value": 3090000.0},
                {"date": "2025-02-01", "value": 3040000.0},
                {"date": "2025-03-01", "value": 3060000.0},
                {"date": "2025-04-01", "value": 3080000.0},
                {"date": "2025-05-01", "value": 3100000.0},
                {"date": "2026-02-01", "value": 3050000.0},
                {"date": "2026-03-01", "value": 3070000.0},
                {"date": "2026-04-01", "value": 3090000.0},
                {"date": "2026-05-01", "value": 3110000.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Production",
                reference="test",
                retrieved_at=datetime(2026, 5, 12),
            ),
            meta={"metric": "ng_production_lower48"},
        )
        payload = build_answer_with_openai(
            query="What is the latest U.S. marketed natural gas production, and how does it compare to the same-time 5-year average and range?",
            result=result,
        )
        self.assertIsNotNone(payload.chart_spec)
        self.assertEqual(payload.chart_spec.chart_type, "line")
        self.assertEqual(payload.chart_spec.x, "date")
        self.assertEqual(payload.chart_spec.y, ["value", "five_year_average"])

    def test_latest_marketed_production_five_year_query_with_short_history_reports_gap(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2025-05-01", "value": 3300000.0},
                {"date": "2025-06-01", "value": 3220000.0},
                {"date": "2025-07-01", "value": 3350000.0},
                {"date": "2025-08-01", "value": 3370000.0},
                {"date": "2025-09-01", "value": 3240000.0},
                {"date": "2025-10-01", "value": 3320000.0},
                {"date": "2025-11-01", "value": 3305000.0},
                {"date": "2025-12-01", "value": 3460000.0},
                {"date": "2026-01-01", "value": 3363000.0},
                {"date": "2026-02-01", "value": 3080138.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Production",
                reference="test",
                retrieved_at=datetime(2026, 5, 12),
            ),
            meta={"metric": "ng_production_lower48"},
        )
        payload = build_answer_with_openai(
            query="What is the latest U.S. marketed natural gas production, and how does it compare to the same-time 5-year average and range?",
            result=result,
        )
        self.assertIn("Not enough same-time history was returned", payload.answer_text)

    def test_ng_electricity_seasonal_norms_answer_uses_seasonal_baseline(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2021-02-01", "value": 120000.0},
                {"date": "2022-02-01", "value": 125000.0},
                {"date": "2023-02-01", "value": 127000.0},
                {"date": "2024-02-01", "value": 129000.0},
                {"date": "2025-02-01", "value": 131000.0},
                {"date": "2026-02-01", "value": 132089.232},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="EIA Natural Gas: Electricity",
                reference="test",
                retrieved_at=datetime(2026, 2, 3),
            ),
            meta={"metric": "ng_electricity", "filters": {"normal_years": 5}},
        )

        payload = build_answer_with_openai(
            query="What is current natural gas power burn, and how does it compare to seasonal norms?",
            result=result,
        )

        self.assertIn("seasonal norm", payload.answer_text.lower())
        self.assertIn("Difference vs Norm", [dp.metric for dp in payload.structured_response.data_points])

    def test_weekly_energy_atlas_summary_answer_uses_four_block_format(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "date": "2026-04-24",
                    "weather_as_of": "2026-04-24T00:00:00Z",
                    "weather_demand_delta_bcfd": -0.45,
                    "weather_delta_hdd": -7.0,
                    "weather_delta_cdd": 3.0,
                    "storage_latest_bcf": 50.0,
                    "storage_expected_bcf": 30.0,
                    "storage_surprise_bcf": 20.0,
                    "lng_latest_mmcf": 125.0,
                    "lng_delta_mmcf": 5.0,
                    "production_latest_mmcf": 104300.0,
                    "production_delta_mmcf": 300.0,
                    "price_latest_usd_mmbtu": 2.81,
                    "price_delta_usd_mmbtu": 0.10,
                }
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="Energy Atlas Weekly Summary (Derived)",
                reference="test",
                retrieved_at=datetime(2026, 4, 24),
            ),
            meta={"metric": "weekly_energy_atlas_summary"},
        )

        payload = build_answer_with_openai(
            query="Give me this week's energy atlas summary.",
            result=result,
        )

        self.assertIn("Weather:", payload.answer_text)
        self.assertIn("Storage:", payload.answer_text)
        self.assertIn("LNG / Supply:", payload.answer_text)
        self.assertIn("Price:", payload.answer_text)
        self.assertIsNotNone(payload.chart_spec)
        self.assertEqual(payload.chart_spec.title, "Market Pressure Dashboard")

    def test_ng_electricity_proxy_uses_power_sector_plain_language(self) -> None:
        df = pd.DataFrame(
            [
                {"date": "2026-01-01", "series": "electric_power", "value": 129000.0},
                {"date": "2026-02-01", "series": "electric_power", "value": 132089.232},
                {"date": "2026-02-01", "series": "industrial", "value": 73000.0},
            ]
        )
        result = EIAResult(
            df=df,
            source=SourceRef(
                source_type="eia_api",
                label="EIA Natural Gas: Consumption by Sector",
                reference="test",
                retrieved_at=datetime(2026, 2, 3),
            ),
            meta={
                "metric": "ng_consumption_by_sector",
                "proxy_for_metric": "ng_electricity",
                "proxy_note": "Direct ng_electricity observations unavailable; using power-sector rows from consumption-by-sector as a proxy.",
            },
        )

        payload = build_answer_with_openai(
            query="How much natural gas did power plants use last month?",
            result=result,
        )

        self.assertIn("Power-sector natural gas use", payload.answer_text)
        self.assertIn("proxy", payload.answer_text.lower())
        self.assertIsNotNone(payload.structured_response)
        self.assertEqual(payload.structured_response.data_points[0].metric, "Power-Sector Gas Use")


if __name__ == "__main__":
    unittest.main()
