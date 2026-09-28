from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import Mock

import pandas as pd

from agents.router import route_query
from answer_builder import build_answer_with_openai
from charts.plotly_renderer import render_plotly
from executer import MetricExecutor
from schemas.answer import SourceRef
from tools.eia_adapter import EIAResult


def _result(rows: list[dict]) -> EIAResult:
    return EIAResult(
        df=pd.DataFrame(rows),
        source=SourceRef(
            source_type="eia_api",
            label="EIA natural gas end use",
            reference="eia-ng-client:natural_gas.consumption.end_use",
            retrieved_at=datetime(2026, 9, 27),
        ),
        meta={},
    )


class TestConsumptionAnswers(unittest.TestCase):
    def test_power_burn_in_california_since_2015_plots_monthly_history(self) -> None:
        question = "Plot power burn in California since 2015."
        route = route_query(question)
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.primary_metric, "natural_gas_electric_power_consumption_monthly")
        self.assertEqual(route.states, ["ca"])
        self.assertEqual(route.start_date, "2015-01-01")
        self.assertEqual(route.chart_type, "line")

        eia = Mock()
        eia.consumption_end_use.return_value = _result([
            {"date": "2015-01-01", "value": 100, "state": "ca"},
            {"date": "2026-06-01", "value": 150, "state": "ca"},
        ])
        result = MetricExecutor(eia=eia).execute_consumption_route(route)
        self.assertEqual(eia.consumption_end_use.call_args.kwargs["state"], "ca")
        self.assertEqual(eia.consumption_end_use.call_args.kwargs["type"], "electric_power")
        self.assertEqual(eia.consumption_end_use.call_args.kwargs["start"], "2015-01-01")

        payload = build_answer_with_openai(query=question, result=result, route=route)
        self.assertEqual(payload.chart_spec.chart_type, "line")
        self.assertEqual(len(payload.chart_data_preview.rows), 2)
        self.assertIn("CA Electric Power consumption changed", payload.answer_text)

    def test_state_ranking_fetches_five_year_baseline_history(self) -> None:
        route = route_query("Rank states by electric power gas consumption.")
        eia = Mock()
        eia.consumption_end_use.return_value = _result([
            {"date": "2024-12-01", "value": 100, "state": "tx"}
        ])
        MetricExecutor(eia=eia).execute_consumption_route(route)
        expected_start = (
            pd.Timestamp(route.end_date) - pd.DateOffset(years=8)
        ).date().isoformat()
        self.assertEqual(
            eia.consumption_end_use.call_args_list[0].kwargs["start"],
            expected_start,
        )

    def test_all_major_sectors_are_retrieved_and_charted(self) -> None:
        question = "Show U.S. natural gas consumption by sector."
        route = route_query(question)
        values = {
            "residential": 100,
            "commercial": 50,
            "industrial": 200,
            "electric_power": 300,
        }
        eia = Mock()

        def fetch(*, type: str, **kwargs):
            return _result([{
                "date": "2026-06-01", "value": values[type],
                "state": "us_total", "type": type,
            }])

        eia.consumption_end_use.side_effect = fetch
        result = MetricExecutor(eia=eia).execute_consumption_route(route)
        self.assertEqual(eia.consumption_end_use.call_count, 4)
        self.assertEqual(set(result.df["sector"]), set(values))
        payload = build_answer_with_openai(query=question, result=result, route=route)
        self.assertEqual(len(payload.chart_data_preview.rows), 4)
        self.assertEqual(payload.chart_spec.title, "Natural Gas Consumption by Sector")
        chart_df = pd.DataFrame(
            payload.chart_data_preview.rows, columns=payload.chart_data_preview.columns
        )
        figure = render_plotly(payload.chart_spec, chart_df)
        self.assertEqual(
            set(figure.data[0].x),
            {"Residential", "Commercial", "Industrial", "Electric Power"},
        )
        self.assertEqual(len(figure.data[0].y), 4)
        for sector in values:
            self.assertIn(sector.replace("_", " ").title(), payload.answer_text)
        self.assertNotIn("No common reporting month", payload.answer_text)

        rank_question = "Which sector uses the least natural gas nationally?"
        ranking = build_answer_with_openai(
            query=rank_question, result=result, route=route_query(rank_question)
        )
        self.assertEqual(len(ranking.chart_data_preview.rows), 4)
        self.assertIn("Least consumption", ranking.answer_text)
        self.assertIn("Commercial 50.0 MMcf; Residential 100 MMcf", ranking.answer_text)

    def test_rank_states_uses_same_month_and_values_for_each_state(self) -> None:
        route = route_query("Rank states by commercial natural gas consumption.")
        result = _result([
            {"date": "2026-06-01", "value": 100, "state": "ca", "sector": "commercial"},
            {"date": "2026-06-01", "value": 150, "state": "ny", "sector": "commercial"},
            {"date": "2026-05-01", "value": 200, "state": "tx", "sector": "commercial"},
        ])
        payload = build_answer_with_openai(
            query="Rank states by commercial natural gas consumption.",
            result=result,
            route=route,
        )
        self.assertIn("- 1. NY 150 MMcf", payload.answer_text)
        self.assertIn("- 2. CA 100 MMcf", payload.answer_text)
        self.assertNotIn("TX 200", payload.answer_text)
        self.assertEqual(len(payload.structured_response.data_points), 2)

    def test_state_ranking_summary_shows_only_top_and_bottom_five(self) -> None:
        question = "Rank states by electric power gas consumption."
        states = ["tx", "fl", "pa", "oh", "ca", "ny", "nc", "al", "ms", "va", "az", "ga"]
        result = _result([
            {
                "date": "2024-12-01",
                "value": (len(states) - index) * 100,
                "state": state,
                "sector": "electric_power",
            }
            for index, state in enumerate(states)
        ] + [
            {
                "date": f"{year}-12-01",
                "value": (len(states) - index) * 100 - 100,
                "state": state,
                "sector": "electric_power",
            }
            for year in range(2019, 2024)
            for index, state in enumerate(states)
        ])
        payload = build_answer_with_openai(
            query=question, result=result, route=route_query(question)
        )
        summary = payload.structured_response.summary
        self.assertIn("**Top 5**\n\n- 1. TX 1,200 MMcf", summary)
        self.assertNotIn("**Middle**", summary)
        self.assertNotIn("NY 700 MMcf", summary)
        self.assertNotIn("NC 600 MMcf", summary)
        self.assertIn("**Bottom 5**\n\n- 8. AL 500 MMcf", summary)
        self.assertIn("- 12. GA 100 MMcf", summary)
        self.assertEqual(summary.count("\n- "), 10)
        self.assertEqual(len(payload.chart_data_preview.rows), len(states))
        self.assertEqual(len(payload.structured_response.data_points), len(states))
        table = payload.ranking_table
        self.assertIn(
            "| Latest total (MMcf) | Top (MMcf) | Bottom (MMcf) | "
            "Middle / median (MMcf) | 5-year average (MMcf) |",
            table,
        )
        self.assertIn("| 7,800 | 1,200 | 100 | 650 | 6,600 |", table)
        self.assertEqual(table.count("\n| "), 3)
        self.assertNotIn("| TX |", table)
        self.assertNotIn("| GA |", table)
        self.assertIn("same-month totals", table)

    def test_state_ranking_prefers_recent_month_with_near_complete_coverage(self) -> None:
        question = "Rank states by electric power gas consumption."
        states = ["tx", "fl", "pa", "oh", "ca", "ny", "nc", "al", "ms", "va", "az", "ga"]
        result = _result([
            {"date": "2024-12-01", "value": 100, "state": state, "sector": "electric_power"}
            for state in states
        ] + [
            {"date": "2026-06-01", "value": 200, "state": state, "sector": "electric_power"}
            for state in states[:-1]
        ])
        payload = build_answer_with_openai(
            query=question, result=result, route=route_query(question)
        )
        self.assertIn("2026-06-01", payload.answer_text)
        self.assertEqual(len(payload.chart_data_preview.rows), 11)

    def test_sector_comparison_executes_both_metrics(self) -> None:
        route = route_query("Compare residential and commercial natural gas consumption in California.")
        eia = Mock()

        def fetch(*, type: str, **kwargs):
            return _result([{
                "date": "2026-06-01",
                "value": 20 if type == "residential" else 40,
                "state": "ca",
                "type": type,
            }])

        eia.consumption_end_use.side_effect = fetch
        result = MetricExecutor(eia=eia).execute_consumption_route(route)
        self.assertEqual(eia.consumption_end_use.call_count, 2)
        self.assertEqual(set(result.df["sector"]), {"residential", "commercial"})
        payload = build_answer_with_openai(
            query="Compare residential and commercial natural gas consumption in California.",
            result=result,
            route=route,
        )
        self.assertIn("Residential 20.0 MMcf", payload.answer_text)
        self.assertIn("Commercial 40.0 MMcf", payload.answer_text)

    def test_vehicle_and_commercial_comparison(self) -> None:
        route = route_query("Compare vehicle and commercial gas use in New York.")
        result = _result([
            {"date": "2026-06-01", "value": 58, "state": "ny", "sector": "vehicle"},
            {"date": "2026-06-01", "value": 120, "state": "ny", "sector": "commercial"},
        ])
        payload = build_answer_with_openai(
            query="Compare vehicle and commercial gas use in New York.",
            result=result, route=route,
        )
        self.assertIn("Vehicle 58.0 MMcf", payload.answer_text)
        self.assertIn("Commercial 120 MMcf", payload.answer_text)

    def test_business_geography_comparison_uses_both_states(self) -> None:
        route = route_query("Compare business natural gas consumption in New York and California.")
        result = _result([
            {"date": "2026-06-01", "value": 80, "state": "ny", "sector": "commercial"},
            {"date": "2026-06-01", "value": 120, "state": "ca", "sector": "commercial"},
        ])
        payload = build_answer_with_openai(
            query="Compare business natural gas consumption in New York and California.",
            result=result, route=route,
        )
        self.assertIn("NY 80.0 MMcf", payload.answer_text)
        self.assertIn("CA 120 MMcf", payload.answer_text)

    def test_electric_power_state_ranking(self) -> None:
        for question in (
            "Rank states by electric power gas consumption.",
            "Rank states by electric utility natural gas consumption",
        ):
            with self.subTest(question=question):
                route = route_query(question)
                result = _result([
                    {"date": "2026-06-01", "value": 90, "state": "tx", "sector": "electric_power"},
                    {"date": "2026-06-01", "value": 70, "state": "ca", "sector": "electric_power"},
                ])
                payload = build_answer_with_openai(query=question, result=result, route=route)
                self.assertIn("- 1. TX 90.0 MMcf", payload.answer_text)
                self.assertIn("- 2. CA 70.0 MMcf", payload.answer_text)

    def test_annual_undated_query_fetches_completed_years(self) -> None:
        route = route_query("Show annual total U.S. gas consumption.")
        eia = Mock()
        eia.consumption_total_us.return_value = _result([
            {"date": "2024-01-01", "value": 100, "state": "us_total"},
            {"date": "2025-01-01", "value": 110, "state": "us_total"},
        ])
        result = MetricExecutor(eia=eia).execute_consumption_route(route)
        self.assertLess(eia.consumption_total_us.call_args.kwargs["start"], "2024-01-01")
        eia.consumption_end_use.assert_not_called()
        payload = build_answer_with_openai(
            query="Show annual total U.S. gas consumption.", result=result, route=route
        )
        self.assertIn("2024: 100 MMcf", payload.answer_text)
        self.assertIn("2025: 110 MMcf", payload.answer_text)

    def test_historical_change_uses_same_calendar_month(self) -> None:
        route = route_query("How has gas use in homes changed since 2015?")
        result = _result([
            {"date": "2015-01-01", "value": 300, "state": "us_total", "sector": "residential"},
            {"date": "2015-06-01", "value": 100, "state": "us_total", "sector": "residential"},
            {"date": "2026-06-01", "value": 150, "state": "us_total", "sector": "residential"},
        ])
        payload = build_answer_with_openai(
            query="How has gas use in homes changed since 2015?", result=result, route=route
        )
        self.assertIn("June 2015", payload.answer_text)
        self.assertIn("50 MMcf", payload.answer_text)

    def test_unusualness_uses_same_month_history(self) -> None:
        route = route_query("Is U.S. total consumption unusually high for this month?")
        result = _result([
            {"date": f"{year}-06-01", "value": 100, "state": "us_total", "sector": "total"}
            for year in range(2021, 2026)
        ] + [
            {"date": "2026-06-01", "value": 140, "state": "us_total", "sector": "total"}
        ])
        payload = build_answer_with_openai(
            query="Is U.S. total consumption unusually high for this month?",
            result=result,
            route=route,
        )
        self.assertIn("above the same-month 5-year average", payload.answer_text)
        self.assertIn("40.0 MMcf", payload.answer_text)


if __name__ == "__main__":
    unittest.main()
