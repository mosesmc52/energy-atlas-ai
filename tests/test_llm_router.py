from __future__ import annotations

import unittest
from datetime import date

from agents.llm_router import STORAGE_REGIONS
from agents.router import route_query


class TestStorageRouting(unittest.TestCase):
    def test_current_working_gas_storage(self) -> None:
        route = route_query("What is current working gas in storage?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.value_type, "level")

    def test_east_region_gas_query_is_storage(self) -> None:
        route = route_query("How much gas is currently in the East region?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.regions, ["east"])
        self.assertEqual(route.analysis_type, "latest")

    def test_plot_east_storage_date_range(self) -> None:
        route = route_query("Plot East storage from 2020 to 2026")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.regions, ["east"])
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.date_expression, "from 2020 to 2026")
        self.assertEqual(route.start_date, "2020-01-01")
        self.assertEqual(route.end_date, "2026-12-31")

    def test_compare_two_regions_as_time_series(self) -> None:
        route = route_query("Compare East and Midwest storage since 2021")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.regions, ["east", "midwest"])
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.date_expression, "since 2021")
        self.assertEqual(route.start_date, "2021-01-01")
        self.assertEqual(route.end_date, date.today().isoformat())

    def test_compare_east_storage_month_name_range(self) -> None:
        route = route_query("Compare East storage from January 2020 through December 2023.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.regions, ["east"])
        self.assertEqual(route.start_date, "2020-01-01")
        self.assertEqual(route.end_date, "2023-12-31")

    def test_show_storage_over_last_five_years_implies_time_series(self) -> None:
        route = route_query("Show Lower 48 storage over the last 5 years.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.value_type, "level")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.output_mode, "chart_and_answer")
        self.assertEqual(route.start_date, "2021-06-02")
        self.assertEqual(route.end_date, date.today().isoformat())

    def test_how_has_midwest_storage_changed_since_2021_is_time_series(self) -> None:
        route = route_query("How has Midwest storage changed since 2021?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.regions, ["midwest"])
        self.assertEqual(route.value_type, "level")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.output_mode, "chart_and_answer")
        self.assertEqual(route.start_date, "2021-01-01")
        self.assertEqual(route.end_date, date.today().isoformat())

    def test_compare_current_storage_by_region(self) -> None:
        route = route_query("Compare current storage by region")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "regional_compare")
        self.assertEqual(route.regions, list(STORAGE_REGIONS))
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.output_mode, "chart_and_answer")

    def test_compare_storage_to_five_year_average(self) -> None:
        route = route_query("How does Lower 48 storage compare to the five-year average?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "seasonal_compare")
        self.assertIn("five_year_avg", route.comparisons)
        self.assertEqual(route.chart_type, "seasonal_line")

    def test_this_year_versus_seasonal_average_is_storage_seasonal_compare(self) -> None:
        route = route_query("Show this year versus the seasonal average.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "seasonal_compare")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.comparisons, ["five_year_avg"])

    def test_how_far_above_normal_is_deviation_from_five_year_average(self) -> None:
        route = route_query("How far above normal is East storage?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "deviation_from_normal")
        self.assertEqual(route.regions, ["east"])
        self.assertEqual(route.comparisons, ["five_year_avg"])

    def test_storage_deficit_shrinking_is_deviation_from_normal(self) -> None:
        route = route_query("Is the storage deficit shrinking?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "deviation_from_normal")
        self.assertEqual(route.value_type, "level")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.comparisons, ["five_year_avg"])

    def test_storage_surplus_widening_is_deviation_from_normal(self) -> None:
        route = route_query("Is the storage surplus widening?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "deviation_from_normal")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.comparisons, ["five_year_avg"])

    def test_storage_tightening_or_loosening_is_deviation_from_normal(self) -> None:
        route = route_query("Is storage tightening or loosening?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "deviation_from_normal")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.comparisons, ["five_year_avg"])

    def test_rank_regions_against_normal(self) -> None:
        route = route_query("Which region is most above normal?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.ranking_basis, "deviation_from_normal")
        self.assertIn("five_year_avg", route.comparisons)
        self.assertEqual(route.regions, list(STORAGE_REGIONS))
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.output_mode, "chart_and_answer")

    def test_rank_regions_by_storage_deficit_uses_deviation_basis(self) -> None:
        route = route_query("Rank regions by storage deficit.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.ranking_basis, "deviation_from_normal")
        self.assertIn("five_year_avg", route.comparisons)
        self.assertEqual(route.regions, list(STORAGE_REGIONS))
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.output_mode, "chart_and_answer")

    def test_which_region_is_most_below_normal_uses_deviation_basis(self) -> None:
        route = route_query("Which region is most below normal?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.ranking_basis, "deviation_from_normal")
        self.assertIn("five_year_avg", route.comparisons)
        self.assertEqual(route.regions, list(STORAGE_REGIONS))
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.output_mode, "chart_and_answer")

    def test_injected_this_week_by_region(self) -> None:
        route = route_query("How much gas was injected this week by region?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "regional_compare")
        self.assertEqual(route.value_type, "weekly_change")
        self.assertEqual(route.chart_type, "bar")

    def test_injections_accelerating(self) -> None:
        route = route_query("Are injections accelerating?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "weekly_change")
        self.assertEqual(route.value_type, "weekly_change")
        self.assertEqual(route.comparisons, ["prior_week"])
        self.assertEqual(route.chart_type, "line")

    def test_withdrawals_slowing_is_weekly_change(self) -> None:
        route = route_query("Are withdrawals slowing?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "weekly_change")
        self.assertEqual(route.value_type, "weekly_change")
        self.assertEqual(route.comparisons, ["prior_week"])
        self.assertEqual(route.chart_type, "line")

    def test_henry_hub_price_is_unsupported(self) -> None:
        route = route_query("What is the current Henry Hub price?")

        self.assertEqual(route.domain, "unsupported")


if __name__ == "__main__":
    unittest.main()
