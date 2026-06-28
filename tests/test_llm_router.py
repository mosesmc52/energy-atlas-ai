from __future__ import annotations

import unittest
from datetime import date
import pandas as pd

from agents.llm_router import STORAGE_REGIONS, UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS
from agents.router import context_from_route, route_query


class TestStorageRouting(unittest.TestCase):
    def test_current_working_gas_storage(self) -> None:
        route = route_query("What is current working gas in storage?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "weekly_working_gas")
        self.assertEqual(route.storage_frequency, "weekly")
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
        self.assertEqual(route.storage_dataset, "weekly_working_gas")
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
        self.assertEqual(route.start_date, (pd.Timestamp(date.today().isoformat()) - pd.DateOffset(years=5)).date().isoformat())
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

    def test_monthly_working_gas_in_texas_since_2018_uses_all_operators(self) -> None:
        route = route_query("Show monthly working gas in storage in Texas since 2018.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")

    def test_compare_monthly_injections_in_texas_and_louisiana_since_2020(self) -> None:
        route = route_query("Compare monthly injections in Texas and Louisiana since 2020.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "injections")
        self.assertEqual(route.states, ["tx", "la"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")

    def test_which_state_has_most_base_gas_in_storage(self) -> None:
        route = route_query("Which state has the most base gas in storage?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "base_gas")
        self.assertEqual(route.states, [])
        self.assertTrue(route.states_all)
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.chart_type, "bar")

    def test_rank_states_by_working_gas_storage_excludes_united_states_total(self) -> None:
        route = route_query("Rank states by working gas storage.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.states, [])
        self.assertTrue(route.states_all)

    def test_texas_working_gas_storage_uses_single_state(self) -> None:
        route = route_query("Texas working gas storage.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.states, ["tx"])
        self.assertFalse(route.states_all)

    def test_texas_and_louisiana_injections_uses_requested_states(self) -> None:
        route = route_query("Texas and Louisiana injections.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.states, ["tx", "la"])
        self.assertFalse(route.states_all)

    def test_how_full_is_texas_storage_routes_to_utilization_insight(self) -> None:
        route = route_query("How full is Texas storage?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "explain")
        self.assertEqual(route.storage_insight_type, "storage_utilization")
        self.assertEqual(route.primary_metric, "storage_utilization")
        self.assertEqual(route.states, ["tx"])

    def test_least_remaining_capacity_by_region_routes_to_remaining_capacity_insight(self) -> None:
        route = route_query("Which region has the least remaining storage capacity?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "explain")
        self.assertEqual(route.storage_insight_type, "remaining_capacity")
        self.assertEqual(route.primary_metric, "storage_remaining_capacity")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.regions, list(UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS))

    def test_largest_average_storage_field_size_routes_to_capacity_per_field(self) -> None:
        route = route_query("Which state has the largest average storage field size?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "explain")
        self.assertEqual(route.storage_insight_type, "capacity_per_field")
        self.assertEqual(route.primary_metric, "storage_capacity_per_field")
        self.assertTrue(route.states_all)

    def test_lower48_historical_max_routes_to_storage_explain(self) -> None:
        route = route_query("Is Lower 48 storage near its historical maximum?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "explain")
        self.assertEqual(route.storage_insight_type, "historical_max_compare")
        self.assertEqual(route.primary_metric, "storage_historical_max_compare")
        self.assertEqual(route.regions, ["lower48"])

    def test_weekly_storage_report_analysis_routes_to_report_card(self) -> None:
        route = route_query("Analyze this week's storage report.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "explain")
        self.assertEqual(route.storage_insight_type, "weekly_report_card")
        self.assertEqual(route.primary_metric, "storage_weekly_report_card")
        self.assertEqual(route.storage_dataset, "weekly_working_gas")
        self.assertEqual(route.regions, ["lower48"])

    def test_followup_inherits_storage_utilization_and_replaces_state(self) -> None:
        previous = route_query("How full is Texas storage?")
        route = route_query("What about Louisiana?", previous_context=previous)

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.analysis_type, "explain")
        self.assertEqual(route.storage_insight_type, "storage_utilization")
        self.assertEqual(route.states, ["la"])

    def test_followup_replaces_lower48_with_east_and_keeps_time_window(self) -> None:
        previous = route_query("Show Lower 48 storage over the last 5 years.")
        route = route_query("What about East?", previous_context=context_from_route(previous))

        self.assertEqual(route.storage_dataset, "weekly_working_gas")
        self.assertEqual(route.regions, ["east"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.start_date, previous.start_date)
        self.assertEqual(route.end_date, previous.end_date)

    def test_followup_adds_five_year_average_context(self) -> None:
        previous = route_query("Compare East and Midwest storage since 2021.")
        route = route_query("Show that versus the 5-year average.", previous_context=previous)

        self.assertEqual(route.storage_dataset, "weekly_working_gas")
        self.assertEqual(route.regions, ["east", "midwest"])
        self.assertIn("five_year_avg", route.comparisons)
        self.assertEqual(route.analysis_type, "seasonal_compare")

    def test_followup_switches_metric_to_field_count_and_keeps_ranking_scope(self) -> None:
        previous = route_query("Which state has the most working gas storage capacity?")
        route = route_query("What about field count?", previous_context=previous)

        self.assertEqual(route.storage_metric_type, "storage_field_count")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertTrue(route.states_all)

    def test_followup_replaces_storage_type(self) -> None:
        previous = route_query("What is working gas storage in salt caverns?")
        route = route_query("What about aquifers?", previous_context=previous)

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_type, "aquifer")
        self.assertEqual(route.analysis_type, "latest")

    def test_followup_turns_weekly_report_into_time_series(self) -> None:
        previous = route_query("Analyze this week's storage report.")
        route = route_query("Show it over time.", previous_context=previous)

        self.assertEqual(route.storage_dataset, "weekly_working_gas")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")

    def test_clear_new_question_does_not_inherit_previous_geography(self) -> None:
        previous = route_query("How full is Texas storage?")
        route = route_query("Show Lower 48 storage over the last 5 years.", previous_context=previous)

        self.assertEqual(route.storage_dataset, "weekly_working_gas")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.analysis_type, "time_series")

    def test_total_us_base_gas_in_storage_uses_national_series_only(self) -> None:
        route = route_query("What is total U.S. base gas in storage?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_metric_type, "base_gas")
        self.assertEqual(route.states, ["united_states_total"])
        self.assertFalse(route.states_all)

    def test_national_working_gas_storage_since_2018_uses_national_series_only(self) -> None:
        route = route_query("Show national working gas storage since 2018.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.states, ["united_states_total"])
        self.assertFalse(route.states_all)

    def test_working_gas_pct_change_from_year_ago_in_pennsylvania(self) -> None:
        route = route_query("What is working gas % change from year ago in Pennsylvania?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "working_gas_yoy_pct_change")
        self.assertEqual(route.states, ["pa"])
        self.assertFalse(route.states_all)
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.chart_type, "none")
        self.assertEqual(route.output_mode, "answer")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_yoy_pct_change_monthly")
        self.assertEqual(route.comparisons, ["none"])

    def test_working_gas_volume_change_from_year_ago_in_texas_is_yoy_latest(self) -> None:
        route = route_query("What is the working gas volume change from a year ago in Texas?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "working_gas_yoy_volume_change")
        self.assertEqual(route.states, ["tx"])
        self.assertFalse(route.states_all)
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.chart_type, "none")
        self.assertEqual(route.output_mode, "answer")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_yoy_volume_change_monthly")
        self.assertEqual(route.comparisons, ["none"])

    def test_which_state_has_largest_year_over_year_increase_in_working_gas(self) -> None:
        route = route_query("Which state has the largest year-over-year increase in working gas?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "working_gas_yoy_volume_change")
        self.assertEqual(route.states, [])
        self.assertTrue(route.states_all)
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.output_mode, "chart_and_answer")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_yoy_volume_change_monthly")
        self.assertEqual(route.comparisons, ["none"])

    def test_which_state_has_largest_percent_increase_in_working_gas(self) -> None:
        route = route_query("Which state has the largest percent increase in working gas?")

        self.assertEqual(route.storage_metric_type, "working_gas_yoy_pct_change")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.states, [])
        self.assertTrue(route.states_all)
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_yoy_pct_change_monthly")
        self.assertEqual(route.comparisons, ["none"])

    def test_working_gas_change_from_year_ago_in_texas_since_2015_is_yoy_time_series(self) -> None:
        route = route_query("Show working gas change from year ago in Texas since 2015.")

        self.assertEqual(route.storage_metric_type, "working_gas_yoy_volume_change")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_yoy_volume_change_monthly")
        self.assertEqual(route.comparisons, ["none"])

    def test_plot_working_gas_percent_change_from_year_ago_in_pennsylvania_since_2018(self) -> None:
        route = route_query("Plot working gas percent change from year ago in Pennsylvania since 2018.")

        self.assertEqual(route.storage_metric_type, "working_gas_yoy_pct_change")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.states, ["pa"])
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_yoy_pct_change_monthly")
        self.assertEqual(route.comparisons, ["none"])

    def test_compare_working_gas_storage_by_type(self) -> None:
        route = route_query("Compare working gas storage by type.")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertIsNone(route.storage_type)
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "regional_compare")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.output_mode, "chart_and_answer")
        self.assertEqual(route.primary_metric, "underground_storage_by_type_working_gas_monthly")
        self.assertEqual(
            route.filters,
            {
                "storage_dataset": "underground_storage_by_type",
                "storage_frequency": "monthly",
                "storage_metric_type": "working_gas",
                "storage_type": None,
                "storage_types_all": True,
            },
        )

    def test_rank_storage_types_by_base_gas_storage(self) -> None:
        route = route_query("Rank storage types by base gas storage.")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_metric_type, "base_gas")
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.primary_metric, "underground_storage_by_type_base_gas_monthly")

    def test_working_gas_storage_in_salt_caverns(self) -> None:
        route = route_query("What is working gas storage in salt caverns?")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertEqual(route.storage_type, "salt_cavern")
        self.assertFalse(route.storage_types_all)
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.chart_type, "none")
        self.assertEqual(route.output_mode, "answer")

    def test_salt_cavern_working_gas_storage_since_2015(self) -> None:
        route = route_query("Show salt cavern working gas storage since 2015.")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertEqual(route.storage_type, "salt_cavern")
        self.assertFalse(route.storage_types_all)
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.start_date, "2015-01-01")

    def test_working_gas_storage_by_type_since_2015(self) -> None:
        route = route_query("Show working gas storage by type since 2015.")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertIsNone(route.storage_type)
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.start_date, "2015-01-01")

    def test_show_monthly_working_gas_storage_by_type_is_time_series(self) -> None:
        route = route_query("Show monthly working gas storage by type.")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertIsNone(route.storage_type)
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.output_mode, "chart_and_answer")
        self.assertEqual(route.primary_metric, "underground_storage_by_type_working_gas_monthly")

    def test_aquifer_base_gas_storage(self) -> None:
        route = route_query("What is aquifer base gas storage?")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_metric_type, "base_gas")
        self.assertEqual(route.storage_type, "aquifer")
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.primary_metric, "underground_storage_by_type_base_gas_monthly")

    def test_compare_annual_injections_by_storage_type(self) -> None:
        route = route_query("Compare annual injections by storage type.")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "injections")
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "regional_compare")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.primary_metric, "underground_storage_by_type_injections_annual")

    def test_how_much_gas_is_stored_in_aquifers_routes_to_storage_by_type(self) -> None:
        route = route_query("How much gas is stored in aquifers?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertEqual(route.storage_type, "aquifer")
        self.assertFalse(route.storage_types_all)
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.chart_type, "none")
        self.assertEqual(route.output_mode, "answer")
        self.assertEqual(route.primary_metric, "underground_storage_by_type_working_gas_monthly")

    def test_which_storage_type_withdrew_the_most_gas_uses_withdrawals_ranking(self) -> None:
        route = route_query("Which storage type withdrew the most gas?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertEqual(route.storage_metric_type, "withdrawals")
        self.assertIsNone(route.storage_type)
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.output_mode, "chart_and_answer")
        self.assertEqual(route.primary_metric, "underground_storage_by_type_withdrawals_monthly")

    def test_storage_by_type_changed_over_time_is_time_series(self) -> None:
        route = route_query("How has storage by type changed over time?")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.output_mode, "chart_and_answer")

    def test_plot_storage_type_history_is_time_series(self) -> None:
        route = route_query("Plot storage type history.")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.output_mode, "chart_and_answer")

    def test_compare_storage_types_since_2015_is_time_series(self) -> None:
        route = route_query("Compare storage types since 2015.")

        self.assertEqual(route.storage_dataset, "underground_storage_by_type")
        self.assertTrue(route.storage_types_all)
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.output_mode, "chart_and_answer")
        self.assertEqual(route.start_date, "2015-01-01")

    def test_total_underground_storage_capacity_in_texas(self) -> None:
        route = route_query("What is total underground storage capacity in Texas?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_metric_type, "total_capacity")
        self.assertEqual(route.storage_frequency, "monthly")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.chart_type, "none")
        self.assertEqual(route.primary_metric, "underground_storage_total_capacity_monthly")

    def test_texas_working_gas_storage_capacity_since_2015(self) -> None:
        route = route_query("Show Texas working gas storage capacity since 2015.")

        self.assertEqual(route.storage_metric_type, "working_gas_capacity")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_capacity_monthly")

    def test_annual_working_gas_storage_capacity_in_texas(self) -> None:
        route = route_query("What is annual working gas storage capacity in Texas?")

        self.assertEqual(route.storage_metric_type, "working_gas_capacity")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_capacity_annual")

    def test_which_state_has_most_working_gas_storage_capacity(self) -> None:
        route = route_query("Which state has the most working gas storage capacity?")

        self.assertEqual(route.storage_metric_type, "working_gas_capacity")
        self.assertTrue(route.states_all)
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_capacity_monthly")

    def test_compare_storage_capacity_by_region(self) -> None:
        route = route_query("Compare storage capacity by region.")

        self.assertEqual(route.storage_metric_type, "total_capacity")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.regions, list(UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS))
        self.assertEqual(route.analysis_type, "regional_compare")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.primary_metric, "underground_storage_total_capacity_monthly")

    def test_compare_monthly_storage_field_count_by_region_uses_supported_regions(self) -> None:
        route = route_query("Compare monthly storage field count by region.")

        self.assertEqual(route.storage_metric_type, "storage_field_count")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.regions, list(UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS))
        self.assertEqual(route.analysis_type, "regional_compare")
        self.assertEqual(route.primary_metric, "underground_storage_field_count_monthly")

    def test_which_region_has_most_storage_capacity_uses_supported_regions(self) -> None:
        route = route_query("Which region has the most storage capacity?")

        self.assertEqual(route.storage_metric_type, "total_capacity")
        self.assertEqual(route.regions, list(UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS))
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.primary_metric, "underground_storage_total_capacity_monthly")

    def test_rank_regions_by_working_gas_storage_capacity_uses_supported_regions(self) -> None:
        route = route_query("Rank regions by working gas storage capacity.")

        self.assertEqual(route.storage_metric_type, "working_gas_capacity")
        self.assertEqual(route.regions, list(UNDERGROUND_STORAGE_CAPACITY_COUNT_REGIONS))
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.primary_metric, "underground_storage_working_gas_capacity_monthly")

    def test_lower_48_storage_field_count(self) -> None:
        route = route_query("How many underground storage fields are in the Lower 48?")

        self.assertEqual(route.storage_metric_type, "storage_field_count")
        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.chart_type, "none")
        self.assertEqual(route.primary_metric, "underground_storage_field_count_monthly")

    def test_lower_48_storage_field_count_since_2020(self) -> None:
        route = route_query("Show Lower 48 storage field count since 2020.")

        self.assertEqual(route.storage_metric_type, "storage_field_count")
        self.assertEqual(route.regions, ["lower48"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.primary_metric, "underground_storage_field_count_monthly")

    def test_rank_states_by_number_of_storage_fields(self) -> None:
        route = route_query("Rank states by number of storage fields.")

        self.assertEqual(route.storage_metric_type, "storage_field_count")
        self.assertTrue(route.states_all)
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.chart_type, "bar")
        self.assertEqual(route.primary_metric, "underground_storage_field_count_monthly")

    def test_us_lng_storage_additions(self) -> None:
        route = route_query("What are U.S. LNG storage additions?")

        self.assertEqual(route.domain, "storage")
        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "lng_storage_additions")
        self.assertEqual(route.states, ["united_states_total"])
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.chart_type, "none")
        self.assertEqual(route.primary_metric, "lng_storage_additions_annual")

    def test_us_lng_storage_additions_since_2020(self) -> None:
        route = route_query("Show U.S. LNG storage additions since 2020.")

        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_metric_type, "lng_storage_additions")
        self.assertEqual(route.states, ["united_states_total"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")
        self.assertEqual(route.primary_metric, "lng_storage_additions_annual")

    def test_texas_lng_storage_withdrawals(self) -> None:
        route = route_query("What are Texas LNG storage withdrawals?")

        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "lng_storage_withdrawals")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.primary_metric, "lng_storage_withdrawals_annual")

    def test_texas_lng_storage_withdrawals_since_2020(self) -> None:
        route = route_query("Show Texas LNG storage withdrawals since 2020.")

        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "lng_storage_withdrawals")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")

    def test_us_lng_storage_net_withdrawals(self) -> None:
        route = route_query("What are U.S. LNG storage net withdrawals?")

        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "lng_storage_net_withdrawals")
        self.assertEqual(route.states, ["united_states_total"])
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.primary_metric, "lng_storage_net_withdrawals_annual")

    def test_compare_texas_and_louisiana_lng_storage_withdrawals_since_2020(self) -> None:
        route = route_query("Compare Texas and Louisiana LNG storage withdrawals since 2020.")

        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "lng_storage_withdrawals")
        self.assertEqual(route.states, ["tx", "la"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")

    def test_compare_lng_storage_additions_in_texas_and_louisiana_defaults_to_time_series(self) -> None:
        route = route_query("Compare LNG storage additions in Texas and Louisiana.")

        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "lng_storage_additions")
        self.assertEqual(route.states, ["tx", "la"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.chart_type, "line")

    def test_which_state_has_the_most_lng_storage_additions_expands_states(self) -> None:
        route = route_query("Which state has the most LNG storage additions?")

        self.assertEqual(route.storage_dataset, "lng_storage")
        self.assertEqual(route.storage_frequency, "annual")
        self.assertEqual(route.storage_metric_type, "lng_storage_additions")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.chart_type, "bar")
        self.assertTrue(route.states_all)
        self.assertEqual(route.states, [])

    def test_underground_storage_withdrawals_in_texas_not_lng(self) -> None:
        route = route_query("What are underground storage withdrawals in Texas?")

        self.assertEqual(route.storage_dataset, "underground_storage_all_operators")
        self.assertEqual(route.storage_metric_type, "withdrawals")

    def test_weekly_storage_withdrawn_not_lng(self) -> None:
        route = route_query("How much gas was withdrawn from storage this week?")

        self.assertEqual(route.storage_dataset, "weekly_working_gas")
        self.assertEqual(route.storage_metric_type, "working_gas")
        self.assertEqual(route.value_type, "weekly_change")


if __name__ == "__main__":
    unittest.main()
