import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from agents.router import LLMRouteOutput
from agents.router import route_query


class TestRouter(unittest.TestCase):
    def test_production_growth_query_stays_on_rule_route(self) -> None:
        result = route_query("Is production growing year over year?")
        self.assertEqual(result.intent, "single_metric")
        self.assertEqual(result.primary_metric, "ng_production_lower48")
        self.assertEqual(result.metrics, ["ng_production_lower48"])
        self.assertEqual(result.source, "rule")
        self.assertEqual(result.filters, {"region": "united_states_total"})

    def test_production_query_routes_allowed_state_filter(self) -> None:
        result = route_query("Is production growing year over year in TX?")
        self.assertEqual(result.primary_metric, "ng_production_lower48")
        self.assertEqual(result.filters, {"region": "tx"})

    def test_consumption_query_routes_allowed_state_filter(self) -> None:
        result = route_query("How is gas consumption in California?")
        self.assertEqual(result.primary_metric, "ng_consumption_lower48")
        self.assertEqual(result.filters, {"region": "ca"})

    def test_imports_query_routes_country_filter(self) -> None:
        result = route_query("Are imports from Qatar rising?")
        self.assertEqual(result.primary_metric, "lng_imports")
        self.assertEqual(result.filters, {"region": "qatar"})

    def test_exports_query_routes_country_filter(self) -> None:
        result = route_query("Are exports to Japan higher than last year?")
        self.assertEqual(result.primary_metric, "lng_exports")
        self.assertEqual(result.filters, {"region": "japan"})

    def test_imports_query_routes_compressed_total_filter(self) -> None:
        result = route_query("How are imports for United States compressed total doing?")
        self.assertEqual(result.primary_metric, "lng_imports")
        self.assertEqual(result.filters, {"region": "united_states_compressed_total"})

    def test_exports_query_routes_truck_total_filter(self) -> None:
        result = route_query("How are exports for United States truck total doing?")
        self.assertEqual(result.primary_metric, "lng_exports")
        self.assertEqual(result.filters, {"region": "united_states_truck_total"})

    @patch("agents.router.llm_route_structured")
    def test_ambiguous_query_can_still_fall_back_to_llm(self, mock_llm) -> None:
        mock_llm.return_value = LLMRouteOutput(
            intent="ambiguous",
            primary_metric=None,
            metrics=[],
            filters=None,
            reason="Need clarification",
            confidence=0.2,
            ambiguous=True,
        )

        result = route_query("How is gas doing?")
        self.assertEqual(result.intent, "unsupported")
        self.assertTrue(result.ambiguous)

    def test_reserves_query_uses_long_default_window(self) -> None:
        result = route_query("Are reserves increasing or decreasing?")
        self.assertEqual(result.primary_metric, "ng_exploration_reserves_lower48")
        self.assertEqual(result.start, "2000-01-01")

    def test_reserves_query_preserves_explicit_date_window(self) -> None:
        result = route_query("Are reserves increasing or decreasing this year?")
        self.assertEqual(result.primary_metric, "ng_exploration_reserves_lower48")
        self.assertNotEqual(result.start, "2000-01-01")

    def test_reserves_query_routes_state_and_resource_category(self) -> None:
        result = route_query("Show proved NGL reserves in Texas")
        self.assertEqual(result.primary_metric, "ng_exploration_reserves_lower48")
        self.assertEqual(
            result.filters,
            {"region": "tx", "resource_category": "proved_ngl"},
        )

    def test_sector_consumption_ranking_uses_rule_route(self) -> None:
        result = route_query(
            "Which sector consumes the most gas (power, residential, industrial)?"
        )
        self.assertEqual(result.intent, "ranking")
        self.assertEqual(result.primary_metric, "ng_consumption_by_sector")
        self.assertEqual(result.metrics, ["ng_consumption_by_sector"])
        self.assertEqual(result.source, "rule")

    def test_forecast_query_sets_forecast_flags(self) -> None:
        result = route_query("Forecast Henry Hub for the next 14 days")
        self.assertEqual(result.primary_metric, "henry_hub_spot")
        self.assertTrue(result.include_forecast)
        self.assertEqual(result.forecast_horizon_days, 14)

    def test_pipeline_capacity_query_routes_pipeline_dataset(self) -> None:
        result = route_query("Show pipeline state to state capacity in the Northeast")
        self.assertEqual(result.primary_metric, "ng_pipeline")
        self.assertEqual(
            result.filters,
            {"dataset": "pipeline_state2_state_capacity"},
        )

    def test_pipeline_projects_query_routes_project_dataset(self) -> None:
        result = route_query("What natural gas pipeline projects are in the data?")
        self.assertEqual(result.primary_metric, "ng_pipeline")
        self.assertEqual(
            result.filters,
            {"dataset": "natural_gas_pipeline_projects"},
        )

    def test_storage_build_by_region_routes_to_weekly_change_grouped_by_region(self) -> None:
        result = route_query("Show storage build by region.")
        self.assertEqual(result.primary_metric, "working_gas_storage_change_weekly")
        self.assertEqual(result.filters, {"group_by": "region"})

    def test_storage_in_the_south_routes_to_south_central(self) -> None:
        result = route_query("How much gas is in storage in the South?")
        self.assertEqual(result.primary_metric, "working_gas_storage_lower48")
        self.assertEqual(result.filters, {"region": "south_central"})

    def test_withdrawals_happening_fastest_routes_to_regional_storage_change(self) -> None:
        result = route_query("Where are withdrawals happening fastest?")
        self.assertEqual(result.primary_metric, "working_gas_storage_change_weekly")
        self.assertEqual(result.filters, {"group_by": "region"})

    def test_compare_storage_and_weekly_change_together_routes_to_combined_storage_view(self) -> None:
        result = route_query("Compare East storage and weekly change together.")
        self.assertEqual(result.primary_metric, "working_gas_storage_lower48")
        self.assertEqual(
            result.filters,
            {"region": "east", "include_weekly_change": True},
        )

    def test_degree_day_forecast_routes_to_weather_metric(self) -> None:
        result = route_query(
            "How do current cooling/heating degree day forecasts compare to the 5-year average, and what impact does this have on natural gas demand?"
        )
        self.assertEqual(result.primary_metric, "weather_degree_days_forecast_vs_5y")
        self.assertEqual(result.filters, {"region": "lower48", "normal_years": 5})

    def test_degree_day_forecast_routes_requested_normal_window(self) -> None:
        result = route_query("How do HDD forecasts compare to the 3-year average?")
        self.assertEqual(result.primary_metric, "weather_degree_days_forecast_vs_5y")
        self.assertEqual(result.filters, {"region": "lower48", "normal_years": 3})

    def test_degree_day_forecast_routes_four_year_normal_window(self) -> None:
        result = route_query("How do HDD forecasts compare to the 4-year average?")
        self.assertEqual(result.primary_metric, "weather_degree_days_forecast_vs_5y")
        self.assertEqual(result.filters, {"region": "lower48", "normal_years": 4})

    def test_supply_expanding_or_tightening_routes_to_supply_balance_metric(self) -> None:
        result = route_query("Is U.S. gas supply expanding or tightening?")
        self.assertEqual(result.primary_metric, "ng_supply_balance_regime")
        self.assertEqual(result.filters, {"region": "united_states_total"})

    def test_weather_demand_next_7_14_days_routes_to_weather_metric(self) -> None:
        result = route_query("How will weather impact natural gas demand over the next 7-14 days?")
        self.assertEqual(result.primary_metric, "weather_degree_days_forecast_vs_5y")

    def test_weather_regions_driving_routes_to_weather_metric(self) -> None:
        result = route_query("Which regions are driving weather-related demand right now?")
        self.assertEqual(result.primary_metric, "weather_regional_demand_drivers")

    def test_weather_bullish_bearish_routes_to_weather_metric(self) -> None:
        result = route_query("Is the weather forecast becoming more bullish or bearish compared to last week?")
        self.assertEqual(result.primary_metric, "weather_degree_days_forecast_vs_5y")

    def test_weather_vs_seasonal_norms_routes_to_weather_metric(self) -> None:
        result = route_query("How does the current weather forecast compare to seasonal norms?")
        self.assertEqual(result.primary_metric, "weather_degree_days_forecast_vs_5y")

    def test_power_burn_vs_seasonal_norms_routes_to_ng_electricity(self) -> None:
        result = route_query(
            "What is current natural gas power burn, and how does it compare to seasonal norms?"
        )
        self.assertEqual(result.primary_metric, "ng_electricity")
        expected_start = (pd.Timestamp(date.today()) - pd.DateOffset(years=5)).date().isoformat()
        self.assertEqual(result.start, expected_start)
        self.assertEqual(result.filters, {"normal_years": 5})

    def test_percentage_generation_from_gas_routes_to_iso_gas_dependency(self) -> None:
        result = route_query(
            "What percentage of electricity generation is coming from natural gas, and how is that changing?"
        )
        self.assertEqual(result.primary_metric, "iso_gas_dependency")

    def test_renewables_impact_on_power_sector_gas_demand_routes_to_iso_gas_dependency(self) -> None:
        result = route_query(
            "Are renewables increasing or decreasing natural gas demand in the power sector?"
        )
        self.assertEqual(result.primary_metric, "iso_gas_dependency")

    def test_consumption_query_defaults_to_two_year_window_without_explicit_dates(self) -> None:
        result = route_query("How is gas consumption in California?")
        expected_start = (pd.Timestamp(date.today()) - pd.DateOffset(years=2)).date().isoformat()
        self.assertEqual(result.primary_metric, "ng_consumption_lower48")
        self.assertEqual(result.start, expected_start)

    def test_consumption_query_honors_last_year_phrase(self) -> None:
        result = route_query("How is gas consumption in California over the last year?")
        expected_start = (pd.Timestamp(date.today()) - pd.DateOffset(years=1)).date().isoformat()
        self.assertEqual(result.primary_metric, "ng_consumption_lower48")
        self.assertEqual(result.start, expected_start)

    def test_ng_electricity_query_defaults_to_two_year_window_without_explicit_dates(self) -> None:
        result = route_query("What is current natural gas power burn?")
        expected_start = (pd.Timestamp(date.today()) - pd.DateOffset(years=2)).date().isoformat()
        self.assertEqual(result.primary_metric, "ng_electricity")
        self.assertEqual(result.start, expected_start)

    def test_weekly_energy_atlas_summary_routes_to_derived_weekly_metric(self) -> None:
        result = route_query(
            "Give me a week in energy atlas summary with weather, storage, LNG/supply, and price."
        )
        self.assertEqual(result.primary_metric, "weekly_energy_atlas_summary")
        self.assertEqual(result.metrics, ["weekly_energy_atlas_summary"])
        self.assertEqual(result.source, "rule")


if __name__ == "__main__":
    unittest.main()
