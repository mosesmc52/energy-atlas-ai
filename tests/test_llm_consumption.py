from __future__ import annotations

import unittest

from agents.router import route_query


class TestNaturalGasConsumptionRouting(unittest.TestCase):
    def test_latest_residential_texas(self) -> None:
        route = route_query("What is current residential natural gas consumption in Texas?")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "latest")
        self.assertEqual(route.consumption_frequency, "monthly")
        self.assertEqual(route.consumption_sector, "residential")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.primary_metric, "natural_gas_residential_consumption_monthly")
        self.assertEqual(route.chart_type, "none")

    def test_annual_time_series(self) -> None:
        route = route_query(
            "Show annual commercial natural gas consumption in California since 2010."
        )
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.consumption_frequency, "annual")
        self.assertEqual(route.primary_metric, "natural_gas_commercial_consumption_annual")
        self.assertEqual(route.chart_type, "line")

    def test_geography_compare_does_not_use_storage_fields(self) -> None:
        route = route_query("Compare residential consumption in Texas and Louisiana.")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "geography_compare")
        self.assertEqual(route.states, ["tx", "la"])
        self.assertEqual(route.regions, [])
        self.assertEqual(route.filters["consumption_dataset"], "natural_gas_consumption_by_end_use")
        self.assertNotIn("storage_dataset", route.filters)

    def test_all_end_use_sectors_resolve_deterministically(self) -> None:
        route = route_query(
            "Compare natural gas consumption by end use in the United States."
        )
        self.assertEqual(route.analysis_type, "sector_compare")
        self.assertTrue(route.consumption_sectors_all)
        self.assertEqual(
            route.metrics,
            [
                "natural_gas_residential_consumption_monthly",
                "natural_gas_commercial_consumption_monthly",
                "natural_gas_vehicle_consumption_monthly",
                "natural_gas_electric_power_consumption_monthly",
                "natural_gas_total_consumption_monthly",
            ],
        )

    def test_storage_and_consumption_causal_question_is_not_storage(self) -> None:
        route = route_query(
            "How did residential demand affect this week's storage injection?"
        )
        self.assertEqual(route.domain, "unsupported")
        self.assertEqual(route.primary_metric, None)

    def test_consumption_followup_inherits_scope(self) -> None:
        first = route_query("Show Texas residential consumption since 2015.")
        route = route_query("What about commercial?", previous_context=first)
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.consumption_sector, "commercial")
        self.assertEqual(route.states, ["tx"])
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.start_date, first.start_date)
        self.assertEqual(route.end_date, first.end_date)

    def test_total_consumption_state_history_uses_consumption_metric(self) -> None:
        route = route_query(
            "How has total natural gas consumption in Ohio changed over time?"
        )
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "time_series")
        self.assertEqual(route.consumption_sector, "total")
        self.assertEqual(route.states, ["oh"])
        self.assertEqual(
            route.primary_metric, "natural_gas_total_consumption_monthly"
        )
        self.assertEqual(route.regions, [])

    def test_end_use_ranking_marks_all_sectors(self) -> None:
        route = route_query("Which end-use sector consumes the most gas in Texas?")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertIsNone(route.consumption_sector)
        self.assertTrue(route.consumption_sectors_all)
        self.assertEqual(route.states, ["tx"])

    def test_national_sector_ranking_marks_all_sectors(self) -> None:
        route = route_query("Which sector uses the least natural gas nationally?")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertTrue(route.consumption_sectors_all)
        self.assertEqual(route.states, ["us_total"])

    def test_home_use_maps_to_residential_consumption(self) -> None:
        route = route_query("How has gas use in homes changed since 2015?")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.consumption_sector, "residential")
        self.assertEqual(route.primary_metric, "natural_gas_residential_consumption_monthly")

    def test_commercial_buildings_map_to_commercial_consumption(self) -> None:
        route = route_query("Show commercial-building gas use in Illinois.")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.consumption_sector, "commercial")
        self.assertEqual(route.primary_metric, "natural_gas_commercial_consumption_monthly")

    def test_electric_utility_consumption_maps_to_power_sector(self) -> None:
        route = route_query("Rank states by electric utility natural gas consumption")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "ranking")
        self.assertEqual(route.consumption_sector, "electric_power")
        self.assertEqual(route.states, [])
        self.assertTrue(route.states_all)
        self.assertEqual(
            route.primary_metric,
            "natural_gas_electric_power_consumption_monthly",
        )

    def test_unusually_high_this_month_is_seasonal_compare(self) -> None:
        route = route_query("Is U.S. total consumption unusually high for this month?")
        self.assertEqual(route.domain, "consumption")
        self.assertEqual(route.analysis_type, "seasonal_compare")
        self.assertEqual(route.consumption_sector, "total")
        self.assertEqual(route.chart_type, "seasonal_line")


if __name__ == "__main__":
    unittest.main()
