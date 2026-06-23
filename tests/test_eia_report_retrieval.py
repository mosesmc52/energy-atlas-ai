from __future__ import annotations

import unittest
from types import SimpleNamespace

from scripts.eia.rag.report_selector import select_report_filters
from scripts.eia.rag.retrieval import search_report_chunks


class TestEIAReportRetrieval(unittest.TestCase):
    def test_search_report_chunks_filters_by_report_family(self) -> None:
        chunks = [
            {
                "title": "Natural Gas Weekly Update",
                "report_type": "natural_gas_weekly_update",
                "report_family": "natural_gas_weekly",
                "text": "Storage withdrawals increased as cold weather lifted demand.",
                "published_date": "2026-01-18",
                "domain_tags": ["storage"],
                "metric_tags": ["withdrawals"],
                "geography_tags": ["lower48"],
                "topics": ["storage"],
            },
            {
                "title": "STEO Natural Gas",
                "report_type": "steo_natural_gas",
                "report_family": "steo_natural_gas",
                "text": "Natural gas outlook changed.",
                "published_date": "2026-01-18",
                "domain_tags": ["outlook"],
                "metric_tags": ["production"],
                "geography_tags": ["united_states_total"],
                "topics": ["production"],
            },
        ]

        matches = search_report_chunks(
            "Why did storage withdrawals increase?",
            chunks,
            filters={"report_families": ["natural_gas_weekly"]},
        )

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["report_family"], "natural_gas_weekly")

    def test_search_report_chunks_filters_by_metric_tag(self) -> None:
        chunks = [
            {
                "title": "Weekly storage commentary",
                "report_type": "natural_gas_weekly_update",
                "report_family": "natural_gas_weekly",
                "text": "Storage injections stayed firm.",
                "published_date": "2026-01-18",
                "domain_tags": ["storage"],
                "metric_tags": ["injections"],
                "topics": ["storage"],
            },
            {
                "title": "LNG commentary",
                "report_type": "today_in_energy_natural_gas",
                "report_family": "today_in_energy_natural_gas",
                "text": "LNG exports and LNG market conditions remained strong.",
                "published_date": "2026-01-18",
                "domain_tags": ["natural_gas"],
                "metric_tags": ["lng"],
                "topics": ["lng"],
            },
        ]

        matches = search_report_chunks(
            "Why is LNG demand strong?",
            chunks,
            filters={"metric_tags": ["lng"]},
        )

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["title"], "LNG commentary")

    def test_select_report_filters_for_weekly_storage_route(self) -> None:
        route = SimpleNamespace(
            domain="storage",
            storage_dataset="weekly_working_gas",
            storage_metric_type="working_gas",
            storage_type=None,
            states=[],
            regions=["east"],
        )

        filters = select_report_filters("Why did storage tighten this week?", route)

        self.assertIn("natural_gas_weekly", filters["report_families"])
        self.assertIn("wngsr_supplement", filters["report_families"])
        self.assertIn("storage", filters["domain_tags"])

    def test_select_report_filters_for_lng_storage_route(self) -> None:
        route = SimpleNamespace(
            domain="storage",
            storage_dataset="lng_storage",
            storage_metric_type="lng_storage_additions",
            storage_type=None,
            states=["tx"],
            regions=[],
        )

        filters = select_report_filters("Why are LNG storage additions changing?", route)

        self.assertIn("lng", filters["metric_tags"])
        self.assertIn("natural_gas_weekly", filters["report_families"])


if __name__ == "__main__":
    unittest.main()
