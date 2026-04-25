import unittest

from agents.filter_resolvers import FilterResolverDeps, build_filters


def _none(_q: str):
    return None


class TestFilterResolvers(unittest.TestCase):
    def test_iso_metric_defaults_to_ercot_on_high_confidence(self) -> None:
        deps = FilterResolverDeps(
            route_iso=_none,
            route_storage_region=_none,
            wants_storage_level_and_change=lambda _q: False,
            wants_regional_grouping=lambda _q: False,
            wants_storage_ranking_by_region=lambda _q: False,
            route_export_region=_none,
            route_import_region=_none,
            route_consumption_state=_none,
            route_production_state=_none,
            resolve_ng_electricity_normal_years=lambda _q: None,
            route_reserves_state=_none,
            route_reserves_resource_category=_none,
            route_pipeline_dataset=_none,
            route_weather_region=_none,
            route_weather_normal_years=lambda _q: None,
            allowed_weather_normal_years={1, 2, 3, 4, 5},
        )
        filters = build_filters(
            metric="iso_gas_dependency",
            q="general gas share question",
            confidence=0.9,
            deps=deps,
        )
        self.assertEqual(filters, {"iso": "ercot"})


if __name__ == "__main__":
    unittest.main()
