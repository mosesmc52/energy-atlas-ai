import unittest

from agents.scoring_policy import ScoreAdjustmentDeps, apply_metric_score_adjustments


def _none(_q: str):
    return None


class TestScoringPolicy(unittest.TestCase):
    def test_power_burn_boost_applies_to_ng_electricity(self) -> None:
        deps = ScoreAdjustmentDeps(
            route_consumption_state=_none,
            route_production_state=_none,
            route_reserves_state=_none,
            route_reserves_resource_category=_none,
            route_import_region=_none,
            route_export_region=_none,
        )
        adjusted = apply_metric_score_adjustments(
            metric="ng_electricity",
            q="natural gas power burn",
            score=0.0,
            deps=deps,
        )
        self.assertGreaterEqual(adjusted, 2.0)

    def test_renewables_penalty_applies_to_sector_metric(self) -> None:
        deps = ScoreAdjustmentDeps(
            route_consumption_state=_none,
            route_production_state=_none,
            route_reserves_state=_none,
            route_reserves_resource_category=_none,
            route_import_region=_none,
            route_export_region=_none,
        )
        adjusted = apply_metric_score_adjustments(
            metric="ng_consumption_by_sector",
            q="renewables in power sector demand",
            score=5.0,
            deps=deps,
        )
        self.assertLess(adjusted, 5.0)


if __name__ == "__main__":
    unittest.main()
