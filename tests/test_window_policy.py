import unittest

from agents.metric_capabilities import get_metric_capability
from agents.window_policy import (
    WindowPolicyDeps,
    resolve_metric_lookback_years,
    resolved_normal_years_for_query,
)


class TestWindowPolicy(unittest.TestCase):
    def test_ng_electricity_seasonal_norm_uses_default_five_years(self) -> None:
        deps = WindowPolicyDeps(
            get_metric_capability=get_metric_capability,
            wants_seasonal_norm_comparison=lambda q: "seasonal" in q,
            route_weather_normal_years=lambda _q: None,
            allowed_weather_normal_years={1, 2, 3, 4, 5},
        )
        years = resolved_normal_years_for_query(
            metric="ng_electricity",
            q="power burn vs seasonal norms",
            deps=deps,
        )
        self.assertEqual(years, 5)

    def test_consumption_metric_defaults_to_two_year_lookback(self) -> None:
        deps = WindowPolicyDeps(
            get_metric_capability=get_metric_capability,
            wants_seasonal_norm_comparison=lambda _q: False,
            route_weather_normal_years=lambda _q: None,
            allowed_weather_normal_years={1, 2, 3, 4, 5},
        )
        years = resolve_metric_lookback_years(
            metric="ng_consumption_lower48",
            q="how is consumption",
            has_explicit_dates=False,
            current_like_only=False,
            deps=deps,
        )
        self.assertEqual(years, 2)

    def test_five_year_average_query_forces_six_year_lookback(self) -> None:
        deps = WindowPolicyDeps(
            get_metric_capability=get_metric_capability,
            wants_seasonal_norm_comparison=lambda _q: False,
            route_weather_normal_years=lambda _q: None,
            allowed_weather_normal_years={1, 2, 3, 4, 5},
        )
        years = resolve_metric_lookback_years(
            metric="ng_consumption_lower48",
            q="how does current consumption compare to the 5 year average?",
            has_explicit_dates=False,
            current_like_only=False,
            deps=deps,
        )
        self.assertEqual(years, 6)

    def test_five_year_seasonal_query_overrides_explicit_this_week_window(self) -> None:
        deps = WindowPolicyDeps(
            get_metric_capability=get_metric_capability,
            wants_seasonal_norm_comparison=lambda _q: False,
            route_weather_normal_years=lambda _q: None,
            allowed_weather_normal_years={1, 2, 3, 4, 5},
        )
        years = resolve_metric_lookback_years(
            metric="ng_electricity",
            q="How is power demand this week compared to 5 year historical seasonal demand?",
            has_explicit_dates=True,
            current_like_only=False,
            deps=deps,
        )
        self.assertEqual(years, 6)


if __name__ == "__main__":
    unittest.main()
