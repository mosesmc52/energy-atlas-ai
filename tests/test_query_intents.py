import unittest

from utils.query_intents import (
    has_seasonal_norm_phrase,
    is_current_like_without_explicit_window,
    is_iso_gas_share_question,
    is_power_burn_seasonal_question,
    is_renewables_power_sector_demand_question,
)


class TestQueryIntents(unittest.TestCase):
    def test_seasonal_norm_phrase_detected(self) -> None:
        self.assertTrue(
            has_seasonal_norm_phrase(
                "How does power burn compare to seasonal norms?"
            )
        )

    def test_seasonal_norm_phrase_not_detected(self) -> None:
        self.assertFalse(
            has_seasonal_norm_phrase(
                "What is the latest natural gas power burn?"
            )
        )

    def test_current_like_without_explicit_window_detected(self) -> None:
        self.assertTrue(is_current_like_without_explicit_window("current power burn"))
        self.assertFalse(
            is_current_like_without_explicit_window("current power burn over the last year")
        )

    def test_power_burn_seasonal_question_detected(self) -> None:
        self.assertTrue(
            is_power_burn_seasonal_question(
                "What is current natural gas power burn versus seasonal norms?"
            )
        )

    def test_iso_gas_share_question_detected(self) -> None:
        self.assertTrue(
            is_iso_gas_share_question(
                "What percentage of electricity generation is coming from natural gas?"
            )
        )

    def test_renewables_power_sector_demand_question_detected(self) -> None:
        self.assertTrue(
            is_renewables_power_sector_demand_question(
                "Are renewables increasing or decreasing natural gas demand in the power sector?"
            )
        )


if __name__ == "__main__":
    unittest.main()
