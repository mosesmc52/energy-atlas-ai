import unittest

from utils.query_intents import (
    has_seasonal_norm_phrase,
    is_current_like_without_explicit_window,
    is_power_burn_seasonal_question,
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

if __name__ == "__main__":
    unittest.main()
