from __future__ import annotations

import unittest

from agents.query_classifier import (
    QUESTION_TYPE_AVERAGE_N_DAYS,
    QUESTION_TYPE_FIVE_YEAR_RANGE,
    QUESTION_TYPE_LATEST,
    QUESTION_TYPE_MOM,
    QUESTION_TYPE_REGIONAL_RANKING,
    QUESTION_TYPE_YOY,
    classify_query,
)


class TestQueryClassifier(unittest.TestCase):
    def test_classifies_latest(self) -> None:
        result = classify_query("what is the latest henry hub price?")
        self.assertEqual(result.question_type, QUESTION_TYPE_LATEST)

    def test_classifies_mom(self) -> None:
        result = classify_query("is production up or down month over month?")
        self.assertEqual(result.question_type, QUESTION_TYPE_MOM)

    def test_classifies_yoy(self) -> None:
        result = classify_query("how does storage compare to same week last year?")
        self.assertEqual(result.question_type, QUESTION_TYPE_YOY)

    def test_classifies_five_year_range(self) -> None:
        result = classify_query("are inventories tight versus the five-year range?")
        self.assertEqual(result.question_type, QUESTION_TYPE_FIVE_YEAR_RANGE)

    def test_classifies_regional_ranking(self) -> None:
        result = classify_query("which region had the largest weekly storage change?")
        self.assertEqual(result.question_type, QUESTION_TYPE_REGIONAL_RANKING)

    def test_classifies_average_n_days(self) -> None:
        result = classify_query("what was the average henry hub price over the last 7 days?")
        self.assertEqual(result.question_type, QUESTION_TYPE_AVERAGE_N_DAYS)
        self.assertEqual(result.params, {"days": 7})


if __name__ == "__main__":
    unittest.main()

