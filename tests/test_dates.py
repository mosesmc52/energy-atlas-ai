import unittest
from datetime import date, timedelta

import pandas as pd

from utils.dates import resolve_date_range


class TestDates(unittest.TestCase):
    def test_last_month_resolves_to_full_previous_calendar_month(self) -> None:
        start, end = resolve_date_range("How much natural gas did power plants use last month?")

        today = date.today()
        current_month_start = pd.Timestamp(today).replace(day=1)
        expected_start = (current_month_start - pd.DateOffset(months=1)).date().isoformat()
        expected_end = (
            (current_month_start - pd.DateOffset(months=1)) + pd.offsets.MonthEnd(1)
        ).date().isoformat()

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_end)

    def test_last_n_weeks_resolves_to_week_lookback(self) -> None:
        start, end = resolve_date_range("Show weekly storage change over the last 24 weeks.")
        today = date.today()
        expected_start = (today - timedelta(weeks=24)).isoformat()
        expected_end = today.isoformat()
        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_end)

    def test_last_week_resolves_to_one_week_lookback(self) -> None:
        start, end = resolve_date_range("Did storage increase last week?")
        today = date.today()
        expected_start = (today - timedelta(weeks=1)).isoformat()
        expected_end = today.isoformat()
        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_end)


if __name__ == "__main__":
    unittest.main()
