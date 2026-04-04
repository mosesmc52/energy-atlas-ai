from __future__ import annotations

import unittest

import pandas as pd

from atlas.features.des_features import expectation_minus_spot


class TestDesFeatures(unittest.TestCase):
    def test_expectation_minus_spot(self) -> None:
        expectations = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-03-31", "2025-06-30"]),
                "value": [74.0, 76.0],
            }
        )
        spot = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-03-31", "2025-06-30"]),
                "value": [70.0, 73.5],
            }
        )

        out = expectation_minus_spot(expectations, spot)

        self.assertEqual(list(out["expectation_minus_spot"]), [4.0, 2.5])


if __name__ == "__main__":
    unittest.main()
