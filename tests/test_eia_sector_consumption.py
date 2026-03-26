from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from tools.eia_adapter import EIAAdapter


SAMPLE_HTML = """
<html>
  <body>
    <table>
      <thead>
        <tr><th>Ignore</th><th>Me</th></tr>
      </thead>
      <tbody>
        <tr><td>a</td><td>b</td></tr>
      </tbody>
    </table>
    <table>
      <thead>
        <tr><th>Year</th><th>Jan</th><th>Feb</th><th>Mar</th></tr>
      </thead>
      <tbody>
        <tr><td>2025</td><td>100</td><td>200</td><td>-</td></tr>
        <tr><td>2026</td><td>300</td><td>W</td><td>400</td></tr>
      </tbody>
    </table>
  </body>
</html>
"""


class TestEIASectorConsumption(unittest.TestCase):
    @patch("tools.eia_adapter.EIAClient", return_value=object())
    @patch("tools.eia_adapter.requests.get")
    def test_fetch_ng_consumption_sector_history_parses_monthly_table(
        self, mock_get: Mock, _: Mock
    ) -> None:
        response = Mock()
        response.text = SAMPLE_HTML
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        adapter = EIAAdapter()
        df = adapter._fetch_ng_consumption_sector_history(
            sector="industrial",
            start="2025-01-01",
            end="2026-03-31",
        )

        self.assertEqual(df["series"].unique().tolist(), ["industrial"])
        self.assertEqual(
            df["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2025-01-01", "2025-02-01", "2026-01-01", "2026-03-01"],
        )
        self.assertEqual(df["value"].tolist(), [100.0, 200.0, 300.0, 400.0])


if __name__ == "__main__":
    unittest.main()
