from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from atlas.ingest.des_reports import build_des_report_records, crawl_des_archive, parse_des_report


ARCHIVE_HTML = """
<html><body>
<h1>Dallas Fed Energy Survey</h1>
<p>2025</p>
<ul>
  <li><a href="/research/surveys/des/2025/2501">First quarter</a></li>
  <li><a href="/research/surveys/des/2025/2502">Second quarter</a></li>
</ul>
</body></html>
"""

REPORT_HTML = """
<html><body>
  <h1>Oil and gas activity rises amid elevated uncertainty</h1>
  <h2>Energy activity expanded in the quarter.</h2>
  <p>First Quarter | March 26, 2025</p>
  <p>Summary paragraph one.</p>
  <p>Summary paragraph two.</p>
  <h2>Price Forecasts</h2>
  <p>WTI is expected to average $75.</p>
  <h2>Special Questions</h2>
  <p>Respondents highlighted policy uncertainty.</p>
  <h2>Comments</h2>
  <p>Service firms reported weaker margins.</p>
</body></html>
"""


class TestDesReports(unittest.TestCase):
    def test_crawl_archive_parses_quarter_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw_dir = Path(tmp)
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / "archive.html").write_text(ARCHIVE_HTML, encoding="utf-8")

            links = crawl_des_archive(raw_dir=raw_dir)

            self.assertEqual(len(links), 2)
            self.assertEqual(links[0]["year"], "2025")
            self.assertEqual(links[0]["quarter"], "First Quarter")

    def test_parse_report_extracts_key_sections(self) -> None:
        record = parse_des_report(
            year="2025",
            quarter="First Quarter",
            url="https://www.dallasfed.org/research/surveys/des/2025/2501",
            html_text=REPORT_HTML,
        )

        self.assertEqual(record["report_date"], "March 26, 2025")
        self.assertIn("WTI", record["price_forecasts_text"])
        self.assertIn("policy uncertainty", record["special_questions_text"])
        self.assertIn("weaker margins", record["comments_text"])

        frame = build_des_report_records([record])
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["quarter"], "First Quarter")


if __name__ == "__main__":
    unittest.main()
