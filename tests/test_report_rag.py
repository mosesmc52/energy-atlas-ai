from __future__ import annotations

import json
import unittest
from tempfile import NamedTemporaryFile

from scripts.eia.rag.prompt_context import format_report_context
from scripts.eia.rag.retrieval import (
    load_report_chunks,
    search_report_chunks,
    should_use_report_rag,
)


class TestReportRag(unittest.TestCase):
    def test_should_use_report_rag_matches_narrative_query(self) -> None:
        self.assertTrue(
            should_use_report_rag("Why is the natural gas market tightening?")
        )
        self.assertTrue(
            should_use_report_rag("What drove henry hub price movement this week?")
        )
        self.assertFalse(should_use_report_rag("What is current storage?"))

    def test_load_and_search_report_chunks(self) -> None:
        rows = [
            {
                "title": "Natural Gas Weekly Update",
                "report_type": "weekly",
                "text": "Storage withdrawals were strong and cold weather lifted demand.",
                "published_date": "2026-01-22",
                "topics": ["storage", "weather"],
            },
            {
                "title": "Today in Energy",
                "report_type": "article",
                "text": "LNG exports remained near record highs.",
                "published_date": "2026-01-18",
                "topics": ["lng"],
            },
        ]
        with NamedTemporaryFile("w", suffix=".jsonl", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
            handle.flush()

            chunks = load_report_chunks(handle.name)

        matches = search_report_chunks(
            "What drivers are affecting storage demand?", chunks, top_k=2
        )
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["title"], "Natural Gas Weekly Update")

    def test_format_report_context_limits_output(self) -> None:
        context = format_report_context(
            [
                {
                    "title": "Natural Gas Weekly Update",
                    "published_date": "2026-01-22",
                    "report_type": "weekly",
                    "text": "A" * 1200,
                }
            ],
            max_chars=300,
        )
        self.assertIn("Report Context:", context)
        self.assertLessEqual(len(context), 303)


if __name__ == "__main__":
    unittest.main()
