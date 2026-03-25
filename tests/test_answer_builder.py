from __future__ import annotations

import unittest
from datetime import datetime

import pandas as pd

from answer_builder import build_answer_with_openai
from schemas.answer import SourceRef
from tools.eia_adapter import EIAResult


class TestAnswerBuilder(unittest.TestCase):
    def test_sector_consumption_empty_dataframe_does_not_crash(self) -> None:
        result = EIAResult(
            df=pd.DataFrame(columns=["date", "value", "series"]),
            source=SourceRef(
                source_type="eia_api",
                label="Test",
                reference="test",
                retrieved_at=datetime.utcnow(),
            ),
            meta={"metric": "ng_consumption_by_sector"},
        )

        payload = build_answer_with_openai(
            query="Which sector consumes the most gas (power, residential, industrial)?",
            result=result,
        )

        self.assertEqual(
            payload.answer_text, "No data was returned for the requested period."
        )


if __name__ == "__main__":
    unittest.main()
