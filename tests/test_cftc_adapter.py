from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from atlas.tools.cftc_adapter import CFTCAdapter


def _sample_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Market and Exchange Names": [
                "HENRY HUB NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
                "HENRY HUB BASIS SWAP - NEW YORK MERCANTILE EXCHANGE",
                "HENRY HUB NATURAL GAS - NEW YORK MERCANTILE EXCHANGE",
            ],
            "As of Date in Form YYYY-MM-DD": ["2024-01-02", "2024-01-02", "2024-01-09"],
            "Open Interest (All)": [1000, 200, 1100],
            "Prod_Merc_Positions_Long_All": [200, 10, 220],
            "Prod_Merc_Positions_Short_All": [300, 10, 320],
            "Prod_Merc_Positions_Spread_All": [10, 0, 12],
            "Swap_Positions_Long_All": [150, 5, 155],
            "Swap_Positions_Short_All": [180, 5, 182],
            "Swap_Positions_Spread_All": [20, 0, 21],
            "M_Money_Positions_Long_All": [400, 5, 430],
            "M_Money_Positions_Short_All": [250, 5, 240],
            "M_Money_Positions_Spread_All": [30, 0, 32],
            "Other_Rept_Positions_Long_All": [100, 2, 98],
            "Other_Rept_Positions_Short_All": [90, 2, 97],
            "Other_Rept_Positions_Spread_All": [15, 0, 14],
            "NonRept_Positions_Long_All": [80, 1, 81],
            "NonRept_Positions_Short_All": [70, 1, 73],
        }
    )


class TestCFTCAdapter(unittest.TestCase):
    def test_parse_csv_and_excel_inputs(self) -> None:
        adapter = CFTCAdapter(cache_dir=Path(tempfile.mkdtemp()) / "cftc")
        frame = _sample_rows()

        csv_payload = frame.to_csv(index=False).encode("utf-8")
        parsed_csv = adapter._parse_file_bytes(csv_payload, file_name="current.txt")

        with patch.object(adapter, "_read_excel", return_value=frame) as mock_read_excel:
            parsed_xlsx = adapter._parse_file_bytes(b"placeholder", file_name="history.xlsx")
        mock_read_excel.assert_called_once()

        self.assertEqual(len(parsed_csv), 3)
        self.assertEqual(len(parsed_xlsx), 3)

    def test_zip_csv_parsing_and_henry_hub_filtering(self) -> None:
        adapter = CFTCAdapter(cache_dir=Path(tempfile.mkdtemp()) / "cftc")
        frame = _sample_rows()

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("2024.txt", frame.to_csv(index=False))

        parsed = adapter._parse_file_bytes(zip_buffer.getvalue(), file_name="2024.zip")
        normalized = adapter._normalize_df(parsed)
        filtered = adapter._filter_contract(normalized, contract="henry_hub_natural_gas")

        self.assertEqual(len(filtered), 2)
        self.assertTrue((filtered["market_name"] == "HENRY HUB NATURAL GAS").all())

    def test_deduplication_keeps_latest_per_date(self) -> None:
        adapter = CFTCAdapter(cache_dir=Path(tempfile.mkdtemp()) / "cftc")
        frame = _sample_rows()
        duplicate = frame.iloc[[0]].copy()
        duplicate["Open Interest (All)"] = [999]
        combined = pd.concat([frame, duplicate], ignore_index=True)

        normalized = adapter._normalize_df(combined)
        filtered = adapter._filter_contract(normalized, contract="henry_hub_natural_gas")

        self.assertEqual(len(filtered), 2)
        self.assertEqual(int(filtered.iloc[0]["open_interest"]), 999)

    def test_derived_metrics(self) -> None:
        adapter = CFTCAdapter(cache_dir=Path(tempfile.mkdtemp()) / "cftc")
        normalized = adapter._normalize_df(_sample_rows())
        filtered = adapter._filter_contract(normalized, contract="henry_hub_natural_gas")
        derived = adapter._derive_metrics(filtered)

        self.assertEqual(int(derived.iloc[0]["managed_money_net"]), 150)
        self.assertEqual(int(derived.iloc[0]["producer_net"]), -100)
        self.assertEqual(int(derived.iloc[1]["managed_money_wow_change"]), 40)
        self.assertEqual(int(derived.iloc[1]["open_interest_wow_change"]), 100)
        self.assertAlmostEqual(float(derived.iloc[0]["managed_money_net_pct_oi"]), 15.0)

    def test_fail_loudly_when_no_henry_hub_row(self) -> None:
        adapter = CFTCAdapter(cache_dir=Path(tempfile.mkdtemp()) / "cftc")
        frame = pd.DataFrame(
            {
                "Market and Exchange Names": ["CRUDE OIL - NEW YORK MERCANTILE EXCHANGE"],
                "As of Date in Form YYYY-MM-DD": ["2024-01-02"],
                "Open Interest (All)": [100],
            }
        )
        normalized = adapter._normalize_df(frame)
        with self.assertRaisesRegex(ValueError, "No Henry Hub Natural Gas"):
            adapter._filter_contract(normalized, contract="henry_hub_natural_gas")


if __name__ == "__main__":
    unittest.main()
