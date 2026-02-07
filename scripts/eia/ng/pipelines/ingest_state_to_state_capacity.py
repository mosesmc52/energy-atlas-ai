"""
Parse EIA-StatetoStateCapacity_Jan2026.xlsx tab-by-tab using *anchor cells*.

Goal:
- For each sheet, you provide a header anchor cell (top-left of header row),
  and a data anchor cell (top-left of first data row).
- The script *dynamically* finds the last used column (from the header row)
  and the last used row (from the data area), so row counts can expand.

Outputs:
- One CSV per sheet in ./out_csv/
- Also returns a dict of pandas DataFrames if you import + call parse_workbook()

Requirements:
  pip install pandas openpyxl requests
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.utils.cell import (
    column_index_from_string,
    coordinate_from_string,
    get_column_letter,
)
from openpyxl.worksheet.worksheet import Worksheet

EIA_URL = (
    "https://www.eia.gov/naturalgas/pipelines/EIA-StatetoStateCapacity_Jan2026.xlsx"
)


# -----------------------------
# Config: sheet -> anchors
# -----------------------------
@dataclass(frozen=True)
class SheetSpec:
    sheet: str
    header_cell: str  # top-left header cell
    data_cell: str  # top-left data cell (first data row)
    # If header_cell == data_cell (some tabs in your notes), we auto-shift data row to header_row+1


SPECS = [
    SheetSpec("Major Pipeline Summary", "B5", "B6"),
    SheetSpec("Inflow By Region", "A5", "A6"),
    SheetSpec("Outflow By Region", "A5", "A6"),
    SheetSpec("Inflow By State", "A5", "A6"),
    SheetSpec("Outflow By State", "A5", "A6"),
    SheetSpec("Inflow By State and Pipeline", "A5", "A6"),
    SheetSpec("Outflow By State and Pipeline", "A5", "A6"),
    SheetSpec("Pipeline State2State Capacity", "A2", "A3"),
    SheetSpec("State2StateAIMMS", "A1", "A1"),  # will auto-shift data to row+1
    SheetSpec(
        "Pipeline State2State CapacityH", "A5", "A5"
    ),  # will auto-shift data to row+1
    SheetSpec("InFlow Single Year", "A5", "A5"),  # will auto-shift data to row+1
    SheetSpec("Outflow Single Year", "A4", "A5"),
]


# -----------------------------
# Helpers
# -----------------------------
def download_if_needed(url: str, filepath: str) -> str:
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        return filepath

    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with open(filepath, "wb") as f:
        f.write(r.content)
    return filepath


def _split_cell(cell: str) -> Tuple[int, int]:
    """
    "B5" -> (row=5, col=2)
    """
    col_letters, row = coordinate_from_string(cell)
    return int(row), int(column_index_from_string(col_letters))


def _is_blank(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def detect_last_col_from_header(
    ws: Worksheet,
    header_row: int,
    start_col: int,
    max_consecutive_blanks: int = 10,
    hard_max_cols: int = 5000,
) -> int:
    """
    Scan right from (header_row, start_col) and stop after N consecutive blank header cells.
    Returns last non-blank header column index.
    """
    last_nonblank = start_col
    blanks = 0

    # Use ws.max_column as a weak upper bound but allow some slack
    upper = min(max(ws.max_column, start_col) + 200, hard_max_cols)

    for c in range(start_col, upper + 1):
        v = ws.cell(row=header_row, column=c).value
        if _is_blank(v):
            blanks += 1
            if blanks >= max_consecutive_blanks:
                break
        else:
            last_nonblank = c
            blanks = 0

    return last_nonblank


def detect_last_row_from_data(
    ws: Worksheet,
    start_row: int,
    start_col: int,
    end_col: int,
    max_consecutive_blank_rows: int = 50,
) -> int:
    """
    Scan downward from start_row. A row counts as "data" if any cell in [start_col..end_col] is non-blank.
    Stop after N consecutive blank rows.
    Returns last data row index (>= start_row-1).
    """
    last_data_row = start_row - 1
    blanks = 0

    # ws.max_row can be huge; read_only still okay, but keep logic simple
    for r in range(start_row, ws.max_row + 1):
        any_data = False
        for c in range(start_col, end_col + 1):
            if not _is_blank(ws.cell(row=r, column=c).value):
                any_data = True
                break

        if any_data:
            last_data_row = r
            blanks = 0
        else:
            blanks += 1
            if blanks >= max_consecutive_blank_rows:
                break

    return last_data_row


def read_table(
    ws: Worksheet,
    header_cell: str,
    data_cell: str,
    *,
    header_blank_run: int = 10,
    row_blank_run: int = 50,
) -> pd.DataFrame:
    """
    Extract a rectangular table using:
      - header row from header_cell
      - data start row from data_cell
      - dynamic end column (header scan)
      - dynamic end row (data scan)
    """
    header_row, start_col = _split_cell(header_cell)
    data_row, data_col = _split_cell(data_cell)

    # If user provided same cell for header/data, treat data as next row (common in “header at A5; data starts below”)
    if header_row == data_row and start_col == data_col:
        data_row = header_row + 1

    # Some sheets might have data anchor col different from header anchor col; use the left-most
    start_col = min(start_col, data_col)

    end_col = detect_last_col_from_header(
        ws,
        header_row=header_row,
        start_col=start_col,
        max_consecutive_blanks=header_blank_run,
    )
    last_row = detect_last_row_from_data(
        ws,
        start_row=data_row,
        start_col=start_col,
        end_col=end_col,
        max_consecutive_blank_rows=row_blank_run,
    )

    if last_row < data_row:
        # No data rows detected; still return empty df with headers if present
        headers = [
            ws.cell(row=header_row, column=c).value
            for c in range(start_col, end_col + 1)
        ]
        headers = [("" if h is None else str(h).strip()) for h in headers]
        headers = _dedupe_headers(headers)
        return pd.DataFrame(columns=headers)

    headers = [
        ws.cell(row=header_row, column=c).value for c in range(start_col, end_col + 1)
    ]
    headers = [("" if h is None else str(h).strip()) for h in headers]
    headers = _dedupe_headers(headers)

    rows = []
    for r in range(data_row, last_row + 1):
        rows.append(
            [ws.cell(row=r, column=c).value for c in range(start_col, end_col + 1)]
        )

    df = pd.DataFrame(rows, columns=headers)

    # Drop fully-empty rows (sometimes you get spacer rows inside the detected range)
    df = df.dropna(how="all")

    # Normalize column names (optional)
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]

    return df


def _dedupe_headers(headers: list[str]) -> list[str]:
    """
    Make headers unique: ["State","State",""] -> ["State","State_2","Unnamed_3"]
    """
    out = []
    seen: Dict[str, int] = {}
    for i, h in enumerate(headers, start=1):
        base = h if h else f"Unnamed_{i}"
        k = base
        if k in seen:
            seen[k] += 1
            k = f"{base}_{seen[base]}"
        else:
            seen[k] = 1
        out.append(k)
    return out


def safe_filename(name: str) -> str:
    name = name.strip().replace("/", "-")
    name = re.sub(r"[^A-Za-z0-9._ -]+", "", name)
    name = re.sub(r"\s+", "_", name)
    return name


# -----------------------------
# Main parsing
# -----------------------------
def parse_workbook(
    xlsx_path: str,
    specs: list[SheetSpec] = SPECS,
    out_dir: Optional[str] = "out_csv",
) -> Dict[str, pd.DataFrame]:
    wb = load_workbook(xlsx_path, read_only=True, data_only=True, keep_links=False)
    out: Dict[str, pd.DataFrame] = {}

    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    for spec in specs:
        if spec.sheet not in wb.sheetnames:
            raise KeyError(
                f"Sheet not found: {spec.sheet!r}. Available: {wb.sheetnames}"
            )

        ws = wb[spec.sheet]
        df = read_table(ws, spec.header_cell, spec.data_cell)

        out[spec.sheet] = df

        if out_dir:
            csv_path = os.path.join(out_dir, f"{safe_filename(spec.sheet)}.csv")
            df.to_csv(csv_path, index=False)

        # quick console trace
        print(f"[OK] {spec.sheet}: rows={len(df):,} cols={df.shape[1]:,}")

    return out


def main():
    # If you already downloaded the file locally, set xlsx_path directly.
    xlsx_path = "EIA-StatetoStateCapacity_Jan2026.xlsx"

    # Optionally download from EIA if not present
    if not os.path.exists(xlsx_path):
        print(f"Downloading: {EIA_URL}")
        download_if_needed(EIA_URL, xlsx_path)

    parse_workbook(xlsx_path, SPECS, out_dir="out_csv")


if __name__ == "__main__":
    main()
