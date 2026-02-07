"""
EIA Natural Gas Pipeline Projects (Jan 2026) parser

Sheets:
- "Natural Gas Pipeline Projects"  (Header A2, Data starts A3)
- "Historical Projects (1996-2024)" (Header A2, Data starts A3)

Row counts may expand -> we detect:
- last column from header row (scan right until a run of blank header cells)
- last row from data region (scan down until a run of blank rows)

Usage:
  python scripts/eia/ng/pipeline/ingest_pipeline_projects.py \
      --xlsx /path/to/EIA-NaturalGasPipelineProjects_Jan2026.xlsx \
      --out out_csv

Or (in this environment):
  --xlsx /mnt/data/EIA-NaturalGasPipelineProjects_Jan2026.xlsx

Requires:
  pip install pandas openpyxl
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.cell import column_index_from_string, coordinate_from_string
from openpyxl.worksheet.worksheet import Worksheet


# -----------------------------
# Config
# -----------------------------
@dataclass(frozen=True)
class SheetSpec:
    sheet: str
    header_cell: str
    data_cell: str  # top-left of first data row


SPECS = [
    SheetSpec("Natural Gas Pipeline Projects", "A2", "A3"),
    SheetSpec("Historical Projects (1996-2024)", "A2", "A3"),
]


# -----------------------------
# Helpers
# -----------------------------


def current_year() -> int:
    return datetime.now().year


def current_january_filename(prefix: str) -> str:
    return f"{prefix}_Jan{current_year()}.xlsx"


def current_january_url(prefix: str) -> str:
    return (
        f"https://www.eia.gov/naturalgas/pipelines/{current_january_filename(prefix)}"
    )


def download_if_needed(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and dest.stat().st_size > 0:
        return dest

    print(f"[download] {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with open(dest, "wb") as f:
        f.write(r.content)

    return dest


def _split_cell(cell: str) -> Tuple[int, int]:
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
    max_consecutive_blanks: int = 12,
    hard_max_cols: int = 5000,
) -> int:
    """
    Scan right from header_row/start_col until we hit N consecutive blank header cells.
    Returns last non-blank header column index.
    """
    last_nonblank = start_col
    blanks = 0
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
    max_consecutive_blank_rows: int = 80,
) -> int:
    """
    Scan downward; a row is "data" if any cell in [start_col..end_col] is non-blank.
    Stop after N consecutive blank rows.
    """
    last_data_row = start_row - 1
    blanks = 0

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


def _dedupe_headers(headers: list[str]) -> list[str]:
    """
    Make headers unique: ["State","State",""] -> ["State","State_2","Unnamed_3"]
    """
    out: list[str] = []
    seen: Dict[str, int] = {}
    for i, h in enumerate(headers, start=1):
        base = (h or "").strip() or f"Unnamed_{i}"
        if base in seen:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
        else:
            seen[base] = 1
            out.append(base)
    return out


def safe_filename(name: str) -> str:
    name = name.strip().replace("/", "-")
    name = re.sub(r"[^A-Za-z0-9._ -]+", "", name)
    name = re.sub(r"\s+", "_", name)
    return name


def read_table(ws: Worksheet, header_cell: str, data_cell: str) -> pd.DataFrame:
    header_row, start_col = _split_cell(header_cell)
    data_row, data_col = _split_cell(data_cell)
    start_col = min(start_col, data_col)

    end_col = detect_last_col_from_header(
        ws, header_row=header_row, start_col=start_col
    )
    last_row = detect_last_row_from_data(
        ws, start_row=data_row, start_col=start_col, end_col=end_col
    )

    headers = [
        ws.cell(row=header_row, column=c).value for c in range(start_col, end_col + 1)
    ]
    headers = [("" if h is None else str(h).strip()) for h in headers]
    headers = _dedupe_headers(headers)

    if last_row < data_row:
        return pd.DataFrame(columns=headers)

    rows = []
    for r in range(data_row, last_row + 1):
        rows.append(
            [ws.cell(row=r, column=c).value for c in range(start_col, end_col + 1)]
        )

    df = pd.DataFrame(rows, columns=headers)
    df = df.dropna(how="all")
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]
    return df


# -----------------------------
# Main
# -----------------------------
def parse_workbook(
    xlsx_path: str, out_dir: Optional[str] = "out_csv"
) -> Dict[str, pd.DataFrame]:
    wb = load_workbook(xlsx_path, read_only=True, data_only=True, keep_links=False)

    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    out: Dict[str, pd.DataFrame] = {}
    for spec in SPECS:
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

        print(f"[OK] {spec.sheet}: rows={len(df):,} cols={df.shape[1]:,}")

    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="out_csv", help="Output folder for CSVs")
    p.add_argument("--raw-dir", default="data/raw/eia/ng/pipeline/pipeline_projects")
    args = p.parse_args()

    prefix = "EIA-NaturalGasPipelineProjects"

    url = current_january_url(prefix)
    fname = current_january_filename(prefix)

    raw_dir = Path(args.raw_dir)
    xlsx_path = raw_dir / fname

    download_if_needed(url, xlsx_path)

    parse_workbook(str(xlsx_path), out_dir=args.out)


if __name__ == "__main__":
    main()
