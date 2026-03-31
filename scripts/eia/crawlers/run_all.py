from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = CURRENT_DIR.parent
REPO_ROOT = CURRENT_DIR.parents[2]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from crawlers import (
    NaturalGasWeeklyArchiveCrawler,
    STEONaturalGasCrawler,
    TodayInEnergyNaturalGasCrawler,
    WNGSRSupplementCrawler,
)
from crawlers.models import ReportChunk
from crawlers.utils import chunk_text, make_doc_id, write_jsonl


def build_chunks(records, chunk_size: int = 800, overlap: int = 120):
    chunk_rows = []
    for record in records:
        doc_id = make_doc_id(
            record.source, record.report_type, record.title, record.url
        )
        chunks = chunk_text(
            record.body_text or record.summary_text,
            chunk_size=chunk_size,
            overlap=overlap,
        )
        for i, chunk in enumerate(chunks):
            item = ReportChunk(
                doc_id=doc_id,
                chunk_id=f"{doc_id}-{i:04d}",
                source=record.source,
                report_type=record.report_type,
                title=record.title,
                url=record.url,
                chunk_index=i,
                text=chunk,
                published_date=record.published_date,
                release_date=record.release_date,
                period_ending=record.period_ending,
                topics=record.topics,
                metadata=record.metadata,
            )
            chunk_rows.append(asdict(item))
    return chunk_rows


def main():
    parser = argparse.ArgumentParser(
        description="Crawl EIA report families and produce RAG-ready JSONL files."
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Legacy override: write both reports and chunks to the same directory.",
    )
    parser.add_argument(
        "--reports-out",
        default=None,
        help="Directory for reports.jsonl. Defaults to data/raw/eia/ng/crawlers.",
    )
    parser.add_argument(
        "--chunks-out",
        default=None,
        help="Directory for report_chunks.jsonl. Defaults to data/processed/eia/ng/crawlers.",
    )
    parser.add_argument("--chunk-size", type=int, default=800)
    parser.add_argument("--chunk-overlap", type=int, default=120)
    parser.add_argument(
        "--start-date",
        default=None,
        help="Only include records on or after this date (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    default_reports_dir = REPO_ROOT / "data" / "raw" / "eia" / "ng" / "crawlers"
    default_chunks_dir = REPO_ROOT / "data" / "processed" / "eia" / "ng" / "crawlers"

    if args.out_dir:
        reports_out_dir = Path(args.out_dir)
        chunks_out_dir = Path(args.out_dir)
    else:
        reports_out_dir = (
            Path(args.reports_out) if args.reports_out else default_reports_dir
        )
        chunks_out_dir = (
            Path(args.chunks_out) if args.chunks_out else default_chunks_dir
        )

    reports_out_dir.mkdir(parents=True, exist_ok=True)
    chunks_out_dir.mkdir(parents=True, exist_ok=True)

    start_date = None
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)

    crawlers = [
        NaturalGasWeeklyArchiveCrawler(start_date=start_date),
        WNGSRSupplementCrawler(start_date=start_date),
        STEONaturalGasCrawler(start_date=start_date),
        TodayInEnergyNaturalGasCrawler(start_date=start_date),
    ]

    records = []
    for crawler in crawlers:
        for record in crawler.crawl():
            records.append(record)

    report_rows = [asdict(r) for r in records]
    chunk_rows = build_chunks(
        records, chunk_size=args.chunk_size, overlap=args.chunk_overlap
    )

    reports_path = reports_out_dir / "reports.jsonl"
    chunks_path = chunks_out_dir / "report_chunks.jsonl"

    write_jsonl(reports_path, report_rows)
    write_jsonl(chunks_path, chunk_rows)

    summary = {
        "reports": len(report_rows),
        "chunks": len(chunk_rows),
        "reports_out_dir": str(reports_out_dir.resolve()),
        "chunks_out_dir": str(chunks_out_dir.resolve()),
        "start_date": args.start_date,
        "files": [
            str(reports_path.resolve()),
            str(chunks_path.resolve()),
        ],
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
