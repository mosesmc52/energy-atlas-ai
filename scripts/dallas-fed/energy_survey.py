#!/usr/bin/env python3
"""
Crawl Dallas Fed Energy Survey (DES) content using lxml.

Examples:
    python crawl_des.py
    python crawl_des.py --start-date 2023-01-01
    python crawl_des.py --start-date 2024-01-01 --out-dir data/des
    python crawl_des.py --include-historical
    python crawl_des.py --delay 1.0 --timeout 20

Requires:
    pip install requests lxml
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from lxml import html

BASE_URL = "https://www.dallasfed.org"
ARCHIVE_URL = "https://www.dallasfed.org/research/surveys/des"
HISTORICAL_URL = "https://www.dallasfed.org/research/surveys/des/data"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


class CrawlError(RuntimeError):
    """Raised when the crawler cannot fetch or parse a required resource."""


@dataclass
class ReportRecord:
    year: str
    quarter: str
    title: str
    url: str
    release_date: Optional[str]
    headline: Optional[str]
    summary: Optional[str]
    sections: Dict[str, str]


@dataclass
class HistoricalLink:
    category: str
    label: str
    url: str


class DallasEnergySurveyCrawler:
    def __init__(self, delay_seconds: float = 0.5, timeout: int = 30):
        self.delay_seconds = delay_seconds
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def fetch(self, url: str) -> str:
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise CrawlError(
                f"Failed to fetch {url}: {exc}. "
                "Check network access, DNS resolution, and site availability."
            ) from exc

        time.sleep(self.delay_seconds)
        return resp.text

    @staticmethod
    def clean_text(value: str) -> str:
        value = re.sub(r"\s+", " ", value or "").strip()
        return value

    @staticmethod
    def parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None

        value = value.strip()
        formats = [
            "%B %d, %Y",  # March 26, 2025
            "%b. %d, %Y",  # Mar. 26, 2025
            "%b %d, %Y",  # Mar 26, 2025
            "%Y-%m-%d",  # 2025-03-26
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        return None

    def parse_archive_links(self) -> List[Dict[str, str]]:
        html_text = self.fetch(ARCHIVE_URL)
        doc = html.fromstring(html_text)

        reports: List[Dict[str, str]] = []
        anchor_nodes = doc.xpath("//a[@href]")

        for node in anchor_nodes:
            label = self.clean_text(" ".join(node.xpath(".//text()")))
            href = node.get("href", "")

            if not href:
                continue

            if not re.search(r"(first|second|third|fourth)\s+quarter", label, flags=re.I):
                continue

            full_url = urljoin(BASE_URL, href)
            match = re.search(r"/research/surveys/des/(\d{4})/\d{4,}$", full_url)
            if not match:
                continue

            reports.append(
                {
                    "year": match.group(1),
                    "quarter": label.title(),
                    "url": full_url,
                }
            )

        deduped: List[Dict[str, str]] = []
        seen = set()
        for item in reports:
            key = (item["year"], item["quarter"], item["url"])
            if key not in seen:
                seen.add(key)
                deduped.append(item)

        return deduped

    def parse_report_page(self, year: str, quarter: str, url: str) -> ReportRecord:
        html_text = self.fetch(url)
        doc = html.fromstring(html_text)

        title = None
        h1_nodes = doc.xpath("//h1[1]")
        if h1_nodes:
            title = self.clean_text(" ".join(h1_nodes[0].xpath(".//text()")))

        headline = None
        h2_nodes = doc.xpath("//h2[1]")
        if h2_nodes:
            headline = self.clean_text(" ".join(h2_nodes[0].xpath(".//text()")))

        page_text = self.clean_text(" ".join(doc.xpath("//body//text()")))

        release_date = None
        match = re.search(
            r"(First|Second|Third|Fourth)\s+Quarter\s*\|\s*([A-Za-z\.]+\s+\d{1,2},\s+\d{4})",
            page_text,
            flags=re.I,
        )
        if match:
            release_date = match.group(2)

        summary = None
        summary_parts: List[str] = []

        if h2_nodes:
            first_h2 = h2_nodes[0]
            for sib in first_h2.itersiblings():
                if sib.tag is not None and str(sib.tag).lower() in {"h2", "h3"}:
                    break
                if sib.tag is not None and str(sib.tag).lower() == "p":
                    txt = self.clean_text(" ".join(sib.xpath(".//text()")))
                    if txt:
                        summary_parts.append(txt)

        if summary_parts:
            summary = "\n\n".join(summary_parts[:6])

        sections: Dict[str, str] = {}
        heading_nodes = doc.xpath("//h2 | //h3")

        for heading in heading_nodes:
            section_name = self.clean_text(" ".join(heading.xpath(".//text()")))
            if not section_name:
                continue

            parts: List[str] = []
            for sib in heading.itersiblings():
                sib_tag = str(sib.tag).lower() if sib.tag is not None else ""
                if sib_tag in {"h2", "h3"}:
                    break

                texts = sib.xpath(".//text()")
                txt = self.clean_text(" ".join(texts))
                if txt:
                    parts.append(txt)

            if parts:
                sections[section_name] = "\n\n".join(parts)

        return ReportRecord(
            year=year,
            quarter=quarter,
            title=title or "",
            url=url,
            release_date=release_date,
            headline=headline,
            summary=summary,
            sections=sections,
        )

    def parse_historical_links(self) -> List[HistoricalLink]:
        html_text = self.fetch(HISTORICAL_URL)
        doc = html.fromstring(html_text)

        rows: List[HistoricalLink] = []
        current_category: Optional[str] = None

        nodes = doc.xpath("//body//*[self::h2 or self::h3 or self::a]")

        for node in nodes:
            tag = node.tag.lower()

            if tag in {"h2", "h3"}:
                current_category = self.clean_text(" ".join(node.xpath(".//text()")))
                continue

            if tag == "a" and current_category:
                label = self.clean_text(" ".join(node.xpath(".//text()")))
                href = node.get("href", "")
                if not href:
                    continue

                full_url = urljoin(BASE_URL, href)

                if (
                    ".xlsx" in full_url.lower()
                    or ".xls" in full_url.lower()
                    or "download" in label.lower()
                    or "/research/surveys/des/" in full_url.lower()
                ):
                    rows.append(
                        HistoricalLink(
                            category=current_category,
                            label=label,
                            url=full_url,
                        )
                    )

        deduped: List[HistoricalLink] = []
        seen = set()
        for item in rows:
            key = (item.category, item.label, item.url)
            if key not in seen:
                seen.add(key)
                deduped.append(item)

        return deduped


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Crawl Dallas Fed Energy Survey content using lxml."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Only keep reports with release_date >= start-date. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="data/raw/dallas-fed/energy/",
        help="Directory where JSON files will be written",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.75,
        help="Delay in seconds between HTTP requests",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--include-historical",
        action="store_true",
        help="Also crawl DES historical data/download links",
    )
    return parser


def filter_reports_by_start_date(
    reports: List[ReportRecord],
    start_date_str: Optional[str],
) -> List[ReportRecord]:
    if not start_date_str:
        return reports

    try:
        cutoff = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(
            f"Invalid --start-date '{start_date_str}'. Use YYYY-MM-DD."
        ) from exc

    filtered: List[ReportRecord] = []
    skipped_without_date = 0

    for report in reports:
        parsed = DallasEnergySurveyCrawler.parse_date(report.release_date)
        if parsed is None:
            skipped_without_date += 1
            continue
        if parsed >= cutoff:
            filtered.append(report)

    print(
        f"Applied start-date filter: {start_date_str}. "
        f"Kept {len(filtered)} reports, skipped {skipped_without_date} without parseable date."
    )
    return filtered


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    crawler = DallasEnergySurveyCrawler(
        delay_seconds=args.delay,
        timeout=args.timeout,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        archive_links = crawler.parse_archive_links()
    except CrawlError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Found {len(archive_links)} report links in archive")

    reports: List[ReportRecord] = []
    for item in archive_links:
        try:
            report = crawler.parse_report_page(
                year=item["year"],
                quarter=item["quarter"],
                url=item["url"],
            )
            reports.append(report)
            print(
                f"Parsed report: year={report.year}, "
                f"quarter={report.quarter}, date={report.release_date}, url={report.url}"
            )
        except Exception as exc:
            print(f"Failed to parse report {item['url']}: {exc}")

    reports = filter_reports_by_start_date(reports, args.start_date)

    reports_path = out_dir / "des_reports.json"
    with reports_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in reports], f, indent=2, ensure_ascii=False)

    print(f"Saved {len(reports)} reports to {reports_path}")

    if args.include_historical:
        try:
            historical_links = crawler.parse_historical_links()
        except CrawlError as exc:
            raise SystemExit(str(exc)) from exc

        historical_path = out_dir / "des_historical_links.json"
        with historical_path.open("w", encoding="utf-8") as f:
            json.dump(
                [asdict(r) for r in historical_links], f, indent=2, ensure_ascii=False
            )

        print(f"Saved {len(historical_links)} historical links to {historical_path}")


if __name__ == "__main__":
    main()
