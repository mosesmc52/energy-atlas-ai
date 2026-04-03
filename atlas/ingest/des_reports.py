from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from lxml import html

BASE_URL = "https://www.dallasfed.org"
ARCHIVE_URL = "https://www.dallasfed.org/research/surveys/des"
DEFAULT_RAW_DIR = Path("data/raw/des")
DEFAULT_PROCESSED_DIR = Path("data/processed/des")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "des_report"


def _quarter_title(value: str) -> str:
    text = _clean(value).title()
    return re.sub(r"\bQ([1-4])\b", r"Quarter \1", text)


def _extract_release_date(page_text: str) -> str | None:
    match = re.search(
        r"(First|Second|Third|Fourth)\s+Quarter\s*\|\s*([A-Za-z\.]+\s+\d{1,2},\s+\d{4})",
        page_text,
        flags=re.I,
    )
    return match.group(2) if match else None


def crawl_des_archive(
    *,
    session: requests.Session | None = None,
    raw_dir: str | Path = DEFAULT_RAW_DIR,
    force_refresh: bool = False,
    archive_url: str = ARCHIVE_URL,
) -> list[dict[str, str]]:
    raw_root = Path(raw_dir)
    raw_root.mkdir(parents=True, exist_ok=True)
    archive_path = raw_root / "archive.html"
    if force_refresh or not archive_path.exists():
        sess = session or requests.Session()
        sess.headers.update(HEADERS)
        response = sess.get(archive_url, timeout=30)
        response.raise_for_status()
        archive_path.write_text(response.text, encoding="utf-8")

    doc = html.fromstring(archive_path.read_text(encoding="utf-8"))
    links: list[dict[str, str]] = []
    for node in doc.xpath("//a[@href]"):
        label = _clean(" ".join(node.xpath(".//text()")))
        href = node.get("href", "")
        if not href or not re.search(r"(first|second|third|fourth)\s+quarter", label, flags=re.I):
            continue
        full_url = urljoin(BASE_URL, href)
        match = re.search(r"/research/surveys/des/(?P<year>\d{4})/\d{4,}$", full_url)
        if not match:
            continue
        links.append(
            {
                "year": match.group("year"),
                "quarter": _quarter_title(label),
                "url": full_url,
            }
        )
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in links:
        key = (item["year"], item["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def extract_des_sections(doc: html.HtmlElement) -> dict[str, str]:
    sections: dict[str, str] = {}
    for heading in doc.xpath("//h2 | //h3"):
        title = _clean(" ".join(heading.xpath(".//text()")))
        if not title:
            continue
        parts: list[str] = []
        for sibling in heading.itersiblings():
            sibling_tag = getattr(sibling, "tag", "")
            if isinstance(sibling_tag, str) and sibling_tag.lower() in {"h2", "h3"}:
                break
            text = _clean(" ".join(sibling.xpath(".//text()")))
            if text:
                parts.append(text)
        if parts:
            sections[title] = "\n\n".join(parts)
    return sections


def parse_des_report(
    *,
    year: str,
    quarter: str,
    url: str,
    session: requests.Session | None = None,
    raw_dir: str | Path = DEFAULT_RAW_DIR,
    force_refresh: bool = False,
    html_text: str | None = None,
) -> dict[str, Any]:
    raw_root = Path(raw_dir)
    raw_root.mkdir(parents=True, exist_ok=True)
    cache_path = raw_root / "reports" / f"{year}_{_slugify(quarter)}.html"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if html_text is None:
        if force_refresh or not cache_path.exists():
            sess = session or requests.Session()
            sess.headers.update(HEADERS)
            response = sess.get(url, timeout=30)
            response.raise_for_status()
            html_text = response.text
            cache_path.write_text(html_text, encoding="utf-8")
        else:
            html_text = cache_path.read_text(encoding="utf-8")

    doc = html.fromstring(html_text)
    title_nodes = doc.xpath("//h1[1]")
    title = _clean(" ".join(title_nodes[0].xpath(".//text()"))) if title_nodes else ""
    h2_nodes = doc.xpath("//h2[1]")
    headline = _clean(" ".join(h2_nodes[0].xpath(".//text()"))) if h2_nodes else ""
    page_text = _clean(" ".join(doc.xpath("//body//text()")))
    release_date = _extract_release_date(page_text)
    sections = extract_des_sections(doc)

    summary = ""
    if h2_nodes:
        summary_parts: list[str] = []
        for sibling in h2_nodes[0].itersiblings():
            tag = getattr(sibling, "tag", "")
            if isinstance(tag, str) and tag.lower() in {"h2", "h3"}:
                break
            text = _clean(" ".join(sibling.xpath(".//text()")))
            if text:
                summary_parts.append(text)
        summary = "\n\n".join(summary_parts[:6])

    def _section_text(patterns: tuple[str, ...]) -> str | None:
        for section_title, text in sections.items():
            section_lower = section_title.lower()
            if any(pattern in section_lower for pattern in patterns):
                return text
        return None

    record = {
        "report_date": release_date,
        "year": year,
        "quarter": quarter,
        "title": title,
        "headline": headline or None,
        "url": url,
        "summary": summary or None,
        "price_forecasts_text": _section_text(("price forecast", "price expectation")),
        "special_questions_text": _section_text(("special question",)),
        "results_tables_text": _section_text(("results table", "activity chart", "results")),
        "comments_text": _section_text(("comment",)),
        "sections": sections,
    }
    json_path = cache_path.with_suffix(".json")
    json_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return record


def build_des_report_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(
            columns=[
                "report_date",
                "year",
                "quarter",
                "title",
                "headline",
                "url",
                "summary",
                "price_forecasts_text",
                "special_questions_text",
                "results_tables_text",
                "comments_text",
            ]
        )
    frame = pd.DataFrame(records)
    frame["report_date"] = pd.to_datetime(frame["report_date"], errors="coerce")
    frame = frame.sort_values(["report_date", "year", "quarter"]).reset_index(drop=True)
    return frame
