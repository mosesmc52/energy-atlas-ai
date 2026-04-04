from __future__ import annotations
import re

from .base import BaseCrawler
from .models import ReportRecord
from .utils import absolute_url, clean_text, extract_topics_from_text

class STEONaturalGasCrawler(BaseCrawler):
    source_name = "eia"
    landing_url = "https://www.eia.gov/outlooks/steo/report/natgas.php"
    archive_urls = [
        "https://www.eia.gov/outlooks/steo/archive.php",
        "https://www.eia.gov/outlooks/steo/archives/",
        "https://www.eia.gov/outlooks/steo/",
    ]

    def _parse_page(self, url: str) -> ReportRecord:
        soup = self.fetch_soup(url)
        main = soup.find("main") or soup.find(id="content") or soup.body
        body_text = clean_text(main.get_text(" ", strip=True)) if main else clean_text(soup.get_text(" ", strip=True))
        page_text = clean_text(soup.get_text(" ", strip=True))

        title = "Short-Term Energy Outlook: Natural Gas"
        title_node = soup.find(["h1", "h2"], string=re.compile("Short-Term Energy Outlook", re.I))
        if title_node:
            title = clean_text(title_node.get_text(" ", strip=True))

        pub_match = re.search(r"Release Date[:\s]*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, re.I)

        return ReportRecord(
            source=self.source_name,
            report_type="steo_natural_gas",
            title=title,
            url=url,
            published_date=pub_match.group(1) if pub_match else None,
            summary_text=body_text[:2000],
            body_text=body_text,
            topics=extract_topics_from_text(body_text),
            metadata={"crawler": self.__class__.__name__},
        )

    def _candidate_urls(self) -> list[str]:
        urls = [self.landing_url]
        for index_url in self.archive_urls:
            try:
                soup = self.fetch_soup(index_url)
            except Exception:
                continue
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                text = clean_text(a.get_text(" ", strip=True))
                full = absolute_url("https://www.eia.gov", href)
                if not full:
                    continue
                if "natgas.php" in href or re.search(r"natural gas", text, re.I):
                    if full not in urls:
                        urls.append(full)
        return urls

    def crawl(self):
        seen: set[str] = set()
        for url in self._candidate_urls():
            if url in seen:
                continue
            seen.add(url)
            try:
                record = self._parse_page(url)
                if self.include_record(record):
                    yield record
            except Exception as exc:
                print(f"[WARN] Failed to parse {url}: {exc}")
