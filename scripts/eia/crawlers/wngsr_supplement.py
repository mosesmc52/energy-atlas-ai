from __future__ import annotations
import re
from .base import BaseCrawler
from .models import ReportRecord
from .utils import absolute_url, clean_text, extract_topics_from_text

class WNGSRSupplementCrawler(BaseCrawler):
    source_name = "eia"
    landing_url = "https://www.eia.gov/naturalgas/weekly/supplement/"
    whats_new_url = "https://www.eia.gov/about/new/"
    archive_urls = [
        "https://www.eia.gov/about/new/",
    ]

    def _parse_page(self, url: str) -> ReportRecord:
        soup = self.fetch_soup(url)
        page_text = clean_text(soup.get_text(" ", strip=True))
        main = soup.find("main") or soup.find(id="content") or soup.body
        body_text = clean_text(main.get_text(" ", strip=True)) if main else page_text

        title = "Weekly Natural Gas Storage Report Supplement"
        title_node = soup.find(["h1", "h2"], string=re.compile("Weekly Natural Gas Storage Report Supplement", re.I))
        if title_node:
            title = clean_text(title_node.get_text(" ", strip=True))

        release_match = re.search(r"Release Date:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, re.I)
        period_match = re.search(r"For week ending\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text, re.I)

        return ReportRecord(
            source=self.source_name,
            report_type="wngsr_supplement",
            title=title,
            url=url,
            release_date=release_match.group(1) if release_match else None,
            period_ending=period_match.group(1) if period_match else None,
            summary_text=body_text[:1600],
            body_text=body_text,
            topics=extract_topics_from_text(body_text),
            metadata={"crawler": self.__class__.__name__},
        )

    def crawl(self):
        urls = [self.landing_url]
        for index_url in self.archive_urls:
            try:
                wn_soup = self.fetch_soup(index_url)
            except Exception as exc:
                print(f"[WARN] Could not scan {index_url}: {exc}")
                continue
            for a in wn_soup.find_all("a", href=True):
                href = a.get("href", "")
                text = clean_text(a.get_text(" ", strip=True))
                full = absolute_url("https://www.eia.gov", href)
                if not full:
                    continue
                if (
                    "Weekly Natural Gas Storage Report Supplement" in text
                    or "naturalgas/weekly/supplement" in href
                ):
                    if full not in urls:
                        urls.append(full)

        seen = set()
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            try:
                record = self._parse_page(url)
                if self.include_record(record):
                    yield record
            except Exception as exc:
                print(f"[WARN] Failed to parse {url}: {exc}")
