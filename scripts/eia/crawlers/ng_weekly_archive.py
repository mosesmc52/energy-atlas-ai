from __future__ import annotations
import re
from .base import BaseCrawler
from .models import ReportRecord
from .utils import absolute_url, clean_text, extract_topics_from_text

class NaturalGasWeeklyArchiveCrawler(BaseCrawler):
    source_name = "eia"
    landing_url = "https://www.eia.gov/naturalgas/weekly/"

    def _extract_main_body(self, soup) -> str:
        main = soup.find("main") or soup.find(id="content") or soup.body
        return clean_text(main.get_text(" ", strip=True)) if main else clean_text(soup.get_text(" ", strip=True))

    def _parse_report_page(self, url: str) -> ReportRecord:
        soup = self.fetch_soup(url)
        body_text = self._extract_main_body(soup)
        page_text = clean_text(soup.get_text(" ", strip=True))

        title = "Natural Gas Weekly Update"
        title_node = soup.find(["h1", "h2"], string=re.compile("Natural Gas Weekly Update", re.I))
        if title_node:
            title = clean_text(title_node.get_text(" ", strip=True))

        week_match = re.search(
            r"for week ending\s+(.+?)\s+\|\s+Release date:\s+(.+?)(?:\||JUMP TO:)",
            page_text,
            re.I,
        )
        period_ending = week_match.group(1).strip() if week_match else None
        release_date = week_match.group(2).strip() if week_match else None

        return ReportRecord(
            source=self.source_name,
            report_type="natural_gas_weekly_update",
            title=title,
            url=url,
            release_date=release_date,
            period_ending=period_ending,
            summary_text=body_text[:1600],
            body_text=body_text,
            topics=extract_topics_from_text(body_text),
            metadata={"crawler": self.__class__.__name__},
        )

    def crawl(self):
        soup = self.fetch_soup(self.landing_url)
        urls = [self.landing_url]
        prev_link = soup.find("a", string=re.compile("Previous weeks", re.I))
        archive_url = absolute_url("https://www.eia.gov", prev_link.get("href")) if prev_link else None

        if archive_url:
            archive_soup = self.fetch_soup(archive_url)
            for a in archive_soup.find_all("a", href=True):
                href = a.get("href", "")
                text = clean_text(a.get_text(" ", strip=True))
                if "weekly" in href and ("20" in text or "Natural Gas Weekly Update" in text):
                    full = absolute_url("https://www.eia.gov", href)
                    if full and full not in urls:
                        urls.append(full)

        seen = set()
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            try:
                record = self._parse_report_page(url)
                if self.include_record(record):
                    yield record
            except Exception as exc:
                print(f"[WARN] Failed to parse {url}: {exc}")
