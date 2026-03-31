from __future__ import annotations
import re

from .base import BaseCrawler
from .models import ReportRecord
from .utils import absolute_url, clean_text, extract_topics_from_text


class TodayInEnergyNaturalGasCrawler(BaseCrawler):
    source_name = "eia"
    archive_url = "https://www.eia.gov/todayinenergy/archive.php"
    article_base_url = "https://www.eia.gov/todayinenergy/"
    NATGAS_PATTERNS = [
        r"\bnatural gas\b",
        r"\bhenry hub\b",
        r"\blng\b",
        r"\bstorage\b",
        r"\bwithdrawal\b",
        r"\binjection\b",
        r"\bproduction\b",
    ]

    def _is_natural_gas_title(self, title: str) -> bool:
        t = title.lower()
        return any(re.search(p, t) for p in self.NATGAS_PATTERNS)

    def _article_url(self, href: str | None) -> str | None:
        if not href:
            return None
        href = href.strip()
        if not href:
            return None

        # EIA archive links may point to `/detail.php?...`, which is root-relative
        # but should resolve under `/todayinenergy/`.
        if href.startswith("/detail.php") or href.startswith("detail.php"):
            return absolute_url(self.article_base_url, href.lstrip("/"))
        return absolute_url(self.archive_url, href)

    def _parse_detail_page(self, url: str, published_date: str | None) -> ReportRecord:
        soup = self.fetch_soup(url)
        title_node = soup.find(["h1", "h2"])
        title = (
            clean_text(title_node.get_text(" ", strip=True))
            if title_node
            else "Today in Energy"
        )
        main = soup.find("main") or soup.find(id="content") or soup.body
        body_text = (
            clean_text(main.get_text(" ", strip=True))
            if main
            else clean_text(soup.get_text(" ", strip=True))
        )

        if published_date is None:
            page_text = clean_text(soup.get_text(" ", strip=True))
            date_match = re.search(
                r"\b([A-Za-z]+\s+\d{1,2},\s+\d{4})\b",
                page_text,
            )
            if date_match:
                published_date = date_match.group(1)

        return ReportRecord(
            source=self.source_name,
            report_type="today_in_energy_natural_gas",
            title=title,
            url=url,
            published_date=published_date,
            summary_text=body_text[:1500],
            body_text=body_text,
            topics=extract_topics_from_text(body_text),
            metadata={"crawler": self.__class__.__name__},
        )

    def crawl(self):
        soup = self.fetch_soup(self.archive_url)
        seen: set[str] = set()
        current_date: str | None = None

        for node in soup.find_all(["h2", "h3", "a", "li", "p"]):
            text = clean_text(node.get_text(" ", strip=True))
            if re.match(r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}$", text):
                current_date = text
                continue
            if node.name == "a" and node.get("href"):
                title = text
                if not title or not self._is_natural_gas_title(title):
                    continue
                url = self._article_url(node["href"])
                if not url or url in seen:
                    continue
                seen.add(url)
                try:
                    record = self._parse_detail_page(url, current_date)
                    if self.include_record(record):
                        yield record
                except Exception as exc:
                    print(f"[WARN] Failed to parse {url}: {exc}")
