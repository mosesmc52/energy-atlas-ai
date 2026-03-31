from __future__ import annotations
from datetime import date, datetime
import logging
import time

import requests
from bs4 import BeautifulSoup

from .models import ReportRecord

logger = logging.getLogger(__name__)

class BaseCrawler:
    source_name: str = "eia"
    base_url: str = "https://www.eia.gov"

    def __init__(
        self,
        timeout: int = 30,
        sleep_seconds: float = 0.4,
        start_date: str | date | None = None,
    ) -> None:
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.start_date = self._normalize_date(start_date)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; EnergyAtlasCrawler/1.0; "
                    "+https://example.com/energy-atlas)"
                )
            }
        )

    def fetch_html(self, url: str) -> str:
        logger.info("Fetching %s", url)
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        time.sleep(self.sleep_seconds)
        return resp.text

    def fetch_soup(self, url: str) -> BeautifulSoup:
        return BeautifulSoup(self.fetch_html(url), "lxml")

    def _normalize_date(self, value: str | date | None) -> date | None:
        if value is None or value == "":
            return None
        if isinstance(value, date):
            return value
        return self.parse_date(value)

    def parse_date(self, value: str | None) -> date | None:
        if not value:
            return None
        text = value.strip()
        for fmt in (
            "%Y-%m-%d",
            "%B %d, %Y",
            "%b %d, %Y",
            "%B %d %Y",
            "%b %d %Y",
        ):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    def include_record(self, record: ReportRecord) -> bool:
        if self.start_date is None:
            return True
        candidate_dates = [
            self.parse_date(record.published_date),
            self.parse_date(record.release_date),
            self.parse_date(record.period_ending),
        ]
        candidate_dates = [item for item in candidate_dates if item is not None]
        if not candidate_dates:
            return True
        return max(candidate_dates) >= self.start_date
