from __future__ import annotations

import io
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

import pandas as pd
import requests
from lxml import html

from schemas.answer import SourceRef
from tools.cache_base import CacheBackedTimeseriesAdapterBase

logger = logging.getLogger(__name__)

CFTC_COT_INDEX_URL = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm"
CFTC_HISTORICAL_URL = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm"
HENRY_HUB_CONTRACT = "henry_hub_natural_gas"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

CFTC_METRIC_UNITS: dict[str, str] = {
    "open_interest": "contracts",
    "producer_long": "contracts",
    "producer_short": "contracts",
    "producer_spread": "contracts",
    "swap_dealer_long": "contracts",
    "swap_dealer_short": "contracts",
    "swap_dealer_spread": "contracts",
    "managed_money_long": "contracts",
    "managed_money_short": "contracts",
    "managed_money_spread": "contracts",
    "other_reportables_long": "contracts",
    "other_reportables_short": "contracts",
    "other_reportables_spread": "contracts",
    "nonreportable_long": "contracts",
    "nonreportable_short": "contracts",
    "managed_money_net": "contracts",
    "producer_net": "contracts",
    "swap_dealer_net": "contracts",
    "other_reportables_net": "contracts",
    "managed_money_net_pct_oi": "pct_open_interest",
    "managed_money_net_zscore_52w": "zscore",
    "managed_money_net_percentile_156w": "percentile",
    "managed_money_wow_change": "contracts",
    "open_interest_wow_change": "contracts",
}

CFTC_CANONICAL_WIDE_COLUMNS = [
    "date",
    "market_name",
    "exchange_name",
    "open_interest",
    "producer_long",
    "producer_short",
    "producer_spread",
    "swap_dealer_long",
    "swap_dealer_short",
    "swap_dealer_spread",
    "managed_money_long",
    "managed_money_short",
    "managed_money_spread",
    "other_reportables_long",
    "other_reportables_short",
    "other_reportables_spread",
    "nonreportable_long",
    "nonreportable_short",
    "contract",
    "managed_money_net",
    "producer_net",
    "swap_dealer_net",
    "other_reportables_net",
    "managed_money_net_pct_oi",
    "managed_money_net_zscore_52w",
    "managed_money_net_percentile_156w",
    "managed_money_wow_change",
    "open_interest_wow_change",
]

_COLUMN_MAP = {
    "market_and_exchange_names": "market_and_exchange_names",
    "market_and_exchange_name": "market_and_exchange_names",
    "as_of_date_form_yyyy_mm_dd": "date",
    "as_of_date_form_yyyy_mm_dd_": "date",
    "as_of_date_in_form_yyyy_mm_dd": "date",
    "as_of_date_form_yyyy-mm-dd": "date",
    "as_of_date_in_form_yy_mm_dd": "date_yy",
    "open_interest_all": "open_interest",
    "prod_merc_positions_long_all": "producer_long",
    "prod_merc_positions_short_all": "producer_short",
    "prod_merc_positions_spread_all": "producer_spread",
    "swap_positions_long_all": "swap_dealer_long",
    "swap_positions_short_all": "swap_dealer_short",
    "swap_positions_spread_all": "swap_dealer_spread",
    "m_money_positions_long_all": "managed_money_long",
    "m_money_positions_short_all": "managed_money_short",
    "m_money_positions_spread_all": "managed_money_spread",
    "other_rept_positions_long_all": "other_reportables_long",
    "other_rept_positions_short_all": "other_reportables_short",
    "other_rept_positions_spread_all": "other_reportables_spread",
    "nonrept_positions_long_all": "nonreportable_long",
    "nonrept_positions_short_all": "nonreportable_short",
}


@dataclass(frozen=True)
class CFTCResult:
    df: pd.DataFrame
    source: SourceRef
    meta: dict[str, Any] | None = None


class CFTCAdapter(CacheBackedTimeseriesAdapterBase):
    def __init__(
        self,
        *,
        cache_dir: str | Path = "data/cache/cftc",
        raw_dir: str | Path | None = None,
    ) -> None:
        super().__init__(cache_dir=cache_dir, date_col="date")
        self.raw_dir = Path(raw_dir or Path(cache_dir) / "raw")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get_metric(
        self,
        metric_key: str,
        *,
        start: str,
        end: str,
        contract: str = HENRY_HUB_CONTRACT,
        force_refresh: bool = False,
    ) -> CFTCResult:
        if metric_key not in CFTC_METRIC_UNITS:
            raise ValueError(f"Unsupported CFTC metric: {metric_key}")

        cache_key_parts = {"contract": contract}
        if force_refresh:
            self._clear_metric_cache("cftc_disagg_futures_only", cache_key_parts)

        try:
            wide, cache_info = self._cached_timeseries(
                metric_key="cftc_disagg_futures_only",
                start=start,
                end=end,
                cache_key_parts=cache_key_parts,
                fetch_ctx={"contract": contract},
                allow_internal_gap_fill_daily=False,
                expected_calendar="W-FRI",
            )
        except Exception as exc:
            message = str(exc)
            is_missing_date = isinstance(exc, KeyError) and str(exc).strip("'") == "date"
            is_duplicate_cols = "Duplicate column names found" in message
            if not is_missing_date and not is_duplicate_cols:
                raise
            logger.warning(
                "cftc_bad_cache contract=%s retrying_with_fresh_cache reason=%s",
                contract,
                "missing_date" if is_missing_date else "duplicate_columns",
            )
            self._clear_metric_cache("cftc_disagg_futures_only", cache_key_parts)
            wide, cache_info = self._cached_timeseries(
                metric_key="cftc_disagg_futures_only",
                start=start,
                end=end,
                cache_key_parts=cache_key_parts,
                fetch_ctx={"contract": contract},
                allow_internal_gap_fill_daily=False,
                expected_calendar="W-FRI",
            )
        if wide.empty:
            long_df = pd.DataFrame(
                columns=["date", "value", "metric", "unit", "source", "source_ref", "contract"]
            )
        else:
            long_df = self._to_long_format(wide, metric_key=metric_key, contract=contract)

        source = self._make_source(
            label=f"CFTC COT Disaggregated Futures Only: {metric_key}",
            reference="cftc:cot:disaggregated_futures_only",
            parameters={
                "metric": metric_key,
                "contract": contract,
                "start": start,
                "end": end,
                "cache": cache_info.__dict__,
            },
        )
        return CFTCResult(df=long_df, source=source, meta={"metric": metric_key, "cache": cache_info.__dict__})

    def managed_money_long(self, *, start: str, end: str) -> CFTCResult:
        return self.get_metric("managed_money_long", start=start, end=end)

    def managed_money_short(self, *, start: str, end: str) -> CFTCResult:
        return self.get_metric("managed_money_short", start=start, end=end)

    def managed_money_net(self, *, start: str, end: str) -> CFTCResult:
        return self.get_metric("managed_money_net", start=start, end=end)

    def managed_money_net_percentile_156w(self, *, start: str, end: str) -> CFTCResult:
        return self.get_metric("managed_money_net_percentile_156w", start=start, end=end)

    def open_interest(self, *, start: str, end: str) -> CFTCResult:
        return self.get_metric("open_interest", start=start, end=end)

    def _fetch_timeseries(self, start: str, end: str, **kwargs) -> pd.DataFrame:
        contract = str(kwargs.get("contract") or HENRY_HUB_CONTRACT)
        logger.info("cftc_fetch start=%s end=%s contract=%s", start, end, contract)
        frames = [self._fetch_current_disagg(), *self._fetch_historical_disagg()]
        combined = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
        combined = self._normalize_df(combined)
        combined = self._filter_contract(combined, contract=contract)
        combined = self._derive_metrics(combined)
        return self._canonicalize_wide_frame(combined)

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "market_name", "exchange_name", *CFTC_METRIC_UNITS.keys()])

        out = df.copy()
        if out.columns.duplicated().any():
            out = out.loc[:, ~out.columns.duplicated(keep="last")].copy()
        out.columns = [self._normalize_column_name(col) for col in out.columns]
        if out.columns.duplicated().any():
            out = out.loc[:, ~out.columns.duplicated(keep="last")].copy()
        rename_map = {col: _COLUMN_MAP[col] for col in out.columns if col in _COLUMN_MAP}
        out = out.rename(columns=rename_map)
        if out.columns.duplicated().any():
            out = out.loc[:, ~out.columns.duplicated(keep="last")].copy()

        if "date" not in out.columns:
            fallback_date_col = next(
                (
                    col
                    for col in out.columns
                    if "date" in col and any(token in col for token in ("yyyy", "yy", "form"))
                ),
                None,
            )
            if fallback_date_col is not None:
                if "yy" in fallback_date_col and "yyyy" not in fallback_date_col:
                    out["date_yy"] = out[fallback_date_col]
                else:
                    out["date"] = out[fallback_date_col]

        if "date" not in out.columns and "date_yy" in out.columns:
            out["date"] = pd.to_datetime(out["date_yy"], format="%y%m%d", errors="coerce")
        elif "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
        else:
            raise ValueError(
                "CFTC parser could not find a date column. "
                f"Normalized columns: {sorted(out.columns.tolist())}"
            )
        out["date"] = out["date"].dt.normalize()

        if "market_and_exchange_names" not in out.columns:
            if "market_name" in out.columns and "exchange_name" in out.columns:
                out["market_and_exchange_names"] = (
                    out["market_name"].astype(str).str.strip()
                    + " - "
                    + out["exchange_name"].astype(str).str.strip()
                )
            else:
                fallback_market_col = next(
                    (
                        col
                        for col in out.columns
                        if "market" in col and "exchange" in col
                    ),
                    None,
                )
                if fallback_market_col is not None:
                    out["market_and_exchange_names"] = out[fallback_market_col]
                else:
                    raise ValueError(
                        "CFTC parser could not find market/exchange names column. "
                        f"Normalized columns: {sorted(out.columns.tolist())}"
                    )

        market_split = out["market_and_exchange_names"].astype(str).str.split(" - ", n=1, expand=True)
        out["market_name"] = market_split[0].fillna("").str.strip()
        out["exchange_name"] = market_split[1].fillna("").str.strip()

        for column in (
            "open_interest",
            "producer_long",
            "producer_short",
            "producer_spread",
            "swap_dealer_long",
            "swap_dealer_short",
            "swap_dealer_spread",
            "managed_money_long",
            "managed_money_short",
            "managed_money_spread",
            "other_reportables_long",
            "other_reportables_short",
            "other_reportables_spread",
            "nonreportable_long",
            "nonreportable_short",
        ):
            if column not in out.columns:
                out[column] = pd.NA
            out[column] = pd.to_numeric(out[column], errors="coerce")

        out = out.dropna(subset=["date", "market_name", "exchange_name"])
        return out.reset_index(drop=True)

    def _dedupe_cols(self, df: pd.DataFrame) -> list[str]:
        return ["date", "market_name", "exchange_name"]

    @staticmethod
    def _normalize_column_name(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")

    @staticmethod
    def _normalize_contract_name(value: str) -> str:
        text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
        text = re.sub(r"\s+", " ", text)
        return text

    def _contract_matches(self, market_name: str, exchange_name: str) -> bool:
        market_norm = self._normalize_contract_name(market_name)
        exchange_norm = self._normalize_contract_name(exchange_name)
        market_has_henry = "henry hub" in market_norm
        market_has_natgas = (
            market_norm == "henry hub"
            or
            "natural gas" in market_norm
            or "nat gas" in market_norm
            or re.search(r"\bng\b", market_norm) is not None
        )
        exchange_matches = "new york mercantile exchange" in exchange_norm or "nymex" in exchange_norm
        excludes = any(
            token in market_norm
            for token in ("basis", "penultimate", "ld1", "last day", "fixed", "mini", "financial")
        )
        return market_has_henry and market_has_natgas and exchange_matches and not excludes

    def _filter_contract(self, df: pd.DataFrame, *, contract: str) -> pd.DataFrame:
        matched = df.loc[
            df.apply(
                lambda row: self._contract_matches(row["market_name"], row["exchange_name"]),
                axis=1,
            )
        ].copy()
        if matched.empty:
            raise ValueError("No Henry Hub Natural Gas CFTC rows found after normalization/filtering.")

        grouped = matched.groupby("date")[["market_name", "exchange_name"]].nunique()
        multi_match_dates = grouped.loc[(grouped["market_name"] > 1) | (grouped["exchange_name"] > 1)]
        if not multi_match_dates.empty:
            raise ValueError(
                "Multiple Henry Hub Natural Gas CFTC contracts matched on dates: "
                + ", ".join(str(idx.date()) for idx in multi_match_dates.index[:5])
            )

        matched["contract"] = contract
        matched = matched.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        logger.info("cftc_filter contract=%s rows=%d", contract, len(matched))
        return matched.reset_index(drop=True)

    def _derive_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.sort_values("date").reset_index(drop=True).copy()
        out["managed_money_net"] = out["managed_money_long"] - out["managed_money_short"]
        out["producer_net"] = out["producer_long"] - out["producer_short"]
        out["swap_dealer_net"] = out["swap_dealer_long"] - out["swap_dealer_short"]
        out["other_reportables_net"] = (
            out["other_reportables_long"] - out["other_reportables_short"]
        )
        out["managed_money_net_pct_oi"] = (
            out["managed_money_net"] / out["open_interest"].replace({0: pd.NA})
        ) * 100.0
        rolling_mean = out["managed_money_net"].rolling(window=52, min_periods=26).mean()
        rolling_std = out["managed_money_net"].rolling(window=52, min_periods=26).std(ddof=0)
        out["managed_money_net_zscore_52w"] = (
            out["managed_money_net"] - rolling_mean
        ) / rolling_std.replace({0: pd.NA})
        percentile = out["managed_money_net"].rolling(window=156, min_periods=26).apply(
            lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100.0,
            raw=False,
        )
        out["managed_money_net_percentile_156w"] = percentile
        out["managed_money_wow_change"] = out["managed_money_net"].diff()
        out["open_interest_wow_change"] = out["open_interest"].diff()
        return out

    def _to_long_format(self, df: pd.DataFrame, *, metric_key: str, contract: str) -> pd.DataFrame:
        if metric_key not in df.columns:
            raise ValueError(f"Metric {metric_key} not found in normalized CFTC dataframe.")
        source_ref = f"cftc:cot:disaggregated_futures_only:{contract}"
        return pd.DataFrame(
            {
                "date": pd.to_datetime(df["date"], errors="coerce"),
                "value": pd.to_numeric(df[metric_key], errors="coerce"),
                "metric": metric_key,
                "unit": CFTC_METRIC_UNITS[metric_key],
                "source": "CFTC",
                "source_ref": source_ref,
                "contract": contract,
            }
        ).dropna(subset=["date"]).reset_index(drop=True)

    def _canonicalize_wide_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for column in CFTC_CANONICAL_WIDE_COLUMNS:
            if column not in out.columns:
                out[column] = pd.NA

        out = out[CFTC_CANONICAL_WIDE_COLUMNS].copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
        for column in out.columns:
            if column in {"date", "market_name", "exchange_name", "contract"}:
                continue
            out[column] = pd.to_numeric(out[column], errors="coerce")
        out["market_name"] = out["market_name"].astype(str)
        out["exchange_name"] = out["exchange_name"].astype(str)
        out["contract"] = out["contract"].astype(str)
        return out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    def _fetch_current_disagg(self) -> pd.DataFrame:
        index_path = self.raw_dir / "current_index.html"
        if not index_path.exists():
            response = self.session.get(CFTC_COT_INDEX_URL, timeout=30)
            response.raise_for_status()
            index_path.write_text(response.text, encoding="utf-8")
        doc = html.fromstring(index_path.read_text(encoding="utf-8"))
        href = None
        for node in doc.xpath("//a[@href]"):
            text = " ".join(node.xpath(".//text()")).strip().lower()
            if "disaggregated futures-only commitments of traders comma delimited" in text:
                href = node.get("href")
                break
        if not href:
            raise ValueError("Unable to locate current CFTC disaggregated futures-only file link.")
        url = urljoin(CFTC_COT_INDEX_URL, href)
        out_path = self.raw_dir / Path(url).name
        if not out_path.exists():
            self._download_to_path(url, out_path)
        try:
            return self._parse_file_bytes(out_path.read_bytes(), file_name=out_path.name)
        except Exception:
            if out_path.exists():
                out_path.unlink()
            self._download_to_path(url, out_path)
            return self._parse_file_bytes(out_path.read_bytes(), file_name=out_path.name)

    def _fetch_historical_disagg(self) -> list[pd.DataFrame]:
        index_path = self.raw_dir / "historical_index.html"
        if not index_path.exists():
            response = self.session.get(CFTC_HISTORICAL_URL, timeout=30)
            response.raise_for_status()
            index_path.write_text(response.text, encoding="utf-8")
        doc = html.fromstring(index_path.read_text(encoding="utf-8"))

        links_by_year: dict[int, dict[str, str]] = {}
        for node in doc.xpath("//a[@href]"):
            href = str(node.get("href") or "").strip()
            if not href:
                continue

            href_lower = href.lower()
            text_match = re.search(r"fut_disagg_txt_(20\d{2})\.zip$", href_lower)
            excel_match = re.search(r"fut_disagg_xls_(20\d{2})\.zip$", href_lower)
            if text_match:
                year = int(text_match.group(1))
                if year >= 2009:
                    links_by_year.setdefault(year, {})["text"] = urljoin(
                        CFTC_HISTORICAL_URL, href
                    )
                continue
            if excel_match:
                year = int(excel_match.group(1))
                if year >= 2009:
                    links_by_year.setdefault(year, {})["excel"] = urljoin(
                        CFTC_HISTORICAL_URL, href
                    )

        if not links_by_year:
            raise ValueError(
                "Unable to locate CFTC historical disaggregated futures-only archive links."
            )

        frames: list[pd.DataFrame] = []
        for year in sorted(y for y in links_by_year.keys() if y >= 2009):
            links = links_by_year[year]
            url = links.get("text") or links.get("excel")
            if not url:
                continue
            out_path = self.raw_dir / f"historical_{year}{Path(url).suffix or '.zip'}"
            if not out_path.exists():
                self._download_to_path(url, out_path)
            try:
                frames.append(self._parse_file_bytes(out_path.read_bytes(), file_name=out_path.name))
            except Exception:
                if out_path.exists():
                    out_path.unlink()
                self._download_to_path(url, out_path)
                frames.append(self._parse_file_bytes(out_path.read_bytes(), file_name=out_path.name))
        return frames

    def _parse_file_bytes(self, data: bytes, *, file_name: str) -> pd.DataFrame:
        suffix = Path(file_name).suffix.lower()
        if self._looks_like_html(data):
            raise ValueError(
                f"CFTC download for {file_name} returned HTML instead of a data file."
            )
        if suffix == ".txt":
            return self._read_csv(io.BytesIO(data))
        if suffix in {".xls", ".xlsx"}:
            return self._read_excel(io.BytesIO(data))
        if suffix == ".zip":
            if zipfile.is_zipfile(io.BytesIO(data)):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    members = [name for name in zf.namelist() if not name.endswith("/")]
                    if not members:
                        raise ValueError(f"Empty CFTC zip archive: {file_name}")
                    preferred = sorted(
                        members,
                        key=lambda name: (
                            0 if name.lower().endswith(".txt") else 1,
                            0 if name.lower().endswith((".xlsx", ".xls")) else 1,
                            name,
                        ),
                    )[0]
                    with zf.open(preferred) as handle:
                        payload = handle.read()
                    return self._parse_file_bytes(payload, file_name=preferred)
            # Some CFTC links may return plain text content even when the URL path suggests an archive.
            if self._looks_like_csv_or_txt(data):
                return self._read_csv(io.BytesIO(data))
            raise ValueError(
                f"CFTC file {file_name} was expected to be a zip archive but was not a valid zip payload."
            )
        raise ValueError(f"Unsupported CFTC file format: {file_name}")

    @staticmethod
    def _read_csv(buffer: io.BytesIO) -> pd.DataFrame:
        try:
            frame = pd.read_csv(buffer)
        except Exception:
            buffer.seek(0)
            frame = pd.read_csv(buffer, header=None)
        return CFTCAdapter._promote_header_row(frame)

    @staticmethod
    def _read_excel(buffer: io.BytesIO) -> pd.DataFrame:
        sheets = pd.read_excel(buffer, sheet_name=None, engine="openpyxl")
        if len(sheets) != 1:
            frame = next(iter(sheets.values()))
        else:
            frame = list(sheets.values())[0]
        return CFTCAdapter._promote_header_row(frame)

    def _make_source(self, *, label: str, reference: str, parameters: dict[str, Any]) -> SourceRef:
        return SourceRef(
            source_type="cftc",
            label=label,
            reference=reference,
            parameters=parameters,
            retrieved_at=datetime.utcnow(),
        )

    def _download_to_path(self, url: str, path: Path) -> None:
        response = self.session.get(
            url,
            timeout=60,
            headers={"Referer": CFTC_HISTORICAL_URL},
        )
        response.raise_for_status()
        path.write_bytes(response.content)

    @staticmethod
    def _looks_like_html(data: bytes) -> bool:
        sample = data[:512].lstrip().lower()
        return sample.startswith(b"<!doctype html") or sample.startswith(b"<html") or b"<html" in sample

    @staticmethod
    def _looks_like_csv_or_txt(data: bytes) -> bool:
        sample = data[:1024].decode("utf-8", errors="ignore").lower()
        return "market and exchange" in sample or "open interest" in sample or sample.count(",") > 5

    def _clear_metric_cache(self, metric_key: str, cache_key_parts: dict[str, Any]) -> None:
        cache_path = self._cache_path(metric_key, cache_key_parts)
        for suffix in (".parquet", ".csv"):
            candidate = cache_path.with_suffix(suffix)
            if candidate.exists():
                candidate.unlink()

    @staticmethod
    def _promote_header_row(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame()

        def _norm_cell(value: Any) -> str:
            return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")

        expected_markers = {
            "market_and_exchange_names",
            "open_interest_all",
            "as_of_date_in_form_yyyy_mm_dd",
            "as_of_date_form_yyyy_mm_dd",
            "as_of_date_in_form_yy_mm_dd",
        }

        current_cols = {_norm_cell(col) for col in frame.columns}
        if current_cols & expected_markers:
            return frame

        search = frame.reset_index(drop=True).copy()
        max_rows = min(len(search), 25)
        for idx in range(max_rows):
            row_values = [_norm_cell(value) for value in search.iloc[idx].tolist()]
            row_set = {value for value in row_values if value}
            if "market_and_exchange_names" in row_set and any("date" in value for value in row_set):
                promoted = search.iloc[idx + 1 :].copy()
                promoted.columns = search.iloc[idx].tolist()
                return promoted.reset_index(drop=True)
            if len(row_set & expected_markers) >= 2:
                promoted = search.iloc[idx + 1 :].copy()
                promoted.columns = search.iloc[idx].tolist()
                return promoted.reset_index(drop=True)

        return frame
