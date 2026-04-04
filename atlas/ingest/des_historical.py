from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from lxml import html

DES_HISTORICAL_URL = "https://www.dallasfed.org/research/surveys/des/data"
BASE_URL = "https://www.dallasfed.org"
DEFAULT_RAW_DIR = Path("data/raw/des")
DEFAULT_PROCESSED_DIR = Path("data/processed/des")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

CANONICAL_COLUMN_PATTERNS: list[tuple[str, str, str]] = [
    (r"\bbusiness activity\b", "des_business_activity_index", "index"),
    (r"\bcompany outlook\b", "des_company_outlook_index", "index"),
    (r"\buncertainty\b", "des_outlook_uncertainty_index", "index"),
    (r"\boil production\b", "des_oil_production_index", "index"),
    (r"\bnatural gas wellhead production\b|\bgas production\b", "des_gas_production_index", "index"),
    (r"\bcapital expenditures\b|\bcapex\b", "des_capex_index", "index"),
    (r"\bnumber of employees\b|\bemployment\b", "des_employment_index", "index"),
    (r"\binput costs\b", "des_input_cost_index", "index"),
    (r"\bfinding and development costs\b", "des_finding_development_costs_index", "index"),
    (r"\blease operating expenses\b", "des_lease_operating_expense_index", "index"),
    (r"\bprices received for services\b", "des_prices_received_services_index", "index"),
    (r"\butilization of equipment\b|\bequipment utilization\b", "des_equipment_utilization_index", "index"),
    (r"\boperating margin\b", "des_operating_margin_index", "index"),
    (r"\bwti\b.*\b6 month|\b6 month\b.*\bwti\b", "des_wti_price_expectation_6m", "usd_per_bbl"),
    (r"\bwti\b.*\b1 year|\b1 year\b.*\bwti\b|\bwti\b.*\b12 month", "des_wti_price_expectation_1y", "usd_per_bbl"),
    (r"\bwti\b.*\b2 year", "des_wti_price_expectation_2y", "usd_per_bbl"),
    (r"\bwti\b.*\b5 year", "des_wti_price_expectation_5y", "usd_per_bbl"),
    (r"\bhenry hub\b.*\b6 month|\b6 month\b.*\bhenry hub\b", "des_hh_price_expectation_6m", "usd_per_mmbtu"),
    (r"\bhenry hub\b.*\b1 year|\b1 year\b.*\bhenry hub\b|\bhenry hub\b.*\b12 month", "des_hh_price_expectation_1y", "usd_per_mmbtu"),
    (r"\bhenry hub\b.*\b2 year", "des_hh_price_expectation_2y", "usd_per_mmbtu"),
    (r"\bhenry hub\b.*\b5 year", "des_hh_price_expectation_5y", "usd_per_mmbtu"),
    (r"\bbreak[- ]?even oil\b.*\bpermian\b", "des_breakeven_oil_permian", "usd_per_bbl"),
    (r"\bbreak[- ]?even oil\b.*\beagle ford\b", "des_breakeven_oil_eagle_ford", "usd_per_bbl"),
    (r"\bbreak[- ]?even oil\b.*\bu\.?s\.?\b|\bbreak[- ]?even oil\b.*\bunited states\b", "des_breakeven_oil_us", "usd_per_bbl"),
    (r"\bbreak[- ]?even gas\b.*\bu\.?s\.?\b|\bbreak[- ]?even gas\b.*\bunited states\b", "des_breakeven_gas_us", "usd_per_mmbtu"),
]

DOWNLOAD_CATEGORY_HINTS = {
    "index": "index",
    "all data": "all_data",
    "price expectations": "price_expectations",
    "breakeven": "breakeven",
}


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "des"


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _quarter_end(year: int, quarter: int) -> pd.Timestamp:
    month = quarter * 3
    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)


def _infer_release_date(value: Any) -> pd.Timestamp | pd.NaT:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value.normalize()
    text = _normalize_text(value)
    if not text:
        return pd.NaT

    quarter_match = re.search(r"(?P<year>20\d{2})\s*[Qq](?P<quarter>[1-4])", text)
    if quarter_match:
        return _quarter_end(
            int(quarter_match.group("year")), int(quarter_match.group("quarter"))
        )

    text = re.sub(
        r"\b(first|second|third|fourth)\s+quarter\b",
        lambda m: {
            "first quarter": "Q1",
            "second quarter": "Q2",
            "third quarter": "Q3",
            "fourth quarter": "Q4",
        }[m.group(0).lower()],
        text,
        flags=re.I,
    )
    quarter_match = re.search(r"\bQ(?P<quarter>[1-4])\b.*?(?P<year>20\d{2})", text, flags=re.I)
    if quarter_match:
        return _quarter_end(
            int(quarter_match.group("year")), int(quarter_match.group("quarter"))
        )

    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    return parsed.normalize()


def _derive_quarter_label(ts: pd.Timestamp | pd.NaT) -> str | None:
    if pd.isna(ts):
        return None
    return f"{ts.year}Q{((ts.month - 1) // 3) + 1}"


def _infer_region(column_name: str, category: str, row: pd.Series) -> str:
    explicit_region = _normalize_text(
        row.get("region") or row.get("basin") or row.get("location") or row.get("area")
    )
    if explicit_region:
        return explicit_region.lower().replace(" ", "_")

    name = column_name.lower()
    if "permian" in name:
        return "permian"
    if "eagle ford" in name:
        return "eagle_ford"
    if "u.s." in name or "united states" in name or "us" in name:
        return "us"
    if "price expectation" in category:
        return "us"
    return "us"


def _map_metric(column_name: str, category: str) -> tuple[str | None, str]:
    normalized = _normalize_text(column_name).lower()
    for pattern, metric, unit in CANONICAL_COLUMN_PATTERNS:
        if re.search(pattern, normalized, flags=re.I):
            return metric, unit

    if category == "price_expectations":
        if "wti" in normalized:
            return f"des_wti_price_expectation_{_slugify(normalized.split('wti')[-1])}", "usd_per_bbl"
        if "henry hub" in normalized:
            return f"des_hh_price_expectation_{_slugify(normalized.split('henry hub')[-1])}", "usd_per_mmbtu"

    return None, "value"


def _source_filename(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    if name:
        return name
    return f"des_{_hash_text(url)}.bin"


def _cache_path_for_url(raw_dir: Path, url: str) -> Path:
    filename = _source_filename(url)
    return raw_dir / filename


def _read_source_file(path: Path) -> dict[str, pd.DataFrame]:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=None)
    if suffix == ".csv":
        return {"sheet1": pd.read_csv(path)}
    if suffix == ".json":
        frame = pd.read_json(path)
        return {"sheet1": frame}
    raise ValueError(f"Unsupported DES source file format: {path.name}")


def _find_date_column(columns: Iterable[str]) -> str | None:
    priority = [
        "release_date",
        "report_date",
        "date",
        "survey_date",
        "quarter",
        "period",
        "year",
    ]
    lowered = {str(col).lower(): str(col) for col in columns}
    for candidate in priority:
        if candidate in lowered:
            return lowered[candidate]
    for col in columns:
        if any(token in str(col).lower() for token in ("date", "quarter", "period", "year")):
            return str(col)
    return None


def _normalize_wide_sheet(
    df: pd.DataFrame,
    *,
    category: str,
    source_url: str,
    file_name: str,
) -> pd.DataFrame:
    working = df.copy()
    working.columns = [_normalize_text(c) for c in working.columns]
    working = working.dropna(how="all")
    if working.empty:
        return pd.DataFrame()

    date_col = _find_date_column(working.columns)
    if date_col is None:
        return pd.DataFrame()

    working["release_date"] = working[date_col].apply(_infer_release_date)
    working["quarter"] = working["release_date"].apply(_derive_quarter_label)
    working = working.dropna(subset=["release_date"])
    if working.empty:
        return pd.DataFrame()

    id_vars = [col for col in working.columns if col in {date_col, "release_date", "quarter", "region", "basin", "location", "area", "unit"}]
    value_vars = [
        col
        for col in working.columns
        if col not in id_vars and pd.api.types.is_numeric_dtype(working[col])
    ]
    if not value_vars and {"metric", "value"}.issubset(set(c.lower() for c in working.columns)):
        rename_map = {col: col.lower() for col in working.columns}
        working = working.rename(columns=rename_map)
        tidy = working.copy()
        tidy["date"] = tidy["release_date"]
        tidy["metric"] = tidy["metric"].map(lambda x: _map_metric(str(x), category)[0] or _slugify(str(x)))
        tidy["unit"] = tidy.get("unit", "value").fillna("value")
        tidy["region"] = tidy.apply(lambda row: _infer_region(str(row.get("metric", "")), category, row), axis=1)
        tidy["frequency"] = "quarterly"
        tidy["source"] = "Dallas Fed"
        tidy["source_url"] = source_url
        tidy["vintage"] = datetime.utcnow().date().isoformat()
        tidy["file_name"] = file_name
        return tidy[
            ["date", "quarter", "metric", "value", "unit", "region", "frequency", "source", "source_url", "release_date", "vintage", "file_name"]
        ]

    if not value_vars:
        return pd.DataFrame()

    melted = working.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="raw_metric",
        value_name="value",
    )
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
    melted = melted.dropna(subset=["value"])
    if melted.empty:
        return pd.DataFrame()

    metric_meta = melted["raw_metric"].map(lambda x: _map_metric(str(x), category))
    melted["metric"] = metric_meta.map(lambda item: item[0] if item else None)
    melted["unit"] = metric_meta.map(lambda item: item[1] if item else "value")
    melted = melted.dropna(subset=["metric"])
    if melted.empty:
        return pd.DataFrame()

    melted["date"] = melted["release_date"]
    melted["region"] = melted.apply(
        lambda row: _infer_region(str(row["raw_metric"]), category, row), axis=1
    )
    melted["frequency"] = "quarterly"
    melted["source"] = "Dallas Fed"
    melted["source_url"] = source_url
    melted["vintage"] = datetime.utcnow().date().isoformat()
    melted["file_name"] = file_name

    return melted[
        ["date", "quarter", "metric", "value", "unit", "region", "frequency", "source", "source_url", "release_date", "vintage", "file_name"]
    ]


def _normalize_frames(
    frames: dict[str, pd.DataFrame],
    *,
    category: str,
    source_url: str,
    file_name: str,
) -> pd.DataFrame:
    out_frames: list[pd.DataFrame] = []
    for frame in frames.values():
        normalized = _normalize_wide_sheet(
            frame,
            category=category,
            source_url=source_url,
            file_name=file_name,
        )
        if not normalized.empty:
            out_frames.append(normalized)
    if not out_frames:
        return pd.DataFrame(
            columns=[
                "date",
                "quarter",
                "metric",
                "value",
                "unit",
                "region",
                "frequency",
                "source",
                "source_url",
                "release_date",
                "vintage",
                "file_name",
            ]
        )
    combined = pd.concat(out_frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined["release_date"] = pd.to_datetime(combined["release_date"], errors="coerce")
    combined = combined.dropna(subset=["date"]).sort_values(["metric", "region", "date"])
    combined = combined.drop_duplicates(subset=["date", "metric", "region", "file_name"])
    return combined.reset_index(drop=True)


def fetch_des_historical_links(
    *,
    session: requests.Session | None = None,
    raw_dir: str | Path = DEFAULT_RAW_DIR,
    force_refresh: bool = False,
    url: str = DES_HISTORICAL_URL,
) -> list[dict[str, str]]:
    raw_root = Path(raw_dir)
    raw_root.mkdir(parents=True, exist_ok=True)
    page_path = raw_root / "historical_page.html"

    if force_refresh or not page_path.exists():
        sess = session or requests.Session()
        sess.headers.update(HEADERS)
        response = sess.get(url, timeout=30)
        response.raise_for_status()
        page_path.write_text(response.text, encoding="utf-8")

    doc = html.fromstring(page_path.read_text(encoding="utf-8"))
    current_category = ""
    rows: list[dict[str, str]] = []
    for node in doc.xpath("//body//*[self::h2 or self::h3 or self::a]"):
        tag = getattr(node, "tag", "").lower()
        if tag in {"h2", "h3"}:
            current_category = _normalize_text(" ".join(node.xpath(".//text()")))
            continue
        href = node.get("href", "")
        label = _normalize_text(" ".join(node.xpath(".//text()")))
        if not href or not label:
            continue
        full_url = urljoin(BASE_URL, href)
        if full_url == url or "historical" in label.lower():
            continue
        if any(key in label.lower() for key in DOWNLOAD_CATEGORY_HINTS) or full_url.lower().endswith((".xlsx", ".xls", ".csv")):
            category = "other"
            current_lower = current_category.lower()
            for needle, mapped in DOWNLOAD_CATEGORY_HINTS.items():
                if needle in current_lower or needle in label.lower():
                    category = mapped
                    break
            rows.append(
                {
                    "category": category,
                    "label": label,
                    "url": full_url,
                }
            )
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (row["category"], row["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def download_des_historical_file(
    link: dict[str, str],
    *,
    session: requests.Session | None = None,
    raw_dir: str | Path = DEFAULT_RAW_DIR,
    force_refresh: bool = False,
) -> Path:
    raw_root = Path(raw_dir)
    raw_root.mkdir(parents=True, exist_ok=True)
    out_path = _cache_path_for_url(raw_root, link["url"])
    if out_path.exists() and not force_refresh:
        return out_path

    sess = session or requests.Session()
    sess.headers.update(HEADERS)
    response = sess.get(link["url"], timeout=60)
    response.raise_for_status()
    out_path.write_bytes(response.content)
    meta_path = out_path.with_suffix(out_path.suffix + ".json")
    meta_path.write_text(json.dumps(link, indent=2), encoding="utf-8")
    return out_path


def _fetch_by_category(
    category: str,
    *,
    links: list[dict[str, str]] | None = None,
    raw_dir: str | Path = DEFAULT_RAW_DIR,
    force_refresh: bool = False,
) -> pd.DataFrame:
    available_links = links or fetch_des_historical_links(raw_dir=raw_dir, force_refresh=force_refresh)
    frames: list[pd.DataFrame] = []
    for link in available_links:
        if link["category"] != category:
            continue
        path = download_des_historical_file(link, raw_dir=raw_dir, force_refresh=force_refresh)
        normalized = _normalize_frames(
            _read_source_file(path),
            category=category,
            source_url=link["url"],
            file_name=path.name,
        )
        if not normalized.empty:
            frames.append(normalized)
    return build_des_timeseries(frames)


def fetch_des_historical_index_data(**kwargs: Any) -> pd.DataFrame:
    return _fetch_by_category("index", **kwargs)


def fetch_des_historical_all_data(**kwargs: Any) -> pd.DataFrame:
    return _fetch_by_category("all_data", **kwargs)


def fetch_des_price_expectations(**kwargs: Any) -> pd.DataFrame:
    return _fetch_by_category("price_expectations", **kwargs)


def fetch_des_breakeven_data(**kwargs: Any) -> pd.DataFrame:
    return _fetch_by_category("breakeven", **kwargs)


def build_des_timeseries(frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame.copy() for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        return pd.DataFrame(
            columns=[
                "date",
                "quarter",
                "metric",
                "value",
                "unit",
                "region",
                "frequency",
                "source",
                "source_url",
                "release_date",
                "vintage",
                "file_name",
            ]
        )
    combined = pd.concat(non_empty, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined["release_date"] = pd.to_datetime(combined["release_date"], errors="coerce")
    combined["value"] = pd.to_numeric(combined["value"], errors="coerce")
    combined = combined.dropna(subset=["date", "metric", "value"])
    combined["quarter"] = combined["quarter"].fillna(combined["date"].map(_derive_quarter_label))
    combined["frequency"] = combined["frequency"].fillna("quarterly")
    combined["source"] = combined["source"].fillna("Dallas Fed")
    combined["region"] = combined["region"].fillna("us")
    combined = combined.sort_values(["metric", "region", "date"])
    combined = combined.drop_duplicates(subset=["date", "metric", "region", "file_name"])
    return combined.reset_index(drop=True)
