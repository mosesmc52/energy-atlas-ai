from __future__ import annotations
import hashlib
import json
import re
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def absolute_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    href = re.sub(r"\s+", "", href)
    if not href:
        return None
    return urljoin(base.rstrip("/") + "/", href)

def extract_topics_from_text(text: str) -> list[str]:
    t = (text or "").lower()
    topic_map = {
        "storage": ["storage", "withdrawal", "injection", "inventories", "stocks"],
        "lng": ["lng", "liquefied natural gas", "exports", "export"],
        "production": ["production", "output", "dry gas", "marketed gas"],
        "prices": ["henry hub", "spot price", "futures", "mmbtu", "price"],
        "weather": ["weather", "hdd", "heating degree days", "cold", "winter storm"],
        "power": ["electricity", "power sector", "generation", "load"],
    }
    out = []
    for topic, keywords in topic_map.items():
        if any(k in t for k in keywords):
            out.append(topic)
    return sorted(set(out))


def infer_metric_tags_from_text(text: str) -> list[str]:
    t = (text or "").lower()
    tag_map = {
        "working_gas": ["working gas"],
        "base_gas": ["base gas", "cushion gas"],
        "injections": ["injection", "injections", "inject"],
        "withdrawals": ["withdrawal", "withdrawals", "withdrawn"],
        "net_withdrawals": ["net withdrawal", "net withdrawals", "net withdrawl", "net withdrawls"],
        "lng": ["lng", "liquefied natural gas"],
        "production": ["production", "dry gas", "marketed gas", "output"],
        "consumption": ["consumption", "demand"],
        "prices": ["henry hub", "price", "prices", "mmbtu", "futures"],
        "capacity": ["capacity"],
        "storage_field_count": ["storage fields", "field count", "number of storage fields"],
    }
    out = []
    for tag, keywords in tag_map.items():
        if any(keyword in t for keyword in keywords):
            out.append(tag)
    return sorted(set(out))


def infer_geography_tags_from_text(text: str) -> list[str]:
    t = (text or "").lower()
    geography_map = {
        "lower48": ["lower 48", "lower48"],
        "east": [" east "],
        "midwest": [" midwest "],
        "south_central": ["south central", "south_central"],
        "mountain": [" mountain "],
        "pacific": [" pacific "],
        "united_states_total": ["united states", "u.s.", "u.s", "national"],
    }
    padded = f" {t} "
    out = []
    for tag, keywords in geography_map.items():
        if any(keyword in padded for keyword in keywords):
            out.append(tag)
    return sorted(set(out))

def make_doc_id(source: str, report_type: str, title: str, url: str) -> str:
    raw = f"{source}|{report_type}|{title}|{url}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]

def chunk_text(text: str, chunk_size: int = 800, overlap: int = 120) -> list[str]:
    text = clean_text(text)
    if not text:
        return []
    words = text.split()
    chunks: list[str] = []
    start = 0
    while start < len(words):
        end = min(len(words), start + chunk_size)
        chunks.append(" ".join(words[start:end]))
        if end == len(words):
            break
        start = max(0, end - overlap)
    return chunks

def write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
