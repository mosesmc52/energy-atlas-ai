from __future__ import annotations

from typing import Any

from scripts.eia.rag.report_registry import report_types_for_families

NARRATIVE_TERMS = (
    "why",
    "driver",
    "drivers",
    "explain",
    "report",
    "reports",
    "what did eia say",
    "weekly report",
    "commentary",
    "context",
    "tightening",
    "loosening",
)


def select_report_filters(query: str, route: Any | None) -> dict[str, object]:
    normalized_query = str(query or "").strip().lower()
    report_families: list[str] = []
    domain_tags: list[str] = []
    metric_tags: list[str] = []
    geography_tags: list[str] = []
    top_k = 4
    recency_days = 180

    if route is not None and str(getattr(route, "domain", "") or "") == "storage":
        domain_tags.append("storage")
        storage_dataset = str(getattr(route, "storage_dataset", "") or "")
        storage_metric_type = str(getattr(route, "storage_metric_type", "") or "")
        storage_type = str(getattr(route, "storage_type", "") or "")
        states = [str(s) for s in list(getattr(route, "states", []) or []) if str(s)]
        regions = [str(r) for r in list(getattr(route, "regions", []) or []) if str(r)]

        if storage_dataset == "lng_storage":
            metric_tags.append("lng")
            report_families.extend(["natural_gas_weekly", "today_in_energy_natural_gas"])
        elif storage_dataset == "weekly_working_gas":
            report_families.extend(["natural_gas_weekly", "wngsr_supplement"])
        elif storage_dataset in {"underground_storage_all_operators", "underground_storage_by_type"}:
            report_families.extend(["natural_gas_weekly", "wngsr_supplement"])

        metric_tag_map = {
            "working_gas": "working_gas",
            "base_gas": "base_gas",
            "injections": "injections",
            "withdrawals": "withdrawals",
            "net_withdrawals": "net_withdrawals",
            "lng_storage_additions": "lng",
            "lng_storage_withdrawals": "lng",
            "lng_storage_net_withdrawals": "lng",
        }
        mapped_tag = metric_tag_map.get(storage_metric_type)
        if mapped_tag:
            metric_tags.append(mapped_tag)
        if storage_type:
            metric_tags.append(storage_type)
        geography_tags.extend(states)
        geography_tags.extend(regions)

    if "outlook" in normalized_query:
        report_families.append("steo_natural_gas")
        recency_days = max(recency_days, 365)
    if any(term in normalized_query for term in ("today in energy", "analysis")):
        report_families.append("today_in_energy_natural_gas")
    if any(term in normalized_query for term in ("weekly report", "natural gas weekly", "latest report")):
        report_families.append("natural_gas_weekly")

    if route is None and any(term in normalized_query for term in NARRATIVE_TERMS):
        domain_tags.append("natural_gas")

    report_families = _dedupe(report_families)
    domain_tags = _dedupe(domain_tags)
    metric_tags = _dedupe(metric_tags)
    geography_tags = _dedupe(geography_tags)

    return {
        "report_families": report_families,
        "report_types": sorted(report_types_for_families(report_families)),
        "domain_tags": domain_tags,
        "metric_tags": metric_tags,
        "geography_tags": geography_tags,
        "top_k": top_k,
        "recency_days": recency_days,
    }


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out
