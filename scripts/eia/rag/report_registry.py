from __future__ import annotations

from typing import Iterable

REPORT_FAMILIES: dict[str, dict[str, object]] = {
    "natural_gas_weekly": {
        "report_types": {"natural_gas_weekly_update"},
        "domain_tags": {"natural_gas", "storage"},
        "default_recency_days": 120,
        "priority": 100,
    },
    "wngsr_supplement": {
        "report_types": {"wngsr_supplement"},
        "domain_tags": {"natural_gas", "storage"},
        "default_recency_days": 180,
        "priority": 90,
    },
    "steo_natural_gas": {
        "report_types": {"steo_natural_gas"},
        "domain_tags": {"natural_gas", "outlook"},
        "default_recency_days": 365,
        "priority": 70,
    },
    "today_in_energy_natural_gas": {
        "report_types": {"today_in_energy_natural_gas"},
        "domain_tags": {"natural_gas"},
        "default_recency_days": 365,
        "priority": 60,
    },
}


def family_for_report_type(report_type: str) -> str | None:
    normalized = str(report_type or "").strip()
    if not normalized:
        return None
    for family, config in REPORT_FAMILIES.items():
        if normalized in set(config.get("report_types") or set()):
            return family
    return None


def report_types_for_families(families: Iterable[str]) -> set[str]:
    report_types: set[str] = set()
    for family in families:
        config = REPORT_FAMILIES.get(str(family))
        if not config:
            continue
        report_types.update(str(value) for value in set(config.get("report_types") or set()))
    return report_types
