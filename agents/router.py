from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

from agents.llm_router import LLMRouterError
from agents.llm_router import llm_route_structured as llm_route_structured_impl
from utils.dates import has_explicit_date_reference, resolve_date_range
from utils.helpers import contains_any

# ----------------------------
# Existing keyword maps
# ----------------------------
ISO_KEYWORDS = {
    "ercot": ["ercot", "texas"],
    "pjm": ["pjm", "mid-atlantic", "pennsylvania", "new jersey", "maryland", "dc"],
    "isone": [
        "isone",
        "iso-ne",
        "new england",
        "massachusetts",
        "connecticut",
        "maine",
        "nh",
        "vermont",
        "rhode island",
    ],
    "nyiso": ["nyiso", "new york iso", "new york"],
    "caiso": ["caiso", "california iso", "california"],
}

STORAGE_REGION_KEYWORDS = {
    "lower48": ["lower48", "lower 48"],
    "east": ["east", "eastern"],
    "midwest": ["midwest", "mid-west"],
    "south_central": ["south_central", "south central"],
    "mountain": ["mountain"],
    "pacific": ["pacific", "west coast"],
}

TRADE_REGION_KEYWORDS = {
    "united_states_pipeline_total": [
        "us total",
        "u.s. total",
        "united states total",
        "total pipeline",
    ],
    "canada_pipeline": ["canada_pipeline", "canada pipeline", "canadian pipeline"],
    "mexico_pipeline": ["mexico_pipeline", "mexico pipeline"],
}

IMPORT_REGION_KEYWORDS = {
    "united_states_pipeline_total": [
        "us total",
        "u.s. total",
        "united states total",
        "total pipeline",
    ],
    "canada_compressed": ["canada_compressed", "canada compressed"],
    "united_states_compressed_total": [
        "united_states_compressed_total",
        "united states compressed total",
        "us compressed total",
        "u.s. compressed total",
    ],
    "canada_pipeline": ["canada_pipeline", "canada pipeline", "canadian pipeline"],
    "mexico_pipeline": ["mexico_pipeline", "mexico pipeline"],
    "algeria": ["algeria"],
    "australia": ["australia"],
    "brunei": ["brunei"],
    "egypt": ["egypt"],
    "equatorial_guinea": ["equatorial_guinea", "equatorial guinea"],
    "france": ["france"],
    "indonesia": ["indonesia"],
    "jamaica": ["jamaica"],
    "malaysia": ["malaysia"],
    "nigeria": ["nigeria"],
    "norway": ["norway"],
    "oman": ["oman"],
    "peru": ["peru"],
    "qatar": ["qatar"],
    "trinidad_and_tobago": ["trinidad_and_tobago", "trinidad and tobago"],
    "united_arab_emirates": ["united_arab_emirates", "united arab emirates", "uae"],
    "united_kingdom": ["united_kingdom", "united kingdom", "uk"],
    "yemen": ["yemen"],
}

EXPORT_REGION_KEYWORDS = {
    "united_states_lng_total": [
        "united_states_lng_total",
        "united states lng total",
        "us lng total",
        "u.s. lng total",
    ],
    "canada_truck": ["canada_truck", "canada truck"],
    "mexico_truck": ["mexico_truck", "mexico truck"],
    "united_states_truck_total": [
        "united_states_truck_total",
        "united states truck total",
        "us truck total",
        "u.s. truck total",
    ],
    "canada_compressed": ["canada_compressed", "canada compressed"],
    "united_states_compressed_total": [
        "united_states_compressed_total",
        "united states compressed total",
        "us compressed total",
        "u.s. compressed total",
    ],
    "united_states_pipeline_total": [
        "us total",
        "u.s. total",
        "united states total",
        "total pipeline",
    ],
    "canada_pipeline": ["canada_pipeline", "canada pipeline", "canadian pipeline"],
    "mexico_pipeline": ["mexico_pipeline", "mexico pipeline"],
    "argentina": ["argentina"],
    "australia": ["australia"],
    "bahrain": ["bahrain"],
    "bangladesh": ["bangladesh"],
    "barbados": ["barbados"],
    "belgium": ["belgium"],
    "brazil": ["brazil"],
    "chile": ["chile"],
    "china": ["china"],
    "colombia": ["colombia"],
    "croatia": ["croatia"],
    "dominican_republic": ["dominican_republic", "dominican republic"],
    "egypt": ["egypt"],
    "el_salvador": ["el_salvador", "el salvador"],
    "finland": ["finland"],
    "france": ["france"],
    "germany": ["germany"],
    "greece": ["greece"],
    "haiti": ["haiti"],
    "india": ["india"],
    "indonesia": ["indonesia"],
    "israel": ["israel"],
    "italy": ["italy"],
    "jamaica": ["jamaica"],
    "japan": ["japan"],
    "jordan": ["jordan"],
    "kuwait": ["kuwait"],
    "lithuania": ["lithuania"],
    "malta": ["malta"],
    "mauritania": ["mauritania"],
    "mexico": ["mexico"],
    "netherlands": ["netherlands"],
    "nicaragua": ["nicaragua"],
    "pakistan": ["pakistan"],
    "panama": ["panama"],
    "philippines": ["philippines"],
    "poland": ["poland"],
    "portugal": ["portugal"],
    "russia": ["russia"],
    "senegal": ["senegal"],
    "singapore": ["singapore"],
    "south_korea": ["south_korea", "south korea"],
    "spain": ["spain"],
    "taiwan": ["taiwan"],
    "thailand": ["thailand"],
    "turkiye": ["turkiye", "turkey"],
    "united_arab_emirates": ["united_arab_emirates", "united arab emirates", "uae"],
    "united_kingdom": ["united_kingdom", "united kingdom", "uk"],
}

CONSUMPTION_STATE_KEYWORDS = {
    "al": ["al", "alabama"],
    "ak": ["ak", "alaska"],
    "az": ["az", "arizona"],
    "ar": ["ar", "arkansas"],
    "ca": ["ca", "california"],
    "co": ["co", "colorado"],
    "ct": ["ct", "connecticut"],
    "de": ["de", "delaware"],
    "fl": ["fl", "florida"],
    "ga": ["ga", "georgia"],
    "hi": ["hi", "hawaii"],
    "id": ["id", "idaho"],
    "il": ["il", "illinois"],
    "in": ["in", "indiana"],
    "ia": ["ia", "iowa"],
    "ks": ["ks", "kansas"],
    "ky": ["ky", "kentucky"],
    "la": ["la", "louisiana"],
    "me": ["me", "maine"],
    "md": ["md", "maryland"],
    "ma": ["ma", "massachusetts"],
    "mi": ["mi", "michigan"],
    "mn": ["mn", "minnesota"],
    "ms": ["ms", "mississippi"],
    "mo": ["mo", "missouri"],
    "mt": ["mt", "montana"],
    "ne": ["ne", "nebraska"],
    "nv": ["nv", "nevada"],
    "nh": ["nh", "new hampshire"],
    "nj": ["nj", "new jersey"],
    "nm": ["nm", "new mexico"],
    "ny": ["ny", "new york"],
    "nc": ["nc", "north carolina"],
    "nd": ["nd", "north dakota"],
    "oh": ["oh", "ohio"],
    "ok": ["ok", "oklahoma"],
    "or": ["or", "oregon"],
    "pa": ["pa", "pennsylvania"],
    "ri": ["ri", "rhode island"],
    "sc": ["sc", "south carolina"],
    "sd": ["sd", "south dakota"],
    "tn": ["tn", "tennessee"],
    "tx": ["tx", "texas"],
    "ut": ["ut", "utah"],
    "vt": ["vt", "vermont"],
    "va": ["va", "virginia"],
    "wa": ["wa", "washington"],
    "wv": ["wv", "west virginia"],
    "wi": ["wi", "wisconsin"],
    "wy": ["wy", "wyoming"],
    "united_states_total": [
        "us total",
        "u.s. total",
        "united states total",
        "united_states_total",
        "national total",
    ],
}

PRODUCTION_STATE_KEYWORDS = {
    "al": ["al", "alabama"],
    "ak": ["ak", "alaska"],
    "az": ["az", "arizona"],
    "ar": ["ar", "arkansas"],
    "ca": ["ca", "california"],
    "co": ["co", "colorado"],
    "fl": ["fl", "florida"],
    "il": ["il", "illinois"],
    "in": ["in", "indiana"],
    "ks": ["ks", "kansas"],
    "ky": ["ky", "kentucky"],
    "la": ["la", "louisiana"],
    "md": ["md", "maryland"],
    "mi": ["mi", "michigan"],
    "mo": ["mo", "missouri"],
    "ms": ["ms", "mississippi"],
    "mt": ["mt", "montana"],
    "ne": ["ne", "nebraska"],
    "nv": ["nv", "nevada"],
    "nm": ["nm", "new mexico"],
    "ny": ["ny", "new york"],
    "nd": ["nd", "north dakota"],
    "oh": ["oh", "ohio"],
    "ok": ["ok", "oklahoma"],
    "or": ["or", "oregon"],
    "pa": ["pa", "pennsylvania"],
    "sd": ["sd", "south dakota"],
    "tn": ["tn", "tennessee"],
    "tx": ["tx", "texas"],
    "ut": ["ut", "utah"],
    "va": ["va", "virginia"],
    "wv": ["wv", "west virginia"],
    "united_states_total": [
        "us total",
        "u.s. total",
        "united states total",
        "united_states_total",
        "national total",
    ],
}

RESERVES_STATE_KEYWORDS = {
    "al": ["al", "alabama"],
    "ak": ["ak", "alaska"],
    "ar": ["ar", "arkansas"],
    "ca": ["ca", "california"],
    "co": ["co", "colorado"],
    "fl": ["fl", "florida"],
    "ks": ["ks", "kansas"],
    "ky": ["ky", "kentucky"],
    "la": ["la", "louisiana"],
    "mi": ["mi", "michigan"],
    "ms": ["ms", "mississippi"],
    "mt": ["mt", "montana"],
    "nd": ["nd", "north dakota"],
    "nm": ["nm", "new mexico"],
    "ny": ["ny", "new york"],
    "oh": ["oh", "ohio"],
    "ok": ["ok", "oklahoma"],
    "pa": ["pa", "pennsylvania"],
    "tx": ["tx", "texas"],
    "ut": ["ut", "utah"],
    "va": ["va", "virginia"],
    "wv": ["wv", "west virginia"],
    "wy": ["wy", "wyoming"],
    "us": ["us", "u.s.", "united states"],
    "all": ["all"],
}

RESERVES_RESOURCE_CATEGORY_KEYWORDS = {
    "proved_associated_gas": [
        "proved associated gas",
        "associated gas",
    ],
    "proved_nonassociated_gas": [
        "proved nonassociated gas",
        "nonassociated gas",
        "non-associated gas",
    ],
    "proved_ngl": [
        "proved ngl",
        "ngl",
        "natural gas liquids",
    ],
    "expected_future_gas_production": [
        "expected future gas production",
        "future gas production",
        "expected future production",
    ],
}

PIPELINE_DATASET_KEYWORDS = {
    "historical_projects": [
        "historical projects",
        "historical pipeline projects",
        "pipeline project history",
    ],
    "inflow_by_region": [
        "inflow by region",
        "regional inflow",
        "inflows by region",
        "region inflow",
    ],
    "inflow_by_state": [
        "inflow by state",
        "state inflow",
        "inflows by state",
    ],
    "inflow_single_year": [
        "inflow single year",
        "single year inflow",
    ],
    "major_pipeline_summary": [
        "major pipeline summary",
        "major pipelines",
        "pipeline summary",
    ],
    "natural_gas_pipeline_projects": [
        "pipeline projects",
        "natural gas pipeline projects",
        "project list",
        "pipeline project",
    ],
    "outflow_by_region": [
        "outflow by region",
        "regional outflow",
        "outflows by region",
        "region outflow",
    ],
    "outflow_by_state": [
        "outflow by state",
        "state outflow",
        "outflows by state",
    ],
    "pipeline_state2_state_capacity": [
        "state to state capacity",
        "state2state capacity",
        "pipeline capacity",
        "capacity by state pair",
        "interstate capacity",
    ],
}

ROUTE_MAP = {
    "iso_gas_dependency": [
        "gas share",
        "gas dependency",
        "grid gas",
        "gas burn",
        "gas-fired",
        "fuel mix",
        "generation mix",
        "power mix",
        "dispatch",
        "how much gas generation",
    ],
    "iso_renewables": [
        "renewables",
        "renewable generation",
        "renewable share",
        "wind and solar",
        "wind solar",
        "solar and wind",
        "wind generation",
        "solar generation",
    ],
    "iso_fuel_mix": [
        "fuel mix",
        "generation mix",
        "power mix",
        "by fuel",
        "gas wind solar",
    ],
    "iso_load": [
        "load",
        "demand",
        "electric demand",
        "power demand",
        "system demand",
    ],
    "working_gas_storage_change_weekly": [
        "storage change",
        "weekly storage change",
        "week over week storage",
        "storage wow",
        "net injection",
        "net withdrawal",
        "change in storage",
    ],
    "working_gas_storage_lower48": [
        "storage",
        "inventory",
        "working gas",
        "injection",
        "withdrawal",
    ],
    "henry_hub_spot": [
        "henry hub",
        "spot price",
        "gas price",
        "benchmark price",
    ],
    "lng_exports": [
        "lng exports",
        "lng export",
        "lng export capacity",
        "lng capacity utilization",
        "export capacity",
        "imports vs exports",
        "import vs export",
        "export vs import",
        "import and export",
        "which regions import vs export",
        "export lng",
        "liquefied natural gas export",
        "gas exports",
        "pipeline exports",
        "pipeline flow",
        "gas flow",
        "pipeline throughput",
    ],
    "lng_imports": [
        "lng imports",
        "import lng",
        "liquefied natural gas import",
        "gas imports",
        "pipeline imports",
    ],
    "ng_consumption_lower48": [
        "consumption",
        "consumes",
        "usage",
    ],
    "ng_consumption_by_sector": [
        "sector consumption",
        "consumption by sector",
        "end use",
        "residential",
        "commercial",
        "industrial",
        "electric power",
        "power sector",
        "which sector",
    ],
    "ng_electricity": ["electricity", "power plants", "power generation"],
    "ng_production_lower48": ["production", "output", "supply", "dry gas production"],
    "ng_exploration_reserves_lower48": [
        "exploration",
        "reserves",
        "proved reserves",
    ],
    "ng_pipeline": [
        "pipeline projects",
        "pipeline project",
        "pipeline capacity",
        "state to state capacity",
        "major pipeline",
        "pipeline summary",
        "inflow by region",
        "inflow by state",
        "outflow by region",
        "outflow by state",
        "historical projects",
    ],
}

ALLOWED_METRICS = set(ROUTE_MAP.keys())
ALLOWED_ISOS = set(ISO_KEYWORDS.keys())
ALLOWED_STORAGE_REGIONS = set(STORAGE_REGION_KEYWORDS.keys())
ALLOWED_TRADE_REGIONS = set(TRADE_REGION_KEYWORDS.keys())
ALLOWED_PRODUCTION_STATES = set(PRODUCTION_STATE_KEYWORDS.keys())
ALLOWED_CONSUMPTION_STATES = set(CONSUMPTION_STATE_KEYWORDS.keys())
ALLOWED_IMPORT_REGIONS = set(IMPORT_REGION_KEYWORDS.keys())
ALLOWED_EXPORT_REGIONS = set(EXPORT_REGION_KEYWORDS.keys())
ALLOWED_RESERVES_STATES = set(RESERVES_STATE_KEYWORDS.keys())
ALLOWED_RESERVES_RESOURCE_CATEGORIES = set(
    RESERVES_RESOURCE_CATEGORY_KEYWORDS.keys()
)
ALLOWED_PIPELINE_DATASETS = set(PIPELINE_DATASET_KEYWORDS.keys())

# ----------------------------
# Normalization
# ----------------------------
NORMALIZE_PATTERNS: List[Tuple[str, str]] = [
    (r"\blower forty[- ]?eight\b", "lower 48"),
    (r"\bnat gas\b", "natural gas"),
    (r"\bdraw\b", "withdrawal"),
    (r"\bstorage build\b", "injection"),
    (r"\bbuild in storage\b", "injection"),
    (r"\bgrid mix\b", "fuel mix"),
    (r"\bpower mix\b", "fuel mix"),
    (r"\bstack\b", "fuel mix"),
    (r"\bclean energy\b", "renewables"),
    (r"\bgreen power\b", "renewables"),
    (r"\brenewable penetration\b", "renewable share"),
    (r"\bgas burn\b", "gas-fired generation"),
    (r"\belectric-sector\b", "electricity"),
    (r"\bcash gas\b", "spot price"),
]

# metric-specific bonus terms
BONUS_TERMS: Dict[str, List[str]] = {
    "working_gas_storage_change_weekly": ["last week", "weekly", "wow"],
    "working_gas_storage_lower48": ["storage", "inventory", "working gas"],
    "henry_hub_spot": ["henry hub", "spot", "benchmark"],
    "lng_exports": ["exports", "export", "pipeline flow", "throughput"],
    "lng_imports": ["imports", "import"],
    "ng_consumption_lower48": ["consumption", "usage"],
    "ng_consumption_by_sector": [
        "sector",
        "residential",
        "commercial",
        "industrial",
        "electric power",
        "power",
    ],
    "ng_electricity": ["power plants", "electricity", "power generation"],
    "ng_production_lower48": ["production", "output", "supply"],
    "ng_exploration_reserves_lower48": ["reserves", "exploration"],
    "ng_pipeline": ["pipeline", "capacity", "projects", "inflow", "outflow"],
    "iso_load": ["load", "demand", "system demand"],
    "iso_gas_dependency": ["gas share", "gas-fired", "gas-fired generation"],
    "iso_renewables": ["renewables", "wind", "solar"],
    "iso_fuel_mix": ["fuel mix", "by fuel", "generation mix"],
}

COMPARE_PATTERNS = [r"\bcompare\b", r"\bversus\b", r"\bvs\b"]
RANK_PATTERNS = [
    r"\bhighest\b",
    r"\blowest\b",
    r"\bmost\b",
    r"\bleast\b",
    r"\bwhich region\b",
]
DERIVED_PATTERNS = [
    r"\bnet exporter\b",
    r"\bbalance\b",
    r"\bversus normal\b",
    r"\btight\b",
]
EXPLAIN_PATTERNS = [r"\bwhy\b", r"\bbecause\b", r"\bdid .* rise\b", r"\bdid .* fall\b"]
FORECAST_PATTERNS = [
    r"\bforecast\b",
    r"\bprojection\b",
    r"\bproject\b",
    r"\bprojected\b",
    r"\bnext week\b",
    r"\bnext 7 days\b",
    r"\bnext 14 days\b",
    r"\bnext two weeks\b",
    r"\bnext 2 weeks\b",
]


@dataclass(frozen=True)
class RouteCandidate:
    metric: str
    score: float
    matched_terms: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class HybridRouteResult:
    intent: Literal[
        "single_metric",
        "compare",
        "ranking",
        "derived",
        "explain",
        "ambiguous",
        "unsupported",
    ]
    primary_metric: Optional[str]
    metrics: List[str]
    start: str
    end: str
    filters: Optional[Dict[str, Any]] = None
    confidence: float = 0.0
    ambiguous: bool = False
    candidates: List[RouteCandidate] = field(default_factory=list)
    source: Literal["rule", "llm"] = "rule"
    reason: Optional[str] = None
    normalized_query: Optional[str] = None
    include_forecast: bool = False
    forecast_horizon_days: Optional[int] = None


# ----------------------------
# Helpers
# ----------------------------
def normalize_query(user_query: str) -> str:
    q = user_query.lower().strip()
    for pattern, replacement in NORMALIZE_PATTERNS:
        q = re.sub(pattern, replacement, q, flags=re.IGNORECASE)
    q = re.sub(r"\s+", " ", q).strip()
    return q


def route_iso(q: str) -> str | None:
    q = q.lower()
    for iso, keys in ISO_KEYWORDS.items():
        if contains_any(keys, q):
            return iso
    return None


def route_storage_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in STORAGE_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_trade_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in TRADE_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_import_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in IMPORT_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_export_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in EXPORT_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_production_state(q: str) -> str | None:
    q = q.lower()
    for state, keys in PRODUCTION_STATE_KEYWORDS.items():
        long_keys = [key for key in keys if len(key) > 2]
        if long_keys and contains_any(long_keys, q):
            return state

    safe_abbrev_states = ALLOWED_PRODUCTION_STATES - {"ar", "in", "or", "united_states_total"}
    tokens = set(re.findall(r"\b[a-z]{2}\b", q))
    for state in safe_abbrev_states:
        if state in tokens:
            return state

    return None


def route_consumption_state(q: str) -> str | None:
    q = q.lower()
    for state, keys in CONSUMPTION_STATE_KEYWORDS.items():
        long_keys = [key for key in keys if len(key) > 2]
        if long_keys and contains_any(long_keys, q):
            return state

    safe_abbrev_states = ALLOWED_CONSUMPTION_STATES - {
        "ar",
        "de",
        "hi",
        "id",
        "in",
        "me",
        "or",
        "united_states_total",
    }
    tokens = set(re.findall(r"\b[a-z]{2}\b", q))
    for state in safe_abbrev_states:
        if state in tokens:
            return state

    return None


def route_reserves_state(q: str) -> str | None:
    q = q.lower()
    for state, keys in RESERVES_STATE_KEYWORDS.items():
        long_keys = [key for key in keys if len(key) > 2]
        if long_keys and contains_any(long_keys, q):
            return state

    safe_abbrev_states = ALLOWED_RESERVES_STATES - {"al", "ar", "oh", "ok", "all", "us"}
    tokens = set(re.findall(r"\b[a-z]{2}\b", q))
    for state in safe_abbrev_states:
        if state in tokens:
            return state

    return None


def route_reserves_resource_category(q: str) -> str | None:
    q = q.lower()
    for category, keys in RESERVES_RESOURCE_CATEGORY_KEYWORDS.items():
        if contains_any(keys, q):
            return category
    return None


def route_pipeline_dataset(q: str) -> str | None:
    q = q.lower()
    for dataset, keys in PIPELINE_DATASET_KEYWORDS.items():
        if contains_any(keys, q):
            return dataset
    return None


def detect_intent(q: str) -> str:
    if any(re.search(p, q) for p in COMPARE_PATTERNS):
        return "compare"
    if any(re.search(p, q) for p in RANK_PATTERNS):
        return "ranking"
    if any(re.search(p, q) for p in DERIVED_PATTERNS):
        return "derived"
    if any(re.search(p, q) for p in EXPLAIN_PATTERNS):
        return "explain"
    return "single_metric"


def detect_forecast_request(q: str) -> bool:
    return any(re.search(pattern, q) for pattern in FORECAST_PATTERNS)


def detect_forecast_horizon_days(q: str) -> Optional[int]:
    if not detect_forecast_request(q):
        return None
    if re.search(r"\b(14|fourteen)\s*day", q) or re.search(r"\b(two|2)\s+weeks?\b", q):
        return 14
    return 7


def score_metric(q: str, metric: str, keywords: List[str]) -> RouteCandidate:
    score = 0.0
    matched_terms: List[str] = []

    for kw in keywords:
        if kw in q:
            score += 3.0 if len(kw.split()) > 1 else 1.5
            matched_terms.append(kw)

    for bonus in BONUS_TERMS.get(metric, []):
        if bonus in q and bonus not in matched_terms:
            score += 1.0
            matched_terms.append(bonus)

    # penalize known overlaps a bit
    if metric == "iso_load" and "gas" in q and "demand" in q:
        score -= 1.0
    if metric == "ng_consumption_lower48" and "texas" in q:
        score -= 0.5
    if metric == "ng_consumption_by_sector" and "most" in q:
        score += 0.75
    if metric == "ng_electricity" and "share" in q:
        score -= 0.75
    if metric == "iso_fuel_mix" and "consumption" in q:
        score -= 1.0
    if metric == "lng_exports" and any(
        term in q
        for term in (
            "pipeline projects",
            "pipeline capacity",
            "state to state capacity",
            "inflow",
            "outflow",
            "major pipeline",
        )
    ):
        score -= 1.25
    if metric == "lng_imports" and any(
        term in q
        for term in ("pipeline capacity", "inflow", "outflow", "major pipeline")
    ):
        score -= 1.0

    return RouteCandidate(
        metric=metric, score=max(score, 0.0), matched_terms=matched_terms
    )


def score_routes(q: str) -> List[RouteCandidate]:
    candidates = [
        score_metric(q, metric, keywords) for metric, keywords in ROUTE_MAP.items()
    ]
    candidates = [c for c in candidates if c.score > 0]
    candidates.sort(key=lambda x: x.score, reverse=True)
    return candidates


def build_filters(metric: str, q: str, confidence: float) -> Optional[Dict[str, Any]]:
    filters: Dict[str, Any] = {}

    if metric.startswith("iso_"):
        iso = route_iso(q)
        if iso:
            filters["iso"] = iso
        elif confidence >= 0.85:
            # only default ISO when confidence is strong
            filters["iso"] = "ercot"

    elif metric in {"working_gas_storage_lower48", "working_gas_storage_change_weekly"}:
        region = route_storage_region(q)
        if region:
            filters["region"] = region
        elif confidence >= 0.85:
            filters["region"] = "lower48"

    elif metric == "lng_exports":
        region = route_export_region(q)
        if region:
            filters["region"] = region
        elif confidence >= 0.85:
            filters["region"] = "united_states_pipeline_total"

    elif metric == "lng_imports":
        region = route_import_region(q)
        if region:
            filters["region"] = region
        elif confidence >= 0.85:
            filters["region"] = "united_states_pipeline_total"

    elif metric == "ng_consumption_lower48":
        state = route_consumption_state(q)
        if state:
            filters["region"] = state
        else:
            filters["region"] = "united_states_total"

    elif metric == "ng_production_lower48":
        state = route_production_state(q)
        if state:
            filters["region"] = state
        else:
            filters["region"] = "united_states_total"

    elif metric == "ng_exploration_reserves_lower48":
        state = route_reserves_state(q)
        if state:
            filters["region"] = state
        category = route_reserves_resource_category(q)
        if category:
            filters["resource_category"] = category
    elif metric == "ng_pipeline":
        dataset = route_pipeline_dataset(q)
        if dataset:
            filters["dataset"] = dataset
        elif confidence >= 0.85:
            filters["dataset"] = "natural_gas_pipeline_projects"

    return filters or None


def candidate_confidence(candidates: List[RouteCandidate]) -> float:
    if not candidates:
        return 0.0
    top = candidates[0].score
    second = candidates[1].score if len(candidates) > 1 else 0.0

    # simple heuristic: strong top score + clear gap improves confidence
    conf = min(0.98, 0.15 * top + 0.08 * max(top - second, 0))
    return round(conf, 3)


def is_ambiguous(candidates: List[RouteCandidate]) -> bool:
    if not candidates:
        return True
    if len(candidates) == 1:
        # A lone metric hit should not require LLM fallback unless the match is
        # extremely weak. Otherwise common single-term questions like
        # "Is production growing year over year?" can be misrouted as unsupported.
        return candidates[0].score < 1.5

    top = candidates[0].score
    second = candidates[1].score

    # ambiguous if the best and second-best are very close
    return (top < 3.0) or ((top - second) <= 1.0)


# ----------------------------
# LLM hook contract
# ----------------------------
@dataclass(frozen=True)
class LLMRouteOutput:
    intent: str
    primary_metric: Optional[str]
    metrics: List[str]
    filters: Optional[Dict[str, Any]]
    reason: Optional[str]
    confidence: float
    ambiguous: bool


def llm_route_structured(user_query: str, normalized_query: str) -> LLMRouteOutput:

    try:
        return llm_route_structured_impl(
            user_query=user_query, normalized_query=normalized_query
        )
    except LLMRouterError as err:
        return LLMRouteOutput(
            intent="unsupported",
            primary_metric=None,
            metrics=[],
            filters=None,
            reason=f"LLM router error: {err}",
            confidence=0.0,
            ambiguous=False,
        )


def validate_llm_route(
    llm: LLMRouteOutput,
    start: str,
    end: str,
    normalized_query: str,
) -> HybridRouteResult:
    metrics = [m for m in llm.metrics if m in ALLOWED_METRICS]

    primary_metric = (
        llm.primary_metric if llm.primary_metric in ALLOWED_METRICS else None
    )
    if primary_metric and primary_metric not in metrics:
        metrics = [primary_metric] + metrics

    filters = dict(llm.filters or {})

    if "iso" in filters and filters["iso"] not in ALLOWED_ISOS:
        filters.pop("iso")
    if "region" in filters:
        region = filters["region"]
        if primary_metric and primary_metric.startswith("iso_"):
            filters.pop("region", None)
        elif primary_metric in {
            "working_gas_storage_lower48",
            "working_gas_storage_change_weekly",
        }:
            if region not in ALLOWED_STORAGE_REGIONS:
                filters.pop("region", None)
        elif primary_metric == "lng_exports":
            if region not in ALLOWED_EXPORT_REGIONS:
                filters.pop("region", None)
        elif primary_metric == "lng_imports":
            if region not in ALLOWED_IMPORT_REGIONS:
                filters.pop("region", None)
        elif primary_metric == "ng_consumption_lower48":
            if region not in ALLOWED_CONSUMPTION_STATES:
                filters.pop("region", None)
        elif primary_metric == "ng_production_lower48":
            if region not in ALLOWED_PRODUCTION_STATES:
                filters.pop("region", None)
        elif primary_metric == "ng_exploration_reserves_lower48":
            if region not in ALLOWED_RESERVES_STATES:
                filters.pop("region", None)
        else:
            filters.pop("region", None)
    if "resource_category" in filters:
        resource_category = filters["resource_category"]
        if primary_metric != "ng_exploration_reserves_lower48":
            filters.pop("resource_category", None)
        elif resource_category not in ALLOWED_RESERVES_RESOURCE_CATEGORIES:
            filters.pop("resource_category", None)
    if "dataset" in filters:
        dataset = filters["dataset"]
        if primary_metric != "ng_pipeline":
            filters.pop("dataset", None)
        elif dataset not in ALLOWED_PIPELINE_DATASETS:
            filters.pop("dataset", None)

    if llm.intent not in {
        "single_metric",
        "compare",
        "ranking",
        "derived",
        "explain",
        "ambiguous",
        "unsupported",
    }:
        intent = "unsupported"
    else:
        intent = llm.intent

    if not metrics and primary_metric is None:
        intent = "unsupported"

    return HybridRouteResult(
        intent=intent,
        primary_metric=primary_metric,
        metrics=metrics,
        start=start,
        end=end,
        filters=filters or None,
        confidence=max(0.0, min(1.0, float(llm.confidence))),
        ambiguous=bool(llm.ambiguous),
        candidates=[],
        source="llm",
        reason=llm.reason,
        normalized_query=normalized_query,
        include_forecast=detect_forecast_request(normalized_query),
        forecast_horizon_days=detect_forecast_horizon_days(normalized_query),
    )


# ----------------------------
# Main hybrid router
# ----------------------------
def route_query(user_query: str) -> HybridRouteResult:

    normalized = normalize_query(user_query)
    start, end = resolve_date_range(user_query)
    has_explicit_dates = has_explicit_date_reference(user_query)
    intent = detect_intent(normalized)
    include_forecast = detect_forecast_request(normalized)
    forecast_horizon_days = detect_forecast_horizon_days(normalized)

    candidates = score_routes(normalized)
    confidence = candidate_confidence(candidates)
    ambiguous = is_ambiguous(candidates)

    # No rule candidate -> LLM fallback
    if not candidates:
        llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
        return validate_llm_route(
            llm, start=start, end=end, normalized_query=normalized
        )

    top = candidates[0]
    if not has_explicit_dates and top.metric == "ng_exploration_reserves_lower48":
        start = "2000-01-01"
    filters = build_filters(top.metric, normalized, confidence)

    # Fast path: for single-metric questions, stay on rules unless the match is ambiguous.
    if intent == "single_metric" and not ambiguous:
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"Strong rule match on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if intent == "ranking" and top.metric == "ng_consumption_by_sector" and not ambiguous:
        return HybridRouteResult(
            intent="ranking",
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"Strong rule match on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    # Multi-metric or advanced intent -> LLM assist
    if intent in {"compare", "ranking", "derived", "explain"}:
        llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
        return validate_llm_route(
            llm, start=start, end=end, normalized_query=normalized
        )

    # Ambiguous rule route -> LLM assist
    if ambiguous:
        llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
        return validate_llm_route(
            llm, start=start, end=end, normalized_query=normalized
        )

    # Fallback deterministic return
    return HybridRouteResult(
        intent="single_metric",
        primary_metric=top.metric,
        metrics=[top.metric],
        start=start,
        end=end,
        filters=filters,
        confidence=confidence,
        ambiguous=ambiguous,
        candidates=candidates[:3],
        source="rule",
        reason=f"Fallback rule route to {top.metric}",
        normalized_query=normalized,
        include_forecast=include_forecast,
        forecast_horizon_days=forecast_horizon_days,
    )
