from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

import pandas as pd

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
    "south_central": ["south_central", "south central", "south"],
    "mountain": ["mountain"],
    "pacific": ["pacific", "west coast"],
}

WEATHER_REGION_KEYWORDS = {
    "lower48": ["lower 48", "lower48", "u.s.", "us", "national", "united states"],
    "east": ["east", "eastern", "northeast"],
    "midwest": ["midwest", "mid-west"],
    "south": ["south", "southern", "southeast"],
    "west": ["west", "western"],
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

REGIONAL_GROUP_TERMS = (
    "by region",
    "regional",
    "across regions",
    "all regions",
)

STORAGE_COMPARE_TERMS = (
    "weekly change",
    "storage change",
    "change in storage",
    "week over week storage",
    "storage wow",
)

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
    "managed_money_net_percentile_156w": [
        "managed money percentile",
        "managed money extreme",
        "managed money positioning percentile",
        "managed money percentile 156w",
    ],
    "managed_money_net": [
        "managed money net",
        "net managed money",
        "spec net length",
        "managed money positioning",
        "cot managed money net",
        "cftc managed money net",
    ],
    "managed_money_long": [
        "managed money long",
        "spec longs",
        "cftc longs",
    ],
    "managed_money_short": [
        "managed money short",
        "spec shorts",
        "cftc shorts",
    ],
    "open_interest": [
        "open interest",
        "cftc open interest",
        "cot open interest",
    ],
    "des_special_questions_text": [
        "dallas fed special questions",
        "special questions",
        "survey special questions",
    ],
    "des_comments_text": [
        "survey comments",
        "des comments",
        "dallas fed comments",
        "respondent comments",
    ],
    "des_report_summary_text": [
        "dallas fed energy survey",
        "energy survey",
        "oil and gas survey",
        "current report",
        "latest survey summary",
    ],
    "des_business_activity_index": [
        "business activity index",
        "dallas fed business activity",
        "des business activity",
    ],
    "des_company_outlook_index": [
        "company outlook",
        "company outlook index",
    ],
    "des_outlook_uncertainty_index": [
        "uncertainty index",
        "outlook uncertainty",
    ],
    "des_oil_production_index": [
        "oil production index",
        "des oil production",
    ],
    "des_gas_production_index": [
        "gas production index",
        "natural gas production index",
        "des gas production",
    ],
    "des_capex_index": [
        "capex index",
        "capital expenditures index",
        "capital expenditure index",
    ],
    "des_employment_index": [
        "employment index",
        "des employment",
    ],
    "des_input_cost_index": [
        "input cost index",
        "input costs",
    ],
    "des_finding_development_costs_index": [
        "finding and development costs",
        "development costs index",
    ],
    "des_lease_operating_expense_index": [
        "lease operating expense",
        "lease operating expenses",
    ],
    "des_prices_received_services_index": [
        "prices received services",
        "prices received for services",
    ],
    "des_equipment_utilization_index": [
        "equipment utilization",
        "utilization of equipment",
    ],
    "des_operating_margin_index": [
        "operating margin",
        "operating margin index",
    ],
    "des_wti_price_expectation_1y": [
        "wti expectations",
        "wti price expectations",
        "wti 1 year",
        "wti one year",
        "price expectations",
    ],
    "des_hh_price_expectation_1y": [
        "henry hub expectations",
        "henry hub price expectations",
        "hh expectations",
        "henry hub 1 year",
    ],
    "des_breakeven_oil_us": [
        "break-even oil",
        "breakeven oil",
        "oil breakeven",
    ],
    "des_breakeven_gas_us": [
        "break-even gas",
        "breakeven gas",
        "gas breakeven",
    ],
    "iso_gas_dependency": [
        "gas share",
        "gas dependency",
        "percentage of electricity generation",
        "percent of electricity generation",
        "what percentage of electricity generation",
        "electricity generation from natural gas",
        "share of electricity from natural gas",
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
        "storage build",
        "storage injection",
        "storage withdrawal",
        "build",
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
    "ng_electricity": [
        "electricity",
        "power plants",
        "power generation",
        "power burn",
        "natural gas power burn",
        "gas power burn",
    ],
    "ng_production_lower48": ["production", "output", "supply", "dry gas production"],
    "ng_supply_balance_regime": [
        "gas supply",
        "natural gas supply",
        "supply expanding",
        "supply tightening",
        "expanding or tightening",
        "tightening or expanding",
        "market tightening",
        "market balance",
        "supply-demand balance",
        "supply demand balance",
        "fundamentals",
        "supply constrained",
        "tightening",
        "loosening",
    ],
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
    "weather_degree_days_forecast_vs_5y": [
        "degree day",
        "degree days",
        "weather forecast",
        "weather-related demand",
        "weather demand",
        "seasonal norm",
        "seasonal norms",
        "seasonal normal",
        "seasonal normals",
        "bullish",
        "bearish",
        "hdd",
        "cdd",
        "heating degree",
        "cooling degree",
        "1-year average",
        "2-year average",
        "3-year average",
        "4-year average",
        "vs 5-year",
        "vs 5 year",
        "five-year average",
        "5-year average",
        "year average",
        "weather normal",
    ],
    "weather_regional_demand_drivers": [
        "which regions are driving",
        "regions driving weather-related demand",
        "regional weather demand",
        "weather demand by region",
        "weather drivers by region",
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
ALLOWED_WEATHER_REGIONS = set(WEATHER_REGION_KEYWORDS.keys())
ALLOWED_WEATHER_NORMAL_YEARS = {1, 2, 3, 4, 5}

# ----------------------------
# Normalization
# ----------------------------
NORMALIZE_PATTERNS: List[Tuple[str, str]] = [
    (r"[–—]", "-"),
    (r"\blower forty[- ]?eight\b", "lower 48"),
    (r"\bnat gas\b", "natural gas"),
    (r"\bdraw\b", "withdrawal"),
    (r"\bwithdrawals\b", "withdrawal"),
    (r"\binjections\b", "injection"),
    (r"\bstorage build\b", "storage injection"),
    (r"\bbuild in storage\b", "storage injection"),
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
    "managed_money_net_percentile_156w": ["managed money", "percentile", "cftc", "cot"],
    "managed_money_net": ["managed money", "net", "cftc", "cot"],
    "managed_money_long": ["managed money", "long", "cftc", "cot"],
    "managed_money_short": ["managed money", "short", "cftc", "cot"],
    "open_interest": ["open interest", "cftc", "cot"],
    "des_special_questions_text": ["special questions", "des", "dallas fed"],
    "des_comments_text": ["comments", "survey comments", "des"],
    "des_report_summary_text": ["energy survey", "dallas fed", "des"],
    "des_business_activity_index": ["business activity", "des", "dallas fed"],
    "des_company_outlook_index": ["company outlook", "des"],
    "des_outlook_uncertainty_index": ["uncertainty", "des"],
    "des_oil_production_index": ["oil production", "des"],
    "des_gas_production_index": ["gas production", "des"],
    "des_capex_index": ["capex", "capital expenditures", "des"],
    "des_wti_price_expectation_1y": ["wti", "expectations", "des"],
    "des_hh_price_expectation_1y": ["henry hub", "expectations", "des"],
    "des_breakeven_oil_us": ["breakeven", "oil", "des"],
    "des_breakeven_gas_us": ["breakeven", "gas", "des"],
    "working_gas_storage_change_weekly": [
        "last week",
        "weekly",
        "wow",
        "build",
        "injection",
        "withdrawal",
    ],
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
    "ng_electricity": [
        "power plants",
        "electricity",
        "power generation",
        "power burn",
        "gas power burn",
    ],
    "ng_production_lower48": ["production", "output", "supply"],
    "ng_supply_balance_regime": [
        "gas supply",
        "market balance",
        "fundamentals",
        "tightening",
        "loosening",
        "supply constrained",
    ],
    "ng_exploration_reserves_lower48": ["reserves", "exploration"],
    "ng_pipeline": ["pipeline", "capacity", "projects", "inflow", "outflow"],
    "weather_degree_days_forecast_vs_5y": [
        "weather",
        "forecast",
        "seasonal",
        "norm",
        "bullish",
        "bearish",
        "demand",
        "hdd",
        "cdd",
        "degree day",
        "5-year",
        "normal",
    ],
    "weather_regional_demand_drivers": [
        "regions",
        "regional",
        "driving",
        "weather",
        "demand",
    ],
    "iso_load": ["load", "demand", "system demand"],
    "iso_gas_dependency": [
        "gas share",
        "gas-fired",
        "gas-fired generation",
        "percentage",
        "percent",
        "generation",
    ],
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
    r"\bfastest\b",
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


def route_weather_region(q: str) -> str | None:
    q = q.lower()
    for region, keys in WEATHER_REGION_KEYWORDS.items():
        if contains_any(keys, q):
            return region
    return None


def route_weather_normal_years(q: str) -> int | None:
    q = q.lower()
    direct = re.search(r"\b([12345])\s*[- ]?years?\b", q)
    if direct:
        return int(direct.group(1))
    word_map = {
        "one year": 1,
        "two year": 2,
        "three year": 3,
        "four year": 4,
        "five year": 5,
    }
    for phrase, value in word_map.items():
        if phrase in q:
            return value
    return None


def wants_regional_grouping(q: str) -> bool:
    q = q.lower()
    return contains_any(REGIONAL_GROUP_TERMS, q)


def wants_storage_ranking_by_region(q: str) -> bool:
    q = q.lower()
    return any(term in q for term in ("withdrawal", "injection", "build")) and any(
        term in q for term in ("where", "fastest", "largest", "biggest", "most")
    )


def wants_storage_level_and_change(q: str) -> bool:
    q = q.lower()
    return "storage" in q and contains_any(STORAGE_COMPARE_TERMS, q) and any(
        term in q for term in ("together", "compare")
    )


def wants_seasonal_norm_comparison(q: str) -> bool:
    q = q.lower()
    return any(
        phrase in q
        for phrase in (
            "seasonal norm",
            "seasonal norms",
            "seasonal normal",
            "seasonal normals",
            "versus normal",
            "vs normal",
            "compared to normal",
        )
    )


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
    if metric == "ng_electricity" and "power burn" in q:
        score += 2.0
    if metric == "ng_electricity" and "seasonal" in q and any(
        token in q for token in ("power", "electricity", "burn")
    ):
        score += 1.5
    if metric == "iso_gas_dependency" and any(
        phrase in q
        for phrase in (
            "percentage of electricity generation",
            "percent of electricity generation",
            "electricity generation from natural gas",
            "share of electricity from natural gas",
        )
    ):
        score += 2.5
    if metric == "iso_gas_dependency" and "renewables" in q and any(
        token in q for token in ("gas demand", "power sector", "electricity")
    ):
        score += 2.0
    if metric == "iso_gas_dependency" and all(
        token in q for token in ("renewables", "power sector", "demand")
    ):
        score += 2.5
    if metric == "ng_consumption_lower48" and route_consumption_state(q) and any(
        token in q for token in ("consumption", "usage")
    ):
        score += 1.5
    if metric == "ng_production_lower48" and route_production_state(q) and any(
        token in q for token in ("production", "output", "supply")
    ):
        score += 1.5
    if metric == "ng_supply_balance_regime" and "supply" in q and any(
        token in q for token in ("tight", "tightening", "expand", "expanding", "loosen", "loosening")
    ):
        score += 2.0
    if metric == "ng_supply_balance_regime" and any(
        token in q for token in ("market balance", "fundamentals")
    ):
        score += 1.0
    if metric == "weather_degree_days_forecast_vs_5y" and "weather" in q and any(
        token in q
        for token in (
            "forecast",
            "demand",
            "normal",
            "seasonal",
            "bullish",
            "bearish",
            "region",
        )
    ):
        score += 2.0
    if metric == "weather_degree_days_forecast_vs_5y" and any(
        phrase in q
        for phrase in (
            "power burn",
            "electricity generation",
            "power sector",
        )
    ):
        score -= 2.0
    if metric == "weather_regional_demand_drivers" and "weather" in q and any(
        token in q for token in ("region", "regions", "driving", "driver")
    ):
        score += 2.5
    if metric == "ng_exploration_reserves_lower48" and (
        route_reserves_state(q) or route_reserves_resource_category(q)
    ):
        score += 1.5
    if metric == "lng_imports" and route_import_region(q) and "import" in q:
        score += 1.5
    if metric == "lng_exports" and route_export_region(q) and "export" in q:
        score += 1.5
    if metric == "ng_electricity" and "share" in q:
        score -= 0.75
    if metric == "ng_consumption_by_sector" and "renewables" in q and "power sector" in q:
        score -= 2.0
    if metric == "ng_consumption_by_sector" and all(
        token in q for token in ("renewables", "power sector", "demand")
    ):
        score -= 2.0
    if metric == "working_gas_storage_change_weekly" and any(
        term in q for term in ("build", "injection", "withdrawal", "storage injection")
    ):
        score += 1.5
    if metric == "working_gas_storage_lower48" and any(
        term in q for term in ("build", "injection", "withdrawal", "storage injection")
    ):
        score -= 1.0
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
    if metric == "des_gas_production_index" and "consumption" in q:
        score -= 2.0
    if metric == "open_interest" and "interest rate" in q:
        score -= 1.5
    if metric.startswith("des_") and "survey" not in q and "index" not in q and "dallas fed" not in q:
        if metric not in {
            "des_breakeven_oil_us",
            "des_breakeven_gas_us",
            "des_wti_price_expectation_1y",
            "des_hh_price_expectation_1y",
        }:
            score -= 0.5
    if metric.startswith("des_") and "demand" in q and "iso" in q:
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
        if metric == "working_gas_storage_lower48" and wants_storage_level_and_change(q):
            if region:
                filters["region"] = region
            filters["include_weekly_change"] = True
        elif region:
            filters["region"] = region
        elif wants_regional_grouping(q) or wants_storage_ranking_by_region(q):
            filters["group_by"] = "region"
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
    elif metric == "ng_electricity":
        if wants_seasonal_norm_comparison(q):
            normal_years = route_weather_normal_years(q)
            filters["normal_years"] = (
                normal_years
                if normal_years in ALLOWED_WEATHER_NORMAL_YEARS
                else 5
            )
    elif metric == "ng_supply_balance_regime":
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
    elif metric == "weather_degree_days_forecast_vs_5y":
        region = route_weather_region(q)
        if region:
            filters["region"] = region
        else:
            filters["region"] = "lower48"
        normal_years = route_weather_normal_years(q)
        filters["normal_years"] = (
            normal_years
            if normal_years in ALLOWED_WEATHER_NORMAL_YEARS
            else 5
        )
    elif metric == "weather_regional_demand_drivers":
        normal_years = route_weather_normal_years(q)
        filters["normal_years"] = (
            normal_years
            if normal_years in ALLOWED_WEATHER_NORMAL_YEARS
            else 5
        )

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
        elif primary_metric == "weather_degree_days_forecast_vs_5y":
            if region not in ALLOWED_WEATHER_REGIONS:
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
    if "normal_years" in filters:
        normal_years = filters["normal_years"]
        if primary_metric != "weather_degree_days_forecast_vs_5y":
            filters.pop("normal_years", None)
        else:
            try:
                parsed_years = int(normal_years)
            except (TypeError, ValueError):
                filters.pop("normal_years", None)
            else:
                if parsed_years in ALLOWED_WEATHER_NORMAL_YEARS:
                    filters["normal_years"] = parsed_years
                else:
                    filters.pop("normal_years", None)

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
    current_like_only = any(token in normalized for token in ("current", "latest", "right now", "today")) and not re.search(
        r"(20\d{2})-(\d{2})|(?:last|past)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+"
        r"(day|days|week|weeks|month|months|year|years)|ytd|year to date|this year|last year|past year|last month|past month|last week|past week",
        normalized,
    )

    candidates = score_routes(normalized)
    confidence = candidate_confidence(candidates)
    ambiguous = is_ambiguous(candidates)

    # Deterministic domain fast-paths for frequent electricity + gas asks.
    if (
        "power burn" in normalized
        and "natural gas" in normalized
        and any(term in normalized for term in ("seasonal norm", "seasonal norms", "seasonal"))
    ):
        metric = "ng_electricity"
        seasonal_years = route_weather_normal_years(normalized)
        if seasonal_years not in ALLOWED_WEATHER_NORMAL_YEARS:
            seasonal_years = 5
        fast_start = start
        if not has_explicit_dates or current_like_only:
            fast_start = (
                pd.Timestamp(end) - pd.DateOffset(years=seasonal_years)
            ).date().isoformat()
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=fast_start,
            end=end,
            filters=build_filters(metric, normalized, 1.0),
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for natural gas power burn seasonal comparison",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if (
        any(
            phrase in normalized
            for phrase in (
                "percentage of electricity generation",
                "percent of electricity generation",
                "what percentage of electricity generation",
                "electricity generation from natural gas",
                "share of electricity from natural gas",
            )
        )
        or ("renewables" in normalized and "power sector" in normalized and "demand" in normalized)
    ):
        metric = "iso_gas_dependency"
        return HybridRouteResult(
            intent="single_metric",
            primary_metric=metric,
            metrics=[metric],
            start=start,
            end=end,
            filters=build_filters(metric, normalized, 1.0),
            confidence=max(confidence, 0.9),
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason="Deterministic route for electricity generation gas-share dependency",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    # No rule candidate -> LLM fallback
    if not candidates:
        llm = llm_route_structured(user_query=user_query, normalized_query=normalized)
        return validate_llm_route(
            llm, start=start, end=end, normalized_query=normalized
        )

    top = candidates[0]
    if not has_explicit_dates and top.metric == "ng_exploration_reserves_lower48":
        start = "2000-01-01"
    if (not has_explicit_dates or current_like_only) and top.metric == "ng_electricity" and wants_seasonal_norm_comparison(normalized):
        normal_years = route_weather_normal_years(normalized)
        if normal_years not in ALLOWED_WEATHER_NORMAL_YEARS:
            normal_years = 5
        start = (pd.Timestamp(end) - pd.DateOffset(years=normal_years)).date().isoformat()
    if (not has_explicit_dates or current_like_only) and top.metric in {
        "ng_consumption_lower48",
        "ng_consumption_by_sector",
        "ng_electricity",
    }:
        start = (pd.Timestamp(end) - pd.DateOffset(years=2)).date().isoformat()
    filters = build_filters(top.metric, normalized, confidence)

    if (
        top.metric in {"working_gas_storage_lower48", "working_gas_storage_change_weekly"}
        and filters
        and (
            filters.get("region") in ALLOWED_STORAGE_REGIONS
            or
            filters.get("group_by") == "region"
            or filters.get("include_weekly_change") is True
        )
    ):
        return HybridRouteResult(
            intent="single_metric" if filters.get("include_weekly_change") else intent,
            primary_metric=top.metric,
            metrics=[top.metric],
            start=start,
            end=end,
            filters=filters,
            confidence=confidence,
            ambiguous=False,
            candidates=candidates[:3],
            source="rule",
            reason=f"Storage rule route on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=include_forecast,
            forecast_horizon_days=forecast_horizon_days,
        )

    if top.metric == "weather_degree_days_forecast_vs_5y" and confidence >= 0.6:
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
            reason=f"Weather degree-day rule route on {top.metric} using {top.matched_terms}",
            normalized_query=normalized,
            include_forecast=True,
            forecast_horizon_days=15,
        )

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

    # If a non-single intent still has a very strong single top metric, keep rule routing.
    if intent in {"compare", "derived", "explain"} and not ambiguous and confidence >= 0.8:
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
            reason=f"High-confidence rule match on {top.metric} for {intent} phrasing",
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
