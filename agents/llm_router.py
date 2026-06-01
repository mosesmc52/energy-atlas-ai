from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from agents.router import LLMRouteOutput

INTENTS: Final[tuple[str, ...]] = (
    "single_metric",
    "compare",
    "ranking",
    "derived",
    "explain",
    "ambiguous",
    "unsupported",
)

METRICS: Final[tuple[str, ...]] = (
    "working_gas_storage_change_weekly",
    "working_gas_storage_lower48",
    "henry_hub_spot",
    "lng_exports",
    "lng_imports",
    "ng_consumption_lower48",
    "ng_consumption_by_sector",
    "ng_electricity",
    "ng_production_lower48",
    "ng_exploration_reserves_lower48",
    "ng_pipeline",
    "weather_degree_days_forecast_vs_5y",
    "weekly_energy_atlas_summary",
)

REGION_FILTERS: Final[tuple[str, ...]] = (
    "lower48",
    "east",
    "midwest",
    "south_central",
    "mountain",
    "pacific",
    "south",
    "west",
    "united_states_pipeline_total",
    "canada_compressed",
    "united_states_compressed_total",
    "canada_pipeline",
    "mexico_pipeline",
    "algeria",
    "argentina",
    "al",
    "ak",
    "australia",
    "az",
    "ar",
    "bahrain",
    "bangladesh",
    "barbados",
    "belgium",
    "brazil",
    "brunei",
    "ca",
    "chile",
    "china",
    "colombia",
    "co",
    "croatia",
    "ct",
    "de",
    "dominican_republic",
    "egypt",
    "el_salvador",
    "equatorial_guinea",
    "finland",
    "fl",
    "france",
    "ga",
    "germany",
    "greece",
    "haiti",
    "hi",
    "id",
    "india",
    "indonesia",
    "il",
    "in",
    "ia",
    "israel",
    "italy",
    "jamaica",
    "japan",
    "jordan",
    "ks",
    "kuwait",
    "ky",
    "la",
    "lithuania",
    "ma",
    "malaysia",
    "malta",
    "md",
    "mauritania",
    "me",
    "mi",
    "mn",
    "mo",
    "ms",
    "mt",
    "ne",
    "netherlands",
    "nv",
    "nigeria",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "oh",
    "oman",
    "ok",
    "or",
    "pa",
    "pakistan",
    "panama",
    "peru",
    "philippines",
    "poland",
    "portugal",
    "qatar",
    "ri",
    "russia",
    "sc",
    "sd",
    "senegal",
    "singapore",
    "south_korea",
    "spain",
    "tn",
    "taiwan",
    "thailand",
    "trinidad_and_tobago",
    "tx",
    "united_states_lng_total",
    "canada_truck",
    "mexico_truck",
    "united_states_truck_total",
    "turkiye",
    "ut",
    "va",
    "vt",
    "wa",
    "wv",
    "wi",
    "wy",
    "united_arab_emirates",
    "united_kingdom",
    "united_states_total",
    "yemen",
)

RESOURCE_CATEGORY_FILTERS: Final[tuple[str, ...]] = (
    "proved_associated_gas",
    "proved_nonassociated_gas",
    "proved_ngl",
    "expected_future_gas_production",
)

DATASET_FILTERS: Final[tuple[str, ...]] = (
    "historical_projects",
    "inflow_by_region",
    "inflow_by_state",
    "inflow_single_year",
    "major_pipeline_summary",
    "natural_gas_pipeline_projects",
    "outflow_by_region",
    "outflow_by_state",
    "pipeline_state2_state_capacity",
)

METRIC_DESCRIPTIONS: Final[Dict[str, str]] = {
    "working_gas_storage_change_weekly": "Weekly change in underground working gas storage.",
    "working_gas_storage_lower48": "Total underground working gas storage inventory for lower 48/regions.",
    "henry_hub_spot": "Henry Hub natural gas spot benchmark price.",
    "lng_exports": "Natural gas exports flows; supports allowed pipeline and destination-country filters.",
    "lng_imports": "Natural gas imports flows; supports allowed pipeline and source-country filters.",
    "ng_consumption_lower48": "Natural gas consumption/use; supports allowed state filters and united_states_total.",
    "ng_consumption_by_sector": "Monthly U.S. natural gas consumption by end-use sector (residential, commercial, industrial, electric power).",
    "ng_electricity": "Natural gas consumed by electric power sector.",
    "ng_production_lower48": "Dry natural gas production/supply; supports allowed state filters and united_states_total.",
    "ng_exploration_reserves_lower48": "Natural gas exploration/proved reserves; supports allowed state and resource_category filters.",
    "ng_pipeline": "Parquet-backed natural gas pipeline datasets such as projects, inflow/outflow by region or state, major pipeline summary, and state-to-state capacity; supports dataset filter.",
    "weather_degree_days_forecast_vs_5y": "Weather degree-day forecast versus rolling 5-year normal (HDD/CDD) for 1-5, 6-10, and 11-15 day buckets, including estimated gas demand impact.",
    "weekly_energy_atlas_summary": "Derived weekly Energy Atlas recap combining weather-demand impact, storage surprise versus recent expectation proxy, LNG/supply changes, and Henry Hub price result.",
}

class LLMRouterError(RuntimeError):
    """Compatibility error raised by legacy llm_router shim."""


def _get_openai_client() -> Any:
    from openai import OpenAI

    return OpenAI()


def _to_llm_route_output(parsed) -> "LLMRouteOutput":
    from agents.router import LLMRouteOutput

    return LLMRouteOutput(
        intent=parsed.intent,
        primary_metric=parsed.primary_metric,
        metrics=parsed.metrics,
        filters=parsed.filters,
        reason=parsed.reason,
        confidence=parsed.confidence,
        ambiguous=parsed.ambiguous,
    )


def _is_transient_error(error: Exception) -> bool:
    transient_names = {
        "APIConnectionError",
        "APITimeoutError",
        "RateLimitError",
        "InternalServerError",
    }
    return error.__class__.__name__ in transient_names or isinstance(
        error,
        (ConnectionError, TimeoutError),
    )


def llm_route_structured(user_query: str, normalized_query: str) -> "LLMRouteOutput":
    try:
        from agents import llm_query_parser as parser_mod
        from agents.llm_query_parser import llm_parse_query

        # keep legacy test patch path working by forwarding client provider
        parser_mod._get_openai_client = _get_openai_client  # type: ignore[attr-defined]
        parsed = llm_parse_query(
            user_query=user_query,
            normalized_query=normalized_query,
        )
        return _to_llm_route_output(parsed)
    except Exception as exc:  # noqa: BLE001
        raise LLMRouterError(str(exc)) from exc
