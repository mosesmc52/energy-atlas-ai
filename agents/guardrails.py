from __future__ import annotations


GENERAL_ENERGY_TERMS = {
    "gas",
    "natural gas",
    "lng",
    "producer",
    "producers",
    "production",
}

NATURAL_GAS_CONTEXT_TERMS = {
    "natural gas",
    "nat gas",
    "gas",
    "lng",
    "henry hub",
    "hh",
    "storage",
    "working gas",
    "injection",
    "withdrawal",
    "pipeline",
    "exports",
    "imports",
    "eia",
    "cftc",
    "cot",
    "managed money",
    "dallas fed",
    "des",
    "power plants",
    "electric power",
}

OUT_OF_SCOPE_ENERGY_TERMS = {
    "oil",
    "crude",
    "wti",
    "brent",
    "gasoline",
    "diesel",
    "coal",
    "uranium",
    "nuclear",
    "solar",
    "wind",
    "renewables",
    "renewable",
    "hydrogen",
    "battery",
    "batteries",
}

OUT_OF_SCOPE_MESSAGE = (
    "I can only answer questions in the context of natural gas right now. "
    "Try asking about natural gas production, storage, LNG, Henry Hub, pipeline flows, "
    "gas demand, or gas-related market signals."
)


def looks_like_general_energy_question(question: str, previous_context: str = "") -> bool:
    normalized = question.lower()
    if any(term in normalized for term in GENERAL_ENERGY_TERMS):
        return True
    if previous_context and any(term in normalized for term in ("country", "countries", "top")):
        return True
    return False


def is_natural_gas_question(question: str, previous_context: str = "") -> bool:
    normalized = question.lower()
    if any(term in normalized for term in NATURAL_GAS_CONTEXT_TERMS):
        return True
    if previous_context and not any(term in normalized for term in OUT_OF_SCOPE_ENERGY_TERMS):
        return True
    return False
