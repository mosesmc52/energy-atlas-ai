from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NaturalGasMetricSnapshot:
    metric_name: str
    category: str
    subtype: str | None
    date: str
    current_value: float
    unit: str
    baseline_5y: float | None = None
    baseline_type: str = "average"
    difference: float | None = None
    percent_difference: float | None = None
    prior_value: float | None = None
    prior_difference: float | None = None
    yoy_value: float | None = None
    yoy_difference: float | None = None
    range_5y_min: float | None = None
    range_5y_max: float | None = None


def _classify_signal(category: str, subtype: str | None, percent_difference: float | None) -> str:
    if percent_difference is None:
        return "mixed"
    above = percent_difference > 5
    below = percent_difference < -5
    if not above and not below:
        return "neutral"

    # Category-aware tight/loose interpretation
    if category == "storage" and subtype == "storage_level":
        return "loose" if above else "tight"
    if category == "production":
        return "loose" if above else "tight"
    if category == "imports":
        return "loose" if above else "tight"
    if category == "exports":
        return "tight" if above else "loose"
    if category == "consumption":
        return "tight" if above else "loose"
    if category == "weather":
        return "tight" if above else "loose"

    # fallback
    if above:
        return "tight"
    if below:
        return "loose"
    return "neutral"


def _norm_phrase(percent_difference: float | None) -> str:
    if percent_difference is None:
        return "near seasonal norms"
    if percent_difference > 5:
        return "above seasonal norms"
    if percent_difference < -5:
        return "below seasonal norms"
    return "near seasonal norms"


def _range_phrase(current: float, low: float | None, high: float | None) -> str:
    if low is None or high is None:
        return ""
    span = high - low
    if span <= 0:
        return ""
    ratio = (current - low) / span
    if ratio >= 0.8:
        return "near the upper end of the recent historical range"
    if ratio <= 0.2:
        return "near the lower end of the recent historical range"
    return "within the middle of the recent historical range"


def _market_meaning(category: str, subtype: str | None, signal: str) -> str:
    if category == "storage":
        if subtype == "storage_change":
            if signal == "tight":
                return "This leans tighter because less gas was left over for storage after demand was met."
            if signal == "loose":
                return "This leans looser because more gas was left over for storage after demand was met."
            return "This suggests storage flows are close to seasonal expectations."
        if signal == "tight":
            return "This leans tighter because inventory buffers are less comfortable than normal."
        if signal == "loose":
            return "This leans looser because inventory buffers are more comfortable than normal."
        return "This suggests storage conditions are close to seasonal expectations."
    if category in {"consumption", "weather"}:
        if signal == "tight":
            return "This leans tighter because demand is running stronger than usual."
        if signal == "loose":
            return "This leans looser because demand is running weaker than usual."
        return "This suggests demand is close to seasonal expectations."
    if category == "production":
        if signal == "tight":
            return "This leans tighter because less supply is entering the system than normal."
        if signal == "loose":
            return "This leans looser because more supply is entering the system than normal."
        return "This suggests supply is close to seasonal expectations."
    if category == "exports":
        if signal == "tight":
            return "This leans tighter for the domestic market because more U.S. gas is being pulled into external demand."
        if signal == "loose":
            return "This leans looser domestically because less gas is being pulled out of the U.S. system."
        return "This suggests export pull is close to seasonal expectations."
    if category == "imports":
        if signal == "tight":
            return "This leans tighter because less external supply is entering the domestic system."
        if signal == "loose":
            return "This leans looser because more external supply is entering the domestic system."
        return "This suggests import supply is close to seasonal expectations."
    if category == "price":
        return "Prices are reacting to the balance between storage, weather, production, and export demand."
    return "This signal is mixed and should be read alongside other market drivers."


def format_natural_gas_commentary(snapshot: NaturalGasMetricSnapshot) -> dict[str, Any]:
    signal = _classify_signal(snapshot.category, snapshot.subtype, snapshot.percent_difference)
    norm = _norm_phrase(snapshot.percent_difference)
    range_text = _range_phrase(snapshot.current_value, snapshot.range_5y_min, snapshot.range_5y_max)
    baseline_label = f"5-year {snapshot.baseline_type}" if snapshot.baseline_5y is not None else "seasonal baseline"

    p1 = f"{snapshot.metric_name} is {norm}."

    if snapshot.baseline_5y is not None and snapshot.percent_difference is not None and snapshot.difference is not None:
        direction = "above" if snapshot.difference >= 0 else "below"
        p2 = (
            f"As of {snapshot.date}, the reading came in at {snapshot.current_value:,.0f} {snapshot.unit}, "
            f"about {abs(snapshot.percent_difference):.1f}% {direction} the {baseline_label} for this time of year "
            f"({abs(snapshot.difference):,.0f} {snapshot.unit} {direction})."
        )
    else:
        p2 = (
            f"As of {snapshot.date}, the reading came in at {snapshot.current_value:,.0f} {snapshot.unit}. "
            "A seasonal baseline was unavailable, so this is compared to recent observations instead."
        )

    p3_parts: list[str] = []
    if range_text:
        p3_parts.append(range_text.capitalize() + ".")
    if snapshot.prior_difference is not None:
        if snapshot.prior_difference > 0:
            p3_parts.append(f"From the prior observation, it increased by {snapshot.prior_difference:,.0f} {snapshot.unit}.")
        elif snapshot.prior_difference < 0:
            p3_parts.append(f"From the prior observation, it declined by {abs(snapshot.prior_difference):,.0f} {snapshot.unit}.")
    p3 = " ".join(p3_parts).strip()

    p4 = _market_meaning(snapshot.category, snapshot.subtype, signal)
    summary = " ".join(part for part in (p1, p2, p3, p4) if part)

    return {
        "summary": summary,
        "market_signal": signal,
        "drivers": [norm, range_text or "range context unavailable", p4],
        "supporting_stats": {
            "date": snapshot.date,
            "current_value": snapshot.current_value,
            "unit": snapshot.unit,
            "baseline_5y": snapshot.baseline_5y,
            "percent_difference": snapshot.percent_difference,
            "prior_difference": snapshot.prior_difference,
            "range_5y_min": snapshot.range_5y_min,
            "range_5y_max": snapshot.range_5y_max,
        },
    }
