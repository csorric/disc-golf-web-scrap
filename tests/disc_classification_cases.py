"""Independent reference and examples for checking the production SQL."""

import math
import random


EXAMPLES = (
    ("Aviar", 2, 3, 0, 1, 175, 83.75, "general_candidate"),
    ("Mako3", 5, 5, 0, 0, 180, 90.69444444444444, "general_candidate"),
    ("Leopard", 6, 5, -2, 1, 165, 81.38888888888889, "general_candidate"),
    ("TeeBird", 7, 5, 0, 2, 170, 56.25, "conditional_candidate"),
    ("Roadrunner", 9, 5, -4, 1, 165, 65.97222222222223, "conditional_candidate"),
    ("Mamba", 11, 6, -5, 1, 155, 63.611111111111114, "conditional_candidate"),
    ("Wraith", 11, 5, -1, 3, 170, 35.69444444444444, "not_default"),
    ("Destroyer", 12, 5, -1, 3, 175, 27.63888888888889, "not_default"),
    ("Firebird", 9, 3, 0, 4, 175, 30.13888888888889, "not_default"),
    ("Anax", 10, 6, 0, 3, 167, 39.83333333333333, "not_default"),
)


def reference(row):
    def number(value):
        try:
            result = float(value) if value is not None and not isinstance(value, bool) else None
        except (ValueError, TypeError, OverflowError):
            return None
        return result if result is not None and math.isfinite(result) else None

    def clamp(value, low=0, high=1):
        return min(high, max(low, value))

    flight = []
    unsupported = bool(row.get("flight_invalid_fields"))
    for key, low, high in (("speed", 1, 14), ("glide", 1, 7), ("turn", -5, 1), ("fade", 0, 5)):
        value = number(row.get(key))
        if value is None or not low <= value <= high:
            unsupported |= row.get(key) is not None
            value = None
        flight.append(value)
    s, g, t, f = flight
    conflicted = row.get("flight_conflict_unresolved", False)
    status = ("unresolved_conflict" if conflicted else "unsupported_flight" if unsupported
              else "missing_flight" if all(x is None for x in flight)
              else "partial_flight" if any(x is None for x in flight) else "complete")
    result = {"data_status": status}
    result["power_band"] = None if s is None else ("low" if s <= 5 else "moderate" if s <= 9 else "high" if s <= 12 else "very_high")
    result["turn_band"] = None if t is None else ("high_turn" if t <= -3 else "moderate_turn" if t <= -1 else "mild_turn" if t < 0 else "turn_resistant")
    result["fade_band"] = None if f is None else ("gentle" if f <= 1 else "moderate" if f < 3 else "strong")
    result["glide_band"] = None if g is None else ("low" if g <= 3 else "moderate" if g < 5 else "high")
    if t is None or f is None:
        stability = None
    elif f >= 2:
        stability = "turn_and_fade" if t < -1 else "very_overstable" if f >= 4 else "overstable"
    else:
        stability = "very_understable" if t <= -3 else "understable" if t <= -1.5 else "neutral"
    result["approx_stability"] = stability
    if conflicted:
        for key in ("power_band", "turn_band", "fade_band", "glide_band", "approx_stability"):
            result[key] = None

    a = clamp((s - 5) / 2) if s is not None else None
    base = None
    if status == "complete":
        # Reference expressed as point contributions, without the SQL helpers.
        base = (50 * clamp((13 - s) / 9) + 15 * clamp((g - 2) / 4)
                + 15 * clamp((3 - f) / 3)
                + 20 * ((1 - a) * clamp(1 - max(t, 0)) + a * clamp((1 - t) / 3)))
    w, lo, hi = (number(row.get(name)) for name in
                 ("normalized_weight_g", "normalized_weight_min_g", "normalized_weight_max_g"))
    has_range = any(row.get(name) is not None for name in
                    ("normalized_weight_min_g", "normalized_weight_max_g"))
    if has_range:
        weight_status = ("range" if lo is not None and hi is not None and 80 <= lo <= hi <= 220
                         and row.get("normalized_weight_g") is None else "invalid")
    elif w is not None and 80 <= w <= 220:
        weight_status = "exact"
        lo = hi = w
    elif row.get("normalized_weight_g") is not None or str(row.get("weight_source", "")).startswith("rejected_"):
        weight_status = "invalid"
    else:
        weight_status = "missing"
    exact = weight_status == "exact"
    if weight_status in ("missing", "invalid"):
        lo = hi = None
    low = high = variant = None
    if base is not None:
        low = clamp(base + a * (clamp((170 - hi) / 2, -5, 10) if hi is not None else -5), 0, 100)
        high = clamp(base + a * (clamp((170 - lo) / 2, -5, 10) if lo is not None else 10), 0, 100)
        variant = low if exact else None
    role = "unknown"
    if base is not None:
        role = ("general_candidate" if low >= 70 and s <= 9 and -2.5 <= t <= 0 and f <= 2
                else "conditional_candidate" if high >= 55 and t <= 0 and f <= 2 else "not_default")
    result.update(access_model=base, access_variant=variant, access_low=low, access_high=high,
                  beginner_role=role, has_exact_weight=exact, weight_status=weight_status,
                  weight_min_g=lo, weight_max_g=hi)
    return result


def formula_cases():
    cases = []

    def add(**overrides):
        row = dict(id=str(len(cases)), source_variant_key=f"fixture:{len(cases)}",
                   flight_record_key=str(len(cases)), normalized_manufacturer="Fixture",
                   normalized_model="Fixture", flight_scope="fixture",
                   speed=7, glide=5, turn=-1, fade=2, flight_source="fixture", flight_evidence="fixture",
                   flight_conflict=False, flight_conflict_unresolved=False, flight_invalid_fields=[],
                   normalized_weight_g=170, normalized_weight_min_g=None, normalized_weight_max_g=None,
                   weight_source="variant_title", weight_confidence=1.0, weight_evidence="fixture",
                   catalog_category=None, product_type="disc", title="", tags="", BodyHtml="", store="fixture",
                   IsDistanceDriver=False,
                   IsFairwayDriver=False, IsMidrange=False, IsPutter=False)
        row.update(overrides)
        cases.append(row)

    for name, s, g, t, f, w, _, _ in EXAMPLES:
        add(normalized_model=name, speed=s, glide=g, turn=t, fade=f, normalized_weight_g=w)
    for field, boundaries in (("speed", [1, 4, 5, 6, 7, 9, 12, 13, 14]),
                              ("glide", [1, 2, 3, 5, 6, 7]),
                              ("turn", [-5, -3, -2.5, -2, -1.5, -1, 0, 1]),
                              ("fade", [0, 1, 2, 3, 4, 5])):
        for boundary in boundaries:
            for delta in (-0.001, 0, 0.001):
                add(**{field: boundary + delta})
        for value in (None, "false", "NaN", "Infinity", "", "0.0"):
            add(**{field: value})
    for w in (None, 79, 80, 150, 155, 165, 170, 175, 180, 220, 221, "false", "NaN"):
        for s in (5, 6, 7, 13):
            add(speed=s, normalized_weight_g=w)
    for low, high in ((170, 175), (175, 170), (170, None), (None, 175), (80, 220), (170, 170)):
        add(normalized_weight_g=None, normalized_weight_min_g=low, normalized_weight_max_g=high)
    add(speed=None, glide=None, turn=None, fade=None)
    add(flight_conflict=True, flight_conflict_unresolved=True)
    add(flight_conflict=True)
    add(normalized_weight_g=None, weight_source="rejected_source_weight_g")
    add(glide=None, flight_invalid_fields=["glide"])
    random_source = random.Random(431)
    for _ in range(60):
        base = dict(speed=random_source.uniform(1, 13), glide=random_source.uniform(1, 6),
                    turn=random_source.uniform(-4, 1), fade=random_source.uniform(0, 4),
                    normalized_weight_g=random_source.uniform(100, 190))
        add(**base)
        for field, delta in (("speed", 1), ("glide", 1), ("turn", -1), ("fade", 1),
                             ("normalized_weight_g", -5)):
            add(**{**base, field: base[field] + delta})
    return cases
