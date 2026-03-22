"""
GRID financial convention locking system.

Adapted from Get Physics Done's convention locking across 18 physics fields.
Maintains consistent unit handling, sign conventions, and normalization rules
across all GRID features and computations.

Convention domains:
    rates, spreads, returns, volatility, momentum, flow, macro, fx,
    commodity, credit, equity, sentiment, crypto, alternative
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loguru import logger as log


@dataclass(frozen=True)
class Convention:
    """Immutable convention specification for a financial domain."""

    domain: str
    unit: str
    annualized: bool = False
    day_count: str | None = None
    method: str | None = None
    trading_days: int = 252
    frequency: str | None = None
    notes: str = ""


# ---------------------------------------------------------------------------
# Canonical convention registry
# ---------------------------------------------------------------------------

CONVENTIONS: dict[str, Convention] = {
    "rates": Convention(
        domain="rates",
        unit="percent",
        annualized=True,
        day_count="ACT/360",
        notes="Interest rates expressed as annualized percent. "
        "Fed funds, SOFR, treasuries all use this convention.",
    ),
    "spreads": Convention(
        domain="spreads",
        unit="basis_points",
        annualized=False,
        notes="Credit spreads, term spreads in basis points. "
        "1 bp = 0.01%. OAS, Z-spread, HY-IG spread.",
    ),
    "returns": Convention(
        domain="returns",
        unit="decimal",
        annualized=True,
        method="log",
        trading_days=252,
        notes="Log returns preferred for additivity. "
        "Annualized by multiplying by sqrt(252) for vol, 252 for drift.",
    ),
    "volatility": Convention(
        domain="volatility",
        unit="percent",
        annualized=True,
        method="realized",
        trading_days=252,
        notes="Realized vol = std(log_returns) * sqrt(252). "
        "VIX is already annualized implied vol in percent.",
    ),
    "momentum": Convention(
        domain="momentum",
        unit="decimal",
        annualized=False,
        method="simple_return",
        notes="Momentum = (P_t / P_{t-n}) - 1. "
        "12-1 momentum skips most recent month to avoid reversal.",
    ),
    "flow": Convention(
        domain="flow",
        unit="usd_millions",
        annualized=False,
        method="net",
        notes="Net capital flows in USD millions. "
        "Inflows positive, outflows negative. Must sum to ~zero globally.",
    ),
    "macro": Convention(
        domain="macro",
        unit="level_or_yoy",
        annualized=False,
        frequency="monthly",
        notes="Macro indicators: levels (GDP, IP) or YoY percent change (CPI). "
        "Seasonally adjusted where available.",
    ),
    "fx": Convention(
        domain="fx",
        unit="quote_per_base",
        annualized=False,
        notes="FX quoted as units of quote currency per base. "
        "EUR/USD = 1.10 means 1 EUR = 1.10 USD. DXY is a weighted index.",
    ),
    "commodity": Convention(
        domain="commodity",
        unit="usd_per_unit",
        annualized=False,
        notes="Commodities in USD per contract unit. "
        "Oil: $/barrel, Gold: $/troy oz, Copper: $/lb.",
    ),
    "credit": Convention(
        domain="credit",
        unit="basis_points",
        annualized=False,
        notes="Credit metrics in basis points. "
        "CDS spreads, option-adjusted spreads, default probabilities.",
    ),
    "equity": Convention(
        domain="equity",
        unit="index_level",
        annualized=False,
        notes="Equity indices as price levels. "
        "Derived metrics (PE, breadth) are ratios/percentages.",
    ),
    "sentiment": Convention(
        domain="sentiment",
        unit="index",
        annualized=False,
        notes="Sentiment indicators as index values or z-scores. "
        "Consumer confidence, AAII bull/bear, put/call ratios.",
    ),
    "crypto": Convention(
        domain="crypto",
        unit="usd",
        annualized=False,
        notes="Crypto prices in USD. "
        "24/7 trading — 365 days/year for annualization, not 252.",
    ),
    "alternative": Convention(
        domain="alternative",
        unit="varies",
        annualized=False,
        notes="Alternative data: satellite imagery (VIIRS radiance), "
        "shipping (AIS vessel counts), patents (filing counts). "
        "Units are domain-specific — check source_catalog.",
    ),
}

# ---------------------------------------------------------------------------
# Validation rules
# ---------------------------------------------------------------------------

# Expected value ranges by domain (for sanity checks)
_RANGE_CHECKS: dict[str, tuple[float, float]] = {
    "rates": (-2.0, 30.0),       # Negative rates possible, >30% would be extreme
    "spreads": (-50.0, 5000.0),  # Negative spreads rare, >5000bp = extreme distress
    "volatility": (0.0, 200.0),  # Vol can't be negative, >200% is extreme
    "momentum": (-1.0, 10.0),    # -100% to +1000%
    "fx": (0.001, 50000.0),      # Covers most FX pairs
    "commodity": (0.0, 10000.0), # No negative prices
}


def get_convention(family: str) -> Convention | None:
    """Retrieve the canonical convention for a feature family.

    Parameters:
        family: Feature family name (rates, spreads, returns, etc.).

    Returns:
        Convention object, or None if family not recognized.
    """
    return CONVENTIONS.get(family.lower())


def validate_convention(
    feature_name: str,
    value: float,
    family: str,
) -> list[str]:
    """Check a feature value against its family's convention.

    Returns a list of warnings.  Empty list means no issues detected.

    Parameters:
        feature_name: Name of the feature being validated.
        value: The computed feature value.
        family: Feature family (rates, spreads, returns, etc.).

    Returns:
        list[str]: Warning messages.  Empty if all checks pass.
    """
    warnings: list[str] = []
    family_lower = family.lower()
    convention = CONVENTIONS.get(family_lower)

    if convention is None:
        warnings.append(f"Unknown family '{family}' for feature '{feature_name}'")
        return warnings

    # Range check
    if family_lower in _RANGE_CHECKS:
        lo, hi = _RANGE_CHECKS[family_lower]
        if value < lo or value > hi:
            warnings.append(
                f"{feature_name}: value {value} outside expected range "
                f"[{lo}, {hi}] for family '{family}'"
            )

    # Unit-specific heuristics
    if family_lower == "rates" and abs(value) > 100:
        warnings.append(
            f"{feature_name}: value {value} looks like basis points, "
            f"not percent (convention is percent for rates)"
        )

    if family_lower == "spreads" and 0 < abs(value) < 1:
        warnings.append(
            f"{feature_name}: value {value} looks like percent, "
            f"not basis points (convention is basis_points for spreads)"
        )

    if family_lower == "volatility" and value < 0:
        warnings.append(
            f"{feature_name}: negative volatility ({value}) is not physical"
        )

    if family_lower == "returns" and abs(value) > 2.0:
        warnings.append(
            f"{feature_name}: return {value} ({value*100:.0f}%) is extreme — "
            f"verify this is a single-period value, not cumulative"
        )

    return warnings


def validate_feature_set(
    features: dict[str, float],
    family_map: dict[str, str],
) -> dict[str, list[str]]:
    """Validate a batch of feature values against conventions.

    Parameters:
        features: Mapping of feature_name -> value.
        family_map: Mapping of feature_name -> family.

    Returns:
        dict: feature_name -> list of warnings (only features with warnings).
    """
    all_warnings: dict[str, list[str]] = {}
    for name, value in features.items():
        if value is None:
            continue
        family = family_map.get(name, "unknown")
        warns = validate_convention(name, value, family)
        if warns:
            all_warnings[name] = warns
    return all_warnings


def check_unit_compatibility(
    feature_a: str,
    family_a: str,
    feature_b: str,
    family_b: str,
    operation: str,
) -> list[str]:
    """Check that an operation between two features is dimensionally valid.

    Parameters:
        feature_a: First feature name.
        family_a: First feature's family.
        feature_b: Second feature name.
        family_b: Second feature's family.
        operation: One of 'spread', 'ratio', 'correlation'.

    Returns:
        list[str]: Warnings about unit incompatibility.
    """
    warnings: list[str] = []
    conv_a = CONVENTIONS.get(family_a.lower())
    conv_b = CONVENTIONS.get(family_b.lower())

    if conv_a is None or conv_b is None:
        return warnings  # Can't validate unknown families

    if operation == "spread":
        # Spreads require same units
        if conv_a.unit != conv_b.unit:
            warnings.append(
                f"Spread({feature_a}, {feature_b}): incompatible units "
                f"'{conv_a.unit}' vs '{conv_b.unit}'"
            )
        if conv_a.annualized != conv_b.annualized:
            warnings.append(
                f"Spread({feature_a}, {feature_b}): mixing annualized={conv_a.annualized} "
                f"with annualized={conv_b.annualized}"
            )

    elif operation == "ratio":
        # Ratios produce dimensionless quantities — most combinations are valid
        # but warn if both are already dimensionless
        if conv_a.unit == "decimal" and conv_b.unit == "decimal":
            warnings.append(
                f"Ratio({feature_a}, {feature_b}): ratio of two dimensionless "
                f"quantities — consider if spread is more appropriate"
            )

    # Correlation is always valid (dimensionless by definition)

    return warnings


def list_conventions() -> list[dict[str, Any]]:
    """Return all conventions as a list of dicts for display/API use."""
    return [
        {
            "domain": c.domain,
            "unit": c.unit,
            "annualized": c.annualized,
            "day_count": c.day_count,
            "method": c.method,
            "trading_days": c.trading_days,
            "frequency": c.frequency,
            "notes": c.notes,
        }
        for c in CONVENTIONS.values()
    ]
