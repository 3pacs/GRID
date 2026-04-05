"""
GRID — Extract feature citations from LLM output text.

Identifies which features from the available set were actually referenced
by the LLM in its response. Uses exact match, normalized match, and
family-level matching. No LLM dependency — pure string matching.
"""

from __future__ import annotations

import re
from loguru import logger as log


# Common abbreviations: feature_name fragment → expanded forms
_ALIASES: dict[str, list[str]] = {
    "vix": ["vix", "volatility index", "cboe vix", "fear index"],
    "spy": ["spy", "s&p 500", "s&p500", "sp500", "spx"],
    "qqq": ["qqq", "nasdaq 100", "nasdaq100", "ndx"],
    "dxy": ["dxy", "dollar index", "us dollar index", "usd index"],
    "tlt": ["tlt", "long bond", "20-year treasury", "long-term treasury"],
    "gld": ["gld", "gold", "gold etf"],
    "btc": ["btc", "bitcoin"],
    "eth": ["eth", "ethereum", "ether"],
    "sol": ["sol", "solana"],
    "yld_curve": ["yield curve", "2s10s", "term spread", "curve inversion"],
    "hy_spread": ["high yield spread", "hy spread", "credit spread", "junk spread"],
    "fed_funds": ["fed funds", "federal funds", "fed rate", "policy rate"],
    "cpi": ["cpi", "consumer price", "inflation rate"],
    "pce": ["pce", "personal consumption"],
    "gdp": ["gdp", "gross domestic product"],
    "ism": ["ism", "pmi", "manufacturing index", "services index"],
    "unemployment": ["unemployment", "jobless", "nonfarm payroll", "jobs report"],
}

# Family-level references: if the LLM says "volatility signals", match all vol features
_FAMILY_ALIASES: dict[str, list[str]] = {
    "vol": ["volatility", "vol signals", "implied vol", "realized vol"],
    "rates": ["rates", "yields", "treasury", "bond market", "fixed income"],
    "credit": ["credit", "spreads", "high yield", "investment grade"],
    "equity": ["equities", "stocks", "equity market"],
    "crypto": ["crypto", "cryptocurrency", "digital assets"],
    "sentiment": ["sentiment", "fear", "greed", "social sentiment"],
    "macro": ["macro", "economic data", "macro indicators"],
    "commodity": ["commodities", "oil", "crude", "metals"],
    "fx": ["forex", "currency", "fx", "dollar"],
    "breadth": ["breadth", "advance decline", "market breadth"],
}


def extract_citations(
    llm_output: str,
    features_available: list[str],
    feature_families: dict[str, str] | None = None,
) -> list[str]:
    """Parse LLM output to identify which features were referenced.

    Args:
        llm_output: The text the LLM produced.
        features_available: Feature names that were in the prompt.
        feature_families: Optional {feature_name: family} mapping for family-level matching.

    Returns:
        List of feature names from features_available that were cited.
    """
    if not llm_output or not features_available:
        return []

    text_lower = llm_output.lower()
    cited: set[str] = set()

    # Build a lookup: normalized name → original feature names
    name_to_features: dict[str, list[str]] = {}
    for feat in features_available:
        # Exact name
        name_to_features.setdefault(feat.lower(), []).append(feat)
        # Underscores → spaces
        spaced = feat.lower().replace("_", " ")
        if spaced != feat.lower():
            name_to_features.setdefault(spaced, []).append(feat)

    # 1. Exact and normalized match
    for norm_name, orig_features in name_to_features.items():
        if norm_name in text_lower:
            cited.update(orig_features)

    # 2. Alias match
    for alias_key, alias_list in _ALIASES.items():
        # Find features whose name contains this alias key
        matching_features = [f for f in features_available if alias_key in f.lower()]
        if not matching_features:
            continue
        for alias in alias_list:
            if alias in text_lower:
                cited.update(matching_features)
                break

    # 3. Family-level match (if families provided)
    if feature_families:
        for family, family_aliases in _FAMILY_ALIASES.items():
            for alias in family_aliases:
                if alias in text_lower:
                    # Add all features in this family
                    family_features = [
                        f for f in features_available
                        if feature_families.get(f) == family
                    ]
                    cited.update(family_features)
                    break

    result = sorted(cited)
    if result:
        log.debug(
            "Citations: {n}/{t} features cited ({pct:.0f}%)",
            n=len(result), t=len(features_available),
            pct=len(result) / len(features_available) * 100,
        )

    return result


def compute_citation_ratio(cited: list[str], available: list[str]) -> float:
    """Ratio of cited features to available features."""
    if not available:
        return 0.0
    return len(cited) / len(available)
