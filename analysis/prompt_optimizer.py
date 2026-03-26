"""
Prompt feature selection via orthogonality analysis.

Instead of truncating data to fit LLM context windows, this module
selects the most informative and independent features using a greedy
correlation-based algorithm. This maximises information density per
token in any LLM prompt.

Algorithm:
1. Compute pairwise correlation matrix for all candidate features
2. Greedily select: pick feature with highest |z-score|
3. Remove all features correlated > threshold with the selected one
4. Repeat until max_count reached or no features remain
5. Return selected features sorted by |z-score| descending
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log


def select_prompt_features(
    features: list[dict[str, Any]],
    max_count: int = 15,
    corr_threshold: float = 0.7,
    history: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    """Select the most informative, independent features for an LLM prompt.

    Parameters:
        features: List of dicts, each with at least 'name' and 'z' (z-score).
                  May also include 'value', 'family', etc.
        max_count: Maximum features to select.
        corr_threshold: Features with |correlation| above this are considered
                        redundant. Only the one with the higher |z| is kept.
        history: Optional DataFrame of historical values (columns=feature names,
                 rows=dates) used to compute correlations. If None, selection
                 is based purely on |z| without dedup.

    Returns:
        Subset of `features`, sorted by |z| descending, length <= max_count.
    """
    if not features:
        return []

    # Build name→feature lookup
    by_name: dict[str, dict] = {}
    for f in features:
        name = f.get("name") or f.get("feature")
        if name and f.get("z") is not None:
            by_name[name] = f

    if not by_name:
        return features[:max_count]

    names = list(by_name.keys())

    # If no history provided, just sort by |z| and return top N
    if history is None or history.empty:
        ranked = sorted(by_name.values(), key=lambda f: abs(f.get("z", 0)), reverse=True)
        return ranked[:max_count]

    # Compute correlation matrix for features present in history
    available = [n for n in names if n in history.columns]
    if len(available) < 2:
        ranked = sorted(by_name.values(), key=lambda f: abs(f.get("z", 0)), reverse=True)
        return ranked[:max_count]

    corr = history[available].corr().abs()

    # Greedy selection
    selected: list[str] = []
    remaining = set(available)

    # Also include features not in history (can't dedup them, but still valuable)
    not_in_history = [n for n in names if n not in available]

    while remaining and len(selected) < max_count:
        # Pick feature with highest |z| from remaining
        best = max(remaining, key=lambda n: abs(by_name[n].get("z", 0)))
        selected.append(best)
        remaining.discard(best)

        # Remove all features correlated above threshold with the best
        if best in corr.columns:
            correlated = set(corr.index[corr[best] > corr_threshold]) - {best}
            remaining -= correlated

    # Add non-history features if we have room
    for n in sorted(not_in_history, key=lambda n: abs(by_name[n].get("z", 0)), reverse=True):
        if len(selected) >= max_count:
            break
        selected.append(n)

    result = [by_name[n] for n in selected]
    log.debug(
        "Prompt optimizer: {total} → {selected} features (threshold={t})",
        total=len(names), selected=len(result), t=corr_threshold,
    )
    return result


def format_features_for_prompt(
    features: list[dict[str, Any]],
    include_value: bool = True,
    include_family: bool = False,
) -> str:
    """Format selected features into a compact text block for LLM prompts.

    Parameters:
        features: Output from select_prompt_features().
        include_value: Include raw values alongside z-scores.
        include_family: Include feature family labels.

    Returns:
        Multi-line string suitable for embedding in an LLM prompt.
    """
    if not features:
        return "No feature data available."

    lines = []
    for f in features:
        name = f.get("name") or f.get("feature", "?")
        z = f.get("z", 0)
        parts = [f"{name}: z={z:+.2f}"]
        if include_value and f.get("value") is not None:
            v = f["value"]
            parts.append(f"val={v:.4g}" if isinstance(v, float) else f"val={v}")
        if include_family and f.get("family"):
            parts.append(f"[{f['family']}]")
        lines.append(" | ".join(parts))

    return "\n".join(lines)
