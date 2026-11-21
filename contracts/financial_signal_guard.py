"""Pure identification for the suppressed financial-divergence event family.

This is a replay barrier, not source qualification. A current-looking timestamp
or other payload cannot release this family before downstream lineage and
invalidation are qualified. No database, provider, or engine discovery belongs
in this module.
"""
from __future__ import annotations


_FAMILY = "fundamental_divergence"
_PRODUCER = "intelligence.fundamental_divergence"


def _normalized_marker(value: object) -> str:
    """Normalize outer whitespace and case, without coercing arbitrary objects."""
    return value.strip().casefold() if isinstance(value, str) else ""


def is_financial_divergence_signal(evt: object) -> bool:
    """Recognize any exact type, canonical/colon-prefixed source, or producer.

    One matching marker suffices even if another is missing or conflicting.
    Substrings and similar family names are not matches. This deliberately
    classifies events only; it never edits evidence or assigns provenance.
    """
    signal_type = _normalized_marker(getattr(evt, "signal_type", None))
    source = _normalized_marker(getattr(evt, "source", None))
    producer = _normalized_marker(getattr(evt, "producer_module", None))
    return (
        signal_type == _FAMILY
        or source == _FAMILY
        or source.startswith(_FAMILY + ":")
        or producer == _PRODUCER
    )
