"""
GRID — Post-generation numeric grounding check for LLM-authored market briefings.

On 2026-09-23 ``ollama/market_briefing.py`` published "P/C ratio of 8.96 signals
maximum hedging demand" in a daily and two hourly briefings. No GRID table or
code produces 8.96, and the module had no put/call input at all — the model
invented the figure from nothing. A prompt-only fix ("never invent data") was
drafted elsewhere but never deployed; a prompt instruction is not a control.
This module is the code-level check: it runs on every generated briefing
before it is written, extracts the numeric claims in the text, and verifies
each one against the numbers actually present in the data context that was
sent to the LLM for that generation.

Relationship to existing guards (grepped first, per repo CLAUDE.md, before
writing this):

- ``oracle/claim_extractor.py`` + ``oracle/claim_verifier.py`` +
  ``oracle/sanity_checker.py`` + ``oracle/firewall.py`` extract ticker-anchored
  claims (price/percentage/direction) and re-verify them with a **live DB
  query** at check time. That pipeline would not have caught the 8.96
  incident: the claim has no ticker, so it falls through to
  ``_verify_generic`` → "ambiguous", never "contradicted". It also checks a
  *different* thing — "is this still true against the DB right now" — not
  "did the model have any basis for saying this at all".
- ``verification/ref_guard.py`` + ``verification/annotator.py`` (the
  "reference hallucination guard", ``docs/superpowers/plans/2026-04-06-
  reference-hallucination-guard.md``) checks LLM-cited **URLs**, not numbers;
  its own design doc lists content verification as a non-goal / future phase.
  It is wired into ``oracle/report.py`` and ``outputs/llm_logger.py``, but
  never into ``ollama/market_briefing.py`` (listed there as "P1", never done).

This module is a sibling to both: same GuardCheck-style audit-trail idiom
(a deterministic check, a verdict, a reason), and the same inline-tag +
footer annotation idiom as ``verification/annotator.py`` (``[unverified]``).
But it checks a number against the literal prompt context string for *this*
generation — no DB round-trip, no network call — which is the right and only
mechanism that would have caught "8.96": the number simply is not
anywhere in the text the model was given.

Scope / limitations (documented, not accidental):

- This checks numeric *grounding* (does this figure appear, at some
  reasonable rounding, in the source data), not units or semantics. A model
  that correctly copies a context value but relabels its unit (states a
  ratio as if it were a percentage) will still pass — that is a different,
  future check, same non-goal carve-out as the reference guard's.
- Dates, times, years, markdown list numbering, and small bare integers
  (<=2 digits, no $/%/decimal/comma marker) are treated as prose/labels, not
  data, and are never extracted as claims — seeded by the false-positive
  risk of flagging "## 2." or "3 sources" as ungrounded numbers. See
  ``extract_numeric_mentions`` for the exact rules.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from loguru import logger as log

NumberKind = Literal["dollar", "percent", "decimal", "integer", "ratio"]

# ── Configuration ────────────────────────────────────────────────────────
# Public so callers (and tests) can reference or override it explicitly —
# no config.py entry needed for a single, self-contained default.

DEFAULT_BANNER_THRESHOLD = 0.30  # matches oracle/publisher_gate.py's review ratio

_UNVERIFIED_TAG = " [unverified]"
_FOOTER_TEMPLATE = "\n\n---\n**Unverified figures:** {values}\n"
_BANNER_TEMPLATE = (
    "> **GROUNDING WARNING:** {ungrounded}/{total} numeric claims in this "
    "briefing ({ratio:.0%}) could not be verified against the source data "
    "context. Figures marked `[unverified]` below were not found in the "
    "data handed to the model — review before acting on them.\n\n"
)

# Bare 4-digit integers in this range are treated as calendar years, never
# as data, regardless of surrounding text. GRID's own price/level/ratio
# data never lands in this window (indices and rates are far outside it),
# so this is a safe, simple rule rather than a context-sensitive one.
_YEAR_MIN, _YEAR_MAX = 1990, 2039


# ── Data structures ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class NumericMention:
    """A single numeric token found in text, with its parsed value and span."""

    raw: str
    value: float
    kind: NumberKind
    decimals: int
    start: int
    end: int


@dataclass(frozen=True)
class GroundingResult:
    """Result of checking a briefing's numeric claims against its data context."""

    annotated_text: str
    total_numbers: int
    grounded_count: int
    ungrounded: tuple[NumericMention, ...]
    ungrounded_ratio: float
    banner_triggered: bool
    threshold: float
    checked_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a JSON-safe dict for a sidecar file or a JSONB column."""
        return {
            "checked_at": self.checked_at,
            "total_numbers": self.total_numbers,
            "grounded_count": self.grounded_count,
            "ungrounded_count": len(self.ungrounded),
            "ungrounded_ratio": round(self.ungrounded_ratio, 4),
            "ungrounded_values": list(dict.fromkeys(m.raw for m in self.ungrounded)),
            "banner_triggered": self.banner_triggered,
            "threshold": self.threshold,
        }


# ── Exclusion patterns: dates, times, list numbering ────────────────────
# Matched first; any numeric candidate whose span overlaps one of these is
# dropped, regardless of which numeric pattern below would otherwise match
# it. This is what keeps "2026-09-24T06:09:53.123456" (a real timestamp
# format this codebase writes, e.g. outputs/market_briefings/*.md footers)
# from being misread as a decimal number "53.123456".

_ISO_DATETIME_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?\b"
)
_SLASH_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b")
_MONTH_NAME_DATE_RE = re.compile(
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
    r"Dec(?:ember)?)\s+\d{1,2},?\s+\d{4}\b",
    re.IGNORECASE,
)
# Clock times, incl. seconds/microseconds and an optional zone/meridiem tag.
# Deliberately matches "H:MM" through "HH:MM:SS.ffffff" unconditionally: this
# also means a genuine colon-ratio ("14:30" as 14-to-30) would be excluded.
# GRID never renders ratios that way (put/call etc. are plain decimals — see
# module docstring), so colon-ratio notation is intentionally not a
# supported claim form here; timestamps are the far more common real case.
_CLOCK_TIME_RE = re.compile(
    r"\b\d{1,2}:\d{2}(?::\d{2}(?:\.\d+)?)?"
    r"\s*(?:AM|PM|am|pm|ET|UTC|EST|EDT|PST|PDT|CET|CT)?\b"
)
_LIST_MARKER_RE = re.compile(r"(?m)^[ \t]*\d{1,2}[.)][ \t]+")

_EXCLUSION_PATTERNS = (
    _ISO_DATETIME_RE,
    _SLASH_DATE_RE,
    _MONTH_NAME_DATE_RE,
    _CLOCK_TIME_RE,
    _LIST_MARKER_RE,
)


def _excluded_spans(text: str) -> list[tuple[int, int]]:
    """Spans that must never be read as numeric claims."""
    spans: list[tuple[int, int]] = []
    for pattern in _EXCLUSION_PATTERNS:
        spans.extend((m.start(), m.end()) for m in pattern.finditer(text))
    return spans


def _overlaps(start: int, end: int, spans: list[tuple[int, int]]) -> bool:
    return any(start < e and s < end for s, e in spans)


# ── Numeric claim patterns, in priority order ────────────────────────────
# Priority matters: a dollar/percent/ratio match claims its full span first
# so a lower-priority pattern (e.g. bare decimal) never re-matches part of
# it. Negative lookbehinds (rather than a leading \b) are used wherever a
# leading sign is allowed, since `\b` does not hold between two non-word
# characters (e.g. a space and a "-").

_DOLLAR_RE = re.compile(
    r"\$\s?(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)"
    r"(?:\s?(trillion|billion|million|thousand|T|B|M|K)\b)?",
    re.IGNORECASE,
)
_PERCENT_RE = re.compile(
    r"(?<![\w.])([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?|[+-]?\d+(?:\.\d+)?)\s?%"
)
_RATIO_X_RE = re.compile(r"(?<![\w.])(\d+(?:\.\d+)?)\s?[xX]\b")
_DECIMAL_RE = re.compile(
    r"(?<![\w.])([+-]?\d{1,3}(?:,\d{3})*\.\d+|[+-]?\d+\.\d+)\b"
)
_COMMA_INT_RE = re.compile(r"(?<![\w.,])([+-]?\d{1,3}(?:,\d{3})+)\b")
_BARE_INT_RE = re.compile(r"(?<![\w.,])([+-]?\d+)\b")

_UNIT_MULTIPLIERS: dict[str, float] = {
    "t": 1e12, "trillion": 1e12,
    "b": 1e9, "billion": 1e9,
    "m": 1e6, "million": 1e6,
    "k": 1e3, "thousand": 1e3,
}


def _to_float(mantissa: str) -> float:
    return float(mantissa.replace(",", ""))


def _decimal_places(mantissa: str) -> int:
    mantissa = mantissa.replace(",", "")
    if "." in mantissa:
        return len(mantissa.split(".", 1)[1])
    return 0


def extract_numeric_mentions(text: str) -> list[NumericMention]:
    """Extract numeric claims from text: decimals, percents, $ amounts, ratios.

    Deliberately ignores (never extracts):

    - Dates (ISO, slash, or month-name form) and clock times, including the
      ``*Generated: 2026-09-24T06:09:53.123456*`` footer this codebase writes.
    - Markdown list numbering ("1. ", "2) ") at line start.
    - Bare 4-digit integers that read as a calendar year (1990-2039).
    - Bare integers of 1-2 digits with no $ / % / decimal / comma marker —
      these are section/step counts ("3 sources", "## 2.", "Top 5"), not
      data. GRID's own context renderer always emits at least one decimal
      digit for a real measured value (``round(x, 4)`` + f-string), so a
      genuine data integer in this codebase's context text is never bare
      and unmarked. A bare integer of 3+ digits (or 4+ outside the year
      window) *is* extracted, since it can only plausibly be a real
      quantity (e.g. an index level, an OI count) at that magnitude.

    Returns mentions in left-to-right (start offset) order. Overlapping
    matches are resolved by pattern priority: $ > % > ratio (Nx) > decimal
    > comma-grouped integer > bare integer.
    """
    if not text:
        return []

    excluded = _excluded_spans(text)
    claimed: list[tuple[int, int]] = []
    mentions: list[NumericMention] = []

    def _try_claim(start: int, end: int) -> bool:
        if _overlaps(start, end, excluded) or _overlaps(start, end, claimed):
            return False
        claimed.append((start, end))
        return True

    for m in _DOLLAR_RE.finditer(text):
        if not _try_claim(m.start(), m.end()):
            continue
        mantissa, unit = m.group(1), m.group(2)
        value = _to_float(mantissa)
        if unit:
            value *= _UNIT_MULTIPLIERS.get(unit.lower(), 1.0)
        mentions.append(NumericMention(
            raw=m.group(0), value=value, kind="dollar",
            decimals=_decimal_places(mantissa), start=m.start(), end=m.end(),
        ))

    for m in _PERCENT_RE.finditer(text):
        if not _try_claim(m.start(), m.end()):
            continue
        mantissa = m.group(1)
        mentions.append(NumericMention(
            raw=m.group(0), value=_to_float(mantissa), kind="percent",
            decimals=_decimal_places(mantissa), start=m.start(), end=m.end(),
        ))

    for m in _RATIO_X_RE.finditer(text):
        if not _try_claim(m.start(), m.end()):
            continue
        mantissa = m.group(1)
        mentions.append(NumericMention(
            raw=m.group(0), value=_to_float(mantissa), kind="ratio",
            decimals=_decimal_places(mantissa), start=m.start(), end=m.end(),
        ))

    for m in _DECIMAL_RE.finditer(text):
        if not _try_claim(m.start(), m.end()):
            continue
        mantissa = m.group(1)
        mentions.append(NumericMention(
            raw=m.group(0), value=_to_float(mantissa), kind="decimal",
            decimals=_decimal_places(mantissa), start=m.start(), end=m.end(),
        ))

    for m in _COMMA_INT_RE.finditer(text):
        if not _try_claim(m.start(), m.end()):
            continue
        mentions.append(NumericMention(
            raw=m.group(0), value=_to_float(m.group(1)), kind="integer",
            decimals=0, start=m.start(), end=m.end(),
        ))

    for m in _BARE_INT_RE.finditer(text):
        digits = m.group(1).lstrip("+-")
        if len(digits) <= 2:
            continue
        value = _to_float(m.group(1))
        if len(digits) == 4 and _YEAR_MIN <= abs(int(value)) <= _YEAR_MAX:
            continue
        if not _try_claim(m.start(), m.end()):
            continue
        mentions.append(NumericMention(
            raw=m.group(0), value=value, kind="integer",
            decimals=0, start=m.start(), end=m.end(),
        ))

    mentions.sort(key=lambda mm: mm.start)
    return mentions


# ── Matching ──────────────────────────────────────────────────────────────


def _values_match(claim: NumericMention, context: NumericMention) -> bool:
    """True if *claim* is supported by *context*, allowing displayed-precision rounding.

    Normalizes away formatting (commas, $ / % markers, unit suffixes) —
    that happens in extraction already, since both mentions carry a parsed
    ``value``. What is left here is precision: "1,234.5" (1 dp) and
    "1234.50" (2 dp) are the same number, so both are rounded to the
    *coarser* of the two displayed precisions before comparing. Large
    numbers (e.g. a $B/$T-suffixed amount) use a small relative tolerance
    instead, since decimal-place rounding stops being meaningful at that
    scale.
    """
    if claim.value == context.value:
        return True

    scale = max(abs(claim.value), abs(context.value), 1.0)
    if scale >= 1_000_000:
        return abs(claim.value - context.value) / scale <= 0.005

    dp = min(claim.decimals, context.decimals)
    return round(claim.value, dp) == round(context.value, dp)


def _is_grounded(claim: NumericMention, pool: list[NumericMention]) -> bool:
    return any(_values_match(claim, ctx) for ctx in pool)


# ── Annotation ────────────────────────────────────────────────────────────


def _annotate_inline(text: str, ungrounded: list[NumericMention]) -> str:
    """Insert ``[unverified]`` right after each ungrounded number."""
    if not ungrounded:
        return text
    result = text
    for m in sorted(ungrounded, key=lambda mm: mm.start, reverse=True):
        result = result[: m.end] + _UNVERIFIED_TAG + result[m.end :]
    return result


# ── Main entry point ────────────────────────────────────────────────────


def check_numbers_grounded(
    text: str,
    data_context: str,
    *,
    threshold: float = DEFAULT_BANNER_THRESHOLD,
) -> GroundingResult:
    """Verify every numeric claim in *text* against *data_context*.

    Parameters:
        text: The LLM-generated briefing text (or fallback text) to check.
        data_context: The exact data context string handed to the LLM for
            this generation (``MarketBriefingEngine._build_data_context``'s
            output, plus whatever was appended to it before the call).
        threshold: Ungrounded-claim ratio above which a banner is prepended
            to the returned text. Defaults to ``DEFAULT_BANNER_THRESHOLD``.

    Returns:
        GroundingResult with the annotated text (inline ``[unverified]``
        tags, a footer listing them, and a banner if the threshold is
        exceeded) and the grounding stats for that check.
    """
    if not text or not text.strip():
        return GroundingResult(
            annotated_text=text or "",
            total_numbers=0,
            grounded_count=0,
            ungrounded=(),
            ungrounded_ratio=0.0,
            banner_triggered=False,
            threshold=threshold,
        )

    claims = extract_numeric_mentions(text)
    pool = extract_numeric_mentions(data_context or "")

    ungrounded = [c for c in claims if not _is_grounded(c, pool)]

    total = len(claims)
    ungrounded_count = len(ungrounded)
    ratio = (ungrounded_count / total) if total else 0.0

    annotated = _annotate_inline(text, ungrounded)
    if ungrounded:
        unique_raw = list(dict.fromkeys(m.raw for m in ungrounded))
        annotated += _FOOTER_TEMPLATE.format(values=", ".join(unique_raw))

    banner_triggered = total > 0 and ratio > threshold
    if banner_triggered:
        annotated = _BANNER_TEMPLATE.format(
            ungrounded=ungrounded_count, total=total, ratio=ratio,
        ) + annotated

    if ungrounded_count:
        log.warning(
            "Briefing number grounding: {u}/{t} numeric claims ungrounded "
            "({r:.0%}): {vals}",
            u=ungrounded_count, t=total, r=ratio,
            vals=", ".join(dict.fromkeys(m.raw for m in ungrounded)),
        )
    else:
        log.debug(
            "Briefing number grounding: {t} numeric claims, all grounded",
            t=total,
        )

    return GroundingResult(
        annotated_text=annotated,
        total_numbers=total,
        grounded_count=total - ungrounded_count,
        ungrounded=tuple(ungrounded),
        ungrounded_ratio=ratio,
        banner_triggered=banner_triggered,
        threshold=threshold,
    )
