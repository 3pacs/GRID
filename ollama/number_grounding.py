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
import math
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

from loguru import logger as log

NumberKind = Literal["dollar", "percent", "decimal", "integer", "ratio"]


PUBLICATION_EVIDENCE_RULES = 'Correlation and structural flow edges are model proxies, not measured money transfers. Stock changes are not transaction flows. Unknown sources cannot support transfer claims. Put/call ratios measure relative contracts, not buyer initiation, opening/closing, hedging intent, protection purchases or signed dealer gamma. VIX measures implied volatility; do not substitute it for realized volatility. Narrative text is commentary, never authoritative evidence for those claims. Price moves, insider/congressional ratios and confidence do not establish institutional distribution, accumulation, retail positioning or an actor-driven trend. Distinguish explicit unverified hypotheses/caveats from source-reported holdings facts. Do not invent causal predictions or action thresholds. VIX tenor comparisons require compatible observation dates. A single regime record does not establish a trend. Classifier confidence, thesis conviction and sentiment are distinct model outputs, not calibrated outcome probabilities. Narrative text is commentary, never authoritative evidence for those claims. Do not invent causal predictions or action thresholds.'

PUBLICATION_EVIDENCE_RULES += ' Signal counts do not identify informed exits or institutional conviction. Order-book depth, gamma-driven market moves and tail-risk valuation require their own measurements; those measurements are not supplied here.'

def publication_context_facts(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Limits of the current latest-only collector; prose cannot add evidence.

    Observation dates describe the readings, not release/ingest times. Matching
    dates are necessary for this daily context's curve comparison, not proof of
    simultaneous intraday quotes. The snapshot calendar date bounds eligible
    observations; no maximum age or regime ordering is inferred.
    """
    volatility = snapshot.get("volatility") or {}
    try:
        # The collector constructs this frame timestamp before reading data.
        # Use its calendar cutoff, never a date asserted by generated prose.
        cutoff = datetime.fromisoformat(snapshot.get("timestamp")).date()
    except (TypeError, ValueError):
        cutoff = None
    dates = {}
    for ticker in ("^VIX", "^VIX3M", "^VIX9D"):
        info = volatility.get(ticker) or volatility.get(ticker[1:]) or {}
        try:
            observation = date.fromisoformat(info.get("date", ""))
            if cutoff is None or observation > cutoff:
                raise ValueError("Observation is not eligible at the snapshot cutoff")
            value = info.get("value")
            valid = not isinstance(value, bool) and isinstance(value, (float, int))
            if not valid or not math.isfinite(value) or value < 0:
                raise ValueError("Invalid volatility reading")
            dates[ticker] = observation.isoformat()
        except (TypeError, ValueError, OverflowError):
            dates[ticker] = None
    return {
        "regime_timestamp": (snapshot.get("latest_regime") or {}).get("timestamp"),
        "snapshot_cutoff_date": cutoff.isoformat() if cutoff else None,
        "snapshot_cutoff_basis": "collector_snapshot_timestamp_calendar_date",
        "regime_comparator_available": False,  # Collector supplies only LIMIT 1.
        "vix_observation_dates": dates,
        "vix_dates_compatible": all(dates.values()) and len(set(dates.values())) == 1,
        "measurement_basis": _publication_measurement_facts(snapshot, cutoff),
    }


def _publication_measurement_facts(snapshot: dict[str, Any], cutoff: date | None) -> list[dict[str, Any]]:
    """Describe supplied observations; only complete typed pairs permit trends.

    No lookup, aggregation, inferred finality or broad 'verified' flag. Previous
    observations must be explicitly supplied on the same record, with their own
    dates and source receipts. Metadata is a declaration, never truth/PIT proof.
    """
    records = []
    fields = ("instrument", "measure", "unit", "population", "expiry_scope",
              "source_id", "receipt_id", "session_finality", "denominator",
              "window", "universe", "basis", "price_basis")
    optional_scope = ("basis", "price_basis", "denominator", "window", "universe")
    unknown = {"unknown", "unavailable", "none", "null", "missing", "unmeasured",
               "unspecified", "unverified", "n/a", "na", "not available", "not supplied", "-"}

    def observation(info: Any, path: str, instrument: str) -> dict[str, Any]:
        info = info if isinstance(info, dict) else {}
        record = {key: info[key].strip() if type(info.get(key)) is str and info[key].strip()
                  else None for key in fields}
        for key in fields:
            if record[key] and (record[key].casefold() in unknown or re.match(
                    r"^(?:unknown|unavailable|missing|unmeasured|unspecified)(?:\b|_)",
                    record[key], flags=re.IGNORECASE)):
                record[key] = None
        if record["population"] and record["population"].casefold() == "not_applicable":
            record["population"] = None
        # A known snapshot key binds an absent instrument, not an explicitly
        # unknown or malformed one. Keep optional-field presence so equal nulls
        # cannot turn supplied unknown comparison metadata into permission.
        if "instrument" not in info:
            record["instrument"] = instrument
        record["comparison_metadata_supplied"] = [key for key in optional_scope if key in info]
        value = info.get("value")
        try:
            valid = type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            valid = False
        record["value"] = value if valid else None
        try:
            observed = date.fromisoformat(info.get("date", ""))
            eligible = cutoff is not None and observed <= cutoff
        except (TypeError, ValueError):
            observed, eligible = None, False
        record.update(path=path, date=observed.isoformat() if observed else None,
                      eligible=eligible, source_truth="unknown", freshness="unknown")
        return record

    for category in ("equities", "options", "credit"):
        data = snapshot.get(category)
        for name, info in (data.items() if isinstance(data, dict) else []):
            if not isinstance(info, dict):
                continue
            instrument = "SPY" if category == "options" and name == "spy_put_call" else str(name)
            record = observation(info, f"{category}.{name}", instrument)
            previous = observation(info.get("previous"), f"{category}.{name}.previous", instrument)
            required = ("instrument", "measure", "unit", "population", "expiry_scope")
            record["comparator"] = previous
            record["comparator_available"] = bool(
                record["eligible"] and previous["eligible"]
                and record["value"] is not None and previous["value"] is not None
                and previous["date"] < record["date"]
                and all(record[key] is not None and record[key] == previous[key] for key in required)
                and all(item[key] is not None for item in (record, previous)
                        for key in ("source_id", "receipt_id"))
                and record["source_id"] == previous["source_id"]
                and all(record[key] is not None and record[key] == previous[key]
                        for key in optional_scope
                        if key in record["comparison_metadata_supplied"]
                        or key in previous["comparison_metadata_supplied"])
            )
            records.append(record)
    return records


def _publication_basis_reasons(text: str, snapshot: dict[str, Any], facts: dict[str, Any]) -> set[str]:
    """Bound the four retained basis defects within the existing context gate.

    This is deliberately not a natural-language fact verifier. Permissions use
    exact attributed forms and typed observations, not a disclaimer elsewhere.
    """
    normalized = _publication_normalized(text).strip()
    reasons: set[str] = set()
    records = facts["measurement_basis"]
    # Remove only literal disclosures. Any assertion after them remains visible.
    probe = normalized
    for disclosure in (
        r"\bfinal (?:session )?close is unknown\b",
        r"\ba measured credit spread is unavailable\b",
        r"\bit is not a measured oas series\b",
        r"\bnot a measured credit spread or oas\b",
        r"\bfriday spread is unavailable\b",
        r"\btraded volume and initiation are unmeasured\b",
        r"\baggregate put/call open interest does not identify buyer initiation, hedging intent or participant motives\b",
        r"\bthese are different measurement bases and cannot be directly compared\b",
    ):
        probe = re.sub(disclosure, "", probe)

    def number(value: float) -> str:
        return re.escape(f"{value:.2f}")

    def date_label(value: str) -> str:
        day = date.fromisoformat(value)
        return day.strftime("%B").lower() + " " + str(day.day)

    def attributed(record: dict[str, Any]) -> str:
        source = re.escape(record["source_id"].lower().replace("_", " "))
        return rf"{source} reports"

    close_claim = re.search(r"\b(?:closed at|(?:final|session) (?:session )?close(?: of| at| was| is))\b", probe)
    equity_records = [r for r in records if r["path"].startswith("equities.")]
    if close_claim and (re.search(r"\bs&p\s*500\b|\bgspc\b", probe) or
                        (len(equity_records) == 1 and re.search(r"\bit closed at\b", probe))):
        supported = False
        for record in equity_records:
            if not (record["instrument"] == "^GSPC" and record["eligible"]
                    and record["value"] is not None and record["value"] >= 0 and record["measure"] == "session_close"
                    and record["unit"] == "index_points"
                    and record["session_finality"] == "source_reported_final"
                    and record["source_id"] and record["receipt_id"]):
                continue
            grammar = (attributed(record) + r" a final " + re.escape(date_label(record["date"]))
                       + r" s&p 500 session close of " + number(record["value"]) + r"\.?\s*")
            supported = supported or bool(re.fullmatch(grammar, normalized))
        if not supported:
            reasons.add("unestablished_session_close_finality")

    pcr = [r for r in records if r["path"] == "options.spy_put_call"]
    options_context = ((bool(pcr) and re.search(r"\b(?:spy|p/c|pcr|put|call|ratio|participants)\b", probe))
                       or re.search(r"\bspy\b.*\b(?:p/c|pcr|put|call|ratio)\b", probe))
    change = re.search(r"\b(?:rise|rose|risen|rising|fell|fall|fallen|falling|increas\w*|decreas\w*|stable|unchanged|tighten\w*|widen\w*|changed)\b", probe)
    if options_context:
        if re.search(r"\b(?:put|call|contract)\s+volume\b|\bvolume (?:put/call|pcr)\b", probe):
            reasons.add("open_interest_is_not_traded_volume")
        if change and re.search(r"\b(?:spy|p/c|pcr|put|call|ratio)\b", probe):
            supported = False
            for record in pcr:
                if not (record["comparator_available"] and record["instrument"] == "SPY"
                        and record["unit"] == "ratio" and record["expiry_scope"] == "all"
                        and record["value"] >= 0 and record["comparator"]["value"] >= 0
                        and record["measure"] == "put_open_interest/call_open_interest"):
                    continue
                previous = record["comparator"]
                if record["value"] == previous["value"]:
                    continue
                direction = "rose" if record["value"] > previous["value"] else "fell"
                grammar = (r"source-reported spy put/call open-interest ratio " + direction
                           + " from " + number(previous["value"]) + " to " + number(record["value"])
                           + r" between the two supplied dates\.?\s*")
                supported = supported or bool(re.fullmatch(grammar, normalized))
            if not supported:
                reasons.add("missing_compatible_options_comparator")
        if re.search(r"\bparticipants\b.*\b(?:positioning|pricing in)\b|\bproves? defensive positioning\b", probe):
            reasons.add("unsupported_options_participant_intent")

    credit = [r for r in records if r["path"].startswith("credit.")]
    spread_claim = re.search(r"\b(?:credit spreads?|credit oas|oas|spread stability)\b", probe)
    if spread_claim or (credit and change and re.search(r"\bhyg(?:/lqd)?\b|\bprice-relative proxy\b", probe)):
        supported = False
        for record in credit:
            if not (record["eligible"] and record["value"] is not None
                    and record["source_id"] and record["receipt_id"]):
                continue
            if record["measure"] == "oas" and record["unit"] == "basis_points":
                grammar = (attributed(record) + " oas of " + re.escape(f"{record['value']:g}")
                           + "bp for " + re.escape(date_label(record["date"])))
                if record["comparator_available"]:
                    previous = record["comparator"]
                    grammar += (" versus " + re.escape(f"{previous['value']:g}") + "bp "
                                + re.escape(date_label(previous["date"])))
                supported = supported or bool(re.fullmatch(grammar + r"\.?\s*", normalized))
            if (not spread_claim and record["measure"] == "price_relative_proxy"
                    and record["instrument"] == "HYG/LQD" and record["unit"] == "percent"
                    and record["comparator_available"]):
                grammar = (r"the supplied hyg/lqd price-relative proxy changed "
                           + number(record["value"] - record["comparator"]["value"]) + r"%\.?\s*")
                supported = supported or bool(re.fullmatch(grammar, normalized))
        if not supported:
            if spread_claim:
                reasons.add("unestablished_credit_spread_basis")
            if change:
                reasons.add("missing_compatible_credit_comparator")

    # Quoted arithmetic is checked only against co-located or explicitly supplied
    # counts. It does not authenticate a recipe, universe, window or transaction.
    count_match = re.search(r"\b(\d+) buys?\s+(?:vs\.?|versus|and|/)\s+(\d+) sells?\b", probe)
    supplied = snapshot.get("insider_counts")
    supplied = supplied if isinstance(supplied, dict) else {}
    counts = (int(count_match[1]), int(count_match[2])) if count_match else (supplied.get("buys"), supplied.get("sells"))
    valid_counts = all(type(v) is int and 0 <= v <= 10**12 for v in counts)
    ratio_claim = re.search(r"\b(?:buy(?:/sell|-to-sell)|sell(?:/buy|-to-buy)) ratio\b|\bbuy share\b|\bbuys divided by sells\b", probe)
    if ratio_claim:
        incompatible = any(supplied.get(key + "_buy") != supplied.get(key + "_sell")
                           for key in ("window", "universe")
                           if key + "_buy" in supplied or key + "_sell" in supplied)
        if incompatible or re.search(r"\b(?:market-wide|all transactions)\b", probe):
            reasons.add("unestablished_insider_population")
        label_pattern = (r"(?P<sell_buy>\bsell(?:/buy|-to-buy) ratio\b)|"
                         r"(?P<buy_sell>\bbuy(?:/sell|-to-sell) ratio\b|\bbuys divided by sells\b)|"
                         r"(?P<buy_share>\bbuy share\b)")
        # Consume the entire adjacent atom before a unit, then validate its
        # numeric grammar. A malformed/grouped/exponent atom cannot restart at
        # a convenient suffix. Spaced numeric prefixes are rejected below.
        token_pattern = r"(?P<shown>(?:[+\-−]\s*)*[^\s%:]+)\s*(?P<unit>%|:\s*(?:[+\-−]\s*)*\d+(?:\.\d+)?\b)"
        denominator_kind = None
        for assertion in re.split(r",(?!\s*\d)\s*|\s+(?:and|but|while|whereas|however)\s+", probe):
            labels = {match.lastgroup for match in re.finditer(label_pattern, assertion)}
            if labels:
                # Multiple different labels without a clause boundary are
                # ambiguous, not permission to choose whichever fits a token.
                denominator_kind = next(iter(labels)) if len(labels) == 1 else None
                if denominator_kind is None:
                    reasons.add("insider_ratio_denominator_mismatch")
            if denominator_kind is None:
                continue
            tokens = list(re.finditer(token_pattern, assertion))
            if any(not any(token.start() <= marker.start() < token.end() for token in tokens)
                   for marker in re.finditer(r"%|:\s*(?:[+\-−]\s*)*[\d.]+|(?<=[\d.,])\s*:\s*\S+", assertion)):
                reasons.add("insider_ratio_denominator_mismatch")
            for token in tokens:
                if not valid_counts:
                    reasons.add("unknown_insider_denominator")
                    continue
                # Opening prose brackets are permitted; digits or punctuation
                # already attached to the atom are never stripped away.
                shown = re.sub(r"\s+", "", token["shown"].lstrip("([").replace("−", "-"))
                prefix = assertion[:token.start()]
                # A prose comma is not a numeric prefix. A numeric/sign atom
                # before commas or whitespace must still not restart here.
                if re.search(r"[\d.'’+\-−/][\s,]*\s+$", prefix):
                    reasons.add("insider_ratio_denominator_mismatch")
                    continue
                if not re.fullmatch(r"[+\-]?(?:\d+(?:\.\d*)?|\.\d+|[1-9]\d{0,2}(?:,\d{3})+(?:\.\d*)?)", shown):
                    reasons.add("insider_ratio_denominator_mismatch")
                    continue
                shown = shown.replace(",", "")
                remainder = assertion[token.end():]
                if remainder and not re.match(r"(?:\s|[)\]]|[.!?](?:\s|$))", remainder):
                    reasons.add("insider_ratio_denominator_mismatch")
                    continue
                if token["unit"] != "%" and not re.fullmatch(r":\s*1", token["unit"]):
                    # Only the declared x:1 ratio form is in this bounded
                    # publication contract; other denominators need a label.
                    reasons.add("insider_ratio_denominator_mismatch")
                    continue
                buys, sells = counts
                numerator, denominator = ((sells, buys) if denominator_kind == "sell_buy"
                                          else (buys, buys + sells) if denominator_kind == "buy_share"
                                          else (buys, sells))
                expected = ((100 if token["unit"] == "%" else 1) * numerator / denominator
                            if denominator else None)
                # Preserve sign and displayed precision. This arithmetic check
                # does not alter the existing numeric extraction or matcher.
                decimals = len(shown.partition(".")[2])
                if expected is None or round(expected, decimals) != float(shown):
                    reasons.add("insider_ratio_denominator_mismatch")
    return reasons


def _publication_normalized(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    normalized = "".join(c for c in normalized if unicodedata.category(c) not in {"Cf", "Mn"})
    normalized = re.sub(r"</?[a-zA-Z][^>]*>", "", normalized)
    return re.sub(r"[*_`~]+", "", normalized).lower()


def _strip_publication_disclosures(text: str) -> str:
    """Remove fixed statements of measurement limits, never asserted activity.

    'Investors are not buying puts' still asserts an unobserved participant side.
    Removing 'does not measure hedging intent' is narrower and keeps trailing
    positive claims visible to the existing clause-level check.
    """
    return re.sub(
        r"\b(?:hedging intent (?:is )?(?:unmeasured|unavailable)|"
        r"(?:the )?(?:put/call ratio|aggregate pcr|pcr) does not "
        r"(?:measure|establish|identify|reflect) "
        r"(?:hedging intent|dealer side|dealer inventory|(?:signed )?dealer gamma|downside protection purchases)|"
        r"(?:we )?cannot infer (?:signed )?dealer (?:gamma|inventory|side)(?: or inventory)? from aggregate pcr|"
        r"no dealer inventory is supplied in this snapshot|"
        r"signed dealer gamma cannot be inferred from aggregate pcr|"
        r"aggregate put/call open interest does not identify buyer initiation, hedging intent or participant motives|"
        r"no regime improvement can be inferred without a comparator|"
        r"a regime transition cannot be inferred from one record|"
        r"does not predict a crash)\b",
        "", text,
    )


def annotate_publication_context(text: str, snapshot: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Omit specific unsupported sentences, retaining other descriptive prose.

    A bounded lexical check for frozen-audit defects, not a general fact verifier.
    It grants no permission from narrative disclaimers, calibration adjectives,
    comparator booleans or engine recommendations. This collector supplies no
    historical comparator, calibrated forecasts or validated positioning policy.
    """
    facts = publication_context_facts(snapshot)
    violations = []
    reasons_seen: set[str] = set()
    # Preserve original formatting and decimal numbers. Newlines also bound
    # markdown headings/lists; a direction label is checked independently.
    segments = re.split(r"(\n+|(?<=[.!?])(?<!vs\.)\s+|,\s*(?:while|whereas|but|however|and|yet|although)\s+|\s+and\s+(?=vix(?:\s*(?:9d|3m))?\s+(?:is|was)\s+(?:unavailable|missing|unmeasured)\b)|;\s*|\s*\|\s*)", text, flags=re.IGNORECASE)
    original_segments = tuple(segments)
    state = (snapshot.get("latest_regime") or {}).get("state")
    state_label = _publication_normalized(state).strip() if isinstance(state, str) else None
    pipe_regime_context = False
    regime_metadata_context = False
    vix_clause_context = False
    confidence_forecast_context = False
    for index in range(0, len(segments), 2):
        if index == 0 or "|" not in segments[index - 1]:
            pipe_regime_context = False
        if index == 0 or not re.search(r"[|\n]", segments[index - 1]):
            regime_metadata_context = False
        if index == 0 or not re.search(r"[;,]", segments[index - 1]):
            vix_clause_context = False
        if index == 0 or not re.match(r",\s*(?:while|whereas|but|however|and|yet|although)\s+", segments[index - 1], flags=re.IGNORECASE):
            confidence_forecast_context = False
        original = segments[index]
        normalized = _strip_publication_disclosures(_publication_normalized(original))
        if re.search(r"\b(?:regime|fragile)\b|\bstate\s*:", normalized) or (
            state_label and normalized.strip() == state_label
        ):
            pipe_regime_context = True
            regime_metadata_context = True
        if re.match(r"^\s*#+\s", normalized):
            regime_metadata_context = bool(re.match(r"^\s*#+\s+(?:latest )?regime\b", normalized))
        # Strip only fixed disclosures, never arbitrary text after a negation.
        # A trailing positive assertion in the same sentence remains checked.
        for disclosure in (
            r"\blower classifier confidence does not establish stability\b",
            r"\b(?:the )?regime trend is unmeasured\b",
            r"\bcontemporaneous (?:vix )?term structure is unavailable\b",
            r"\bdealer inventory is unmeasured\b",
            r"\bpcr does not establish dealer side\b",
            r"\bnot a calibrated outcome probability\b",
        ):
            normalized = re.sub(disclosure, "", normalized)
        reasons: set[str] = _publication_basis_reasons(original, snapshot, facts)
        trend = r"\b(?:worsen\w*|deteriorat\w*|improv\w*|stabili[sz]\w*|transitioning|shifting|remained|unchanged)\b"
        if (re.search(r"\b(?:regime|fragile)\b|\bstate\s*:|^\s*(?:the\s+)?conditions\b|\bmarket conditions\b", normalized) and re.search(trend, normalized)) or re.search(
            r"\bdirection(?: of travel)?\s*(?::|-|is|=)\s*(?:\w+\s+){0,2}"
            r"(?:worsen\w*|deteriorat\w*|improv\w*|stable)\b", normalized
        ):
            reasons.add("missing_regime_comparator")
        if regime_metadata_context and re.search(
            r"^\s*trend\s*(?::|-|is|=)\s*(?:stable\b|" + trend + ")", normalized
        ):
            reasons.add("missing_regime_comparator")
        if index > 0 and "|" in segments[index - 1] and pipe_regime_context and re.search(
            r"^\s*(?:trend\s*(?::|-|is|=)\s*)?(?:stable\b|" + trend + ")", normalized
        ):
            reasons.add("missing_regime_comparator")
        if re.search(
            r"\bconfidence\b.*(?:\b(?:indicat\w*|impli\w*|means?|proves?|signals?|confirms?|suggests?|establish\w*|shows?|demonstrat\w*)\b|=>|=)"
            r".*\b(?:stability|stable|transitional state)\b|"
            r"\b(?:stability|stable)\b.*\b(?:because|due to)\b.*\bconfidence\b", normalized
        ):
            reasons.add("confidence_is_not_stability")
        tenor_text = re.sub(r"\b(?:(?:9|nine)[- ]days? vix|vix\s*(?:9|nine)[- ]days?|9d[- ]+vix)\b", "vix9d", normalized)
        tenor_text = re.sub(r"\b(?:(?:3|three)[- ]months? vix|vix\s*(?:3|three)[- ]months?|3m[- ]+vix)\b", "vix3m", tenor_text)
        tenors = {"^" + t.upper().replace(" ", "") for t in re.findall(r"\bvix(?:\s*(?:9d|3m))?\b", tenor_text)}
        curve_words = re.search(r"\b(?:contango|backwardation|term structure|volatility (?:curve|slope))\b", normalized)
        implicit_vix_curve = (
            vix_clause_context and curve_words
            and not re.search(r"\b(?:treasury|yield|bond|credit|commodit\w*|oil|gold)\b", normalized)
        )
        curve = curve_words and (bool(tenors) or implicit_vix_curve)
        if tenors:
            vix_clause_context = True
        comparison = len(tenors) > 1 and re.search(
            r"[<>]|\b(?:above|below|higher|lower|versus|vs|exceeds|outpaces|surpasses|spread|slope)\b", normalized
        )
        # A full curve assertion needs the collector's complete three-tenor
        # evidence even when the same clause also names a compatible pair.
        required = set(facts["vix_observation_dates"]) if curve else (
            tenors if len(tenors) > 1 else set(facts["vix_observation_dates"])
        )
        dates = [facts["vix_observation_dates"][t] for t in required]
        compatible = all(dates) and len(set(dates)) == 1
        if compatible and all(facts["vix_observation_dates"].values()) and not facts["vix_dates_compatible"]:
            # A dated subset remains descriptive evidence. Without its date,
            # do not promote two older matching tenors into the current curve
            # when the supplied third tenor has a different observation date.
            compatible = dates[0] in normalized
        if (curve or comparison) and not compatible:
            reasons.add("incompatible_vix_observation_dates")
        if any(facts["vix_observation_dates"][t] is None for t in tenors) and extract_numeric_mentions(
            tenor_text, include_small_integers=True
        ):
            reasons.add("ineligible_vix_observation")
        quantity = re.search(r"\d+(?:\.\d+)?\s*(?:%|percent)?", normalized)
        if quantity and re.search(r"\bconfidence\b", normalized):
            confidence_forecast_context = True
        if re.search(
            r"\b(?:markets?|equities|stocks|spy|qqq|dvn|vix|regime)\s+will\s+"
            r"(?:rise|fall|rally|plunge|surge|dump|crash)\b", normalized
        ):
            reasons.add("unsupported_directional_forecast")
        if re.search(
            r"\b(?:we|i|grid|(?:the )?model)\s+(?:forecast|predict)\w*\b.*"
            r"\b(?:recession|crash|rally|plunge|surge|dump)\b", normalized
        ):
            reasons.add("unsupported_outcome_forecast")
        if re.search(r"\bhigh[- ]probability\b", normalized) or (
            quantity and re.search(r"\b(?:probabilit\w*|chance|odds|likelihood)\b", normalized)
        ) or re.search(r"\b(?:bull|base|bear) case\s*[:(-]?\s*\d+\s*%", normalized) or (
            confidence_forecast_context
            and re.search(r"\b(?:predict\w*|forecast\w*|will (?:rise|fall|rally|plunge|surge|dump)|crash|recession|reversal)\b", normalized)
        ):
            reasons.add("uncalibrated_outcome_probability")
        non_options_dealer_description = (
            re.search(r"\b(?:treasur(?:y|ies)|commodit(?:y|ies)|corporate bonds?)\b", normalized)
            and not re.search(r"\b(?:pcr|put/call|gamma|options?)\b", normalized)
        )
        if not non_options_dealer_description and re.search(r"\b(?:dealers?|market makers?)\b", normalized) and re.search(
            r"\b(?:long|short|gamma|inventory|position\w*|buy\w*|sell\w*|hedg\w*|suppress\w*)\b", normalized
        ):
            reasons.add("aggregate_pcr_does_not_identify_dealer_side")
        if re.search(
            r"(?:^|\b(?:then|you|should|must|immediately)\s+|[,—:]\s*)"
            r"(?:exit|short|buy|sell|reduce|increase|tighten|hedge|avoid|hold|liquidate|trim|close|cut|dump|unwind)\s+"
            r"(?:all\s+|your\s+|the\s+)?(?:(?:equity|equities|portfolio|net|gross|leveraged|directional|long|short)\s+){0,3}(?:positions?|exposure|risk|stops|shares|equities|stocks|bonds|puts|calls|spy|qqq|dvn|long|short)\b|"
            r"\b(?:exit all|tighten stops|do not (?:initiate|add|chase)|reduce exposure|stay (?:flat|defensive))\b|"
            r"\bwatch for\b.*\b(?:above|below|trigger)\b.*\d", normalized
        ):
            reasons.add("unsupported_positioning_instruction")
        # This is a publication-owned defect, using the existing registry's
        # exact ticker match; no security-master or resolver mutation.
        if "dvn" in normalized:
            from analysis.market_universe import search_company
            canonical = [c for c in search_company("DVN") if c["ticker"] == "DVN"]
            if canonical and canonical[0]["sector"] == "Energy" and re.search(
                r"\b(?:biotech\w*|technology|tech)\s+(?:names|basket|stocks|tickers|(?:convergence )?signals|puts)"
                r"\s*(?:(?:for|like|including|such as)\s*)?[:(\[]?\s*(?:[a-z]{1,6}\s*,\s*)*dvn\b|"
                r"\bdvn(?:'s\s+|\s+(?:is|as|belongs to)\s+|,\s*)"
                r"(?:a\s+)?(?:leading\s+)?(?:biotech\w*|technology|tech|healthcare|pharma)\b", normalized
            ):
                reasons.add("dvn_sector_misclassification")
        if reasons:
            # Bind offsets to the unmodified input, before replacements change
            # lengths. The digest/role is supplied by the briefing caller.
            start = sum(len(part) for part in original_segments[:index])
            violations.append({"segment_index": index // 2, "reasons": sorted(reasons),
                               "start": start, "end": start + len(original),
                               "span_binding": "unmodified_context_guard_input"})
            reasons_seen.update(reasons)
            segments[index] = "[Claim omitted: " + ", ".join(sorted(reasons)) + ".]"
            if "insider_ratio_denominator_mismatch" in reasons:
                quoted = re.search(r"\b(\d+) buys?\s+(?:vs\.?|versus|and|/)\s+(\d+) sells?\b",
                                   _publication_normalized(original))
                if quoted:
                    segments[index] += (f" [Quoted counts only: {int(quoted[1])} buys / {int(quoted[2])} sells; "
                                        "aggregation recipe, window and universe unknown.]")
    return "".join(segments), {
        "passed": not violations, "reasons": sorted(reasons_seen),
        "omitted_segments": violations, "context_facts": facts,
        "scope": "bounded_publication_context_v1",
    }


def flow_evidence_kind(edge: dict[str, Any]) -> str:
    """Classify structured evidence, never prose or a confidence adjective.

    All current FlowEngine edges are inferred. A future source-reported transfer
    needs a known transactional source, receipt reference, bounded interval,
    explicit USD unit and declared conservation/dedup receipts. These declarations
    are provenance requirements, not independent verification of provider truth.
    """
    if edge.get("channel") == "risk_correlation":
        return "correlation_proxy"
    if edge.get("source_type") in ("structural_estimate", "inferred_proxy"):
        return "structural_estimate"
    if edge.get("source_type") not in ("transaction_ledger", "etf_flow", "on_chain_transfer"):
        return "unknown"
    if edge.get("unit") != "USD" or edge.get("direction") not in ("inflow", "outflow"):
        return "unknown"
    value = edge.get("value_usd")
    if isinstance(value, bool) or not isinstance(value, (float, int)) or value < 0:
        return "unknown"
    try:
        if not math.isfinite(value):
            return "unknown"
    except OverflowError:
        return "unknown"
    for key in ("source_id", "receipt_id", "conservation_receipt_id", "dedup_receipt_id"):
        if not isinstance(edge.get(key), str) or not edge[key].strip():
            return "unknown"
    if edge.get("conservation_status") != "passed" or edge.get("dedup_status") != "passed":
        return "unknown"
    try:
        start = datetime.fromisoformat(edge["interval_start"])
        end = datetime.fromisoformat(edge["interval_end"])
        if start.utcoffset() is None or end.utcoffset() is None or end <= start:
            return "unknown"
    except (KeyError, TypeError, ValueError):
        return "unknown"
    return "reported_flow"


def _reported_flow_clause_supported(clause: str, reported: list[dict[str, Any]]) -> bool:
    """Bind a single source/direction/amount, never a bag of sentence tokens.

    Only this narrow attribution grammar grants permission. Additional causal
    prose, multiple directions and unmatched trailing clauses fail closed.
    Rounding is at the amount's explicitly displayed unit/decimal precision.
    """
    match = re.fullmatch(
        r"\s*(?P<source>[\w:/.-]+)\s+"
        r"(?:(?:recorded|reported|saw)\s+(?:an?\s+)?|source-reported\s+)?"
        r"(?P<direction>inflow|outflow)\s+(?:of\s+)?"
        r"(?P<amount>\$[\d,.]+\s*(?:trillion|billion|million|thousand|[tbmk])?)"
        r"(?:\s+over\s+the\s+(?:reported\s+)?interval)?\s*",
        clause,
    )
    if match is None:
        return False
    amount = _DOLLAR_RE.fullmatch(match['amount'].strip())
    if amount is None:
        return False
    scale = _UNIT_MULTIPLIERS.get((amount[2] or '').lower(), 1.0)
    precision = _decimal_places(amount[1])
    return any(
        edge['source_id'].lower() == match['source']
        and edge['direction'] == match['direction']
        and round(edge['value_usd'] / scale, precision) == _to_float(amount[1])
        for edge in reported
    )


def _actor_clause_nonclaim(clause: str) -> bool:
    """Only whole bounded caveats/hypotheses; never a permission-bearing adjective."""
    actor = r"(?:institutional|retail)"
    subject = actor + r"\s+(?:distribution|accumulation|positioning|ownership|buying|selling|conviction|sentiment)"
    if re.fullmatch(subject + r"\s+(?:is|remains)\s+(?:unmeasured|unknown|unverified|unavailable|not established(?: by these data)?)", clause):
        return True
    if re.fullmatch(r"(?:prices|ratios) alone cannot establish\s+" + subject, clause):
        return True
    if re.fullmatch(
            r"hypothesis:\s*" + subject
            + r"\s+(?:may|might|could)\s+(?:be occurring|occur|explain (?:the |a )?(?:rally|move|uptrend|decline))"
            + r"(?:\s+\(unverified\))?", clause):
        return True
    return False


def _actor_clause_supported(clause: str, evidence: list[dict[str, Any]]) -> bool:
    """Narrow attributed holdings direction; never permission for distribution causality.

    References/type are declared provenance, not verification of filing truth.
    No narrative text, confidence, price/ratio or generic transfer receipt qualifies.
    """
    match = re.fullmatch(
        r"\s*(?P<source>[\w:/.-]+) source-reported (?P<actor>institutional|retail) "
        r"holdings (?P<direction>increased|decreased|unchanged) for "
        r"(?P<instrument>[\w:/.^-]+) over the reported interval\s*", clause)
    if match is None:
        return False
    for record in evidence:
        if not isinstance(record, dict) or record.get("source_type") not in (
                "ownership_filing", "position_report", "broker_classified_transactions"):
            continue
        if not all(isinstance(record.get(key), str) and record[key].strip()
                   for key in ("source_id", "receipt_id", "actor", "instrument")):
            continue
        if (record["source_id"].lower() != match["source"]
                or record["actor"].lower() != match["actor"]
                or record["instrument"].lower() != match["instrument"]
                or record.get("measure") != "holdings"
                or record.get("direction") != match["direction"]):
            continue
        try:
            start = datetime.fromisoformat(record["interval_start"])
            end = datetime.fromisoformat(record["interval_end"])
            if start.utcoffset() is None or end.utcoffset() is None or end <= start:
                continue
        except (KeyError, TypeError, ValueError):
            continue
        return True
    return False


def _publication_structure_reasons(clause: str, context: set[str]) -> set[str]:
    """Reject bounded proxy-to-motive/depth/mechanism upgrades, not the nouns.

    Current inputs contain no order-book, informed-investor identification,
    dealer exposure or independently valued tail distribution. Their labels or
    a narrative flag cannot authenticate those measurements. Context carries
    only to reject a subsequent elided assertion, never to grant permission.
    """
    actor = bool(re.search(r"\b(?:informed participants?|insiders?|congressional (?:activity|traders?))\b", clause))
    book = bool(re.search(r"\b(?:bids?|bid depth|order[- ]book(?: depth)?)\b", clause))
    gamma = bool(re.search(r"\bgamma\b", clause))
    tail = bool(re.search(r"\btail[\s\u2010-\u2015-]+risk\b", clause))
    context.update(k for k, present in (("actor", actor), ("book", book), ("gamma", gamma), ("tail", tail)) if present)
    subject = (r"(?:bid depth|order[- ]book depth|informed participant exits|insider distribution|"
               r"gamma[- ]driven moves|gamma exposure|tail risk mispricing)")
    if re.fullmatch(subject + r"\s+(?:is|are|remains?)\s+(?:unknown|unmeasured|unverified|unavailable|not established)", clause.strip()):
        return set()
    if re.fullmatch(r"(?:prices|counts|pcr|ratios|vix|these data)(?: alone)?\s+(?:cannot|do not|does not) establish\s+" + subject, clause.strip()):
        return set()
    if re.fullmatch(r"hypothesis:\s*(?:informed participants (?:may|might|could) be exiting|gamma[- ]driven moves (?:may|might|could) occur)(?:\s+\(unverified\))?", clause.strip()):
        return set()
    pronoun = bool(re.match(r"^\s*(?:(?:but|and|however|yet|whereas)\s+)?(?:it|they|this|that|these|those|is|are)\b", clause))
    reasons: set[str] = set()
    if (actor or pronoun and "actor" in context) and re.search(
        r"\b(?:exit\w*|distribut\w*|accumulat\w*|dump\w*|offload\w*|conviction|leading indicator|predict\w*|occur\w*)\b", clause
    ):
        reasons.add("unsupported_informed_actor")
    if (book or pronoun and "book" in context) and re.search(
        r"\b(?:robust|thin|weak|deep|strong|absent|shallow)\b", clause
    ):
        reasons.add("unsupported_bid_depth")
    if re.search(r"\bgamma[\s\u2010-\u2015-]+driven\b", clause) or (
        pronoun and "gamma" in context and re.search(r"\b(?:occur\w*|happening)\b", clause)
    ) or (
        (gamma or pronoun and "gamma" in context)
        and re.search(r"\b(?:driv\w*|caus\w*|amplif\w*)\b.*\b(?:moves?|volatil\w*|decline|rally|risk)\b", clause)
    ):
        reasons.add("unsupported_gamma_mechanism")
    if (tail or pronoun and "tail" in context) and re.search(r"\b(?:under|over)[- ]?pric\w*|\bmispric\w*", clause):
        reasons.add("unsupported_tail_risk_valuation")
    return reasons


def _publication_inference_nonclaim(clause: str) -> bool:
    """Whole measurement denials only, never a denial of observed activity.

    No amounts, causal tails or free-form descriptions enter this grammar.
    In particular, 'institutions are not buying puts' still asserts actor side.
    """
    target = (r"(?:institutional (?:distribution|positioning)|hedg(?:e|ing) intent|"
              r"hedging|distribution|buyer initiation|fear|protection buying)")
    targets = target + r"(?:\s+or\s+" + target + r"){0,3}"
    return bool(
        re.fullmatch(r"not (?:a measure|evidence) of\s+" + targets, clause)
        or re.fullmatch(r"(?:it|this proxy)\s+(?:does not|cannot) (?:prove|confirm)\s+" + targets, clause)
        or re.fullmatch(r"so it cannot be used to claim\s+" + targets, clause)
        or clause in {"not a measured flow", "no positioning instructions are provided"}
    )


def _publication_explicit_nonactor_subject(clause: str) -> bool:
    """Whole bounded model/count observations; never erase an actor referent."""
    number = r"[+-]?\d+(?:\.\d+)?"
    return bool(
        re.fullmatch(r"sentiment is (?:neutral|bullish|bearish)(?: \(" + number + r"\))?", clause)
        or re.fullmatch(
            r"regime confidence: monitor if the classifier confidence moves above " + number
            + r" \(higher conviction\)(?: or below " + number + r" \(lower conviction\))?", clause)
        or clause == "insider data: check for any significant changes in the insider sell ratio"
        or clause == "a shift toward buys could invalidate the negative insider signal"
    )


def check_publication_claims(text: str, flow_edges: list[dict[str, Any]] | None = None, actor_evidence: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Bounded lexical gate for known publication failures, before writes/TTS.

    Fail closed on protected claims. No broad flag such as has_measured_flow,
    social commentary, or model confidence grants permission. A flow statement
    must identify its structured source and direction with a matching amount.
    This is deliberately conservative, not a complete natural-language verifier.
    """
    normalized = _strip_publication_disclosures(_publication_normalized(text))
    reasons: set[str] = set()
    reported = [e for e in (flow_edges or []) if flow_evidence_kind(e) == "reported_flow"]
    # Bind each clause independently. Decimal dots are never boundaries.
    # Commas in numbers are thousands separators, not clause separators.
    sentences = re.split(r"([.!?]+(?:\s+|$)|[;\n]+|,(?!\d)\s*|\s+(?:and|while|but|whereas|however)\s+)", normalized)
    flow_clause_context = False
    actor_context = False
    sentiment_subject = False
    structure_context: set[str] = set()
    for index in range(0, len(sentences), 2):
        if index == 0 or re.search(r"[.!?\n]", sentences[index - 1]):
            flow_clause_context = False
            sentiment_subject = False
        sentence = sentences[index]
        # Remove only fixed nonclaim phrases, never a whole sentence containing
        # an unmeasured/unavailable adjective (which can precede a false claim).
        sentence = re.sub(r"\b(?:transfer amount (?:unmeasured|unavailable)|(?:unmeasured|unavailable) transfer amount|hedging intent unmeasured|realized vol(?:atility)? (?:is )?(?:unavailable|unmeasured)|hedge funds?|inflation hedge|(?:operating|free) cash flows?|(?:money )?flow engine)\b", "", sentence)
        if not sentence.strip():
            continue
        # A model observation never erases an earlier protected actor. Its
        # exception applies only to this whole bounded clause, not later prose.
        actor_explicit = bool(re.search(
            r"\b(?:institution(?:al|s)?|retail|smart money|professional investors)\b", sentence))
        inference_nonclaim = _publication_inference_nonclaim(sentence.strip())
        actor_context = actor_context or actor_explicit
        model_nonclaim = _publication_explicit_nonactor_subject(sentence.strip())
        # The observed score is the grammatical subject of this one comma
        # continuation. No free-form 'driven by' clause receives permission.
        sentiment_continuation = sentiment_subject and index > 0 and sentences[index - 1].startswith(',') and bool(re.fullmatch(
            r"driven by conflicting signals: strong trend \([+-]?\d+(?:\.\d+)?\)", sentence.strip()))
        sentiment_subject = bool(re.fullmatch(
            r"sentiment is (?:neutral|bullish|bearish)(?: \([+-]?\d+(?:\.\d+)?\))?", sentence.strip()))
        if inference_nonclaim:
            # A denial can still be the antecedent of a later unsupported
            # assertion. It grants no permission to a following amount/claim.
            flow_clause_context = flow_clause_context or bool(re.search(r"\bflow\b", sentence))
            continue
        reasons.update(_publication_structure_reasons(sentence, structure_context))
        if (actor_context
                and re.search(r"\b(?:distribut\w*|accumulat\w*|occur\w*|sell\w*|sold|buy\w*|bought|dump\w*|offload\w*|unload\w*|led|driv\w*|driven|fuel\w*|support\w*|position\w*|holdings|ownership|own\w*|exposur\w*|bid\w*|conviction|euphori\w*|exit\w*)\b", sentence)
                and not _actor_clause_nonclaim(sentence.strip())
                and not model_nonclaim and not sentiment_continuation
                and not _actor_clause_supported(sentence.strip(), actor_evidence if isinstance(actor_evidence, list) else [])):
            reasons.add("unsupported_actor_interpretation")
        commercial_hedge_subject = (
            re.search(r"\b(?:fuel (?:costs?|prices?)|inflation|currency fluctuations?|foreign exchange|interest rates?)\b", sentence)
            and not re.search(r"\b(?:pcr|put/call|puts?|calls?|options?|dealers?|protection)\b", sentence)
        )
        options_sentence = re.sub(r"\bhedg\w*\b", "", sentence) if commercial_hedge_subject else sentence
        if re.search(r"\b(?:hedg\w*|(?:buy\w*|bought|purchas\w*|sell\w*|writ\w*)\s+(?:[\w'-]+\s+){0,5}(?:downside\s+protection|puts?|calls?)|(?:puts?|calls?)\s+(?:buy\w*|bought|purchas\w*|sell\w*)|downside\s+protection|signed\s+(?:dealer\s+)?gamma)\b", options_sentence):
            reasons.add("unsupported_options_intent")
        if re.search(r"\breali[sz]ed\s+vol(?:atility)?\b", sentence):
            reasons.add("unsupported_realized_volatility")
        flow_claim = re.search(r"\b(?:(?:in|out)?flows?|flowed|transfer(?:red|s)?|wired|money\s+(?:is\s+)?flowing|capital\s+(?:moved|rotat\w*|flight)|(?:moved|poured|rotated|drained|siphoned)\s+(?:directly\s+)?(?:from|into|out))\b", sentence)
        # An isolated currency price is not a transfer. An additional amount
        # in the same flow statement still needs its own source receipt.
        if flow_claim or (flow_clause_context and _DOLLAR_RE.fullmatch(sentence.strip())):
            if not _reported_flow_clause_supported(sentence.strip(), reported):
                reasons.add("unsupported_money_transfer")
        flow_clause_context = bool(flow_claim) or flow_clause_context
    return {"passed": not reasons, "reasons": sorted(reasons), "scope": "bounded_publication_claims_v1"}

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


def extract_numeric_mentions(text: str, *, include_small_integers: bool = False) -> list[NumericMention]:
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

    The publication VIX eligibility check opts into small integers so reading
    eligibility does not depend on the writer's reporting verb. Date/time/year
    and list-label exclusions remain; ordinary numeric grounding keeps its
    existing default policy.

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
        if len(digits) <= 2 and not include_small_integers:
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
