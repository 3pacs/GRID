"""GRID - QuiverQuant act identity and period helpers (pure, no I/O).

``signal_sources`` upserts on ``(source_type, source_id, ticker, signal_date,
signal_type)``. The QuiverQuant writer used to write the constant
``source_id = "qq_<endpoint>"``, so every act that shared a ticker and a date
(and, for insiders, a side) collapsed into one row and the last payload won.
This module builds a ``source_id`` from the act's own identity for the four
endpoints that return one row per act, and keeps the constant id for the
ticker-level aggregate feeds.

The ``qq_`` prefix is deliberate. Every existing "is this a feed artefact?"
filter keys on it (``scripts/graph_analytics.ARTEFACT_ID_PREFIX``, the
``NOT LIKE 'qq_%'`` clauses in ``api/routers/intelligence_actors.py``, the
``qq_house_trading`` / ``qq_senate_trading`` actor exclusions in
``scripts/connect_dots*.py``). A keyed id is ``<constant feed id>:<identity>``,
so the part before the first colon is always the old constant, and
:func:`feed_source_id` recovers it for consumers that group by feed.

The identity may only use fields that survive into ``signal_value`` (the
writer drops ``Ticker``/``Date``/``ReportDate`` only), so that the re-key script
can recompute a row's identity from its stored payload and get exactly what the
writer would have written.

This module also holds the federal fiscal / calendar quarter-end helpers for
the ``gov_contracts`` endpoint, shared by the writer and the re-date script.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

# endpoint key (ingestion.altdata.quiverquant.ENDPOINTS) -> constant feed id.
_ENDPOINT_KEYS = (
    "wsb", "lobbying", "insider_trading", "gov_contracts", "off_exchange",
    "flights", "senate_trading", "house_trading", "twitter", "political_beta",
)
FEED_IDS: dict[str, str] = {k: f"qq_{k}" for k in _ENDPOINT_KEYS}

# Endpoints that return one row per act and therefore get a keyed source_id.
# Everything else is a ticker-level aggregate with nobody behind it
# (see intelligence.lever_pullers.AGGREGATE_SOURCE_TYPES) and keeps the constant.
KEYED_ENDPOINTS: frozenset[str] = frozenset(
    {"insider_trading", "house_trading", "senate_trading", "lobbying"}
)

# signal_sources.source_type -> endpoint key, for the keyed feeds. The re-key
# script walks these.
KEYED_SOURCE_TYPES: dict[str, str] = {
    "quiverquant:insider": "insider_trading",
    "quiverquant:house": "house_trading",
    "quiverquant:senate": "senate_trading",
    "quiverquant:lobbying": "lobbying",
}

IDENTITY_SEPARATOR = "|"
FEED_SEPARATOR = ":"
# The unique index is a btree; stay well below its row-size limit and keep ids
# readable. A longer identity is cut and suffixed with a digest of the whole
# thing, so two long identities still differ.
MAX_IDENTITY_LEN = 160
_DIGEST_LEN = 12

_UNKNOWN = "?"

# Field-name fallbacks mirror the people-events adapters
# (intelligence/people_events_pipeline/adapters.py: _QQ_OWNER, _QQ_MEMBER,
# _QQ_TXN, _QQ_RANGE) so both read a payload the same way.
_MEMBER_KEYS = (
    "Representative", "Senator", "Name", "Politician", "Member",
    "representative", "senator", "name", "politician", "member",
)
_NAME_KEYS = {
    "insider_trading": ("Name", "Insider", "InsiderName", "Reporter", "Owner", "OwnerName", "name"),
    "house_trading": _MEMBER_KEYS,
    "senate_trading": _MEMBER_KEYS,
}
_BIOGUIDE_KEYS = ("BioGuideID", "BioguideID", "bioguide_id", "bioguide")
_CODE_KEYS = ("TransactionCode", "transactionCode", "transaction_code", "Code")
_SHARES_KEYS = ("Shares", "shares")
_PRICE_KEYS = ("PricePerShare", "pricePerShare", "Price", "price")
_TXN_KEYS = ("Transaction", "transaction", "Type", "TransactionType")
# A disclosure range; when the payload has none the adapters fall back to Amount.
_RANGE_KEYS = ("Range", "range", "amount_range", "Amount", "amount")
_REGISTRANT_KEYS = ("Registrant", "registrant")
_CLIENT_KEYS = ("Client", "client")
_AMOUNT_KEYS = ("Amount", "amount")


# ── normalised string forms ────────────────────────────────────────────────


def _first(rec: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        v = rec.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return None


def normalize_text(value: Any) -> str:
    """Lower-case ASCII words separated by single spaces ("?" when empty).

    Accents are folded, every non-alphanumeric run becomes a space, so
    ``"SMITH, John Q."`` and ``"smith john q"`` are the same identity.
    """
    if value is None:
        return _UNKNOWN
    folded = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    words = re.sub(r"[^a-z0-9]+", " ", folded.lower()).split()
    return " ".join(words) if words else _UNKNOWN


def normalize_code(value: Any) -> str:
    """A short code (Form 4 TransactionCode, Transaction side): trimmed, lower-case."""
    if value is None:
        return _UNKNOWN
    text = re.sub(r"\s+", " ", str(value).strip().lower())
    return text or _UNKNOWN


def normalize_number(value: Any) -> str:
    """Canonical decimal string: ``1000``, ``1000.0``, ``"1,000.00"`` -> ``1000``.

    No exponent, no trailing zeros, ``-0`` is ``0``. Unparseable or missing
    values are "?" (a missing field must not collide with a real 0).
    """
    if value is None or isinstance(value, bool):
        return _UNKNOWN
    raw = str(value).strip().replace(",", "").replace("$", "")
    if not raw:
        return _UNKNOWN
    try:
        dec = Decimal(raw)
    except (InvalidOperation, ValueError):
        return _UNKNOWN
    if not dec.is_finite():
        return _UNKNOWN
    if dec == 0:
        return "0"
    out = format(dec.normalize(), "f")
    return out


def normalize_range(value: Any) -> str:
    """A disclosure range: ``"$1,001 - $15,000"`` -> ``"1001-15000"``."""
    if value is None:
        return _UNKNOWN
    text = str(value).strip().lower().replace("–", "-").replace("—", "-")
    text = text.replace("$", "").replace(",", "")
    text = re.sub(r"\s*-\s*", "-", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or _UNKNOWN


def _bound(raw: str) -> str:
    """Keep an identity inside MAX_IDENTITY_LEN, deterministically."""
    if len(raw) <= MAX_IDENTITY_LEN:
        return raw
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:_DIGEST_LEN]
    return f"{raw[: MAX_IDENTITY_LEN - _DIGEST_LEN - 1]}~{digest}"


# ── act identity ───────────────────────────────────────────────────────────


def _person(endpoint_key: str, rec: Mapping[str, Any]) -> str:
    """Stable id of the member of Congress: BioGuideID, else the normalised name."""
    bioguide = _first(rec, _BIOGUIDE_KEYS)
    if bioguide is not None:
        return str(bioguide).strip().upper()
    return normalize_text(_first(rec, _NAME_KEYS[endpoint_key]))


def identity_parts(endpoint_key: str, rec: Mapping[str, Any]) -> list[str] | None:
    """The normalised fields an act's identity is made of, or ``None`` for an aggregate endpoint.

    A part is ``"?"`` when the payload does not carry that field.
    """
    if endpoint_key not in KEYED_ENDPOINTS:
        return None
    if endpoint_key == "insider_trading":
        return [
            normalize_text(_first(rec, _NAME_KEYS[endpoint_key])),
            normalize_code(_first(rec, _CODE_KEYS)),
            normalize_number(_first(rec, _SHARES_KEYS)),
            normalize_number(_first(rec, _PRICE_KEYS)),
        ]
    if endpoint_key in ("house_trading", "senate_trading"):
        return [
            _person(endpoint_key, rec),
            normalize_code(_first(rec, _TXN_KEYS)),
            normalize_range(_first(rec, _RANGE_KEYS)),
        ]
    return [  # lobbying
        normalize_text(_first(rec, _REGISTRANT_KEYS)),
        normalize_text(_first(rec, _CLIENT_KEYS)),
        normalize_number(_first(rec, _AMOUNT_KEYS)),
    ]


def act_identity(endpoint_key: str, rec: Mapping[str, Any]) -> str | None:
    """The normalised identity of one act, or ``None`` for an aggregate endpoint.

    The identity is what distinguishes two acts that share a ticker and a
    signal date:

    * ``insider_trading``: insider name + TransactionCode + shares + price
    * ``house_trading`` / ``senate_trading``: BioGuideID (else member name)
      + Transaction + Range
    * ``lobbying``: Registrant + Client + Amount

    Known limit: every part is part of the key, so an act whose part *changes*
    between pulls gets a second key. That is how an amended Form 4 (different
    shares or price), a lobbying filing whose Amount is restated, and a House or
    Senate record that gains or loses its BioGuideID behave: the old row stays and
    a new one appears. The re-key script's duplicate probe counts the
    BioGuideID case.

    Parameters:
        endpoint_key: Key of ``quiverquant.ENDPOINTS``.
        rec: The raw QuiverQuant record, or the stored ``signal_value`` of one
            (both carry every identity field).

    Returns:
        The identity string (not including the feed prefix), or ``None`` when
        the endpoint keeps the constant id.
    """
    parts = identity_parts(endpoint_key, rec)
    if parts is None:
        return None
    return _bound(IDENTITY_SEPARATOR.join(parts))


def has_bioguide(rec: Mapping[str, Any]) -> bool:
    """True when a House/Senate payload carries a BioGuideID."""
    return _first(rec, _BIOGUIDE_KEYS) is not None


def is_partial_identity(endpoint_key: str, rec: Mapping[str, Any]) -> bool:
    """True when at least one identity field is missing from the payload."""
    parts = identity_parts(endpoint_key, rec)
    return bool(parts) and any(part == _UNKNOWN for part in parts)


def member_loose_key(endpoint_key: str, rec: Mapping[str, Any]) -> str | None:
    """House/Senate identity that ignores BioGuideID (name + Transaction + Range).

    Two stored rows on one ticker, date and side with the same loose key but
    different ``source_id`` are one act that got two keys because BioGuideID
    appeared or disappeared between pulls.
    """
    if endpoint_key not in ("house_trading", "senate_trading"):
        return None
    return IDENTITY_SEPARATOR.join([
        normalize_text(_first(rec, _NAME_KEYS[endpoint_key])),
        normalize_code(_first(rec, _TXN_KEYS)),
        normalize_range(_first(rec, _RANGE_KEYS)),
    ])


def source_id_for(endpoint_key: str, rec: Mapping[str, Any]) -> str:
    """The ``signal_sources.source_id`` the writer stores for one record."""
    feed = FEED_IDS.get(endpoint_key, f"qq_{endpoint_key}")
    identity = act_identity(endpoint_key, rec)
    if identity is None:
        return feed
    return f"{feed}{FEED_SEPARATOR}{identity}"


def legacy_source_id(endpoint_key: str) -> str:
    """The constant id written before act keys existed."""
    return FEED_IDS.get(endpoint_key, f"qq_{endpoint_key}")


def feed_source_id(source_id: str) -> str:
    """The constant feed id behind any QuiverQuant ``source_id``.

    ``qq_house_trading:a000055|purchase|1001-15000`` -> ``qq_house_trading``.
    Anything that is not a keyed QuiverQuant id is returned unchanged.
    """
    sid = source_id or ""
    if sid.startswith("qq_") and FEED_SEPARATOR in sid:
        return sid.split(FEED_SEPARATOR, 1)[0]
    return sid


def feed_source_id_sql(column: str = "source_id") -> str:
    """PostgreSQL mirror of :func:`feed_source_id` for ``GROUP BY`` clauses.

    No ``%`` and no bind-looking ``:name``: the fragment can be interpolated into
    a ``sqlalchemy.text`` statement (and a raw DB-API one) without escaping.
    """
    return (
        f"CASE WHEN substr({column}, 1, 3) = 'qq_' AND position(':' in {column}) > 0 "
        f"THEN split_part({column}, ':', 1) ELSE {column} END"
    )


def parse_payload(value: Any) -> dict[str, Any]:
    """A stored ``signal_value`` (dict, JSON text or None) as a dict."""
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes)) and value:
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


# ── gov_contracts period ───────────────────────────────────────────────────

# Calendar quarter ends: what #694 (GD-FIX) wrote, reading (Year, Qtr) as a
# calendar quarter. Kept to recognise those rows.
_CALENDAR_QUARTER_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


def parse_year_qtr(rec: Mapping[str, Any]) -> tuple[int, int] | None:
    """``(Year, Qtr)`` of a gov_contracts record or payload; ``None`` when unusable."""
    year = rec.get("Year") or rec.get("year")
    qtr = rec.get("Qtr") or rec.get("qtr") or rec.get("Quarter") or rec.get("quarter")
    try:
        year_i, qtr_i = int(year), int(qtr)
    except (TypeError, ValueError):
        return None
    if qtr_i not in (1, 2, 3, 4):
        return None
    return year_i, qtr_i


def fiscal_quarter_end(year: int, qtr: int) -> date | None:
    """End of US federal fiscal quarter ``qtr`` of fiscal year ``year``.

    FY Y Q1 = Oct-Dec of Y-1, Q2 = Jan-Mar Y, Q3 = Apr-Jun Y, Q4 = Jul-Sep Y.
    """
    try:
        if qtr == 1:
            return date(year - 1, 12, 31)
        month, day = {2: (3, 31), 3: (6, 30), 4: (9, 30)}[qtr]
        return date(year, month, day)
    except (KeyError, ValueError):
        return None


def calendar_quarter_end(year: int, qtr: int) -> date | None:
    """End of calendar quarter ``qtr`` of ``year`` (the pre-fix, wrong mapping)."""
    month_day = _CALENDAR_QUARTER_END.get(qtr)
    if month_day is None:
        return None
    try:
        return date(year, *month_day)
    except ValueError:
        return None


# ── transition guard ───────────────────────────────────────────────────────

MARKER_FILE_ENV = "GRID_QQ_TRANSITION_DONE_FILE"
DEFAULT_MARKER_FILE = "~/.grid/quiverquant_transition_done"
GUARD_OFF_ENV = "GRID_QQ_TRANSITION_GUARD"  # set to "off" to disable the guard outright

# Endpoints whose stored rows the transition scripts touch: a pull of any of them
# before the scripts have run writes keyed duplicates next to legacy rows (the four
# act-keyed feeds) or overwrites a calendar-dated row with the next quarter's payload
# (gov_contracts). The aggregate endpoints are unaffected and keep pulling.
TRANSITION_GUARDED_ENDPOINTS: frozenset[str] = KEYED_ENDPOINTS | {"gov_contracts"}


def transition_marker_path() -> Path:
    """Where the "transition applied" marker file lives (env-overridable)."""
    return Path(os.environ.get(MARKER_FILE_ENV) or DEFAULT_MARKER_FILE).expanduser()


def transition_marker_exists() -> bool:
    try:
        return transition_marker_path().exists()
    except OSError:
        return False


def transition_guard_blocks(endpoint_key: str) -> bool:
    """True when the writer must refuse to pull ``endpoint_key``.

    Fail-closed: the guarded endpoints do not pull until the coordinator creates the
    marker file after running ``scripts/qq_rekey_signal_sources.py`` and
    ``scripts/qq_gov_contracts_redate.py`` (or decides to skip them). The guard exists
    because the scheduler rebuilds its state from ``pull_log`` on restart and fires an
    overdue pull on the first tick after a deploy. Remove the guard in a follow-up once
    the transition is done.
    """
    if os.environ.get(GUARD_OFF_ENV, "").strip().lower() == "off":
        return False
    return endpoint_key in TRANSITION_GUARDED_ENDPOINTS and not transition_marker_exists()
