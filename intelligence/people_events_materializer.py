"""Materialize `people_events` rows from existing people-linked channels (GD2).

SUPERSEDED (2026-10-01): `intelligence/people_events_pipeline/` replaces this
module (design: wha/outputs/GRID-PEOPLE-EVENTS-PIPELINE-DESIGN-20261001.md).
Its Form 4 dedup key (``TICKER|NAME|...``, order-sensitive name, no key
version) and its known_at convention (next-session 14:30Z) differ from the
pipeline's (``f4v2|...`` token-sorted names; 22:00 ET Section 16 cutoff, as
VS1), and it reads `quiverquant:insider` rows whose ``created_at`` the
pipeline proves is not a valid known_at bound. Do not activate this module;
it is kept only until the pipeline's writer ships with its migration.

Scope of this file -- read this before adding a channel
----------------------------------------------------------
GD2 (wha/outputs/GRID-GRANULAR-DISCOVERY-PLAN-20260927.md, section 2.1 /
gap G3) ships exactly ONE materialized channel: QuiverQuant Form 4
(`signal_sources.source_type = 'quiverquant:insider'`). Plan section 1.3 is
the audit this choice rests on: QuiverQuant Form 4 is the only people-linked
channel confirmed to carry a filing timestamp (`fileDate` and `uploaded`) on
100% of its rows. Every other channel has its own known_at gap or honesty
bug (gaps G3/G10) that this slice does not fix:

* EDGAR-native Form 4 (`source_type = 'insider'`, materialized into
  `insider_trades`): `insider_trades.filing_date` is NULL on 100% of rows.
  The real filing date, if it exists at all, sits only in
  `raw_series.raw_payload`, which this program was explicitly told not to
  read. Wire this channel in once GD-FIX verifies and populates it.
* Congress (QuiverQuant house/senate, native `congressional`): Senate
  `last_modified` is an edit date, not a disclosure date. Native
  `disclosure_date` equals `transaction_date` on the April/May 2026 rows
  (lag 0 is not real) and 91 rows have NULL `transaction_date`. These are
  GD-FIX honesty bugs, not GD2's to paper over with a guess.
* 13F, gov contracts, lobbying, news: each has its own gap/bug (G3/G10) and
  backfill dependency (GD3/GD11) documented in the plan.

Every unimplemented channel below raises `NotImplementedError` rather than
returning zero rows, so a caller cannot mistake "not built yet" for "no
events found".

Read-only on every existing table
-----------------------------------
This module only ever reads `signal_sources`. It writes only to
`people_events`, via `store.people_events.upsert_event`. It never writes to
`signal_sources`, `insider_trades`, or any other existing table, and it is
not registered with `ingestion/scheduler.py` or any systemd unit -- running
it against production is an activation decision for the plan's owner
(see the plan's blockers section), not something this module does on load.

Field-name honesty
--------------------
QuiverQuant's own JSON field names are not documented anywhere in this repo;
`ingestion/altdata/quiverquant.py` itself only demonstrates that the API's
casing is inconsistent (`AcquiredDisposedCode`, `TransactionCode` are
PascalCase; the plan's own production read found `fileDate`/`uploaded` in
lowerCamelCase). `_extract_qq_form4_fields` below therefore tries a short,
documented list of candidate keys per logical field and returns `None` --
logged, never guessed -- for a row missing a field it cannot substitute for.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.market_calendar import next_trading_day
from store.people_events import PeopleEvent, upsert_event

MATERIALIZER_VERSION = "gd2-20260927"

# A fixed-offset approximation of 09:30 America/New_York, using the winter
# (EST, UTC-5) offset year-round: 09:30 EST = 14:30 UTC. During EDT
# (UTC-4, roughly March-November) the true open is an hour earlier, 13:30
# UTC, so this anchor is always >= the true session open and never rounds a
# known_at to an instant *before* the market actually opened -- the direction
# PIT correctness requires. It is up to an hour later than the true open
# during EDT, which only makes known_at more conservative, never less.
_SESSION_OPEN_UTC = time(14, 30)


def round_up_to_next_session_open(d: date) -> datetime:
    """The earliest defensible instant for a known-only-as-a-date disclosure.

    Plan section 2.1: "use the earliest defensible public time, rounded up
    to the next session open when it is only a date." A bare date does not
    say whether the filing became public before or after that day's own
    open, so the conservative choice is the *next* trading day's open, not
    the same day's.
    """
    return datetime.combine(next_trading_day(d + timedelta(days=1)), _SESSION_OPEN_UTC, tzinfo=timezone.utc)


def normalize_actor_name(name: str) -> str:
    """Fold a free-text actor name to a stable dedup/actor-id string.

    Uppercases, collapses internal whitespace, and strips characters other
    than letters, digits, spaces and hyphens (punctuation varies across
    sources for the same person -- "Cook, Timothy D." vs "TIMOTHY D COOK").
    Not a substitute for a real canonical actor key (gap G2) -- it exists
    only so the same person's name folds to the same string across two
    sources of the *same* channel, which is all channel-internal
    deduplication needs.
    """
    cleaned = re.sub(r"[^A-Za-z0-9\s-]", " ", name)
    return re.sub(r"\s+", " ", cleaned).strip().upper()


# TransactionCode -> direction. Codes are the public SEC Form 4 vocabulary
# (Table I/II transaction codes): P = open-market purchase, S = open-market
# sale, A = grant/award. Every other code -- M/X/C (exercise or conversion of
# a derivative security), F (payment of tax by withholding), G (gift), and
# anything else this repo has not seen -- is deliberately left out of this
# map rather than forced into buy/sell/award: `people_events.direction`'s
# CHECK constraint (migrations/versions/people_events_20260927.py) has no
# slot for "exercise"/"tax"/"gift", and the plan's own signed-density formula
# (GRID-GRANULAR-DISCOVERY-PLAN-20260927.md section 2.2: "A_insider_buy: code
# P only; excludes A/M/F/G award and exercise codes and 10b5-1") treats those
# codes as their own thing, never as a buy or a sell.
_TRANSACTION_CODE_DIRECTIONS = {
    "P": "buy",
    "S": "sell",
    "A": "award",
}


def form4_transaction_direction(acquired_disposed_code: str | None, transaction_code: str | None) -> str | None:
    """Map a SEC Form 4 TransactionCode to a coarse direction, or None when it is not one.

    TransactionCode takes precedence -- it is the SEC's own economic-act
    code. AcquiredDisposedCode (A = acquired, D = disposed) is used only as a
    *fallback* when TransactionCode itself is missing: it is coarser than
    TransactionCode (an award, an option exercise, and an open-market
    purchase are all "A" under AcquiredDisposedCode) and reading it whenever
    TransactionCode is present is exactly the bug this function exists to
    fix -- on production data this counted 5,625 (TransactionCode=A,
    AcquiredDisposedCode=A) award rows and 4,036 (TransactionCode=M,
    AcquiredDisposedCode=A) option-exercise rows as "buy".
    """
    code = (transaction_code or "").strip().upper()[:1]
    if code:
        return _TRANSACTION_CODE_DIRECTIONS.get(code)
    adc = (acquired_disposed_code or "").strip().upper()[:1]
    if adc == "A":
        return "buy"
    if adc == "D":
        return "sell"
    return None


def form4_dedup_key(
    *,
    issuer_ticker: str,
    owner_id: str,
    transaction_date: date,
    transaction_code: str,
    shares: float | None,
) -> str:
    """Plan section 2.1's Form 4 dedup key.

    "(issuer CIK/ticker, reporting-owner CIK, or normalized name if no CIK,
    transaction_date, transaction_code, round(shares))". This merges
    QuiverQuant, EDGAR native, `insider_trades`, `signal_data` and the
    backlinker edges *when they are all eventually materialized* -- today
    only the QuiverQuant side writes into this table, but the key is built
    exactly as the plan specifies so a later EDGAR-native materializer
    collides into the same row instead of creating a duplicate.

    Issuer identity convention (this is the enforced part): the plan allows
    either issuer CIK or issuer ticker. This materializer's only channel
    (QuiverQuant Form 4, via `_extract_qq_form4_fields`) does not carry an
    issuer CIK under any documented key -- `signal_sources.ticker` is the
    only issuer identity `materialize_qq_form4` has -- so this function's
    convention is **ticker, upper-cased**, and the parameter is named
    `issuer_ticker` (not `issuer_id`) to say so instead of leaving the caller
    to guess which identity space a bare string belongs to. If a future
    EDGAR-native materializer (see `materialize_form4_edgar_native`) turns
    out to carry a real issuer CIK, colliding into the *same* row for the
    same act requires that materializer to resolve CIK -> ticker (or this
    function to grow an explicit `issuer_cik=` alternative) before calling
    this function -- silently mixing CIK and ticker values into one
    "issuer_ticker" slot would produce two rows for one act instead of a
    merge. All parameters are keyword-only so a call site cannot pass the
    issuer identity positionally and leave which convention it used
    ambiguous to a reader.
    """
    shares_key = "NA" if shares is None else str(round(shares))
    return "|".join([
        issuer_ticker.strip().upper(),
        owner_id.strip().upper(),
        transaction_date.isoformat(),
        (transaction_code or "").strip().upper(),
        shares_key,
    ])


@dataclass(frozen=True)
class _Form4Fields:
    owner_name: str
    transaction_code: str | None
    acquired_disposed_code: str | None
    shares: float | None
    price: float | None
    file_date: date | None
    uploaded_raw: str | None


def _first_present(d: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _parse_date_like(v: Any) -> date | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        s = str(v).replace("Z", "+00:00")
        return datetime.fromisoformat(s[:10]).date() if len(s) >= 10 else None
    except (ValueError, TypeError):
        return None


def _parse_float_like(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# Candidate keys per logical field. Only `AcquiredDisposedCode`,
# `TransactionCode` and `TransactionType` are verified against this repo's
# own code (ingestion/altdata/quiverquant.py::_insider_signal_type);
# `fileDate`/`uploaded` are verified against the plan's own read-only
# production audit (section 1.3). The rest are unverified best-effort
# aliases for a field this repo does not otherwise parse -- a row missing
# every alias for `owner_name` or `file_date` is skipped, never guessed.
_OWNER_NAME_KEYS = ("Name", "Insider", "InsiderName", "Reporter", "Owner", "OwnerName")
_TRANSACTION_CODE_KEYS = ("TransactionCode", "transactionCode")
_ACQ_DISP_KEYS = ("AcquiredDisposedCode", "acquiredDisposedCode")
_SHARES_KEYS = ("Shares", "shares")
_PRICE_KEYS = ("Price", "PricePerShare", "price", "pricePerShare")
_FILE_DATE_KEYS = ("fileDate", "FileDate", "FilingDate")
_UPLOADED_KEYS = ("uploaded", "Uploaded")


def _extract_qq_form4_fields(signal_value: dict[str, Any]) -> _Form4Fields | None:
    """Best-effort extraction from a `quiverquant:insider` `signal_value` JSON blob.

    Returns None (and logs why) when the fields this table cannot do
    without -- an identifiable actor and a filing date -- are absent under
    every known alias, rather than substituting a default.
    """
    owner_name = _first_present(signal_value, _OWNER_NAME_KEYS)
    file_date = _parse_date_like(_first_present(signal_value, _FILE_DATE_KEYS))
    if not owner_name or file_date is None:
        log.debug(
            "people_events_materializer: skipping quiverquant:insider row, "
            "missing owner_name={on!r} or file_date={fd!r} under known aliases",
            on=owner_name, fd=file_date,
        )
        return None
    return _Form4Fields(
        owner_name=str(owner_name),
        transaction_code=_first_present(signal_value, _TRANSACTION_CODE_KEYS),
        acquired_disposed_code=_first_present(signal_value, _ACQ_DISP_KEYS),
        shares=_parse_float_like(_first_present(signal_value, _SHARES_KEYS)),
        price=_parse_float_like(_first_present(signal_value, _PRICE_KEYS)),
        file_date=file_date,
        uploaded_raw=_first_present(signal_value, _UPLOADED_KEYS),
    )


def qq_form4_known_at(file_date: date | None, uploaded_raw: str | None) -> tuple[datetime, str] | None:
    """Plan section 2.1's Form 4 known_at rule: QuiverQuant `fileDate`, basis `filing`.

    Falls back to `uploaded` (QuiverQuant's own ingestion timestamp) under
    basis `first_seen` only if `fileDate` is absent -- the plan's audit found
    it present on 100% of rows, so this branch is a safety net, not the
    intended path. Returns None if neither is present.
    """
    if file_date is not None:
        return round_up_to_next_session_open(file_date), "filing"
    if uploaded_raw:
        try:
            return datetime.fromisoformat(str(uploaded_raw).replace("Z", "+00:00")), "first_seen"
        except (ValueError, TypeError):
            return None
    return None


@dataclass(frozen=True)
class MaterializeResult:
    """Outcome of one `materialize_*` pass."""

    channel: str
    rows_read: int = 0
    rows_upserted: int = 0
    rows_skipped: int = 0


_SELECT_QQ_FORM4 = text("""
    SELECT id, source_id, ticker, signal_date, signal_value
    FROM signal_sources
    WHERE source_type = 'quiverquant:insider'
      AND (:since IS NULL OR signal_date >= :since)
    ORDER BY signal_date ASC
""")


def materialize_qq_form4(engine: Engine, since: date | None = None) -> MaterializeResult:
    """Read `quiverquant:insider` rows from `signal_sources` and upsert `people_events`.

    Read-only on `signal_sources`. Writes only via
    `store.people_events.upsert_event`, which merges on `(channel,
    dedup_key)` rather than duplicating an act already seen from another
    source. Not scheduled anywhere -- call it explicitly.
    """
    rows_read = 0
    rows_upserted = 0
    rows_skipped = 0

    with engine.connect() as conn:
        result = conn.execute(_SELECT_QQ_FORM4, {"since": since})
        for row in result:
            rows_read += 1
            sig_id, source_id, ticker, signal_date, signal_value = row
            if not ticker or signal_date is None:
                rows_skipped += 1
                continue

            fields = _extract_qq_form4_fields(signal_value or {})
            if fields is None:
                rows_skipped += 1
                continue

            known = qq_form4_known_at(fields.file_date, fields.uploaded_raw)
            if known is None:
                log.debug(
                    "people_events_materializer: skipping signal_sources.id={id}, "
                    "no usable known_at", id=sig_id,
                )
                rows_skipped += 1
                continue
            known_at, known_at_basis = known

            actor_id = normalize_actor_name(fields.owner_name)
            direction = form4_transaction_direction(fields.acquired_disposed_code, fields.transaction_code)
            size_usd = None
            if fields.shares is not None and fields.price is not None:
                size_usd = abs(fields.shares * fields.price)

            dedup_key = form4_dedup_key(
                issuer_ticker=ticker,
                owner_id=actor_id,
                transaction_date=signal_date,
                transaction_code=fields.transaction_code or "",
                shares=fields.shares,
            )

            event = PeopleEvent(
                channel="form4",
                dedup_key=dedup_key,
                event_time=datetime.combine(signal_date, time.min, tzinfo=timezone.utc),
                known_at=known_at,
                known_at_basis=known_at_basis,
                actor_id=actor_id,
                actor_id_basis="normalized_name",  # QuiverQuant exposes no owner CIK (gap G2)
                actor_type="insider",
                entity_ticker=ticker.upper(),
                direction=direction,
                transaction_code=fields.transaction_code,  # raw SEC code, stored verbatim
                size_usd=size_usd,
                source="quiverquant",
                source_record_id=None,
                source_refs=({"source_type": "quiverquant:insider", "source_id": source_id, "signal_sources_id": sig_id},),
                n_sources=1,
                provenance={
                    "materializer_version": MATERIALIZER_VERSION,
                    "signal_sources_id": sig_id,
                    "uploaded_raw": fields.uploaded_raw,
                },
            )
            upsert_event(engine, event)
            rows_upserted += 1

    return MaterializeResult(
        channel="form4",
        rows_read=rows_read,
        rows_upserted=rows_upserted,
        rows_skipped=rows_skipped,
    )


def _not_implemented(channel: str, reason: str):
    def _raise(*_args: Any, **_kwargs: Any) -> MaterializeResult:
        raise NotImplementedError(f"materialize_{channel}() is out of GD2's scope: {reason}")
    return _raise


materialize_form4_edgar_native = _not_implemented(
    "form4_edgar_native",
    "insider_trades.filing_date is NULL on 100% of rows (gap G3); the real "
    "filing date is unverified in raw_series.raw_payload. Wire in after GD-FIX.",
)
materialize_congress = _not_implemented(
    "congress",
    "Senate last_modified is an edit date and native disclosure_date == "
    "transaction_date on all April/May 2026 rows (GD-FIX honesty bugs, not "
    "GD2's to paper over).",
)
materialize_thirteen_f = _not_implemented(
    "thirteen_f",
    "13F writer is not producing (last ingest 2026-04-12) and two disagreeing "
    "CIK maps exist for the same filers; needs GD-FIX/G6 revival first.",
)
materialize_gov_contract = _not_implemented(
    "gov_contract",
    "USASpending Start Date is used as event date with no publication date "
    "stored (gap G3); needs the GD-FIX action_date/date_signed rule first.",
)
materialize_gov_contract_qq_aggregate = _not_implemented(
    "gov_contract_qq_aggregate",
    "QuiverQuant gov-contract rows are a daily re-dump of a quarterly "
    "aggregate; known_at needs first-seen dedup-on-ingest from GD-FIX first.",
)
materialize_lobbying = _not_implemented(
    "lobbying",
    "No filing timestamp in the live feed; needs the LDA filing-date backfill (GD11) first.",
)
materialize_news = _not_implemented(
    "news",
    "published_at is usable, but echo-linking to other channels (plan section "
    "2.1's cross-channel sameness rule) needs those channels materialized first.",
)
