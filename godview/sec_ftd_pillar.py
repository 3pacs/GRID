"""godview.sec_ftd_pillar — SEC Fails-to-Deliver (FTD) God View pillar.

Consumes (read-only) ``raw_series`` rows under the ``sec:ftd_balance:<cusip>``
namespace written by ``ingestion/altdata/sec_ftd.py`` (that module lives on
``origin/fable/sources-finra-ftd-20260918``, commit ``40a9f1ae``, not yet
merged into this branch -- read via ``git show``, never checked out).

**Each row is an outstanding BALANCE as of one settlement date -- never
summed across dates, never a T+35 timeline, never a squeeze score.** Per
the SEC's own page (https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data,
quoted verbatim in that branch's contracts doc):

    "The values of total fails-to-deliver shares represent the aggregate
    net balance of shares that failed to be delivered as of a particular
    settlement date."

    "Fails to deliver on a given day are a cumulative number of all fails
    outstanding until that day, plus new fails that occur that day, less
    fails that settle that day."

    "Fails-to-deliver can occur for a number of reasons on both long and
    short sales. Therefore, fails-to-deliver are not necessarily the
    result of short selling, and are not evidence of abusive short
    selling or 'naked' short selling."

    "...the age of fails cannot be determined by looking at these numbers."

That last quote matters for what this pillar calls "age": it is the age of
the OBSERVATION relative to ``as_of`` (``as_of - settlement_date``, computed
at read time, never persisted) -- NOT an attempt to determine how old the
underlying fails within the balance are, which the SEC explicitly says
cannot be done from this data. This module never diffs, sums, or nets
balances across settlement dates, and implements no T+35 forced-buy-in
logic or squeeze score -- ``mandatory_buyin_date``/``days_remaining``/
``squeeze_risk_score`` (already columns on the tracked
``sec_regsho_ftd_cns`` table) stay permanently NULL.

**Display symbol:** no CUSIP->ticker mapping table exists in ``schema.sql``
(grepped 2026-09-18) -- but ``ingestion/altdata/sec_ftd.py``'s own
``raw_payload`` already carries the FTD file's OWN self-reported ``symbol``
field per row (measured, from the same source, not a separate mapping
dependency). This pillar surfaces that symbol when present; when a row's
payload lacks a usable symbol, it falls back to exposing the CUSIP itself
and says so explicitly (``ticker_source = "cusip_fallback"``) rather than
guessing or leaving a blank.

**closing_price / total_failed_usd:** the FTD file's own PRICE field is
measured directly from the file (not looked up from a separate price
series); ``total_failed_usd = failed_shares * closing_price`` is derived,
same settlement date only, computed only when both inputs are present --
never a fabricated dollar figure and never summed across dates.

**Release schedule** -- SEC publishes twice monthly, quoted 2026-09-18
from the same page:

    "The first half of a given month is available at the end of the
    month. The second half of a given month is available at about the
    15th of the next month."

File names (``cnsfails<yyyymm><a|b>.zip``) confirm the split: one real
capture (``cnsfails202608b.zip``) covered settlement dates 2026-08-17
through 2026-08-31 -- so "b" (second half) is roughly the 16th through
month-end, and "a" (first half) is the 1st through the 15th.
``release_date`` is therefore INFERRED from this half-month rule, never
observed directly from an SEC-published release calendar (there is no
such calendar) -- ``availability_basis`` reflects that: it is
``inferred_schedule`` unless a pull's own timestamp genuinely falls near
the inferred release date (via the shared
``godview/availability_basis.py::classify_availability_basis``), in which
case it is ``observed_acquisition``. Given the SEC's own language is
already approximate ("about the 15th"), this pillar uses a WIDER
tolerance (10 days, not the 3 used elsewhere) so that imprecision in the
SEC's own schedule doesn't itself get misread as "not observed."

Everything above the "DB wrappers" marker is pure Python.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.availability_basis import classify_availability_basis
from godview.generations import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    latest_attempt as _latest_attempt,
    latest_complete_generation as _latest_complete_generation,
    new_generation_id,
    record_generation,
)
from store.availability import measured_or_none

PILLAR_NAME = "sec_ftd"
SERIES_PREFIX = "sec:ftd_balance"
UNIT_SHARES = "shares"
UNIT_USD = "usd"

RELEASE_RULE_ID = "sec_ftd_half_month_v1"
_RELEASE_RULE_URL = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"
AVAILABILITY_TOLERANCE_DAYS = 10  # wider than usual -- see module docstring
FIRST_HALF_LAST_DAY = 15  # settlement dates 1..15 -> "first half"

NOT_A_TIMELINE_NOTE = (
    "outstanding balance as of one settlement date; never summed across dates, "
    "no T+35 buy-in timeline, no squeeze score"
)
AGE_NOTE = (
    "age is (as_of - settlement_date), i.e. how stale this observation is -- "
    "NOT the age of the underlying fails, which the SEC states cannot be determined from this data"
)


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    next_month_first = date(year, month + 1, 1)
    return (next_month_first - date(year, month, 1)).days


def compute_release_date(settlement_date: date) -> tuple[date, str]:
    """Half-month rule: first-half files at month-end; second-half ~15th of next month.

    Always returns a release_date (never withheld) -- the half-month rule
    applies to every settlement date, unlike CFTC's Tuesday-only gate.
    """
    if settlement_date.day <= FIRST_HALF_LAST_DAY:
        last_day = _last_day_of_month(settlement_date.year, settlement_date.month)
        release_date = date(settlement_date.year, settlement_date.month, last_day)
        half = "first"
    else:
        if settlement_date.month == 12:
            next_year, next_month = settlement_date.year + 1, 1
        else:
            next_year, next_month = settlement_date.year, settlement_date.month + 1
        release_date = date(next_year, next_month, 15)
        half = "second"

    source_ref = (
        f"{RELEASE_RULE_ID}: settlement_date is in the {half} half of the month -> "
        f"release_date inferred per the half-month rule ({_RELEASE_RULE_URL})"
    )
    return release_date, source_ref


def compute_total_failed_usd(failed_shares: float, closing_price: float | None) -> float | None:
    """failed_shares * closing_price, same settlement date only. None if price is unknown."""
    if closing_price is None:
        return None
    return failed_shares * closing_price


def resolve_display_symbol(cusip: str, raw_symbol: str | None) -> tuple[str, str]:
    """(display_symbol, ticker_source). Falls back to CUSIP, explicitly labelled."""
    if raw_symbol and raw_symbol.strip():
        return raw_symbol.strip().upper(), "ftd_file_symbol"
    return cusip, "cusip_fallback"


def compute_age_days(settlement_date: date, as_of: date) -> int:
    """See AGE_NOTE: age of the OBSERVATION, not of the underlying fails."""
    return (as_of - settlement_date).days


@dataclass(frozen=True)
class MaterializationResult:
    status: str
    generation_id: str
    rows_written: int = 0
    #: Rows this run ATTEMPTED to insert but were silently dropped by
    #: ``ON CONFLICT (settlement_date, ticker) DO NOTHING`` -- e.g. two
    #: CUSIPs reporting the same display symbol on the same settlement
    #: date (the tracked table's unique key is (settlement_date, ticker),
    #: not cusip). ``rows_written`` counts only rows that actually landed;
    #: this field makes the discarded ones visible instead of silent.
    #: 2026-09-18, real-Postgres run (composition d7ffa7f1).
    rows_skipped_conflict: int = 0
    cusips_with_data: int = 0
    cusips_discovered: int = 0
    message: str = ""


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------


def _discover_cusips(conn: Connection, as_of: date) -> list[str]:
    rows = conn.execute(
        text(
            "SELECT DISTINCT series_id FROM raw_series "
            "WHERE series_id LIKE :prefix AND obs_date <= :as_of AND pull_status = 'SUCCESS'"
        ),
        {"prefix": f"{SERIES_PREFIX}:%", "as_of": as_of},
    ).fetchall()
    cusips: set[str] = set()
    for (sid,) in rows:
        parts = sid.split(":")
        if len(parts) >= 3:
            cusips.add(parts[2])
    return sorted(cusips)


def _read_cusip_history(conn: Connection, cusip: str, as_of: date) -> dict[date, dict[str, Any]]:
    """PIT-style (LATEST_AS_OF) read of one CUSIP's FTD balance series."""
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (obs_date)
                obs_date, value, raw_payload, pull_timestamp
            FROM raw_series
            WHERE series_id = :sid AND obs_date <= :as_of AND pull_status = 'SUCCESS'
            ORDER BY obs_date, pull_timestamp DESC
            """
        ),
        {"sid": f"{SERIES_PREFIX}:{cusip}", "as_of": as_of},
    ).mappings().all()
    out: dict[date, dict[str, Any]] = {}
    for row in rows:
        payload = row["raw_payload"] or {}
        out[row["obs_date"]] = {
            "failed_shares": measured_or_none(row["value"]),
            "symbol": payload.get("symbol"),
            "price": measured_or_none(payload.get("price")),
            "pull_timestamp": row["pull_timestamp"],
        }
    return out


def _existing_settlement_dates(conn: Connection, ticker: str, cusip: str) -> set[date]:
    rows = conn.execute(
        text("SELECT settlement_date FROM sec_regsho_ftd_cns WHERE cusip = :c"),
        {"c": cusip},
    ).fetchall()
    return {r[0] for r in rows}


def _distinct_pull_count(conn: Connection, cusip: str, obs_date: date) -> int:
    n = conn.execute(
        text(
            "SELECT COUNT(DISTINCT pull_timestamp) FROM raw_series "
            "WHERE series_id = :sid AND obs_date = :od AND pull_status = 'SUCCESS'"
        ),
        {"sid": f"{SERIES_PREFIX}:{cusip}", "od": obs_date},
    ).scalar()
    return int(n or 0)


def materialize_sec_ftd_pillar(engine: Engine, *, as_of: date | None = None) -> MaterializationResult:
    """Materialize new SEC FTD balance rows as one atomic generation."""
    as_of = as_of or date.today()
    generation_id = new_generation_id()

    try:
        with engine.begin() as conn:
            cusips = _discover_cusips(conn, as_of)
            if not cusips:
                raise _EmptyUpstream()

            rows_to_insert: list[dict[str, Any]] = []
            cusips_with_data: set[str] = set()

            for cusip in cusips:
                history = _read_cusip_history(conn, cusip, as_of)
                if not history:
                    continue
                existing = _existing_settlement_dates(conn, "", cusip)

                for settlement_date, entry in sorted(history.items()):
                    if settlement_date in existing:
                        continue
                    failed_shares = entry["failed_shares"]
                    if failed_shares is None:
                        continue  # no fallback -- an unparseable balance stays unmaterialized

                    symbol, ticker_source = resolve_display_symbol(cusip, entry["symbol"])
                    closing_price = entry["price"]
                    total_failed_usd = compute_total_failed_usd(failed_shares, closing_price)

                    release_date, source_ref = compute_release_date(settlement_date)
                    available_at = entry["pull_timestamp"]
                    distinct_pulls = _distinct_pull_count(conn, cusip, settlement_date)
                    basis, basis_note = classify_availability_basis(
                        release_date, available_at,
                        distinct_pull_count=distinct_pulls,
                        tolerance_days=AVAILABILITY_TOLERANCE_DAYS,
                    )
                    full_source_ref = f"{source_ref}; {NOT_A_TIMELINE_NOTE}; ticker_source={ticker_source}"
                    if basis_note:
                        full_source_ref = f"{full_source_ref}; {basis_note}"

                    rows_to_insert.append(
                        {
                            "settlement_date": settlement_date,
                            "ticker": symbol,
                            "cusip": cusip,
                            "failed_shares": failed_shares,
                            "closing_price": closing_price,
                            "total_failed_usd": total_failed_usd,
                            "mandatory_buyin_date": None,
                            "days_remaining": None,
                            "squeeze_risk_score": None,
                            "release_date": release_date,
                            "available_at": available_at,
                            "provenance": "measured",
                            "availability_basis": basis,
                            "generation_id": generation_id,
                            "coverage_fraction": None,
                            "source_ref": full_source_ref,
                        }
                    )
                    cusips_with_data.add(cusip)

            rows_written = 0
            rows_skipped_conflict = 0
            for row in rows_to_insert:
                insert_result = conn.execute(
                    text(
                        """
                        INSERT INTO sec_regsho_ftd_cns (
                            settlement_date, ticker, cusip, failed_shares, closing_price,
                            total_failed_usd, mandatory_buyin_date, days_remaining,
                            squeeze_risk_score, release_date, available_at, provenance,
                            availability_basis, generation_id, coverage_fraction, source_ref
                        ) VALUES (
                            :settlement_date, :ticker, :cusip, :failed_shares, :closing_price,
                            :total_failed_usd, :mandatory_buyin_date, :days_remaining,
                            :squeeze_risk_score, :release_date, :available_at, :provenance,
                            :availability_basis, :generation_id, :coverage_fraction, :source_ref
                        )
                        ON CONFLICT (settlement_date, ticker) DO NOTHING
                        """
                    ),
                    row,
                )
                # ON CONFLICT DO NOTHING reports rowcount == 0 for a row it
                # silently dropped -- never assume "attempted" == "landed".
                if insert_result.rowcount and insert_result.rowcount > 0:
                    rows_written += 1
                else:
                    rows_skipped_conflict += 1

            coverage_fraction = len(cusips_with_data) / len(cusips) if cusips else None
            record_generation(
                conn, pillar=PILLAR_NAME, generation_id=generation_id,
                status=STATUS_COMPLETE, row_count=rows_written,
                coverage_fraction=coverage_fraction,
            )

        status = "SUCCESS" if rows_written else "SUCCESS_NOOP"
        message = f"{rows_written} new row(s) across {len(cusips_with_data)} CUSIP(s)"
        if rows_skipped_conflict:
            message += (
                f"; {rows_skipped_conflict} attempted row(s) skipped -- "
                "(settlement_date, ticker) already claimed by a different CUSIP"
            )
        return MaterializationResult(
            status=status, generation_id=generation_id, rows_written=rows_written,
            rows_skipped_conflict=rows_skipped_conflict,
            cusips_with_data=len(cusips_with_data), cusips_discovered=len(cusips),
            message=message,
        )
    except _EmptyUpstream:
        _record_failure(engine, generation_id, "empty_upstream")
        return MaterializationResult(status="EMPTY", generation_id=generation_id, message="no sec:ftd_balance:* rows in raw_series")
    except Exception as exc:  # noqa: BLE001
        _record_failure(engine, generation_id, str(exc))
        return MaterializationResult(status="FAILED", generation_id=generation_id, message=str(exc))


class _EmptyUpstream(Exception):
    pass


def _record_failure(engine: Engine, generation_id: str, reason: str) -> None:
    try:
        with engine.begin() as conn:
            record_generation(conn, pillar=PILLAR_NAME, generation_id=generation_id, status=STATUS_FAILED, failure_reason=reason)
    except Exception:  # noqa: BLE001
        pass


@dataclass(frozen=True)
class PillarReadResult:
    state: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    cusips_with_data: int = 0
    generation_id: str | None = None
    generation_published_at: Any = None


def read_sec_ftd_pillar(
    conn: Connection, as_of: date, *, include_inferred: bool = False
) -> PillarReadResult:
    """Latest qualifying row per CUSIP as of ``as_of``. See module docstring."""
    generation = _latest_complete_generation(conn, PILLAR_NAME)
    attempt = _latest_attempt(conn, PILLAR_NAME)

    if generation is None:
        if attempt is not None and attempt["status"] == STATUS_FAILED:
            return PillarReadResult(state="materializer_failed")
        return PillarReadResult(state="never_configured")

    basis_filter = "" if include_inferred else "AND availability_basis = 'observed_acquisition'"
    rows = conn.execute(
        text(
            f"""
            SELECT DISTINCT ON (cusip)
                cusip, ticker, settlement_date, failed_shares, closing_price, total_failed_usd,
                release_date, available_at, provenance, availability_basis, generation_id,
                coverage_fraction, source_ref
            FROM sec_regsho_ftd_cns
            WHERE release_date IS NOT NULL AND release_date <= :as_of
              {basis_filter}
            ORDER BY cusip, settlement_date DESC
            """
        ),
        {"as_of": as_of},
    ).mappings().all()

    rows_out = [dict(r) for r in rows]
    return PillarReadResult(
        state="ok", rows=rows_out, cusips_with_data=len(rows_out),
        generation_id=generation["generation_id"], generation_published_at=generation["published_at"],
    )
