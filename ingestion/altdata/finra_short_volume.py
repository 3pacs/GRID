"""
GRID FINRA Daily Short Sale Volume puller (Regulation SHO daily files).

ACTIVATED (Wave 1, 2026-09-27): registered as ``finra_short_volume`` in
``ingestion/smart_scheduler.py::PULLER_REGISTRY``, running ``pull_recent``
on a 24h cadence -- NOT via the unapplied
``docs/handoffs/2026-09-18/fable-w5b-scheduler-registration.patch`` (which
targets ``ingestion/scheduler.py``, a module nothing schedules; see the
"Wave 1 activation helpers" comment above ``PULLER_REGISTRY``). See
``docs/handoffs/2026-09-18/fable-w5b-source-contracts.md`` for the full
identifier/units/cadence table and the exact documentation quotes this
module was built from.

Documentation relied on (quoted verbatim in the contracts doc):
- https://www.finra.org/finra-data/browse-catalog/short-sale-volume
- https://www.finra.org/sites/default/files/2021-07/DailyShortSaleVolumeFileLayout.pdf
  ("Regulation SHO Daily Short Sale Volume File Layout")

Semantics this module is careful to keep straight
--------------------------------------------------
This is DAILY SHORT SALE **VOLUME** (Reg SHO daily files) — the aggregate
share volume of short-sale trades *executed on a given trade date*. Per the
FINRA catalog page:

    "Short Sale Files do not -- and are not intended to -- equate to
    bi-monthly reported short interest position information. The short
    interest data reflects short positions held by market participants at
    a specific moment in time on two discrete days each month, while the
    Daily File reflects the aggregate volume of short trades effected on
    each trade date..."

So this is NOT:
- short INTEREST (a bi-monthly point-in-time position; see
  ``finra_ats.py::pull_short_interest`` -> series ``finra.short_interest_total``)
- ATS / dark-pool VOLUME (a different FINRA data set entirely; see
  ``finra_ats.py::pull_ats_volume`` -> series ``finra.ats_total_volume`` /
  ``finra.ats_dark_pct``)

To keep that straight at the storage layer, this module uses its own
colon-delimited series-id namespace (``finra:short_volume:<symbol>``)
that is disjoint from the existing dot-delimited ``finra.*`` namespace used
by ``finra_ats.py``. Tests assert the two namespaces never collide.

File format (per the FINRA layout PDF above; current post-2011-02-28 layout)
------------------------------------------------------------------------------
Pipe-delimited text, one file per trade date::

    Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market

- Date: "8 numeric characters" / "Trade Date YYYYMMDD"
- Symbol: "Up to 14 characters" / "Security symbol"
- ShortVolume: "Numeric value, no decimals" / aggregate short + short-exempt
  share volume executed during regular trading hours
- ShortExemptVolume: aggregate short-exempt share volume (subset of the
  above)
- TotalVolume: aggregate share volume of ALL executed trades that day
- Market: "1 alpha character" reporting-facility identifier -- N (NYSE
  TRF), Q (NASDAQ TRF Carteret), B (NASDAQ TRF Chicago), D (ADF), O (ORF)

Per the layout doc: "The first row of every file shall contain a header
with the column names described above. The last row of every file shall be
a trailer denoting the number of records produced for the file." and "In
the event there is no short data for a particular day, a file will still
be produced for that day that will only contain a header and a trailer
with a count of '0'."

Series-id scheme
-----------------
``finra:short_volume:<symbol>`` -- one series per SYMBOL (not per
symbol+market), one row per trade date. ``value`` = ShortVolume (shares).
``market``, ``short_exempt_volume`` and ``total_volume`` are carried in
``raw_payload`` rather than invented as separate series.

Design decision needing owner sign-off (2026-09-27 review, series
fragmentation): the CNMS consolidated file already usually reports one row
per (date, symbol) with a comma-joined multi-facility ``Market`` value
(see the live-response anomaly note below) rather than one row per
(date, symbol, market) -- so keying the series on symbol ALONE, with
``market`` demoted to payload metadata, matches what the live file
actually looks like and avoids fragmenting one economically meaningful
number (a symbol's total short volume for the day) across facility-keyed
series. The tradeoff: on the rare row where the same (date, symbol) DOES
appear twice with two different single-facility ``Market`` values (e.g. a
non-CNMS market-prefix file, or a CNMS anomaly), only the first row
written wins -- the second is treated as a duplicate of an
already-ingested (series_id, obs_date) and silently skipped, same as any
other idempotent re-pull. This module was previously keyed on
(symbol, market) instead (fragmenting every multi-facility row into
separate never-again-reconciled series); flagged here for explicit owner
sign-off rather than assumed correct.

Endpoint verified live (W5c, 2026-09-18)
-----------------------------------------
Confirmed by WebFetch against
https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files
and by one real HTTP GET (see
``tests/fixtures/sources/finra_short_volume/REAL_CAPTURE_NOTE.txt`` for
the exact URL, timestamp, and response headers):

    https://cdn.finra.org/equity/regsho/daily/<MarketPrefix>shvol<YYYYMMDD>.txt

Market-prefix codes documented on that page (``MARKET_PREFIXES`` below):
``CNMS`` (Consolidated NMS -- the default), ``FNQC`` (FINRA/NASDAQ TRF
Chicago), ``FNRA`` (ADF), ``FNSQ`` (FINRA/NASDAQ TRF Carteret), ``FNYX``
(FINRA/NYSE TRF), ``FORF`` (ORF). ``_fetch_raw_text`` builds this URL from
``settings.FINRA_SHORT_VOLUME_BASE_URL`` (config.py; not a secret -- no
key is required) unless an explicit ``url`` is passed.

**Live-response anomaly (recorded, not silently normalized):** the real
CNMS file captured for the fixture does NOT match the layout PDF exactly
-- ``ShortVolume``/``TotalVolume`` are fractional rather than whole
shares, and ~65% of rows carry a comma-joined multi-facility ``Market``
value (e.g. ``"B,Q,N"``) rather than the documented single alpha
character. The parser and ``series_id()`` tolerate both (values pass
through ``float()``; ``market`` is stored as whatever string is present).
See the fixture NOTE for the open question of whether that is expected
CNMS behavior or a sandboxed-network artifact -- unresolved. Registered
anyway (Wave 1, see above) because the parser already tolerates both
shapes; not a blocker for activation.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from config import settings
from ingestion.base import (
    BasePuller,
    _http_status_from_exc,
    log_pull_failure,
    retry_on_failure,
)

# ---- Series-id namespace (disjoint from finra_ats.py's "finra.*") ----
_SERIES_PREFIX = "finra:short_volume"

# ---- Documentation source (informational only; not fetched at runtime) ----
_DOCS_URL = "https://www.finra.org/finra-data/browse-catalog/short-sale-volume"
_DAILY_FILES_PAGE_URL = (
    "https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/"
    "daily-short-sale-volume-files"
)
_LAYOUT_PDF_URL = (
    "https://www.finra.org/sites/default/files/2021-07/"
    "DailyShortSaleVolumeFileLayout.pdf"
)

# Verified live (see module docstring): "<MarketPrefix>shvol<YYYYMMDD>.txt"
# under settings.FINRA_SHORT_VOLUME_BASE_URL (config.py; public CDN, no key).
_DAILY_FILE_URL_TEMPLATE = "{base_url}{market_prefix}shvol{yyyymmdd}.txt"

# Market-prefix codes documented on _DAILY_FILES_PAGE_URL. "CNMS"
# (Consolidated NMS) is the default -- one file covering all facilities.
MARKET_PREFIXES: dict[str, str] = {
    "CNMS": "Consolidated NMS",
    "FNQC": "FINRA/NASDAQ TRF Chicago",
    "FNRA": "ADF",
    "FNSQ": "FINRA/NASDAQ TRF Carteret",
    "FNYX": "FINRA/NYSE TRF",
    "FORF": "ORF",
}
_DEFAULT_MARKET_PREFIX = "CNMS"

# Request identification header. FINRA's CDN does not require a specific
# User-Agent (unlike SEC's fails-to-deliver endpoint -- see sec_ftd.py /
# settings.SEC_USER_AGENT), but this puller still identifies itself.
_REQUEST_HEADERS = {"User-Agent": "GRID/1.0 (aniksrobot@gmail.com)"}

_REQUEST_TIMEOUT: int = 30

# Reporting-facility ("Market") codes documented in the FINRA layout PDF.
# NOTE: the live CNMS file captured for this puller's fixture frequently
# reports a comma-joined LIST of these codes in one row (e.g. "B,Q,N")
# rather than a single code -- see REAL_CAPTURE_NOTE.txt in the fixtures
# directory. This dict is retained as the documented reference table; it
# is not used to validate/reject the parsed Market field.
MARKET_CODES: dict[str, str] = {
    "N": "NYSE TRF",
    "Q": "NASDAQ TRF Carteret",
    "B": "NASDAQ TRF Chicago",
    "D": "ADF",
    "O": "ORF",
}

_EXPECTED_HEADER_PREFIX = ("DATE", "SYMBOL")
_EXPECTED_FIELD_COUNT = 6


def _bounded_error(exc: BaseException, *, max_len: int = 300) -> str:
    """Render an exception as a short, credential-safe string.

    Truncates long messages (e.g. full HTML error bodies) and never
    includes request headers, so nothing resembling a token/secret can
    leak into logs or into ``raw_series.pull_status`` metadata. This
    endpoint takes no API key, but we apply the same discipline as the
    other pullers in this package (see ``eia_puller.py``) for consistency.
    """
    msg = str(exc)
    if len(msg) > max_len:
        msg = msg[:max_len] + "...(truncated)"
    return msg


def parse_daily_short_volume_file(raw_text: str) -> dict[str, Any]:
    """Parse a FINRA Reg SHO Daily Short Sale Volume file.

    Pure function -- no network, no database. See module docstring for the
    documented format this follows.

    Parameters:
        raw_text: Full decoded text of one daily file (header line,
            zero or more data lines, trailer line).

    Returns:
        dict with:
          - "rows": list of dicts {date, symbol, market, short_volume,
            short_exempt_volume, total_volume}
          - "skipped": count of data lines that did not parse (wrong
            field count, non-numeric value, unparseable date)

    Raises:
        ValueError: if the input has no lines, or the first line is not
            recognizable as the documented header (i.e. the file is not
            this format at all -- a catastrophic parse failure, distinct
            from an individual malformed row).
    """
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("FINRA short volume file: empty input (no header)")

    header_fields = [f.strip().upper() for f in lines[0].split("|")]
    if tuple(header_fields[:2]) != _EXPECTED_HEADER_PREFIX:
        raise ValueError(f"FINRA short volume file: unrecognized header {lines[0]!r}")

    # Header + trailer only (or header alone) => no data rows.
    body_lines = lines[1:-1] if len(lines) > 1 else []
    trailer_line = lines[-1] if len(lines) > 1 else None

    rows: list[dict[str, Any]] = []
    skipped = 0

    for ln in body_lines:
        fields = ln.split("|")
        if len(fields) != _EXPECTED_FIELD_COUNT:
            log.warning(
                "FINRA short volume: malformed row (expected {exp} fields, got {got}): {l}",
                exp=_EXPECTED_FIELD_COUNT,
                got=len(fields),
                l=ln[:120],
            )
            skipped += 1
            continue

        raw_date, symbol, short_vol, short_exempt_vol, total_vol, market = (
            f.strip() for f in fields
        )

        try:
            obs_date = date(int(raw_date[0:4]), int(raw_date[4:6]), int(raw_date[6:8]))
            short_vol_f = float(short_vol)
            short_exempt_f = float(short_exempt_vol)
            total_vol_f = float(total_vol)
        except (ValueError, TypeError, IndexError):
            log.warning(
                "FINRA short volume: unparseable date/numeric field: {l}", l=ln[:120]
            )
            skipped += 1
            continue

        symbol = symbol.upper()
        market = market.upper()
        if not symbol or not market:
            log.warning("FINRA short volume: missing symbol/market: {l}", l=ln[:120])
            skipped += 1
            continue

        rows.append(
            {
                "date": obs_date,
                "symbol": symbol,
                "market": market,
                "short_volume": short_vol_f,
                "short_exempt_volume": short_exempt_f,
                "total_volume": total_vol_f,
            }
        )

    if trailer_line is not None:
        try:
            trailer_count = int(trailer_line)
        except ValueError:
            log.warning(
                "FINRA short volume: trailer row is not a plain record count: {t!r}",
                t=trailer_line,
            )
        else:
            if trailer_count != len(body_lines):
                log.warning(
                    "FINRA short volume: trailer count {t} != body line count {n} "
                    "(parsed={p}, skipped={s})",
                    t=trailer_count,
                    n=len(body_lines),
                    p=len(rows),
                    s=skipped,
                )

    return {"rows": rows, "skipped": skipped}


class FINRAShortVolumePuller(BasePuller):
    """Pulls FINRA Reg SHO Daily Short Sale Volume files.

    Registered in the scheduler as ``finra_short_volume`` (see module
    docstring). Distinct ``SOURCE_NAME`` from ``finra_ats.py``'s
    ``FINRA_ATS`` -- this is a different FINRA data product (daily
    short-sale *volume*, not ATS volume and not short *interest*).
    """

    SOURCE_NAME: str = "FINRA_SHORT_VOLUME"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _DOCS_URL,
        "cost_tier": "FREE",
        # DB CHECK constraint (schema.sql) only allows REALTIME/EOD/WEEKLY/
        # MONTHLY. The file is published once per trade date (a "Daily
        # File" per FINRA's own language), which maps to EOD.
        "latency_class": "EOD",
        "pit_available": True,
        # FINRA's documentation does not describe revisions to a
        # previously-published daily file; treated as immutable per file.
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 40,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "FINRAShortVolumePuller initialised -- source_id={sid}",
            sid=self.source_id,
        )

    @staticmethod
    def series_id(symbol: str) -> str:
        """Build the series_id for a symbol.

        Keyed on symbol ALONE, not (symbol, market) -- see the module
        docstring's "Design decision needing owner sign-off" note.
        ``market`` is still recorded, but only in ``raw_payload``.

        Parameters:
            symbol: Security symbol as reported in the file.

        Returns:
            ``finra:short_volume:<SYMBOL>``.
        """
        return f"{_SERIES_PREFIX}:{symbol.strip().upper()}"

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_raw_text(
        self,
        trade_date: date,
        url: str | None = None,
        *,
        market_prefix: str = _DEFAULT_MARKET_PREFIX,
    ) -> str:
        """Fetch the raw pipe-delimited daily file for one trade date.

        Builds the URL from ``settings.FINRA_SHORT_VOLUME_BASE_URL`` and
        the documented ``<MarketPrefix>shvol<YYYYMMDD>.txt`` naming
        convention (verified live -- see module docstring) unless an
        explicit ``url`` is supplied. Tests monkeypatch this method
        directly rather than hitting the network.

        Parameters:
            trade_date: Trade date to fetch.
            url: Explicit download URL, overriding the built one.
            market_prefix: One of ``MARKET_PREFIXES`` (default ``"CNMS"``,
                the consolidated file). Ignored if ``url`` is given.

        Returns:
            Decoded file text.

        Raises:
            requests.RequestException: on HTTP failure after retries.
        """
        if url is None:
            url = _DAILY_FILE_URL_TEMPLATE.format(
                base_url=settings.FINRA_SHORT_VOLUME_BASE_URL,
                market_prefix=market_prefix,
                yyyymmdd=trade_date.strftime("%Y%m%d"),
            )
        resp = requests.get(url, headers=_REQUEST_HEADERS, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def pull(
        self,
        trade_date: date | str,
        *,
        url: str | None = None,
        market_prefix: str = _DEFAULT_MARKET_PREFIX,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Pull and store one trade date's daily short-sale volume file.

        Parameters:
            trade_date: Trade date (date or "YYYY-MM-DD" string).
            url: Explicit download URL for :meth:`_fetch_raw_text`.
            market_prefix: Market-prefix code (default ``"CNMS"``,
                consolidated). Ignored if ``url`` is given.
            dry_run: If True, fetch and parse but write nothing to the
                database. Returns the same shape of result with
                ``dry_run=True`` and a ``rows_would_insert`` count instead
                of ``rows_inserted``.

        Returns:
            dict with status ("SUCCESS", "SKIPPED" or "FAILED"),
            rows_inserted (or rows_would_insert if dry_run), rows_skipped,
            and dry_run. "SKIPPED" means the file for this trade_date does
            not exist yet at FINRA (HTTP 403/404 -- a market holiday with
            no file at all, or a same-day request made before FINRA has
            published it) -- this is NOT a failure of this puller and must
            never advance a cooldown as if it were one.
        """
        if isinstance(trade_date, str):
            trade_date = date.fromisoformat(trade_date)

        try:
            raw_text = self._fetch_raw_text(
                trade_date, url=url, market_prefix=market_prefix
            )
        except Exception as exc:  # noqa: BLE001 -- bounded below
            status = _http_status_from_exc(exc)
            if status in (403, 404):
                # Per FINRA's own layout doc, a file WITH NO TRADING (e.g.
                # a market holiday) still gets published -- header +
                # trailer("0") only, never a 403/404 (see module docstring's
                # "Per the layout doc" quote). A 403/404 therefore means
                # something else: this trade_date has no file at all yet
                # (requested too early the same evening) or FINRA has
                # removed/renamed it -- neither is a GRID-side failure, so
                # this is SKIPPED, not FAILED, and must not feed the
                # exponential-backoff cooldown as a real failure would.
                log.info(
                    "{s}: no file yet for {d} (HTTP {c}) -- treated as "
                    "not-yet-published, not a failure",
                    s=self.SOURCE_NAME, d=trade_date, c=status,
                )
                return {
                    "status": "SKIPPED",
                    "rows_inserted": 0,
                    "rows_skipped": 0,
                    "skipped_reason": (
                        f"HTTP {status} -- no file published yet for "
                        f"{trade_date} (requested too early, or a date "
                        f"FINRA never files for)"
                    ),
                    "dry_run": dry_run,
                }
            log_pull_failure(self.SOURCE_NAME, str(trade_date), exc)
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "rows_skipped": 0,
                "error": _bounded_error(exc),
                "dry_run": dry_run,
            }

        try:
            parsed = parse_daily_short_volume_file(raw_text)
        except ValueError as exc:
            log.error(
                "{s}: unparseable file for {d}: {e}",
                s=self.SOURCE_NAME,
                d=trade_date,
                e=str(exc),
            )
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "rows_skipped": 0,
                "error": _bounded_error(exc),
                "dry_run": dry_run,
            }

        rows = parsed["rows"]
        skipped = parsed["skipped"]

        if not rows:
            # Documented empty-file case: header + trailer("0") only.
            # No value=0 placeholder rows are ever written.
            log.info("{s}: no data rows for {d}", s=self.SOURCE_NAME, d=trade_date)
            return {
                "status": "SUCCESS",
                "rows_inserted": 0,
                "rows_skipped": skipped,
                "dry_run": dry_run,
            }

        # One cheap query for the whole file instead of one per unique
        # series_id (a CNMS file can carry thousands of symbols) -- see
        # ingestion/base.py::_get_existing_pairs_in_range. A single-date
        # file has exactly one obs_date, so this is trivially bounded.
        file_min_date = min(row["date"] for row in rows)
        file_max_date = max(row["date"] for row in rows)

        if dry_run:
            with self.engine.connect() as conn:
                existing_pairs = self._get_existing_pairs_in_range(
                    conn, file_min_date, file_max_date
                )
            would_insert = 0
            for row in rows:
                sid = self.series_id(row["symbol"])
                existing_dates = existing_pairs.setdefault(sid, set())
                if row["date"] in existing_dates:
                    # Also catches TWO rows in the same file collapsing
                    # onto the same (symbol, date) -- e.g. a multi-market
                    # CNMS row -- which must count once, not once per raw
                    # file row (see the module docstring's series-keying
                    # design-decision note).
                    continue
                existing_dates.add(row["date"])
                would_insert += 1
            return {
                "status": "SUCCESS",
                "rows_inserted": 0,
                "rows_would_insert": would_insert,
                "rows_skipped": skipped,
                "dry_run": True,
            }

        inserted = 0
        with self.engine.begin() as conn:
            # Transaction-scoped lock for this exact trade_date -- closes
            # the race where an abandoned SmartScheduler-timeout thread for
            # this same date finishes late, concurrently with a fresh
            # retry, and both would otherwise see "not yet ingested" and
            # both insert (see ingestion/base.py::_file_advisory_lock).
            self._file_advisory_lock(conn, "pull", trade_date.isoformat())
            existing_pairs = self._get_existing_pairs_in_range(
                conn, file_min_date, file_max_date
            )
            for row in rows:
                sid = self.series_id(row["symbol"])
                existing_dates = existing_pairs.setdefault(sid, set())
                if row["date"] in existing_dates:
                    continue  # idempotent: already stored for this series/date

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=row["date"],
                    value=row["short_volume"],
                    raw_payload={
                        "short_exempt_volume": row["short_exempt_volume"],
                        "total_volume": row["total_volume"],
                        "symbol": row["symbol"],
                        "market": row["market"],
                        "source": "FINRA Reg SHO Daily Short Sale Volume File",
                        "docs_url": _DOCS_URL,
                    },
                )
                existing_dates.add(row["date"])
                inserted += 1

        log.info(
            "{s}: {n} rows inserted, {sk} rows skipped for {d}",
            s=self.SOURCE_NAME,
            n=inserted,
            sk=skipped,
            d=trade_date,
        )
        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "rows_skipped": skipped,
            "dry_run": False,
        }

    def pull_recent(
        self,
        *,
        anchor_date: date | str | None = None,
        weekdays_back: int = 5,
        market_prefix: str = _DEFAULT_MARKET_PREFIX,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Catch-up pull over the last ``weekdays_back`` weekday trade dates.

        This is the method the scheduler runs on its 24h cadence (see
        ``ingestion/smart_scheduler.py::PULLER_REGISTRY``'s
        ``finra_short_volume`` entry) instead of the single-date
        :meth:`pull` -- a scheduler that only ever pulls "today" silently
        loses any date whose tick was missed (deploy restart, an
        overrun/TIMEOUT tick, a transient network blip) or whose file
        wasn't published yet when that day's tick ran. Walking back over a
        short recent window and skipping dates already stored for this
        source (one cheap query -- see
        ``ingestion/base.py::_get_existing_source_dates``) means a missed
        day self-heals on the next run instead of being gone forever.

        Parameters:
            anchor_date: Most recent date to consider (date or ISO string).
                Defaults to today. The scheduler registry supplies
                ``smart_scheduler._finra_short_volume_trade_date`` here so
                the anchor itself already accounts for the ~18:00 ET
                same-day publish cutoff.
            weekdays_back: How many WEEKDAYS (Sat/Sun never counted) to walk
                back from ``anchor_date``, inclusive of it.
            market_prefix: Forwarded to :meth:`pull` for each date.
            dry_run: Forwarded to :meth:`pull` for each date.

        Returns:
            dict with an aggregate ``status`` ("SUCCESS" unless at least
            one date's :meth:`pull` call itself returned "FAILED" -- a
            per-date "SKIPPED", whether for "already stored" or for a
            not-yet-published file, never counts as a failure), aggregate
            ``rows_inserted``/``rows_skipped``, and a per-date ``dates``
            breakdown.
        """
        if anchor_date is None:
            anchor = date.today()
        elif isinstance(anchor_date, str):
            anchor = date.fromisoformat(anchor_date)
        else:
            anchor = anchor_date

        candidates: list[date] = []
        d = anchor
        while len(candidates) < weekdays_back:
            if d.weekday() < 5:  # Mon-Fri only; Sat=5, Sun=6 never have a file
                candidates.append(d)
            d -= timedelta(days=1)
        candidates.reverse()  # oldest first

        with self.engine.connect() as conn:
            already_stored = self._get_existing_source_dates(
                conn, start_date=candidates[0], end_date=candidates[-1]
            )

        per_date: list[dict[str, Any]] = []
        total_inserted = 0
        total_skipped_rows = 0
        any_hard_failure = False

        for d in candidates:
            if d in already_stored:
                per_date.append(
                    {"date": d.isoformat(), "status": "SKIPPED", "reason": "already stored"}
                )
                continue
            result = self.pull(d, market_prefix=market_prefix, dry_run=dry_run)
            per_date.append({"date": d.isoformat(), **result})
            if result["status"] == "FAILED":
                any_hard_failure = True
            total_inserted += result.get("rows_inserted", 0) or 0
            total_skipped_rows += result.get("rows_skipped", 0) or 0

        return {
            "status": "FAILED" if any_hard_failure else "SUCCESS",
            "rows_inserted": total_inserted,
            "rows_skipped": total_skipped_rows,
            "dates": per_date,
            "dry_run": dry_run,
        }
