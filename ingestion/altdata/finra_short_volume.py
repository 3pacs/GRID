"""
GRID FINRA Daily Short Sale Volume puller (Regulation SHO daily files).

CONTRACT-FIRST / NOT ACTIVATED: this puller is not imported by
``ingestion/scheduler.py``. A proposed (unapplied) registration patch is at
``docs/handoffs/2026-09-18/fable-w5b-scheduler-registration.patch``. See
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
colon-delimited series-id namespace (``finra:short_volume:<symbol>:<market>``)
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
``finra:short_volume:<symbol>:<market>`` -- one series per (symbol, market)
pair, one row per trade date. ``value`` = ShortVolume (shares). The other
documented columns (ShortExemptVolume, TotalVolume) are carried in
``raw_payload`` rather than invented as separate series, since the task
only specifies this one series-id shape.

Endpoint NOT verified live
---------------------------
Per this workstream's constraints, no call was made to any FINRA API or
download endpoint while building this module (WebFetch was used only
against the public documentation pages above). ``_DAILY_FILE_URL_TEMPLATE``
below is a placeholder built from the commonly-documented FINRA daily-file
naming convention; it has **not** been confirmed against a live response.
``_fetch_raw_text`` therefore requires an explicit URL (or a monkeypatched
``requests.get`` in tests) rather than silently trusting that placeholder --
confirm the real endpoint before this puller is registered in the
scheduler.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, log_pull_failure, retry_on_failure

# ---- Series-id namespace (disjoint from finra_ats.py's "finra.*") ----
_SERIES_PREFIX = "finra:short_volume"

# ---- Documentation source (informational only; not fetched at runtime) ----
_DOCS_URL = "https://www.finra.org/finra-data/browse-catalog/short-sale-volume"
_LAYOUT_PDF_URL = (
    "https://www.finra.org/sites/default/files/2021-07/"
    "DailyShortSaleVolumeFileLayout.pdf"
)

# Commonly-documented FINRA daily short-volume file naming convention.
# NOT independently verified against a live FINRA response for this pass
# (no API calls were made). Confirm before activating this puller.
_DAILY_FILE_URL_TEMPLATE = (
    "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{yyyymmdd}.txt"
)

_REQUEST_TIMEOUT: int = 30

# Reporting-facility ("Market") codes documented in the FINRA layout PDF.
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

    NOT registered in the scheduler (see module docstring). Distinct
    ``SOURCE_NAME`` from ``finra_ats.py``'s ``FINRA_ATS`` -- this is a
    different FINRA data product (daily short-sale *volume*, not ATS
    volume and not short *interest*).
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
            "FINRAShortVolumePuller initialised (NOT scheduled) -- source_id={sid}",
            sid=self.source_id,
        )

    @staticmethod
    def series_id(symbol: str, market: str) -> str:
        """Build the series_id for a (symbol, market) pair.

        Parameters:
            symbol: Security symbol as reported in the file.
            market: Reporting-facility code (N/Q/B/D/O).

        Returns:
            ``finra:short_volume:<SYMBOL>:<MARKET>``.
        """
        return f"{_SERIES_PREFIX}:{symbol.strip().upper()}:{market.strip().upper()}"

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_raw_text(self, trade_date: date, url: str | None = None) -> str:
        """Fetch the raw pipe-delimited daily file for one trade date.

        Never called by the test suite (tests operate on
        :func:`parse_daily_short_volume_file` directly, or monkeypatch this
        method / ``requests.get``). Requires an explicit ``url`` because
        ``_DAILY_FILE_URL_TEMPLATE`` has not been verified against a live
        FINRA response -- see module docstring.

        Parameters:
            trade_date: Trade date to fetch.
            url: Explicit download URL. If omitted, raises rather than
                silently trusting the unverified placeholder template.

        Returns:
            Decoded file text.

        Raises:
            NotImplementedError: if no ``url`` is supplied.
            requests.RequestException: on HTTP failure after retries.
        """
        if url is None:
            raise NotImplementedError(
                "FINRA daily short-volume download URL was not verified live "
                "for this contract-first pass. Pass url= explicitly (or "
                "confirm and hardcode the endpoint) before activating this "
                "puller."
            )
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def pull(
        self,
        trade_date: date | str,
        *,
        url: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Pull and store one trade date's daily short-sale volume file.

        Parameters:
            trade_date: Trade date (date or "YYYY-MM-DD" string).
            url: Explicit download URL for :meth:`_fetch_raw_text`.
            dry_run: If True, fetch and parse but write nothing to the
                database. Returns the same shape of result with
                ``dry_run=True`` and a ``rows_would_insert`` count instead
                of ``rows_inserted``.

        Returns:
            dict with status ("SUCCESS" or "FAILED"), rows_inserted (or
            rows_would_insert if dry_run), rows_skipped, and dry_run.
        """
        if isinstance(trade_date, str):
            trade_date = date.fromisoformat(trade_date)

        try:
            raw_text = self._fetch_raw_text(trade_date, url=url)
        except Exception as exc:  # noqa: BLE001 -- bounded below
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

        if dry_run:
            would_insert = 0
            existing_cache: dict[str, set[date]] = {}
            with self.engine.connect() as conn:
                for row in rows:
                    sid = self.series_id(row["symbol"], row["market"])
                    if sid not in existing_cache:
                        existing_cache[sid] = self._get_existing_dates(sid, conn)
                    if row["date"] not in existing_cache[sid]:
                        would_insert += 1
            return {
                "status": "SUCCESS",
                "rows_inserted": 0,
                "rows_would_insert": would_insert,
                "rows_skipped": skipped,
                "dry_run": True,
            }

        inserted = 0
        existing_cache = {}
        with self.engine.begin() as conn:
            for row in rows:
                sid = self.series_id(row["symbol"], row["market"])
                if sid not in existing_cache:
                    existing_cache[sid] = self._get_existing_dates(sid, conn)
                if row["date"] in existing_cache[sid]:
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
                existing_cache[sid].add(row["date"])
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
