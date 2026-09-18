"""
GRID SEC Fails-to-Deliver (FTD) puller.

CONTRACT-FIRST / NOT ACTIVATED: this puller is not imported by
``ingestion/scheduler.py``. A proposed (unapplied) registration patch is at
``docs/handoffs/2026-09-18/fable-w5b-scheduler-registration.patch``. See
``docs/handoffs/2026-09-18/fable-w5b-source-contracts.md`` for the full
identifier/units/cadence table, the exact documentation quotes this module
was built from, and the ``raw_series`` ``release_date`` contract gap.

Documentation relied on (quoted verbatim in the contracts doc):
- https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data

Semantics this module is careful to keep straight
--------------------------------------------------
Per the official SEC page:

    "The values of total fails-to-deliver shares represent the aggregate
    net balance of shares that failed to be delivered as of a particular
    settlement date."

    "Fails to deliver on a given day are a cumulative number of all fails
    outstanding until that day, plus new fails that occur that day, less
    fails that settle that day."

    "The figure is not a daily amount of fails, but a combined figure that
    includes both new fails on the reporting day as well as existing
    fails."

So each row is an OUTSTANDING BALANCE as of a settlement date, NOT a count
of newly-created fails that day. This module:

- stores the SETTLEMENT DATE from the file as ``obs_date`` (never a
  "release"/publication date -- see the gap below)
- never sums balances across days as if they were new daily fails
- never implements any T+35 forced-buy-in timeline logic (out of scope
  for a contract-first puller and not supported by anything in the quotes
  above)

The page also disclaims:

    "Fails-to-deliver can occur for a number of reasons on both long and
    short sales. Therefore, fails-to-deliver are not necessarily the
    result of short selling, and are not evidence of abusive short
    selling or 'naked' short selling."

    "We cannot guarantee the accuracy of the data" ... "the age of fails
    cannot be determined by looking at these numbers."

``raw_series`` has NO ``release_date`` column
-----------------------------------------------
``ingestion/base.py::BasePuller._insert_raw`` writes only
``(series_id, source_id, obs_date, value, raw_payload, pull_status)`` --
there is no column for "when did SEC actually publish this file". Per the
SEC page, publication cadence is:

    "The first half of a given month is available at the end of the
    month. The second half of a given month is available at about the
    15th of the next month."

That publication date is a genuinely different concept from the
settlement date stored in ``obs_date``, and this puller does NOT invent a
substitute column or repurpose an existing one (e.g. stuffing a guessed
release date into ``obs_date``) to work around the gap. It is recorded
here, and in the contracts doc, as an open schema gap. The half-month
window ("a" = first half, "b" = second half) that a given file covers is
the one piece of publication-window context we *do* have -- from the file
name pattern itself -- and it is recorded in ``raw_payload`` purely as
descriptive metadata, not as a stand-in for a verified release_date.

File format (per the SEC page above)
-------------------------------------
"The data items are provided as a 'pipe delimited' text file" inside a
ZIP archive named like ``cnsfails<YYYYMM><a|b>.zip`` (file names observed
on the SEC page, e.g. ``cnsfails202608a.zip`` / ``cnsfails202608b.zip``).
Fields, in order: SETTLEMENT DATE (8 digits), CUSIP (9 characters), SYMBOL
(10 characters), QUANTITY (FAILS) (unlimited), DESCRIPTION (30
characters), PRICE (unlimited).

Series-id scheme
-----------------
``sec:ftd_balance:<cusip>`` -- one series per CUSIP, one row per
settlement date. ``value`` = QUANTITY (FAILS), i.e. the outstanding
balance. This is a wholly separate namespace from any ``finra:*`` series
so a FTD balance can never land under a FINRA short-volume/short-interest
series id or vice versa.

Endpoint NOT verified live
---------------------------
No call was made to any SEC API or file server while building this
module. ``_FTD_ZIP_URL_TEMPLATE`` below reflects the file-naming
convention observed on the documentation page but the absolute URL prefix
has not been confirmed against a live response. ``_fetch_zip_bytes``
requires an explicit URL rather than trusting that placeholder silently.
"""

from __future__ import annotations

import zipfile
from datetime import date
from io import BytesIO
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, log_pull_failure, retry_on_failure

# ---- Series-id namespace (disjoint from any "finra:*" or "finra.*") ----
_SERIES_PREFIX = "sec:ftd_balance"

# ---- Documentation source (informational only; not fetched at runtime) ----
_DOCS_URL = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"

# File-naming convention observed on the SEC page (e.g. "cnsfails202608a.zip",
# "cnsfails202608b.zip"). Absolute URL prefix NOT independently verified
# against a live SEC response for this pass -- confirm before activation.
_FTD_ZIP_URL_TEMPLATE = (
    "https://www.sec.gov/files/data/fails-deliver-data/cnsfails{yyyymm}{half}.zip"
)

_REQUEST_TIMEOUT: int = 30
_EXPECTED_FIELD_COUNT = 6
_HEADER_FIRST_FIELD = "SETTLEMENT DATE"


def _bounded_error(exc: BaseException, *, max_len: int = 300) -> str:
    """Render an exception as a short, credential-safe string (see
    ``finra_short_volume.py`` for the identical rationale)."""
    msg = str(exc)
    if len(msg) > max_len:
        msg = msg[:max_len] + "...(truncated)"
    return msg


def parse_ftd_file(raw_text: str) -> dict[str, Any]:
    """Parse one decoded SEC Fails-to-Deliver pipe-delimited file.

    Pure function -- no network, no database. See module docstring for
    the documented format and the balance-vs-new-fails distinction this
    preserves (it does not compute or return anything but the per-row
    balance as published).

    Parameters:
        raw_text: Decoded text of one half-month FTD file.

    Returns:
        dict with:
          - "rows": list of dicts {date, cusip, symbol, quantity_fails,
            description, price} -- one per settlement-date/security row.
            ``quantity_fails`` is the OUTSTANDING BALANCE, not a new-fails
            count.
          - "skipped": count of data lines that did not parse.

    Raises:
        ValueError: if the input has no non-blank lines at all.
    """
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("SEC FTD file: empty input (no lines)")

    body_lines = lines
    first_fields = [f.strip().upper() for f in lines[0].split("|")]
    if first_fields and first_fields[0] == _HEADER_FIRST_FIELD:
        body_lines = lines[1:]

    rows: list[dict[str, Any]] = []
    skipped = 0

    for ln in body_lines:
        fields = ln.split("|")
        if len(fields) != _EXPECTED_FIELD_COUNT:
            log.warning(
                "SEC FTD: malformed row (expected {exp} fields, got {got}): {l}",
                exp=_EXPECTED_FIELD_COUNT,
                got=len(fields),
                l=ln[:120],
            )
            skipped += 1
            continue

        raw_date, cusip, symbol, qty, description, price = (f.strip() for f in fields)

        try:
            obs_date = date(int(raw_date[0:4]), int(raw_date[4:6]), int(raw_date[6:8]))
            qty_f = float(qty)
        except (ValueError, TypeError, IndexError):
            log.warning(
                "SEC FTD: unparseable settlement date/quantity: {l}", l=ln[:120]
            )
            skipped += 1
            continue

        cusip = cusip.upper()
        if not cusip:
            log.warning("SEC FTD: missing CUSIP: {l}", l=ln[:120])
            skipped += 1
            continue

        try:
            price_f: float | None = float(price) if price else None
        except (ValueError, TypeError):
            price_f = None

        rows.append(
            {
                "date": obs_date,
                "cusip": cusip,
                "symbol": symbol.upper(),
                "quantity_fails": qty_f,
                "description": description,
                "price": price_f,
            }
        )

    return {"rows": rows, "skipped": skipped}


def extract_ftd_text_from_zip(zip_bytes: bytes) -> str:
    """Extract and decode the single data file inside an SEC FTD zip.

    Parameters:
        zip_bytes: Raw bytes of the downloaded ``cnsfails*.zip`` archive.

    Returns:
        Decoded text of the first (only expected) member.

    Raises:
        ValueError: if the archive has no members.
    """
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("SEC FTD zip: archive contains no files")
        with zf.open(names[0]) as fh:
            return fh.read().decode("utf-8", errors="replace")


class SECFTDPuller(BasePuller):
    """Pulls SEC Fails-to-Deliver (FTD) outstanding-balance data.

    NOT registered in the scheduler (see module docstring).
    """

    SOURCE_NAME: str = "SEC_FTD"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _DOCS_URL,
        "cost_tier": "FREE",
        # DB CHECK constraint (schema.sql) only allows REALTIME/EOD/WEEKLY/
        # MONTHLY -- there is no semi-monthly/bimonthly value. SEC actually
        # publishes twice a month ("first half ... at the end of the
        # month. The second half ... at about the 15th of the next
        # month."); MONTHLY is the closest available value and is an
        # approximation. This is documented as a second contract gap in
        # the contracts doc.
        "latency_class": "MONTHLY",
        "pit_available": True,
        # SEC disclaims accuracy ("We cannot guarantee the accuracy of the
        # data") but does not explicitly describe a revision process for
        # already-published half-month files. RARE (not NEVER) reflects
        # that this is an unconfirmed-but-plausible assumption, not a
        # documented fact -- see contracts doc.
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 41,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "SECFTDPuller initialised (NOT scheduled) -- source_id={sid}",
            sid=self.source_id,
        )

    @staticmethod
    def series_id(cusip: str) -> str:
        """Build the series_id for a CUSIP.

        Parameters:
            cusip: 9-character CUSIP as reported in the file.

        Returns:
            ``sec:ftd_balance:<CUSIP>``.
        """
        return f"{_SERIES_PREFIX}:{cusip.strip().upper()}"

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _fetch_zip_bytes(self, url: str | None = None) -> bytes:
        """Download the raw FTD zip archive for one half-month.

        Never called by the test suite. Requires an explicit ``url``
        because ``_FTD_ZIP_URL_TEMPLATE`` has not been verified against a
        live SEC response -- see module docstring.

        Parameters:
            url: Explicit zip download URL.

        Returns:
            Raw zip bytes.

        Raises:
            NotImplementedError: if no ``url`` is supplied.
            requests.RequestException: on HTTP failure after retries.
        """
        if url is None:
            raise NotImplementedError(
                "SEC FTD zip download URL was not verified live for this "
                "contract-first pass. Pass url= explicitly (or confirm and "
                "hardcode the endpoint) before activating this puller."
            )
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.content

    def pull(
        self,
        *,
        url: str | None = None,
        publication_half: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Pull and store one half-month's FTD balances.

        Parameters:
            url: Explicit zip download URL for :meth:`_fetch_zip_bytes`.
            publication_half: Optional descriptive tag ("a" / "b" / a
                free-form label) recorded in ``raw_payload`` for
                traceability. This is NOT a verified release_date -- see
                module docstring's contract-gap discussion.
            dry_run: If True, fetch/parse but write nothing. Returns
                ``rows_would_insert`` instead of ``rows_inserted``.

        Returns:
            dict with status ("SUCCESS" or "FAILED"), rows_inserted (or
            rows_would_insert if dry_run), rows_skipped, and dry_run.
        """
        try:
            zip_bytes = self._fetch_zip_bytes(url=url)
            raw_text = extract_ftd_text_from_zip(zip_bytes)
        except Exception as exc:  # noqa: BLE001 -- bounded below
            log_pull_failure(self.SOURCE_NAME, publication_half or "unknown", exc)
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "rows_skipped": 0,
                "error": _bounded_error(exc),
                "dry_run": dry_run,
            }

        return self._pull_from_text(
            raw_text, publication_half=publication_half, dry_run=dry_run
        )

    def pull_from_text(
        self,
        raw_text: str,
        *,
        publication_half: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Parse and store from already-decoded file text.

        Public entry point for callers (and tests) that already have the
        decoded pipe-delimited text -- e.g. after unzipping a file
        obtained some other way -- without going through
        :meth:`_fetch_zip_bytes`.
        """
        return self._pull_from_text(
            raw_text, publication_half=publication_half, dry_run=dry_run
        )

    def _pull_from_text(
        self,
        raw_text: str,
        *,
        publication_half: str | None,
        dry_run: bool,
    ) -> dict[str, Any]:
        try:
            parsed = parse_ftd_file(raw_text)
        except ValueError as exc:
            log.error("{s}: unparseable file: {e}", s=self.SOURCE_NAME, e=str(exc))
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
            log.info("{s}: no data rows parsed", s=self.SOURCE_NAME)
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
                    sid = self.series_id(row["cusip"])
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
                sid = self.series_id(row["cusip"])
                if sid not in existing_cache:
                    existing_cache[sid] = self._get_existing_dates(sid, conn)
                if row["date"] in existing_cache[sid]:
                    continue  # idempotent: balance already stored for this date

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=row["date"],
                    value=row["quantity_fails"],
                    raw_payload={
                        "symbol": row["symbol"],
                        "description": row["description"],
                        "price": row["price"],
                        "cusip": row["cusip"],
                        "is_outstanding_balance_not_new_fails": True,
                        "publication_half": publication_half,
                        "source": "SEC Fails-to-Deliver Data",
                        "docs_url": _DOCS_URL,
                    },
                )
                existing_cache[sid].add(row["date"])
                inserted += 1

        log.info(
            "{s}: {n} rows inserted, {sk} rows skipped",
            s=self.SOURCE_NAME,
            n=inserted,
            sk=skipped,
        )
        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "rows_skipped": skipped,
            "dry_run": False,
        }
