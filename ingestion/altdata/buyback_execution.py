"""
GRID buyback execution rate vs authorization ingestion module (CAT-67).

The alpha: announced buyback authorizations are cheap PR — management can
commit nothing and still generate a headline. Actually *executed* buybacks
are real capital deployment that shows up in the Z.1 Flow of Funds. The
ratio of executed buybacks to corporate profits is a much stronger signal
than the announcement alone:

- Ratio trending up = management deploying cash into own stock
  (earnings accretion, confidence, share-count compression tailwind).
- Ratio trending down = management signaling caution, hitting liquidity
  constraints, or rotating cash into capex / dividends / debt paydown.
- Divergence between announced programs and the Z.1 net-repurchase line
  is the classic tell: big PR pipeline, small checkbook.

This puller fetches the quarterly FRED-published NBER / Z.1 aggregate
series for nonfinancial corporate buyback execution and profits, and
materializes a composite ``buybacks:execution_ratio`` in ``raw_series``
so downstream features can consume it without re-deriving.

FRED series pulled (all quarterly):

- ``NCBBCCB1Q027S``        — Nonfinancial corporate business, net equity
                              repurchases (Z.1 Flow of Funds, $ mil SAAR)
- ``CPATAX``               — Corporate profits after tax
- ``BOGZ1FU104122005Q``    — Nonfinancial corporate business, net
                              purchases of equities
- ``NCBEIAQ027S``          — Nonfinancial corporate business, capital
                              expenditures

The composite ``buybacks:execution_ratio`` is only written for periods
where both net repurchases and profits-after-tax are present and profits
are strictly positive.

Usage::

    from ingestion.altdata.buyback_execution import run_buyback_puller
    result = run_buyback_puller(engine)
    # {"fetched": 400, "inserted": 120, "series": {...}}

Graceful degradation: if ``FRED_API_KEY`` is missing, the puller logs a
warning and returns a zero-row result rather than crashing.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---------------------------------------------------------------------------
# FRED series configuration
# ---------------------------------------------------------------------------

#: Mapping of short GRID label → FRED series config. The short label is
#: used as the ``raw_series.series_id`` suffix (``buybacks:<label>``).
BUYBACK_SERIES: dict[str, dict[str, str]] = {
    "net_repurchases": {
        "fred_id": "NCBBCCB1Q027S",
        "description": (
            "Nonfinancial corporate business, net equity repurchases "
            "(Z.1 Flow of Funds, quarterly, $ millions SAAR)"
        ),
    },
    "profits_after_tax": {
        "fred_id": "CPATAX",
        "description": "Corporate profits after tax ($ billions SAAR)",
    },
    "net_equity_purchases": {
        "fred_id": "BOGZ1FU104122005Q",
        "description": (
            "Nonfinancial corporate business, net purchases of equities "
            "(Z.1, quarterly, $ millions)"
        ),
    },
    "capex": {
        "fred_id": "NCBEIAQ027S",
        "description": (
            "Nonfinancial corporate business, capital expenditures "
            "(Z.1, quarterly, $ millions SAAR)"
        ),
    },
}

#: Label for the materialized composite in raw_series.
EXECUTION_RATIO_LABEL: str = "execution_ratio"

#: ``raw_series.series_id`` prefix used for this puller.
SERIES_PREFIX: str = "buybacks"

#: FRED API endpoint (direct HTTP — no fedfred dependency).
_FRED_ENDPOINT: str = "https://api.stlouisfed.org/fred/series/observations"

#: Default start date for buyback history (Z.1 has deep quarterly history).
_DEFAULT_START: date = date(1990, 1, 1)

#: Rate-limit delay between FRED calls (seconds).
_RATE_LIMIT_DELAY: float = 0.3

#: Per-request HTTP timeout (seconds).
_REQUEST_TIMEOUT: int = 30


# ---------------------------------------------------------------------------
# Snapshot dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BuybackSnapshot:
    """Immutable per-period snapshot of buyback execution vs profits.

    Attributes:
        period_end: Observation date (quarter end, FRED convention).
        net_repurchases_usd: Net equity repurchases for the quarter
            (USD, raw FRED units — millions for NCBBCCB1Q027S, billions
            for CPATAX). The ratio is unit-agnostic so this is fine for
            *relative* trend work; absolute dollars would require unit
            reconciliation.
        profits_after_tax_usd: Corporate profits after tax.
        buyback_ratio: ``net_repurchases_usd / profits_after_tax_usd``
            when both are non-None and ``profits_after_tax_usd > 0``;
            ``None`` otherwise.
    """

    period_end: date
    net_repurchases_usd: float
    profits_after_tax_usd: float
    buyback_ratio: float | None = None

    def __post_init__(self) -> None:
        """Compute and freeze ``buyback_ratio`` if caller did not set it.

        Uses ``object.__setattr__`` because the dataclass is frozen.
        """
        if self.buyback_ratio is not None:
            return
        if self.profits_after_tax_usd is None or self.net_repurchases_usd is None:
            return
        if self.profits_after_tax_usd == 0:
            return
        try:
            ratio = float(self.net_repurchases_usd) / float(
                self.profits_after_tax_usd
            )
        except (TypeError, ValueError, ZeroDivisionError):
            return
        if math.isnan(ratio) or math.isinf(ratio):
            return
        object.__setattr__(self, "buyback_ratio", ratio)


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------


class BuybackExecutionPuller(BasePuller):
    """Pulls buyback execution aggregates from FRED and materializes the ratio.

    Stores each raw FRED series under ``raw_series.series_id`` of the form
    ``buybacks:<label>`` (e.g. ``buybacks:net_repurchases``). A composite
    ``buybacks:execution_ratio`` row is inserted for every period where
    both net repurchases and profits-after-tax are present and profits
    are strictly positive.

    Attributes:
        source_name: Human-readable source name (``buyback_execution``).
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved ``source_catalog.id``.
    """

    SOURCE_NAME: str = "buyback_execution"
    #: Auto-create source_catalog entry so standalone tests/runs do not
    #: fail when the canonical ``FRED`` row has not been seeded.
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _FRED_ENDPOINT,
        "cost_tier": "FREE",
        "latency_class": "QUARTERLY",
        "pit_available": False,
        "revision_behavior": "REVISED",
        "trust_score": "HIGH",
        "priority_rank": 50,
    }
    #: Public attribute matching the task spec (``source_name`` lowercase).
    source_name: str = "buyback_execution"

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the puller.

        Parameters:
            api_key: FRED API key. Empty string triggers graceful
                degradation — ``pull()`` and ``save_to_db()`` return
                empty results instead of raising.
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self._api_key = api_key or ""
        if not self._api_key:
            log.warning(
                "BuybackExecutionPuller: FRED_API_KEY not set — "
                "pulls will be skipped"
            )
        super().__init__(db_engine)

    # ------------------------------------------------------------------
    # Fetch layer
    # ------------------------------------------------------------------

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_fred_series(
        self,
        fred_id: str,
        start_date: date = _DEFAULT_START,
        end_date: date | None = None,
    ) -> list[tuple[date, float]]:
        """Fetch a single FRED series via the public HTTP API.

        Coerces the FRED ``"."`` missing-value sentinel to NaN and drops
        those observations (with a warning).

        Parameters:
            fred_id: FRED series identifier (e.g. ``NCBBCCB1Q027S``).
            start_date: Earliest observation date to request.
            end_date: Latest observation date to request. Defaults to
                today.

        Returns:
            List of ``(obs_date, value)`` tuples, sorted by date.

        Raises:
            requests.HTTPError: if the FRED API returns a non-2xx status.
        """
        params = {
            "series_id": fred_id,
            "api_key": self._api_key,
            "file_type": "json",
            "observation_start": start_date.isoformat(),
            "observation_end": (end_date or date.today()).isoformat(),
        }
        response = requests.get(
            _FRED_ENDPOINT, params=params, timeout=_REQUEST_TIMEOUT
        )
        response.raise_for_status()
        payload = response.json() or {}

        observations = payload.get("observations") or []
        result: list[tuple[date, float]] = []
        skipped = 0
        for obs in observations:
            raw_val = obs.get("value")
            raw_date = obs.get("date")
            if raw_val in (None, ".", ""):
                skipped += 1
                continue
            try:
                value = float(raw_val)
            except (TypeError, ValueError):
                skipped += 1
                continue
            if math.isnan(value) or math.isinf(value):
                skipped += 1
                continue
            try:
                obs_date = date.fromisoformat(raw_date)
            except (TypeError, ValueError):
                skipped += 1
                continue
            result.append((obs_date, value))

        if skipped:
            log.warning(
                "FRED {fid}: {n} observations coerced/skipped (missing or ' .')",
                fid=fred_id,
                n=skipped,
            )
        result.sort(key=lambda row: row[0])
        return result

    def pull(
        self,
        start_date: date = _DEFAULT_START,
        end_date: date | None = None,
    ) -> dict[str, list[tuple[date, float]]]:
        """Fetch every configured FRED series.

        Failures on individual series are logged but do not abort the
        remaining pulls — the returned dict simply omits (or contains an
        empty list for) the failed series.

        Parameters:
            start_date: Earliest observation date.
            end_date: Latest observation date (defaults to today).

        Returns:
            Dict mapping short label (e.g. ``"net_repurchases"``) to
            a list of ``(obs_date, value)`` tuples.
        """
        if not self._api_key:
            return {label: [] for label in BUYBACK_SERIES}

        fetched: dict[str, list[tuple[date, float]]] = {}
        for label, cfg in BUYBACK_SERIES.items():
            fred_id = cfg["fred_id"]
            try:
                fetched[label] = self._fetch_fred_series(
                    fred_id, start_date=start_date, end_date=end_date
                )
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "BuybackExecutionPuller: failed to fetch {fid} ({lbl}): {e}",
                    fid=fred_id,
                    lbl=label,
                    e=str(exc),
                )
                fetched[label] = []
            time.sleep(_RATE_LIMIT_DELAY)
        return fetched

    # ------------------------------------------------------------------
    # Persistence layer
    # ------------------------------------------------------------------

    @staticmethod
    def _series_id(label: str) -> str:
        """Return the full ``raw_series.series_id`` for a label."""
        return f"{SERIES_PREFIX}:{label}"

    def save_to_db(
        self, fetched: dict[str, list[tuple[date, float]]]
    ) -> dict[str, Any]:
        """Upsert fetched observations into ``raw_series`` and materialize
        the composite ``buybacks:execution_ratio``.

        Idempotent: already-present ``(series_id, obs_date)`` pairs are
        skipped so re-runs over overlapping windows insert nothing.

        Parameters:
            fetched: Output of :meth:`pull`.

        Returns:
            A result dict::

                {
                    "fetched": <int>,   # total observations returned by FRED
                    "inserted": <int>,  # rows actually written to raw_series
                    "series": {
                        "<label>": {"fetched": N, "inserted": M},
                        ...,
                        "execution_ratio": {"fetched": X, "inserted": Y},
                    },
                }
        """
        result_series: dict[str, dict[str, int]] = {}
        total_fetched = 0
        total_inserted = 0

        # Build a {label: {date: value}} cache for composite computation.
        cache: dict[str, dict[date, float]] = {}

        with self.engine.begin() as conn:
            for label, rows in fetched.items():
                series_id = self._series_id(label)
                existing = self._get_existing_dates(series_id, conn)
                inserted = 0
                cache[label] = {}
                for obs_date, value in rows:
                    cache[label][obs_date] = value
                    if obs_date in existing:
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=obs_date,
                        value=value,
                        raw_payload={
                            "fred_series": BUYBACK_SERIES[label]["fred_id"],
                            "label": label,
                        },
                    )
                    inserted += 1
                result_series[label] = {
                    "fetched": len(rows),
                    "inserted": inserted,
                }
                total_fetched += len(rows)
                total_inserted += inserted

            # Materialize composite execution_ratio.
            ratio_series_id = self._series_id(EXECUTION_RATIO_LABEL)
            existing_ratio = self._get_existing_dates(ratio_series_id, conn)
            net_rep = cache.get("net_repurchases", {})
            profits = cache.get("profits_after_tax", {})
            common_dates = sorted(set(net_rep.keys()) & set(profits.keys()))
            ratio_fetched = 0
            ratio_inserted = 0
            for obs_date in common_dates:
                prof = profits[obs_date]
                if prof is None or prof == 0:
                    continue
                snapshot = BuybackSnapshot(
                    period_end=obs_date,
                    net_repurchases_usd=net_rep[obs_date],
                    profits_after_tax_usd=prof,
                )
                if snapshot.buyback_ratio is None:
                    continue
                ratio_fetched += 1
                if obs_date in existing_ratio:
                    continue
                self._insert_raw(
                    conn=conn,
                    series_id=ratio_series_id,
                    obs_date=obs_date,
                    value=float(snapshot.buyback_ratio),
                    raw_payload={
                        "net_repurchases": net_rep[obs_date],
                        "profits_after_tax": prof,
                        "formula": "net_repurchases / profits_after_tax",
                    },
                )
                ratio_inserted += 1

            result_series[EXECUTION_RATIO_LABEL] = {
                "fetched": ratio_fetched,
                "inserted": ratio_inserted,
            }
            total_fetched += ratio_fetched
            total_inserted += ratio_inserted

        log.info(
            "BuybackExecutionPuller: fetched={f} inserted={i} series={s}",
            f=total_fetched,
            i=total_inserted,
            s=len(result_series),
        )
        return {
            "fetched": total_fetched,
            "inserted": total_inserted,
            "series": result_series,
        }


# ---------------------------------------------------------------------------
# Module-level entrypoint
# ---------------------------------------------------------------------------


def run_buyback_puller(
    engine: Engine,
    api_key: str | None = None,
    start_date: date = _DEFAULT_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    """Top-level entrypoint matching the GRID puller convention.

    Reads ``FRED_API_KEY`` from ``config.settings`` when not supplied.
    Missing key degrades gracefully to a zero-row result.

    Parameters:
        engine: SQLAlchemy engine for the GRID database.
        api_key: Optional override for the FRED API key. When ``None``
            the value is read from ``config.settings.FRED_API_KEY``.
        start_date: Earliest observation date to request (default 1990).
        end_date: Latest observation date (default today).

    Returns:
        Dict with keys ``fetched``, ``inserted``, and ``series``.
    """
    if api_key is None:
        try:
            from config import settings  # local import to aid testing
            api_key = settings.FRED_API_KEY
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "run_buyback_puller: could not read FRED_API_KEY from "
                "config.settings ({e}) — degrading to empty pull",
                e=str(exc),
            )
            api_key = ""

    if not api_key:
        log.warning(
            "run_buyback_puller: no FRED_API_KEY available — returning "
            "zero-row result (graceful degradation)"
        )
        return {
            "fetched": 0,
            "inserted": 0,
            "series": {
                label: {"fetched": 0, "inserted": 0}
                for label in list(BUYBACK_SERIES.keys()) + [EXECUTION_RATIO_LABEL]
            },
        }

    puller = BuybackExecutionPuller(api_key=api_key, db_engine=engine)
    fetched = puller.pull(start_date=start_date, end_date=end_date)
    return puller.save_to_db(fetched)
