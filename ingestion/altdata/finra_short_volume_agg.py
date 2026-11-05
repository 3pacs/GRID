"""FINRA daily short-volume aggregates: market-wide and index-ETF ratios.

``finra_short_volume.py`` writes one ``finra:short_volume:<SYMBOL>`` series
per symbol per trade date (~12k a day). Those have no feature mapping by
design, and until now nothing outside ingestion read them. This module adds
the aggregates a model can actually use, computed from the same daily file:

==========================================  =====================================
series_id                                   value
==========================================  =====================================
``finra:short_volume_agg:market_ratio``     sum(ShortVolume) / sum(TotalVolume), all rows
``finra:short_volume_agg:spy_ratio``        the same over SPY's rows
``finra:short_volume_agg:qqq_ratio``        ... QQQ
``finra:short_volume_agg:iwm_ratio``        ... IWM
==========================================  =====================================

The sums cover EVERY parsed row, i.e. every reporting facility. The
per-symbol series keep only the first row per (date, symbol), as the
producer's docstring explains. ShortVolume already includes short-exempt
volume per the FINRA layout doc.

Why a subclass instead of editing the producer
-----------------------------------------------
``finra_short_volume.py`` is sha256-pinned by the selected-FINRA custody
route (``smart_scheduler._FINRA_SELECTED_PINS``,
``scripts/finra_occurrence_driver.PRODUCER_SHA``). Editing it would
invalidate that admission chain. The scheduler's ordinary
``finra_short_volume`` entry runs :class:`FINRAShortVolumeAggPuller`
instead, a subclass that leaves the producer's per-symbol behaviour byte for
byte as it was and adds the aggregates after each successful pull.

``pull_recent`` skips trade dates that already have rows for this source, so
dates ingested before this module existed (or whose aggregate write failed
after the per-symbol commit) are filled by :meth:`backfill_aggregates`::

    python -m ingestion.altdata.finra_short_volume_agg --backfill-aggregates 2026-09-01 2026-11-04

Every aggregate row carries the pull's own ``pull_timestamp``, so a
point-in-time read sees backfilled history only from the day it was
backfilled.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from loguru import logger as log

from ingestion.altdata.finra_short_volume import (
    _DEFAULT_MARKET_PREFIX,
    _DOCS_URL,
    FINRAShortVolumePuller,
    _bounded_error,
    parse_daily_short_volume_file,
)
from ingestion.base import _http_status_from_exc

AGG_PREFIX = "finra:short_volume_agg"
AGG_MARKET_RATIO = f"{AGG_PREFIX}:market_ratio"
# Index ETFs whose own short-volume ratio is the classic positioning read.
AGG_ETF_SYMBOLS: tuple[str, ...] = ("SPY", "QQQ", "IWM")


def compute_daily_aggregates(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Short-volume ratios for one trade date's parsed rows.

    Parameters:
        rows: ``parse_daily_short_volume_file(...)["rows"]`` for one file.

    Returns:
        ``{series_id: {"obs_date", "value", "payload"}}`` for the market-wide
        ratio and each ``AGG_ETF_SYMBOLS`` ratio that has volume that day. A
        ratio with zero total volume, or a file spanning more than one trade
        date, is omitted rather than written as 0.
    """
    if not rows:
        return {}
    dates = {r["date"] for r in rows}
    if len(dates) != 1:
        log.warning("FINRA short volume aggregates: file spans {n} dates; skipped", n=len(dates))
        return {}
    (obs_date,) = dates

    def _ratio(selected: list[dict[str, Any]], scope: str) -> dict[str, Any] | None:
        short = sum(r["short_volume"] for r in selected)
        total = sum(r["total_volume"] for r in selected)
        if total <= 0:
            return None
        return {
            "obs_date": obs_date,
            "value": short / total,
            "payload": {
                "scope": scope,
                "short_volume": short,
                "total_volume": total,
                "rows": len(selected),
                "symbols": len({r["symbol"] for r in selected}),
                "method": "sum(ShortVolume) / sum(TotalVolume) over all facility rows",
                "source": "FINRA Reg SHO Daily Short Sale Volume File",
                "docs_url": _DOCS_URL,
            },
        }

    out: dict[str, dict[str, Any]] = {}
    market = _ratio(rows, "all symbols")
    if market is not None:
        out[AGG_MARKET_RATIO] = market
    for symbol in AGG_ETF_SYMBOLS:
        etf = _ratio([r for r in rows if r["symbol"].strip().upper() == symbol], symbol)
        if etf is not None:
            out[f"{AGG_PREFIX}:{symbol.lower()}_ratio"] = etf
    return out


class FINRAShortVolumeAggPuller(FINRAShortVolumePuller):
    """``FINRAShortVolumePuller`` plus the daily aggregates (see module docstring)."""

    # Same source_catalog row as the producer. Restated literally because the
    # registry/catalog tests read it from this class body, not its bases.
    SOURCE_NAME: str = "FINRA_SHORT_VOLUME"

    def pull(
        self,
        trade_date: date | str,
        *,
        url: str | None = None,
        market_prefix: str = _DEFAULT_MARKET_PREFIX,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the producer's pull unchanged, then write that file's aggregates.

        The fetched text is captured from the producer's own fetch, so the
        file is downloaded once. Aggregates are written in their own
        transaction after the per-symbol commit. If that write fails, the
        per-symbol result still stands, ``aggregates_error`` says why, and
        ``backfill_aggregates`` repairs the date.
        """
        captured: list[str] = []
        instance_override = self.__dict__.get("_fetch_raw_text")
        fetch = self._fetch_raw_text

        def _capturing_fetch(*args: Any, **kwargs: Any) -> str:
            text = fetch(*args, **kwargs)
            captured.append(text)
            return text

        self._fetch_raw_text = _capturing_fetch
        try:
            result = super().pull(trade_date, url=url, market_prefix=market_prefix, dry_run=dry_run)
        finally:
            if instance_override is None:
                del self._fetch_raw_text  # the class method again
            else:
                self._fetch_raw_text = instance_override

        if result.get("status") != "SUCCESS" or not captured:
            return result
        try:
            aggregates = compute_daily_aggregates(parse_daily_short_volume_file(captured[-1])["rows"])
        except ValueError:
            return result  # the producer already reported this file's parse state
        if not aggregates:
            return {**result, "aggregates_inserted": 0}

        obs = next(iter(aggregates.values()))["obs_date"]
        if dry_run:
            with self.engine.connect() as conn:
                existing = self._get_existing_pairs_in_range(conn, obs, obs)
            return {**result, "aggregates_would_insert": sum(
                1 for sid in aggregates if obs not in existing.get(sid, set()))}
        try:
            with self.engine.begin() as conn:
                self._file_advisory_lock(conn, "pull", obs.isoformat())
                existing = self._get_existing_pairs_in_range(conn, obs, obs)
                inserted = self._insert_missing_aggregates(conn, aggregates, existing)
        except Exception as exc:  # noqa: BLE001 -- per-symbol rows are already committed
            log.warning("{s}: aggregate write failed for {d}: {e}",
                        s=self.SOURCE_NAME, d=obs, e=_bounded_error(exc))
            return {**result, "aggregates_inserted": 0, "aggregates_error": _bounded_error(exc)}
        return {**result, "aggregates_inserted": inserted}

    def _insert_missing_aggregates(
        self,
        conn: Any,
        aggregates: dict[str, dict[str, Any]],
        existing_pairs: dict[str, set[date]],
    ) -> int:
        """Write each aggregate not already stored for its (series, date)."""
        inserted = 0
        for sid, agg in aggregates.items():
            existing_dates = existing_pairs.setdefault(sid, set())
            if agg["obs_date"] in existing_dates:
                continue
            self._insert_raw(
                conn=conn,
                series_id=sid,
                obs_date=agg["obs_date"],
                value=agg["value"],
                raw_payload=agg["payload"],
            )
            existing_dates.add(agg["obs_date"])
            inserted += 1
        return inserted

    def backfill_aggregates(
        self,
        start: date | str,
        end: date | str,
        *,
        market_prefix: str = _DEFAULT_MARKET_PREFIX,
    ) -> dict[str, Any]:
        """Re-fetch past daily files and write only their missing aggregates.

        Walks ``start``..``end`` (weekdays, inclusive). Per-symbol rows are
        never written. A date with no published file (HTTP 403/404) is
        skipped; any other failure is counted and the walk continues.

        Returns:
            dict with ``status``, ``aggregates_inserted`` and date counts:
            ``dates_written``, ``dates_complete`` (all aggregates already
            stored), ``dates_skipped``, ``dates_failed``.
        """
        start_d = date.fromisoformat(start) if isinstance(start, str) else start
        end_d = date.fromisoformat(end) if isinstance(end, str) else end
        counts = {"dates_written": 0, "dates_complete": 0, "dates_skipped": 0, "dates_failed": 0}
        total = 0
        d = start_d
        while d <= end_d:
            if d.weekday() < 5:
                outcome, n = self._backfill_one(d, market_prefix)
                counts[outcome] += 1
                total += n
            d += timedelta(days=1)
        log.info("{s}: aggregate backfill {a}..{b}: {n} rows, {c}",
                 s=self.SOURCE_NAME, a=start_d, b=end_d, n=total, c=counts)
        return {"status": "FAILED" if counts["dates_failed"] else "SUCCESS",
                "aggregates_inserted": total, **counts}

    def _backfill_one(self, d: date, market_prefix: str) -> tuple[str, int]:
        try:
            raw_text = self._fetch_raw_text(d, market_prefix=market_prefix)
            aggregates = compute_daily_aggregates(parse_daily_short_volume_file(raw_text)["rows"])
            if not aggregates:
                return "dates_skipped", 0
            with self.engine.begin() as conn:
                self._file_advisory_lock(conn, "pull", d.isoformat())
                existing = self._get_existing_pairs_in_range(conn, d, d)
                n = self._insert_missing_aggregates(conn, aggregates, existing)
        except Exception as exc:  # noqa: BLE001 -- counted, the walk continues
            if _http_status_from_exc(exc) in (403, 404):
                return "dates_skipped", 0
            log.warning("{s}: aggregate backfill failed for {d}: {e}",
                        s=self.SOURCE_NAME, d=d, e=_bounded_error(exc))
            return "dates_failed", 0
        return ("dates_written" if n else "dates_complete"), n


if __name__ == "__main__":
    import argparse
    import json

    _parser = argparse.ArgumentParser(
        description="Backfill FINRA daily short-volume aggregates (only missing rows are written)."
    )
    _parser.add_argument("--backfill-aggregates", nargs=2, metavar=("START", "END"), required=True,
                         help="Inclusive YYYY-MM-DD trade-date range.")
    _args = _parser.parse_args()

    from db import get_engine

    _result = FINRAShortVolumeAggPuller(get_engine()).backfill_aggregates(*_args.backfill_aggregates)
    print(json.dumps(_result, sort_keys=True))
    raise SystemExit(0 if _result["status"] == "SUCCESS" else 1)
