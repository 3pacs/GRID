"""
GRID Baltic Dry Index and shipping indices ingestion module.

Pulls the Baltic Dry Index (BDI) and related sub-indices. FRED used to mirror
the Baltic series, but the historical IDs have gone dark. The puller therefore
keeps FRED as a legacy primary and falls back to a current public Baltic
snapshot without logging noisy per-series failures for known-dead FRED IDs.
"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from fedfred import FredAPI
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# Public daily snapshot used by https://balticdryindex.com/. The frontend's
# historical chart uses synthetic history; only latest.json is trusted here.
BALTIC_PUBLIC_SNAPSHOT_URL = (
    "https://raw.githubusercontent.com/balticdryindex/"
    "balticdryindex/gh-pages/data/latest.json"
)

# Maps internal GRID series_id -> legacy FRED code + public snapshot key.
BALTIC_SERIES: dict[str, dict[str, str]] = {
    "baltic.bdi": {
        "fred_id": "DBDI",
        "public_key": "bdi",
        "description": "Baltic Dry Index — composite shipping cost benchmark",
    },
    "baltic.capesize": {
        "fred_id": "BCPI",
        "public_key": "bci",
        "description": "Baltic Capesize Index — large bulk carriers (100K+ DWT)",
    },
    "baltic.panamax": {
        "fred_id": "BPTI",
        "public_key": "bpi",
        "description": "Baltic Panamax Index — mid-size bulk carriers (60-80K DWT)",
    },
    "baltic.supramax": {
        "fred_id": "BSI",
        "public_key": "bsi",
        "description": "Baltic Supramax Index — handymax bulk carriers (45-60K DWT)",
    },
    "baltic.handysize": {
        "fred_id": "BHSI",
        "public_key": "bhsi",
        "description": "Baltic Handysize Index — smaller bulk carriers",
    },
}

# Minimum delay between FRED API calls (seconds)
_RATE_LIMIT_DELAY: float = 0.25
_HTTP_TIMEOUT_SECONDS: float = 15.0


def _coerce_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _date_in_requested_range(
    obs_date: date,
    start_date: str | date,
    end_date: str | date | None,
) -> bool:
    start = _coerce_date(start_date)
    end = _coerce_date(end_date)
    if start is not None and obs_date < start:
        return False
    if end is not None and obs_date > end:
        return False
    return True


def _fred_series_is_known_dead(exc: BaseException) -> bool:
    """Return True for FRED responses saying a legacy Baltic ID no longer exists."""
    parts: list[str] = []
    seen: set[int] = set()
    queue: list[Any] = [exc]
    while queue:
        cur = queue.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        parts.append(str(cur))

        response = getattr(cur, "response", None)
        if response is not None:
            parts.append(str(getattr(response, "text", "") or ""))

        for attr in ("__cause__", "__context__"):
            inner = getattr(cur, attr, None)
            if inner is not None:
                queue.append(inner)

        last_attempt = getattr(cur, "last_attempt", None)
        exception_fn = getattr(last_attempt, "exception", None)
        if callable(exception_fn):
            try:
                inner_exc = exception_fn()
            except Exception:
                inner_exc = None
            if inner_exc is not None:
                queue.append(inner_exc)

    text_blob = " ".join(part.lower() for part in parts if part)
    return "series does not exist" in text_blob


def _parse_public_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the public Baltic latest.json snapshot."""
    obs_date = _coerce_date(payload.get("date"))
    if obs_date is None:
        raise ValueError("Baltic public snapshot missing date")

    values: dict[str, dict[str, Any]] = {}
    for meta in BALTIC_SERIES.values():
        public_key = meta["public_key"]
        raw_entry = payload.get(public_key)
        if not isinstance(raw_entry, dict):
            continue
        raw_value = raw_entry.get("value")
        if raw_value is None:
            continue
        values[public_key] = {
            "value": float(raw_value),
            "prev": raw_entry.get("prev"),
            "change": raw_entry.get("change"),
            "pct": raw_entry.get("pct"),
        }

    if not values:
        raise ValueError("Baltic public snapshot contained no usable index values")

    return {
        "date": obs_date,
        "updated": payload.get("updated"),
        "source": payload.get("source") or "Baltic Exchange",
        "values": values,
        "stats": payload.get("stats") if isinstance(payload.get("stats"), dict) else {},
    }


def _fetch_public_snapshot() -> dict[str, Any]:
    response = requests.get(
        BALTIC_PUBLIC_SNAPSHOT_URL,
        timeout=_HTTP_TIMEOUT_SECONDS,
        headers={"User-Agent": "GRID/1.0"},
    )
    response.raise_for_status()
    return _parse_public_snapshot(response.json())


class BalticDryPuller(BasePuller):
    """Pulls Baltic Dry Index and sub-indices from FRED into ``raw_series``.

    Uses the FRED API via fedfred to fetch BDI, Capesize, Panamax, and
    Supramax indices. Stores each under the ``baltic.*`` series namespace
    with Baltic_Exchange as the logical source.

    Attributes:
        fred: fedfred.FredAPI client instance.
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for Baltic_Exchange.
    """

    SOURCE_NAME: str = "Baltic_Exchange"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.balticexchange.com/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the Baltic Dry puller.

        Parameters:
            api_key: FRED API key (used to fetch Baltic series from FRED).
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.fred = FredAPI(api_key)
        self._public_snapshot_cache: dict[str, Any] | None = None
        super().__init__(db_engine)
        log.info("BalticDryPuller initialised — source_id={sid}", sid=self.source_id)

    def pull_series(
        self,
        grid_series_id: str,
        start_date: str | date = "2000-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Fetch a single Baltic series from FRED and insert into raw_series.

        Parameters:
            grid_series_id: Internal GRID series ID (e.g. 'baltic.bdi').
            start_date: Earliest observation date to fetch.
            end_date: Latest observation date (default: today).

        Returns:
            dict: Result with keys ``series_id``, ``rows_inserted``,
                  ``status``, ``errors``.
        """
        if grid_series_id not in BALTIC_SERIES:
            return {
                "series_id": grid_series_id,
                "rows_inserted": 0,
                "status": "FAILED",
                "errors": [f"Unknown series: {grid_series_id}"],
            }

        series_meta = BALTIC_SERIES[grid_series_id]
        fred_id = series_meta["fred_id"]
        log.info(
            "Pulling {gsid} (FRED {fid}) from {sd}",
            gsid=grid_series_id,
            fid=fred_id,
            sd=start_date,
        )

        result: dict[str, Any] = {
            "series_id": grid_series_id,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            obs_kwargs: dict[str, Any] = {
                "observation_start": str(start_date),
            }
            if end_date:
                obs_kwargs["observation_end"] = str(end_date)

            data: pd.DataFrame = self.fred.get_series_observations(
                fred_id, **obs_kwargs
            )

            if data is None or data.empty:
                log.warning("FRED returned no data for {fid}", fid=fred_id)
                result["status"] = "PARTIAL"
                result["errors"].append("No data returned")
                return result

            # Normalise column names (fedfred may vary by version)
            if "date" in data.columns and "value" in data.columns:
                pass
            elif "observation_date" in data.columns:
                data = data.rename(columns={"observation_date": "date"})
            else:
                if data.index.name == "date" or hasattr(data.index, "date"):
                    data = data.reset_index()

            # Drop rows where value is NaN or FRED's '.' missing marker
            data = data[data["value"].apply(
                lambda v: v != "." and pd.notna(v)
            )].copy()
            data["value"] = pd.to_numeric(data["value"], errors="coerce")
            coerced_count = data["value"].isna().sum()
            if coerced_count > 0:
                log.warning(
                    "Coerced {n} non-numeric values to NaN for {gsid}",
                    n=int(coerced_count),
                    gsid=grid_series_id,
                )
            data = data.dropna(subset=["value"])

            observations = []
            for _, row in data.iterrows():
                obs_date_val = (
                    row["date"].date()
                    if hasattr(row["date"], "date") and callable(row["date"].date)
                    else pd.Timestamp(row["date"]).date()
                )
                observations.append({
                    "obs_date": obs_date_val,
                    "value": float(row["value"]),
                    "payload": {
                        "fred_series": fred_id,
                        "source": "Baltic_Exchange_via_FRED",
                        "provider": "fred_legacy",
                    },
                })

            inserted = self._insert_observations(grid_series_id, observations)
            result["rows_inserted"] = inserted
            log.info(
                "{gsid}: inserted {n} rows",
                gsid=grid_series_id,
                n=inserted,
            )

        except Exception as exc:
            if _fred_series_is_known_dead(exc):
                log.warning(
                    "Legacy FRED Baltic series {fid} is unavailable; using public snapshot fallback",
                    fid=fred_id,
                )
                return self._pull_public_snapshot_series(
                    grid_series_id,
                    start_date=start_date,
                    end_date=end_date,
                    fred_error=str(exc),
                )

            log.error(
                "Baltic pull failed for {gsid} (FRED {fid}): {err}",
                gsid=grid_series_id,
                fid=fred_id,
                err=str(exc),
            )
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

            # Record the failure row for observability
            try:
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, "
                            "raw_payload, pull_status) "
                            "VALUES (:sid, :src, :od, 0, :payload, 'FAILED')"
                        ),
                        {
                            "sid": grid_series_id,
                            "src": self.source_id,
                            "od": date.today(),
                            "payload": json.dumps({"error": str(exc)}),
                        },
                    )
            except Exception as insert_exc:
                log.error(
                    "Failed to record error row for {gsid}: {err}",
                    gsid=grid_series_id,
                    err=str(insert_exc),
                )

        # Rate limiting between FRED API calls
        time.sleep(_RATE_LIMIT_DELAY)
        return result

    def _pull_public_snapshot_series(
        self,
        grid_series_id: str,
        start_date: str | date = "2000-01-01",
        end_date: str | date | None = None,
        fred_error: str | None = None,
    ) -> dict[str, Any]:
        """Fetch one current Baltic value from the public snapshot fallback."""
        result: dict[str, Any] = {
            "series_id": grid_series_id,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
            "source": "public_snapshot",
        }
        try:
            snapshot = self._get_public_snapshot()
            obs_date = snapshot["date"]
            if not _date_in_requested_range(obs_date, start_date, end_date):
                result["status"] = "PARTIAL"
                result["errors"].append(
                    f"Public snapshot date {obs_date} outside requested range"
                )
                return result

            public_key = BALTIC_SERIES[grid_series_id]["public_key"]
            value_info = snapshot["values"].get(public_key)
            if not value_info:
                result["status"] = "PARTIAL"
                result["errors"].append(
                    f"Public snapshot missing key {public_key!r}"
                )
                return result

            payload = {
                "source": "Baltic_Exchange_public_snapshot",
                "provider": "balticdryindex_github_latest",
                "upstream_url": BALTIC_PUBLIC_SNAPSHOT_URL,
                "snapshot_source": snapshot.get("source"),
                "snapshot_updated": snapshot.get("updated"),
                "index_key": public_key,
                "prev": value_info.get("prev"),
                "change": value_info.get("change"),
                "pct": value_info.get("pct"),
            }
            if fred_error:
                payload["legacy_fred_error"] = fred_error[:500]

            inserted = self._insert_observations(
                grid_series_id,
                [{
                    "obs_date": obs_date,
                    "value": value_info["value"],
                    "payload": payload,
                }],
            )
            result["rows_inserted"] = inserted
            return result
        except Exception as exc:
            log.error(
                "Baltic public snapshot fallback failed for {gsid}: {err}",
                gsid=grid_series_id,
                err=str(exc),
            )
            result["status"] = "FAILED"
            result["errors"].append(str(exc))
            return result

    def _get_public_snapshot(self) -> dict[str, Any]:
        if self._public_snapshot_cache is None:
            self._public_snapshot_cache = _fetch_public_snapshot()
        return self._public_snapshot_cache

    def _insert_observations(
        self,
        grid_series_id: str,
        observations: list[dict[str, Any]],
    ) -> int:
        inserted = 0
        with self.engine.begin() as conn:
            # Batch dedup: one query instead of per-row checks
            existing_dates = self._get_existing_dates(grid_series_id, conn)
            skipped = 0

            for row in observations:
                obs_date_val = row["obs_date"]
                if obs_date_val in existing_dates:
                    skipped += 1
                    continue

                conn.execute(
                    text(
                        "INSERT INTO raw_series "
                        "(series_id, source_id, obs_date, value, "
                        "raw_payload, pull_status) "
                        "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                    ),
                    {
                        "sid": grid_series_id,
                        "src": self.source_id,
                        "od": obs_date_val,
                        "val": float(row["value"]),
                        "payload": json.dumps(row["payload"], default=str),
                    },
                )
                inserted += 1

            if skipped:
                log.debug(
                    "{gsid}: skipped {n} existing rows",
                    gsid=grid_series_id,
                    n=skipped,
                )
        return inserted

    def pull_all(
        self,
        series_list: list[str] | None = None,
        start_date: str | date = "2000-01-01",
        end_date: str | date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull all Baltic shipping indices sequentially.

        Never stops on a single-series failure -- logs and continues.

        Parameters:
            series_list: List of GRID series IDs to pull. Defaults to all
                         four Baltic indices.
            start_date: Earliest observation date.
            end_date: Latest observation date (default: today).

        Returns:
            list[dict]: One result dict per series.
        """
        if series_list is None:
            series_list = list(BALTIC_SERIES.keys())

        log.info(
            "Starting Baltic bulk pull — {n} series from {sd}",
            n=len(series_list),
            sd=start_date,
        )

        results: list[dict[str, Any]] = []
        for gsid in series_list:
            # Incremental pull: start from last known date minus 7-day overlap
            latest = self._get_latest_date(gsid)
            effective_start = start_date
            if latest is not None:
                incremental = latest - timedelta(days=7)
                start_as_date = (
                    date.fromisoformat(str(start_date))
                    if isinstance(start_date, str)
                    else start_date
                )
                if incremental > start_as_date:
                    effective_start = incremental.isoformat()
                    log.info(
                        "{gsid}: incremental from {d} (last={l})",
                        gsid=gsid,
                        d=effective_start,
                        l=latest,
                    )

            res = self.pull_series(gsid, effective_start, end_date)
            results.append(res)

        log.info(
            "Baltic bulk pull complete — {ok}/{total} succeeded",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
        )
        return results


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    puller = BalticDryPuller(
        api_key=settings.FRED_API_KEY,
        db_engine=get_engine(),
    )
    results = puller.pull_all(start_date="2015-01-01")
    for r in results:
        print(f"  {r['series_id']}: {r['status']} ({r['rows_inserted']} rows)")
