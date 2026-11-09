"""
GRID GDELT news event data ingestion module.

Pulls news tone, conflict events, and actor-level geopolitical data
from the GDELT Project. Uses GDELT 2.0 API for recent data, GDELT DOC
API for thematic analysis, and GKG (Global Knowledge Graph) daily files
for historical data back to 1979.

Enhanced capabilities:
  - Actor-level tone tracking (named heads of state, central bankers, etc.)
  - Geopolitical event classification (sanctions, trade disputes, conflicts)
  - Financial theme extraction (tariffs, rate decisions, currency moves)
  - Country-pair tension scoring (US-China, US-Russia, etc.)
  - Narrative momentum detection (topic volume acceleration)
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.altdata import gdelt_doc
from ingestion.altdata.gdelt_doc import GDELTRateLimited
from ingestion.base import BasePuller

# ── GDELT query definitions (original + enhanced) ──────────────────────

GDELT_QUERIES: list[dict[str, str]] = [
    # Original queries
    {
        "query": "economy recession",
        "mode": "timelineTone",
        "feature": "gdelt_recession_tone",
        "timespan": "60m",
    },
    {
        "query": "Federal Reserve interest rates",
        "mode": "timelineTone",
        "feature": "gdelt_fed_tone",
        "timespan": "60m",
    },
    {
        "query": "trade war tariffs China",
        "mode": "timelinevol",
        "feature": "gdelt_trade_conflict_volume",
        "timespan": "60m",
    },
    # Enhanced: geopolitical actor/theme queries
    {
        "query": "sanctions Russia energy",
        "mode": "timelineTone",
        "feature": "gdelt_sanctions_russia_tone",
        "timespan": "60m",
    },
    {
        "query": "China Taiwan military",
        "mode": "timelinevol",
        "feature": "gdelt_taiwan_strait_volume",
        "timespan": "60m",
    },
    {
        "query": "OPEC oil production cut",
        "mode": "timelineTone",
        "feature": "gdelt_opec_tone",
        "timespan": "60m",
    },
    {
        "query": "semiconductor chip export controls",
        "mode": "timelinevol",
        "feature": "gdelt_chip_controls_volume",
        "timespan": "60m",
    },
    {
        "query": "central bank rate decision",
        "mode": "timelineTone",
        "feature": "gdelt_central_bank_tone",
        "timespan": "60m",
    },
    {
        "query": "currency devaluation emerging markets",
        "mode": "timelineTone",
        "feature": "gdelt_em_currency_tone",
        "timespan": "60m",
    },
    {
        "query": "NATO defense spending military",
        "mode": "timelinevol",
        "feature": "gdelt_defense_spending_volume",
        "timespan": "60m",
    },
]

# ── GDELT DOC API queries for actor-level tracking ─────────────────────

GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Named actors whose media tone we track — heads of state, central
# bankers, key geopolitical figures. GDELT indexes full text so we can
# search for specific names and measure sentiment around them.
GDELT_ACTOR_QUERIES: list[dict[str, str]] = [
    {"actor": "Jerome Powell", "feature": "gdelt_actor_powell_tone", "context": "Federal Reserve"},
    {"actor": "Christine Lagarde", "feature": "gdelt_actor_lagarde_tone", "context": "ECB"},
    {"actor": "Xi Jinping", "feature": "gdelt_actor_xi_tone", "context": "China"},
    {"actor": "Vladimir Putin", "feature": "gdelt_actor_putin_tone", "context": "Russia"},
    {"actor": "Mohammed bin Salman", "feature": "gdelt_actor_mbs_tone", "context": "Saudi Arabia"},
    {"actor": "Janet Yellen", "feature": "gdelt_actor_yellen_tone", "context": "Treasury"},
    {"actor": "Kazuo Ueda", "feature": "gdelt_actor_ueda_tone", "context": "BOJ"},
]

# ── Country-pair tension queries ───────────────────────────────────────
# Track bilateral tension levels between key country pairs. Rising
# tension correlates with defense spending, sanctions, and trade policy.

GDELT_TENSION_PAIRS: list[dict[str, str]] = [
    {"pair": "United States China", "feature": "gdelt_tension_us_china", "theme": "trade_war"},
    {"pair": "United States Russia", "feature": "gdelt_tension_us_russia", "theme": "sanctions"},
    {"pair": "United States Iran", "feature": "gdelt_tension_us_iran", "theme": "oil_sanctions"},
    {"pair": "China Taiwan", "feature": "gdelt_tension_china_taiwan", "theme": "strait_crisis"},
    {"pair": "Russia Ukraine", "feature": "gdelt_tension_russia_ukraine", "theme": "war"},
    {"pair": "Israel Iran", "feature": "gdelt_tension_israel_iran", "theme": "mideast"},
    {"pair": "India China", "feature": "gdelt_tension_india_china", "theme": "border"},
]

_GDELT_API_URL = GDELT_DOC_API_URL
_GDELT_GKG_URL = "http://data.gdeltproject.org/gkg/"
_GDELT_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "gdelt")

# CAMEO conflict event codes (14-20: protest, force, assault, fight, etc.)
_CONFLICT_CODES = {14, 15, 16, 17, 18, 19, 20}

# DOC API pacing lives in ingestion/altdata/gdelt_doc.py: one host-wide gate
# for GRID's GDELT callers (this puller, the breaking-news monitor, the
# spider) with a shared cooldown after any HTTP 429. This puller used to
# keep a per-instance token bucket, which could not see the other callers on
# the same IP, and it abandoned the whole run on the first 429.
_DOC_API_TIMEOUT: tuple[float, float] = gdelt_doc.REQUEST_TIMEOUT
# Part of a run budget kept for the request itself when turning the budget
# into a slot wait (GDELT takes 10-20 s to answer; the read timeout is 40 s).
_DOC_REQUEST_RESERVE_S: float = 45.0
# grid-scheduler's daily pull_recent has no outer timeout; bound it here.
_DEFAULT_RUN_BUDGET_S: float = 600.0


@dataclass
class _DocRun:
    """DOC API accounting for one pull run.

    A query GDELT refused with 429 (after the gate's cooldown retry) is
    counted in ``refused`` and the run moves on: the next query waits out
    the host-wide cooldown, which doubles with each 429. Once the cooldown
    no longer fits the remaining budget the gate raises without sending,
    and the run stops (``limited``).

    Attributes:
        deadline: ``time.monotonic()`` value after which no new request starts.
        sent: Requests this run put on the wire (429s included).
        answered: Non-429 answers.
        refused: Queries that ended in HTTP 429.
        limited: The no-slot stop that ended the run's DOC work.
        not_attempted: DOC queries the run skipped after that stop.
        retry_at: When the host may send again, from the last rate-limit stop.
    """

    deadline: float
    sent: int = 0
    answered: int = 0
    refused: int = 0
    limited: GDELTRateLimited | None = None
    not_attempted: int = 0
    retry_at: float | None = None

    @classmethod
    def with_budget(cls, budget_s: float | None) -> _DocRun:
        budget = _DEFAULT_RUN_BUDGET_S if budget_s is None else max(0.0, float(budget_s))
        return cls(deadline=time.monotonic() + budget)

    def slot_wait_s(self) -> float:
        """How long the next request may wait for a slot (negative: no time left)."""
        return self.deadline - time.monotonic() - _DOC_REQUEST_RESERVE_S

    def rate_limited(self, exc: GDELTRateLimited, remaining: int) -> bool:
        """Record a rate-limit answer; True when the run must stop sending.

        ``remaining`` counts the current query and those after it in the loop.
        """
        self.retry_at = exc.retry_at or self.retry_at
        if exc.sent:
            self.refused += 1
            return False
        if self.limited is None:
            self.limited = exc
        self.not_attempted += max(0, remaining)
        return True


class GDELTPuller(BasePuller):
    """Pulls news event data from the GDELT Project."""

    SOURCE_NAME = "GDELT"
    SOURCE_CONFIG = {
        "base_url": _GDELT_API_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 36,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        os.makedirs(_GDELT_DATA_DIR, exist_ok=True)
        log.info("GDELTPuller initialised — source_id={sid}", sid=self.source_id)

    def _doc_request(self, params: dict[str, Any], run: _DocRun) -> requests.Response:
        """One DOC API GET through the host-wide gate, counted against ``run``.

        Raises:
            GDELTRateLimited: No slot within the run's budget, or HTTP 429 after
                the gate's retry. ``exc.sent`` says whether anything was sent.
        """
        wait = run.slot_wait_s()
        if wait < 0:
            raise GDELTRateLimited(
                "GDELT run budget spent before the next DOC slot", sent=False, retry_at=None,
            )
        try:
            resp = gdelt_doc.doc_get(params, max_wait_s=wait, timeout=_DOC_API_TIMEOUT,
                                     caller="gdelt_puller")
        except GDELTRateLimited as exc:
            if exc.sent:
                run.sent += 1
            raise
        except requests.RequestException:
            run.sent += 1
            raise
        run.sent += 1
        run.answered += 1
        return resp

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
    def _fetch_gkg_day(self, target_date: date) -> pd.DataFrame | None:
        """Download and parse a daily GKG file.

        GKG daily files are tab-separated with fields including:
        DATE, NUMARTS, COUNTS, THEMES, LOCATIONS, PERSONS, ORGANIZATIONS,
        TONE, GCAM, etc.
        """
        date_str = target_date.strftime("%Y%m%d")
        filename = f"{date_str}.gkg.csv"
        local_path = os.path.join(_GDELT_DATA_DIR, filename)

        if os.path.exists(local_path):
            try:
                return pd.read_csv(local_path, sep="\t", header=None, on_bad_lines="skip")
            except Exception as exc:
                log.warning("Failed to read cached GDELT file {p}: {e}", p=local_path, e=exc)

        url = f"{_GDELT_GKG_URL}{filename}.zip"
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                import zipfile
                from io import BytesIO
                with zipfile.ZipFile(BytesIO(resp.content)) as zf:
                    for name in zf.namelist():
                        with zf.open(name) as f:
                            content = f.read().decode("utf-8", errors="ignore")
                            with open(local_path, "w") as out:
                                out.write(content)
                return pd.read_csv(local_path, sep="\t", header=None, on_bad_lines="skip")
        except Exception as exc:
            log.debug("GKG download failed for {d}: {err}", d=date_str, err=str(exc))

        return None

    def pull_gkg_day(self, target_date: date) -> dict[str, Any]:
        """Process a single day of GKG data.

        Extracts: mean tone, total event count, conflict event count.
        """
        result: dict[str, Any] = {
            "series_id": f"gdelt_{target_date.isoformat()}",
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            df = self._fetch_gkg_day(target_date)
            if df is None or df.empty:
                result["status"] = "PARTIAL"
                return result

            # GKG column 7 (0-indexed) is the TONE field
            # Format: AvgTone,PositiveScore,NegativeScore,...
            tones = []
            event_count = len(df)

            if df.shape[1] > 7:
                for tone_str in df.iloc[:, 7].dropna():
                    try:
                        parts = str(tone_str).split(",")
                        if parts:
                            tones.append(float(parts[0]))
                    except (ValueError, IndexError):
                        continue

            inserted = 0
            with self.engine.begin() as conn:
                # Store average tone
                if tones:
                    avg_tone = sum(tones) / len(tones)
                    if not self._row_exists("gdelt_tone_usa", target_date, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {"sid": "gdelt_tone_usa", "src": self.source_id, "od": target_date, "val": avg_tone},
                        )
                        inserted += 1

                # Store event count
                if not self._row_exists("gdelt_event_count", target_date, conn):
                    conn.execute(
                        text(
                            "INSERT INTO raw_series "
                            "(series_id, source_id, obs_date, value, pull_status) "
                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                        ),
                        {"sid": "gdelt_event_count", "src": self.source_id, "od": target_date, "val": float(event_count)},
                    )
                    inserted += 1

            result["rows_inserted"] = inserted

        except Exception as exc:
            log.error("GKG day pull failed for {d}: {err}", d=target_date, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_historical(self, start_date: date, end_date: date | None = None) -> dict[str, Any]:
        """Pull historical GKG data day-by-day."""
        if end_date is None:
            end_date = date.today() - timedelta(days=1)

        log.info("Pulling GDELT historical from {s} to {e}", s=start_date, e=end_date)
        results: list[dict[str, Any]] = []

        current = start_date
        while current <= end_date:
            res = self.pull_gkg_day(current)
            results.append(res)
            current += timedelta(days=1)
            time.sleep(0.5)  # Gentle rate limiting

        total_rows = sum(r["rows_inserted"] for r in results)
        log.info("GDELT historical: {n} rows from {d} days", n=total_rows, d=len(results))
        return {
            "source": "GDELT",
            "total_rows": total_rows,
            "succeeded": sum(1 for r in results if r["status"] == "SUCCESS"),
            "total": len(results),
        }

    def _fetch_gdelt_api(
        self, query: str, mode: str, timespan: str, run: _DocRun | None = None,
    ) -> dict:
        """Fetch a bounded thematic timeline from GDELT DOC API.

        HTTP 429 is not answered here: the gate cools the host down and
        retries, then raises ``GDELTRateLimited`` for the run to stop on.
        """
        params = {
            "query": query,
            "mode": mode.lower(),
            "timespan": timespan,
            "format": "json",
            "maxrecords": 250,
        }
        resp = self._doc_request(params, run if run is not None else _DocRun.with_budget(None))
        if resp.status_code in {400, 403, 404, 500, 502, 503, 504}:
            log.info(
                "GDELT query skipped status={status} query={query}",
                status=resp.status_code,
                query=query[:80],
            )
            return {"timeline": [], "status": "SKIPPED", "http_status": resp.status_code}
        resp.raise_for_status()
        return resp.json()

    # ── Actor-level tone tracking ──────────────────────────────────────

    def _fetch_gdelt_doc_api(
        self,
        query: str,
        mode: str = "timelinetone",
        timespan: str = "30d",
        run: _DocRun | None = None,
    ) -> dict:
        """Fetch from GDELT DOC 2.0 API (broader than TV API).

        No tenacity retry: re-sending on a 429 only extends the throttle. The
        gate retries a 429 once after the host-wide cooldown.

        Parameters:
            query: Search query string.
            mode: Response mode (timelinetone, timelinevolinfo, etc.).
            timespan: Time span for results.
            run: The pull run's DOC accounting (a fresh one if omitted).

        Returns:
            Parsed JSON response.
        """
        params = {
            "query": query,
            "mode": mode,
            "timespan": timespan,
            "format": "json",
            "maxrecords": 250,
        }
        resp = self._doc_request(params, run if run is not None else _DocRun.with_budget(None))
        resp.raise_for_status()
        return resp.json()

    def _pull_actor_tones(self, run: _DocRun | None = None) -> int:
        """Pull tone data for tracked geopolitical actors.

        Queries GDELT DOC API for each named actor and stores the
        daily average tone as a time series. Stops at the first rate-limit
        stop (recorded on ``run``) instead of sending into a cooldown.

        Returns:
            Number of rows inserted.
        """
        inserted = 0
        run = run if run is not None else _DocRun.with_budget(None)
        if run.limited is not None:
            run.not_attempted += len(GDELT_ACTOR_QUERIES)
            return 0

        for a_idx, actor_def in enumerate(GDELT_ACTOR_QUERIES):
            try:
                query = f'"{actor_def["actor"]}" {actor_def["context"]}'
                data = self._fetch_gdelt_doc_api(
                    query=query,
                    mode="timelinetone",
                    timespan="30d",
                    run=run,
                )

                feature = actor_def["feature"]
                timeline = data.get("timeline", [])

                with self.engine.begin() as conn:
                    for series_data in timeline:
                        # DOC API returns nested series
                        points = series_data.get("data", [series_data])
                        if not isinstance(points, list):
                            points = [points]
                        for point in points:
                            try:
                                dt_str = point.get("date", "")
                                value = point.get("value", point.get("tone", 0))
                                if not dt_str or value is None:
                                    continue
                                obs_dt = pd.Timestamp(dt_str).date()
                                if not self._row_exists(feature, obs_dt, conn):
                                    self._insert_raw(
                                        conn=conn,
                                        series_id=feature,
                                        obs_date=obs_dt,
                                        value=float(value),
                                        raw_payload={
                                            "actor": actor_def["actor"],
                                            "context": actor_def["context"],
                                            "query": query,
                                        },
                                    )
                                    inserted += 1
                            except (ValueError, TypeError):
                                continue

            except GDELTRateLimited as exc:
                if run.rate_limited(exc, len(GDELT_ACTOR_QUERIES) - a_idx):
                    log.info("GDELT actor tones stopped: {e}", e=str(exc))
                    break
                log.info("GDELT actor tone refused for {a}: {e}", a=actor_def["actor"], e=str(exc))
            except Exception as exc:
                log.warning(
                    "GDELT actor tone query failed for {actor}: {err}",
                    actor=actor_def["actor"],
                    err=str(exc),
                )

        log.info("GDELT actor tones: {n} rows inserted", n=inserted)
        return inserted

    # ── Country-pair tension scoring ────────────────────────────────────

    def _pull_tension_scores(self, run: _DocRun | None = None) -> int:
        """Pull bilateral tension scores for tracked country pairs.

        Uses GDELT DOC API with tone mode to measure negative sentiment
        volume between country pairs. Negative tone + high volume =
        elevated tension. Stops at the first rate-limit stop (on ``run``).

        Returns:
            Number of rows inserted.
        """
        inserted = 0
        run = run if run is not None else _DocRun.with_budget(None)
        if run.limited is not None:
            run.not_attempted += len(GDELT_TENSION_PAIRS)
            return 0

        for p_idx, pair_def in enumerate(GDELT_TENSION_PAIRS):
            try:
                data = self._fetch_gdelt_doc_api(
                    query=pair_def["pair"],
                    mode="timelinetone",
                    timespan="30d",
                    run=run,
                )

                feature = pair_def["feature"]
                timeline = data.get("timeline", [])

                with self.engine.begin() as conn:
                    for series_data in timeline:
                        points = series_data.get("data", [series_data])
                        if not isinstance(points, list):
                            points = [points]
                        for point in points:
                            try:
                                dt_str = point.get("date", "")
                                value = point.get("value", point.get("tone", 0))
                                if not dt_str or value is None:
                                    continue
                                obs_dt = pd.Timestamp(dt_str).date()
                                # Invert tone: negative tone = positive tension
                                tension = -float(value) if value else 0.0
                                if not self._row_exists(feature, obs_dt, conn):
                                    self._insert_raw(
                                        conn=conn,
                                        series_id=feature,
                                        obs_date=obs_dt,
                                        value=tension,
                                        raw_payload={
                                            "pair": pair_def["pair"],
                                            "theme": pair_def["theme"],
                                            "raw_tone": float(value),
                                        },
                                    )
                                    inserted += 1
                            except (ValueError, TypeError):
                                continue

            except GDELTRateLimited as exc:
                if run.rate_limited(exc, len(GDELT_TENSION_PAIRS) - p_idx):
                    log.info("GDELT tension scores stopped: {e}", e=str(exc))
                    break
                log.info("GDELT tension query refused for {p}: {e}", p=pair_def["pair"], e=str(exc))
            except Exception as exc:
                log.warning(
                    "GDELT tension query failed for {pair}: {err}",
                    pair=pair_def["pair"],
                    err=str(exc),
                )

        log.info("GDELT tension scores: {n} rows inserted", n=inserted)
        return inserted

    # ── Signal emission for intelligence layer ──────────────────────────

    def _emit_tension_signals(self) -> int:
        """Emit signal_sources rows when tension scores spike.

        Scans recent tension data for significant increases and emits
        signals that the intelligence layer can act on.

        Returns:
            Number of signals emitted.
        """
        signals = 0
        tension_ticker_map = {
            "gdelt_tension_us_china": "SMH",    # semiconductor/trade war proxy
            "gdelt_tension_us_russia": "XLE",   # energy sanctions proxy
            "gdelt_tension_us_iran": "XLE",     # oil sanctions proxy
            "gdelt_tension_china_taiwan": "SMH", # TSMC/chip supply proxy
            "gdelt_tension_russia_ukraine": "ITA", # defense sector proxy
            "gdelt_tension_israel_iran": "XLE",  # oil price proxy
            "gdelt_tension_india_china": "INDA", # India ETF proxy
        }

        with self.engine.begin() as conn:
            for feature, ticker in tension_ticker_map.items():
                try:
                    rows = conn.execute(
                        text(
                            "SELECT obs_date, value FROM raw_series "
                            "WHERE series_id = :sid AND source_id = :src "
                            "AND pull_status = 'SUCCESS' "
                            "ORDER BY obs_date DESC LIMIT 7"
                        ),
                        {"sid": feature, "src": self.source_id},
                    ).fetchall()

                    if len(rows) < 3:
                        continue

                    recent = rows[0][1] if rows[0][1] else 0.0
                    avg_prior = sum(r[1] for r in rows[1:] if r[1]) / max(len(rows) - 1, 1)

                    # Spike detection: >50% increase over recent average
                    if avg_prior > 0 and recent > avg_prior * 1.5:
                        conn.execute(
                            text(
                                "INSERT INTO signal_sources "
                                "(source_type, source_id, ticker, signal_date, "
                                "signal_type, signal_value) "
                                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                                "ON CONFLICT (source_type, source_id, ticker, "
                                "signal_date, signal_type) DO NOTHING"
                            ),
                            {
                                "stype": "geopolitical",
                                "sid": feature,
                                "ticker": ticker,
                                "sdate": rows[0][0],
                                "stype2": "TENSION_SPIKE",
                                "sval": str({
                                    "feature": feature,
                                    "tension_score": round(recent, 3),
                                    "avg_prior": round(avg_prior, 3),
                                    "spike_pct": round(
                                        ((recent - avg_prior) / avg_prior) * 100, 1,
                                    ),
                                }),
                            },
                        )
                        signals += 1
                        log.info(
                            "GDELT TENSION SPIKE: {feat} at {val:.2f} "
                            "(avg {avg:.2f}, +{pct:.0f}%)",
                            feat=feature,
                            val=recent,
                            avg=avg_prior,
                            pct=((recent - avg_prior) / avg_prior) * 100,
                        )

                except Exception as exc:
                    log.debug(
                        "Tension signal check failed for {f}: {e}",
                        f=feature, e=str(exc),
                    )

        return signals

    # ── Main pull methods ───────────────────────────────────────────────

    @staticmethod
    def _apply_rate_limit_outcome(result: dict[str, Any], run: _DocRun) -> None:
        """Turn a run's 429s and rate-limit stop into an honest status (see pull_recent)."""
        if run.limited is None and run.refused == 0:
            return
        parts = []
        if run.refused:
            parts.append(f"rate-limited (HTTP 429) on {run.refused} DOC queries")
        if run.limited is not None:
            parts.append(f"{run.not_attempted} DOC queries not attempted "
                         "(no request slot within the run budget)")
        note = "GDELT DOC API " + "; ".join(parts)
        if run.retry_at:
            resume = datetime.fromtimestamp(run.retry_at, timezone.utc)
            note += f"; host may send again at {resume:%H:%M:%S}Z"
        result["doc_requests_sent"] = run.sent
        result["doc_queries_refused"] = run.refused
        result["doc_queries_not_attempted"] = run.not_attempted
        if (run.sent == 0 and run.refused == 0 and result["total_rows"] == 0
                and not result["errors"]):
            result["status"] = "DEFERRED"
            result["reason"] = (f"{note}; nothing was sent this run, and the next run "
                                "reads the same rolling window")
            return
        result["errors"].append(note)
        result["status"] = ("PARTIAL" if result["total_rows"] > 0 or run.answered > 0
                            else "FAILED")

    def pull_all(self, **kwargs: Any) -> dict[str, Any]:
        """Bounded recent pull -- the entry point generic callers expect.

        ``scripts/hermes_fixers.py`` retries a failed source via
        ``pull_all()``; GDELTPuller had none, so every GDELT repair attempt
        failed with ``AttributeError`` (6 warnings/day). Defaults mirror the
        SmartScheduler "gdelt" entry (one day, four theme queries, no actor
        or tension fan-out), with a 150 s DOC budget for repair callers that
        pass no deadline.
        """
        params: dict[str, Any] = {
            "days_back": 1,
            "max_theme_queries": 4,
            "include_actor_tones": False,
            "include_tensions": False,
            "include_signals": False,
            "run_budget_s": 150,
        }
        params.update(kwargs)
        return self.pull_recent(**params)

    def pull_recent(
        self,
        days_back: int = 30,
        max_theme_queries: int | None = None,
        include_actor_tones: bool = True,
        include_tensions: bool = True,
        include_signals: bool = True,
        run_budget_s: float | None = None,
        deadline_at: float | None = None,
    ) -> dict[str, Any]:
        """Pull recent GDELT data using the 2.0 API.

        Enhanced to include actor tones, country tensions, and
        geopolitical event signals alongside the original queries.

        Every DOC request goes through the host-wide gate
        (``ingestion/altdata/gdelt_doc.py``). A 429 cools the whole host
        down and is retried once at the next slot; a query still refused
        is recorded and the run moves on to the next one, which waits out
        the (doubling) cooldown. When no slot comes up within the run
        budget the run stops sending. Outcome:

        - nothing sent at all (the host was cooling down or busy, the run
          had no slot): ``DEFERRED``. No data is lost: every query reads a
          rolling window (``days_back`` for themes, 30 days for actors and
          tensions), so the next run reads the same dates.
        - some queries refused or not attempted, some answered: ``PARTIAL``.
        - every request that was sent got 429: ``FAILED``.

        Parameters:
            run_budget_s: Seconds this run may spend on DOC requests, slot
                waits included (default ``_DEFAULT_RUN_BUDGET_S``). Keep it
                under the caller's own timeout.
            deadline_at: ``time.monotonic()`` instant no request may start
                after; SmartScheduler passes its cooperative deadline
                (timeout_s minus the stop margin).
        """
        log.info("Pulling GDELT recent data (last {d} days)", d=days_back)
        run = _DocRun.with_budget(run_budget_s)
        if deadline_at is not None:
            run.deadline = min(run.deadline, float(deadline_at))
        result: dict[str, Any] = {
            "source": "GDELT",
            "total_rows": 0,
            "status": "SUCCESS",
            "errors": [],
            "actor_rows": 0,
            "tension_rows": 0,
            "signals_emitted": 0,
        }

        # 1. Original theme/tone queries
        inserted = 0
        theme_queries = GDELT_QUERIES
        if max_theme_queries is not None:
            theme_queries = GDELT_QUERIES[:max(0, max_theme_queries)]

        for q_idx, query_def in enumerate(theme_queries):
            try:
                data = self._fetch_gdelt_api(
                    query_def["query"],
                    query_def["mode"],
                    f"{days_back}d",
                    run=run,
                )
                feature = query_def["feature"]

                # Parse timeline data. The DOC API always wraps points one
                # level deeper than they look: ``timeline`` is a list of
                # per-series objects (``{"series": "...", "data": [...]}``),
                # not a flat list of ``{"date": ..., "value": ...}`` points.
                # This loop used to iterate the outer series objects
                # directly, so ``point.get("date")`` always missed (the
                # outer dict has no "date" key) and every point was
                # silently skipped — 0 rows inserted on every run since
                # this puller was written, even when the API returned real
                # data. ``_pull_actor_tones``/``_pull_tension_scores`` below
                # already unwrap ``data`` correctly; mirror that here.
                timeline = data.get("timeline", [])
                with self.engine.begin() as conn:
                    for series_data in timeline:
                        points = series_data.get("data", [series_data])
                        if not isinstance(points, list):
                            points = [points]
                        for point in points:
                            try:
                                dt_str = point.get("date", "")
                                value = point.get("value", point.get("tone", 0))
                                if not dt_str or value is None:
                                    continue
                                obs_dt = pd.Timestamp(dt_str).date()
                                if not self._row_exists(feature, obs_dt, conn):
                                    conn.execute(
                                        text(
                                            "INSERT INTO raw_series "
                                            "(series_id, source_id, obs_date, value, pull_status) "
                                            "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                        ),
                                        {"sid": feature, "src": self.source_id, "od": obs_dt, "val": float(value)},
                                    )
                                    inserted += 1
                            except (ValueError, TypeError):
                                continue

            except GDELTRateLimited as exc:
                # A refused query (429 after the cooldown retry) moves on: the
                # next one waits out the doubled host-wide cooldown. No slot
                # within the budget stops the run without sending.
                if run.rate_limited(exc, len(theme_queries) - q_idx):
                    log.info("GDELT theme queries stopped: {e}", e=str(exc))
                    break
                log.info("GDELT theme query refused ({f}): {e}", f=query_def["feature"], e=str(exc))
            except (requests.Timeout, requests.ConnectionError) as exc:
                # One unreachable/overloaded DOC API answer stands for the
                # whole run: log once, stop, and let the cooldown back off.
                remaining = len(theme_queries) - q_idx - 1
                log.warning(
                    "GDELT DOC API unavailable ({feature}): {err}; skipping the "
                    "remaining {n} theme queries this run",
                    feature=query_def["feature"],
                    err=str(exc)[:200],
                    n=remaining,
                )
                result["errors"].append(str(exc))
                break
            except Exception as exc:
                log.warning(
                    "GDELT query unavailable for {feature}: {err}",
                    feature=query_def["feature"],
                    err=str(exc),
                )
                result["errors"].append(str(exc))

        result["total_rows"] = inserted

        # 2. Actor-level tone tracking
        if include_actor_tones:
            try:
                actor_rows = self._pull_actor_tones(run=run)
                result["actor_rows"] = actor_rows
                result["total_rows"] += actor_rows
            except Exception as exc:
                log.warning("GDELT actor tone pull failed: {err}", err=str(exc))
                result["errors"].append(f"actor_tones: {exc}")

        # 3. Country-pair tension scores
        if include_tensions:
            try:
                tension_rows = self._pull_tension_scores(run=run)
                result["tension_rows"] = tension_rows
                result["total_rows"] += tension_rows
            except Exception as exc:
                log.warning("GDELT tension pull failed: {err}", err=str(exc))
                result["errors"].append(f"tension_scores: {exc}")

        # 4. Emit signals for tension spikes
        if include_signals:
            try:
                signals = self._emit_tension_signals()
                result["signals_emitted"] = signals
            except Exception as exc:
                log.warning("GDELT signal emission failed: {err}", err=str(exc))
                result["errors"].append(f"signals: {exc}")

        self._apply_rate_limit_outcome(result, run)

        log.info(
            "GDELT recent: {n} total rows (themes={t}, actors={a}, tension={ten}), "
            "{s} signals",
            n=result["total_rows"],
            t=inserted,
            a=result["actor_rows"],
            ten=result["tension_rows"],
            s=result["signals_emitted"],
        )
        return result
