"""
GRID Dune Analytics data ingestion module.

Pulls on-chain intelligence from the Dune Analytics API (requires DUNE_API_KEY).
Dune exposes SQL queries over Ethereum / Solana / Base / Arbitrum / Polygon
decoded blockchain data; this puller reads the results of pre-saved queries.

Three analytic domains are supported out of the box:

1. Smart money watchlist     -- Top wallets by realized PnL on a given token.
2. CEX flow balance          -- Net inflows vs. outflows across all CEX wallets.
3. Narrative heat check      -- Tokens with the biggest w/w jump in unique
                                new holders.

Series stored in ``raw_series``:

- dune.smart_money.<token>           -- leaderboard snapshot (value = wallet count)
- dune.cex_flow.<token>              -- net flow in USD (positive = accumulation)
- dune.narrative_heat                -- top-10 token holder growth snapshot
- dune.holder_growth.<token>         -- % new holders w/w, per token

The Dune query IDs are provided via config (DUNE_QUERY_* settings) so the
same puller can be re-pointed at community-maintained queries without a code
change. When a query ID is blank the puller logs a warning and skips that
category -- graceful degradation, consistent with other altdata pullers.

Data source: https://dune.com/docs/api/
MCP companion: ``.mcp.json`` exposes ``dune-analytics-mcp`` for ad-hoc queries.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ---- API URLs ----
_BASE_URL: str = "https://api.dune.com/api/v1"
_RESULTS_URL_TMPL: str = f"{_BASE_URL}/query/{{query_id}}/results"
_EXECUTE_URL_TMPL: str = f"{_BASE_URL}/query/{{query_id}}/execute"
_STATUS_URL_TMPL: str = f"{_BASE_URL}/execution/{{execution_id}}/status"
_EXEC_RESULTS_URL_TMPL: str = f"{_BASE_URL}/execution/{{execution_id}}/results"

# Series ID prefix
_SERIES_PREFIX: str = "dune"

# HTTP config
_REQUEST_TIMEOUT: int = 60
_RATE_LIMIT_DELAY: float = 1.0
_POLL_INTERVAL: float = 3.0
_POLL_MAX_ATTEMPTS: int = 40  # ~2 minutes

# Top-N leaderboard sizes
_SMART_MONEY_TOP_N: int = 20
_NARRATIVE_TOP_N: int = 10

# Feature definitions for registry/docs.
DUNE_FEATURES: dict[str, str] = {
    "smart_money": "Top wallets by realized PnL on a token (last 30d)",
    "cex_flow": "Net CEX inflow (-) / outflow (+) for a token (last 14d)",
    "narrative_heat": "Tokens with biggest w/w growth in unique new holders",
    "holder_growth": "Per-token % change in unique new holders w/w",
}


def _safe_name(name: str) -> str:
    """Sanitize a token/ticker for use in a series_id.

    Parameters:
        name: Raw token symbol or ticker.

    Returns:
        Lower-cased alphanumeric-with-underscore form.
    """
    out = name.lower()
    for ch in (" ", "-", ".", "/", "$", "#"):
        out = out.replace(ch, "_")
    return out.strip("_") or "unknown"


class DunePuller(BasePuller):
    """Pulls on-chain intelligence from Dune Analytics into ``raw_series``.

    The puller relies on Dune saved queries (either built by the operator or
    community-shared) and reads their cached results via the REST API. When a
    query id is not configured the relevant pull step is skipped with a warning
    so a missing query never blocks the rest of the ingestion cycle.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for Dune.
        api_key: Dune API key (from settings.DUNE_API_KEY).
        query_ids: Mapping of logical name -> Dune saved query id.
    """

    SOURCE_NAME: str = "Dune_Analytics"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _BASE_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    def __init__(
        self,
        db_engine: Engine,
        api_key: str | None = None,
        query_ids: dict[str, int] | None = None,
    ) -> None:
        """Initialise the Dune puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            api_key: Optional explicit Dune API key. Defaults to settings.
            query_ids: Optional override for the {logical_name: query_id} map.
                When omitted, values are read from ``config.settings``.
        """
        super().__init__(db_engine)

        from config import settings  # local import to avoid cycles at module load

        self.api_key: str = api_key or getattr(settings, "DUNE_API_KEY", "") or ""
        self.query_ids: dict[str, int] = query_ids or {
            "smart_money": int(getattr(settings, "DUNE_QUERY_SMART_MONEY", 0) or 0),
            "cex_flow": int(getattr(settings, "DUNE_QUERY_CEX_FLOW", 0) or 0),
            "narrative_heat": int(
                getattr(settings, "DUNE_QUERY_NARRATIVE_HEAT", 0) or 0
            ),
        }

        if not self.api_key:
            log.warning(
                "DunePuller: DUNE_API_KEY not set — puller will no-op on pull_*()"
            )
        log.info(
            "DunePuller initialised -- source_id={sid}, queries={q}",
            sid=self.source_id,
            q={k: v for k, v in self.query_ids.items() if v},
        )

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _series_id(self, category: str, name: str | None = None) -> str:
        """Build the full series_id for a Dune feature.

        Parameters:
            category: Feature category (e.g., ``smart_money``).
            name: Optional per-token suffix.

        Returns:
            Fully-qualified series_id (e.g., ``dune.smart_money.pepe``).
        """
        if name:
            return f"{_SERIES_PREFIX}.{category}.{_safe_name(name)}"
        return f"{_SERIES_PREFIX}.{category}"

    def _headers(self) -> dict[str, str]:
        return {
            "X-Dune-API-Key": self.api_key,
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _get(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """HTTP GET against the Dune API with auth + retries.

        Parameters:
            url: Fully-qualified Dune URL.
            params: Optional query-string parameters.

        Returns:
            Parsed JSON response body.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        resp = requests.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _post(self, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """HTTP POST against the Dune API with auth + retries."""
        resp = requests.post(
            url,
            headers=self._headers(),
            json=payload or {},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _fetch_query_rows(
        self,
        query_id: int,
        parameters: dict[str, Any] | None = None,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch result rows for a Dune saved query.

        When ``use_cache`` is True we read the latest cached results, which
        is free / fast. When False we trigger a fresh execution and poll for
        completion (uses credits).

        Parameters:
            query_id: Dune saved query id (integer).
            parameters: Optional query parameters; only applied on fresh runs.
            use_cache: Whether to read cached results vs. re-execute.

        Returns:
            List of row dicts; empty list when no data or on soft failure.
        """
        if query_id <= 0:
            log.warning("Dune: missing query_id, skipping fetch")
            return []

        if use_cache and not parameters:
            try:
                body = self._get(_RESULTS_URL_TMPL.format(query_id=query_id))
                return (body.get("result") or {}).get("rows") or []
            except requests.RequestException as exc:
                log.warning(
                    "Dune cached results for q={q} failed ({e}); falling back",
                    q=query_id,
                    e=str(exc),
                )

        # Fresh execution path -- spend credits, parameters allowed.
        exec_body = self._post(
            _EXECUTE_URL_TMPL.format(query_id=query_id),
            {"query_parameters": parameters or {}},
        )
        execution_id = exec_body.get("execution_id")
        if not execution_id:
            log.error("Dune: execute returned no execution_id for q={q}", q=query_id)
            return []

        for attempt in range(_POLL_MAX_ATTEMPTS):
            status = self._get(_STATUS_URL_TMPL.format(execution_id=execution_id))
            state = status.get("state") or status.get("status") or ""
            if state == "QUERY_STATE_COMPLETED":
                break
            if state in {"QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"}:
                log.error(
                    "Dune execution {e} for q={q} finished with state {s}",
                    e=execution_id,
                    q=query_id,
                    s=state,
                )
                return []
            time.sleep(_POLL_INTERVAL)
        else:
            log.error(
                "Dune execution {e} for q={q} did not finish within {n} polls",
                e=execution_id,
                q=query_id,
                n=_POLL_MAX_ATTEMPTS,
            )
            return []

        results = self._get(_EXEC_RESULTS_URL_TMPL.format(execution_id=execution_id))
        return (results.get("result") or {}).get("rows") or []

    # ------------------------------------------------------------------ #
    # 1. Smart money watchlist -- top wallets by realized PnL
    # ------------------------------------------------------------------ #

    def pull_smart_money(
        self,
        token: str | None = None,
        lookback_days: int = 30,
    ) -> dict[str, Any]:
        """Pull the top wallets by realized PnL on a given token.

        Expects the configured ``DUNE_QUERY_SMART_MONEY`` saved query to
        return rows shaped like::

            { "wallet": "0x...", "token": "PEPE", "realized_pnl_usd": 1234.56,
              "still_holding": true, "balance_usd": 5678.90 }

        Parameters:
            token: Optional token symbol to filter. When provided we try to
                pass it as the ``token`` query parameter, which means a fresh
                execution (uses credits). When omitted we read the cached
                results (free).
            lookback_days: PnL lookback window, forwarded as ``days`` param.

        Returns:
            dict with status, rows_inserted, leaderboard (top N wallets).
        """
        qid = self.query_ids.get("smart_money", 0)
        if not self.api_key or qid <= 0:
            return {"status": "SKIPPED", "rows_inserted": 0, "reason": "not_configured"}

        parameters: dict[str, Any] | None = None
        use_cache = True
        if token:
            parameters = {"token": token, "days": lookback_days}
            use_cache = False

        try:
            rows = self._fetch_query_rows(qid, parameters=parameters, use_cache=use_cache)
        except Exception as exc:  # noqa: BLE001 -- boundary catch, we log + soft-fail
            log.error("Dune smart_money pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        if not rows:
            log.warning("Dune smart_money: no rows returned for q={q}", q=qid)
            return {"status": "SUCCESS", "rows_inserted": 0}

        # Sort by realized PnL descending, keep top-N.
        def _pnl(r: dict[str, Any]) -> float:
            try:
                return float(r.get("realized_pnl_usd") or 0.0)
            except (TypeError, ValueError):
                return 0.0

        sorted_rows = sorted(rows, key=_pnl, reverse=True)
        leaderboard = sorted_rows[:_SMART_MONEY_TOP_N]

        # Group by token so we write one snapshot series per token.
        by_token: dict[str, list[dict[str, Any]]] = {}
        for r in leaderboard:
            sym = str(r.get("token") or token or "unknown").upper()
            by_token.setdefault(sym, []).append(r)

        today = date.today()
        inserted = 0
        with self.engine.begin() as conn:
            for sym, wallets in by_token.items():
                sid = self._series_id("smart_money", sym)
                if self._row_exists(sid, today, conn):
                    continue

                still_holding = sum(1 for w in wallets if bool(w.get("still_holding")))
                payload = {
                    "token": sym,
                    "lookback_days": lookback_days,
                    "wallet_count": len(wallets),
                    "still_holding": still_holding,
                    "wallets": [
                        {
                            "wallet": w.get("wallet"),
                            "realized_pnl_usd": _pnl(w),
                            "still_holding": bool(w.get("still_holding")),
                            "balance_usd": w.get("balance_usd"),
                        }
                        for w in wallets
                    ],
                    "query_id": qid,
                }
                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=float(len(wallets)),
                    raw_payload=payload,
                )
                inserted += 1

        log.info(
            "Dune smart_money: {n} token leaderboards inserted ({r} raw rows)",
            n=inserted,
            r=len(leaderboard),
        )
        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "leaderboard_size": len(leaderboard),
        }

    # ------------------------------------------------------------------ #
    # 2. CEX flow balance -- accumulation vs distribution
    # ------------------------------------------------------------------ #

    def pull_cex_flows(
        self,
        token: str | None = None,
        lookback_days: int = 14,
    ) -> dict[str, Any]:
        """Pull net inflows vs outflows across all CEX wallets for a token.

        Expects the configured ``DUNE_QUERY_CEX_FLOW`` saved query to
        return rows shaped like::

            { "token": "PEPE", "inflow_usd": 1234.0, "outflow_usd": 567.0,
              "net_usd": 667.0, "exchange_count": 12 }

        A positive ``net_usd`` means coins are leaving exchanges (bullish /
        accumulation); negative means coins are moving onto exchanges
        (bearish / distribution).

        Parameters:
            token: Optional token symbol filter. Triggers a fresh execution
                when provided.
            lookback_days: Flow window, forwarded as ``days`` param.

        Returns:
            dict with status, rows_inserted, tokens_seen.
        """
        qid = self.query_ids.get("cex_flow", 0)
        if not self.api_key or qid <= 0:
            return {"status": "SKIPPED", "rows_inserted": 0, "reason": "not_configured"}

        parameters: dict[str, Any] | None = None
        use_cache = True
        if token:
            parameters = {"token": token, "days": lookback_days}
            use_cache = False

        try:
            rows = self._fetch_query_rows(qid, parameters=parameters, use_cache=use_cache)
        except Exception as exc:  # noqa: BLE001
            log.error("Dune cex_flow pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        if not rows:
            log.warning("Dune cex_flow: no rows returned for q={q}", q=qid)
            return {"status": "SUCCESS", "rows_inserted": 0}

        today = date.today()
        inserted = 0
        seen: set[str] = set()

        with self.engine.begin() as conn:
            for r in rows:
                sym = str(r.get("token") or "").upper()
                if not sym:
                    continue
                try:
                    net_usd = float(r.get("net_usd") or 0.0)
                except (TypeError, ValueError):
                    log.warning(
                        "Dune cex_flow: non-numeric net_usd for {s}, skipping",
                        s=sym,
                    )
                    continue

                sid = self._series_id("cex_flow", sym)
                if self._row_exists(sid, today, conn):
                    continue

                payload = {
                    "token": sym,
                    "lookback_days": lookback_days,
                    "inflow_usd": r.get("inflow_usd"),
                    "outflow_usd": r.get("outflow_usd"),
                    "net_usd": net_usd,
                    "exchange_count": r.get("exchange_count"),
                    "direction": "accumulation" if net_usd > 0 else "distribution",
                    "query_id": qid,
                }
                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=net_usd,
                    raw_payload=payload,
                )
                inserted += 1
                seen.add(sym)

        log.info(
            "Dune cex_flow: {n} tokens inserted (of {r} rows)",
            n=inserted,
            r=len(rows),
        )
        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "tokens_seen": sorted(seen),
        }

    # ------------------------------------------------------------------ #
    # 3. Narrative heat -- new holder growth
    # ------------------------------------------------------------------ #

    def pull_narrative_heat(self) -> dict[str, Any]:
        """Pull tokens with the biggest w/w growth in unique new holders.

        Expects the configured ``DUNE_QUERY_NARRATIVE_HEAT`` saved query to
        return rows shaped like::

            { "token": "PEPE", "new_holders": 1234, "pct_change": 0.42,
              "prior_holders": 2900 }

        Returns:
            dict with status, rows_inserted, top_tokens (top 10 by pct_change).
        """
        qid = self.query_ids.get("narrative_heat", 0)
        if not self.api_key or qid <= 0:
            return {"status": "SKIPPED", "rows_inserted": 0, "reason": "not_configured"}

        try:
            rows = self._fetch_query_rows(qid, use_cache=True)
        except Exception as exc:  # noqa: BLE001
            log.error("Dune narrative_heat pull failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)}

        if not rows:
            log.warning("Dune narrative_heat: no rows returned for q={q}", q=qid)
            return {"status": "SUCCESS", "rows_inserted": 0}

        def _pct(r: dict[str, Any]) -> float:
            try:
                return float(r.get("pct_change") or 0.0)
            except (TypeError, ValueError):
                return 0.0

        sorted_rows = sorted(rows, key=_pct, reverse=True)
        top = sorted_rows[:_NARRATIVE_TOP_N]

        today = date.today()
        inserted = 0
        with self.engine.begin() as conn:
            # Per-token row.
            for r in sorted_rows:
                sym = str(r.get("token") or "").upper()
                if not sym:
                    continue
                pct = _pct(r)
                sid = self._series_id("holder_growth", sym)
                if self._row_exists(sid, today, conn):
                    continue
                payload = {
                    "token": sym,
                    "new_holders": r.get("new_holders"),
                    "prior_holders": r.get("prior_holders"),
                    "pct_change": pct,
                    "query_id": qid,
                }
                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=today,
                    value=pct,
                    raw_payload=payload,
                )
                inserted += 1

            # Global top-10 snapshot.
            sid_top = self._series_id("narrative_heat")
            if not self._row_exists(sid_top, today, conn):
                payload = {
                    "top": [
                        {
                            "token": str(r.get("token") or "").upper(),
                            "new_holders": r.get("new_holders"),
                            "pct_change": _pct(r),
                        }
                        for r in top
                    ],
                    "query_id": qid,
                }
                self._insert_raw(
                    conn=conn,
                    series_id=sid_top,
                    obs_date=today,
                    value=float(len(top)),
                    raw_payload=payload,
                )
                inserted += 1

        log.info(
            "Dune narrative_heat: {n} rows inserted (top {t} snapshot)",
            n=inserted,
            t=len(top),
        )
        return {
            "status": "SUCCESS",
            "rows_inserted": inserted,
            "top_tokens": [str(r.get("token") or "").upper() for r in top],
        }

    # ------------------------------------------------------------------ #
    # Combined pull
    # ------------------------------------------------------------------ #

    def pull_all(self) -> list[dict[str, Any]]:
        """Run every configured Dune pull in sequence.

        Returns:
            List of result dicts (one per category).
        """
        results: list[dict[str, Any]] = []
        log.info("Dune pull_all starting")

        results.append({"source": "smart_money", **self.pull_smart_money()})
        time.sleep(_RATE_LIMIT_DELAY)

        results.append({"source": "cex_flow", **self.pull_cex_flows()})
        time.sleep(_RATE_LIMIT_DELAY)

        results.append({"source": "narrative_heat", **self.pull_narrative_heat()})

        ok = sum(1 for r in results if r.get("status") == "SUCCESS")
        total = sum(int(r.get("rows_inserted", 0)) for r in results)
        log.info(
            "Dune pull_all complete -- {ok}/{n} sources SUCCESS, {r} rows",
            ok=ok,
            n=len(results),
            r=total,
        )
        return results


if __name__ == "__main__":
    from config import settings  # noqa: F401
    from db import get_engine

    puller = DunePuller(db_engine=get_engine())
    for r in puller.pull_all():
        log.info(
            "  {s}: {st} ({n} rows)",
            s=r.get("source", "?"),
            st=r.get("status", "?"),
            n=r.get("rows_inserted", 0),
        )
