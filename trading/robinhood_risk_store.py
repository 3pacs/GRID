"""
Persisted risk state for GRID's exchange connectors (drawdown high-water
mark, start-of-day equity, order-rate counter, and an idempotent order log).

Why this exists: ``trading/robinhood.py`` used to keep its drawdown
high-water mark on the ``RobinhoodCryptoTrader`` instance
(``self._high_water_mark``), and ``get_robinhood_trader()`` builds a fresh,
uncached instance on every call. Every request or script run therefore
restarted drawdown tracking from the current equity, so the halt could never
trip — see the audit notes on PR fix/robinhood-live-guards-20260924.

The fix is to move that state out of the Python object and into a store the
connector talks to. ``RobinhoodCryptoTrader`` never touches SQL directly:

* :class:`InMemoryRiskStore` is a process-local dict — the same "resets per
  instance" behaviour the connector had before this module existed. It is
  the default so the connector and its unit tests need no database.
* :class:`PostgresRiskStore` is the production store (see
  ``migrations/versions/robinhood_guards_20260924.py`` for the schema:
  ``trading_risk_state`` + ``trading_order_log``). Every process reads and
  writes the same row per venue, so the peak survives restarts and the
  factory's fresh-instance-per-call pattern. It does NOT create its own
  tables at runtime — the migration owns the schema, and a missing table
  raises (fails closed) rather than being silently bootstrapped under
  whatever role the app happens to run as.

Both implement the same small protocol (:class:`RiskStore`), so a caller
(``get_robinhood_trader()`` in production, a test fixture elsewhere) picks
the store and the connector code never branches on which one it has.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Protocol

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

#: Order-log statuses that mean "this decision was already acted on" for
#: idempotency purposes. A rejected/blocked attempt never got as far as a
#: real (dry-run or live) order, so it must not permanently burn the key —
#: the same automated decision can retry once whatever guard tripped clears.
_CONSUMED_STATUSES = ("dry_run", "submitted")


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class RiskState:
    """Snapshot of one venue's persisted risk counters."""

    venue: str
    peak_equity: float
    day_start_equity: float
    day_start_date: date
    orders_today: int


class RiskStore(Protocol):
    """What :class:`~trading.robinhood.RobinhoodCryptoTrader` needs from a
    risk-state backend. Duck-typed on purpose — no import of either
    implementation is required to satisfy it."""

    def touch(
        self, venue: str, equity: float, today: date, increment_order: bool = False,
    ) -> RiskState: ...

    def is_duplicate(self, venue: str, client_order_id: str) -> bool: ...

    def log_order(self, venue: str, client_order_id: str, **fields: Any) -> None: ...

    def get_state(self, venue: str) -> RiskState | None: ...

    def average_cost(self, venue: str, ticker: str) -> float | None: ...


# ---------------------------------------------------------------------------
# In-memory (default / test) store
# ---------------------------------------------------------------------------


class InMemoryRiskStore:
    """Process-local :class:`RiskStore` — NOT persisted.

    Default for every ``RobinhoodCryptoTrader`` that isn't explicitly wired
    to a database, so constructing a trader (or importing this module) never
    requires Postgres. ``get_robinhood_trader()`` is the one production call
    site that swaps in :class:`PostgresRiskStore` instead.
    """

    def __init__(self) -> None:
        self._state: dict[str, RiskState] = {}
        self._orders: dict[str, list[dict[str, Any]]] = {}
        #: Flat, insertion-ordered log of every log_order() call — backs
        #: average_cost() the same way a `created_at`-ordered SQL scan does
        #: for PostgresRiskStore.
        self._log: list[dict[str, Any]] = []

    def _roll(self, state: RiskState, equity: float, today: date) -> None:
        if today > state.day_start_date:
            state.day_start_date = today
            state.day_start_equity = equity
            state.orders_today = 0

    def touch(
        self, venue: str, equity: float, today: date, increment_order: bool = False,
    ) -> RiskState:
        state = self._state.get(venue)
        if state is None:
            state = RiskState(venue, equity, equity, today, 0)
            self._state[venue] = state
        else:
            self._roll(state, equity, today)
            if equity > state.peak_equity:
                state.peak_equity = equity
        if increment_order:
            state.orders_today += 1
        return RiskState(state.venue, state.peak_equity, state.day_start_equity,
                          state.day_start_date, state.orders_today)

    def is_duplicate(self, venue: str, client_order_id: str) -> bool:
        rows = self._orders.get(f"{venue}:{client_order_id}", [])
        return any(r.get("status") in _CONSUMED_STATUSES for r in rows)

    def log_order(self, venue: str, client_order_id: str, **fields: Any) -> None:
        key = f"{venue}:{client_order_id}"
        row = {"venue": venue, "client_order_id": client_order_id, **fields}
        self._orders.setdefault(key, []).append(row)
        self._log.append(row)

    def get_state(self, venue: str) -> RiskState | None:
        state = self._state.get(venue)
        if state is None:
            return None
        return RiskState(state.venue, state.peak_equity, state.day_start_equity,
                          state.day_start_date, state.orders_today)

    def average_cost(self, venue: str, ticker: str) -> float | None:
        """Volume-weighted average BUY fill price for *ticker* since the
        most recent SELL, from this store's own order log — a best-effort
        cost basis (Robinhood's API exposes none; see
        ``RobinhoodCryptoTrader._settle_wallet_pnl``)."""
        relevant = [r for r in self._log if r.get("venue") == venue and r.get("ticker") == ticker
                    and r.get("status") in _CONSUMED_STATUSES]
        total_qty = 0.0
        total_cost = 0.0
        for row in reversed(relevant):
            if row.get("side") == "sell":
                break
            qty = _safe_float(row.get("quantity"))
            price = _safe_float(row.get("fill_price"))
            if qty <= 0 or price <= 0:
                continue
            total_qty += qty
            total_cost += qty * price
        return (total_cost / total_qty) if total_qty > 0 else None


# ---------------------------------------------------------------------------
# Postgres (production) store
#
# Schema DDL lives ONLY in migrations/versions/robinhood_guards_20260924.py
# (the frozen, historical record of what ran) -- deliberately not duplicated
# or imported from here, so this module can never drift from what that
# migration actually applied. See PostgresRiskStore's docstring below.
# ---------------------------------------------------------------------------

# Single atomic upsert: refreshes the high-water mark, rolls the day (and
# resets the order counter) when `today` has moved on, and optionally counts
# this call as one more order placed today. `day_start_date < :today` (not
# `<>`) so clock skew that hands us an EARLIER date than what's stored can
# never roll the day backward -- it just keeps the later, already-stored day.
_TOUCH_SQL = """
    INSERT INTO trading_risk_state
        (venue, peak_equity, day_start_equity, day_start_date, orders_today, updated_at)
    VALUES (:venue, :equity, :equity, :today, :inc, NOW())
    ON CONFLICT (venue) DO UPDATE SET
        peak_equity = GREATEST(trading_risk_state.peak_equity, :equity),
        day_start_equity = CASE WHEN trading_risk_state.day_start_date < :today
                                 THEN :equity ELSE trading_risk_state.day_start_equity END,
        orders_today = CASE WHEN trading_risk_state.day_start_date < :today
                             THEN :inc ELSE trading_risk_state.orders_today + :inc END,
        day_start_date = CASE WHEN trading_risk_state.day_start_date < :today
                               THEN :today ELSE trading_risk_state.day_start_date END,
        updated_at = NOW()
    RETURNING peak_equity, day_start_equity, day_start_date, orders_today
"""

_IS_DUPLICATE_SQL = """
    SELECT 1 FROM trading_order_log
    WHERE venue = :venue AND client_order_id = :client_order_id
      AND status = ANY(:statuses)
    LIMIT 1
"""

_LOG_ORDER_SQL = """
    INSERT INTO trading_order_log
        (venue, client_order_id, wallet_id, ticker, side, direction, size_usd, quantity,
         bid, ask, mid, executable_price, spread_bps, spread_cost_usd, order_type, status,
         fill_price, guard_results, error, raw_response, simulated, created_at)
    VALUES
        (:venue, :client_order_id, :wallet_id, :ticker, :side, :direction, :size_usd, :quantity,
         :bid, :ask, :mid, :executable_price, :spread_bps, :spread_cost_usd, :order_type, :status,
         :fill_price, CAST(:guard_results AS JSONB), :error, CAST(:raw_response AS JSONB),
         :simulated, NOW())
"""

_GET_STATE_SQL = """
    SELECT peak_equity, day_start_equity, day_start_date, orders_today
    FROM trading_risk_state WHERE venue = :venue
"""

# Volume-weighted average BUY fill price since the most recent SELL for
# (venue, ticker) — see PostgresRiskStore.average_cost().
_AVERAGE_COST_SQL = """
    WITH last_sell AS (
        SELECT MAX(created_at) AS ts FROM trading_order_log
        WHERE venue = :venue AND ticker = :ticker AND side = 'sell'
          AND status = ANY(:statuses)
    )
    SELECT SUM(CAST(quantity AS DOUBLE PRECISION) * fill_price)
           / NULLIF(SUM(CAST(quantity AS DOUBLE PRECISION)), 0)
    FROM trading_order_log, last_sell
    WHERE venue = :venue AND ticker = :ticker AND side = 'buy'
      AND status = ANY(:statuses) AND fill_price IS NOT NULL AND quantity IS NOT NULL
      AND (last_sell.ts IS NULL OR trading_order_log.created_at > last_sell.ts)
"""

_ORDER_LOG_FIELDS = (
    "wallet_id", "ticker", "side", "direction", "size_usd", "quantity", "bid", "ask", "mid",
    "executable_price", "spread_bps", "spread_cost_usd", "order_type", "status", "fill_price",
    "error", "simulated",
)


def _json_or_none(value: Any) -> str | None:
    return json.dumps(value) if value is not None else None


class PostgresRiskStore:
    """Postgres-backed :class:`RiskStore`.

    Schema is owned entirely by
    ``migrations/versions/robinhood_guards_20260924.py`` (run by ``alembic
    upgrade head`` on every deploy — see deploy.yml). This class does NOT
    create tables at runtime — GRID has been bitten before by untracked
    runtime DDL, and a table created ad hoc under the application role can
    end up with the wrong owner/grants (see the GRANT-footer convention
    every real migration follows). If ``trading_risk_state`` /
    ``trading_order_log`` don't exist yet, every method below raises
    (a plain ``sqlalchemy.exc.ProgrammingError`` — undefined table) instead
    of silently creating them: orders must fail closed until the migration
    has actually been applied, not proceed against a schema nobody
    provisioned on purpose.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def touch(
        self, venue: str, equity: float, today: date, increment_order: bool = False,
    ) -> RiskState:
        with self.engine.begin() as conn:
            row = conn.execute(text(_TOUCH_SQL), {
                "venue": venue, "equity": float(equity), "today": today,
                "inc": 1 if increment_order else 0,
            }).fetchone()
        return RiskState(venue, row[0], row[1], row[2], row[3])

    def is_duplicate(self, venue: str, client_order_id: str) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(text(_IS_DUPLICATE_SQL), {
                "venue": venue, "client_order_id": client_order_id,
                "statuses": list(_CONSUMED_STATUSES),
            }).fetchone()
        return row is not None

    def log_order(self, venue: str, client_order_id: str, **fields: Any) -> None:
        params: dict[str, Any] = {"venue": venue, "client_order_id": client_order_id}
        for name in _ORDER_LOG_FIELDS:
            params[name] = fields.get(name)
        params["simulated"] = bool(params.get("simulated") or False)
        params["guard_results"] = _json_or_none(fields.get("guard_results"))
        params["raw_response"] = _json_or_none(fields.get("raw_response"))
        try:
            with self.engine.begin() as conn:
                conn.execute(text(_LOG_ORDER_SQL), params)
        except Exception as exc:  # noqa: BLE001 — audit logging must never block an order decision
            # Note: in practice this branch is unreachable for a genuinely
            # missing table -- touch() (called earlier in the same
            # open_position/close_position flow, via check_risk_limits())
            # already raised and blocked the order before log_order() could
            # ever be reached. This stays a warn-and-continue for any other
            # (non-schema) failure, so a logging hiccup never blocks an
            # already-decided order.
            log.warning("Robinhood order-log insert failed (venue={v}, key={k}): {e}",
                        v=venue, k=client_order_id, e=str(exc))

    def get_state(self, venue: str) -> RiskState | None:
        with self.engine.connect() as conn:
            row = conn.execute(text(_GET_STATE_SQL), {"venue": venue}).fetchone()
        if row is None:
            return None
        return RiskState(venue, row[0], row[1], row[2], row[3])

    def average_cost(self, venue: str, ticker: str) -> float | None:
        """Volume-weighted average BUY fill price for *ticker* since the
        most recent SELL — a best-effort cost basis from GRID's own order
        log (Robinhood's API exposes none; see
        ``RobinhoodCryptoTrader._settle_wallet_pnl``)."""
        with self.engine.connect() as conn:
            row = conn.execute(text(_AVERAGE_COST_SQL), {
                "venue": venue, "ticker": ticker, "statuses": list(_CONSUMED_STATUSES),
            }).fetchone()
        return float(row[0]) if row and row[0] is not None else None


def utc_today() -> date:
    """Today's date in UTC — the day boundary every persisted counter uses."""
    return datetime.now(timezone.utc).date()
