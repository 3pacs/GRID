"""
Paper Trading Signal Executor.

Runs periodically (hourly during market hours) to:
1. Check all ACTIVE paper strategies for fired signals
2. For each strategy: compare leader's latest daily return to threshold (1%)
3. If signal fires: compute Kelly position size, then reserve gross exposure and
   open the paper trade on the follower as one serialized step (advisory lock)
4. For EVERY open trade past its holding period — whatever its strategy's
   status (ACTIVE, PAUSED or KILLED) — close at current price
5. Log everything to decision journal for audit trail

An optional **venue tag** (``execute_signals(engine, venue="robinhood")``)
mirrors BUY signals to a real exchange connector on top of the paper trade.
Robinhood is crypto spot: long only, crypto tickers only, and dry-run until
``ROBINHOOD_LIVE_TRADING`` is set. The paper trade is opened either way — the
venue tag never changes what the paper book records.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from ingestion.market_calendar import last_trading_day
from store.feature_ids import feature_id
from trading.circuit_breaker import StrategyCircuitBreaker
from trading.paper_engine import (
    MAX_KELLY_FRACTION,
    MIN_TRADES_FOR_KELLY,
    PaperTradingEngine,
)


_SIGNAL_THRESHOLD = 0.01  # 1% daily return triggers a signal
_DEFAULT_POSITION_SIZE = 0.1  # 10% paper-only exploration size when < MIN_TRADES_FOR_KELLY
_DEFAULT_EXPECTED_LAG = 1  # days

#: Portfolio-wide gross exposure cap (sum of OPEN position_size fractions across all strategies).
MAX_GROSS_EXPOSURE: float = 1.0

#: One exposure reservation at a time (Codex review 6011903757). A scheduled
#: run (grid-scheduler) and a manual POST /execute-signals (grid-api) are
#: separate processes: reading total exposure once and capping outside the
#: insert let both read 0.90, each add 0.10 and breach MAX_GROSS_EXPOSURE, or
#: both open the same strategy's entry. The insert transaction takes this
#: transaction-scoped lock, then re-checks the open entry and re-reads the
#: exposure, so an overlapping run waits for the first to commit and then sees
#: its entry. Pattern: ingestion/altdata/cboe_indices.py (#1397).
EXPOSURE_LOCK_KEY: str = "trading.signal_executor.gross_exposure"
_EXPOSURE_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))")
_EXPOSURE_LOCK_TIMEOUT = text("SET LOCAL lock_timeout = '30s'")

#: Fail-closed reason for refusing Robinhood mirror opens until close mirroring exists.
ROBINHOOD_MIRROR_DISABLED_REASON = "robinhood mirror disabled: close mirroring not implemented"

#: Venues the executor can mirror signals to. Paper-only when unset.
SUPPORTED_VENUES: tuple[str, ...] = ("robinhood",)

#: Below this notional an exchange order is not worth sending.
_MIN_VENUE_ORDER_USD = 1.0


def _get_latest_prices(
    conn, feature_name: str, n: int = 2, *, as_of: date | None = None
) -> list[tuple[date, float]]:
    """Get the latest n PIT-eligible daily prices for a feature.

    Enforces point-in-time bounds (``obs_date``, ``release_date`` and
    ``vintage_date`` all ``<= as_of``: a row is knowable at the later of its
    release and vintage dates, as main's vintage-fence guard requires) and
    excludes retracted vintages via ``resolved_series_retractions``. One row
    per ``obs_date``: the newest eligible vintage.

    The id is bound rather than matched through a feature_registry name
    join: the active strategies' features are sector ETF panels (xle_full
    286k rows), which the join read in full on every call (377 ms on griddb,
    2026-11-23; < 1 ms bound).
    """
    from store.pit import retraction_cutoff

    as_of_date = as_of or date.today()
    fid = feature_id(conn, feature_name)
    if fid is None:
        return []
    rows = conn.execute(text(
        "SELECT DISTINCT ON (obs_date) obs_date, value "
        "FROM resolved_series rs "
        "WHERE rs.feature_id = :fid "
        "  AND rs.obs_date <= :as_of "
        "  AND rs.release_date <= :as_of "
        "  AND rs.vintage_date <= :as_of "
        "  AND NOT EXISTS ("
        "      SELECT 1 FROM resolved_series_retractions rr "
        "      WHERE rr.feature_id = rs.feature_id "
        "        AND rr.obs_date = rs.obs_date "
        "        AND rr.vintage_date = rs.vintage_date "
        "        AND rr.retracted_at <= :retraction_cutoff"
        "  ) "
        "ORDER BY obs_date DESC, vintage_date DESC "
        "LIMIT :n"
    ), {
        "fid": fid,
        "as_of": as_of_date,
        "retraction_cutoff": retraction_cutoff(as_of_date),
        "n": n,
    }).fetchall()
    out: list[tuple[date, float]] = []
    for r in rows:
        obs = r[0].date() if isinstance(r[0], datetime) else r[0]
        if obs is None or r[1] is None:
            continue
        val = float(r[1])
        if not math.isfinite(val):
            continue
        out.append((obs, val))
    return out


def _is_price_fresh(obs_date: date | datetime | None, as_of: date | None = None) -> bool:
    """Return True iff *obs_date* is on or after the previous trading session and <= *as_of*."""
    if obs_date is None:
        return False
    if isinstance(obs_date, datetime):
        obs_date = obs_date.date()
    ref = as_of or date.today()
    if obs_date > ref:
        return False
    prev_session = last_trading_day(ref - timedelta(days=1))
    return obs_date >= prev_session


def _get_expected_lag(conn, hypothesis_id: int | None) -> int:
    """Get expected_lag from hypothesis_registry's lag_structure JSON."""
    if hypothesis_id is None:
        return _DEFAULT_EXPECTED_LAG
    row = conn.execute(text(
        "SELECT lag_structure FROM hypothesis_registry WHERE id = :hid"
    ), {"hid": hypothesis_id}).fetchone()
    if not row or not row[0]:
        return _DEFAULT_EXPECTED_LAG
    lag = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    return lag.get("expected_lag", _DEFAULT_EXPECTED_LAG)


def resolve_hold_days(
    strategy_horizon_days: int | None, expected_lag: int | None
) -> int:
    """Holding period for auto-close (LEVER-PACKAGE §7 T2.5).

    The strategy's own ``horizon_days`` wins when set; otherwise the
    hypothesis ``expected_lag``; otherwise the legacy 1-day default. Always
    at least 1 so a trade is never closed on its entry day.
    """
    for candidate in (strategy_horizon_days, expected_lag):
        if candidate is None:
            continue
        try:
            value = int(candidate)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return _DEFAULT_EXPECTED_LAG


class VenueTag:
    """Routes a paper BUY signal to a real exchange connector as well.

    Constraints the tag enforces before anything reaches the venue:

    * **Long only.** Robinhood crypto is spot — a SHORT signal is dropped
      rather than turned into a sell of an unrelated holding.
    * **Tradable tickers only.** The follower's feature name has to resolve to
      an asset the venue reports tradable (``trading_pairs``); ``sp500_close``
      never leaves the paper book.
    * **Wallet-sized.** Notional is the wallet's current capital times the
      Kelly fraction, capped by the connector's own per-order cap. A wallet
      that is not ACTIVE (the auto-kill fired) routes nothing.

    The connector's dry-run flag is the last gate: with
    ``ROBINHOOD_LIVE_TRADING`` false every order comes back ``status:
    dry_run`` having been built and risk-checked but not sent.
    """

    def __init__(
        self,
        venue: str,
        trader: Any,
        wallet: dict | None = None,
        min_order_usd: float = _MIN_VENUE_ORDER_USD,
    ) -> None:
        self.venue = venue
        self.trader = trader
        self.wallet = wallet or {}
        self.min_order_usd = float(min_order_usd)

    @property
    def wallet_id(self) -> str | None:
        return self.wallet.get("id")

    @property
    def capital(self) -> float:
        """Capital the venue may allocate — the wallet's, else the order cap."""
        capital = self.wallet.get("current_capital")
        if capital is None:
            return float(self.trader.max_position_usd)
        return float(capital)

    def order_size_usd(self, position_size: float) -> float:
        """Kelly fraction of wallet capital, capped by the per-order limit."""
        raw = self.capital * float(position_size)
        return round(min(raw, float(self.trader.max_position_usd)), 2)

    def submit(
        self,
        ticker: str,
        direction: str,
        position_size: float,
        decision_id: str | None = None,
        *,
        is_exploration: bool = False,
    ) -> dict | None:
        """Send the signal to the venue. ``None`` when nothing was routed.

        Fails closed for ``robinhood`` because automatic close mirroring is not
        implemented, and never mirrors exploration-sized trades.

        *decision_id* (e.g. ``f"{strategy_id}-{trade_id}"``), when given,
        derives a deterministic idempotency key (see
        ``trading.robinhood.deterministic_client_order_id``) so re-running
        the same signal cycle for a decision that already went through is
        recognized as a duplicate rather than resubmitted. Omitted, the
        connector generates a fresh key each call and does not deduplicate.
        """
        if (self.venue or "").strip().lower() == "robinhood":
            log.error(
                "Venue {v}: refusing OPEN for {t} — {r}",
                v=self.venue, t=ticker, r=ROBINHOOD_MIRROR_DISABLED_REASON,
            )
            return {
                "venue": self.venue,
                "wallet_id": self.wallet_id,
                "asset": ticker,
                "size_usd": 0.0,
                "status": "disabled",
                "error": ROBINHOOD_MIRROR_DISABLED_REASON,
                "result": {
                    "status": "disabled",
                    "error": ROBINHOOD_MIRROR_DISABLED_REASON,
                },
            }
        if is_exploration:
            log.info(
                "Venue {v}: {t} {d} skipped — exploration-sized trade is paper only",
                v=self.venue, t=ticker, d=direction,
            )
            return None
        if direction != "LONG":
            log.debug("Venue {v}: {t} {d} skipped — spot venue is long only",
                      v=self.venue, t=ticker, d=direction)
            return None
        try:
            tradable = self.trader.is_tradable(ticker)
        except Exception as exc:  # noqa: BLE001 — venue lookup failure is not a signal failure
            log.warning("Venue {v}: tradable check for {t} failed: {e}",
                        v=self.venue, t=ticker, e=str(exc))
            return None
        if not tradable:
            log.debug("Venue {v}: {t} is not a tradable pair", v=self.venue, t=ticker)
            return None

        from trading.robinhood import crypto_asset_code, deterministic_client_order_id

        size_usd = self.order_size_usd(position_size)
        if size_usd < self.min_order_usd:
            log.debug("Venue {v}: ${s:.2f} for {t} below the ${m:.2f} minimum",
                      v=self.venue, s=size_usd, t=ticker, m=self.min_order_usd)
            return None

        asset = crypto_asset_code(ticker) or ticker
        client_order_id = (
            deterministic_client_order_id(self.venue, decision_id, asset, "buy", date.today())
            if decision_id is not None else None
        )
        try:
            result = self.trader.open_position(ticker=asset, direction="LONG", size_usd=size_usd,
                                               client_order_id=client_order_id)
        except Exception as exc:  # noqa: BLE001 — the paper book must not care
            log.warning("Venue {v}: {a} order failed: {e}", v=self.venue, a=asset, e=str(exc))
            result = {"error": str(exc)}
        log.info("Venue {v}: {a} LONG ${s:.2f} -> {st}",
                 v=self.venue, a=asset, s=size_usd,
                 st=result.get("status") or result.get("error"))
        return {
            "venue": self.venue,
            "wallet_id": self.wallet_id,
            "asset": asset,
            "size_usd": size_usd,
            "result": result,
        }


def build_venue_tag(
    engine: Engine,
    venue: str | None,
    wallet_id: str | None = None,
) -> VenueTag | None:
    """Build the venue tag for *venue*, or ``None`` for the paper-only path.

    Raises ValueError on an unknown venue — a typo must not silently degrade
    into "paper only" when the caller asked for a live venue.
    """
    if not venue:
        return None
    venue = venue.strip().lower()
    if venue not in SUPPORTED_VENUES:
        raise ValueError(f"Unsupported venue {venue!r}. Known venues: {', '.join(SUPPORTED_VENUES)}")
    if venue == "robinhood":
        raise ValueError(ROBINHOOD_MIRROR_DISABLED_REASON)

    from trading.robinhood import get_robinhood_trader

    trader = get_robinhood_trader()
    if not trader.configured:
        log.warning("Venue {v} requested but credentials are not configured — paper only", v=venue)
        return None

    wallet: dict | None = None
    if wallet_id:
        from trading.wallet_manager import WalletManager

        wallet = WalletManager(engine).get_wallet(wallet_id)
        if "error" in wallet:
            log.warning("Venue {v}: wallet {w} not found — paper only", v=venue, w=wallet_id)
            return None
        if wallet.get("status") != "ACTIVE":
            log.warning("Venue {v}: wallet {w} is {s} — paper only",
                        v=venue, w=wallet_id, s=wallet.get("status"))
            return None
    else:
        # No wallet_id given used to mean "no wallet gate at all" here —
        # VenueTag.capital then fell back to the connector's bare per-order
        # cap instead of a real balance, and nothing blocked a KILLED/absent
        # wallet. RobinhoodCryptoTrader's own wallet_lookup (wired by
        # get_robinhood_trader()) is the authoritative gate either way, but
        # resolving the ACTIVE wallet here too fixes sizing and makes the
        # gate visible at this layer as well.
        from trading.wallet_manager import WalletManager

        wallets = WalletManager(engine).get_all_wallets(exchange=venue, status="ACTIVE")
        if not wallets:
            log.warning("Venue {v}: no ACTIVE wallet for this exchange — paper only", v=venue)
            return None
        wallet = wallets[0]

    log.info("Venue tag active: {v} mode={m} wallet={w}",
             v=venue, m=trader.mode, w=wallet.get("id"))
    return VenueTag(venue, trader, wallet)


def _compute_position_size(
    engine_obj: PaperTradingEngine, conn, strategy_id: str
) -> tuple[float, bool]:
    """Compute position size from closed trades using Kelly criterion.

    Returns ``(size, is_exploration)``:
    - Fewer than ``MIN_TRADES_FOR_KELLY`` closed trades → ``(_DEFAULT_POSITION_SIZE, True)``
      (exploration sizing, paper-only).
    - ``>= MIN_TRADES_FOR_KELLY`` closed trades → half-Kelly fraction (or ``0.0`` when
      edge is non-positive or there are zero wins) with ``is_exploration=False``.
    """
    closed = conn.execute(text(
        "SELECT pnl_pct FROM paper_trades "
        "WHERE strategy_id = :sid AND status = 'CLOSED'"
    ), {"sid": strategy_id}).fetchall()

    pnls = [
        float(r[0]) for r in closed
        if r[0] is not None and math.isfinite(float(r[0]))
    ]
    if len(pnls) < MIN_TRADES_FOR_KELLY:
        return _DEFAULT_POSITION_SIZE, True

    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    if not wins:
        return 0.0, False

    win_rate = len(wins) / len(pnls)
    avg_win = sum(wins) / len(wins)
    avg_loss = sum(losses) / len(losses) if losses else 0.0

    size = engine_obj.kelly_position_size(win_rate, avg_win, avg_loss)
    return (size if size > 0 else 0.0), False


def _compute_kelly_size(engine_obj: PaperTradingEngine, conn, strategy_id: str) -> float:
    """Compute position size from closed trades using Kelly criterion.

    Returns ``_DEFAULT_POSITION_SIZE`` only when fewer than
    ``MIN_TRADES_FOR_KELLY`` closed trades exist; returns ``0.0`` for
    non-positive Kelly or zero wins once ``MIN_TRADES_FOR_KELLY`` is reached.
    """
    size, _is_exploration = _compute_position_size(engine_obj, conn, strategy_id)
    return size


def _get_open_gross_exposure(conn) -> float:
    """Return total open gross exposure (sum of position_size across all OPEN trades)."""
    row = conn.execute(text(
        "SELECT COALESCE(SUM(ABS(position_size)), 0.0) "
        "FROM paper_trades WHERE status = 'OPEN'"
    )).fetchone()
    if not row or row[0] is None:
        return 0.0
    val = float(row[0])
    return val if math.isfinite(val) else float("inf")


def _reserve_and_open_trade(
    conn: Connection,
    pe: PaperTradingEngine,
    *,
    strategy_id: str,
    ticker: str,
    direction: str,
    entry_price: float,
    position_size: float,
    signal_strength: float,
    hypothesis_id: int | None,
) -> tuple[int, str | None, float]:
    """Reserve gross exposure and insert the entry as one serialized step.

    Runs on *conn* inside the caller's open transaction (``engine.begin()``).
    Takes the executor's transaction-scoped advisory lock, then, with the lock
    held, re-checks that the strategy has no OPEN entry, re-reads the
    portfolio's gross exposure, applies ``MAX_GROSS_EXPOSURE`` and only then
    inserts on the same connection. The lock is released when the caller's
    transaction commits or rolls back. Under PostgreSQL's default READ
    COMMITTED each statement after the lock sees what an earlier run
    committed, so a second run waits here and then sees the first's entry and
    exposure. A lock wait past ``lock_timeout`` raises (fail closed).

    Returns ``(trade_id, skip_reason, gross_exposure_under_lock)``. When
    nothing was inserted ``trade_id`` is ``-1`` and ``skip_reason`` is
    ``open_entry_exists``, ``max_gross_exposure`` or ``strategy_not_active``.
    """
    conn.execute(_EXPOSURE_LOCK_TIMEOUT)
    conn.execute(_EXPOSURE_LOCK, {"key": EXPOSURE_LOCK_KEY})

    open_entry = conn.execute(text(
        "SELECT id FROM paper_trades "
        "WHERE strategy_id = :sid AND status = 'OPEN' LIMIT 1"
    ), {"sid": strategy_id}).fetchone()
    gross = _get_open_gross_exposure(conn)
    if open_entry:
        return -1, "open_entry_exists", gross
    if gross + position_size > MAX_GROSS_EXPOSURE + 1e-9:
        return -1, "max_gross_exposure", gross

    trade_id = pe.open_trade(
        strategy_id=strategy_id,
        ticker=ticker,
        direction=direction,
        entry_price=entry_price,
        position_size=position_size,
        signal_strength=signal_strength,
        hypothesis_id=hypothesis_id,
        threshold_used=_SIGNAL_THRESHOLD,
        conn=conn,
    )
    if trade_id <= 0:
        return -1, "strategy_not_active", gross
    return trade_id, None, gross


def _list_open_positions(conn: Connection) -> list[Any]:
    """Every OPEN paper trade with its strategy's status and holding inputs.

    Not filtered on strategy status (Codex review 6011903757): the exit pass
    runs for ACTIVE, PAUSED and KILLED strategies alike. A trade whose
    strategy row is missing comes back with ``status`` NULL.
    Rows: ``(trade_id, strategy_id, ticker, entry_date, strategy_status,
    hypothesis_id, follower, horizon_days)``.
    """
    return conn.execute(text(
        "SELECT pt.id, pt.strategy_id, pt.ticker, pt.entry_date, "
        "ps.status, ps.hypothesis_id, ps.follower, ps.horizon_days "
        "FROM paper_trades pt "
        "LEFT JOIN paper_strategies ps ON ps.id = pt.strategy_id "
        "WHERE pt.status = 'OPEN' "
        "ORDER BY pt.id"
    )).fetchall()


def execute_signals(
    engine: Engine,
    venue: str | None = None,
    venue_wallet_id: str | None = None,
) -> dict:
    """Execute paper trading signals for all ACTIVE strategies.

    *venue* optionally mirrors BUY signals to an exchange connector on top of
    the paper trade (see :class:`VenueTag`); *venue_wallet_id* names the
    ``trading_wallets`` row that sizes and risk-gates those orders.
    ``venue="robinhood"`` fails closed because close mirroring is not implemented.

    Returns summary dict with signals_checked, trades_opened, trades_closed, etc.
    """
    if venue and venue.strip().lower() == "robinhood":
        log.error(
            "Signal executor refusing venue=robinhood: {r}",
            r=ROBINHOOD_MIRROR_DISABLED_REASON,
        )
        return {
            "error": ROBINHOOD_MIRROR_DISABLED_REASON,
            "signals_checked": 0,
            "trades_opened": 0,
            "trades_closed": 0,
            "strategies_killed": 0,
            "strategies_halted": 0,
            "venue": "robinhood",
            "venue_mode": None,
            "venue_orders": [],
            "details": [],
        }

    pe = PaperTradingEngine(engine)
    breaker = StrategyCircuitBreaker(engine)
    today = date.today()
    venue_tag = build_venue_tag(engine, venue, venue_wallet_id)

    details: list[dict] = []
    venue_orders: list[dict] = []
    signals_checked = 0
    trades_opened = 0
    trades_closed = 0
    strategies_killed = 0
    strategies_halted = 0

    # ------------------------------------------------------------------
    # 1. Load all ACTIVE strategies (entry candidates) and a snapshot of
    #    portfolio gross exposure. The snapshot only gives an early skip;
    #    the authoritative cap check is the re-read under the exposure lock.
    # ------------------------------------------------------------------
    with engine.connect() as conn:
        strategies = conn.execute(text(
            "SELECT id, hypothesis_id, leader, follower, horizon_days "
            "FROM paper_strategies WHERE status = 'ACTIVE' ORDER BY id"
        )).fetchall()
        gross_exposure = _get_open_gross_exposure(conn)

    log.info(
        "Signal executor: checking {n} active strategies (open gross exposure={g:.3f})",
        n=len(strategies), g=gross_exposure,
    )

    entry_errors: dict[str, Exception] = {}
    breaker_updates: list[str] = []  # strategies whose breaker this run may update

    for strat in strategies:
        strategy_id, hypothesis_id, leader, follower, strategy_horizon_days = strat
        signals_checked += 1

        # Circuit breaker check — gates ENTRIES only; exits always run.
        allow_entries = True
        breaker_ok = True
        try:
            if not breaker.should_execute(strategy_id):
                allow_entries = False
                strategies_halted += 1
                last_err = getattr(breaker, "last_error", None)
                if isinstance(last_err, str) and last_err:
                    breaker_ok = False
                    details.append({
                        "action": "HALTED",
                        "strategy_id": strategy_id,
                        "reason": "circuit_breaker_error",
                        "error": last_err,
                    })
                else:
                    details.append({
                        "action": "HALTED",
                        "strategy_id": strategy_id,
                        "reason": "circuit breaker OPEN",
                    })
        except Exception as exc:  # noqa: BLE001 — missing table/DB error must not stop closes
            allow_entries = False
            breaker_ok = False
            strategies_halted += 1
            log.error(
                "Circuit breaker check failed for strategy {s} — skipping entries, exits still run: {e}",
                s=strategy_id, e=str(exc),
            )
            details.append({
                "action": "HALTED",
                "strategy_id": strategy_id,
                "reason": "circuit_breaker_error",
                "error": str(exc),
            })

        if allow_entries and breaker_ok:
            breaker_updates.append(strategy_id)

        # ------------------------------------------------------------------
        # 2-5. Entry signal evaluation (skipped if breaker halted/errored)
        # ------------------------------------------------------------------
        if allow_entries:
            try:
                with engine.connect() as conn:
                    leader_prices = _get_latest_prices(conn, leader, 2)
                    if len(leader_prices) < 2:
                        log.debug(
                            "Strategy {s}: leader {l} has < 2 prices, skipping entry",
                            s=strategy_id, l=leader,
                        )
                    else:
                        leader_obs_date, price_today = leader_prices[0]
                        price_yesterday = leader_prices[1][1]
                        if not _is_price_fresh(leader_obs_date, today):
                            log.warning(
                                "Strategy {s}: leader {l} price dated {d} is stale (as_of={t}), skipping entry",
                                s=strategy_id, l=leader, d=leader_obs_date, t=today,
                            )
                            details.append({
                                "action": "SKIP",
                                "strategy_id": strategy_id,
                                "ticker": follower,
                                "feature": leader,
                                "obs_date": str(leader_obs_date),
                                "reason": "stale_price",
                            })
                        elif (
                            math.isfinite(price_today)
                            and math.isfinite(price_yesterday)
                            and price_yesterday != 0
                        ):
                            leader_return = (price_today - price_yesterday) / price_yesterday

                            if abs(leader_return) > _SIGNAL_THRESHOLD:
                                direction = "LONG" if leader_return > 0 else "SHORT"

                                follower_prices = _get_latest_prices(conn, follower, 1)
                                if not follower_prices:
                                    log.debug(
                                        "Strategy {s}: no follower price for {f}",
                                        s=strategy_id, f=follower,
                                    )
                                elif not _is_price_fresh(follower_prices[0][0], today):
                                    log.warning(
                                        "Strategy {s}: follower {f} entry price dated {d} is stale "
                                        "(as_of={t}), skipping entry",
                                        s=strategy_id, f=follower,
                                        d=follower_prices[0][0], t=today,
                                    )
                                    details.append({
                                        "action": "SKIP",
                                        "strategy_id": strategy_id,
                                        "ticker": follower,
                                        "direction": direction,
                                        "obs_date": str(follower_prices[0][0]),
                                        "reason": "stale_price",
                                    })
                                elif not math.isfinite(follower_prices[0][1]) or follower_prices[0][1] <= 0:
                                    pass  # invalid price, skip opening
                                else:
                                    entry_price = follower_prices[0][1]

                                    # Check no existing OPEN trade for this strategy
                                    open_trade = conn.execute(text(
                                        "SELECT id FROM paper_trades "
                                        "WHERE strategy_id = :sid AND status = 'OPEN' LIMIT 1"
                                    ), {"sid": strategy_id}).fetchone()

                                    if not open_trade:
                                        position_size, is_exploration = _compute_position_size(
                                            pe, conn, strategy_id
                                        )
                                        if position_size <= 0:
                                            log.info(
                                                "Strategy {s}: non-positive Kelly sizing, skipping entry",
                                                s=strategy_id,
                                            )
                                            details.append({
                                                "action": "SKIP",
                                                "strategy_id": strategy_id,
                                                "ticker": follower,
                                                "direction": direction,
                                                "reason": "non_positive_kelly",
                                            })
                                        else:
                                            signal_strength = abs(leader_return)

                                            # Convergence adjustment — only for Kelly-validated
                                            # strategies (never exploration), capped at 2x Kelly
                                            # and MAX_KELLY_FRACTION.
                                            if not is_exploration:
                                                try:
                                                    from intelligence.trust_scorer import detect_convergence
                                                    convergence = detect_convergence(engine, ticker=follower)
                                                    trade_dir = "BUY" if direction == "LONG" else "SELL"
                                                    for evt in (convergence or []):
                                                        if evt.get("signal_type") == trade_dir:
                                                            src_count = evt.get("source_count") or 0
                                                            combined_conf = evt.get("combined_confidence")
                                                            if combined_conf is None:
                                                                combined_conf = 0.5
                                                            if src_count >= 3 and combined_conf > 0:
                                                                convergence_mult = (
                                                                    1.0 + 0.15 * (src_count - 2) * combined_conf
                                                                )
                                                                position_size = min(
                                                                    position_size * convergence_mult,
                                                                    position_size * 2.0,
                                                                    MAX_KELLY_FRACTION,
                                                                )
                                                                signal_strength *= convergence_mult
                                                                log.info(
                                                                    "Convergence boost: {t} {d} — {n} sources, "
                                                                    "conf={c:.2f}, size={s:.3f}",
                                                                    t=follower, d=trade_dir, n=src_count,
                                                                    c=combined_conf, s=position_size,
                                                                )
                                                            break
                                                except Exception:
                                                    pass  # Convergence is optional — degrade gracefully

                                            if gross_exposure + position_size > MAX_GROSS_EXPOSURE + 1e-9:
                                                # Early skip on this run's snapshot; the re-read under the
                                                # exposure lock below is the authoritative check.
                                                log.info(
                                                    "Strategy {s}: gross exposure {g:.3f} + {p:.3f} "
                                                    "exceeds cap {cap:.2f}, skipping entry",
                                                    s=strategy_id, g=gross_exposure,
                                                    p=position_size, cap=MAX_GROSS_EXPOSURE,
                                                )
                                                details.append({
                                                    "action": "SKIP",
                                                    "strategy_id": strategy_id,
                                                    "ticker": follower,
                                                    "direction": direction,
                                                    "reason": "max_gross_exposure",
                                                    "current_gross_exposure": round(gross_exposure, 4),
                                                    "proposed_position_size": round(position_size, 4),
                                                })
                                            else:
                                                # Exposure reservation + insert: one transaction under the
                                                # executor's advisory lock (Codex review 6011903757).
                                                with engine.begin() as tx:
                                                    trade_id, reserve_skip, gross_exposure = _reserve_and_open_trade(
                                                        tx, pe,
                                                        strategy_id=strategy_id,
                                                        ticker=follower,
                                                        direction=direction,
                                                        entry_price=entry_price,
                                                        position_size=position_size,
                                                        signal_strength=signal_strength,
                                                        hypothesis_id=hypothesis_id,
                                                    )
                                                if reserve_skip is not None:
                                                    log.info(
                                                        "Strategy {s}: entry {p:.3f} refused under the exposure lock "
                                                        "({r}, gross exposure {g:.3f}, cap {cap:.2f})",
                                                        s=strategy_id, p=position_size, r=reserve_skip,
                                                        g=gross_exposure, cap=MAX_GROSS_EXPOSURE,
                                                    )
                                                    skip = {
                                                        "action": "SKIP",
                                                        "strategy_id": strategy_id,
                                                        "ticker": follower,
                                                        "direction": direction,
                                                        "reason": reserve_skip,
                                                    }
                                                    if reserve_skip == "max_gross_exposure":
                                                        skip["current_gross_exposure"] = round(gross_exposure, 4)
                                                        skip["proposed_position_size"] = round(position_size, 4)
                                                    details.append(skip)

                                                if trade_id > 0:
                                                    trades_opened += 1
                                                    gross_exposure += position_size
                                                    detail = {
                                                        "action": "OPEN",
                                                        "strategy_id": strategy_id,
                                                        "trade_id": trade_id,
                                                        "direction": direction,
                                                        "ticker": follower,
                                                        "entry_price": entry_price,
                                                        "position_size": round(position_size, 4),
                                                        "signal_strength": round(signal_strength, 4),
                                                        "leader_return": round(leader_return, 4),
                                                        "is_exploration": is_exploration,
                                                    }
                                                    if venue_tag is not None:
                                                        if is_exploration:
                                                            log.info(
                                                                "Venue {v}: skipping mirror for {s} ({t}) "
                                                                "— exploration-sized trade is paper only",
                                                                v=venue_tag.venue, s=strategy_id, t=follower,
                                                            )
                                                        else:
                                                            order = venue_tag.submit(
                                                                follower, direction, position_size,
                                                                decision_id=f"{strategy_id}-{trade_id}",
                                                                is_exploration=is_exploration,
                                                            )
                                                            if order:
                                                                detail["venue_order"] = order
                                                                if "error" not in order:
                                                                    venue_orders.append({
                                                                        "strategy_id": strategy_id,
                                                                        "trade_id": trade_id,
                                                                        **order,
                                                                    })
                                                    details.append(detail)
                                    else:
                                        log.debug(
                                            "Strategy {s}: already has open trade #{t}, skipping open",
                                            s=strategy_id, t=open_trade[0],
                                        )
            except Exception as exc:  # noqa: BLE001 — entry failure must not block closes
                entry_errors[strategy_id] = exc
                log.warning("Strategy {s} entry check failed: {e}", s=strategy_id, e=str(exc))
                details.append({
                    "action": "ERROR",
                    "strategy_id": strategy_id,
                    "error": str(exc),
                })

    # ------------------------------------------------------------------
    # 6. Close trades past the holding period, for EVERY open position
    #    (Codex review 6011903757): listed from paper_trades, not from the
    #    ACTIVE strategies above, so a PAUSED or KILLED strategy's open trade
    #    is still closed. ALWAYS runs, even when the circuit breaker is
    #    OPEN/errored or leader data is missing/stale. Strategy horizon_days
    #    beats the hypothesis expected_lag so long-horizon paper trades are
    #    not closed next day. The exit price is the strategy follower's.
    # ------------------------------------------------------------------
    close_errors: dict[str, Exception] = {}
    listing_error: Exception | None = None
    positions: list[Any] = []
    try:
        with engine.connect() as conn:
            positions = _list_open_positions(conn)
    except Exception as exc:  # noqa: BLE001
        listing_error = exc
        log.warning("Open-position listing for the exit pass failed: {e}", e=str(exc))
        details.append({"action": "ERROR", "stage": "exit_pass", "error": str(exc)})

    exits_by_strategy: dict[str, tuple] = {}
    for (trade_id, strategy_id, ticker, entry_date, strategy_status,
         hypothesis_id, follower, strategy_horizon_days) in positions:
        if strategy_status is None:
            log.warning(
                "Trade #{id} ({t}) has no paper_strategies row {s}: no holding rule, kept open",
                id=trade_id, t=ticker, s=strategy_id,
            )
            details.append({
                "action": "SKIP_CLOSE",
                "strategy_id": strategy_id,
                "trade_id": trade_id,
                "ticker": ticker,
                "reason": "strategy_missing",
            })
            continue
        exits_by_strategy.setdefault(
            strategy_id,
            (strategy_status, hypothesis_id, follower, strategy_horizon_days, []),
        )[4].append((trade_id, ticker, entry_date))

    for strategy_id, exit_group in exits_by_strategy.items():
        strategy_status, hypothesis_id, follower, strategy_horizon_days, open_trades = exit_group
        try:
            with engine.connect() as conn:
                expected_lag = resolve_hold_days(
                    strategy_horizon_days, _get_expected_lag(conn, hypothesis_id)
                )

                for ot in open_trades:
                    trade_id, ticker, entry_date = ot[0], ot[1], ot[2]
                    if entry_date + timedelta(days=expected_lag) <= today:
                        # Get follower's current price for exit
                        exit_prices = _get_latest_prices(conn, follower, 1)
                        if not exit_prices:
                            log.debug("No exit price for {f}, skipping close", f=follower)
                            continue
                        exit_obs_date, exit_price = exit_prices[0]
                        if not _is_price_fresh(exit_obs_date, today):
                            log.warning(
                                "Strategy {s}: trade #{id} exit price for {f} dated {d} is stale "
                                "(as_of={t}) — keeping trade open",
                                s=strategy_id, id=trade_id, f=follower, d=exit_obs_date, t=today,
                            )
                            details.append({
                                "action": "SKIP_CLOSE",
                                "strategy_id": strategy_id,
                                "trade_id": trade_id,
                                "ticker": ticker,
                                "obs_date": str(exit_obs_date),
                                "reason": "stale_price",
                            })
                            continue
                        if not math.isfinite(exit_price) or exit_price <= 0:
                            continue

                        result = pe.close_trade(
                            trade_id=trade_id,
                            exit_price=exit_price,
                            notes=f"Auto-close after {expected_lag}d lag",
                        )

                        if "error" not in result:
                            trades_closed += 1
                            details.append({
                                "action": "CLOSE",
                                "strategy_id": strategy_id,
                                "strategy_status": strategy_status,
                                "trade_id": trade_id,
                                "ticker": ticker,
                                "exit_price": exit_price,
                                "pnl": result.get("pnl"),
                                "pnl_pct": result.get("pnl_pct"),
                            })
        except Exception as exc:  # noqa: BLE001
            close_errors[strategy_id] = exc
            log.warning("Strategy {s} auto-close check failed: {e}", s=strategy_id, e=str(exc))
            details.append({
                "action": "ERROR",
                "strategy_id": strategy_id,
                "error": str(exc),
            })

    # Update breakers only for strategies whose entries were permitted and
    # whose breaker table was usable (non-ACTIVE strategies are never here).
    for strategy_id in breaker_updates:
        err = entry_errors.get(strategy_id) or close_errors.get(strategy_id) or listing_error
        try:
            if err is not None:
                breaker.record_failure(strategy_id, str(err))
            else:
                breaker.record_success(strategy_id)
        except Exception as bexc:  # noqa: BLE001
            log.error(
                "Circuit breaker state update failed for strategy {s}: {e}",
                s=strategy_id, e=str(bexc),
            )

    # ------------------------------------------------------------------
    # 7. Count strategies killed during this run (from close_trade). Only
    #    strategies ACTIVE at the start count: closing an already-KILLED
    #    strategy's trade also bumps its updated_at.
    # ------------------------------------------------------------------
    active_ids = [s[0] for s in strategies]
    if active_ids:
        with engine.connect() as conn:
            killed = conn.execute(text(
                "SELECT COUNT(*) FROM paper_strategies "
                "WHERE status = 'KILLED' AND id = ANY(CAST(:active_ids AS TEXT[])) "
                "AND updated_at >= NOW() - INTERVAL '5 minutes'"
            ), {"active_ids": active_ids}).fetchone()
            strategies_killed = killed[0] if killed else 0

    summary = {
        "signals_checked": signals_checked,
        "trades_opened": trades_opened,
        "trades_closed": trades_closed,
        "strategies_killed": strategies_killed,
        "strategies_halted": strategies_halted,
        "venue": venue_tag.venue if venue_tag else None,
        "venue_mode": venue_tag.trader.mode if venue_tag else None,
        "venue_orders": venue_orders,
        "details": details,
    }

    log.info(
        "Signal executor complete: {c} checked, {o} opened, {cl} closed, {k} killed, {h} halted, {v} venue orders",
        c=signals_checked, o=trades_opened, cl=trades_closed,
        k=strategies_killed, h=strategies_halted, v=len(venue_orders),
    )
    return summary
