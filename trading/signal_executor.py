"""
Paper Trading Signal Executor.

Runs periodically (hourly during market hours) to:
1. Check all ACTIVE paper strategies for fired signals
2. For each strategy: compare leader's latest daily return to threshold (1%)
3. If signal fires: compute Kelly position size, open paper trade on follower
4. For open trades past their expected_lag days: close at current price
5. Log everything to decision journal for audit trail

An optional **venue tag** (``execute_signals(engine, venue="robinhood")``)
mirrors BUY signals to a real exchange connector on top of the paper trade.
Robinhood is crypto spot: long only, crypto tickers only, and dry-run until
``ROBINHOOD_LIVE_TRADING`` is set. The paper trade is opened either way — the
venue tag never changes what the paper book records.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from trading.circuit_breaker import StrategyCircuitBreaker
from trading.paper_engine import PaperTradingEngine


_SIGNAL_THRESHOLD = 0.01  # 1% daily return triggers a signal
_DEFAULT_POSITION_SIZE = 0.1  # 10% of capital when no trade history
_DEFAULT_EXPECTED_LAG = 1  # days

#: Venues the executor can mirror signals to. Paper-only when unset.
SUPPORTED_VENUES: tuple[str, ...] = ("robinhood",)

#: Below this notional an exchange order is not worth sending.
_MIN_VENUE_ORDER_USD = 1.0


def _get_latest_prices(conn, feature_name: str, n: int = 2) -> list[tuple[date, float]]:
    """Get the latest n daily prices for a feature from resolved_series."""
    rows = conn.execute(text(
        "SELECT DISTINCT ON (obs_date) obs_date, value "
        "FROM resolved_series rs "
        "JOIN feature_registry fr ON fr.id = rs.feature_id "
        "WHERE fr.name = :feature_name "
        "ORDER BY obs_date DESC, vintage_date DESC "
        "LIMIT :n"
    ), {"feature_name": feature_name, "n": n}).fetchall()
    return [(r[0], float(r[1])) for r in rows]


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

    def submit(self, ticker: str, direction: str, position_size: float) -> dict | None:
        """Send the signal to the venue. ``None`` when nothing was routed."""
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

        from trading.robinhood import crypto_asset_code

        size_usd = self.order_size_usd(position_size)
        if size_usd < self.min_order_usd:
            log.debug("Venue {v}: ${s:.2f} for {t} below the ${m:.2f} minimum",
                      v=self.venue, s=size_usd, t=ticker, m=self.min_order_usd)
            return None

        asset = crypto_asset_code(ticker) or ticker
        try:
            result = self.trader.open_position(ticker=asset, direction="LONG", size_usd=size_usd)
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

    log.info("Venue tag active: {v} mode={m} wallet={w}",
             v=venue, m=trader.mode, w=wallet_id or "none")
    return VenueTag(venue, trader, wallet)


def _compute_kelly_size(engine_obj: PaperTradingEngine, conn, strategy_id: str) -> float:
    """Compute position size from closed trades using Kelly criterion.

    Falls back to DEFAULT_POSITION_SIZE if insufficient history.
    """
    closed = conn.execute(text(
        "SELECT pnl_pct FROM paper_trades "
        "WHERE strategy_id = :sid AND status = 'CLOSED'"
    ), {"sid": strategy_id}).fetchall()

    if len(closed) < 5:
        return _DEFAULT_POSITION_SIZE

    pnls = [float(r[0]) for r in closed]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    if not wins or not losses:
        return _DEFAULT_POSITION_SIZE

    win_rate = len(wins) / len(pnls)
    avg_win = sum(wins) / len(wins)
    avg_loss = sum(losses) / len(losses)

    size = engine_obj.kelly_position_size(win_rate, avg_win, avg_loss)
    return size if size > 0 else _DEFAULT_POSITION_SIZE


def execute_signals(
    engine: Engine,
    venue: str | None = None,
    venue_wallet_id: str | None = None,
) -> dict:
    """Execute paper trading signals for all ACTIVE strategies.

    *venue* optionally mirrors BUY signals to an exchange connector on top of
    the paper trade (see :class:`VenueTag`); *venue_wallet_id* names the
    ``trading_wallets`` row that sizes and risk-gates those orders.

    Returns summary dict with signals_checked, trades_opened, trades_closed, etc.
    """
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
    # 1. Load all ACTIVE strategies
    # ------------------------------------------------------------------
    with engine.connect() as conn:
        strategies = conn.execute(text(
            "SELECT id, hypothesis_id, leader, follower, horizon_days "
            "FROM paper_strategies WHERE status = 'ACTIVE'"
        )).fetchall()

    log.info("Signal executor: checking {n} active strategies", n=len(strategies))

    for strat in strategies:
        strategy_id, hypothesis_id, leader, follower, strategy_horizon_days = strat
        signals_checked += 1

        # Circuit breaker check — skip halted strategies
        if not breaker.should_execute(strategy_id):
            strategies_halted += 1
            details.append({
                "action": "HALTED",
                "strategy_id": strategy_id,
                "reason": "circuit breaker OPEN",
            })
            continue

        try:
            with engine.connect() as conn:
                # ----------------------------------------------------------
                # 2-4. Get leader's latest 2 daily prices, compute return
                # ----------------------------------------------------------
                leader_prices = _get_latest_prices(conn, leader, 2)
                if len(leader_prices) < 2:
                    log.debug("Strategy {s}: leader {l} has < 2 prices, skipping",
                              s=strategy_id, l=leader)
                    continue

                price_today, price_yesterday = leader_prices[0][1], leader_prices[1][1]
                if price_yesterday == 0:
                    continue
                leader_return = (price_today - price_yesterday) / price_yesterday

                # ----------------------------------------------------------
                # 5. Check signal threshold
                # ----------------------------------------------------------
                if abs(leader_return) > _SIGNAL_THRESHOLD:
                    direction = "LONG" if leader_return > 0 else "SHORT"

                    # Get follower's latest price for entry
                    follower_prices = _get_latest_prices(conn, follower, 1)
                    if not follower_prices:
                        log.debug("Strategy {s}: no follower price for {f}",
                                  s=strategy_id, f=follower)
                    elif follower_prices[0][1] <= 0:
                        pass  # invalid price, skip opening
                    else:
                        entry_price = follower_prices[0][1]

                        # Check no existing OPEN trade for this strategy
                        open_trade = conn.execute(text(
                            "SELECT id FROM paper_trades "
                            "WHERE strategy_id = :sid AND status = 'OPEN' LIMIT 1"
                        ), {"sid": strategy_id}).fetchone()

                        if not open_trade:
                            # Kelly position sizing
                            position_size = _compute_kelly_size(pe, conn, strategy_id)
                            signal_strength = abs(leader_return)

                            # Convergence adjustment — if trust_scorer detects
                            # 3+ independent sources agreeing on this ticker+direction,
                            # scale position size up (capped at 2x Kelly)
                            try:
                                from intelligence.trust_scorer import detect_convergence
                                convergence = detect_convergence(engine, ticker=follower)
                                trade_dir = "BUY" if direction == "LONG" else "SELL"
                                for evt in (convergence or []):
                                    if evt.get("signal_type") == trade_dir:
                                        src_count = evt.get("source_count", 0)
                                        combined_conf = evt.get("combined_confidence", 0.5)
                                        convergence_mult = 1.0 + 0.15 * (src_count - 2) * combined_conf
                                        position_size = min(position_size * convergence_mult, position_size * 2.0)
                                        signal_strength *= convergence_mult
                                        log.info(
                                            "Convergence boost: {t} {d} — {n} sources, conf={c:.2f}, size={s:.3f}",
                                            t=follower, d=trade_dir, n=src_count,
                                            c=combined_conf, s=position_size,
                                        )
                                        break
                            except Exception:
                                pass  # Convergence is optional — degrade gracefully

                            trade_id = pe.open_trade(
                                strategy_id=strategy_id,
                                ticker=follower,
                                direction=direction,
                                entry_price=entry_price,
                                position_size=position_size,
                                signal_strength=signal_strength,
                                hypothesis_id=hypothesis_id,
                                threshold_used=_SIGNAL_THRESHOLD,
                            )

                            if trade_id > 0:
                                trades_opened += 1
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
                                }
                                if venue_tag is not None:
                                    order = venue_tag.submit(follower, direction, position_size)
                                    if order:
                                        detail["venue_order"] = order
                                        venue_orders.append(
                                            {"strategy_id": strategy_id, "trade_id": trade_id, **order}
                                        )
                                details.append(detail)
                        else:
                            log.debug("Strategy {s}: already has open trade #{t}, skipping open",
                                      s=strategy_id, t=open_trade[0])

                # ----------------------------------------------------------
                # 6. Close trades past the holding period (ALWAYS runs).
                #    Strategy horizon_days beats the hypothesis expected_lag
                #    so long-horizon paper trades are not closed next day.
                # ----------------------------------------------------------
                expected_lag = resolve_hold_days(
                    strategy_horizon_days, _get_expected_lag(conn, hypothesis_id)
                )

                open_trades = conn.execute(text(
                    "SELECT id, ticker, entry_date FROM paper_trades "
                    "WHERE strategy_id = :sid AND status = 'OPEN'"
                ), {"sid": strategy_id}).fetchall()

                for ot in open_trades:
                    trade_id, ticker, entry_date = ot
                    if entry_date + timedelta(days=expected_lag) <= today:
                        # Get follower's current price for exit
                        exit_prices = _get_latest_prices(conn, follower, 1)
                        if not exit_prices:
                            log.debug("No exit price for {f}, skipping close", f=follower)
                            continue
                        exit_price = exit_prices[0][1]
                        if exit_price <= 0:
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
                                "trade_id": trade_id,
                                "ticker": ticker,
                                "exit_price": exit_price,
                                "pnl": result.get("pnl"),
                                "pnl_pct": result.get("pnl_pct"),
                            })

            # All operations succeeded for this strategy — record success
            breaker.record_success(strategy_id)

        except Exception as exc:
            log.warning("Strategy {s} signal check failed: {e}", s=strategy_id, e=str(exc))
            breaker.record_failure(strategy_id, str(exc))
            details.append({
                "action": "ERROR",
                "strategy_id": strategy_id,
                "error": str(exc),
            })

    # ------------------------------------------------------------------
    # 7. Count any strategies killed during this run (from close_trade)
    # ------------------------------------------------------------------
    with engine.connect() as conn:
        killed = conn.execute(text(
            "SELECT COUNT(*) FROM paper_strategies "
            "WHERE status = 'KILLED' AND updated_at >= NOW() - INTERVAL '5 minutes'"
        )).fetchone()
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
