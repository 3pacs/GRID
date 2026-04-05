"""
Paper Trading P&L Review Script.

Queries all paper strategies and open trades, calculates unrealized P&L
using latest available prices from raw_series, and closes trades open > 7 days.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta

from loguru import logger as log
from sqlalchemy import create_engine, text

DB_URL = "postgresql://grid:gridmaster2026@localhost:5432/griddb"

# Mapping from paper_trades ticker to raw_series series_id
# Paper trades use e.g. "xlv_full" -> YF series uses "YF:XLV:close"
TICKER_TO_SERIES = {
    "xlv_full": "YF:XLV:close",
    "xlk_full": "YF:XLK:close",
    "xlc_full": "YF:XLC:close",
    "googl_full": "YF:GOOGL:close",
}

MAX_HOLD_DAYS = 7


def _ticker_symbol(ticker: str) -> str:
    """Convert paper trade ticker to display symbol."""
    return ticker.replace("_full", "").upper()


def fetch_latest_prices(conn, tickers: list[str]) -> dict[str, tuple[float, date]]:
    """Fetch the latest available close price for each ticker from raw_series.

    Returns dict of ticker -> (price, obs_date).
    """
    series_ids = [TICKER_TO_SERIES[t] for t in tickers if t in TICKER_TO_SERIES]
    if not series_ids:
        return {}

    rows = conn.execute(text("""
        SELECT DISTINCT ON (series_id) series_id, obs_date, value
        FROM raw_series
        WHERE series_id = ANY(:ids) AND pull_status = 'SUCCESS'
        ORDER BY series_id, obs_date DESC, pull_timestamp DESC
    """), {"ids": series_ids}).fetchall()

    # Reverse-map series_id back to ticker
    series_to_ticker = {v: k for k, v in TICKER_TO_SERIES.items()}
    result = {}
    for row in rows:
        ticker = series_to_ticker.get(row[0])
        if ticker:
            result[ticker] = (row[2], row[1])
    return result


def print_strategies(conn) -> None:
    """Print all paper strategies with their stats."""
    rows = conn.execute(text(
        "SELECT id, hypothesis_id, leader, follower, description, "
        "total_trades, wins, losses, total_pnl, max_drawdown, sharpe, "
        "status, capital, high_water_mark "
        "FROM paper_strategies ORDER BY id"
    )).fetchall()

    log.info("=" * 110)
    log.info("PAPER STRATEGIES")
    log.info("=" * 110)
    fmt = "{:<30s} {:>4s} {:>6s} {:>4s} {:>4s} {:>10s} {:>10s} {:>8s} {:>8s}"
    log.info(fmt.format(
        "STRATEGY", "H_ID", "TRADES", "W", "L",
        "TOTAL_PNL", "CAPITAL", "DD%", "STATUS",
    ))
    log.info("-" * 110)
    for r in rows:
        (sid, hid, leader, follower, desc,
         trades, wins, losses, pnl, dd, sharpe,
         status, capital, hwm) = r
        log.info(fmt.format(
            sid[:30], str(hid), str(trades), str(wins), str(losses),
            f"${pnl:,.2f}", f"${capital:,.2f}",
            f"{dd:.2%}", status,
        ))
    log.info("\nTotal strategies: {}", len(rows))



def print_open_trades(conn) -> None:
    """Print all open trades with current state."""
    rows = conn.execute(text(
        "SELECT id, strategy_id, ticker, direction, entry_price, entry_date, "
        "position_size, signal_strength "
        "FROM paper_trades WHERE status = 'OPEN' ORDER BY id"
    )).fetchall()

    log.info("=" * 110)
    log.info("OPEN TRADES")
    log.info("=" * 110)
    fmt = "{:>4s}  {:<30s} {:<10s} {:>5s} {:>12s} {:>12s} {:>8s} {:>10s}"
    log.info(fmt.format(
        "ID", "STRATEGY", "TICKER", "DIR", "ENTRY_PRICE",
        "ENTRY_DATE", "SIZE", "SIGNAL",
    ))
    log.info("-" * 110)
    for r in rows:
        (tid, sid, ticker, direction, entry_price,
         entry_date, pos_size, signal) = r
        log.info(fmt.format(
            str(tid), sid[:30], _ticker_symbol(ticker), direction,
            f"${entry_price:,.2f}", str(entry_date),
            f"{pos_size:.1%}", f"{signal:.4f}",
        ))
    log.info("\nTotal open trades: {}", len(rows))



def run_pnl_review(engine) -> None:
    """Main P&L review: compute unrealized P&L and close stale trades."""
    # Import PaperTradingEngine
    sys.path.insert(0, "/data/grid_v4/grid_repo")
    from trading.paper_engine import PaperTradingEngine

    pte = PaperTradingEngine(engine)

    # Get dashboard
    dashboard = pte.get_dashboard()
    log.info("=" * 110)
    log.info("DASHBOARD SUMMARY")
    log.info("=" * 110)
    log.info("  Active strategies : {}", dashboard['active_strategies'])
    log.info("  Killed strategies : {}", dashboard['killed_strategies'])
    log.info("  Total strategies  : {}", dashboard['total_strategies'])
    log.info("  Realized P&L      : ${:,.2f}", dashboard['total_pnl'])
    log.info("  Open trades       : {}", len(dashboard['open_trades']))


    # Fetch open trades and current prices
    open_trades = dashboard["open_trades"]
    if not open_trades:
        log.info("No open trades to review.")
        return

    tickers = list({t["ticker"] for t in open_trades})

    with engine.connect() as conn:
        latest_prices = fetch_latest_prices(conn, tickers)

    today = date.today()

    # Compute unrealized P&L
    log.info("=" * 110)
    log.info("UNREALIZED P&L")
    log.info("=" * 110)
    fmt = "{:>4s}  {:<10s} {:>5s} {:>12s} {:>12s} {:>12s} {:>10s} {:>8s} {:>6s}"
    log.info(fmt.format(
        "ID", "TICKER", "DIR", "ENTRY", "CURRENT",
        "UNREAL_PNL", "PNL_%", "DAYS", "STALE?",
    ))
    log.info("-" * 110)

    total_unrealized = 0.0
    trades_to_close: list[tuple[int, float, str]] = []

    for t in open_trades:
        trade_id = t["id"]
        ticker = t["ticker"]
        direction = t["direction"]
        entry_price = t["entry_price"]
        entry_date_str = t["entry_date"]
        position_size = t["position_size"]

        # Parse entry_date
        if isinstance(entry_date_str, str):
            entry_date = date.fromisoformat(entry_date_str)
        else:
            entry_date = entry_date_str

        days_open = (today - entry_date).days

        price_info = latest_prices.get(ticker)
        if price_info is None:
            log.info("  #{}  {:10s}  -- NO PRICE DATA AVAILABLE --", trade_id, _ticker_symbol(ticker))
            continue

        current_price, price_date = price_info

        # Calculate unrealized P&L
        if direction == "LONG":
            pnl_pct = (current_price - entry_price) / entry_price
        else:
            pnl_pct = (entry_price - current_price) / entry_price

        pnl_dollar = pnl_pct * position_size * 10000.0  # initial_capital = 10000
        total_unrealized += pnl_dollar

        is_stale = days_open > MAX_HOLD_DAYS
        stale_flag = "YES" if is_stale else "no"

        log.info(fmt.format(
            str(trade_id),
            _ticker_symbol(ticker),
            direction,
            f"${entry_price:,.2f}",
            f"${current_price:,.2f}",
            f"${pnl_dollar:+,.2f}",
            f"{pnl_pct:+.2%}",
            str(days_open),
            stale_flag,
        ))

        if is_stale:
            trades_to_close.append((trade_id, current_price, ticker))

    log.info("-" * 110)
    log.info("  Total unrealized P&L: ${:+,.2f}", total_unrealized)


    # Close stale trades
    if trades_to_close:
        log.info("=" * 110)
        log.info("CLOSING {} STALE TRADES (open > {} days)", len(trades_to_close), MAX_HOLD_DAYS)
        log.info("=" * 110)
        for trade_id, exit_price, ticker in trades_to_close:
            result = pte.close_trade(
                trade_id,
                exit_price,
                notes=f"Auto-closed: held > {MAX_HOLD_DAYS} days",
            )
            if "error" in result:
                log.info("  #{} {}: ERROR - {}", trade_id, _ticker_symbol(ticker), result['error'])
            else:
                log.info("  #{} {}: CLOSED at ${:,.2f} -> P&L ${:+,.2f} ({:+.4%})",
                         trade_id, _ticker_symbol(ticker), exit_price,
                         result['pnl'], result['pnl_pct'])
    
    else:
        log.info("No trades exceed the 7-day hold limit. Nothing to close.\n")

    # Final summary
    log.info("=" * 110)
    log.info("FINAL P&L SUMMARY")
    log.info("=" * 110)

    # Refresh dashboard after closures
    updated = pte.get_dashboard()
    realized = updated["total_pnl"]
    remaining_open = len(updated["open_trades"])

    # Recalculate unrealized for remaining open trades
    remaining_unrealized = 0.0
    with engine.connect() as conn:
        remaining_tickers = list({t["ticker"] for t in updated["open_trades"]})
        if remaining_tickers:
            remaining_prices = fetch_latest_prices(conn, remaining_tickers)
            for t in updated["open_trades"]:
                pi = remaining_prices.get(t["ticker"])
                if pi:
                    cp = pi[0]
                    ep = t["entry_price"]
                    if t["direction"] == "LONG":
                        pct = (cp - ep) / ep
                    else:
                        pct = (ep - cp) / ep
                    remaining_unrealized += pct * t["position_size"] * 10000.0

    log.info("  Realized P&L   : ${:+,.2f}", realized)
    log.info("  Unrealized P&L : ${:+,.2f}", remaining_unrealized)
    log.info("  Combined P&L   : ${:+,.2f}", realized + remaining_unrealized)
    log.info("  Open trades    : {}", remaining_open)
    log.info("  Active strats  : {}", updated['active_strategies'])
    log.info("  Killed strats  : {}", updated['killed_strategies'])
    log.info("=" * 110)


def main() -> None:
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        print_strategies(conn)
        print_open_trades(conn)

    run_pnl_review(engine)


if __name__ == "__main__":
    main()
