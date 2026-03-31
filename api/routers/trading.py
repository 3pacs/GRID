"""Paper trading, Hyperliquid perp, and prediction market API endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/trading", tags=["trading"])


# ---------------------------------------------------------------------------
# Prediction market request models (EXCH-02, EXCH-03)
# ---------------------------------------------------------------------------

class PolymarketBuyRequest(BaseModel):
    condition_id: str
    outcome: str  # "Yes" or "No"
    amount_usd: float


class KalshiBuyRequest(BaseModel):
    event_ticker: str
    side: str  # "yes" or "no"
    contracts: int
    price_cents: int


# ---------------------------------------------------------------------------
# Hyperliquid request models (EXCH-01)
# ---------------------------------------------------------------------------

class HyperliquidTradeRequest(BaseModel):
    ticker: str
    direction: str  # LONG or SHORT
    size_usd: float


class HyperliquidCloseRequest(BaseModel):
    ticker: str


class TradeRequest(BaseModel):
    strategy_id: str
    ticker: str
    direction: str = "LONG"
    entry_price: float
    position_size: float = 1.0
    signal_strength: float = 0.0
    physics_score: float = 0.0


class CloseTradeRequest(BaseModel):
    exit_price: float
    notes: str = ""


class CreateWalletRequest(BaseModel):
    exchange: str
    wallet_type: str = "paper"
    initial_capital: float
    risk_limit_pct: float = 0.05
    max_drawdown_limit: float = 0.20


class KillWalletRequest(BaseModel):
    reason: str


def _get_engine():
    from trading.paper_engine import PaperTradingEngine
    return PaperTradingEngine(get_db_engine())


def _get_wallet_manager():
    from trading.wallet_manager import WalletManager
    return WalletManager(get_db_engine())


@router.get("/dashboard")
async def trading_dashboard(_token: str = Depends(require_auth)) -> dict:
    """Paper trading dashboard — all strategies, recent trades, P&L."""
    return _get_engine().get_dashboard()


@router.post("/register-all")
async def register_all_strategies(_token: str = Depends(require_auth)) -> dict:
    """Register paper strategies for all PASSED TACTICAL hypotheses."""
    count = _get_engine().register_all_passed()
    return {"registered": count}


@router.post("/trade")
async def open_trade(
    req: TradeRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Open a new paper trade."""
    engine = _get_engine()
    trade_id = engine.open_trade(
        strategy_id=req.strategy_id,
        ticker=req.ticker,
        direction=req.direction,
        entry_price=req.entry_price,
        position_size=req.position_size,
        signal_strength=req.signal_strength,
        physics_score=req.physics_score,
    )
    return {"trade_id": trade_id}


@router.post("/trade/{trade_id}/close")
async def close_trade(
    trade_id: int,
    req: CloseTradeRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Close an open paper trade."""
    return _get_engine().close_trade(trade_id, req.exit_price, req.notes)


@router.get("/strategies")
async def list_strategies(_token: str = Depends(require_auth)) -> dict:
    """List all paper strategies with live P&L, open positions, and display names."""
    from datetime import date as _date, datetime as _dt
    from analysis.backtest_scanner import _display_name
    from sqlalchemy import text as _text

    engine = get_db_engine()
    with engine.connect() as conn:
        strategies = conn.execute(_text(
            "SELECT * FROM paper_strategies ORDER BY total_pnl DESC"
        )).fetchall()

        strat_list = []
        for row in strategies:
            d = dict(row._mapping)
            # Serialize dates
            for k, v in d.items():
                if isinstance(v, (_date, _dt)):
                    d[k] = str(v)

            # Open position count
            open_count = conn.execute(_text(
                "SELECT COUNT(*) FROM paper_trades "
                "WHERE strategy_id = :sid AND status = 'OPEN'"
            ), {"sid": d["id"]}).scalar() or 0
            d["open_positions"] = open_count

            # Human-readable names
            d["leader_display"] = _display_name(d.get("leader", ""))
            d["follower_display"] = _display_name(d.get("follower", ""))

            # Win rate
            total = d.get("total_trades", 0)
            wins = d.get("wins", 0)
            d["win_rate"] = round(wins / total, 4) if total > 0 else 0

            strat_list.append(d)

    return {"strategies": strat_list}


@router.get("/strategies/{strategy_id}/history")
async def strategy_trade_history(
    strategy_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict:
    """Trade history for a specific paper strategy."""
    from datetime import date as _date, datetime as _dt
    from analysis.backtest_scanner import _display_name
    from sqlalchemy import text as _text

    engine = get_db_engine()
    with engine.connect() as conn:
        # Verify strategy exists
        strat = conn.execute(_text(
            "SELECT * FROM paper_strategies WHERE id = :sid"
        ), {"sid": strategy_id}).fetchone()
        if not strat:
            raise HTTPException(status_code=404, detail="Strategy not found")

        trades = conn.execute(_text(
            "SELECT * FROM paper_trades "
            "WHERE strategy_id = :sid "
            "ORDER BY created_at DESC LIMIT :lim"
        ), {"sid": strategy_id, "lim": limit}).fetchall()

    trade_list = []
    for row in trades:
        d = dict(row._mapping)
        for k, v in d.items():
            if isinstance(v, (_date, _dt)):
                d[k] = str(v)
        trade_list.append(d)

    sd = dict(strat._mapping)
    for k, v in sd.items():
        if isinstance(v, (_date, _dt)):
            sd[k] = str(v)
    sd["leader_display"] = _display_name(sd.get("leader", ""))
    sd["follower_display"] = _display_name(sd.get("follower", ""))

    return {"strategy": sd, "trades": trade_list, "count": len(trade_list)}


class PromoteToStrategyRequest(BaseModel):
    leader: str
    follower: str
    sharpe: float = 0
    win_rate: float = 0
    total_return: float = 0


@router.post("/strategies/promote")
async def promote_to_strategy(
    req: PromoteToStrategyRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Create a new paper trading strategy from a backtest winner."""
    from analysis.backtest_scanner import _display_name

    engine = _get_engine()
    leader_disp = _display_name(req.leader)
    follower_disp = _display_name(req.follower)
    description = (
        f"{leader_disp} leads {follower_disp} "
        f"(Sharpe {req.sharpe:.1f}, {req.win_rate*100:.0f}% WR, "
        f"{req.total_return*100:.1f}% return)"
    )
    strategy_id = engine.register_strategy(
        hypothesis_id=0,
        leader=req.leader,
        follower=req.follower,
        description=description,
    )
    return {"strategy_id": strategy_id, "status": "created"}


@router.post("/execute-signals")
async def execute_signals_now(_token: str = Depends(require_auth)) -> dict:
    """Manually trigger signal execution."""
    from trading.signal_executor import execute_signals
    return execute_signals(get_db_engine())


@router.post("/strategies/{strategy_id}/kill")
async def kill_strategy(
    strategy_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Manually kill a paper strategy."""
    from sqlalchemy import text
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE paper_strategies SET status = 'KILLED', "
            "kill_reason = 'Manual kill', updated_at = NOW() "
            "WHERE id = :id"
        ), {"id": strategy_id})
    return {"status": "killed", "strategy_id": strategy_id}


# ------------------------------------------------------------------
# Wallet management endpoints (EXCH-04)
# ------------------------------------------------------------------

@router.get("/wallets/dashboard")
async def wallet_dashboard(_token: str = Depends(require_auth)) -> dict:
    """Aggregated wallet dashboard — total capital, P&L, per-exchange breakdown."""
    return _get_wallet_manager().get_dashboard()


@router.get("/wallets")
async def list_wallets(
    exchange: str | None = Query(None),
    status: str | None = Query(None),
    _token: str = Depends(require_auth),
) -> dict:
    """List all trading wallets, optionally filtered by exchange and status."""
    wallets = _get_wallet_manager().get_all_wallets(exchange=exchange, status=status)
    return {"wallets": wallets}


@router.post("/wallets")
async def create_wallet(
    req: CreateWalletRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Create a new trading wallet."""
    wm = _get_wallet_manager()
    wallet_id = wm.create_wallet(
        exchange=req.exchange,
        wallet_type=req.wallet_type,
        initial_capital=req.initial_capital,
        risk_limit_pct=req.risk_limit_pct,
        max_drawdown_limit=req.max_drawdown_limit,
    )
    return {"wallet_id": wallet_id}


@router.get("/wallets/{wallet_id}")
async def get_wallet(
    wallet_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Get a single wallet by ID."""
    result = _get_wallet_manager().get_wallet(wallet_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/wallets/{wallet_id}/risk")
async def wallet_risk_check(
    wallet_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Check risk status for a wallet — drawdown, headroom, status."""
    result = _get_wallet_manager().check_risk(wallet_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/wallets/{wallet_id}/kill")
async def kill_wallet(
    wallet_id: str,
    req: KillWalletRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Kill a wallet with a reason."""
    return _get_wallet_manager().kill_wallet(wallet_id, req.reason)


@router.post("/wallets/{wallet_id}/pause")
async def pause_wallet(
    wallet_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Pause an active wallet."""
    return _get_wallet_manager().pause_wallet(wallet_id)


@router.post("/wallets/{wallet_id}/resume")
async def resume_wallet(
    wallet_id: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Resume a paused wallet."""
    return _get_wallet_manager().resume_wallet(wallet_id)


# ------------------------------------------------------------------
# Hyperliquid perp endpoints (EXCH-01)
# ------------------------------------------------------------------

def _get_hyperliquid():
    from trading.hyperliquid import get_hyperliquid_trader
    return get_hyperliquid_trader()


@router.get("/hyperliquid/balance")
async def hyperliquid_balance(
    _token: str = Depends(require_auth),
) -> dict:
    """Hyperliquid wallet state — equity, margin, open position count."""
    return _get_hyperliquid().get_balance()


@router.get("/hyperliquid/positions")
async def hyperliquid_positions(
    _token: str = Depends(require_auth),
) -> dict:
    """Open Hyperliquid perp positions with unrealized P&L."""
    positions = _get_hyperliquid().get_positions()
    return {"positions": positions, "count": len(positions)}


@router.post("/hyperliquid/trade")
async def hyperliquid_trade(
    req: HyperliquidTradeRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Open a Hyperliquid perp position (market order)."""
    result = _get_hyperliquid().open_position(
        ticker=req.ticker,
        direction=req.direction,
        size_usd=req.size_usd,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/hyperliquid/close")
async def hyperliquid_close(
    req: HyperliquidCloseRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Close all of a ticker's Hyperliquid perp position."""
    result = _get_hyperliquid().close_position(ticker=req.ticker)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# ------------------------------------------------------------------
# Polymarket endpoints (EXCH-02)
# ------------------------------------------------------------------

def _get_polymarket():
    from trading.prediction_markets import PolymarketTrader
    return PolymarketTrader()


def _get_kalshi():
    from trading.prediction_markets import KalshiTrader
    return KalshiTrader()


@router.get("/polymarket/markets")
async def polymarket_markets(
    q: str | None = Query(None, description="Search query"),
    limit: int = Query(20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict:
    """Search or list active Polymarket prediction markets."""
    markets = _get_polymarket().get_markets(query=q, limit=limit)
    return {"markets": markets, "count": len(markets)}


@router.get("/polymarket/portfolio")
async def polymarket_portfolio(
    _token: str = Depends(require_auth),
) -> dict:
    """Get Polymarket positions and portfolio value."""
    return _get_polymarket().get_portfolio()


@router.post("/polymarket/buy")
async def polymarket_buy(
    req: PolymarketBuyRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Buy outcome shares on a Polymarket market."""
    result = _get_polymarket().buy(
        condition_id=req.condition_id,
        outcome=req.outcome,
        amount_usd=req.amount_usd,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# ------------------------------------------------------------------
# Kalshi endpoints (EXCH-03)
# ------------------------------------------------------------------

@router.get("/kalshi/events")
async def kalshi_events(
    category: str | None = Query(None, description="Filter by series/category"),
    limit: int = Query(20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict:
    """Get active Kalshi event contracts."""
    events = _get_kalshi().get_events(category=category, limit=limit)
    return {"events": events, "count": len(events)}


@router.get("/kalshi/portfolio")
async def kalshi_portfolio(
    _token: str = Depends(require_auth),
) -> dict:
    """Get Kalshi positions."""
    return _get_kalshi().get_portfolio()


@router.post("/kalshi/buy")
async def kalshi_buy(
    req: KalshiBuyRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Buy contracts on a Kalshi event."""
    result = _get_kalshi().buy(
        event_ticker=req.event_ticker,
        side=req.side,
        contracts=req.contracts,
        price_cents=req.price_cents,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# ------------------------------------------------------------------
# Options recommender endpoint
# ------------------------------------------------------------------

@router.post("/options-recommendations")
async def options_recommendations(
    _token: str = Depends(require_auth),
) -> dict:
    """Run the options recommendation engine and return actionable trade tickets."""
    from trading.options_recommender import OptionsRecommender
    recommender = OptionsRecommender(db_engine=get_db_engine())
    recs = recommender.generate_recommendations()
    return {
        "recommendations": [vars(r) for r in recs],
        "count": len(recs),
    }


# ------------------------------------------------------------------
# Options tracker endpoint
# ------------------------------------------------------------------

@router.post("/options-tracker/score")
async def options_tracker_score(
    _token: str = Depends(require_auth),
) -> dict:
    """Run the options outcome tracking and self-improvement cycle."""
    from trading.options_tracker import run_improvement_cycle
    result = run_improvement_cycle(get_db_engine())
    return result
