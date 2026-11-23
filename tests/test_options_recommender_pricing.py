"""Regression tests for verified pricing, sizing, freshness, and sanity defects
in ``trading/options_recommender.py``.

All tests are offline (synthetic chains + fake DB/mocks; no database, no network).
Covers defects 1-8 plus rejection-count observability.
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

import trading.options_recommender as recommender_mod
from analysis.vol_surface import _bs_call_price, _bs_put_price
from trading.options_recommender import (
    MAX_KELLY_PER_TICKET,
    OptionsRecommendation,
    OptionsRecommender,
)
from trading.trade_ticket_generator import (
    MAX_KELLY_PER_TICKET as TICKET_MAX_KELLY_PER_TICKET,
)


RUN_WED = date(2026, 11, 18)  # Wednesday; previous trading session is Tue 2026-11-17
EXP_21D = date(2026, 12, 9)   # 21 calendar days from RUN_WED
EXP_MONTHLY = date(2026, 12, 18)  # 3rd Friday of Dec 2026 (30 DTE from RUN_WED)


def _freeze_date(monkeypatch: pytest.MonkeyPatch, today: date) -> None:
    """Pin ``date.today()`` inside ``trading.options_recommender`` while preserving ``isinstance(..., date)``."""

    class _DateMeta(type):
        def __instancecheck__(cls, instance: Any) -> bool:
            return isinstance(instance, date)

    class _FixedDate(date, metaclass=_DateMeta):
        @classmethod
        def today(cls) -> date:
            return today

    monkeypatch.setattr(recommender_mod, "date", _FixedDate)


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class _FakeConn:
    """In-memory DB double supporting the recommender's SQL read surfaces."""

    def __init__(
        self,
        *,
        snapshots: list[dict[str, Any]] | None = None,
        atm_iv: float | None = None,
        spot: float | None = None,
    ) -> None:
        self.snapshots = list(snapshots or [])
        self.atm_iv = atm_iv
        self.spot = spot
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def execute(self, statement: Any, params: dict[str, Any] | None = None) -> _Result:
        sql = str(statement)
        bound = dict(params or {})
        self.queries.append((sql, bound))
        upper = sql.upper()

        # 1. Chain loader query
        if "IMPLIED_VOL AS IMPLIED_VOLATILITY" in upper:
            ticker = bound.get("ticker")
            min_snap = bound.get("min_snap_date")
            max_snap = bound.get("run_date")
            rows = [r for r in self.snapshots if r["ticker"] == ticker and r["open_interest"] > 0]
            if min_snap is not None:
                rows = [r for r in rows if r["snap_date"] >= min_snap]
            if max_snap is not None:
                rows = [r for r in rows if r["snap_date"] <= max_snap]
                rows = [r for r in rows if r["expiry"] > max_snap]
            if not rows:
                return _Result([])
            latest_snap = max(r["snap_date"] for r in rows)
            rows = [r for r in rows if r["snap_date"] == latest_snap]
            rows.sort(key=lambda r: (r["expiry"], r["strike"]))
            if "AS DTE" not in upper.split("FROM")[0]:
                return _Result([
                    (
                        r["strike"],
                        r["opt_type"],
                        r["open_interest"],
                        r["implied_vol"],
                        r["expiry"],
                        r["bid"],
                        r["ask"],
                        r["snap_date"],
                    )
                    for r in rows
                ])
            return _Result([
                (
                    r["strike"],
                    r["opt_type"],
                    r["open_interest"],
                    r["implied_vol"],
                    r["expiry"],
                    (r["expiry"] - r["snap_date"]).days,
                    r["bid"],
                    r["ask"],
                )
                for r in rows
            ])

        # 2. Data-quality sanity check query
        if "SELECT IMPLIED_VOL, OPEN_INTEREST, BID, ASK" in upper:
            matches = [
                r for r in self.snapshots
                if r["ticker"] == bound.get("ticker")
                and float(r["strike"]) == float(bound.get("strike"))
                and r["opt_type"] == bound.get("opt_type")
            ]
            # Only filter by expiry if the SQL actually bound :expiry (defect 5!)
            if "expiry" in bound:
                exp_target = bound["expiry"]
                if isinstance(exp_target, str):
                    exp_target = date.fromisoformat(exp_target)
                matches = [r for r in matches if r["expiry"] == exp_target]
            if "min_snap_date" in bound:
                matches = [r for r in matches if r["snap_date"] >= bound["min_snap_date"]]
            if "run_date" in bound:
                matches = [r for r in matches if r["snap_date"] <= bound["run_date"]]
            matches.sort(key=lambda r: r["snap_date"], reverse=True)
            if not matches:
                return _Result([])
            r = matches[0]
            if "SNAP_DATE" in upper.split("FROM")[0]:
                return _Result([(r["implied_vol"], r["open_interest"], r["bid"], r["ask"], r["snap_date"])])
            return _Result([(r["implied_vol"], r["open_interest"], r["bid"], r["ask"])])

        # 3. Single-contract entry quote query
        if "SELECT BID, ASK" in upper:
            exp_target = bound.get("expiry")
            if isinstance(exp_target, str):
                exp_target = date.fromisoformat(exp_target)
            matches = [
                r for r in self.snapshots
                if r["ticker"] == bound.get("ticker")
                and float(r["strike"]) == float(bound.get("strike"))
                and r["expiry"] == exp_target
                and r["opt_type"] == bound.get("opt_type")
            ]
            if "min_snap_date" in bound:
                matches = [r for r in matches if r["snap_date"] >= bound["min_snap_date"]]
            if "run_date" in bound:
                matches = [r for r in matches if r["snap_date"] <= bound["run_date"]]
            matches.sort(key=lambda r: r["snap_date"], reverse=True)
            if not matches:
                return _Result([])
            r = matches[0]
            if "SNAP_DATE" in upper.split("FROM")[0]:
                return _Result([(r["bid"], r["ask"], r.get("last_price", 0.0), r["snap_date"])])
            return _Result([(r["bid"], r["ask"], r.get("last_price", 0.0))])

        # 4. ATM IV fallback query
        if "SELECT IV_ATM" in upper:
            if self.atm_iv is None:
                return _Result([])
            return _Result([(self.atm_iv,)])

        # 5. Resolved series (spot / momentum)
        if "FROM RESOLVED_SERIES" in upper:
            if self.spot is None:
                return _Result([])
            return _Result([(self.spot,), (self.spot,)])

        # 6. Decision journal / historical analogs / persistence
        return _Result([])


class _FakeDb:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def connect(self):
        conn = self.conn

        class _Ctx:
            def __enter__(self_inner):
                return conn

            def __exit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()

    def begin(self):
        return self.connect()


def _make_opp(
    *,
    ticker: str = "SPY",
    direction: str = "CALL",
    spot: float = 100.0,
    score: float = 8.5,
    confidence: str = "HIGH",
    iv_atm: float | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        ticker=ticker,
        direction=direction,
        spot_price=spot,
        score=score,
        confidence=confidence,
        thesis=f"{ticker} directional mispricing setup",
        signals={"gamma_squeeze": {"score": 3.0, "direction": "bullish" if direction == "CALL" else "bearish"}},
        iv_atm=iv_atm,
    )


def _make_gex_engine(profile: dict[str, Any]) -> MagicMock:
    engine = MagicMock()
    engine.compute_gex_profile.return_value = profile
    return engine


def _disable_llm(recommender: OptionsRecommender, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        recommender,
        "_sanity_llm_review",
        lambda rec, opp, gex: {"status": "SKIP", "reason": "LLM disabled in unit test"},
    )


def _contract_row(
    *,
    ticker: str = "SPY",
    strike: float = 105.0,
    expiry: date = EXP_21D,
    opt_type: str = "call",
    bid: float = 0.95,
    ask: float = 1.05,
    implied_vol: float | None = 0.28,
    open_interest: int = 1500,
    snap_date: date = date(2026, 11, 17),
    last_price: float = 1.00,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "strike": strike,
        "expiry": expiry,
        "opt_type": opt_type,
        "bid": bid,
        "ask": ask,
        "implied_vol": implied_vol,
        "open_interest": open_interest,
        "snap_date": snap_date,
        "last_price": last_price,
    }


# ── Defect 1: Target price must reprice via Black-Scholes (not delta=1) ─────


def test_defect1_target_price_uses_black_scholes_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spot 100, call wall 110, strike 105, bid/ask 0.95/1.05 (mid 1.00), 21 DTE, IV 0.28.

    Delta=1 bug produced $11.00 (11x). BS repricing at S=110 with remaining time
    (half DTE = 10.5d) minus half-spread (0.05) minus fee ($0.65/100 = 0.0065)
    is ~$5.50 — realistic and bounded by BS theory.
    """
    _freeze_date(monkeypatch, RUN_WED)
    db = _FakeDb(_FakeConn(snapshots=[_contract_row(strike=105.0, expiry=EXP_21D, bid=0.95, ask=1.05, implied_vol=0.28)]))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)

    opp = _make_opp(direction="CALL", spot=100.0, score=8.5)
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0, "put_wall": 90.0})

    rec = rec_engine._build_recommendation(opp, gex, db)
    assert rec is not None

    expected_theo = _bs_call_price(110.0, 105.0, 10.5 / 365.0, 0.05, 0.28)
    expected_exit = expected_theo - 0.05 - 0.0065
    assert rec.target_price == pytest.approx(expected_exit, abs=0.05)
    assert rec.target_price < 6.50  # Nowhere near the fabricated $11.00 delta=1 target


def test_defect1_target_price_uses_black_scholes_put(monkeypatch: pytest.MonkeyPatch) -> None:
    """PUT direction also reprices via Black-Scholes at put_wall and remaining time."""
    _freeze_date(monkeypatch, RUN_WED)
    db = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=95.0, expiry=EXP_21D, opt_type="put", bid=0.95, ask=1.05, implied_vol=0.28),
    ]))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)

    opp = _make_opp(direction="PUT", spot=100.0, score=8.5)
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0, "put_wall": 90.0})

    rec = rec_engine._build_recommendation(opp, gex, db)
    assert rec is not None

    expected_theo = _bs_put_price(90.0, 95.0, 10.5 / 365.0, 0.05, 0.28)
    expected_exit = expected_theo - 0.05 - 0.0065
    assert rec.target_price == pytest.approx(expected_exit, abs=0.05)
    assert rec.target_price < 6.50


def test_defect1_missing_iv_uses_fallback_or_rejects_no_iv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing contract IV uses repo IV fallback; if none exists, rejects with reason 'no_iv'."""
    _freeze_date(monkeypatch, RUN_WED)

    # Case A: No contract IV AND no ATM IV fallback -> reject with 'no_iv'
    db_no_iv = _FakeDb(_FakeConn(
        snapshots=[_contract_row(strike=105.0, expiry=EXP_21D, implied_vol=None)],
        atm_iv=None,
    ))
    rec_engine = OptionsRecommender(db_no_iv)
    _disable_llm(rec_engine, monkeypatch)
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0})

    rec = rec_engine._build_recommendation(_make_opp(iv_atm=None), gex, db_no_iv)
    assert rec is None
    assert getattr(rec_engine, "last_rejection_counts", {}).get("no_iv") == 1

    # Case B: Missing contract IV, but repo fallback iv_atm=0.28 exists -> reprices & succeeds
    db_fallback = _FakeDb(_FakeConn(
        snapshots=[_contract_row(strike=105.0, expiry=EXP_21D, implied_vol=None)],
        atm_iv=0.28,
    ))
    rec_engine_fb = OptionsRecommender(db_fallback)
    _disable_llm(rec_engine_fb, monkeypatch)

    rec_fb = rec_engine_fb._build_recommendation(_make_opp(iv_atm=0.28), gex, db_fallback)
    assert rec_fb is not None
    expected_theo = _bs_call_price(110.0, 105.0, 10.5 / 365.0, 0.05, 0.28)
    assert rec_fb.target_price == pytest.approx(expected_theo - 0.05 - 0.0065, abs=0.05)


# ── Defect 2: Kelly cap must default to 0.05 and block silent bypasses ──────


def test_defect2_kelly_cap_default_and_override_gate() -> None:
    """Default Kelly cap is 0.05 (shared with trade_ticket_generator); >0.05 requires explicit override."""
    assert MAX_KELLY_PER_TICKET == pytest.approx(0.05)
    assert MAX_KELLY_PER_TICKET == TICKET_MAX_KELLY_PER_TICKET

    rec_default = OptionsRecommender(MagicMock())
    assert rec_default.max_kelly == pytest.approx(0.05)
    assert rec_default._compute_kelly(0.65, 20.0) == pytest.approx(0.05)

    # Lower cap is permitted
    rec_low = OptionsRecommender(MagicMock(), max_kelly=0.02)
    assert rec_low._compute_kelly(0.65, 20.0) == pytest.approx(0.02)

    # Higher cap without explicit override is rejected
    with pytest.raises(ValueError):
        OptionsRecommender(MagicMock(), max_kelly=0.25)

    # Explicit override allows > 0.05
    rec_override = OptionsRecommender(MagicMock(), max_kelly=0.15, allow_kelly_cap_override=True)
    assert rec_override._compute_kelly(0.65, 20.0) == pytest.approx(0.15)


# ── Defect 3: Zero-Kelly / negative-EV recommendations must not be emitted ──


def test_defect3_zero_kelly_and_negative_ev_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Negative EV / zero Kelly must not be emitted (no max(1, ...) contract fabrication)."""
    _freeze_date(monkeypatch, RUN_WED)
    # Low score (2.0 -> win_prob=0.10) + narrow wall (104) => negative EV and zero Kelly
    db = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=103.0, expiry=EXP_21D, bid=0.95, ask=1.05, implied_vol=0.25),
    ]))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)

    opp = _make_opp(direction="CALL", spot=100.0, score=2.0, confidence="LOW")
    gex = _make_gex_engine({"regime": "NEUTRAL", "call_wall": 104.0, "put_wall": 95.0})

    rec = rec_engine._build_recommendation(opp, gex, db)
    assert rec is None
    counts = getattr(rec_engine, "last_rejection_counts", {})
    assert counts.get("nonpositive_ev", 0) + counts.get("zero_kelly", 0) == 1


def test_positive_kelly_below_one_contract_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """A positive-Kelly rec whose capped budget can't buy one contract is not emitted (no 0-contract recs)."""
    _freeze_date(monkeypatch, RUN_WED)
    db = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=104.0, expiry=EXP_21D, bid=0.90, ask=1.10, implied_vol=0.25),
    ]))
    # 0.05 Kelly cap x $1,000 capital = $50 budget < one contract (~$110.65)
    rec_engine = OptionsRecommender(db, capital=1_000.0)
    _disable_llm(rec_engine, monkeypatch)

    opp = _make_opp(direction="CALL", spot=100.0, score=9.0, confidence="HIGH")
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 112.0})

    assert rec_engine._build_recommendation(opp, gex, db) is None
    assert rec_engine.last_rejection_counts.get("below_one_contract", 0) == 1


# ── Defect 4: Required sanity checks must PASS (SKIP on required rejects) ───


def test_defect4_required_sanity_check_skip_rejects(monkeypatch: pytest.MonkeyPatch) -> None:
    """SKIP on DATA_QUALITY (or empty sanity_status) fails closed with required_check_skipped."""
    rec_skipped_dq = OptionsRecommendation(
        ticker="SPY",
        direction="CALL",
        strike=105.0,
        expiry=EXP_21D.isoformat(),
        entry_price=1.06,
        sanity_status={
            "DATA_QUALITY": {"status": "SKIP", "reason": "No snapshot data"},
            "DEALER_FLOW": {"status": "PASS", "reason": "ok"},
            "CROSS_ASSET": {"status": "PASS", "reason": "ok"},
            "LLM_REVIEW": {"status": "SKIP", "reason": "offline"},
            "HISTORICAL_ANALOG": {"status": "SKIP", "reason": "cold start"},
        },
    )
    assert rec_skipped_dq.all_sanity_passed is False

    rec_empty = OptionsRecommendation(
        ticker="SPY",
        direction="CALL",
        strike=105.0,
        expiry=EXP_21D.isoformat(),
        entry_price=1.06,
        sanity_status={},
    )
    assert rec_empty.all_sanity_passed is False

    # End-to-end rejection reason code must be required_check_skipped:DATA_QUALITY
    _freeze_date(monkeypatch, RUN_WED)
    db = _FakeDb(_FakeConn(snapshots=[_contract_row(strike=105.0, expiry=EXP_21D)]))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)
    monkeypatch.setattr(
        rec_engine,
        "_sanity_data_quality",
        lambda rec, _db, **kw: {"status": "SKIP", "reason": "simulated skip"},
    )
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0})

    result = rec_engine._build_recommendation(_make_opp(), gex, db)
    assert result is None
    assert getattr(rec_engine, "last_rejection_counts", {}).get("required_check_skipped:DATA_QUALITY") == 1


def test_defect4_optional_checks_constant_and_skip_allowed() -> None:
    """OPTIONAL_CHECKS names genuinely optional checks; SKIP on them is allowed when required checks PASS."""
    optional = getattr(recommender_mod, "OPTIONAL_CHECKS", None)
    assert optional is not None
    assert "DATA_QUALITY" not in optional
    assert "LLM_REVIEW" in optional
    assert "HISTORICAL_ANALOG" in optional

    rec_ok = OptionsRecommendation(
        ticker="SPY",
        direction="CALL",
        strike=105.0,
        expiry=EXP_21D.isoformat(),
        entry_price=1.06,
        sanity_status={
            "DATA_QUALITY": {"status": "PASS", "reason": "ok"},
            "DEALER_FLOW": {"status": "SKIP", "reason": "no GEX"},
            "CROSS_ASSET": {"status": "SKIP", "reason": "no journal"},
            "LLM_REVIEW": {"status": "SKIP", "reason": "offline"},
            "HISTORICAL_ANALOG": {"status": "SKIP", "reason": "cold start"},
        },
    )
    assert rec_ok.all_sanity_passed is True


# ── Defect 5: Data-quality SQL must filter by expiry with bound params ──────


def test_defect5_data_quality_sql_filters_by_expiry_with_bound_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_sanity_data_quality must judge the recommended expiry, not another expiry at the same strike."""
    _freeze_date(monkeypatch, RUN_WED)
    exp_other = date(2026, 12, 4)
    exp_recommended = EXP_MONTHLY  # 2026-12-18

    conn = _FakeConn(snapshots=[
        # Different expiry with healthy metrics (would falsely PASS if expiry isn't filtered)
        _contract_row(
            strike=105.0, expiry=exp_other, bid=2.00, ask=2.10,
            implied_vol=0.25, open_interest=5000, snap_date=RUN_WED,
        ),
        # Recommended expiry with bad OI and wide spread (must FAIL)
        _contract_row(
            strike=105.0, expiry=exp_recommended, bid=0.50, ask=1.50,
            implied_vol=0.25, open_interest=10, snap_date=date(2026, 11, 17),
        ),
    ])
    db = _FakeDb(conn)
    rec_engine = OptionsRecommender(db)

    rec = OptionsRecommendation(
        ticker="SPY",
        direction="CALL",
        strike=105.0,
        expiry=exp_recommended.isoformat(),
        entry_price=1.5065,
    )
    result = rec_engine._sanity_data_quality(rec, db)

    assert result["status"] == "FAIL", result
    dq_sql, dq_params = conn.queries[-1]
    assert ":expiry" in dq_sql
    assert "expiry" in dq_params
    assert exp_recommended.isoformat() not in dq_sql  # bound param, never string-formatted


# ── Defect 6: Ask+fee entry, half-spread+fee exit, freshness, DTE from run_date


def test_defect6_entry_pays_ask_plus_fee_and_exit_pays_half_spread_and_fee(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EV and Kelly pay ask + $0.65/contract leg fee; target exit deducts half-spread + fee; mid stays displayed."""
    _freeze_date(monkeypatch, RUN_WED)
    fee_const = getattr(recommender_mod, "OPTION_FEE_PER_CONTRACT", None)
    assert fee_const == pytest.approx(0.65)

    bid, ask = 0.90, 1.10  # mid=1.00, half_spread=0.10, spread=20% (< 25% limit)
    db = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=104.0, expiry=EXP_21D, bid=bid, ask=ask, implied_vol=0.25),
    ]))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)

    opp = _make_opp(direction="CALL", spot=100.0, score=9.0, confidence="HIGH")
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 112.0})

    rec = rec_engine._build_recommendation(opp, gex, db)
    assert rec is not None

    fee_per_share = 0.65 / 100.0
    expected_entry_cost = ask + fee_per_share
    expected_theo = _bs_call_price(112.0, 104.0, 10.5 / 365.0, 0.05, 0.25)
    expected_exit = expected_theo - 0.10 - fee_per_share

    assert rec.entry_price == pytest.approx(expected_entry_cost, abs=1e-4)
    assert rec.entry_bid == pytest.approx(bid)
    assert rec.entry_ask == pytest.approx(ask)
    assert getattr(rec, "entry_mid", None) == pytest.approx(1.00)
    assert rec.target_price == pytest.approx(expected_exit, abs=0.02)

    # EV must reflect ask+fee entry and spread+fee exit (strictly below fee-free mid EV)
    win_prob = rec_engine._estimate_win_probability(opp, {"regime": "SHORT_GAMMA", "call_wall": 112.0})
    fee_free_ev = win_prob * (expected_theo - 1.00) - (1.0 - win_prob) * (1.00 - 0.50)
    assert rec.expected_return < fee_free_ev


def test_defect6_stale_snapshot_rejected_and_trading_calendar_freshness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshots older than the previous trading session are rejected; weekends & holidays are respected."""
    # Case 1: Wednesday 2026-11-18 -> Monday 2026-11-16 is 2 trading sessions old -> REJECT
    _freeze_date(monkeypatch, RUN_WED)
    db_stale = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=105.0, expiry=EXP_21D, snap_date=date(2026, 11, 16)),
    ]))
    rec_engine = OptionsRecommender(db_stale)
    _disable_llm(rec_engine, monkeypatch)
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0})

    assert rec_engine._build_recommendation(_make_opp(), gex, db_stale) is None

    # Case 2: Monday 2026-11-23 -> Friday 2026-11-20 (3 cal days, prev trading session) -> ACCEPT
    mon = date(2026, 11, 23)
    _freeze_date(monkeypatch, mon)
    db_fri = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=105.0, expiry=EXP_MONTHLY, snap_date=date(2026, 11, 20)),
    ]))
    rec_mon = OptionsRecommender(db_fri)
    _disable_llm(rec_mon, monkeypatch)
    assert rec_mon._build_recommendation(_make_opp(), gex, db_fri) is not None

    # Thursday 2026-11-19 on Monday 2026-11-23 -> REJECT
    db_thu = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=105.0, expiry=EXP_MONTHLY, snap_date=date(2026, 11, 19)),
    ]))
    rec_mon2 = OptionsRecommender(db_thu)
    _disable_llm(rec_mon2, monkeypatch)
    assert rec_mon2._build_recommendation(_make_opp(), gex, db_thu) is None

    # Case 3: Tuesday after Labor Day (2026-09-08) -> Friday 2026-09-04 (prev trading session) -> ACCEPT
    tue_after_labor = date(2026, 9, 8)
    _freeze_date(monkeypatch, tue_after_labor)
    db_holiday = _FakeDb(_FakeConn(snapshots=[
        _contract_row(strike=105.0, expiry=date(2026, 10, 16), snap_date=date(2026, 9, 4)),
    ]))
    rec_hol = OptionsRecommender(db_holiday)
    _disable_llm(rec_hol, monkeypatch)
    assert rec_hol._build_recommendation(_make_opp(), gex, db_holiday) is not None


def test_defect6_dte_measured_from_run_date_not_snap_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """DTE must be measured from run_date (Wed -> Fri = 2 DTE < 3 min), not snap_date (Tue -> Fri = 3)."""
    _freeze_date(monkeypatch, RUN_WED)  # Wed 2026-11-18
    fri_2dte = date(2026, 11, 20)       # 2 days from RUN_WED, 3 days from Tue snap_date
    db = _FakeDb(_FakeConn(snapshots=[
        _contract_row(
            strike=105.0, expiry=fri_2dte, snap_date=date(2026, 11, 17),
            bid=0.95, ask=1.05, implied_vol=0.45, open_interest=2000,
        ),
    ]))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0})

    # Measured from run_date, DTE is 2 < _MIN_DTE (3), so contract must be rejected
    rec = rec_engine._build_recommendation(_make_opp(), gex, db)
    assert rec is None


# ── Defect 7: Joint strike/expiry selection, no BS fallback for missing row ─


def test_defect7_joint_strike_expiry_never_invents_missing_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strike and expiry are chosen jointly from rows in the same snapshot; missing pairs reject."""
    _freeze_date(monkeypatch, RUN_WED)
    exp_weekly = date(2026, 11, 27)   # 9 DTE weekly
    exp_monthly = EXP_MONTHLY         # 30 DTE monthly (3rd Friday)

    # Weekly has strike 103 (higher gamma due to short T), monthly ONLY has strike 105.
    # Independent selection picks (103, exp_monthly) which does NOT exist in the chain!
    snapshots = [
        _contract_row(strike=103.0, expiry=exp_weekly, open_interest=700, bid=0.95, ask=1.05, implied_vol=0.25),
        _contract_row(strike=105.0, expiry=exp_monthly, open_interest=5000, bid=0.95, ask=1.05, implied_vol=0.25),
    ]
    db = _FakeDb(_FakeConn(snapshots=snapshots, spot=100.0, atm_iv=0.25))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0})

    rec = rec_engine._build_recommendation(_make_opp(), gex, db)
    assert rec is not None
    # Must be an actual pair from the snapshot: (105.0, exp_monthly), NEVER (103.0, exp_monthly)
    existing_pairs = {(r["strike"], r["expiry"].isoformat()) for r in snapshots}
    assert (rec.strike, rec.expiry) in existing_pairs
    assert (rec.strike, rec.expiry) == (105.0, exp_monthly.isoformat())


def test_defect7_missing_contract_rejects_with_contract_not_in_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no eligible (strike, expiry) exists in chain, reject with 'contract_not_in_chain' (no BS fallback)."""
    _freeze_date(monkeypatch, RUN_WED)
    exp_weekly = date(2026, 11, 27)
    exp_monthly = EXP_MONTHLY

    # Monthly has high OI but only strike 130 (outside OTM window [102, 110]).
    # Weekly has strike 105 but total expiry OI=200 < _MIN_OI_FOR_EXPIRY (500).
    # Independent selection found strike=105 (from weekly) and expiry=monthly, then fell back to BS!
    snapshots = [
        _contract_row(strike=105.0, expiry=exp_weekly, open_interest=200, bid=0.95, ask=1.05, implied_vol=0.25),
        _contract_row(strike=130.0, expiry=exp_monthly, open_interest=5000, bid=0.10, ask=0.12, implied_vol=0.25),
    ]
    db = _FakeDb(_FakeConn(snapshots=snapshots, spot=100.0, atm_iv=0.25))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)
    gex = _make_gex_engine({"regime": "SHORT_GAMMA", "call_wall": 110.0})

    rec = rec_engine._build_recommendation(_make_opp(), gex, db)
    assert rec is None
    assert getattr(rec_engine, "last_rejection_counts", {}).get("contract_not_in_chain") == 1

    # Direct _get_entry_price on a non-existent contract must also return None (no BS fallback)
    assert rec_engine._get_entry_price(db, "SPY", 105.0, exp_monthly, "CALL") is None


# ── Defect 8: to_trade_ticket() stop-loss stock direction ───────────────────


def test_defect8_to_trade_ticket_stop_loss_direction() -> None:
    """CALL stop triggers when stock drops BELOW stop_loss_stock; PUT when stock rises ABOVE."""
    call_rec = OptionsRecommendation(
        ticker="SPY",
        direction="CALL",
        strike=105.0,
        expiry=EXP_21D.isoformat(),
        entry_price=1.06,
        stop_loss=0.53,
        stop_loss_stock=95.0,
    )
    put_rec = OptionsRecommendation(
        ticker="SPY",
        direction="PUT",
        strike=95.0,
        expiry=EXP_21D.isoformat(),
        entry_price=1.06,
        stop_loss=0.53,
        stop_loss_stock=105.0,
    )
    assert "stock below $95.00" in call_rec.to_trade_ticket()
    assert "stock above $105.00" in put_rec.to_trade_ticket()


# ── Rejection counts exposed on run summary ─────────────────────────────────


def test_generate_recommendations_exposes_rejection_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """generate_recommendations exposes per-reason rejection counts without changing its list return."""
    _freeze_date(monkeypatch, RUN_WED)
    snapshots = [
        _contract_row(ticker="AAA", strike=105.0, expiry=EXP_21D, bid=0.95, ask=1.05, implied_vol=0.28),
        _contract_row(ticker="BBB", strike=105.0, expiry=EXP_21D, snap_date=date(2026, 11, 10)),  # stale
        _contract_row(ticker="CCC", strike=103.0, expiry=EXP_21D, bid=0.95, ask=1.05, implied_vol=0.25),
    ]
    db = _FakeDb(_FakeConn(snapshots=snapshots))
    rec_engine = OptionsRecommender(db)
    _disable_llm(rec_engine, monkeypatch)
    monkeypatch.setattr(rec_engine, "_persist_recommendations", lambda _db, recs: len(recs))

    opps = [
        _make_opp(ticker="AAA", direction="CALL", spot=100.0, score=8.5),
        _make_opp(ticker="BBB", direction="CALL", spot=100.0, score=8.5),
        _make_opp(ticker="CCC", direction="CALL", spot=100.0, score=2.0, confidence="LOW"),
    ]
    gex_profiles = {
        "AAA": {"regime": "SHORT_GAMMA", "call_wall": 110.0},
        "BBB": {"regime": "SHORT_GAMMA", "call_wall": 110.0},
        "CCC": {"regime": "NEUTRAL", "call_wall": 104.0},
    }

    class _FakeScanner:
        def __init__(self, db_engine):
            pass

        def scan_all(self, tickers=None, min_score=0.0):
            return opps

    class _FakeGex:
        def __init__(self, db_engine):
            pass

        def compute_gex_profile(self, ticker):
            return gex_profiles[ticker]

    import sys
    import discovery.options_scanner as scanner_mod
    import physics.dealer_gamma as gex_mod

    monkeypatch.setattr(scanner_mod, "OptionsScanner", _FakeScanner)
    monkeypatch.setattr(gex_mod, "DealerGammaEngine", _FakeGex)
    monkeypatch.setitem(sys.modules, "api.main", SimpleNamespace(broadcast_event=lambda *_a, **_k: None))

    recs = rec_engine.generate_recommendations(engine=db)
    assert len(recs) == 1
    assert recs[0].ticker == "AAA"

    counts = getattr(rec_engine, "last_rejection_counts", None)
    assert isinstance(counts, dict)
    assert sum(counts.values()) == 2
    summary = getattr(rec_engine, "last_run_summary", None)
    assert isinstance(summary, dict)
    assert summary["rejection_counts"] == counts
    assert summary["emitted"] == 1
    assert summary["rejected"] == 2
