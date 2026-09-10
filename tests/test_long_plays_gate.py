"""``intelligence/long_plays.py`` — the three-route coverage gate (2026-09-10).

The board marked 0 entry candidates out of 223 names because the coverage
gate had only two routes and neither could ever fire for a sub-$2 B trial
gem: the 90 d sweep's universe is the edge scanner's 33 playbook names, and
small caps rarely have listed options depth. This file pins the third
route (``trial``), the route each name qualified by, and the fact that
sweep coverage stays authoritative.

No live DB: loaders are monkeypatched or driven by a ``MagicMock`` engine,
the same pattern as ``tests/test_long_plays.py``.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence import long_plays as lp

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2026, 9, 6)


# ── trial_route_qualifies truth table ─────────────────────────────────────


def _trial(**kw: Any) -> bool:
    base: dict[str, Any] = dict(
        signal_type="BUY",
        signal_age_days=2.0,
        cash_runway_score=0.6,
        catalyst_days_out=200.0,
    )
    base.update(kw)
    return lp.trial_route_qualifies(**base)


@pytest.mark.parametrize(
    "kw,expected",
    [
        ({}, True),
        ({"signal_type": "WATCHLIST"}, True),
        ({"signal_type": "watchlist"}, True),  # case-insensitive
        ({"signal_type": "AVOID"}, False),
        ({"signal_type": None}, False),
        ({"signal_type": ""}, False),
        # recency: inclusive at the 30 d boundary, nothing older, nothing future-stamped
        ({"signal_age_days": 0.0}, True),
        ({"signal_age_days": 30.0}, True),
        ({"signal_age_days": 30.01}, False),
        ({"signal_age_days": -1.0}, False),
        ({"signal_age_days": None}, False),
        # runway: the unit score, inclusive at 0.4
        ({"cash_runway_score": 0.4}, True),
        ({"cash_runway_score": 0.39}, False),
        ({"cash_runway_score": None}, False),
        ({"cash_runway_score": float("nan")}, False),
        # OLMA on 2026-09-10: BUY, but 3.2 months of cash = 0.133 unit score
        ({"cash_runway_score": 0.1333}, False),
        # catalyst: inside 18 months, not in the past
        ({"catalyst_days_out": 0.0}, True),
        ({"catalyst_days_out": 548.0}, True),
        ({"catalyst_days_out": 549.0}, False),
        ({"catalyst_days_out": -1.0}, False),
        ({"catalyst_days_out": None}, False),
    ],
)
def test_trial_route_truth_table(kw: dict[str, Any], expected: bool) -> None:
    assert _trial(**kw) is expected


# ── coverage_route truth table ────────────────────────────────────────────


def _route(**kw: Any) -> str | None:
    base: dict[str, Any] = dict(
        sweep_verdict=None,
        has_sweep_coverage=False,
        options_payoff_multiple=None,
        has_trial_route=False,
    )
    base.update(kw)
    return lp.coverage_route(**base)


@pytest.mark.parametrize(
    "kw,expected",
    [
        # sweep coverage is authoritative in both directions
        ({"has_sweep_coverage": True, "sweep_verdict": "high"}, "sweep"),
        ({"has_sweep_coverage": True, "sweep_verdict": "medium"}, "sweep"),
        ({"has_sweep_coverage": True, "sweep_verdict": "MEDIUM"}, "sweep"),
        ({"has_sweep_coverage": True, "sweep_verdict": "moderate"}, "sweep"),
        ({"has_sweep_coverage": True, "sweep_verdict": "low"}, None),
        ({"has_sweep_coverage": True, "sweep_verdict": "no_trade"}, None),
        ({"has_sweep_coverage": True, "sweep_verdict": None}, None),
        # ...and neither fallback can rescue a covered-but-weak name
        ({"has_sweep_coverage": True, "sweep_verdict": "low", "options_payoff_multiple": 90.0}, None),
        ({"has_sweep_coverage": True, "sweep_verdict": "low", "has_trial_route": True}, None),
        # uncovered: options, then trial
        ({"options_payoff_multiple": 20.0}, "options"),
        ({"options_payoff_multiple": 19.9}, None),
        ({"has_trial_route": True}, "trial"),
        ({"options_payoff_multiple": 90.0, "has_trial_route": True}, "options"),
        ({"options_payoff_multiple": 1.0, "has_trial_route": True}, "trial"),
        ({}, None),
    ],
)
def test_coverage_route_truth_table(kw: dict[str, Any], expected: str | None) -> None:
    assert _route(**kw) == expected


def test_medium_is_the_producers_vocabulary() -> None:
    """``universe_ranker`` writes high/medium — never "moderate"."""
    from intelligence.universe_ranker import _RANKABLE_VERDICTS

    assert _RANKABLE_VERDICTS <= lp.ENTRY_SWEEP_VERDICTS
    assert "medium" in lp.ENTRY_SWEEP_VERDICTS


# ── evaluate_gate: the three gates, and classify_stance agrees ────────────


def _gate(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        p50_3y_multiple=2.0,
        p10_3y_multiple=0.8,
        max_drawdown=-0.4,
        sweep_verdict=None,
        has_sweep_coverage=False,
        catalyst_strength_12m=None,
        options_payoff_multiple=None,
        has_catalyst=False,
        has_trial_route=False,
    )
    base.update(kw)
    return lp.evaluate_gate(**base)


def test_evaluate_gate_reports_each_gate_and_the_route() -> None:
    out = _gate(has_trial_route=True)
    assert out == {
        "stance": "entry_candidate",
        "return_gate": True,
        "drawdown_gate": True,
        "coverage_gate": True,
        "coverage_route": "trial",
    }
    # the trial route alone does not carry a name past return/drawdown
    assert _gate(has_trial_route=True, p50_3y_multiple=1.1)["stance"] == "watch"
    assert _gate(has_trial_route=True, max_drawdown=-0.9)["stance"] == "watch"
    blocked = _gate(has_trial_route=True, max_drawdown=-0.9)
    assert blocked["coverage_gate"] is True and blocked["drawdown_gate"] is False
    # no route at all
    none_route = _gate()
    assert none_route["coverage_gate"] is False and none_route["coverage_route"] is None
    assert none_route["stance"] == "watch"


@pytest.mark.parametrize(
    "kw",
    [
        {},
        {"has_trial_route": True},
        {"has_sweep_coverage": True, "sweep_verdict": "medium"},
        {"options_payoff_multiple": 50.0},
        {"p50_3y_multiple": 0.9, "p10_3y_multiple": 0.2},
        {"p50_3y_multiple": None, "max_drawdown": None},
    ],
)
def test_classify_stance_is_the_stance_field_of_evaluate_gate(kw: dict[str, Any]) -> None:
    base: dict[str, Any] = dict(
        p50_3y_multiple=2.0,
        p10_3y_multiple=0.8,
        max_drawdown=-0.4,
        sweep_verdict=None,
        has_sweep_coverage=False,
    )
    base.update(kw)
    assert lp.classify_stance(**base) == lp.evaluate_gate(**base)["stance"]


def test_classify_stance_default_keeps_the_pre_trial_behaviour() -> None:
    """Callers that never heard of the trial route see no change."""
    assert lp.classify_stance(
        p50_3y_multiple=2.0, p10_3y_multiple=0.8, max_drawdown=-0.4,
        sweep_verdict=None, has_sweep_coverage=False,
    ) == "watch"


# ── _next_catalyst ────────────────────────────────────────────────────────


def test_next_catalyst_prefers_the_calendar_then_falls_back_to_the_trial() -> None:
    events = [
        {"days_out": 400, "expected_date": "2027-10-11"},
        {"days_out": 90, "expected_date": "2026-12-05"},
        {"days_out": -3, "expected_date": "2026-09-03"},  # past events ignored
    ]
    assert lp._next_catalyst(events, None, AS_OF) == (90, "2026-12-05")

    # no calendar row → the trial's own primary completion date is the readout
    trial = {"primary_completion_date": "2026-10-31"}
    days, when = lp._next_catalyst([], trial, AS_OF)
    assert when == "2026-10-31" and days == (date(2026, 10, 31) - AS_OF).days

    assert lp._next_catalyst([], None, AS_OF) == (None, None)
    assert lp._next_catalyst([], {"primary_completion_date": "2026-01-01"}, AS_OF) == (None, None)
    assert lp._next_catalyst([], {"primary_completion_date": "not-a-date"}, AS_OF) == (None, None)
    assert lp._next_catalyst([{"days_out": None}], None, AS_OF) == (None, None)


# ── _load_trial_tickers: gate columns, PIT bounds, parameterised ──────────


def _engine(rows: list[Any] | None = None) -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    def execute(stmt: Any, params: Any = None) -> MagicMock:
        result = MagicMock()
        result.fetchall.return_value = rows or []
        result.first.return_value = None
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine, conn


def test_load_trial_tickers_carries_the_gate_columns_and_is_pit_bounded() -> None:
    created = datetime(2026, 9, 4, 8, 0, tzinfo=timezone.utc)
    rows = [
        ("olma", "Olema", "oncology", 906.48, "buy", 0.1333, 3.2, 0.6633, date(2026, 10, 31), created),
        ("TECX", "Tectonic", "immunology", 595.86, "WATCHLIST", 1.0, 37.0, 0.57, date(2026, 11, 24), created),
        (None, "skip", None, None, None, None, None, None, None, None),
    ]
    engine, conn = _engine(rows=rows)
    out = lp._load_trial_tickers(engine, AS_OF)

    assert set(out) == {"OLMA", "TECX"}
    olma = out["OLMA"]
    assert olma["signal_type"] == "BUY"  # normalised upper
    assert olma["cash_runway_score"] == pytest.approx(0.1333)
    assert olma["cash_runway_months"] == 3.2
    assert olma["primary_completion_date"] == "2026-10-31"
    assert olma["market_cap_mm"] == 906.48
    # as_of is end-of-day 2026-09-06; the row is stamped 2026-09-04 08:00
    assert olma["signal_age_days"] == pytest.approx(2.67, abs=0.02)

    stmt, params = conn.execute.call_args.args
    sql = str(stmt)
    assert "FROM trial_signals" in sql
    assert "cash_runway_score" in sql and "signal_type" in sql
    # bound on both sides, both binds
    assert "created_at >= :start" in sql and "created_at <= :as_of_ts" in sql
    assert params["as_of_ts"].date() == AS_OF and params["as_of_ts"].tzinfo is not None
    assert params["start"] == params["as_of_ts"] - timedelta(days=lp.TRIAL_LOOKBACK_DAYS)


def test_load_trial_tickers_tolerates_a_null_created_at() -> None:
    rows = [("X", "X Co", None, None, "BUY", 0.9, None, None, None, None)]
    engine, _ = _engine(rows=rows)
    out = lp._load_trial_tickers(engine, AS_OF)
    assert out["X"]["signal_age_days"] is None
    assert lp.trial_route_qualifies(
        signal_type=out["X"]["signal_type"],
        signal_age_days=out["X"]["signal_age_days"],
        cash_runway_score=out["X"]["cash_runway_score"],
        catalyst_days_out=100.0,
    ) is False


# ── board: the trial route end to end ─────────────────────────────────────


def _series(years: float = 10.0, cagr: float = 0.25, start: float = 10.0, wobble: float = 0.05) -> list[tuple[date, float]]:
    n_days = int(years * 365.25)
    first = AS_OF - timedelta(days=n_days)
    return [
        (
            first + timedelta(days=i),
            start * math.exp(math.log1p(cagr) * (i / 365.25)) * (1.0 + wobble * math.sin((i / 365.25) * 4.0)),
        )
        for i in range(0, n_days)
    ]


def _trial_gem_board(monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> dict[str, Any]:
    """A board whose only interesting name is an uncovered small-cap trial gem."""
    defaults: dict[str, Any] = {
        "_playbook_index": lambda: {},
        "_load_trial_tickers": lambda engine, as_of: {
            # clears the trial route: WATCHLIST, fresh, runway 0.9, readout in 5 months
            "GEMX": {
                "company_name": "Gemx Bio", "primary_indication": "oncology", "market_cap_mm": 700.0,
                "signal_type": "WATCHLIST", "cash_runway_score": 0.9, "cash_runway_months": 21.6,
                "trial_strength_score": 0.5, "primary_completion_date": (as_of + timedelta(days=150)).isoformat(),
                "signal_age_days": 1.0,
            },
            # BUY but 3.2 months of cash — the runway gate disqualifies it
            "THIN": {
                "company_name": "Thin Runway Bio", "primary_indication": "cns", "market_cap_mm": 900.0,
                "signal_type": "BUY", "cash_runway_score": 0.1333, "cash_runway_months": 3.2,
                "trial_strength_score": 0.5, "primary_completion_date": (as_of + timedelta(days=55)).isoformat(),
                "signal_age_days": 1.0,
            },
            # stale signal (older than 30 d)
            "STAL": {
                "company_name": "Stale Bio", "primary_indication": "cns", "market_cap_mm": 400.0,
                "signal_type": "BUY", "cash_runway_score": 0.9, "cash_runway_months": 21.6,
                "trial_strength_score": 0.5, "primary_completion_date": (as_of + timedelta(days=100)).isoformat(),
                "signal_age_days": 45.0,
            },
        },
        "_load_catalysts": lambda engine, as_of: {},
        "_load_options_asymmetry": lambda engine, as_of: {},
        "_load_adj_close": lambda engine, tickers, years, as_of: {
            "GEMX": _series(10.0, 0.30),
            "THIN": _series(10.0, 0.30),
            "STAL": _series(10.0, 0.30),
        },
        "_load_market_caps": lambda engine, tickers, as_of: {},
        "_load_sweep": lambda engine: {},  # sweep ran, no rankable verdicts
        "_load_realized_alpha": lambda engine: None,
        "_company_name": lambda ticker: None,
        "_load_company_profiles": lambda engine, as_of: {},
    }
    defaults.update(overrides)
    for name, fn in defaults.items():
        monkeypatch.setattr(lp, name, fn)
    return lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=100)


def test_trial_route_makes_the_gate_reachable_for_an_uncovered_small_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    board = _trial_gem_board(monkeypatch)
    by_ticker = {c["ticker"]: c for c in board["candidates"]}

    gem = by_ticker["GEMX"]
    assert gem["stance"] == "entry_candidate"
    assert gem["coverage_route"] == "trial"
    assert gem["gate"]["coverage_gate"] is True
    assert gem["gate"]["return_gate"] is True and gem["gate"]["drawdown_gate"] is True
    assert "route trial" in gem["why"]
    # the row carries what an entry candidate is judged on
    assert gem["gate"]["cash_runway_months"] == 21.6
    assert gem["gate"]["next_catalyst_date"] == (AS_OF + timedelta(days=150)).isoformat()
    assert gem["gate"]["next_catalyst_days_out"] == 150
    assert gem["gate"]["p50_3y_multiple"] == gem["projection"]["3y"]["p50_multiple"]
    assert gem["gate"]["p90_3y_multiple"] == gem["projection"]["3y"]["p90_multiple"]
    assert gem["market_cap_usd"] == 700e6 and gem["market_cap_bucket"] == "small"
    # runway falls back to the trial snapshot when no profile is enriched yet
    assert gem["fundamentals"]["cash_runway_months"] == 21.6

    # a thin-runway BUY and a stale signal both stay out
    for ticker in ("THIN", "STAL"):
        assert by_ticker[ticker]["stance"] != "entry_candidate"
        assert by_ticker[ticker]["coverage_route"] is None
        assert by_ticker[ticker]["gate"]["coverage_gate"] is False

    assert board["entry_candidates"] >= 1
    assert board["entry_candidates_by_route"]["trial"] >= 1
    assert board["entry_candidates_by_route"]["options"] == 0
    assert board["stand_down_reason"] is None


def test_options_route_wins_when_both_qualify(monkeypatch: pytest.MonkeyPatch) -> None:
    board = _trial_gem_board(
        monkeypatch,
        _load_options_asymmetry=lambda engine, as_of: {
            "GEMX": {
                "max_payoff_multiple": 40.0, "score": 0.7, "direction": "call",
                "thesis": "cheap tail", "is_100x": False, "scan_date": as_of.isoformat(),
            }
        },
    )
    gem = next(c for c in board["candidates"] if c["ticker"] == "GEMX")
    assert gem["stance"] == "entry_candidate" and gem["coverage_route"] == "options"
    assert board["entry_candidates_by_route"]["options"] == 1


def test_a_covered_but_weak_sweep_verdict_still_blocks_the_trial_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    board = _trial_gem_board(
        monkeypatch,
        _load_sweep=lambda engine: {
            "GEMX": {"verdict": "low", "composite_score": 0.1, "horizon_days": 90, "generated_at": None}
        },
    )
    gem = next(c for c in board["candidates"] if c["ticker"] == "GEMX")
    assert gem["stance"] == "watch" and gem["coverage_route"] is None


def test_a_medium_sweep_verdict_now_passes_the_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """The producer writes "medium"; the gate used to only accept "moderate"."""
    board = _trial_gem_board(
        monkeypatch,
        _load_trial_tickers=lambda engine, as_of: {},
        _load_adj_close=lambda engine, tickers, years, as_of: {"CCJ": _series(10.0, 0.30)},
        _load_sweep=lambda engine: {
            "CCJ": {"verdict": "medium", "composite_score": 0.9, "horizon_days": 90, "generated_at": None}
        },
    )
    ccj = next(c for c in board["candidates"] if c["ticker"] == "CCJ")
    assert ccj["stance"] == "entry_candidate" and ccj["coverage_route"] == "sweep"
    assert board["entry_candidates_by_route"]["sweep"] == 1


def test_stand_down_reason_counts_which_gate_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    board = _trial_gem_board(
        monkeypatch,
        _load_trial_tickers=lambda engine, as_of: {
            "FLAT": {
                "company_name": "Flat", "primary_indication": None, "market_cap_mm": 100.0,
                "signal_type": "BUY", "cash_runway_score": 0.9, "cash_runway_months": 21.6,
                "trial_strength_score": 0.5, "primary_completion_date": (as_of + timedelta(days=100)).isoformat(),
                "signal_age_days": 1.0,
            }
        },
        # a flat chart: clears the drawdown gate, fails the return gate
        _load_adj_close=lambda engine, tickers, years, as_of: {"FLAT": _series(10.0, 0.0, wobble=0.0)},
    )
    assert board["entry_candidates"] == 0
    reason = board["stand_down_reason"]
    assert "no entry candidates" in reason
    assert "a coverage route" in reason and "live trial signal" in reason
    assert "Failing: return" in reason
    assert board["entry_candidates_by_route"] == {"sweep": 0, "options": 0, "trial": 0}


def test_an_entry_candidate_below_top_k_is_never_truncated_away(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A gated name must not be dropped for a higher-scoring ``watch``."""
    board = _trial_gem_board(monkeypatch, )  # GEMX is the only entry candidate
    gem_score = next(c for c in board["candidates"] if c["ticker"] == "GEMX")["asymmetry_score"]
    assert gem_score is not None

    # top_k=1 would keep only the single highest-scoring row
    tight = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=1)
    tickers = [c["ticker"] for c in tight["candidates"]]
    assert "GEMX" in tickers
    assert tight["entry_candidates"] == 1
    assert tight["entry_candidates_by_route"]["trial"] == 1
    assert tight["stand_down_reason"] is None
    # still ordered by score, and the extra row is disclosed
    scores = [c["asymmetry_score"] for c in tight["candidates"]]
    assert scores == sorted(scores, reverse=True)
    if len(tight["candidates"]) > 1:
        assert any("kept past top_k=1" in n for n in tight["method_notes"])


# ── serialization ─────────────────────────────────────────────────────────


def test_board_rows_stay_json_safe_with_the_new_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    board = _trial_gem_board(monkeypatch)
    json.dumps(board)  # whole board, not just a row
    for cand in board["candidates"]:
        assert {"coverage_route", "gate", "trial_signal"} <= set(cand)
        assert set(cand["gate"]) == {
            "return_gate", "drawdown_gate", "coverage_gate", "coverage_route",
            "cash_runway_months", "next_catalyst_date", "next_catalyst_days_out",
            "p50_3y_multiple", "p90_3y_multiple", "max_drawdown",
        }
        assert cand["coverage_route"] in {None, "sweep", "options", "trial"}
        assert cand["coverage_route"] == cand["gate"]["coverage_route"]


def test_persisted_board_round_trips_the_route(monkeypatch: pytest.MonkeyPatch) -> None:
    board = _trial_gem_board(monkeypatch)
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.first.return_value = (7,)
    engine = MagicMock()
    engine.begin.return_value = conn
    assert lp.persist_board(engine, board) == 7

    insert = [c for c in conn.execute.call_args_list if "INSERT INTO long_plays_board" in str(c.args[0])][0]
    persisted = json.loads(insert.args[1]["candidates"])
    gem = next(c for c in persisted if c["ticker"] == "GEMX")
    assert gem["coverage_route"] == "trial" and gem["gate"]["coverage_gate"] is True


def test_load_latest_board_reports_route_counts() -> None:
    candidates = [
        {"ticker": "GEMX", "stance": "entry_candidate", "coverage_route": "trial", "asymmetry_score": 0.4},
        {"ticker": "CCJ", "stance": "entry_candidate", "coverage_route": "sweep", "asymmetry_score": 0.6},
        {"ticker": "UEC", "stance": "avoid", "coverage_route": None, "asymmetry_score": 0.1},
        "not a dict",
    ]
    row = (9, AS_OF, datetime(2026, 9, 6, 5, 30, tzinfo=timezone.utc), 31, json.dumps(candidates), None, [])
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.first.return_value = row
    engine = MagicMock()
    engine.connect.return_value = conn

    out = lp.load_latest_board(engine)
    assert out is not None
    assert out["entry_candidates"] == 2
    assert out["entry_candidates_by_route"] == {"sweep": 1, "options": 0, "trial": 1}


# ── entry_first ordering (shared by both digest surfaces) ─────────────────


def test_entry_first_puts_gated_names_above_higher_scores() -> None:
    rows = [
        {"ticker": "HIGH", "stance": "watch", "asymmetry_score": 0.9},
        {"ticker": "GEMX", "stance": "entry_candidate", "asymmetry_score": 0.4},
        {"ticker": "CCJ", "stance": "entry_candidate", "asymmetry_score": 0.5},
        {"ticker": "BAD", "stance": "avoid", "asymmetry_score": 0.95},
        {"ticker": "NULL", "stance": "watch", "asymmetry_score": None},
    ]
    assert [c["ticker"] for c in lp.entry_first(rows)] == ["CCJ", "GEMX", "BAD", "HIGH", "NULL"]
    assert lp.entry_first([]) == []
    assert lp.entry_first(["junk", {"ticker": "A", "stance": "watch"}]) == [{"ticker": "A", "stance": "watch"}]


# ── universe export: the sweep and the board score the same names ─────────


def test_long_plays_universe_matches_the_board_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    _trial_gem_board(monkeypatch)  # installs the fake loaders
    universe = lp.long_plays_universe(MagicMock(), as_of=AS_OF)
    assert "GEMX" in universe and "THIN" in universe
    assert universe == sorted(set(universe))


def test_long_plays_universe_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("relation does not exist")

    for name in ("_load_company_profiles", "_load_trial_tickers", "_load_catalysts", "_load_options_asymmetry", "_playbook_index"):
        monkeypatch.setattr(lp, name, boom)
    assert isinstance(lp.long_plays_universe(MagicMock(), as_of=AS_OF), list)


# ── wiring pins ───────────────────────────────────────────────────────────


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_the_weekly_sweep_covers_the_long_plays_universe() -> None:
    src = _read("intelligence/scheduler.py")
    block = src[src.index("def _long_horizon_sweep"): src.index("def _long_plays_weekly")]
    assert "from intelligence.long_plays import long_plays_universe" in block
    assert "playbook_pool | board_pool" in block
    # a failing board universe must not kill the sweep
    assert "board_pool = set()" in block


def test_both_digest_surfaces_list_entry_candidates_first() -> None:
    email_src = _read("alerts/email.py")
    block = email_src[email_src.index("def daily_digest"): email_src.index("def _section_code_block")]
    assert "from intelligence.long_plays import entry_first, load_latest_board" in block
    assert "entry_first(board.get(\"candidates\") or [])" in block
    assert "coverage_route" in block

    digest_src = _read("scripts/daily_digest.py")
    assert "def _build_long_plays_section" in digest_src
    assert "from intelligence.long_plays import entry_first" in digest_src
    assert "_build_long_plays_section(long_plays)" in digest_src


def test_raw_series_reads_are_bounded_on_both_sides() -> None:
    for rel, table in (("scripts/daily_digest.py", "raw_series"), ("intelligence/long_plays.py", "raw_series")):
        src = _read(rel)
        for chunk in src.split(f"FROM {table}")[1:]:
            window = chunk[:600]
            assert ">=" in window and "<=" in window, rel


def test_new_sql_has_no_fstrings() -> None:
    src = _read("intelligence/long_plays.py")
    assert 'f"""' not in src and "f'''" not in src and ".format(" not in src
