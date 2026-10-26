"""Tests for `status` — must report activity/data-quality counts only,
NEVER any outcome (return, held/broke, win rate). See status.py's module
docstring: this is enforced "by construction", and the tests below try
hard to catch a regression, not just confirm the happy path.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from paper_log.gex_levels.status import compute_status, format_status
from paper_log.gex_levels.storage import PaperLogStore

RUN_AT = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)


def _preopen(d: date, *, excluded: bool = False, reason: str | None = None) -> dict:
    return {
        "kind": "preopen", "run_at": RUN_AT, "session_date": d, "code_sha": "x",
        "excluded": excluded, "exclusion_reason": reason,
    }


def _postclose(
    d: date, *, excluded: bool = False, reason: str | None = None,
    reaches: dict | None = None, h3_trade: dict | None = None,
) -> dict:
    return {
        "kind": "postclose", "run_at": RUN_AT, "session_date": d, "code_sha": "x",
        "excluded": excluded, "exclusion_reason": reason,
        "reaches": reaches, "h3_trade": h3_trade,
    }


def _reach(status: str, held: bool | None) -> dict:
    return {"status": status, "side": "above", "bar_time": None, "held": held}


def _trade(triggered: bool, return_pct: float | None = None) -> dict:
    return {
        "triggered": triggered, "regime_rule": "follow", "direction": "long" if triggered else None,
        "trigger_level_name": "call_wall" if triggered else None, "trigger_time": None,
        "raw_entry_price": None, "raw_exit_price": None, "entry_price_after_cost": None,
        "exit_price_after_cost": None, "return_pct": return_pct, "pnl_usd": None,
    }


def test_counts_preopen_and_postclose_records(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append(_preopen(date(2026, 9, 21)))
    store.append(_postclose(date(2026, 9, 21)))
    store.append(_preopen(date(2026, 9, 22)))

    report = compute_status(tmp_path)
    assert report.sessions_preopen == 2
    assert report.sessions_postclose == 1


def test_valid_sessions_requires_both_legs_non_excluded(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append(_preopen(date(2026, 9, 21)))
    store.append(_postclose(date(2026, 9, 21)))  # valid pair

    store.append(_preopen(date(2026, 9, 22), excluded=True, reason="no_chain"))
    store.append(_postclose(date(2026, 9, 22), excluded=True, reason="no_chain"))  # excluded pair

    store.append(_preopen(date(2026, 9, 23)))  # postclose not run yet — not valid, not excluded

    report = compute_status(tmp_path)
    assert report.valid_sessions == 1


def test_excluded_by_reason_tallies_across_both_kinds(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append(_preopen(date(2026, 9, 21), excluded=True, reason="stale_chain"))
    store.append(_postclose(date(2026, 9, 21), excluded=True, reason="stale_chain"))
    store.append(_preopen(date(2026, 9, 22), excluded=True, reason="market_closed"))

    report = compute_status(tmp_path)
    assert report.excluded_by_reason == {"stale_chain": 2, "market_closed": 1}


def test_reach_and_trade_counts_from_valid_postclose_only(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append(_preopen(date(2026, 9, 21)))
    store.append(_postclose(
        date(2026, 9, 21),
        reaches={
            "real": {"gamma_flip": _reach("reached", True), "put_wall": _reach("none", None), "call_wall": _reach("gap_through", None)},
            "placebo": {"gamma_flip": _reach("reached", False), "put_wall": None, "call_wall": _reach("none", None)},
        },
        h3_trade={"real": _trade(True, 0.01), "placebo": _trade(False)},
    ))
    # An excluded postclose's reach/trade data must NOT be counted.
    store.append(_preopen(date(2026, 9, 22), excluded=True, reason="stale_chain"))
    store.append(_postclose(
        date(2026, 9, 22), excluded=True, reason="stale_chain",
        reaches={"real": {"gamma_flip": _reach("reached", True)}, "placebo": {}},
        h3_trade={"real": _trade(True, 0.99), "placebo": _trade(True, 0.99)},
    ))

    report = compute_status(tmp_path)
    assert report.real_reaches == 1
    assert report.placebo_reaches == 1
    assert report.real_trades == 1
    assert report.placebo_trades == 0


def test_stop_review_advisory_excludes_market_closed_from_denominator(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    # 25 valid, 4 excluded for a real reason, 5 market_closed (set aside
    # entirely) -> 29 "real" sessions, 4/29 = 13.8% > 10%, and 29 >= 30? No —
    # need >= 30 real sessions for the advisory to even consider firing.
    for i in range(25):
        store.append(_preopen(date(2026, 1, 1) + _d(i)))
    for i in range(4):
        store.append(_preopen(date(2026, 2, 1) + _d(i), excluded=True, reason="stale_chain"))
    for i in range(5):
        store.append(_preopen(date(2026, 3, 1) + _d(i), excluded=True, reason="market_closed"))

    report = compute_status(tmp_path)
    # 25 + 4 = 29 real sessions < 30 -> no advisory yet.
    assert report.stop_review_advisory is None

    # One more real (excluded) session pushes real-session count to 30.
    store.append(_preopen(date(2026, 4, 1), excluded=True, reason="stale_chain"))
    report2 = compute_status(tmp_path)
    assert report2.stop_review_advisory is not None
    assert "market_closed" in report2.stop_review_advisory
    assert "5/30" in report2.stop_review_advisory  # 5 excluded / 30 real sessions


def _d(i: int):
    from datetime import timedelta
    return timedelta(days=i)


def test_format_status_never_mentions_outcome_words(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append(_preopen(date(2026, 9, 21)))
    store.append(_postclose(
        date(2026, 9, 21),
        reaches={
            "real": {"gamma_flip": _reach("reached", True), "put_wall": _reach("reached", False), "call_wall": _reach("none", None)},
            "placebo": {"gamma_flip": _reach("reached", True), "put_wall": None, "call_wall": None},
        },
        h3_trade={"real": _trade(True, 0.1234), "placebo": _trade(True, -0.0456)},
    ))

    report = compute_status(tmp_path)
    text = format_status(report).lower()

    for forbidden in ("held", "broke", "return", "pnl", "win", "hit rate", "profit", "loss", "0.1234", "0.0456"):
        assert forbidden not in text, f"status output leaked outcome-shaped text: {forbidden!r}"


def test_format_status_is_human_readable(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append(_preopen(date(2026, 9, 21)))
    report = compute_status(tmp_path)
    text = format_status(report)
    assert "preopen records:   1" in text
    assert "activity and data quality only" in text
