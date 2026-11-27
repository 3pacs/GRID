"""Unit tests for alpha_research.adapters.signal_adapter publish helpers."""

from __future__ import annotations

import json
import sys
import types
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pytest

from alpha_research.adapters import signal_adapter
from intelligence.signal_registry import make_signal_id


AS_OF = date(2026, 6, 15)

REGISTRY_PARAM_KEYS = {
    "signal_id",
    "source_module",
    "signal_type",
    "ticker",
    "direction",
    "value",
    "z_score",
    "confidence",
    "valid_from",
    "valid_until",
    "freshness_hours",
    "metadata",
    "provenance",
}


class _ExecuteResult:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount


class _RecordingConnection:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _ExecuteResult:
        sql = getattr(stmt, "text", None) or str(stmt)
        self.calls.append((sql, dict(params or {})))
        return _ExecuteResult(self.rowcount)


class _BeginContext:
    def __init__(self, conn: _RecordingConnection) -> None:
        self.conn = conn

    def __enter__(self) -> _RecordingConnection:
        return self.conn

    def __exit__(self, *exc: Any) -> bool:
        return False


class _RecordingEngine:
    def __init__(self, rowcount: int = 1) -> None:
        self.conn = _RecordingConnection(rowcount=rowcount)
        self.begin_calls = 0

    def begin(self) -> _BeginContext:
        self.begin_calls += 1
        return _BeginContext(self.conn)


def _registry_rows(engine: _RecordingEngine) -> list[dict[str, Any]]:
    return [
        params
        for sql, params in engine.conn.calls
        if "INSERT INTO signal_registry" in sql
    ]


def _registry_rows_by_ticker(engine: _RecordingEngine) -> dict[str, dict[str, Any]]:
    return {row["ticker"]: row for row in _registry_rows(engine)}


def _factor_panel() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "AAPL": 0.10,
                "MSFT": 0.90,
                "NVDA": 0.50,
                "TSLA": 0.70,
                "META": 0.30,
            },
            {
                "AAPL": 0.95,
                "MSFT": 0.05,
                "NVDA": 0.50,
                "TSLA": 0.80,
                "META": 0.20,
            },
        ],
        index=pd.to_datetime(["2026-06-14", "2026-06-15"]),
    )


def _module(name: str, **attrs: Any) -> types.ModuleType:
    module = types.ModuleType(name)
    for attr_name, value in attrs.items():
        setattr(module, attr_name, value)
    return module


@pytest.mark.parametrize(
    ("signal_name", "expected"),
    [
        (
            "vol_price_divergence",
            {
                "AAPL": "bearish",
                "MSFT": "bullish",
                "NVDA": "neutral",
                "TSLA": "bearish",
                "META": "bullish",
            },
        ),
        (
            "dual_horizon_equity",
            {
                "AAPL": "bullish",
                "MSFT": "bearish",
                "NVDA": "neutral",
                "TSLA": "bullish",
                "META": "bearish",
            },
        ),
    ],
)
def test_publish_factor_signals_inverts_direction_by_signal_type(
    signal_name: str,
    expected: dict[str, str],
) -> None:
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine,
        signal_name,
        _factor_panel(),
        as_of_date=AS_OF,
        top_pct=0.20,
        confidence=0.75,
        valid_hours=6,
    )

    assert count == 5
    rows = _registry_rows_by_ticker(engine)
    assert {ticker: row["direction"] for ticker, row in rows.items()} == expected


def test_publish_factor_signals_writes_registry_payload_shape() -> None:
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine,
        "dual_horizon_equity",
        _factor_panel(),
        as_of_date=AS_OF,
        confidence=0.72,
        valid_hours=3,
    )

    assert count == 5
    assert engine.begin_calls == 1
    sql, _ = engine.conn.calls[0]
    assert "INSERT INTO signal_registry" in sql
    assert "ON CONFLICT (signal_id, valid_from) DO NOTHING" in sql

    row = _registry_rows_by_ticker(engine)["AAPL"]
    assert set(row) == REGISTRY_PARAM_KEYS
    assert row["signal_id"] == make_signal_id(
        "alpha_research:dual_horizon_equity",
        f"AAPL:{AS_OF}",
    )
    assert row["source_module"] == "alpha_research:dual_horizon_equity"
    assert row["signal_type"] == "DIRECTIONAL"
    assert row["ticker"] == "AAPL"
    assert row["direction"] == "bullish"
    assert row["value"] == pytest.approx(0.9)
    assert row["z_score"] == pytest.approx(0.9)
    assert row["confidence"] == 0.72
    assert row["freshness_hours"] == 3.0
    assert row["valid_from"].tzinfo is timezone.utc
    assert row["valid_until"] - row["valid_from"] == timedelta(hours=3)
    # The unbounded path keeps the original payload: no factor_obs_date, so
    # the registry reader leaves its copies (and their vote) as they were.
    assert json.loads(row["metadata"]) == {
        "signal_name": "dual_horizon_equity",
        "rank": 0.95,
    }
    assert row["provenance"] == f"alpha_research/dual_horizon_equity as_of {AS_OF}"


def test_publish_factor_signals_returns_zero_without_registry_write_for_sparse_panel() -> None:
    engine = _RecordingEngine()
    panel = pd.DataFrame(
        [{"AAPL": 0.9, "MSFT": 0.1, "NVDA": 0.5, "TSLA": 0.8}],
        index=pd.to_datetime(["2026-06-15"]),
    )

    count = signal_adapter.publish_factor_signals(
        engine,
        "dual_horizon_equity",
        panel,
        as_of_date=AS_OF,
    )

    assert count == 0
    assert engine.begin_calls == 0
    assert engine.conn.calls == []


def test_publish_regime_signal_writes_neutral_regime_payload() -> None:
    engine = _RecordingEngine()

    count = signal_adapter.publish_regime_signal(
        engine,
        "vix_exposure",
        "stressed",
        confidence=0.66,
        metadata={"ratio": 1.45, "scalar": 0.55},
        valid_hours=12,
    )

    assert count == 1
    rows = _registry_rows(engine)
    assert len(rows) == 1
    row = rows[0]
    assert set(row) == REGISTRY_PARAM_KEYS
    assert row["signal_id"] == make_signal_id(
        "alpha_research:vix_exposure",
        f"regime:stressed:{row['valid_from'].date()}",
    )
    assert row["source_module"] == "alpha_research:vix_exposure"
    assert row["signal_type"] == "REGIME"
    assert row["ticker"] is None
    assert row["direction"] == "neutral"
    assert row["value"] == 0.66
    assert row["confidence"] == 0.66
    assert row["z_score"] is None
    assert row["freshness_hours"] == 12.0
    assert row["valid_from"].tzinfo is timezone.utc
    assert row["valid_until"] - row["valid_from"] == timedelta(hours=12)
    assert json.loads(row["metadata"]) == {
        "state": "stressed",
        "ratio": 1.45,
        "scalar": 0.55,
    }
    assert row["provenance"] == "alpha_research/vix_exposure"


def test_publish_all_alpha_signals_orchestrates_factor_and_regime_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _Clock.at(monkeypatch, _publish_hour(AS_OF))
    engine = _RecordingEngine()
    calls: list[tuple[str, Any]] = []
    prices = pd.DataFrame(
        {"AAPL": [100.0, 101.0], "MSFT": [50.0, 51.0]},
        index=pd.to_datetime(["2026-06-14", "2026-06-15"]),
    )

    def build_price_panel(engine_arg: Any, start_date: date, end_date: date) -> pd.DataFrame:
        calls.append(("build_price_panel", (engine_arg, start_date, end_date)))
        return prices

    def factor(name: str) -> Any:
        def compute(prices_arg: pd.DataFrame) -> pd.DataFrame:
            calls.append((name, prices_arg))
            return _factor_panel()

        return compute

    def compute_vix(engine_arg: Any, as_of_date: date) -> dict[str, Any]:
        calls.append(("compute_vix_exposure_scalar", (engine_arg, as_of_date)))
        return {"regime_hint": "elevated", "ratio": 1.35, "scalar": 0.65}

    def compute_credit(engine_arg: Any, as_of_date: date) -> dict[str, Any]:
        calls.append(("compute_credit_cycle", (engine_arg, as_of_date)))
        return {"state": "contraction", "confidence": 0.44}

    monkeypatch.setitem(
        sys.modules,
        "alpha_research.data.panel_builder",
        _module(
            "alpha_research.data.panel_builder",
            build_price_panel=build_price_panel,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.exposure_scaler",
        _module(
            "alpha_research.signals.exposure_scaler",
            compute_vix_exposure_scalar=compute_vix,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.credit_cycle",
        _module(
            "alpha_research.signals.credit_cycle",
            compute_credit_cycle=compute_credit,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.quanta_alpha",
        _module(
            "alpha_research.signals.quanta_alpha",
            MIN_RANK_PEERS=5,
            vol_price_divergence=factor("vol_price_divergence"),
            vol_regime_adaptive_equity=factor("vol_regime_adaptive_equity"),
            dual_horizon_equity=factor("dual_horizon_equity"),
        ),
    )

    results = signal_adapter.publish_all_alpha_signals(engine, as_of_date=AS_OF)

    assert results == {
        "vol_price_divergence": 5,
        "vol_regime_equity": 5,
        "dual_horizon_equity": 5,
        "vix_exposure": 1,
        "credit_cycle": 1,
    }
    assert calls[0] == (
        "build_price_panel",
        (engine, AS_OF - timedelta(days=120), AS_OF),
    )
    assert [name for name, _ in calls] == [
        "build_price_panel",
        "vol_price_divergence",
        "vol_regime_adaptive_equity",
        "dual_horizon_equity",
        "compute_vix_exposure_scalar",
        "compute_credit_cycle",
    ]

    source_counts: dict[str, int] = {}
    for row in _registry_rows(engine):
        source_counts[row["source_module"]] = source_counts.get(row["source_module"], 0) + 1

    assert source_counts == {
        "alpha_research:vol_price_divergence": 5,
        "alpha_research:vol_regime_equity": 5,
        "alpha_research:dual_horizon_equity": 5,
        "alpha_research:vix_exposure": 1,
        "alpha_research:credit_cycle": 1,
    }

    for row in _registry_rows(engine):
        meta = json.loads(row["metadata"])
        if row["source_module"] == "alpha_research:vol_price_divergence":
            assert "factor_obs_date" not in meta  # unbounded: payload and vote unchanged
        elif row["source_module"] in ("alpha_research:vol_regime_equity",
                                      "alpha_research:dual_horizon_equity"):
            assert meta["factor_obs_date"] == "2026-06-15"

    vix_row = [
        row
        for row in _registry_rows(engine)
        if row["source_module"] == "alpha_research:vix_exposure"
    ][0]
    assert vix_row["confidence"] == pytest.approx(0.70)
    assert json.loads(vix_row["metadata"])["regime_hint"] == "elevated"


def test_publish_all_alpha_signals_skips_empty_prices_and_unknown_regimes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _RecordingEngine()

    monkeypatch.setitem(
        sys.modules,
        "alpha_research.data.panel_builder",
        _module(
            "alpha_research.data.panel_builder",
            build_price_panel=lambda *_args, **_kwargs: pd.DataFrame(),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.exposure_scaler",
        _module(
            "alpha_research.signals.exposure_scaler",
            compute_vix_exposure_scalar=lambda *_args, **_kwargs: {
                "regime_hint": "unknown"
            },
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.credit_cycle",
        _module(
            "alpha_research.signals.credit_cycle",
            compute_credit_cycle=lambda *_args, **_kwargs: {
                "state": "expansion",
                "confidence": 0,
            },
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.signals.quanta_alpha",
        _module(
            "alpha_research.signals.quanta_alpha",
            MIN_RANK_PEERS=5,
            vol_price_divergence=lambda *_args, **_kwargs: _factor_panel(),
            vol_regime_adaptive_equity=lambda *_args, **_kwargs: _factor_panel(),
            dual_horizon_equity=lambda *_args, **_kwargs: _factor_panel(),
        ),
    )

    results = signal_adapter.publish_all_alpha_signals(engine, as_of_date=AS_OF)

    assert results == {}
    assert engine.begin_calls == 0
    assert engine.conn.calls == []


def test_publish_regime_signal_docstring_has_no_dangling_todo_doc_reference() -> None:
    """Regression: PUNCH-LIST-2026-05-13 line 117.

    The comment block inside ``publish_regime_signal`` used to point readers
    at ``docs/TODO-REGIME-SIGNAL-USAGE.md`` for the "correct architecture"
    follow-up plan, but that doc has never existed in the repo. Drop the
    dangling reference so future readers do not chase a 404.
    """
    import inspect

    source = inspect.getsource(signal_adapter.publish_regime_signal)
    assert "TODO-REGIME-SIGNAL-USAGE" not in source, (
        "publish_regime_signal must not reference the missing "
        "docs/TODO-REGIME-SIGNAL-USAGE.md file"
    )


# ── Session-bounded latest rank (max_sessions_behind) ───────────────────────


def _late_receipt_panel(last_day: str, spy_day: str) -> pd.DataFrame:
    """Six tickers ranked on every row; SPY has no value after ``spy_day``."""
    days = pd.to_datetime(["2026-11-20", "2026-11-23", "2026-11-24", "2026-11-25",
                           "2026-11-27", "2026-11-30"])
    days = days[days <= pd.Timestamp(last_day)]
    tickers = ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLK"]
    rows = []
    for i, day in enumerate(days):
        ranks = {t: (j + 1 + i) % 6 / 6 + 1 / 12 for j, t in enumerate(tickers)}
        if day > pd.Timestamp(spy_day):
            ranks["SPY"] = float("nan")
        rows.append(ranks)
    return pd.DataFrame(rows, index=days)


class _Clock:
    """Pin signal_adapter's wall clock; a bounded row's validity depends on it."""

    @staticmethod
    def at(monkeypatch: pytest.MonkeyPatch, instant: datetime) -> None:
        class _Fixed(datetime):
            @classmethod
            def now(cls, tz=None):
                return instant if tz is None else instant.astimezone(tz)

        monkeypatch.setattr(signal_adapter, "datetime", _Fixed)


def _publish_hour(as_of: date) -> datetime:
    return datetime(as_of.year, as_of.month, as_of.day, 15, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(("as_of", "spy_day", "published"), [
    # Wednesday: Tuesday's settled close is one session back.
    (date(2026, 11, 25), "2026-11-24", True),
    # Wednesday: Monday's close is two sessions back -> fail closed.
    (date(2026, 11, 25), "2026-11-23", False),
    # Friday after Thanksgiving: Wednesday is one NYSE session back.
    (date(2026, 11, 27), "2026-11-25", True),
    # Monday: Friday is one session back, across the weekend.
    (date(2026, 11, 30), "2026-11-27", True),
    # Monday: Wednesday is two sessions back (Thursday was a holiday).
    (date(2026, 11, 30), "2026-11-25", False),
])
def test_late_receipt_ticker_is_published_from_its_own_row_within_one_session(
    as_of: date, spy_day: str, published: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _Clock.at(monkeypatch, _publish_hour(as_of))
    panel = _late_receipt_panel(as_of.isoformat(), spy_day)
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", panel, as_of_date=as_of, max_sessions_behind=1,
    )

    rows = _registry_rows_by_ticker(engine)
    assert ("SPY" in rows) is published
    assert count == (6 if published else 5)
    if published:
        meta = json.loads(rows["SPY"]["metadata"])
        assert meta["factor_obs_date"] == spy_day
        assert meta["rank"] == pytest.approx(panel.loc[spy_day, "SPY"])
        assert meta["rank_universe_size"] == 6
    # Tickers with a value on the last row are published from it, as before.
    assert json.loads(rows["QQQ"]["metadata"])["factor_obs_date"] == as_of.isoformat()
    assert json.loads(rows["QQQ"]["metadata"])["rank"] == pytest.approx(panel.iloc[-1]["QQQ"])


def test_without_a_session_bound_only_the_last_row_is_published() -> None:
    panel = _late_receipt_panel("2026-11-25", "2026-11-24")
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", panel, as_of_date=date(2026, 11, 25),
    )

    assert count == 5
    assert "SPY" not in _registry_rows_by_ticker(engine)
    assert all("factor_obs_date" not in json.loads(r["metadata"])
               for r in _registry_rows(engine))


def test_publish_all_bounds_only_the_equity_momentum_factors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def recorder(engine_arg: Any, signal_name: str, panel: Any, as_of_date: Any = None,
                 **kwargs: Any) -> int:
        seen[signal_name] = (kwargs.get("max_sessions_behind"), kwargs.get("min_row_universe", 5))
        return 0

    prices = pd.DataFrame({"AAPL": [1.0, 2.0]}, index=pd.to_datetime(["2026-06-12", "2026-06-15"]))
    monkeypatch.setattr(signal_adapter, "publish_factor_signals", recorder)
    monkeypatch.setitem(sys.modules, "alpha_research.data.panel_builder", _module(
        "alpha_research.data.panel_builder", build_price_panel=lambda *a, **k: prices))
    monkeypatch.setitem(sys.modules, "alpha_research.signals.exposure_scaler", _module(
        "alpha_research.signals.exposure_scaler",
        compute_vix_exposure_scalar=lambda *a: {"regime_hint": "unknown"}))
    monkeypatch.setitem(sys.modules, "alpha_research.signals.credit_cycle", _module(
        "alpha_research.signals.credit_cycle", compute_credit_cycle=lambda *a: {"confidence": 0}))
    monkeypatch.setitem(sys.modules, "alpha_research.signals.quanta_alpha", _module(
        "alpha_research.signals.quanta_alpha",
        MIN_RANK_PEERS=20,
        vol_price_divergence=lambda p: p,
        vol_regime_adaptive_equity=lambda p: p,
        dual_horizon_equity=lambda p: p,
    ))

    signal_adapter.publish_all_alpha_signals(_RecordingEngine(), as_of_date=AS_OF)

    assert seen == {
        "vol_price_divergence": (None, 5),
        "vol_regime_equity": (1, 20),
        "dual_horizon_equity": (1, 20),
    }


@pytest.mark.parametrize(("obs_day", "published_at", "valid_until"), [
    # Friday's close published Monday 15:00Z: stops voting as Tuesday (UTC) begins.
    ("2026-11-20", datetime(2026, 11, 23, 15, tzinfo=timezone.utc),
     datetime(2026, 11, 24, tzinfo=timezone.utc)),
    # Wednesday's close published Sunday: Thursday 11-26 is a holiday, so the
    # second session after it is Monday 11-30.
    ("2026-11-25", datetime(2026, 11, 29, 15, tzinfo=timezone.utc),
     datetime(2026, 11, 30, tzinfo=timezone.utc)),
    # Tuesday's close published Wednesday: the 24 h window ends first.
    ("2026-11-24", datetime(2026, 11, 25, 15, tzinfo=timezone.utc),
     datetime(2026, 11, 26, 15, tzinfo=timezone.utc)),
])
def test_a_bounded_row_stops_voting_when_its_observation_leaves_the_bound(
    obs_day: str, published_at: datetime, valid_until: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _Clock.at(monkeypatch, published_at)
    as_of = published_at.date()
    panel = _late_receipt_panel(as_of.isoformat(), obs_day)
    engine = _RecordingEngine()

    signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", panel, as_of_date=as_of, max_sessions_behind=1,
    )

    spy = _registry_rows_by_ticker(engine)["SPY"]
    assert json.loads(spy["metadata"])["factor_obs_date"] == obs_day
    assert spy["valid_until"] == valid_until


def test_an_expired_observation_is_not_published_at_all(monkeypatch: pytest.MonkeyPatch) -> None:
    # Monday 11-23's close, published at Wednesday 02:00 UTC with an as_of of
    # Tuesday (a host date lagging UTC): already past its Wednesday 00:00 UTC expiry.
    _Clock.at(monkeypatch, datetime(2026, 11, 25, 2, 0, tzinfo=timezone.utc))
    panel = _late_receipt_panel("2026-11-24", "2026-11-23")
    engine = _RecordingEngine()

    signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", panel, as_of_date=date(2026, 11, 24), max_sessions_behind=1,
    )

    assert "SPY" not in _registry_rows_by_ticker(engine)
    assert "QQQ" in _registry_rows_by_ticker(engine)


# ── Per-row peer coverage (min_row_universe) ────────────────────────────────


def _sparse_latest_panel(prior_names: int) -> pd.DataFrame:
    """Tuesday: ``prior_names`` tickers ranked. Wednesday: QQQ alone (percentile 1.0)."""
    names = ["QQQ"] + [f"T{i}" for i in range(prior_names - 1)]
    tue = {t: (i + 1) / prior_names for i, t in enumerate(names)}
    wed = {t: float("nan") for t in names} | {"QQQ": 1.0}
    return pd.DataFrame([tue, wed], index=pd.to_datetime(["2026-11-24", "2026-11-25"]))


def test_a_lone_name_on_the_newest_row_is_published_from_its_last_populated_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _Clock.at(monkeypatch, _publish_hour(date(2026, 11, 25)))
    panel = _sparse_latest_panel(prior_names=8)
    engine = _RecordingEngine()

    signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", panel, as_of_date=date(2026, 11, 25),
        max_sessions_behind=1, min_row_universe=8,
    )

    meta = json.loads(_registry_rows_by_ticker(engine)["QQQ"]["metadata"])
    assert meta["factor_obs_date"] == "2026-11-24"   # not the 1-name Wednesday row
    assert meta["rank"] == pytest.approx(1 / 8)
    assert meta["rank_universe_size"] == 8


@pytest.mark.parametrize(("prior_names", "published"), [(8, True), (7, False)])
def test_peer_minimum_boundary(prior_names: int, published: bool,
                               monkeypatch: pytest.MonkeyPatch) -> None:
    _Clock.at(monkeypatch, _publish_hour(date(2026, 11, 25)))
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", _sparse_latest_panel(prior_names),
        as_of_date=date(2026, 11, 25), max_sessions_behind=1, min_row_universe=8,
    )

    assert ("QQQ" in _registry_rows_by_ticker(engine)) is published
    assert count == (prior_names if published else 0)


def test_a_lone_name_with_no_populated_row_in_the_bound_is_not_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Thursday 11-26 is a holiday; on Monday 11-30 the bound reaches back to
    # Friday 11-27 only, so Tuesday's populated row is out of reach.
    _Clock.at(monkeypatch, _publish_hour(date(2026, 11, 30)))
    panel = _sparse_latest_panel(prior_names=8)
    panel.index = pd.to_datetime(["2026-11-25", "2026-11-30"])
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", panel, as_of_date=date(2026, 11, 30),
        max_sessions_behind=1, min_row_universe=8,
    )

    assert count == 0


def test_recorded_same_calendar_peers_take_precedence_over_the_row_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A factor that ranked within a calendar group records that group's size."""
    _Clock.at(monkeypatch, _publish_hour(date(2026, 11, 25)))
    panel = _sparse_latest_panel(prior_names=8)
    sizes = panel.notna().astype(float).where(panel.notna())
    sizes.loc[pd.Timestamp("2026-11-24")] = 3.0  # only 3 same-calendar peers that day
    panel.attrs["rank_universe_size"] = sizes
    engine = _RecordingEngine()

    count = signal_adapter.publish_factor_signals(
        engine, "dual_horizon_equity", panel, as_of_date=date(2026, 11, 25),
        max_sessions_behind=1, min_row_universe=8,
    )

    assert count == 0
