"""Tests for evaluation/prices.py (workstream W3d).

Pure Python — no database, no network. A small hand-rolled fake engine
stands in for SQLAlchemy: its ``.connect()``/``.execute()`` dispatch on the
SQL text (checking which table is being queried), which lets these tests
exercise the REAL ``store/pit.py::PITStore.get_pit`` code path (not a
reimplementation of it) without a live PostgreSQL — ``store/pit.py``'s own
correctness is covered separately by ``test_pit.py`` against a real
database.
"""

from __future__ import annotations

from datetime import date

import pytest

from evaluation.prices import (
    BAR_KIND,
    DEFAULT_SANITY_BOUNDS,
    AmbiguousInstrumentError,
    PITPriceAccessor,
    PriceSeriesContract,
    REASON_PRICE_OUT_OF_BOUNDS,
    UnsupportedInstrumentError,
)
from evaluation.signal_outcomes import UnsupportedInstrumentError as SignalOutcomesUnsupported


# ---------------------------------------------------------------------------
# Fake engine — dispatches on SQL text, real semantics in plain Python
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc_info):
        return False


class _FakeUrl:
    def __init__(self, database: str):
        self.database = database


class FakeConnection:
    """Answers exactly the queries evaluation/prices.py and store/pit.py
    issue, dispatched on SQL substring — nothing more general than that."""

    def __init__(self, *, feature_rows, source_rows, resolved_rows):
        self.feature_rows = feature_rows  # list[(id, name)]
        self.source_rows = source_rows  # list[(id, name)]
        self.resolved_rows = resolved_rows  # list[dict]

    def execute(self, query, params=None):
        sql = str(query)
        params = params or {}

        if "FROM feature_registry" in sql:
            names = set(params.get("names", []))
            rows = [(fid, name) for fid, name in self.feature_rows if name in names]
            return _FakeResult(rows)

        if "FROM source_catalog" in sql:
            wanted = params.get("name", "").lower()
            rows = [(sid, name) for sid, name in self.source_rows if name.lower() == wanted]
            return _FakeResult(rows)

        if "FROM resolved_series" in sql:
            fids = set(params.get("fids", []))
            aod = params["aod"]
            matches = [
                r
                for r in self.resolved_rows
                if r["feature_id"] in fids and r["obs_date"] <= aod and r["release_date"] <= aod
            ]
            # DISTINCT ON (feature_id, obs_date), latest vintage_date wins
            # (mirrors store/pit.py's LATEST_AS_OF branch — the only policy
            # PITPriceAccessor uses).
            best: dict[tuple, dict] = {}
            for r in matches:
                key = (r["feature_id"], r["obs_date"])
                if key not in best or r["vintage_date"] > best[key]["vintage_date"]:
                    best[key] = r
            rows = [
                (r["feature_id"], r["obs_date"], r["value"], r["release_date"], r["vintage_date"])
                for r in best.values()
            ]
            return _FakeResult(rows)

        raise AssertionError(f"FakeConnection cannot answer this query: {sql!r}")


class FakeEngine:
    def __init__(self, conn: FakeConnection, db_name: str = "griddb_test"):
        self._conn = conn
        self.url = _FakeUrl(db_name)

    def connect(self):
        return _FakeCtx(self._conn)

    def begin(self):
        return _FakeCtx(self._conn)


def make_fake_engine(
    *,
    features: dict[str, int],
    sources: dict[str, int] | None = None,
    resolved: list[dict] | None = None,
) -> FakeEngine:
    sources = sources if sources is not None else {"yfinance": 1}
    conn = FakeConnection(
        feature_rows=[(fid, name) for name, fid in features.items()],
        source_rows=[(sid, name) for name, sid in sources.items()],
        resolved_rows=resolved or [],
    )
    return FakeEngine(conn)


D0 = date(2026, 1, 5)


# ---------------------------------------------------------------------------
# PriceSeriesContract.resolve — mapped / unmapped / ambiguous
# ---------------------------------------------------------------------------


def test_resolve_mapped_instrument():
    engine = make_fake_engine(features={"aapl_close": 42})
    contract = PriceSeriesContract(
        engine, candidate_name_fn=lambda instrument: ["aapl_close", "aapl_full", "aapl"]
    )
    descriptor = contract.resolve("AAPL")
    assert descriptor.feature_id == 42
    assert descriptor.feature_name == "aapl_close"
    assert descriptor.series_id == "YF:AAPL:close"
    assert descriptor.source_catalog_name == "yfinance"
    assert descriptor.bar_kind == BAR_KIND


def test_resolve_is_cached_across_calls():
    calls = []

    def counting_names(instrument):
        calls.append(instrument)
        return ["aapl_close"]

    engine = make_fake_engine(features={"aapl_close": 42})
    contract = PriceSeriesContract(engine, candidate_name_fn=counting_names)
    contract.resolve("AAPL")
    contract.resolve("AAPL")
    assert calls == ["AAPL"]  # second resolve() served from cache


def test_resolve_unmapped_instrument_raises():
    engine = make_fake_engine(features={"aapl_close": 42})
    contract = PriceSeriesContract(engine, candidate_name_fn=lambda instrument: ["zzzz_close"])
    with pytest.raises(UnsupportedInstrumentError) as excinfo:
        contract.resolve("ZZZZ")
    assert excinfo.value.instrument == "ZZZZ"
    assert "no feature_registry row" in excinfo.value.reason


def test_resolve_no_candidate_names_raises():
    engine = make_fake_engine(features={})
    contract = PriceSeriesContract(engine, candidate_name_fn=lambda instrument: [])
    with pytest.raises(UnsupportedInstrumentError):
        contract.resolve("EMPTY")


def test_resolve_ambiguous_instrument_raises_and_never_picks():
    engine = make_fake_engine(features={"aapl_close": 42, "aapl_full": 43})
    contract = PriceSeriesContract(
        engine, candidate_name_fn=lambda instrument: ["aapl_close", "aapl_full"]
    )
    with pytest.raises(AmbiguousInstrumentError) as excinfo:
        contract.resolve("AAPL")
    ids = {fid for fid, _name in excinfo.value.candidates}
    assert ids == {42, 43}


def test_resolve_missing_source_catalog_row_raises():
    engine = make_fake_engine(features={"aapl_close": 42}, sources={})
    contract = PriceSeriesContract(engine, candidate_name_fn=lambda instrument: ["aapl_close"])
    with pytest.raises(UnsupportedInstrumentError) as excinfo:
        contract.resolve("AAPL")
    assert "source_catalog" in excinfo.value.reason


def test_resolve_source_catalog_lookup_is_case_insensitive():
    engine = make_fake_engine(features={"aapl_close": 42}, sources={"YFinance": 1})
    contract = PriceSeriesContract(engine, candidate_name_fn=lambda instrument: ["aapl_close"])
    descriptor = contract.resolve("AAPL")
    assert descriptor.source_catalog_name == "YFinance"


# ---------------------------------------------------------------------------
# PITPriceAccessor — as_of capping (through the real PITStore.get_pit)
# ---------------------------------------------------------------------------


def _accessor_for(engine):
    contract = PriceSeriesContract(engine, candidate_name_fn=lambda instrument: ["aapl_close"])
    return PITPriceAccessor(engine, contract)


def test_accessor_returns_none_before_any_bar_exists():
    engine = make_fake_engine(features={"aapl_close": 42}, resolved=[])
    accessor = _accessor_for(engine)
    assert accessor("AAPL", D0) is None


def test_accessor_caps_at_as_of_never_reads_ahead():
    engine = make_fake_engine(
        features={"aapl_close": 42},
        resolved=[
            {"feature_id": 42, "obs_date": D0, "value": 100.0, "release_date": D0, "vintage_date": D0},
            {
                "feature_id": 42,
                "obs_date": date(2026, 1, 10),
                "value": 999.0,
                "release_date": date(2026, 1, 10),
                "vintage_date": date(2026, 1, 10),
            },
        ],
    )
    accessor = _accessor_for(engine)
    point = accessor("AAPL", D0)
    assert point is not None
    assert point.value == 100.0
    assert point.obs_date == D0
    # Never sees the later, out-of-window bar.
    assert point.value != 999.0


def test_accessor_result_is_duck_type_compatible_with_signal_outcomes_pricepoint():
    engine = make_fake_engine(
        features={"aapl_close": 42},
        resolved=[
            {"feature_id": 42, "obs_date": D0, "value": 100.0, "release_date": D0, "vintage_date": D0}
        ],
    )
    accessor = _accessor_for(engine)
    point = accessor("AAPL", D0)
    assert point.price == 100.0
    assert point.bar_date == D0
    assert point.basis == BAR_KIND


def test_accessor_carries_full_provenance():
    engine = make_fake_engine(
        features={"aapl_close": 42},
        resolved=[
            {
                "feature_id": 42,
                "obs_date": D0,
                "value": 100.0,
                "release_date": date(2026, 1, 6),
                "vintage_date": date(2026, 1, 6),
            }
        ],
    )
    accessor = _accessor_for(engine)
    point = accessor("AAPL", date(2026, 1, 6))
    assert point.obs_date == D0
    assert point.release_date == date(2026, 1, 6)
    assert point.vintage_date == date(2026, 1, 6)
    assert point.source_ref == "yfinance:YF:AAPL:close"


def test_accessor_unmapped_instrument_raises_signal_outcomes_error_type():
    engine = make_fake_engine(features={})
    contract = PriceSeriesContract(engine, candidate_name_fn=lambda instrument: ["nope"])
    accessor = PITPriceAccessor(engine, contract)
    with pytest.raises(SignalOutcomesUnsupported):
        accessor("NOPE", D0)


# ---------------------------------------------------------------------------
# Sanity flag — flagged, not dropped, not raised
# ---------------------------------------------------------------------------


def test_accessor_flags_price_outside_sanity_bounds_but_still_returns_it():
    engine = make_fake_engine(
        features={"aapl_close": 42},
        resolved=[{"feature_id": 42, "obs_date": D0, "value": -5.0, "release_date": D0, "vintage_date": D0}],
    )
    accessor = _accessor_for(engine)
    point = accessor("AAPL", D0)
    assert point is not None
    assert point.value == -5.0
    assert point.sanity_ok is False
    assert point.sanity_reason == REASON_PRICE_OUT_OF_BOUNDS


def test_accessor_sanity_ok_true_within_default_bounds():
    engine = make_fake_engine(
        features={"aapl_close": 42},
        resolved=[{"feature_id": 42, "obs_date": D0, "value": 150.0, "release_date": D0, "vintage_date": D0}],
    )
    accessor = _accessor_for(engine)
    point = accessor("AAPL", D0)
    assert point.sanity_ok is True
    assert point.sanity_reason is None


def test_accessor_respects_custom_sanity_bounds():
    engine = make_fake_engine(
        features={"aapl_close": 42},
        resolved=[{"feature_id": 42, "obs_date": D0, "value": 150.0, "release_date": D0, "vintage_date": D0}],
    )
    contract = PriceSeriesContract(engine, candidate_name_fn=lambda instrument: ["aapl_close"])
    accessor = PITPriceAccessor(engine, contract, sanity_bounds=(0.0, 100.0))
    point = accessor("AAPL", D0)
    assert point.sanity_ok is False


def test_default_sanity_bounds_match_evaluate_signal_default():
    # evaluation/signal_outcomes.py::evaluate_signal's own default
    # sanity_bounds is (0.0, 1_000_000.0) — this contract's default must
    # not silently drift from that.
    assert DEFAULT_SANITY_BOUNDS == (0.0, 1_000_000.0)
