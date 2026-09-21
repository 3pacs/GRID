"""D-M32: `oracle_predictions.entry_price` is a measurement or it is NULL.

`oracle/publish.py` published the literal `0.0` as the entry price of every
AstroGrid prediction. It rendered as "$0.00" on the prediction card and made
every downstream `(exit - entry) / entry` meaningless, while being
indistinguishable from a price somebody had looked up.

These tests pin the three halves of the fix:

* the publish path stores the measured spot at `as_of_date` **with its source
  and the date it was observed on**, or `None` -- never `0.0`;
* the scorers treat `None` as *not scorable* -- excluded, with a reason
  written to `score_notes` -- never as a 0% return and never as a division;
* the read path (`/oracle/predictions` tracking P&L) returns no P&L rather
  than dividing.

Policy: `docs/reference/CONFIDENCE_POLICY.md` -- null plus a basis, never a
placeholder number.
"""

from __future__ import annotations

import inspect
import json
import re
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest


# -- Fake engine -------------------------------------------------------------
#
# `oracle/publish.py` writes postgres-flavoured SQL (jsonb casts, an
# expression-index ON CONFLICT), so the insert is captured rather than run.


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def scalar(self):
        return self._rows[0][0] if self._rows else None


class _Conn:
    def __init__(self, engine):
        self._engine = engine

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self._engine.calls.append((sql, params))
        if "FROM options_daily_signals" in sql:
            if self._engine.spot_raises:
                raise RuntimeError("options_daily_signals is not there")
            return _Result(self._engine.spot_rows)
        return _Result([])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Engine:
    def __init__(self, spot_rows=(), spot_raises=False):
        self.spot_rows = list(spot_rows)
        self.spot_raises = spot_raises
        self.calls: list[tuple] = []

    def connect(self):
        return _Conn(self)

    def begin(self):
        return _Conn(self)


def _insert_params(engine: _Engine) -> dict:
    for sql, params in engine.calls:
        if "INSERT INTO oracle_predictions" in sql:
            return params
    raise AssertionError("no INSERT INTO oracle_predictions was issued")


@pytest.fixture()
def no_context(monkeypatch):
    """The conviction-context lookup is not what these tests are about."""
    from oracle import publish

    monkeypatch.setattr(
        publish,
        "build_prediction_context",
        lambda *a, **k: {
            "regime": "NEUTRAL",
            "fci_regime": "NEUTRAL",
            "vix_level": None,
            "signal_contributions": {},
        },
    )
    return publish


_PAYLOAD = {
    "prediction_id": "p-1",
    "target_symbols": ["AAPL"],
    "horizon_label": "swing",
    "as_of_ts": "2026-09-16T14:00:00+00:00",
    "call": "buy the dip",
    "confidence": 0.61,
}

# fable/packet2a-recovery-20260921 reverted oracle/publish.py's writer to
# the pre-#544 literal defaults (entry_price=0.0, confidence/signal_strength/
# coherence = payload value or 0.5) so the recovery tree stops producing new
# NULL entry_price/confidence rows, while keeping the #547 readers, the
# historical-write hold and the migration (schema stays nullable). These two
# classes pin the retired _measured_entry_price/_measured_or_none write-path
# behavior and cannot pass against the reverted writer -- marked skipped on
# this branch only, not deleted, so the assertions still exist for whichever
# tree is deployed next.
_RECOVERY_WRITER_REVERTED = pytest.mark.skip(
    reason=(
        "recovery branch (fable/packet2a-recovery-20260921): "
        "oracle/publish.py's writer was reverted to the pre-#544 literal "
        "defaults (entry_price=0.0, confidence/signal_strength/coherence = "
        "payload value or 0.5); _measured_entry_price/_measured_or_none no "
        "longer exist on this branch. See "
        "docs/handoffs/2026-09-21/fable-packet2a-extraction.md."
    ),
)


@_RECOVERY_WRITER_REVERTED
class TestMeasuredEntryPrice:
    def test_an_observed_spot_comes_back_with_the_day_it_was_observed(self):
        from oracle.publish import _measured_entry_price

        engine = _Engine(spot_rows=[(214.5, date(2026, 9, 15))])
        price, basis = _measured_entry_price(engine, "AAPL", date(2026, 9, 16))

        assert price == pytest.approx(214.5)
        assert basis["status"] == "measured"
        assert basis["source"] == "options_daily_signals.spot_price"
        assert basis["observed_on"] == "2026-09-15"
        assert basis["as_of"] == "2026-09-16"

    def test_the_lookup_is_point_in_time(self):
        """A close from after the prediction is not an entry price."""
        from oracle.publish import _measured_entry_price

        engine = _Engine(spot_rows=[(214.5, date(2026, 9, 15))])
        _measured_entry_price(engine, "AAPL", date(2026, 9, 16))
        sql, params = engine.calls[0]
        assert "signal_date <= :d" in sql
        assert params["d"] == date(2026, 9, 16)

    def test_no_observation_is_none_with_a_reason(self):
        from oracle.publish import _measured_entry_price

        price, basis = _measured_entry_price(_Engine(), "AAPL", date(2026, 9, 16))
        assert price is None
        assert basis["status"] == "unavailable"
        assert basis["source"] is None
        assert "AAPL" in basis["reason"]
        assert "2026-09-16" in basis["reason"]

    def test_a_failed_lookup_is_none_not_a_raise(self):
        from oracle.publish import _measured_entry_price

        price, basis = _measured_entry_price(
            _Engine(spot_raises=True), "AAPL", date(2026, 9, 16)
        )
        assert price is None
        assert basis["status"] == "unavailable"
        assert "lookup failed" in basis["reason"]


@_RECOVERY_WRITER_REVERTED
class TestPublishedRow:
    def test_the_published_row_carries_the_measured_price(self, no_context):
        from oracle.publish import publish_astrogrid_prediction

        engine = _Engine(spot_rows=[(214.5, date(2026, 9, 15))])
        out = publish_astrogrid_prediction(engine, dict(_PAYLOAD))

        params = _insert_params(engine)
        assert params["entry_price"] == pytest.approx(214.5)
        assert out["entry_price"] == pytest.approx(214.5)
        assert out["entry_price_basis"]["observed_on"] == "2026-09-15"

    def test_the_basis_is_stored_on_the_row_beside_the_price(self, no_context):
        from oracle.publish import publish_astrogrid_prediction

        engine = _Engine(spot_rows=[(214.5, date(2026, 9, 15))])
        publish_astrogrid_prediction(engine, dict(_PAYLOAD))

        signals = json.loads(_insert_params(engine)["signals"])
        assert signals["entry_price_basis"]["status"] == "measured"
        assert signals["entry_price_basis"]["source"] == (
            "options_daily_signals.spot_price"
        )

    def test_no_observation_publishes_null_not_zero(self, no_context):
        from oracle.publish import publish_astrogrid_prediction

        engine = _Engine()          # no options_daily_signals row at all
        out = publish_astrogrid_prediction(engine, dict(_PAYLOAD))

        params = _insert_params(engine)
        assert params["entry_price"] is None, "a missing price is NULL, not 0.0"
        assert out["entry_price"] is None

        signals = json.loads(params["signals"])
        assert signals["entry_price_basis"]["status"] == "unavailable"

    def test_the_retired_literal_is_gone_from_the_source(self):
        from oracle import publish

        src = inspect.getsource(publish.publish_astrogrid_prediction)
        assert '"entry_price": 0.0' not in src
        assert '"entry_price": entry_price' in src

    def test_a_publish_with_no_target_symbol_still_measures_hybrid(self, no_context):
        from oracle.publish import publish_astrogrid_prediction

        payload = dict(_PAYLOAD)
        payload.pop("target_symbols")
        engine = _Engine()
        publish_astrogrid_prediction(engine, payload)

        params = _insert_params(engine)
        assert params["ticker"] == "HYBRID"
        assert params["entry_price"] is None


# -- Scoring consumers -------------------------------------------------------


class _SqliteConn:
    """Enough of a SQLAlchemy connection for `score_one_chunk`."""

    def __init__(self, raw):
        self._raw = raw

    def execute(self, stmt, params=None):
        raw_sql = str(stmt)
        sql = raw_sql.replace("NOW()", "CURRENT_TIMESTAMP")
        ordered = []
        if params:
            names = [n for n in re.findall(r":(\w+)", raw_sql) if n in params]
            ordered = [params[n] for n in names]
            for name in sorted(set(names), key=len, reverse=True):
                sql = sql.replace(f":{name}", "?")
        return self._raw.execute(sql, ordered)


sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_converter("DATE", lambda b: date.fromisoformat(b.decode()))


def _scoring_db(rows):
    raw = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
    raw.execute(
        "CREATE TABLE oracle_predictions ("
        "id TEXT, ticker TEXT, direction TEXT, target_price REAL, "
        "entry_price REAL, expiry DATE, confidence REAL, "
        "expected_move_pct REAL, model_name TEXT, signals TEXT, "
        "created_at DATE, verdict TEXT, actual_price REAL, "
        "actual_move_pct REAL, pnl_pct REAL, scored_at TEXT, "
        "score_notes TEXT)"
    )
    raw.execute(
        "CREATE TABLE oracle_models ("
        "name TEXT, hits INTEGER DEFAULT 0, partials INTEGER DEFAULT 0, "
        "misses INTEGER DEFAULT 0, predictions_made INTEGER DEFAULT 0, "
        "cumulative_pnl REAL DEFAULT 0.0, last_updated TEXT)"
    )
    raw.execute("INSERT INTO oracle_models (name) VALUES ('m')")
    raw.executemany(
        "INSERT INTO oracle_predictions "
        "(id, ticker, direction, target_price, entry_price, expiry, "
        " confidence, expected_move_pct, model_name, signals, created_at, "
        " verdict) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    return raw


class TestScorerExcludesNullEntry:
    def test_a_null_entry_row_is_never_fetched_for_scoring(self):
        from scripts.score_oracle_trades import score_one_chunk

        yesterday = date.today() - timedelta(days=1)
        raw = _scoring_db([
            ("null-entry", "AAPL", "CALL", None, None, yesterday,
             0.6, 2.0, "m", "{}", yesterday, "pending"),
            ("priced", "MSFT", "CALL", None, 100.0, yesterday,
             0.6, 2.0, "m", "{}", yesterday, "pending"),
        ])

        counters = score_one_chunk(
            _SqliteConn(raw),
            engine=None,
            prices={"MSFT": {yesterday: 110.0}},
            today=date.today(),
            chunk_size=100,
        )

        assert counters["fetched"] == 1, (
            "the NULL-entry prediction must not enter the scoring chunk"
        )
        assert counters["scored"] == 1

        verdicts = dict(
            raw.execute("SELECT id, verdict FROM oracle_predictions").fetchall()
        )
        assert verdicts["null-entry"] == "pending", (
            "excluded from scoring, not scored as a 0% return"
        )

    def test_a_null_entry_never_produces_a_return(self):
        """No ZeroDivisionError, no infinite move, no 0.0 pnl."""
        from scripts.score_oracle_trades import score_one_chunk

        yesterday = date.today() - timedelta(days=1)
        raw = _scoring_db([
            ("null-entry", "AAPL", "CALL", None, None, yesterday,
             0.6, 2.0, "m", "{}", yesterday, "pending"),
        ])
        counters = score_one_chunk(
            _SqliteConn(raw),
            engine=None,
            prices={"AAPL": {yesterday: 110.0}},
            today=date.today(),
            chunk_size=100,
        )
        assert counters["scored"] == 0
        row = raw.execute(
            "SELECT pnl_pct, actual_move_pct FROM oracle_predictions"
        ).fetchone()
        assert row == (None, None)

    def test_the_chunk_query_excludes_null_explicitly(self):
        from scripts import score_oracle_trades

        src = inspect.getsource(score_oracle_trades.score_one_chunk)
        assert "entry_price IS NOT NULL" in src, (
            "NULL exclusion must be written down, not left to SQL's "
            "NULL > 0 -> NULL accident"
        )

    def test_a_null_entry_is_swept_to_no_data_with_a_reason(self):
        """`entry_price = 0` never matches a NULL, so a published-with-no-price
        row would sit 'pending' forever unless NULL is named.

        The sweep binds the reason from ``oracle.entry_price_policy`` rather
        than spelling it inline, so the scorer, the engine and the API route
        cannot drift into different wordings for the same finding.

        HISTORICAL-WRITE HOLD: the sweep's WHERE names only
        ``entry_price IS NULL``. A non-null invalid entry_price (0 or
        negative) can only be a legacy row written before the column was
        nullable, and it must never be part of this sweep — it is held,
        never updated/closed/rescored/re-labelled.
        """
        from oracle.entry_price_policy import SCORE_NOTE_ENTRY_NULL
        from scripts import score_oracle_trades

        src = inspect.getsource(score_oracle_trades.main)
        assert "AND entry_price IS NULL" in src
        assert "entry_price IS NULL OR entry_price <= 0" not in src
        assert ":note_null" in src
        assert SCORE_NOTE_ENTRY_NULL == "No entry price was measured at publish time"

    def test_the_backfill_skip_is_an_explicit_none_test(self):
        from scripts import score_oracle_trades

        src = inspect.getsource(score_oracle_trades.main)
        assert "if entry_price is not None and entry_price > 0:" in src


class TestCalibrationIgnoresEntryPrice:
    def test_calibration_does_not_read_entry_price(self):
        """Reliability is scored off `confidence` and `verdict`; nothing in
        oracle/calibration.py computes a return from `entry_price`, so a NULL
        entry cannot reach it. Pinned so a future edit has to notice."""
        import oracle.calibration as calibration

        src = Path(calibration.__file__).read_text(encoding="utf-8")
        assert "entry_price" not in src


class TestTrackingPnlGuard:
    def test_the_read_path_refuses_a_null_entry_explicitly(self):
        """The route decides before it divides, and says which case it hit.

        The guard used to be `and r[6]`, which only escaped a divide-by-zero
        because 0.0 happens to be falsy. It is now the shared classifier, and
        the row carries a `tracking_pnl_basis` naming the reason instead of a
        bare null the client has to interpret.
        """
        import api.routers.oracle as oracle_router

        src = Path(oracle_router.__file__).read_text(encoding="utf-8")
        assert 'if r[17] == "pending" and r[6]:' not in src
        assert "entry_price_pnl_basis(entry_raw)" in src
        assert '"tracking_pnl_basis": tracking_pnl_basis,' in src

    def test_the_basis_names_null_and_zero_differently(self):
        from oracle.entry_price_policy import (
            PNL_BASIS_ENTRY_NULL,
            PNL_BASIS_ENTRY_ZERO,
            entry_price_pnl_basis,
        )

        assert entry_price_pnl_basis(None) == PNL_BASIS_ENTRY_NULL
        assert entry_price_pnl_basis(0) == PNL_BASIS_ENTRY_ZERO
        assert entry_price_pnl_basis(0.0) == PNL_BASIS_ENTRY_ZERO
        assert PNL_BASIS_ENTRY_NULL != PNL_BASIS_ENTRY_ZERO
        # A positive entry divides: no reason, so no basis to report.
        assert entry_price_pnl_basis(214.5) is None


class TestSchemaAllowsNull:
    def test_the_bootstrap_ddl_does_not_declare_entry_price_not_null(self):
        import oracle.engine as engine_mod

        src = Path(engine_mod.__file__).read_text(encoding="utf-8")
        assert "entry_price DOUBLE PRECISION NOT NULL" not in src
        # The relaxation of an existing table lives in alembic, not in the
        # bootstrap (see tests/test_oracle_predictions_schema_parity.py).
        rev = Path(engine_mod.__file__).resolve().parents[1] / "migrations" / "versions" / "oracle_pred_nullable_0918.py"
        assert "DROP NOT NULL" in rev.read_text(encoding="utf-8")
