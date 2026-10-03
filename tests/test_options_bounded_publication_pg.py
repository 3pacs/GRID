"""Private synthetic PG contract; never fetches provider data.

All setup/migration/fixture DATA transactions are globally counted at commit.
Old >50 defect is an arithmetic/static witness, never executed on PostgreSQL.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from ingestion import options, options_publication as pub

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2026, 10, 2)
START = datetime(2026, 10, 2, 14, tzinfo=timezone.utc)
SQL = ROOT / "docs/handoffs/2026-10-02/options-bounded-publication.sql"


def test_original_required_fk_guards_and_registration_view_refusal(bounded_pg_engine):
    engine, _, counts = bounded_pg_engine
    statements = [
        ("options_snapshots_all_batch_required", """
            INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike)
            VALUES ('SPY', '2026-10-02', '2026-10-16', 'call', 1)
        """),
        ("options_snapshots_all_batch_fk", """
            INSERT INTO options_snapshots_all (ticker, snap_date, expiry, opt_type, strike,
                capture_batch_id, capture_ordinal, capture_started_at, capture_completed_at,
                provider_regular_market_at)
            VALUES ('SPY', '2026-10-02', '2026-10-16', 'call', 1,
                'unregistered', 99999999, now(), now(), now())
        """),
        ("is not a table", "TRUNCATE options_capture_batches CASCADE"),
        ("append-only", "TRUNCATE options_capture_batches_all CASCADE"),
    ]
    for expected, statement in statements:
        with pytest.raises(DBAPIError, match=expected):
            with engine.begin() as conn:
                conn.exec_driver_sql(statement)
    assert max(counts) <= 50


@pytest.fixture
def bounded_pg_engine():
    from tests.options_production_shape_fixture import owned_fixture
    yield from owned_fixture(ROOT)


def packet(n, ordinal=10):
    batch = str(uuid4())
    header = {"batch_id": batch, "ticker": "SPY", "snap_date": DAY.isoformat(),
              "ordinal": ordinal, "started_at": START, "completed_at": START + timedelta(minutes=2),
              "row_count": n, "spot": 100., "source": "daily_scheduler"}
    contracts = [{"ticker": "SPY", "snap_date": DAY.isoformat(), "expiry": "2026-10-16",
                  "opt_type": "call" if i % 2 else "put", "strike": 80. + i / 2,
                  "last_price": 2., "bid": 1., "ask": 3., "volume": 3, "oi": 5,
                  "iv": .2, "itm": False, "batch_id": batch,
                  "provider_regular_market_at": START - timedelta(minutes=1)} for i in range(n)]
    return header, contracts


def visible(engine):
    with engine.connect() as conn:
        return conn.exec_driver_sql("SELECT DISTINCT capture_batch_id FROM options_snapshots").scalars().all()


@pytest.mark.parametrize("n", [62, 122])
def test_complete_large_chains_never_partial_between_batches(bounded_pg_engine, monkeypatch, n):
    engine, puller, counts = bounded_pg_engine
    h0, c0 = packet(8, 1)
    assert pub.publish(engine, h0, c0, lambda conn: 0)["published"]
    h, contracts = packet(n)
    real = pub.transaction
    observations = []

    def observe(*args, **kwargs):
        r = real(*args, **kwargs)
        observations.append(visible(engine))
        with engine.connect() as conn:
            registered = conn.execute(text("SELECT count(*) FROM options_capture_batches "
                                           "WHERE capture_batch_id=:b"), {"b": h["batch_id"]}).scalar_one()
            if not registered:
                assert visible(engine) == [h0["batch_id"]]
                assert conn.exec_driver_sql("SELECT count(*) FROM resolved_series").scalar_one() == 0
        return r

    monkeypatch.setattr(pub, "transaction", observe)

    def finish(conn):
        assert visible(engine) == [h0["batch_id"]]
        return puller._push_to_resolved(conn, "SPY", DAY.isoformat(), {
            str(i): ("vol", "synthetic fixture", i + 1.) for i in range(10)})

    r = pub.publish(engine, h, contracts, finish)
    assert r["published"] and not r["stop_scope"]
    assert r["transaction_rows"] == [1] + [50] * (n // 50) + [n % 50, 21]
    assert observations[-1] == [h["batch_id"]]
    assert all(x == [h0["batch_id"]] for x in observations[:-1])
    assert max(counts) == 50
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM options_snapshots_all").scalar_one() == n + 8
        assert conn.exec_driver_sql("SELECT count(*) FROM options_snapshots").scalar_one() == n
        assert conn.exec_driver_sql("SELECT count(*) FROM resolved_series "
                                    "WHERE release_date <= '2026-10-02'").scalar_one() == 10
        assert conn.exec_driver_sql("SELECT count(*) FROM resolved_series "
                                    "WHERE release_date <= '2026-10-01'").scalar_one() == 0
        assert conn.exec_driver_sql("SELECT count(*) FROM options_capture_batches "
                                    "WHERE registered_at < capture_completed_at").scalar_one() == 0


class FaultEngine:
    """Real server COMMIT, then simulated transport loss or post-ACK cleanup."""
    def __init__(self, engine, at, cleanup=False):
        self.engine, self.at, self.cleanup, self.calls = engine, at, cleanup, 0

    def connect(self):
        self.calls += 1
        selected = self.calls == self.at
        actual = self.engine.connect()
        owner = self

        class Connection:
            def __getattr__(self, name):
                return getattr(actual, name)

            def begin(self):
                tx = actual.begin()

                class Tx:
                    def commit(self):
                        tx.commit()
                        if selected and not owner.cleanup:
                            raise ConnectionError("synthetic lost COMMIT acknowledgement")

                    def rollback(self):
                        tx.rollback()
                return Tx()

            def close(self):
                actual.close()
                if selected and owner.cleanup:
                    raise RuntimeError("synthetic post-ACK cleanup failure")
        return Connection()


@pytest.mark.parametrize("at,cleanup", [(2, False), (4, False), (2, True), (4, True)])
def test_real_commit_unknown_ack_and_cleanup_stop_without_replay(bounded_pg_engine, at, cleanup):
    engine, _, counts = bounded_pg_engine
    h, c = packet(62)
    fault = FaultEngine(engine, at, cleanup)
    r = pub.publish(fault, h, c, lambda conn: 0)
    assert r["stop_scope"] and fault.calls == at
    assert r["commit_ack"] == ("ACKNOWLEDGED" if cleanup else "UNKNOWN")
    assert r["cleanup_failed"] is cleanup
    assert r["data_rows_acknowledged"] == (51 if at == 2 and cleanup else
                                           64 if at == 4 and cleanup else
                                           1 if at == 2 else 63)
    if not cleanup:
        assert r["rows_inserted"] is None
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM options_snapshots_all").scalar_one() == (50 if at == 2 else 62)
        assert conn.exec_driver_sql("SELECT count(*) FROM options_capture_batches").scalar_one() == (0 if at == 2 else 1)
    assert max(counts) <= 50
    assert r["published"] is (at == 4 and cleanup)


def test_incomplete_completion_and_unreviewed_audit_trigger_fail_closed(bounded_pg_engine):
    engine, _, _ = bounded_pg_engine
    h, c = packet(62)
    def stop():
        return False
    # One acknowledged header is durable evidence; no partial public capture.
    assert pub.transaction(engine, lambda conn: conn.execute(pub._HEADER, h)).commit_ack == "ACKNOWLEDGED"
    assert not visible(engine)
    r = pub.transaction(engine, lambda conn: conn.execute(text(
        "INSERT INTO options_capture_publications(capture_batch_id) VALUES (:batch_id)"), h))
    assert r.commit_ack == "NOT_COMMITTED"
    with engine.begin() as conn:
        conn.exec_driver_sql("""
            CREATE TABLE private_audit_fixture(id serial);
            CREATE FUNCTION private_audit_sidewrite() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN INSERT INTO private_audit_fixture DEFAULT VALUES; RETURN NEW; END $$;
            CREATE TRIGGER private_audit_fixture AFTER INSERT ON options_snapshots_all
              FOR EACH ROW EXECUTE FUNCTION private_audit_sidewrite();
        """)
    # Closure rejection occurs before any DATA write, no >50 negative witness.
    r = pub.publish(engine, *packet(122, 11), lambda conn: 0)
    assert r["stop_scope"] and r["data_rows_acknowledged"] == 0
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM private_audit_fixture").scalar_one() == 0
        assert conn.exec_driver_sql("SELECT count(*) FROM options_snapshots_all").scalar_one() == 0
    assert pub.transaction(engine, lambda conn: None, stop).commit_ack == "NOT_COMMITTED"


def test_old_defect_static_arithmetic_witness_only():
    # Registered header + 122 contracts already exceeds 50 before signals,
    # registry, resolved or sidewrites. This is never executed on a server.
    assert 1 + 122 > 50


def test_scope_stops_after_partial_and_keeps_acknowledged_prefix(monkeypatch):
    p = options.OptionsPuller.__new__(options.OptionsPuller)
    calls = []
    monkeypatch.setattr(options, "_utc_now", lambda: START)
    monkeypatch.setattr(options, "YahooOptionsClient", lambda: type("Y", (), {"is_available": True})())
    monkeypatch.setattr(options.time, "sleep", lambda _: None)
    def pull(ticker, *_args, **_kwargs):
        calls.append(ticker)
        return {"ticker": ticker, "status": "SUCCESS" if ticker == "SPY" else "PARTIAL",
                "rows_inserted": 8 if ticker == "SPY" else None,
                "data_rows_acknowledged": 8 if ticker == "SPY" else 51,
                "stop_scope": ticker != "SPY"}
    p._pull_ticker = pull
    r = p.pull_all(["SPY", "QQQ", "IWM"])
    assert calls == ["SPY", "QQQ"] and r[2]["status"] == "DEFERRED"
    assert r.summary["status"] == "PARTIAL" and r.summary["rows_inserted"] is None


def test_real_puller_large_chain_fetches_outside_transactions(bounded_pg_engine, monkeypatch):
    engine, p, counts = bounded_pg_engine
    fetched = []
    def frozen_clock(_conn, _cursor, statement, parameters, _context, _many):
        if statement == "SELECT txid_current(), clock_timestamp()":
            return "SELECT txid_current(), TIMESTAMPTZ '2026-10-02 14:00Z'", parameters
        if statement == "SELECT clock_timestamp()":
            return "SELECT TIMESTAMPTZ '2026-10-02 14:02Z'", parameters
        return statement, parameters
    class Yahoo:
        def get_options(self, *_args):
            assert engine.pool.checkedout() == 0
            fetched.append(True)
            rows = [{"strike": 80 + i, "volume": 3, "openInterest": 5,
                     "impliedVolatility": .2, "lastPrice": 2, "bid": 1, "ask": 3}
                    for i in range(61)]
            return {"quote": {"regularMarketPrice": 100,
                              "regularMarketTime": int((START-timedelta(minutes=1)).timestamp())},
                    "expirations": [int(datetime(2026, 10, 16, tzinfo=timezone.utc).timestamp())],
                    "calls": rows, "puts": rows}
    p._yahoo = Yahoo()
    monkeypatch.setattr(options, "_utc_now", lambda: START)
    event.listen(engine, "before_cursor_execute", frozen_clock, retval=True)
    try:
        r = p._pull_ticker("SPY", DAY.isoformat(), capture_source="smart_scheduler")
    finally:
        event.remove(engine, "before_cursor_execute", frozen_clock)
    assert r["status"] == "SUCCESS", r
    assert r["snapshots_inserted"] == 122 and r["transaction_rows"][:4] == [1, 50, 50, 22]
    assert r["transaction_rows"][-1] <= 22 and fetched == [True]
    assert max(counts) == 50


def test_legacy_writer_view_insert_and_older_overlap_compatibility(bounded_pg_engine):
    engine, _, _ = bounded_pg_engine
    h, c = packet(8, 10)
    with engine.begin() as conn:
        conn.execute(text(str(pub._HEADER).replace("options_capture_batches_all", "options_capture_batches")
                          .replace(", requires_publication", "").replace(", true)", ")")), h)
        for row in c:
            conn.execute(pub._CONTRACT, {**row, "ordinal": 10, "started_at": h["started_at"],
                                        "completed_at": h["completed_at"]})
    assert visible(engine) == [h["batch_id"]]
    def must_not_run(conn):
        raise AssertionError("older capture cannot overwrite latest signals")
    old, old_rows = packet(62, 9)
    r = pub.publish(engine, old, old_rows, must_not_run)
    assert r["published"] and not r["latest_batch"] and visible(engine) == [h["batch_id"]]
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM options_capture_batches").scalar_one() == 2


def test_prior_complete_signal_pit_and_image_survive_uncertain_new_prefix(bounded_pg_engine):
    engine, p, counts = bounded_pg_engine
    old, old_rows = packet(8, 1)
    old["source"] = "options_puller"  # preserved unknown historical writer provenance
    def old_finish(conn):
        conn.execute(text("INSERT INTO options_daily_signals(ticker,signal_date,put_call_ratio) "
                          "VALUES ('SPY',:day,17)"), {"day": DAY})
        return 1 + p._push_to_resolved(conn, "SPY", DAY.isoformat(),
                                       {"pcr": ("sentiment", "synthetic fixture", 17.)})
    assert pub.publish(engine, old, old_rows, old_finish)["published"]
    with engine.connect() as conn:
        image = conn.execute(text("SELECT row_to_json(h)::text FROM options_capture_batches_all h "
                                  "WHERE capture_batch_id=:b"), {"b": old["batch_id"]}).scalar_one()
        cutoff = conn.execute(text("SELECT registered_at FROM options_capture_batches "
                                   "WHERE capture_batch_id=:b"), {"b": old["batch_id"]}).scalar_one()
    new, new_rows = packet(122, 10)
    def must_not_publish(conn):
        raise AssertionError("uncertain contract prefix cannot publish signals")
    fault = FaultEngine(engine, 2)
    r = pub.publish(fault, new, new_rows, must_not_publish)
    assert r["commit_ack"] == "UNKNOWN" and r["data_rows_acknowledged"] == 1
    assert fault.calls == 2 and visible(engine) == [old["batch_id"]]
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT put_call_ratio FROM options_daily_signals").scalar_one() == 17
        assert conn.exec_driver_sql("SELECT value FROM resolved_series "
                                    "WHERE release_date <= '2026-10-02' AND vintage_date <= '2026-10-02'").scalar_one() == 17
        assert conn.execute(text("SELECT count(*) FROM options_capture_batches WHERE registered_at <= :cutoff"),
                            {"cutoff": cutoff}).scalar_one() == 1
        assert conn.execute(text("SELECT row_to_json(h)::text FROM options_capture_batches_all h "
                                 "WHERE capture_batch_id=:b"), {"b": old["batch_id"]}).scalar_one() == image
        assert conn.execute(text("SELECT count(*) FROM options_snapshots_all s JOIN options_capture_batches b "
                                 "USING(capture_batch_id) WHERE s.capture_batch_id=:b"),
                            {"b": new["batch_id"]}).scalar_one() == 0
    assert max(counts) <= 50


def test_completed_capture_is_sealed_without_erasing_history(bounded_pg_engine):
    engine, _, _ = bounded_pg_engine
    h, rows = packet(62)
    assert pub.publish(engine, h, rows, lambda conn: 0)["published"]
    extra = {**rows[0], "strike": 777., "ordinal": h["ordinal"],
             "started_at": h["started_at"], "completed_at": h["completed_at"]}
    r = pub.transaction(engine, lambda conn: conn.execute(pub._CONTRACT, extra))
    assert r.commit_ack == "NOT_COMMITTED" and r.rows == 0
    for sql in ["UPDATE options_capture_batches_all SET row_count=1",
                "DELETE FROM options_snapshots_all", "TRUNCATE options_capture_publications"]:
        assert pub.transaction(engine, lambda conn: conn.execute(text(sql))).commit_ack == "NOT_COMMITTED"
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM options_snapshots").scalar_one() == 62
