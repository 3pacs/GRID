"""Honest outcomes for KOSIS, USDA_NASS and ICIJ_OFFSHORE (2026-11-07 triage).

* KOSIS / USDA_NASS: grid-scheduler logged ``PullerReportedFailure: 0 of 5
  items completed`` every week since 2026-10-04. Neither KOSIS_API_KEY nor
  USDA_NASS_API_KEY is set on grid-svr, so every item skipped before any
  request (runs took ~0.01 s). The run is SKIPPED with the missing key as the
  reason, not FAILED; the key itself is an owner action.
* ICIJ_OFFSHORE: the 2026-10-08..14 runs died with ``InFailedSqlTransaction``
  on a dedupe SELECT. The first failing statement was the signal_sources
  INSERT naming a ``metadata`` column that table does not have (verified on
  griddb 2026-11-07); in the old one-transaction loop its caught exception
  left the transaction aborted. Current code writes each batch and its
  signals in separate short transactions; the test below pins that with
  Postgres aborted-transaction semantics. A run that matched nobody now
  reports 0 writes instead of an opaque ``NO_MATCHES``.
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import MagicMock, patch

os.environ.setdefault("FRED_API_KEY", "test")

from ingestion.smart_scheduler import _classify_outcome


def _bare(cls: type, **attrs: Any) -> Any:
    obj = cls.__new__(cls)
    obj.engine = MagicMock()
    obj.source_id = 7
    for k, v in attrs.items():
        setattr(obj, k, v)
    return obj


# ── KOSIS / USDA_NASS without a key ─────────────────────────────────────


def test_kosis_without_key_is_skipped_with_reason_and_no_request() -> None:
    from ingestion.international.kosis import KOSISPuller

    puller = _bare(KOSISPuller, api_key="")
    with patch("ingestion.international.kosis.requests.get") as get, \
            patch("ingestion.international.kosis.time.sleep"):
        out = puller.pull_all()
    get.assert_not_called()
    outcome, rows, note = _classify_outcome(out)
    assert outcome == "SKIPPED"
    assert rows == 0
    assert "KOSIS_API_KEY" in note


def test_usda_nass_without_key_is_skipped_with_reason_and_no_request() -> None:
    from ingestion.physical.usda_nass import USDAPuller

    puller = _bare(USDAPuller, api_key="")
    with patch("ingestion.physical.usda_nass.requests.get") as get, \
            patch("ingestion.physical.usda_nass.time.sleep"):
        out = puller.pull_all()
    get.assert_not_called()
    outcome, rows, note = _classify_outcome(out)
    assert outcome == "SKIPPED"
    assert rows == 0
    assert "USDA_NASS_API_KEY" in note


def test_kosis_with_key_still_fails_when_every_request_fails() -> None:
    """The SKIPPED path is only for a missing key; real failures stay FAILED."""
    from ingestion.international.kosis import KOSISPuller

    puller = _bare(KOSISPuller, api_key="synthetic")
    with patch.object(KOSISPuller, "_fetch_kosis_data", side_effect=RuntimeError("bad tblId")), \
            patch("ingestion.international.kosis.time.sleep"):
        out = puller.pull_all()
    assert _classify_outcome(out)[0] == "FAILED"


# ── ICIJ_OFFSHORE ───────────────────────────────────────────────────────


class _AbortedTxn(Exception):
    """psycopg2 InFailedSqlTransaction stand-in."""


class _PgConn:
    """Postgres semantics: after any failed statement the transaction is aborted."""

    def __init__(self, log: list[tuple[str, str]]) -> None:
        self.aborted = False
        self.log = log

    def execute(self, stmt: Any, params: dict | None = None) -> MagicMock:
        sql = " ".join(str(stmt).split()).upper()
        if self.aborted:
            raise _AbortedTxn("current transaction is aborted")
        if sql.startswith("INSERT INTO SIGNAL_SOURCES") and "METADATA" in sql:
            self.aborted = True
            raise RuntimeError('column "metadata" of relation "signal_sources" does not exist')
        if sql.startswith("INSERT INTO RAW_SERIES"):
            self.log.append(("raw", str(params["sid"])))
        res = MagicMock()
        res.fetchall.return_value = []
        return res

    def begin_nested(self) -> None:
        raise AssertionError("no savepoints")


class _Begin:
    def __init__(self, log: list[tuple[str, str]], committed: list[tuple[str, str]]) -> None:
        self.log = log
        self.committed = committed
        self.pending: list[tuple[str, str]] = []

    def __enter__(self) -> _PgConn:
        self.conn = _PgConn(self.pending)
        return self.conn

    def __exit__(self, exc_type: Any, *rest: Any) -> bool:
        if exc_type is None and not self.conn.aborted:
            self.committed.extend(self.pending)
        return False


def _matches(n: int) -> list[dict]:
    return [{
        "actor_name": f"A{i}", "actor_id": f"a{i}", "officer_name": f"O{i}",
        "officer_node_id": str(i), "officer_jurisdiction": "VGB", "match_type": "exact",
        "connected_entities": [{"entity_name": f"E{i}", "entity_jurisdiction": "VGB"}],
    } for i in range(n)]


def test_offshore_signal_insert_failure_cannot_abort_raw_series_writes() -> None:
    """The 2026-10-14 failure mode: signal INSERT error, then InFailedSqlTransaction."""
    from ingestion.altdata import offshore_leaks as ol

    log: list[tuple[str, str]] = []
    committed: list[tuple[str, str]] = []
    puller = _bare(ol.OffshoreLeaksPuller)
    puller.engine.begin.side_effect = lambda: _Begin(log, committed)

    n = ol.STORE_BATCH_ROWS * 2 + 3
    out = puller.store_matches(_matches(n))
    assert out["failed_batches"] == 0
    assert out["raw_series_inserted"] == n
    assert len(committed) == n
    assert out["signals_emitted"] == 0  # the broken signal stream stays inactive


def test_offshore_no_matches_reports_zero_writes() -> None:
    from ingestion.altdata import offshore_leaks as ol

    puller = _bare(ol.OffshoreLeaksPuller)
    puller.ensure_data = lambda: True
    puller.match_actors = lambda: []
    out = puller.pull()
    outcome, rows, _ = _classify_outcome(out)
    assert outcome == "NO_NEW_DATA"
    assert rows == 0
