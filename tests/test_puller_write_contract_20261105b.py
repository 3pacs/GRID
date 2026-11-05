"""Write-count contract for Etherscan, FinDKG and ICIJ_Full (grid-scheduler).

Production audit (grid-svr, 2026-11-05): all three were recorded FAILED with
``puller did not report committed write count`` although Etherscan committed
31 raw_series rows on its 20:44Z run and ICIJ_Full loaded 5.35M rows at
05:14Z. Their returns had no row-count key (``api_calls``/``whale_balances``,
``total_relationships``, per-table counts). FinDKG has never had data files
on grid-svr, so its honest outcome is SKIPPED with the reason, not FAILED.

Only grid-scheduler (``ingestion/scheduler.py::run_pull_group``) runs these
three; it classifies the return with ``smart_scheduler._classify_outcome``,
which every test here applies to the real puller's return value.

The earnings_calendar UniqueViolation from the same audit is covered by
``tests/test_earnings_calendar_dedupe.py``: every occurrence in the journal
(IVA, last at 05:15Z) came from grid-hermes still running the stale
``grid_repo`` checkout (2026-09-11, before ``_dedupe_earnings_rows``), and
none occurred after the unit moved to the release tree.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

os.environ.setdefault("FRED_API_KEY", "test")

from ingestion.smart_scheduler import _classify_outcome
from tests.test_puller_write_contract_20261105 import (
    OPAQUE,
    FakeConn,
    FakeEngine,
    FakeResult,
    Resp,
    _bare,
    _no_sleep,
    _wiring_grid_scheduler,
)


class StatefulConn(FakeConn):
    """INSERT affects a row only the first time its key is seen.

    Models ``ON CONFLICT DO NOTHING`` / ``WHERE NOT EXISTS`` inserts, and
    answers the raw_series existence probe from rows inserted so far, so a
    second run against the same engine sees what the first one stored.
    """

    def __init__(self) -> None:
        super().__init__()
        self.keys: set[tuple] = set()

    @staticmethod
    def _key(sql: str, p: dict) -> tuple:
        table = sql.split("INSERT INTO ", 1)[1].split()[0]
        if table == "raw_series":
            return (table, p["sid"], p["od"])
        if table == "icij_relationships":
            return (table, p["from_n"], p["to_n"], p["rel"], p["src"])
        return (table, p.get("nid", p.get("node_id")))

    def execute(self, stmt: Any, params: dict | None = None) -> FakeResult:
        sql = str(stmt)
        p = dict(params or {})
        if sql.lstrip().upper().startswith("SELECT 1 FROM RAW_SERIES"):
            self.statements.append((sql, p))
            hit = any(k[0] == "raw_series" and k[1] == p["sid"] for k in self.keys)
            return FakeResult(rowcount=0, one=(1,) if hit else None)
        if "INSERT INTO" in sql:
            self.statements.append((sql, p))
            key = self._key(sql, p)
            if key in self.keys:
                return FakeResult(rowcount=0)
            self.keys.add(key)
            return FakeResult(rowcount=1)
        return super().execute(stmt, params)


# ── Etherscan ───────────────────────────────────────────────────────────

_WEI = str(10**18)


def _etherscan_api(overrides: dict[str, Any] | None = None) -> Any:
    """Route Etherscan v2 calls by ``action``; overrides replace a response."""
    payloads: dict[str, Any] = {
        "ethprice": {"status": "1", "result": {"ethusd": "2247.98", "ethbtc": "0.0296",
                                               "ethusd_timestamp": "1762375458"}},
        "gasoracle": {"status": "1", "result": {"SafeGasPrice": "0.16", "ProposeGasPrice": "0.17",
                                                "FastGasPrice": "0.18", "suggestBaseFee": "0.16"}},
        "ethsupply2": {"status": "1", "result": {"EthSupply": "1" + "0" * 26, "Eth2Staking": _WEI,
                                                 "BurntFees": _WEI, "WithdrawnTotal": _WEI}},
        "balance": {"status": "1", "result": _WEI},
        "tokensupply": {"status": "1", "result": "123456"},
        "txlist": {"status": "1", "result": [{"value": str(2000 * 10**18), "from": "a", "to": "b",
                                              "hash": "h", "timeStamp": "1"}]},
    }
    payloads.update(overrides or {})

    def get(url: str, params: dict | None = None, timeout: Any = None) -> Resp:
        reply = payloads[params["action"]]
        return reply if isinstance(reply, Resp) else Resp(200, reply)

    return get


def _run_etherscan(get: Any, engine: FakeEngine | None = None, api_key: str = "k") -> dict:
    import ingestion.altdata.etherscan_puller as mod

    puller = _bare(mod.EtherscanPuller, engine, api_key=api_key)
    with patch.object(mod.requests, "get", side_effect=get), \
         patch.object(mod.time, "sleep"), _no_sleep(), \
         patch("intelligence.actor_ingest.ingest_actor", return_value=False):
        return puller.pull()


class TestEtherscan:
    def test_success_reports_committed_raw_rows(self) -> None:
        # 2 price + 4 gas + 3 supply + 14 whales + 8 tokens = 31, as on
        # grid-svr 2026-11-05 20:44Z (31 raw_series rows, run marked FAILED).
        engine = FakeEngine()
        out = _run_etherscan(_etherscan_api(), engine)
        assert out["status"] == "SUCCESS"
        assert out["rows_inserted"] == 31 == len(engine.conn.inserts())
        assert out["lookups"] == 30 and out["lookups_failed"] == 0
        assert out["whale_balances"] == 14 and len(out["anomalies"]) == 5
        assert _classify_outcome(out) == ("SUCCESS", 31, None)

    def test_missing_api_key_is_skipped_not_failed(self) -> None:
        engine = FakeEngine()
        out = _run_etherscan(_etherscan_api(), engine, api_key="")
        assert _classify_outcome(out) == ("SKIPPED", 0, "ETHERSCAN_API_KEY not configured")
        assert engine.begins == 0

    def test_every_lookup_failing_is_failed_with_reason(self) -> None:
        engine = FakeEngine()
        out = _run_etherscan(lambda *a, **k: Resp(429), engine)
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert "30 of 30 lookups failed" in out["error"] and "429" in out["error"]
        outcome, rows, note = _classify_outcome(out)
        assert (outcome, rows) == ("FAILED", 0) and note != OPAQUE
        assert engine.begins == 0

    def test_rate_limited_token_supply_is_partial_with_committed_rows(self) -> None:
        # Etherscan answers rate limits with HTTP 200, status "0" and a text
        # result; float() of that text is a failed lookup, not a value.
        limited = {"status": "0", "message": "NOTOK", "result": "Max rate limit reached"}
        engine = FakeEngine()
        out = _run_etherscan(_etherscan_api({"tokensupply": limited}), engine)
        assert out["status"] == "PARTIAL"
        assert out["rows_inserted"] == 23 == len(engine.conn.inserts())
        assert out["lookups_failed"] == 8
        assert _classify_outcome(out)[:2] == ("PARTIAL", 23)

    def test_failed_write_is_not_counted(self) -> None:
        class FailingWhaleConn(FakeConn):
            def execute(self, stmt: Any, params: dict | None = None) -> FakeResult:
                if params and str(params.get("sid", "")).startswith("eth:whale:"):
                    raise RuntimeError("db write failed")
                return super().execute(stmt, params)

        engine = FakeEngine(FailingWhaleConn())
        out = _run_etherscan(_etherscan_api(), engine)
        assert out["rows_inserted"] == 17 == len(engine.conn.inserts())
        assert out["whale_balances"] == 0
        assert _classify_outcome(out)[:2] == ("PARTIAL", 17)

    def test_grid_scheduler_wiring(self) -> None:
        _wiring_grid_scheduler("Etherscan", "ingestion.altdata.etherscan_puller",
                               "EtherscanPuller", "pull", "etherscan")


# ── FinDKG ──────────────────────────────────────────────────────────────


def _triple(head: str, rel: str, tail: str) -> dict:
    return {"head": head, "relation": rel, "tail": tail, "timestamp": "2023-01-01"}


def _run_findkg(data_dir: Path, engine: FakeEngine) -> dict:
    import ingestion.altdata.findkg_puller as mod

    puller = _bare(mod.FinDKGPuller, engine)
    with patch("intelligence.actor_ingest.ingest_actor", return_value=False):
        return puller.pull(data_path=str(data_dir))


def _write_jsonl(path: Path, records: list[Any]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")


class TestFinDKG:
    def test_no_data_files_is_skipped_with_reason(self, tmp_path: Path) -> None:
        # grid-svr 2026-11-05 06:11Z: "0 relationships loaded", run FAILED.
        engine = FakeEngine()
        out = _run_findkg(tmp_path, engine)
        outcome, rows, note = _classify_outcome(out)
        assert (outcome, rows) == ("SKIPPED", 0)
        assert "no FinDKG data files" in note
        assert engine.begins == 0

    def test_success_reports_committed_rows_and_skips_in_file_duplicate(self, tmp_path: Path) -> None:
        _write_jsonl(tmp_path / "findkg.jsonl", [
            _triple("Apple", "Supplier Of", "Foxconn"),
            _triple("Apple", "Competitor Of", "Samsung"),
            _triple("Apple", "Supplier Of", "Foxconn"),  # same series, same txn
            {"head": "Apple", "tail": ""},                # malformed
        ])
        engine = FakeEngine(StatefulConn())
        out = _run_findkg(tmp_path, engine)
        assert out["status"] == "SUCCESS"
        assert out["rows_inserted"] == 2 == len(engine.conn.inserts())
        assert out["already_stored"] == 1 and out["malformed_records"] == 1
        assert out["total_relationships"] == 3
        assert out["by_type"] == {"supplier_of": 2, "competitor_of": 1}
        assert _classify_outcome(out) == ("SUCCESS", 2, None)

    def test_rerun_of_same_dataset_is_no_new_data(self, tmp_path: Path) -> None:
        _write_jsonl(tmp_path / "findkg.jsonl", [_triple("Apple", "partner_of", "OpenAI")])
        engine = FakeEngine(StatefulConn())
        assert _classify_outcome(_run_findkg(tmp_path, engine))[:2] == ("SUCCESS", 1)
        out = _run_findkg(tmp_path, engine)
        assert out["rows_inserted"] == 0 and len(engine.conn.inserts()) == 1
        assert _classify_outcome(out) == ("NO_NEW_DATA", 0, "run completed, 0 rows written")

    def test_corrupt_file_beside_good_file_is_partial(self, tmp_path: Path) -> None:
        _write_jsonl(tmp_path / "a.jsonl", [_triple("Apple", "acquires", "Beats")])
        (tmp_path / "b.json").write_text("{not json")
        out = _run_findkg(tmp_path, FakeEngine(StatefulConn()))
        assert out["status"] == "PARTIAL" and out["rows_inserted"] == 1
        assert "b.json" in out["error"]
        assert _classify_outcome(out)[:2] == ("PARTIAL", 1)

    def test_only_unusable_files_is_failed(self, tmp_path: Path) -> None:
        (tmp_path / "b.json").write_text("{not json")
        _write_jsonl(tmp_path / "c.jsonl", [{"subject": "x"}])  # no valid triple
        out = _run_findkg(tmp_path, FakeEngine(StatefulConn()))
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert "no valid triples" in out["error"]
        assert _classify_outcome(out)[:2] == ("FAILED", 0)

    def test_grid_scheduler_wiring(self) -> None:
        _wiring_grid_scheduler("FinDKG", "ingestion.altdata.findkg_puller",
                               "FinDKGPuller", "pull", "findkg")


# ── ICIJ_Full ───────────────────────────────────────────────────────────

_ICIJ_FILES = {
    "nodes-entities.csv": "node_id,name,jurisdiction\n1,Shell A,PA\n2,Shell B,BVI\n",
    "nodes-officers.csv": "node_id,name,country_codes\n10,Jane Roe,GBR\n",
    "nodes-intermediaries.csv": "node_id,name,country_codes\n20,Law Firm,PAN\n",
    "nodes-addresses.csv": "node_id,address,country_codes\n30,1 Main St,PAN\n",
    "relationships.csv": (
        "node_id_start,node_id_end,rel_type,sourceID\n"
        "10,1,officer_of,Panama Papers\n"
        "20,1,intermediary_of,Panama Papers\n"
        "10,1,officer_of,Panama Papers\n"  # identical edge in the same dump
    ),
}


def _icij_dir(tmp_path: Path, skip: tuple[str, ...] = ()) -> Path:
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    for name, body in _ICIJ_FILES.items():
        if name not in skip:
            (csv_dir / name).write_text(body)
    return csv_dir


def _run_icij(csv_dir: Path, engine: FakeEngine) -> dict:
    from ingestion.altdata.icij_puller import ICIJPuller

    puller = _bare(ICIJPuller, engine)
    puller.download_data = lambda: csv_dir
    puller._discover_actors = lambda: 0
    return puller.pull()


class TestICIJFull:
    def test_success_reports_rows_actually_added(self, tmp_path: Path) -> None:
        engine = FakeEngine(StatefulConn())
        out = _run_icij(_icij_dir(tmp_path), engine)
        assert out["status"] == "SUCCESS"
        assert out["by_table"] == {
            "icij_entities": 2, "icij_officers": 1, "icij_intermediaries": 1,
            "icij_addresses": 1, "icij_relationships": 2,
        }
        assert out["rows_inserted"] == 7
        assert _classify_outcome(out) == ("SUCCESS", 7, None)

    def test_relationship_insert_skips_identical_stored_edge(self, tmp_path: Path) -> None:
        engine = FakeEngine(StatefulConn())
        _run_icij(_icij_dir(tmp_path), engine)
        rel_sql = {sql for sql, _ in engine.conn.statements if "INSERT INTO icij_relationships" in sql}
        assert len(rel_sql) == 1
        sql = " ".join(rel_sql.pop().split())
        assert "WHERE NOT EXISTS (SELECT 1 FROM icij_relationships WHERE from_node = :from_n" in sql

    def test_reload_of_unchanged_dump_is_no_new_data(self, tmp_path: Path) -> None:
        engine = FakeEngine(StatefulConn())
        csv_dir = _icij_dir(tmp_path)
        _run_icij(csv_dir, engine)
        out = _run_icij(csv_dir, engine)
        assert out["rows_inserted"] == 0
        assert _classify_outcome(out) == ("NO_NEW_DATA", 0, "run completed, 0 rows written")

    def test_missing_file_is_partial_with_committed_rows(self, tmp_path: Path) -> None:
        out = _run_icij(_icij_dir(tmp_path, skip=("relationships.csv",)), FakeEngine(StatefulConn()))
        assert out["status"] == "PARTIAL" and out["rows_inserted"] == 5
        assert "no CSV found for relationships" in out["error"]
        assert _classify_outcome(out)[:2] == ("PARTIAL", 5)

    def test_failed_load_is_not_counted(self, tmp_path: Path) -> None:
        class FailingOfficers(StatefulConn):
            def execute(self, stmt: Any, params: dict | None = None) -> FakeResult:
                if "INSERT INTO icij_officers" in str(stmt):
                    raise RuntimeError("officers load failed")
                return super().execute(stmt, params)

        out = _run_icij(_icij_dir(tmp_path), FakeEngine(FailingOfficers()))
        assert out["status"] == "PARTIAL" and "icij_officers" not in out["by_table"]
        assert out["rows_inserted"] == 6 and "officers load failed" in out["error"]
        assert _classify_outcome(out)[:2] == ("PARTIAL", 6)

    def test_nothing_loaded_is_failed(self, tmp_path: Path) -> None:
        out = _run_icij(_icij_dir(tmp_path, skip=tuple(_ICIJ_FILES)), FakeEngine(StatefulConn()))
        assert out["status"] == "FAILED" and out["rows_inserted"] == 0
        assert _classify_outcome(out)[:2] == ("FAILED", 0)

    def test_grid_scheduler_wiring(self) -> None:
        _wiring_grid_scheduler("ICIJ_Full", "ingestion.altdata.icij_puller",
                               "ICIJPuller", "pull", "icij_offshore_leaks")


@pytest.mark.parametrize("source", ["Etherscan", "FinDKG", "ICIJ_Full"])
def test_no_source_regresses_to_opaque_return(source: str, tmp_path: Path) -> None:
    """A run that committed rows must classify SUCCESS with that count."""
    if source == "Etherscan":
        engine = FakeEngine()
        out = _run_etherscan(_etherscan_api(), engine)
    elif source == "FinDKG":
        _write_jsonl(tmp_path / "f.jsonl", [_triple("A", "partner_of", "B")])
        engine = FakeEngine(StatefulConn())
        out = _run_findkg(tmp_path, engine)
    else:
        engine = FakeEngine(StatefulConn())
        out = _run_icij(_icij_dir(tmp_path), engine)
    outcome, rows, note = _classify_outcome(out)
    assert note != OPAQUE, f"{source} regressed to an opaque return"
    writes = sum(1 for sql, _ in engine.conn.statements if "INSERT INTO" in sql)
    assert writes > 0 and outcome == "SUCCESS" and rows is not None and 0 < rows <= writes
