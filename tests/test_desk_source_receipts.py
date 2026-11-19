"""Receipts of what the Research Desk's sources said after each close (#1164).

* A receipt belongs to the latest closed XNYS session; its cutoff is that
  session's close and cannot be chosen.
* One receipt per session: a retry writes nothing.
* An unreadable source is recorded as unknown; an empty one as read, 0 rows.
* The log is chained and anchored; an edit, a truncation or a changed payload
  fails verification, against the local anchors or an off-host copy.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from intelligence import desk_source_receipts as dsr
from intelligence import sleuth_intersections as si

UTC = timezone.utc
WED = date(2026, 11, 18)
THU = date(2026, 11, 19)
WED_CLOSE = datetime(2026, 11, 18, 21, 0, tzinfo=UTC)
THU_CLOSE = datetime(2026, 11, 19, 21, 0, tzinfo=UTC)


def _clock(*moments: datetime):
    """A clock that returns each moment once, then repeats the last."""
    left = list(moments)
    return lambda: left.pop(0) if len(left) > 1 else left[0]


def _source(payload, rows=1):
    return lambda cutoff: dsr.SourceRead(payload=payload, rows=rows, scope={"note": "test"})


def _boom(cutoff):
    raise RuntimeError("connection to secret-host:5432 refused")


def _record(log, sources, at=WED_CLOSE + timedelta(minutes=30), session=None):
    return log.record(sources, session=session, now=_clock(at, at + timedelta(seconds=2)))


# ── sessions and cutoffs ────────────────────────────────────────────────────


@pytest.mark.parametrize(("session", "expected"), [
    (date(2026, 11, 18), datetime(2026, 11, 18, 21, 0, tzinfo=UTC)),   # 16:00 EST
    (date(2026, 7, 1), datetime(2026, 7, 1, 20, 0, tzinfo=UTC)),       # 16:00 EDT
    (date(2026, 11, 27), datetime(2026, 11, 27, 18, 0, tzinfo=UTC)),   # 13:00 early close
])
def test_the_cutoff_is_the_sessions_own_close(session, expected):
    assert dsr.session_cutoff(session) == expected


@pytest.mark.parametrize("closed", [date(2026, 11, 21), date(2026, 11, 26)])
def test_a_closed_day_has_no_cutoff(closed):
    with pytest.raises(dsr.ReceiptError, match="not an XNYS session"):
        dsr.session_cutoff(closed)


@pytest.mark.parametrize(("now", "expected"), [
    (WED_CLOSE - timedelta(seconds=1), date(2026, 11, 17)),
    (WED_CLOSE, WED),
    (datetime(2026, 11, 21, 15, 0, tzinfo=UTC), date(2026, 11, 20)),   # Saturday
    (datetime(2026, 11, 27, 3, 0, tzinfo=UTC), date(2026, 11, 25)),    # night after Thanksgiving
])
def test_latest_closed_session(now, expected):
    assert dsr.latest_closed_session(now) == expected


# ── one receipt per session ─────────────────────────────────────────────────


def test_a_receipt_names_the_bytes_each_source_returned(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    payload = {"leads": [{"id": "LEAD-INTX-a", "priority": 0.65}]}

    out = _record(log, {"sleuth_leads": _source(payload)})

    record = out["record"]
    assert out["written"] is True
    assert record["session"] == "2026-11-18"
    assert record["evidence_cutoff"] == WED_CLOSE.isoformat()
    assert record["read_started_at"] == (WED_CLOSE + timedelta(minutes=30)).isoformat()
    assert record["promotion_allowed"] is False
    entry = record["sources"]["sleuth_leads"]
    assert entry["status"] == "read" and entry["rows"] == 1
    assert json.loads(log.payload(entry["payload_sha256"])) == payload
    check = log.verify(full=True)
    assert check["ok"] and check["records"] == 2 and check["receipts"] == 1
    assert check["anchored_records"] == 2


def test_a_retry_writes_nothing_even_if_the_source_has_changed(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    first = _record(log, {"s": _source({"v": 1})})
    before = (log.path.read_bytes(), log.anchor_path.read_bytes())

    again = _record(log, {"s": _source({"v": 2})}, at=WED_CLOSE + timedelta(hours=3))

    assert again["written"] is False and again["record"] == first["record"]
    assert (log.path.read_bytes(), log.anchor_path.read_bytes()) == before
    assert len(list(log.payload_dir.iterdir())) == 1


def test_a_session_that_has_not_closed_cannot_be_recorded(tmp_path):
    log = dsr.ReceiptLog(tmp_path)

    with pytest.raises(dsr.ReceiptError, match="has not closed"):
        _record(log, {"s": _source({})}, at=THU_CLOSE - timedelta(hours=1), session=THU)

    assert not log.path.exists()


def test_yesterdays_session_cannot_be_recorded_after_todays_close(tmp_path):
    """The preview defect, at the source: an old brief date with a newer cutoff."""
    log = dsr.ReceiptLog(tmp_path)

    with pytest.raises(dsr.ReceiptError, match="stays unknown"):
        _record(log, {"s": _source({})}, at=THU_CLOSE + timedelta(minutes=5), session=WED)

    assert not log.path.exists()


def test_sessions_chain_in_order(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    _record(log, {"s": _source({"v": 1})})
    _record(log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))

    records = log.records()

    assert [r.get("session") for r in records] == [None, "2026-11-18", "2026-11-19"]
    assert log.receipt_for(THU)["sources"]["s"]["payload_sha256"] != \
        log.receipt_for(WED)["sources"]["s"]["payload_sha256"]
    assert log.receipt_for(date(2026, 11, 17)) is None
    check = log.verify(full=True)
    assert check["ok"] and check["receipts"] == 2


# ── unreadable is not empty ─────────────────────────────────────────────────


def test_a_failed_source_is_recorded_as_unknown_and_the_rest_are_read(tmp_path):
    log = dsr.ReceiptLog(tmp_path)

    record = _record(log, {"sleuth_leads": _boom, "factory_ledger": _source({"head": "x"})})["record"]

    failed = record["sources"]["sleuth_leads"]
    assert failed == {"status": "unknown", "failure": "RuntimeError",
                      "reason": "the read raised; see the run log"}
    assert record["sources"]["factory_ledger"]["status"] == "read"
    assert b"secret-host" not in log.path.read_bytes()
    assert log.verify(full=True)["ok"]


def test_zero_rows_is_a_read_not_an_unknown(tmp_path):
    log = dsr.ReceiptLog(tmp_path)

    entry = _record(log, {"sleuth_leads": _source({"leads": []}, rows=0)})["record"]["sources"]["sleuth_leads"]

    assert entry["status"] == "read" and entry["rows"] == 0 and entry["payload_sha256"]


def test_the_missing_thesis_log_is_on_every_record_with_its_reason(tmp_path):
    log = dsr.ReceiptLog(tmp_path)

    entry = _record(log, {"thesis_log": dsr.thesis_log_source()})["record"]["sources"]["thesis_log"]

    assert entry["status"] == "unknown" and entry["failure"] == "SourceUnavailable"
    assert "#1165" in entry["reason"]


def test_a_payload_that_cannot_be_serialized_is_unknown(tmp_path):
    log = dsr.ReceiptLog(tmp_path)

    entry = _record(log, {"s": _source({"p": float("nan")})})["record"]["sources"]["s"]

    assert entry["status"] == "unknown" and entry["failure"] == "ValueError"


# ── tamper evidence ─────────────────────────────────────────────────────────


def _two_sessions(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    _record(log, {"s": _source({"v": 1})})
    _record(log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))
    return log


def test_an_edited_record_fails(tmp_path):
    log = _two_sessions(tmp_path)
    log.path.write_bytes(log.path.read_bytes().replace(b'"rows":1', b'"rows":9', 1))

    assert not log.verify()["ok"]


def test_a_truncated_log_fails_against_its_anchors(tmp_path):
    log = _two_sessions(tmp_path)
    lines = log.path.read_bytes().splitlines(keepends=True)
    log.path.write_bytes(b"".join(lines[:-1]))

    check = log.verify()

    assert not check["ok"] and "truncated" in check["detail"]


def test_a_changed_or_missing_payload_fails_the_full_check(tmp_path):
    log = _two_sessions(tmp_path)
    digest = log.receipt_for(WED)["sources"]["s"]["payload_sha256"]
    path = log.payload_dir / f"{digest}.json"
    path.write_bytes(b'{"v":3}')

    assert log.verify()["ok"]                # the chain alone does not read payloads
    assert not log.verify(full=True)["ok"]
    path.unlink()
    assert not log.verify(full=True)["ok"]


def test_a_forged_cutoff_fails(tmp_path):
    """A record whose cutoff is not its session's close is not a receipt."""
    log = dsr.ReceiptLog(tmp_path)
    forged = {
        "kind": "receipt", "session": "2026-11-18",
        "evidence_cutoff": THU_CLOSE.isoformat(),          # today's cutoff on yesterday's date
        "read_started_at": (THU_CLOSE + timedelta(minutes=1)).isoformat(),
        "read_finished_at": (THU_CLOSE + timedelta(minutes=2)).isoformat(),
        "sources": {"s": {"status": "unknown", "failure": "X", "reason": "y"}},
        "promotion_allowed": False,
    }
    log._append(forged, THU_CLOSE)

    check = log.verify()

    assert not check["ok"] and "not the session's close" in check["detail"]


def test_a_rewritten_log_fails_against_an_off_host_anchor_copy(tmp_path):
    log = dsr.ReceiptLog(tmp_path / "log")
    _record(log, {"s": _source({"v": 1})})
    vault = tmp_path / "vault"
    exported = log.export_anchors(vault)
    witness = vault / dsr.WITNESS_DIR / dsr.ANCHOR_NAME
    assert len(exported) == 1 and log.verify(witness)["witnessed_records"] == 2

    # Same-host rewrite: a fresh log and fresh local anchors for the same session.
    rewritten = dsr.ReceiptLog(tmp_path / "rewrite")
    _record(rewritten, {"s": _source({"v": "changed"})})

    assert rewritten.verify()["ok"]                       # consistent with itself
    check = rewritten.verify(witness)
    assert not check["ok"] and "rewritten" in check["detail"]


def test_export_appends_only_what_the_vault_lacks(tmp_path):
    log = dsr.ReceiptLog(tmp_path / "log")
    vault = tmp_path / "vault"
    _record(log, {"s": _source({"v": 1})})
    assert len(log.export_anchors(vault)) == 1
    assert log.export_anchors(vault) == []
    _record(log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))

    assert len(log.export_anchors(vault)) == 1
    witness = vault / dsr.WITNESS_DIR / dsr.ANCHOR_NAME
    assert witness.read_bytes() == log.anchor_path.read_bytes()


def test_export_refuses_a_witness_file_of_another_log(tmp_path):
    log = dsr.ReceiptLog(tmp_path / "log")
    other = dsr.ReceiptLog(tmp_path / "other")
    vault = tmp_path / "vault"
    _record(other, {"s": _source({"v": "other"})})
    other.export_anchors(vault)
    _record(log, {"s": _source({"v": 1})})

    with pytest.raises(dsr.ReceiptError, match="another log"):
        log.export_anchors(vault)


def test_an_unanchored_tail_is_reported_and_the_next_append_covers_it(tmp_path):
    """A crash between the log append and the anchor append must not wedge the log."""
    log = _two_sessions(tmp_path)
    anchors = log.anchor_path.read_bytes().splitlines(keepends=True)
    log.anchor_path.write_bytes(b"".join(anchors[:-1]))

    check = log.verify()
    assert check["ok"] and check["records"] == 3 and check["anchored_records"] == 2

    friday = datetime(2026, 11, 20, 21, 30, tzinfo=UTC)
    _record(log, {"s": _source({"v": 3})}, at=friday)
    assert log.verify(full=True)["anchored_records"] == 4


# ── the real sources ────────────────────────────────────────────────────────


def _lead_engine():
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE investigation_leads (id TEXT PRIMARY KEY, question TEXT, category TEXT, "
            "priority NUMERIC, evidence TEXT, status TEXT, findings TEXT, follow_up_leads TEXT, "
            "hypotheses TEXT, created_at TIMESTAMP, resolved_at TEXT, origin TEXT, outcome TEXT, "
            "outcome_reason TEXT, declared_hypothesis_id TEXT)")
        conn.exec_driver_sql(
            "CREATE TABLE discovered_hypotheses (id TEXT PRIMARY KEY, test_criteria TEXT, "
            "status TEXT, created_at TEXT, last_tested TEXT, times_tested INTEGER, "
            "times_correct INTEGER, kill_reason TEXT, killed_at TEXT, scoring_version TEXT, "
            "trial_p_value REAL, trial_q_value REAL, trial_ledger_sha256 TEXT)")
    return engine


def _add_lead(engine, lead_id, created_at, origin="intersection", declared=None):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO investigation_leads (id, question, category, priority, evidence, status, "
            "follow_up_leads, hypotheses, created_at, origin, declared_hypothesis_id) VALUES "
            "(:id, 'q', 'connection_found', 0.6, '[]', 'new', '[]', '[]', :created, :origin, :declared)"
        ), {"id": lead_id, "created": created_at, "origin": origin, "declared": declared})


def test_sleuth_source_reads_intersection_leads_up_to_the_cutoff_with_their_tests():
    engine = _lead_engine()
    in_scope = "LEAD-INTX-bbbbbbbbbbbb"
    _add_lead(engine, in_scope, WED_CLOSE - timedelta(days=3), declared=si.sleuth_test_id(in_scope))
    _add_lead(engine, "LEAD-INTX-aaaaaaaaaaaa", WED_CLOSE - timedelta(days=1))
    _add_lead(engine, "LEAD-INTX-after", WED_CLOSE + timedelta(minutes=1))
    _add_lead(engine, "LEAD-INTX-old", WED_CLOSE - timedelta(days=dsr.LEAD_LOOKBACK_DAYS + 1))
    _add_lead(engine, "LEAD-chat", WED_CLOSE - timedelta(days=1), origin="chat")
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO discovered_hypotheses (id, test_criteria, status, times_tested) "
            "VALUES (:id, :criteria, 'active', 0)"),
            {"id": si.sleuth_test_id(in_scope),
             "criteria": json.dumps({"source": "sleuth", "lead_id": in_scope, "ticker": "BAH"})})
    source = dsr.sleuth_leads_source(engine)

    read = source(WED_CLOSE)

    leads = read.payload["leads"]
    assert [lead["id"] for lead in leads] == ["LEAD-INTX-aaaaaaaaaaaa", in_scope]   # sorted by id
    assert read.rows == 2
    assert leads[1]["declared_tests"][0]["id"] == si.sleuth_test_id(in_scope)
    assert leads[0]["declared_tests"] == []
    assert read.scope["created_until"] == WED_CLOSE.isoformat()
    assert dsr.canonical(source(WED_CLOSE).payload) == dsr.canonical(read.payload)


def test_sleuth_source_with_no_table_is_unknown_on_the_record(tmp_path):
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    log = dsr.ReceiptLog(tmp_path)

    entry = _record(log, {"sleuth_leads": dsr.sleuth_leads_source(engine)})["record"]["sources"]["sleuth_leads"]

    assert entry["status"] == "unknown" and entry["failure"] == "OperationalError"


def test_factory_source_reads_a_ledger_through_its_anchor(tmp_path):
    from analysis import ledger_steered_exploration as lse

    ledger, anchor = tmp_path / "f.jsonl", tmp_path / "f.anchors.jsonl"
    lse.Ledger.create_factory(ledger, ledger_id="factory-test", anchor=anchor)
    source = dsr.factory_ledger_source(ledger, anchor)

    read = source(WED_CLOSE)

    assert read.rows == 1 and read.payload["ledger_id"] == "factory-test"
    assert read.payload["head_sha256"] == lse.Ledger(ledger, anchor=anchor).head
    assert read.payload["summary"]["runs_declared"] == 0


def test_factory_source_with_a_truncated_ledger_is_unknown_on_the_record(tmp_path):
    from analysis import ledger_steered_exploration as lse

    ledger, anchor = tmp_path / "f.jsonl", tmp_path / "f.anchors.jsonl"
    lse.Ledger.create_factory(ledger, ledger_id="factory-test", anchor=anchor)
    ledger.write_bytes(b"")
    log = dsr.ReceiptLog(tmp_path / "log")

    entry = _record(log, {"factory_ledger": dsr.factory_ledger_source(ledger, anchor)})["record"]["sources"]["factory_ledger"]

    assert entry["status"] == "unknown"


# ── command line ────────────────────────────────────────────────────────────


def test_verify_command_exits_nonzero_on_a_broken_log(tmp_path, capsys):
    log = _two_sessions(tmp_path)
    assert dsr.main(["verify", "--dir", str(tmp_path)]) == 0
    log.path.write_bytes(log.path.read_bytes().replace(b'"rows":1', b'"rows":9', 1))

    capsys.readouterr()

    assert dsr.main(["verify", "--dir", str(tmp_path)]) == 1
    assert json.loads(capsys.readouterr().out)["ok"] is False
