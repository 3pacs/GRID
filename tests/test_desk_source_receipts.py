"""Receipts of what the Research Desk's sources said after each close (#1164).

* A receipt belongs to the latest closed XNYS session; its cutoff is that
  session's close and cannot be chosen.
* One receipt per session: a retry writes nothing.
* An unreadable source is recorded as unknown; an empty one as read, 0 rows.
* The log is chained and anchored; an edit, a truncation or a changed payload
  fails verification, against the local anchors or an off-host copy.
* Sessions and closes come from the pinned XNYS manifest; a date outside it
  is refused.
* A run stopped between the receipt and its anchor is finished by the next
  run, on the first write and on later ones, without touching a record.
* Handing a head to the witness can be retried for the same session and head;
  local anchoring and off-host witnessing are reported separately.
* Each source is one version: the factory payload describes one read of the
  ledger and its anchor, Sleuth's leads and tests one database snapshot.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, text
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
    (date(2027, 7, 1), datetime(2027, 7, 1, 20, 0, tzinfo=UTC)),       # 16:00 EDT
    (date(2026, 11, 27), datetime(2026, 11, 27, 18, 0, tzinfo=UTC)),   # 13:00 early close
])
def test_the_cutoff_is_the_sessions_own_close(session, expected):
    assert dsr.session_cutoff(session) == expected


@pytest.mark.parametrize("closed", [date(2026, 11, 21), date(2026, 11, 26)])
def test_a_closed_day_has_no_cutoff(closed):
    with pytest.raises(dsr.ReceiptError, match="not an XNYS session"):
        dsr.session_cutoff(closed)


@pytest.mark.parametrize("outside", [date(2026, 9, 30), date(2028, 1, 3)])
def test_a_date_the_pinned_calendar_does_not_cover_is_refused(outside):
    """No fallback to an approximate calendar: an unpinned date has no cutoff."""
    with pytest.raises(dsr.ReceiptError, match="pinned XNYS calendar"):
        dsr.session_cutoff(outside)


def test_the_calendar_is_the_manifest_the_native_volume_job_pins():
    from ingestion import gex_native_volume as gnv

    assert (dsr.CALENDAR_PATH, dsr.CALENDAR_SHA256) == (gnv.CALENDAR_PATH, gnv.CALENDAR_SHA256)
    assert hashlib.sha256(dsr.CALENDAR_PATH.read_bytes()).hexdigest() == dsr.CALENDAR_SHA256


def test_the_pinned_calendar_and_the_scheduler_helpers_agree_on_every_covered_day():
    """Reconciliation, both ways, over the manifest's whole range: the same
    session days and the same closes. The receipt log reads only the manifest;
    this is what says the approximate helpers would not have answered differently."""
    from ingestion.market_calendar import is_market_open, market_close_time

    calendar = dsr._calendar()
    day, differences = calendar.start, []
    while day <= calendar.end:
        pinned = calendar.for_date(day)
        if (pinned is not None) != is_market_open(day):
            differences.append((day.isoformat(), "session"))
        elif pinned is not None:
            helper = datetime.combine(day, market_close_time(day), tzinfo=dsr._NY)
            if helper.astimezone(UTC) != pinned.closes:
                differences.append((day.isoformat(), "close"))
        day += timedelta(days=1)

    assert differences == []


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
    at = (WED_CLOSE + timedelta(minutes=30, seconds=2)).isoformat()
    assert failed == {"status": "unknown", "failure": "RuntimeError",
                      "reason": "the read raised; see the run log",
                      "read_started_at": at, "read_finished_at": at}
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
    assert check["unanchored_records"] == 1 and check["qualified"]["local_anchor"] is False

    friday = datetime(2026, 11, 20, 21, 30, tzinfo=UTC)
    _record(log, {"s": _source({"v": 3})}, at=friday)
    assert log.verify(full=True)["anchored_records"] == 4


# ── a run stopped between the receipt and its anchor ────────────────────────


class _Stopped(BaseException):
    """The process dying: not an error the recorder may catch."""


def _stop_before_the_anchor(log, monkeypatch):
    def die(*_args, **_kwargs):
        raise _Stopped

    monkeypatch.setattr(log, "_append_anchor", die)


def _anchors(log):
    return [json.loads(line) for line in dsr.raw_lines(log.anchor_path)]


def test_a_first_write_stopped_before_its_anchor_is_finished_by_the_retry(tmp_path, monkeypatch):
    log = dsr.ReceiptLog(tmp_path)
    with monkeypatch.context() as stopped:
        _stop_before_the_anchor(log, stopped)
        with pytest.raises(_Stopped):
            _record(log, {"s": _source({"v": 1})})
    written = log.path.read_bytes()
    assert written and not log.anchor_path.exists()

    check = log.verify()
    assert check["ok"] and check["records"] == 2 and check["anchored_records"] == 0
    assert check["unanchored_records"] == 2
    assert check["qualified"] == {"local_anchor": False, "external_witness": None}

    retry = _record(log, {"s": _source({"v": "changed since"})}, at=WED_CLOSE + timedelta(hours=3))

    assert retry["written"] is False and retry["recovered_anchor"] is True
    assert log.path.read_bytes() == written                 # no record rewritten or added
    assert len(list(log.payload_dir.iterdir())) == 1
    assert _anchors(log) == [{
        "records": 2, "head_sha256": hashlib.sha256(written.splitlines()[-1]).hexdigest(),
        "run_at": (WED_CLOSE + timedelta(hours=3)).isoformat(),
        "prev_anchor_sha256": None, "recovered": True}]
    after = log.verify(full=True)
    assert after["ok"] and after["unanchored_records"] == 0
    assert after["qualified"]["local_anchor"] is True


def test_a_later_write_stopped_before_its_anchor_is_finished_by_the_same_session_retry(
        tmp_path, monkeypatch):
    log = dsr.ReceiptLog(tmp_path)
    _record(log, {"s": _source({"v": 1})})
    with monkeypatch.context() as stopped:
        _stop_before_the_anchor(log, stopped)
        with pytest.raises(_Stopped):
            _record(log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))
    written, first_anchor = log.path.read_bytes(), _anchors(log)
    assert log.verify()["unanchored_records"] == 1

    retry = _record(log, {"s": _source({"v": 3})}, at=THU_CLOSE + timedelta(hours=3))

    assert retry["written"] is False and retry["recovered_anchor"] is True
    assert retry["record"]["session"] == "2026-11-19"
    assert log.path.read_bytes() == written
    anchors = _anchors(log)
    assert anchors[:1] == first_anchor                      # the old anchor line is untouched
    assert anchors[1]["records"] == 3 and anchors[1]["recovered"] is True
    # A third run finds nothing to finish, and the next session appends normally.
    again = _record(log, {"s": _source({"v": 4})}, at=THU_CLOSE + timedelta(hours=4))
    assert again["recovered_anchor"] is False and len(_anchors(log)) == 2
    friday = datetime(2026, 11, 20, 21, 30, tzinfo=UTC)
    assert _record(log, {"s": _source({"v": 5})}, at=friday)["written"] is True
    assert log.verify(full=True)["qualified"]["local_anchor"] is True


def test_more_than_one_unanchored_record_is_not_an_interruption(tmp_path):
    log = _two_sessions(tmp_path)
    friday = datetime(2026, 11, 20, 21, 30, tzinfo=UTC)
    _record(log, {"s": _source({"v": 3})}, at=friday)
    anchors = log.anchor_path.read_bytes().splitlines(keepends=True)
    log.anchor_path.write_bytes(anchors[0])
    before = log.anchor_path.read_bytes()

    check = log.verify()

    assert not check["ok"] and "beyond the last anchor" in check["detail"]
    assert check["qualified"] == {"local_anchor": False, "external_witness": False}
    with pytest.raises(dsr.ReceiptError, match="broken"):
        _record(log, {"s": _source({"v": 4})}, at=friday + timedelta(hours=1))
    assert log.anchor_path.read_bytes() == before


def test_a_missing_anchor_file_on_a_longer_log_is_broken_not_recovered(tmp_path):
    log = _two_sessions(tmp_path)
    log.anchor_path.unlink()

    assert not log.verify()["ok"]
    with pytest.raises(dsr.ReceiptError, match="broken"):
        _record(log, {"s": _source({"v": 3})}, at=THU_CLOSE + timedelta(hours=2))
    assert not log.anchor_path.exists()


def test_a_receipt_written_before_the_added_fields_still_verifies(tmp_path):
    """The log that has run since 2026-11-18 has no per-source clocks, no
    calendar hash and no state_as_of. It must stay valid, byte for byte."""
    log = dsr.ReceiptLog(tmp_path)
    old = {
        "kind": "receipt", "session": "2026-11-18", "evidence_cutoff": WED_CLOSE.isoformat(),
        "read_started_at": (WED_CLOSE + timedelta(minutes=20)).isoformat(),
        "read_finished_at": (WED_CLOSE + timedelta(minutes=21)).isoformat(),
        "sources": {"s": {"status": "unknown", "failure": "SourceUnavailable", "reason": "y"}},
        "promotion_allowed": False,
    }
    log._append(old, WED_CLOSE + timedelta(minutes=20))
    before = log.path.read_bytes()

    assert log.verify(full=True)["ok"]
    new = _record(log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))["record"]

    assert log.path.read_bytes().startswith(before)
    assert new["state_as_of"] == "read" and new["calendar_sha256"] == dsr.CALENDAR_SHA256
    assert log.verify(full=True)["ok"] and log.verify()["receipts"] == 2


def test_each_source_carries_its_own_read_clocks_inside_the_receipts(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    start = WED_CLOSE + timedelta(minutes=30)
    ticks = [start + timedelta(seconds=n) for n in range(6)]

    record = log.record({"a": _source({"v": 1}), "b": _boom}, now=_clock(*ticks))["record"]

    assert record["read_started_at"] == ticks[0].isoformat()
    assert (record["sources"]["a"]["read_started_at"],
            record["sources"]["a"]["read_finished_at"]) == (ticks[1].isoformat(), ticks[2].isoformat())
    assert (record["sources"]["b"]["read_started_at"],
            record["sources"]["b"]["read_finished_at"]) == (ticks[3].isoformat(), ticks[4].isoformat())
    assert record["read_finished_at"] == ticks[5].isoformat()
    assert record["evidence_cutoff"] == WED_CLOSE.isoformat() < record["read_started_at"]


def test_a_source_clock_outside_the_receipts_read_window_fails(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    at = WED_CLOSE + timedelta(minutes=30)
    forged = {
        "kind": "receipt", "session": "2026-11-18", "evidence_cutoff": WED_CLOSE.isoformat(),
        "read_started_at": at.isoformat(), "read_finished_at": at.isoformat(),
        "sources": {"s": {"status": "unknown", "failure": "X", "reason": "y",
                          "read_started_at": WED_CLOSE.isoformat(),   # "known at the close"
                          "read_finished_at": at.isoformat()}},
        "promotion_allowed": False,
    }
    log._append(forged, at)

    check = log.verify()

    assert not check["ok"] and "read window" in check["detail"]


# ── handing a head to the witness ───────────────────────────────────────────


def _line_hashes(log):
    return [hashlib.sha256(line).hexdigest() for line in dsr.raw_lines(log.path)]


def test_a_receipts_head_is_pending_until_noted_and_noting_it_twice_writes_once(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    _record(log, {"s": _source({"v": 1}), "thesis_log": dsr.thesis_log_source()})
    head = _line_hashes(log)[1]

    (row,) = log.witness_rows()
    assert row == {"session": "2026-11-18", "head_sha256": head, "records": 2,
                   "evidence_cutoff": WED_CLOSE.isoformat(),
                   "read_finished_at": (WED_CLOSE + timedelta(minutes=30, seconds=2)).isoformat(),
                   "unknown_sources": ["thesis_log"], "locally_anchored": True,
                   "delivery": "pending", "delivered_at": None, "via": None}

    at = WED_CLOSE + timedelta(minutes=31)
    first = log.note_delivery(WED, head, now=lambda: at)
    second = log.note_delivery(WED, head, now=lambda: at + timedelta(hours=3))

    assert first["written"] is True and second["written"] is False
    assert second["note"] == first["note"]                  # the first delivery time stands
    (row,) = log.witness_rows()
    assert (row["delivery"], row["delivered_at"], row["via"]) == ("delivered", at.isoformat(), "hub")
    assert len(list(log.delivery_dir.iterdir())) == 1
    assert log.verify(full=True)["ok"]                      # notes are not part of the log


def test_a_delivery_is_for_one_exact_head(tmp_path):
    log = _two_sessions(tmp_path)
    wed_head, thu_head = _line_hashes(log)[1:]

    with pytest.raises(dsr.ReceiptError, match="another head"):
        log.note_delivery(WED, thu_head)
    with pytest.raises(dsr.ReceiptError, match="another head"):
        log.note_delivery(WED, wed_head[:-1] + ("0" if wed_head[-1] != "0" else "1"))
    with pytest.raises(dsr.ReceiptError, match="no receipt"):
        log.note_delivery(date(2026, 11, 20), wed_head)

    assert [r["delivery"] for r in log.witness_rows()] == ["pending", "pending"]


def test_an_earlier_sessions_delivery_can_still_be_retried_after_later_receipts(tmp_path):
    """Session and head, not "the log's head now": the pair does not move."""
    log = dsr.ReceiptLog(tmp_path)
    _record(log, {"s": _source({"v": 1})})
    before = log.witness_rows()[0]
    _record(log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))

    rows = log.witness_rows()

    assert rows[0] == before and rows[0]["records"] == 2 and rows[1]["records"] == 3
    assert log.note_delivery(WED, before["head_sha256"])["written"] is True
    assert [r["delivery"] for r in log.witness_rows()] == ["delivered", "pending"]


def test_a_head_this_host_has_not_anchored_is_not_handed_to_the_witness(tmp_path, monkeypatch):
    log = dsr.ReceiptLog(tmp_path)
    with monkeypatch.context() as stopped:
        _stop_before_the_anchor(log, stopped)
        with pytest.raises(_Stopped):
            _record(log, {"s": _source({"v": 1})})
    (row,) = log.witness_rows()

    assert row["locally_anchored"] is False
    with pytest.raises(dsr.ReceiptError, match="not anchored"):
        log.note_delivery(WED, row["head_sha256"])
    _record(log, {"s": _source({"v": 1})})                  # the retry finishes the anchor
    assert log.note_delivery(WED, row["head_sha256"])["written"] is True


def test_a_delivery_note_that_names_another_head_is_refused(tmp_path):
    log = dsr.ReceiptLog(tmp_path)
    _record(log, {"s": _source({"v": 1})})
    head = _line_hashes(log)[1]
    log.delivery_dir.mkdir()
    (log.delivery_dir / f"2026-11-18.{head}.json").write_bytes(
        dsr.canonical({"session": "2026-11-18", "head_sha256": "f" * 64,
                       "delivered_at": "x", "via": "hub"}))

    with pytest.raises(dsr.ReceiptError, match="does not say what its name says"):
        log.witness_rows()


def test_local_anchoring_and_off_host_witnessing_are_reported_separately(tmp_path):
    log = dsr.ReceiptLog(tmp_path / "log")
    vault = tmp_path / "vault"
    witness = vault / dsr.WITNESS_DIR / dsr.ANCHOR_NAME
    _record(log, {"s": _source({"v": 1})})
    log.note_delivery(WED, _line_hashes(log)[1])            # this host's own note

    # Anchored here, noted as handed over, and still not witnessed: no copy was checked.
    assert log.verify()["qualified"] == {"local_anchor": True, "external_witness": None}

    log.export_anchors(vault)
    assert log.verify(witness)["qualified"] == {"local_anchor": True, "external_witness": True}

    _record(log, {"s": _source({"v": 2})}, at=THU_CLOSE + timedelta(minutes=30))
    behind = log.verify(witness)
    assert behind["ok"] and behind["witnessed_records"] == 2
    assert behind["qualified"] == {"local_anchor": True, "external_witness": False}


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
            "CREATE TABLE discovered_hypotheses (id TEXT PRIMARY KEY, pattern_type TEXT, "
            "test_criteria TEXT, status TEXT, created_at TEXT, last_tested TEXT, times_tested INTEGER, "
            "times_correct INTEGER, kill_reason TEXT, killed_at TEXT, scoring_version TEXT, "
            "trial_p_value REAL, trial_q_value REAL, trial_ledger_sha256 TEXT)")
    return engine


_TERMS = {"ticker": "BAH", "direction": "up", "window_days": 14, "min_move_pct": 2.0}


def _add_lead(engine, lead_id, created_at, origin="intersection", declared=None, status="new"):
    hypotheses = json.dumps([{"id": declared, **_TERMS}] if declared else [])
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO investigation_leads (id, question, category, priority, evidence, status, "
            "follow_up_leads, hypotheses, created_at, origin, outcome, declared_hypothesis_id) "
            "VALUES (:id, 'q', 'connection_found', 0.6, '[]', :status, '[]', :hypotheses, "
            ":created, :origin, :outcome, :declared)"
        ), {"id": lead_id, "created": created_at, "origin": origin, "declared": declared,
            "status": "resolved" if declared else status, "hypotheses": hypotheses,
            "outcome": "hypothesis_declared" if declared else None})


def _add_reviewed_test(engine, lead_id):
    criteria = {"ticker": "BAH", "window_days": 14, "min_move_pct": 2.0,
                "expected_direction": "up", "source": "sleuth", "lead_id": lead_id}
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO discovered_hypotheses (id, pattern_type, test_criteria, status, "
            "times_tested) VALUES (:id, :pattern, :criteria, 'active', 0)"),
            {"id": si.sleuth_test_id(lead_id), "pattern": si.HYPOTHESIS_PATTERN,
             "criteria": json.dumps(criteria)})


def test_sleuth_source_reads_intersection_leads_up_to_the_cutoff_with_their_tests():
    engine = _lead_engine()
    in_scope = "LEAD-INTX-bbbbbbbbbbbb"
    staged = "LEAD-INTX-aaaaaaaaaaaa"
    _add_lead(engine, in_scope, WED_CLOSE - timedelta(days=3), declared=si.sleuth_test_id(in_scope))
    _add_lead(engine, staged, WED_CLOSE - timedelta(days=1))
    _add_lead(engine, "LEAD-INTX-after", WED_CLOSE + timedelta(minutes=1))
    _add_lead(engine, "LEAD-INTX-old", WED_CLOSE - timedelta(days=dsr.LEAD_LOOKBACK_DAYS + 1))
    _add_lead(engine, "LEAD-chat", WED_CLOSE - timedelta(days=1), origin="chat")
    _add_reviewed_test(engine, in_scope)
    _add_reviewed_test(engine, staged)   # committed, but its lead does not point at it yet
    source = dsr.sleuth_leads_source(engine)

    read = source(WED_CLOSE)

    leads = read.payload["leads"]
    assert [lead["id"] for lead in leads] == [staged, in_scope]   # sorted by id
    assert read.rows == 2
    assert [t["id"] for t in leads[1]["declared_tests"]] == [si.sleuth_test_id(in_scope)]
    assert leads[1]["unverified_tests"] == []
    # A staged row is on the record as unverified, not as a declared test.
    assert leads[0]["declared_tests"] == []
    assert [(t["id"], t["state"]) for t in leads[0]["unverified_tests"]] == [
        (si.sleuth_test_id(staged), si.TEST_ROW_STAGED)]
    assert read.scope["created_until"] == WED_CLOSE.isoformat()
    assert dsr.canonical(source(WED_CLOSE).payload) == dsr.canonical(read.payload)


def test_sleuth_source_reads_leads_and_tests_in_one_transaction():
    engine = _lead_engine()
    _add_lead(engine, "LEAD-INTX-aaaaaaaaaaaa", WED_CLOSE - timedelta(days=1))
    seen = []

    @event.listens_for(engine, "before_cursor_execute")
    def _record_select(conn, _cursor, statement, *_rest):
        if statement.lstrip().upper().startswith("SELECT"):
            seen.append((id(conn.get_transaction()), conn.in_transaction()))

    dsr.sleuth_leads_source(engine)(WED_CLOSE)

    assert len(seen) == 2 and len({transaction for transaction, _ in seen}) == 1
    assert all(in_transaction for _, in_transaction in seen)


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
    assert read.payload["summary"]["head_sha256"] == read.payload["head_sha256"]
    assert read.payload["ledger_file_sha256"] == hashlib.sha256(ledger.read_bytes()).hexdigest()
    assert read.payload["anchor_file_sha256"] == hashlib.sha256(anchor.read_bytes()).hexdigest()


def test_factory_payload_describes_the_version_read_even_if_the_files_change_meanwhile(
        tmp_path, monkeypatch):
    """The summary, the head and both file hashes are one version: a ledger
    that grows while it is being summarized cannot put two versions in one payload."""
    from analysis import ledger_steered_exploration as lse

    ledger, anchor = tmp_path / "f.jsonl", tmp_path / "f.anchors.jsonl"
    lse.Ledger.create_factory(ledger, ledger_id="factory-test", anchor=anchor)
    before = (ledger.read_bytes(), anchor.read_bytes())
    head_before = lse.Ledger(ledger, anchor=anchor).head
    real_summarize = lse.summarize

    def summarize_while_the_live_files_change(opened):
        ledger.write_bytes(before[0] + b'{"later":"record"}\n')
        anchor.write_bytes(before[1] + b'{"later":"anchor"}\n')
        return real_summarize(opened)

    monkeypatch.setattr(lse, "summarize", summarize_while_the_live_files_change)

    payload = dsr.factory_ledger_source(ledger, anchor)(WED_CLOSE).payload

    assert ledger.read_bytes() != before[0]              # the live file did move
    assert payload["ledger_file_sha256"] == hashlib.sha256(before[0]).hexdigest()
    assert payload["anchor_file_sha256"] == hashlib.sha256(before[1]).hexdigest()
    assert payload["head_sha256"] == payload["summary"]["head_sha256"] == head_before


def test_factory_files_that_never_hold_still_are_unknown_not_guessed(tmp_path, monkeypatch):
    from analysis import ledger_steered_exploration as lse

    ledger, anchor = tmp_path / "f.jsonl", tmp_path / "f.anchors.jsonl"
    lse.Ledger.create_factory(ledger, ledger_id="factory-test", anchor=anchor)
    calls = []
    real = dsr.Path.read_bytes

    def restless(self):
        calls.append(self.name)
        return real(self) + (b" " * len(calls) if self.name == ledger.name else b"")

    monkeypatch.setattr(dsr.Path, "read_bytes", restless)
    try:
        with pytest.raises(dsr.SourceUnavailable, match="changed during every read"):
            dsr.factory_ledger_source(ledger, anchor)(WED_CLOSE)
    finally:
        monkeypatch.undo()

    assert calls.count(ledger.name) == 2 * dsr.FACTORY_READ_ATTEMPTS


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


def test_witness_commands_list_pending_heads_and_note_one_delivery(tmp_path, capsys):
    log = _two_sessions(tmp_path)
    wed_head, thu_head = _line_hashes(log)[1:]
    capsys.readouterr()

    assert dsr.main(["witness-status", "--dir", str(tmp_path), "--pending", "--lines"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"2026-11-18\t{wed_head}\t2\t{WED_CLOSE.isoformat()}\t-",
        f"2026-11-19\t{thu_head}\t3\t{THU_CLOSE.isoformat()}\t-"]

    assert dsr.main(["witness-mark", "--dir", str(tmp_path), "--session", "2026-11-18",
                     "--head", wed_head]) == 0
    assert json.loads(capsys.readouterr().out)["written"] is True
    assert dsr.main(["witness-mark", "--dir", str(tmp_path), "--session", "2026-11-18",
                     "--head", thu_head]) == 2
    assert "another head" in json.loads(capsys.readouterr().out)["refused"]

    assert dsr.main(["witness-status", "--dir", str(tmp_path), "--pending", "--lines"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"2026-11-19\t{thu_head}\t3\t{THU_CLOSE.isoformat()}\t-"]
    assert dsr.main(["witness-status", "--dir", str(tmp_path)]) == 0
    assert [r["delivery"] for r in json.loads(capsys.readouterr().out)["receipts"]] == [
        "delivered", "pending"]
