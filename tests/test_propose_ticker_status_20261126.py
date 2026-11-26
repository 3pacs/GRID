"""scripts/propose_ticker_status.py: evidence for a person to read, never an entry or a status change.

2026-11-26: smart:tiingo went PARTIAL on FNGR, the sixth lapsed ticker in 16 days
(KPTI, ROKU, BKH, GWHT, UTZ, FNGR). Codex HOLD 6031219314: no vendor listing may
clear the PARTIAL on its own. Codex HOLD 6080189252: filing metadata is a lead,
not an issuer or security outcome, so the tool prints candidate evidence as
comments and no sidecar entry; a malformed EDGAR answer is unavailable, not "none".

Offline: scripted HTTP, SEC ticker file and stored closes; nothing is read or written.
"""
from __future__ import annotations

import builtins
import hashlib
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import analysis.sector_map as sm
from ingestion.tiingo_pull import TiingoPuller
from scripts import propose_ticker_status as pts

D = date.fromisoformat
CLOSES = {"TIINGO": (D("2026-11-19"), 0.022), "yfinance": (D("2026-05-13"), 0.31)}


def _ev(ticker="XYZ", *, filings=(), cik="0000000042", tickers=("XYZ",), found=True, end=None,
        closes=CLOSES, name="Xyz Corp", cik_source="SEC ticker file"):
    return pts.Evidence(
        ticker=ticker, tiingo_found=found, tiingo_name=name, tiingo_end=end,
        last_tiingo_close=closes.get("TIINGO"), last_yfinance_close=closes.get("yfinance"),
        cik=cik, cik_source=cik_source, edgar_name=name, edgar_tickers=tuple(tickers),
        filings=[pts.Filing(*f) for f in filings],
    )


def _text(ticker, ev):
    return pts.render(pts.triage(ev), ev)


# ── leads, not findings ──────────────────────────────────────────────────────

def test_utz_shape_is_a_lead_with_accessions_and_never_an_entry():
    ev = _ev("UTZ", end=D("2026-11-19"), name="Utz Brands", closes={"TIINGO": (D("2026-11-19"), 17.3), "yfinance": None},
             filings=[("25-NSE", D("2026-11-20"), "0000876661-26-000942", ()),
                      ("8-K", D("2026-11-20"), "0001193125-26-470261", ("2.01", "3.01", "5.01"))])
    t = pts.triage(ev)
    assert t.lead and len(t.filings) == 2
    text = pts.render(t, ev)
    assert "UTZ: LEAD (triage, not a finding; no entry proposed)" in text
    assert "0000876661-26-000942" in text and "0001193125-26-470261" in text
    assert "not established by this tool: that the filing(s) concern this issuer's listed security" in text
    assert "write the sidecar entry yourself" in text
    # comments only: no line could be pasted into the sidecar as it stands
    assert all(line.startswith("#") for line in text.splitlines())
    assert "status:" not in text and "acquirer:" not in text and "successor:" not in text and "last_session" not in text
    assert "endDate 2026-11-19 (a vendor claim)" in text                 # never presented as the last session


@pytest.mark.parametrize("name, filing", [
    ("an Item 2.01 alone (also covers the filer's own acquisitions and dispositions)",
     ("8-K", D("2026-11-18"), "0000000005-26-000005", ("2.01",))),
    ("a Form 15 alone (does not show the listing stopped trading)",
     ("15-12B", D("2026-11-18"), "0000000006-26-000006", ())),
    ("a delisting notice", ("8-K", D("2026-11-18"), "0000000003-26-000003", ("3.01",))),
    ("a bankruptcy", ("8-K", D("2026-11-18"), "0000000004-26-000004", ("1.03",))),
])
def test_a_single_filing_is_only_ever_a_candidate_and_says_what_is_missing(name, filing):
    ev = _ev("XYZ", filings=[filing])
    text = _text("XYZ", ev)
    assert "LEAD (triage" in text and "candidate:" in text and filing[2] in text, name
    assert "acquired" not in text.split("candidate:")[0] and "STATUS NOT ESTABLISHED" not in text


@pytest.mark.parametrize("name, anchor, filed", [
    ("a stale vendor date: 2020 anchor, Nov 2026 filings", D("2020-03-02"), D("2026-11-20")),
    ("filed long before the last close", D("2026-11-19"), D("2026-09-01")),
    ("filed more than 30 days after the last close", D("2026-11-19"), D("2026-12-20")),
    ("the same Form 25 two years earlier", D("2026-11-19"), D("2024-11-20")),
])
def test_the_filing_window_is_bounded_on_both_sides_of_the_last_stored_close(name, anchor, filed):
    ev = _ev("XYZ", closes={"TIINGO": (anchor, 1.0), "yfinance": None},
             filings=[("25", filed, "0000000007-26-000007", ())])
    t = pts.triage(ev)
    assert not t.lead and t.window == (anchor - pts.timedelta(days=14), anchor + pts.timedelta(days=30)), name


def test_the_edges_of_the_window_are_inside_it():
    anchor = D("2026-11-19")
    inside = [("25", anchor - pts.timedelta(days=14), "0000000008-26-000008", ()),
              ("25", anchor + pts.timedelta(days=30), "0000000009-26-000009", ())]
    assert len(pts.triage(_ev("XYZ", filings=inside)).filings) == 2


def test_current_symbols_are_a_lead_not_a_successor_and_no_entry_is_emitted():
    """Codex 6080189252 item 2: a CIK's current tickers do not establish old-to-new continuity."""
    ev = _ev("ATUS", tickers=("OPTU", "OPTUW"), name="Optimum Communications", cik_source="--cik")
    t = pts.triage(ev)
    assert t.lead and not t.filings
    text = pts.render(t, ev)
    assert "EDGAR now lists OPTU, OPTUW for CIK 0000000042, not ATUS: a current-symbols list, not an old-to-new listing record" in text
    assert "successor" not in text.replace("a successor symbol", "") and all(line.startswith("#") for line in text.splitlines())


# ── what they do not settle: "status not established" ────────────────────────

def test_the_fngr_case_no_cik_is_not_established_and_says_what_would_settle_it():
    ev = _ev("FNGR", cik=None, tickers=(), found=True, end=None,
             closes={"TIINGO": (D("2026-11-24"), 0.022), "yfinance": None})
    text = _text("FNGR", ev)
    assert "STATUS NOT ESTABLISHED (no entry proposed)" in text
    assert "no CIK for this symbol in the SEC ticker file" in text and "--cik TICKER=CIK" in text
    assert "TIINGO 2026-11-24 at 0.022" in text and "yfinance none" in text


def test_a_live_issuer_with_only_a_tiingo_gap_is_not_established():
    ev = _ev("XYZ", filings=[("10-Q", D("2026-11-05"), "0000000002-26-000002", ())])
    assert not pts.triage(ev).lead
    assert "still lists XYZ for this issuer: consistent with a Tiingo-only gap" in _text("XYZ", ev)


def test_without_a_stored_close_there_is_no_window_and_no_lead():
    ev = _ev("XYZ", closes={"TIINGO": None, "yfinance": None},
             filings=[("25", D("2026-11-20"), "0000000010-26-000010", ())])
    t = pts.triage(ev)
    assert not t.lead and t.window is None
    assert "no stored TIINGO close to anchor a filing window" in pts.render(t, ev)


def test_prices_and_a_missing_yfinance_row_never_establish_anything():
    for closes in ({"TIINGO": (D("2026-11-24"), 0.001), "yfinance": None}, {"TIINGO": None, "yfinance": None}):
        assert not pts.triage(_ev("XYZ", closes=closes)).lead


# ── EDGAR answers: only a well-formed answer counts, empty or not ────────────

def _doc(rows=(), *, cik=42, name="Xyz", tickers=("XYZ",), files=()):
    keys = ("form", "filingDate", "accessionNumber", "items")
    recent = {k: [r[i] for r in rows] for i, k in enumerate(keys)}
    return {"cik": cik, "name": name, "tickers": list(tickers),
            "filings": {"recent": recent, "files": list(files)}}


def _deps(*, tiingo=(200, {"name": "Xyz", "endDate": None}), submissions=None, closes=None):
    calls = []
    submissions = (200, _doc()) if submissions is None else submissions

    def get(url, headers):
        calls.append(url)
        if "tiingo.com" in url:
            if isinstance(tiingo, Exception):
                raise tiingo
            return tiingo
        if isinstance(submissions, Exception):
            raise submissions
        return submissions

    return dict(get=get, tiingo_headers={}, sec_headers={}, cik_map={"XYZ": "42", "BAD": "43"},
                cik_overrides={}, last_closes=closes or (lambda t: dict(CLOSES))), calls


@pytest.mark.parametrize("name, doc", [
    ("200 with name and tickers but no filings", {"cik": 42, "name": "Xyz", "tickers": ["ABC"]}),
    ("filings without recent", {"cik": 42, "name": "Xyz", "tickers": ["XYZ"], "filings": {}}),
    ("a missing items array", {"cik": 42, "name": "Xyz", "tickers": ["XYZ"], "filings": {"recent": {
        "form": ["25"], "filingDate": ["2026-11-20"], "accessionNumber": ["a"]}}}),
    ("arrays of different length", {"cik": 42, "name": "Xyz", "tickers": ["XYZ"], "filings": {"recent": {
        "form": ["25", "8-K"], "filingDate": ["2026-11-20"], "accessionNumber": ["a"], "items": [""]}}}),
    ("a bad filing date", _doc([("25", "not-a-date", "a", "")])),
    ("a non-string form", _doc([(25, "2026-11-20", "a", "")])),
    ("the answer is for another CIK", _doc(cik=99)),
    ("no tickers list", {"cik": 42, "name": "Xyz", "filings": _doc()["filings"]}),
    ("older pages exist and recent starts after the window", _doc(
        [("10-Q", "2026-11-10", "a", "")], files=[{"name": "CIK0000000042-submissions-001.json"}])),
])
def test_an_unexpected_edgar_shape_is_unavailable_never_an_empty_answer(name, doc):
    out, err = [], []
    code = pts.run(["XYZ"], out=out.append, err=err.append, **_deps(submissions=(200, doc))[0])
    assert code == 2, name
    assert any("EVIDENCE UNAVAILABLE, nothing reported" in e for e in err), name
    assert not any("XYZ:" in line for line in out), name


def test_a_valid_empty_answer_is_the_only_empty_that_counts():
    out: list[str] = []
    code = pts.run(["XYZ"], out=out.append, err=lambda s: None, **_deps(submissions=(200, _doc()))[0])
    assert code == 0 and any("XYZ: STATUS NOT ESTABLISHED" in line for line in out)
    # older pages exist but the recent array already reaches back past the window: still well-formed
    doc = _doc([("10-Q", "2026-10-01", "a", "")], files=[{"name": "older.json"}])
    assert pts.run(["XYZ"], out=out.append, err=lambda s: None, **_deps(submissions=(200, doc))[0]) == 0


# ── it fails loudly instead of printing a block with missing evidence ────────

def test_every_unavailable_source_gives_no_block_a_message_and_exit_2():
    out, err = [], []
    for kind, deps in {
        "tiingo down": _deps(tiingo=pts.EvidenceUnavailable("Tiingo listing for XYZ: ConnectionError"))[0],
        "tiingo HTTP 500": _deps(tiingo=(500, None))[0],
        "edgar HTTP 500": _deps(submissions=(500, None))[0],
        "edgar down": _deps(submissions=pts.EvidenceUnavailable("EDGAR: Timeout"))[0],
        "database down": _deps(closes=MagicMock(side_effect=pts.EvidenceUnavailable("stored closes for XYZ: OperationalError")))[0],
    }.items():
        out.clear()
        err.clear()
        code = pts.run(["XYZ"], out=out.append, err=err.append, **deps)
        assert code == 2, kind
        assert any("EVIDENCE UNAVAILABLE, nothing reported" in e for e in err), kind
        assert not any("XYZ:" in line for line in out), kind
        assert out[-1].endswith("1 unavailable"), kind


def test_a_healthy_ticker_is_still_reported_when_another_one_is_unavailable():
    deps, _ = _deps()
    real_get = deps["get"]
    deps["get"] = lambda url, headers: (500, None) if "CIK0000000043" in url else real_get(url, headers)
    out, err = [], []
    assert pts.run(["BAD", "XYZ"], out=out.append, err=err.append, **deps) == 2
    assert any("# XYZ: STATUS NOT ESTABLISHED" in line for line in out)
    assert any(e.startswith("# BAD: EVIDENCE UNAVAILABLE") for e in err)


def test_the_database_reader_is_read_only_and_reports_a_failure_as_unavailable():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("TIINGO", D("2026-11-24"), 0.022), ("yfinance", D("2026-05-13"), 0.31)]
    engine = MagicMock()
    engine.connect.return_value = conn
    got = pts.db_last_closes(engine)("FNGR")
    assert str(conn.execute.call_args_list[0].args[0]) == "SET TRANSACTION READ ONLY"
    assert "DISTINCT ON (sc.name)" in str(conn.execute.call_args_list[1].args[0])      # newest row of EACH source
    assert got == {"TIINGO": (D("2026-11-24"), 0.022), "yfinance": (D("2026-05-13"), 0.31)}
    engine.connect.side_effect = RuntimeError("db down")
    with pytest.raises(pts.EvidenceUnavailable):
        pts.db_last_closes(engine)("FNGR")


def test_pull_log_names_are_parsed_and_an_empty_log_is_unavailable():
    msg = ("3 ticker(s) with a close in the 10 days before 2026-11-20 got none for it: "
           "BKH (no dates in Tiingo's listing), GWHT (not found in Tiingo's listing), KPTI (listing ends 2026-11-10)")
    assert pts.names_from_pull_error(msg) == ["BKH", "GWHT", "KPTI"]
    assert pts.names_from_pull_error(
        "1 ticker(s) with a close in the 10 days before 2026-11-25 got none for it: FNGR (no dates in Tiingo's listing)"
    ) == ["FNGR"]
    assert pts.names_from_pull_error("got none for it: A (x), B") == ["A", "B"]
    engine = MagicMock()
    engine.connect.return_value.execute.return_value.fetchone.return_value = None
    with pytest.raises(pts.EvidenceUnavailable):
        pts.latest_pull_names(engine)


# ── it prepares a decision and cannot make one ───────────────────────────────

def test_the_tool_writes_nothing_and_has_no_way_to_(monkeypatch):
    sidecar = Path(sm.__file__).parent / "sector_map_ticker_status.yaml"
    before = hashlib.sha256(sidecar.read_bytes()).hexdigest()
    status_before = dict(sm.TICKER_STATUS)
    real_open = builtins.open

    def guarded(file, mode="r", *a, **k):
        assert not any(c in mode for c in "wax+"), f"write open of {file!r}"
        return real_open(file, mode, *a, **k)

    monkeypatch.setattr(builtins, "open", guarded)
    out: list[str] = []
    pts.run(["XYZ"], out=out.append, err=lambda s: None, **_deps()[0])
    assert hashlib.sha256(sidecar.read_bytes()).hexdigest() == before
    assert dict(sm.TICKER_STATUS) == status_before
    assert all(line.startswith("#") for line in out)            # not one line is a sidecar entry
    assert not [n for n in dir(pts) if n in {"write_sidecar", "apply", "save", "render_entry"}]


_NOW = datetime(2026, 11, 21, 1, 0, tzinfo=timezone.utc)
_OK = {"status": "SUCCESS", "latest_obs": D("2026-11-20"), "rows_inserted": 6}
_EMPTY = {"status": "PARTIAL", "errors": ["No data returned"]}


class _Puller(TiingoPuller):
    """pull_incremental with the lookups, the lock, the fetch and the listing scripted."""

    def __init__(self, latest, fetch, listing) -> None:
        self.engine = MagicMock()
        self.source_id = 524
        self.latest, self.fetch, self.listing = latest, fetch, listing

    def _latest_tiingo_obs(self, ticker):
        return self.latest.get(ticker)

    def _try_acquire_advisory_lock(self):
        return object()

    def _release_advisory_lock(self, conn):
        pass

    def pull_ticker(self, ticker, start_date="2020-01-01", end_date=None, limiter=None):
        return {"ticker": ticker, "rows_inserted": 0, "errors": [], **self.fetch[ticker]}

    def _listing(self, ticker, limiter=None):
        return self.listing[ticker]


def test_a_tiingo_only_lapse_on_a_live_ticker_stays_partial_before_and_after_the_tool():
    """The tool reports no lead for a live issuer and has no channel to clear a status."""
    latest = {"SPY": D("2026-11-19"), "XYZ": D("2026-11-13")}

    def pull():
        return _Puller(latest, {"SPY": _OK, "XYZ": _EMPTY}, {"XYZ": (True, None)}).pull_incremental(
            list(latest), now=_NOW)

    before = pull()
    assert before["status"] == "PARTIAL" and before["lapsed_tickers"] == ["XYZ"]

    out: list[str] = []
    assert pts.run(["XYZ"], out=out.append, err=lambda s: None, **_deps()[0]) == 0     # EDGAR: still listed, no filings
    assert any("STATUS NOT ESTABLISHED" in line for line in out)
    assert "XYZ" not in sm.TICKER_STATUS

    after = pull()
    assert after["status"] == "PARTIAL" and after["lapsed_tickers"] == ["XYZ"]
