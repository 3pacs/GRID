"""scripts/propose_ticker_status.py: evidence for a sidecar entry, never a status change.

2026-11-26: smart:tiingo went PARTIAL on FNGR, the sixth lapsed ticker in 16 days
(KPTI, ROKU, BKH, GWHT, UTZ, FNGR). Codex HOLD 6031219314: no vendor listing may
clear the PARTIAL on its own. The tool only prepares the person's decision, and
says "status not established" when the filings do not settle it.

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
from analysis.sector_map import _load_ticker_status
from ingestion.tiingo_pull import TiingoPuller
from scripts import propose_ticker_status as pts

D = date.fromisoformat
CLOSES = {"TIINGO": (D("2026-11-24"), 0.022), "yfinance": (D("2026-05-13"), 0.31)}


def _ev(ticker="XYZ", *, filings=(), cik="0000000042", tickers=("XYZ",), found=True, end=None,
        closes=CLOSES, name="Xyz Corp", cik_source="SEC ticker file"):
    return pts.Evidence(
        ticker=ticker, tiingo_found=found, tiingo_name=name, tiingo_end=end,
        last_tiingo_close=closes.get("TIINGO"), last_yfinance_close=closes.get("yfinance"),
        cik=cik, cik_source=cik_source, edgar_name=name, edgar_tickers=tuple(tickers),
        filings=[pts.Filing(*f) for f in filings],
    )


def _valid_in_the_sidecar(block: str, tmp_path: Path) -> dict:
    path = tmp_path / "status.yaml"
    path.write_text("TICKER_STATUS:\n" + block + "\n", encoding="utf-8")
    return _load_ticker_status(path)       # the real loader and its rules


# ── what the filings settle ──────────────────────────────────────────────────

def test_utz_shape_is_proposed_as_acquired_with_accessions_and_a_review_placeholder(tmp_path):
    ev = _ev("UTZ", end=D("2026-11-19"), name="Utz Brands", filings=[
        ("25-NSE", D("2026-11-20"), "0000000000-26-000025", ()),
        ("8-K", D("2026-11-19"), "0001193125-26-470261", ("2.01", "3.01", "5.01")),
    ])
    proposal = pts.classify(ev)
    assert proposal.status == "acquired" and proposal.last_session == D("2026-11-19")
    text = pts.render(proposal, ev)
    assert "0001193125-26-470261" in text and "0000000000-26-000025" in text
    assert "REVIEW: name the acquirer from 8-K 0001193125-26-470261" in text     # cannot be committed unread
    entry = _valid_in_the_sidecar(text, tmp_path)["UTZ"]
    assert entry["status"] == "acquired" and str(entry["last_session"]) == "2026-11-19"


@pytest.mark.parametrize("form", ["25", "25-NSE", "15-12B", "15-12G", "15"])
def test_a_delisting_form_without_an_acquisition_is_delisted(form, tmp_path):
    ev = _ev("DEL", filings=[(form, D("2026-11-18"), "0000000001-26-000001", ())])
    proposal = pts.classify(ev)
    assert proposal.status == "delisted"
    assert _valid_in_the_sidecar(pts.render(proposal, ev), tmp_path)["DEL"]["status"] == "delisted"


def test_a_symbol_the_issuer_no_longer_uses_is_proposed_as_renamed(tmp_path):
    ev = _ev("ATUS", tickers=("OPTU",), name="Optimum Communications", cik_source="--cik")
    proposal = pts.classify(ev)
    assert (proposal.status, proposal.successor) == ("renamed", "OPTU")
    entry = _valid_in_the_sidecar(pts.render(proposal, ev), tmp_path)["ATUS"]
    assert entry["successor"] == "OPTU"


# ── what they do not settle: "status not established" ────────────────────────

def test_the_fngr_case_no_cik_is_not_established_and_says_what_would_settle_it():
    ev = _ev("FNGR", cik=None, tickers=(), filings=[], end=None, closes={"TIINGO": (D("2026-11-24"), 0.022), "yfinance": None})
    proposal = pts.classify(ev)
    text = pts.render(proposal, ev)
    assert proposal.status is None
    assert "STATUS NOT ESTABLISHED (no entry proposed)" in text
    assert "no CIK for this symbol in the SEC ticker file" in text and "--cik TICKER=CIK" in text
    assert "TIINGO 2026-11-24 at 0.022" in text and "yfinance none" in text
    assert "status:" not in text.replace("STATUS NOT", "")             # no entry is printed


def test_a_live_issuer_with_only_a_tiingo_gap_is_not_established():
    ev = _ev("XYZ", filings=[("10-Q", D("2026-11-05"), "0000000002-26-000002", ())])
    proposal = pts.classify(ev)
    assert proposal.status is None
    assert "still lists XYZ for this issuer: consistent with a Tiingo-only gap" in pts.render(proposal, ev)


@pytest.mark.parametrize("name, filings", [
    ("delisting notice only", [("8-K", D("2026-11-18"), "0000000003-26-000003", ("3.01",))]),
    ("bankruptcy only", [("8-K", D("2026-11-18"), "0000000004-26-000004", ("1.03",))]),
    ("acquisition item without a delisting form", [("8-K", D("2026-11-18"), "0000000005-26-000005", ("2.01",))]),
    ("a Form 25 two years earlier", [("25", D("2024-11-18"), "0000000006-24-000006", ())]),
])
def test_partial_or_old_evidence_does_not_establish_a_status(name, filings):
    assert pts.classify(_ev("XYZ", filings=filings)).status is None, name


def test_prices_and_a_missing_yfinance_row_never_establish_anything():
    for closes in ({"TIINGO": (D("2026-11-24"), 0.001), "yfinance": None},
                   {"TIINGO": None, "yfinance": None}):
        assert pts.classify(_ev("XYZ", closes=closes)).status is None


# ── it fails loudly instead of printing an entry with missing evidence ───────

def _deps(*, tiingo=(200, {"name": "Xyz", "endDate": None}), submissions=(200, None), closes=None):
    calls = []

    def get(url, headers):
        calls.append(url)
        if "tiingo.com" in url:
            if isinstance(tiingo, Exception):
                raise tiingo
            return tiingo
        if isinstance(submissions, Exception):
            raise submissions
        return submissions[0], submissions[1] or {"name": "Xyz", "tickers": ["XYZ"],
                                                  "filings": {"recent": {"form": [], "filingDate": [],
                                                                         "accessionNumber": [], "items": []}}}

    return dict(get=get, tiingo_headers={}, sec_headers={}, cik_map={"XYZ": "42", "BAD": "43"},
                cik_overrides={}, last_closes=closes or (lambda t: dict(CLOSES))), calls


def test_every_unavailable_source_gives_no_entry_a_message_and_exit_2():
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
        assert any("EVIDENCE UNAVAILABLE, no entry proposed" in e for e in err), kind
        assert not any(line.lstrip().startswith("XYZ:") or "NOT ESTABLISHED" in line for line in out), kind
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

def test_the_tool_writes_nothing(monkeypatch):
    sidecar = Path(sm.__file__).parent / "sector_map_ticker_status.yaml"
    before = hashlib.sha256(sidecar.read_bytes()).hexdigest()
    status_before = dict(sm.TICKER_STATUS)
    real_open = builtins.open

    def guarded(file, mode="r", *a, **k):
        assert not any(c in mode for c in "wax+"), f"write open of {file!r}"
        return real_open(file, mode, *a, **k)

    monkeypatch.setattr(builtins, "open", guarded)
    deps, _ = _deps()
    pts.run(["XYZ"], out=lambda s: None, err=lambda s: None, **deps)
    assert hashlib.sha256(sidecar.read_bytes()).hexdigest() == before
    assert dict(sm.TICKER_STATUS) == status_before
    assert not [n for n in dir(pts) if n in {"write_sidecar", "apply", "save"}]


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
    """The tool proposes nothing for a live issuer and has no channel to clear a status."""
    latest = {"SPY": D("2026-11-19"), "XYZ": D("2026-11-13")}

    def pull():
        return _Puller(latest, {"SPY": _OK, "XYZ": _EMPTY}, {"XYZ": (True, None)}).pull_incremental(
            list(latest), now=_NOW)

    before = pull()
    assert before["status"] == "PARTIAL" and before["lapsed_tickers"] == ["XYZ"]

    deps, _ = _deps()                 # EDGAR: XYZ still listed, only a 10-Q; Tiingo: no endDate
    out: list[str] = []
    assert pts.run(["XYZ"], out=out.append, err=lambda s: None, **deps) == 0
    assert any("STATUS NOT ESTABLISHED" in line for line in out)
    assert "XYZ" not in sm.TICKER_STATUS

    after = pull()
    assert after["status"] == "PARTIAL" and after["lapsed_tickers"] == ["XYZ"]
