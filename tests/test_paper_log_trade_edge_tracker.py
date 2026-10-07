"""trade_edge v2: EDGAR parse, filing records, exits, and end-to-end runs (no network, no DB)."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from paper_log.trade_edge import source, tracker
from paper_log.trade_edge.config import EASTERN, PREREG_PATH, PREREG_SHA256
from paper_log.trade_edge.sec import FetchDeferred, SecReader, SubmissionError, parse_submission, submission_url

REPO = Path(__file__).resolve().parents[1]


def et(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=EASTERN)


def submission(accession="0001-26-000001", accepted="20261007075746", stype="4", ticker="BORR",
               issuer_cik="0001715497", owners=(("0001709630", "Troim Tor Olav"),), lines=None, doc_10b5_1="0"):
    lines = lines if lines is not None else [("2026-10-06", "P", "A", "1500000", "4.2428", "")]
    owner_xml = "".join(
        f"<reportingOwner><reportingOwnerId><rptOwnerCik>{c}</rptOwnerCik><rptOwnerName>{n}</rptOwnerName>"
        f"</reportingOwnerId></reportingOwner>" for c, n in owners)
    line_xml = "".join(
        f"<nonDerivativeTransaction><securityTitle><value>Common Shares</value></securityTitle>"
        f"<transactionDate><value>{d}</value></transactionDate>"
        f"<transactionCoding><transactionCode>{code}</transactionCode>{extra}</transactionCoding>"
        f"<transactionAmounts><transactionShares><value>{sh}</value></transactionShares>"
        f"<transactionPricePerShare><value>{px}</value></transactionPricePerShare>"
        f"<transactionAcquiredDisposedCode><value>{ad}</value></transactionAcquiredDisposedCode>"
        f"</transactionAmounts></nonDerivativeTransaction>" for d, code, ad, sh, px, extra in lines)
    accept = f"<ACCEPTANCE-DATETIME>{accepted}\n" if accepted else ""
    return (
        f"<SEC-DOCUMENT>{accession}.txt : 20261007\n<SEC-HEADER>{accession}.hdr.sgml : 20261007\n{accept}"
        f"ACCESSION NUMBER:\t\t{accession}\nCONFORMED SUBMISSION TYPE:\t{stype}\nFILED AS OF DATE:\t\t20261007\n"
        f"</SEC-HEADER>\n<DOCUMENT>\n<TYPE>{stype}\n<TEXT>\n<XML>\n<?xml version=\"1.0\"?>\n<ownershipDocument>"
        f"<schemaVersion>X0508</schemaVersion><documentType>{stype}</documentType><aff10b5One>{doc_10b5_1}</aff10b5One>"
        f"<issuer><issuerCik>{issuer_cik}</issuerCik><issuerName>Borr Drilling Ltd</issuerName>"
        f"<issuerTradingSymbol>{ticker}</issuerTradingSymbol></issuer>{owner_xml}"
        f"<nonDerivativeTable>{line_xml}</nonDerivativeTable></ownershipDocument>\n</XML>\n</TEXT>\n</DOCUMENT>\n"
    )


# ── EDGAR parse ─────────────────────────────────────────────────────────────


def test_parse_submission_fields():
    sub = parse_submission(submission(
        owners=(("0001709630", "Troim Tor Olav"), ("0000000042", "Magni Partners")),
        lines=[("2026-10-06", "P", "A", "1500000", "4.2428", ""),
               ("2026-10-06", "S", "D", "100", "4.5", ""),
               ("2026-10-06", "P", "A", "1000", "4.30", "<equitySwapInvolved>1</equitySwapInvolved>")]))
    assert sub["acceptance_at"] == et(2026, 10, 7, 7, 57) + timedelta(seconds=46)
    assert sub["submission_type"] == "4"
    assert sub["filing_date"] == date(2026, 10, 7)
    assert sub["issuer_cik"] == 1715497 and sub["ticker"] == "BORR"
    assert [o["cik"] for o in sub["owners"]] == [1709630, 42]
    assert len(sub["lines"]) == 2  # code S dropped
    assert sub["lines"][0]["shares"] == 1_500_000 and sub["lines"][0]["acq_disp"] == "A"
    assert sub["lines"][1]["equity_swap"] is True
    assert not any(line["is_10b5_1"] for line in sub["lines"])


def test_parse_submission_10b5_1_and_errors():
    sub = parse_submission(submission(doc_10b5_1="1"))
    assert sub["lines"][0]["is_10b5_1"] is True
    with pytest.raises(SubmissionError):
        parse_submission("<SEC-HEADER>nothing here</SEC-HEADER>")


def test_submission_url_and_reader_retry_and_cap():
    url = submission_url("https://www.sec.gov/Archives/edgar/data/0001709630/000162828026065290/x.xml",
                         "0001628280-26-065290")
    assert url == "https://www.sec.gov/Archives/edgar/data/1709630/000162828026065290/0001628280-26-065290.txt"
    calls = []

    def flaky(u, ua):
        calls.append(u)
        if len(calls) == 1:
            raise OSError("reset")
        return submission()

    reader = SecReader("test-agent", get=flaky, sleep=lambda s: None)
    assert reader.read("https://www.sec.gov/Archives/edgar/data/1/000162828026065290/x.xml", "a-1")["ticker"] == "BORR"
    assert len(calls) == 2
    with pytest.raises(SubmissionError):
        SecReader("ua", get=lambda u, ua: (_ for _ in ()).throw(OSError("down")), sleep=lambda s: None).read(
            "https://www.sec.gov/Archives/edgar/data/1/000162828026065290/x.xml", "a-1")
    capped = SecReader("ua", get=lambda u, ua: submission(), sleep=lambda s: None, max_fetches=0)
    with pytest.raises(FetchDeferred):
        capped.read("https://www.sec.gov/Archives/edgar/data/1/000162828026065290/x.xml", "a-1")
    with pytest.raises(SubmissionError):
        SecReader("").read("https://www.sec.gov/Archives/edgar/data/1/000162828026065290/x.xml", "a-1")


# ── filing records ──────────────────────────────────────────────────────────


def info(acc="0001628280-26-065290", ingest=None, ticker="BORR", filing_date="2026-10-07", lines=None):
    return {"accession": acc, "first_ingest_at": ingest or et(2026, 10, 7, 16, 5),
            "filing_date": filing_date, "filing_url": "https://www.sec.gov/Archives/edgar/data/1709630/000162828026065290/x.xml",
            "ticker": ticker,
            "db_lines": lines or [{"insider_name": "Troim Tor Olav", "trans_date": "2026-10-06", "code": "P",
                                   "acq_disp": "A", "shares": 1_500_000.0, "price": 4.2428, "is_derivative": False,
                                   "equity_swap": False, "is_10b5_1": False, "security_title": ""}]}


def test_filing_record_from_sec_uses_acceptance_and_ingest():
    rec = tracker.make_filing_record(info(), parse_submission(submission()), et(2026, 10, 7, 18), None)
    assert rec["source"] == "sec" and rec["actor"] == "cik:1709630"
    assert rec["acceptance_at"].startswith("2026-10-07T07:57:46")
    # ingest 16:05 + 15 min margin is later than the 07:57 acceptance -> entry next session
    assert datetime.fromisoformat(rec["known_at"]) == et(2026, 10, 7, 16, 20)
    assert rec["entry_session"] == "2026-10-08"
    assert rec["lines"][0]["exclusion"] is None


def test_filing_record_grid_db_fallback():
    rec = tracker.make_filing_record(info(ingest=et(2026, 10, 7, 9, 0)), None, et(2026, 10, 7, 18), "down")
    assert rec["source"] == "grid_db" and rec["sec_error"] == "down"
    assert rec["actor"] == "name:troim_tor_olav" and rec["issuer_cik"] is None
    assert datetime.fromisoformat(rec["known_at"]) == et(2026, 10, 7, 22, 0)  # 22:00 ET rule
    assert rec["entry_session"] == "2026-10-08"


def test_filing_record_4a_and_late_lines_are_excluded():
    rec = tracker.make_filing_record(info(), parse_submission(submission(stype="4/A")), et(2026, 10, 7, 18), None)
    assert rec["lines"][0]["exclusion"] == "not_form_4"
    late = parse_submission(submission(lines=[("2025-01-02", "P", "A", "1000", "50", "")]))
    rec2 = tracker.make_filing_record(info(), late, et(2026, 10, 7, 18), None)
    assert rec2["lines"][0]["exclusion"] == "late_filing"


# ── exits ───────────────────────────────────────────────────────────────────


def entry(bucket=">=2B", session="2026-10-08", unadj=10.0):
    return {"position_id": f"cik:1|{session}", "entry_session": session, "cap_bucket": bucket,
            "entry_close_unadjusted": unadj}


def sessions_from(start: date, n: int):
    from paper_log.trade_edge.events import shift_sessions
    return [shift_sessions(start, i) for i in range(n + 1)]


def test_exit_closed_net_of_cost():
    days = sessions_from(date(2026, 10, 8), 5)
    adj = {d: 10.0 + i for i, d in enumerate(days)}  # 10 -> 15 (+50%)
    spy = {d: 100.0 + i for i, d in enumerate(days)}  # 100 -> 105 (+5%)
    x = tracker.compute_exit(entry(), 5, adj, spy, [], days[-1], et(2026, 10, 16, 8, 30))
    assert x["status"] == "closed" and x["exit_session"] == days[-1].isoformat()
    assert x["return"] == pytest.approx(0.5) and x["spy_return"] == pytest.approx(0.05)
    assert x["gross_excess"] == pytest.approx(0.45)
    assert x["net_excess"] == pytest.approx(0.45 - 0.001)
    micro = tracker.compute_exit(entry("<300M"), 5, adj, spy, [], days[-1], et(2026, 10, 16, 8, 30))
    assert micro["net_excess"] == pytest.approx(0.45 - 0.01)


def test_exit_waits_for_benchmark_and_grace_then_closes_delisted():
    days = sessions_from(date(2026, 10, 8), 12)
    spy = {d: 100.0 for d in days}
    adj = {days[0]: 10.0, days[1]: 8.0, days[2]: 6.0}  # stops trading after 2 sessions
    x5 = days[5]
    assert tracker.compute_exit(entry(), 5, adj, {}, [], days[12], et(2026, 10, 30)) is None
    assert tracker.compute_exit(entry(), 5, adj, spy, [], days[9], et(2026, 10, 30)) is None  # inside grace
    x = tracker.compute_exit(entry(), 5, adj, spy, [], days[10], et(2026, 10, 30))
    assert x["status"] == "closed_delisted" and x["end_session"] == days[2].isoformat()
    assert x["exit_session"] == x5.isoformat()
    assert x["return"] == pytest.approx(-0.4) and x["price_basis"] == "adjusted_last_available"


def test_exit_delisted_falls_back_to_marks_then_entry_close():
    days = sessions_from(date(2026, 10, 8), 12)
    spy = {d: 100.0 for d in days}
    marks = [(days[1], 9.0), (days[3], 5.0)]
    x = tracker.compute_exit(entry(unadj=10.0), 5, {}, spy, marks, days[12], et(2026, 10, 30))
    assert x["price_basis"] == "unadjusted_marks" and x["return"] == pytest.approx(-0.5)
    y = tracker.compute_exit(entry(unadj=10.0), 5, {}, spy, [], days[12], et(2026, 10, 30))
    assert y["price_basis"] == "entry_close_only" and y["return"] == 0.0


# ── end to end ──────────────────────────────────────────────────────────────


class FakeSec:
    def __init__(self, texts: dict[str, str]):
        self.texts, self.fetches = texts, 0

    def read(self, filing_url, accession):
        self.fetches += 1
        if accession not in self.texts:
            raise SubmissionError("not found")
        return parse_submission(self.texts[accession])


class FakePrices:
    def __init__(self, adj: dict, raw: dict, caps: dict | None = None):
        self.adj, self.raw, self.caps, self.failures = adj, raw, caps or {}, []

    def closes(self, symbols, start, end, *, adjusted):
        src = self.adj if adjusted else self.raw
        return {s: {d: v for d, v in src.get(s, {}).items() if start <= d <= end} for s in symbols}

    def market_cap(self, symbol):
        return self.caps.get(symbol)


@pytest.fixture
def fake_db(monkeypatch):
    state = {"rows": [], "caps": {}}

    def rows(conn, since):
        return [r for r in state["rows"] if r["pull_timestamp"] >= since]

    monkeypatch.setattr(source, "candidate_rows", rows)
    monkeypatch.setattr(source, "market_caps", lambda conn, t, on: {k: v for k, v in state["caps"].items() if k in t})
    monkeypatch.setattr(source, "freshness", lambda conn: {"latest_insider_buy_pull": "x",
                                                           "latest_insider_trades_created_at": "y",
                                                           "insider_trades_rows_24h": 5})
    return state


def raw_row(acc, ticker, ingest, value_shares=200_000, price=5.0, url_cik="1709630"):
    return {"series_id": f"INSIDER:{ticker}:someone:BUY", "obs_date": date(2026, 10, 6), "pull_timestamp": ingest,
            "payload": {"accession": acc, "ticker": ticker, "filing_date": "2026-10-07",
                        "filing_url": f"https://www.sec.gov/Archives/edgar/data/{url_cik}/000000000000000001/x.xml",
                        "insider_name": "Someone", "transaction_date": "2026-10-06", "transaction_code": "P",
                        "shares": value_shares, "price": price, "is_derivative": False, "is_10b5_1": False}}


def make_log(tmp_path):
    from paper_log.trade_edge.__main__ import open_log
    return open_log(tmp_path / "log")


def test_end_to_end_signal_entry_exit_and_chain(tmp_path, fake_db):
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 8), 40)
    spy = {d: 100.0 * (1.001 ** i) for i, d in enumerate(days)}
    big = {d: 5.0 * (1.01 ** i) for i, d in enumerate(days)}
    prices = FakePrices(adj={"BORR": big, "SPY": spy, "XYZ": {}}, raw={"BORR": big, "XYZ": {}},
                        caps={"BORR": 1.3e9})
    sec = FakeSec({
        "acc-1": submission("acc-1", accepted="20261007075746"),
        "acc-2": submission("acc-2", accepted="20261007080000", ticker="XYZ", issuer_cik="0000000777",
                            owners=(("0000000099", "Small Buyer"),),
                            lines=[("2026-10-06", "P", "A", "1000", "20", "")]),
    })
    # genesis: Wednesday 2026-10-07 18:00 ET; filings ingested 16:05 ET -> entry Thursday 10-08
    fake_db["rows"] = [raw_row("acc-1", "BORR", et(2026, 10, 7, 16, 5)),
                       raw_row("acc-2", "XYZ", et(2026, 10, 7, 16, 6))]
    r1 = tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)
    kinds = [r["kind"] for r in r1["written"]]
    assert kinds[0] == "header" and kinds.count("filing") == 2 and kinds.count("signal") == 2
    assert "entry" not in kinds
    sig = {r["ticker"]: r for r in r1["written"] if r["kind"] == "signal"}
    assert sig["BORR"]["stratum"] == "large" and sig["BORR"]["cap_bucket"] == "300M-2B"
    assert sig["BORR"]["entry_session"] == "2026-10-08"
    assert sig["XYZ"]["stratum"] == "small" and sig["XYZ"]["cap_bucket"] == "unknown"

    # second run the same evening: nothing new except a run record
    r2 = tracker.run_once(log, conn=None, now=et(2026, 10, 7, 19, 0), code_sha="a" * 40, sec=sec, prices=prices)
    assert [r["kind"] for r in r2["written"]] == ["run"]
    assert sec.fetches == 2  # filings are read once

    # Friday 08:30: BORR opened; XYZ has no price yet (inside grace)
    r3 = tracker.run_once(log, conn=None, now=et(2026, 10, 9, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    ent = {r["ticker"]: r for r in r3["written"] if r["kind"] == "entry"}
    assert ent["BORR"]["status"] == "opened" and ent["BORR"]["late_logged"] is False
    assert "XYZ" not in ent
    assert any(r["kind"] == "marks" for r in r3["written"])

    # well after 30 sessions: all horizons closed, XYZ no_price
    later = et(2026, 11, 30, 8, 30)
    r4 = tracker.run_once(log, conn=None, now=later, code_sha="a" * 40, sec=sec, prices=prices)
    ent4 = {r["ticker"]: r for r in r4["written"] if r["kind"] == "entry"}
    assert ent4["XYZ"]["status"] == "no_price"
    exits = {r["horizon"]: r for r in r4["written"] if r["kind"] == "exit"}
    assert set(exits) == {5, 20, 30}
    assert exits[30]["return"] == pytest.approx(1.01 ** 30 - 1)
    assert exits[30]["spy_return"] == pytest.approx(1.001 ** 30 - 1)
    assert exits[30]["net_excess"] == pytest.approx(1.01 ** 30 - 1.001 ** 30 - 0.003)
    board = r4["scoreboard"]
    assert board["tables"]["h30_large"]["all"]["n_closed"] == 1
    assert board["label"]["label"] == "UNPROVEN"
    assert board["missing_labels"]["no_price"] == 1

    check = log.verify_chain()
    assert check["ok"], check
    records = log.read_all()
    assert records[0]["kind"] == "header" and records[0]["prereg_sha256"] == PREREG_SHA256
    assert sum(r["kind"] == "run" for r in records) == 4

    from paper_log.trade_edge.report import build_report, render_markdown, write_reports
    rep = build_report(r4, "a" * 40)
    md = render_markdown(rep)
    assert "UNPROVEN — not investment advice, research paper log" in md
    jpath, mpath = write_reports(tmp_path / "reports", rep)
    assert json.loads(jpath.read_text())["label"]["label"] == "UNPROVEN"
    assert (tmp_path / "reports" / "LATEST.md").exists()


def test_positions_before_genesis_are_not_admitted(tmp_path, fake_db):
    log = make_log(tmp_path)
    prices = FakePrices(adj={}, raw={})
    sec = FakeSec({"acc-1": submission("acc-1")})
    # accepted Wednesday 10-07 07:57 -> entry close Wednesday 16:00, before genesis Wednesday 18:00
    fake_db["rows"] = [raw_row("acc-1", "BORR", et(2026, 10, 6, 9, 0))]
    r = tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)
    kinds = [x["kind"] for x in r["written"]]
    assert kinds.count("filing") == 1 and "signal" not in kinds


def test_grid_db_fallback_and_late_logged(tmp_path, fake_db):
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 7), 10)
    px = {d: 5.0 for d in days}
    prices = FakePrices(adj={"QVCG": px, "SPY": px}, raw={"QVCG": px})
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 9, 0), code_sha="a" * 40, sec=FakeSec({}), prices=prices)
    # ingested 10:00 ET Wednesday (filing date 10-07 -> 22:00 rule): entry Thursday; first seen Friday 08:30
    fake_db["rows"] = [raw_row("acc-q", "QVCG", et(2026, 10, 7, 10, 0))]
    r = tracker.run_once(log, conn=None, now=et(2026, 10, 9, 8, 30), code_sha="a" * 40, sec=FakeSec({}), prices=prices)
    f = [x for x in r["written"] if x["kind"] == "filing"][0]
    assert f["source"] == "grid_db" and f["entry_session"] == "2026-10-08"
    e = [x for x in r["written"] if x["kind"] == "entry"][0]
    assert e["status"] == "opened" and e["late_logged"] is True
    assert r["scoreboard"]["missing_labels"]["grid_db_accessions"] == 1


def test_prereg_hash_is_pinned():
    from analysis.research_forward_log import lf_sha256
    assert lf_sha256((REPO / PREREG_PATH).read_bytes()) == PREREG_SHA256


def test_run_refuses_without_sec_user_agent(monkeypatch, tmp_path, capsys):
    from paper_log.trade_edge.__main__ import main
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    assert main(["run", "--log-dir", str(tmp_path / "log")]) == 2


def test_status_and_verify_need_no_database(tmp_path, fake_db, capsys):
    from paper_log.trade_edge.__main__ import main
    log = make_log(tmp_path)
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=FakeSec({}),
                     prices=FakePrices({}, {}))
    assert main(["verify", "--log-dir", str(tmp_path / "log")]) == 0
    assert main(["status", "--log-dir", str(tmp_path / "log")]) == 0
    out = capsys.readouterr().out
    assert "UNPROVEN" in out
