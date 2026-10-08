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


# ── review findings F3 (exit fallback chain), F2 and F1 end to end ──────────


def test_exit_entry_only_adjusted_uses_retained_marks_before_zero():
    days = sessions_from(date(2026, 10, 8), 12)
    spy = {d: 100.0 + i for i, d in enumerate(days)}
    now = et(2026, 10, 30)
    marks = [(days[2], 7.0), (days[3], 5.0)]
    x = tracker.compute_exit(entry(unadj=10.0), 5, {days[0]: 10.0}, spy, marks, days[10], now)
    assert x["status"] == "closed_delisted" and x["price_basis"] == "unadjusted_marks"
    assert x["end_session"] == days[3].isoformat() and x["return"] == pytest.approx(-0.5)
    assert x["spy_return"] == pytest.approx(spy[days[3]] / spy[days[0]] - 1)  # legs aligned on the mark date
    # a usable later close from the exit fetch still comes first
    y = tracker.compute_exit(entry(unadj=10.0), 5, {days[0]: 10.0, days[2]: 6.0}, spy, marks, days[10], now)
    assert y["price_basis"] == "adjusted_last_available" and y["end_session"] == days[2].isoformat()
    assert y["return"] == pytest.approx(-0.4)
    # entry-only and no marks (or no recorded unadjusted entry) -> entry close, 0%
    z = tracker.compute_exit(entry(unadj=10.0), 5, {days[0]: 10.0}, spy, [], days[10], now)
    assert z["price_basis"] == "entry_close_only" and z["return"] == 0.0 and z["end_session"] == days[0].isoformat()
    w = tracker.compute_exit(entry(unadj=None), 5, {days[0]: 10.0}, spy, marks, days[10], now)
    assert w["price_basis"] == "entry_close_only" and w["return"] == 0.0
    # missing benchmark and the grace window still wait
    assert tracker.compute_exit(entry(unadj=10.0), 5, {days[0]: 10.0}, {}, marks, days[10], now) is None
    assert tracker.compute_exit(entry(unadj=10.0), 5, {days[0]: 10.0}, spy, marks, days[9], now) is None


def _large_filing(acc, ticker, cik, accepted, line=("2026-10-06", "P", "A", "200000", "5", "")):
    return submission(acc, accepted=accepted, ticker=ticker, issuer_cik=cik, owners=((f"{cik}9", "Buyer"),),
                      lines=[line])


def test_fallback_first_then_enriched_filing_keeps_one_position(tmp_path, fake_db):
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 8), 45)
    spy = {d: 100.0 for d in days}
    borr = {d: 5.0 + 0.01 * i for i, d in enumerate(days)}
    prices = FakePrices(adj={"BORR": borr, "SPY": spy}, raw={"BORR": borr})
    sec = FakeSec({"acc-2": _large_filing("acc-2", "BORR", "0001715497", "20261008075746",
                                          line=("2026-10-07", "P", "A", "150000", "5", ""))})  # acc-1 -> grid_db
    fake_db["rows"] = [raw_row("acc-1", "BORR", et(2026, 10, 7, 16, 5))]
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)
    r2 = tracker.run_once(log, conn=None, now=et(2026, 10, 9, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    (first,) = [r for r in r2["written"] if r["kind"] == "entry"]
    assert first["position_id"] == "ticker:BORR|2026-10-08" and first["status"] == "opened"

    # a later, distinct, enriched accession identifies BORR as CIK 1715497 (entry the next day)
    fake_db["rows"].append(raw_row("acc-2", "BORR", et(2026, 10, 8, 16, 5)))
    r3 = tracker.run_once(log, conn=None, now=et(2026, 10, 10, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    kinds = [r["kind"] for r in r3["written"]]
    assert kinds.count("alias") == 1 and kinds.count("entry") == 1
    alias = next(r for r in r3["written"] if r["kind"] == "alias")
    assert (alias["ticker"], alias["cik"], alias["accession"]) == ("BORR", 1715497, "acc-2")
    assert set(r3["admitted"]) == {"ticker:BORR|2026-10-08", "cik:1715497|2026-10-09"}
    assert r3["admitted"]["ticker:BORR|2026-10-08"]["issuer_alias"] == "cik:1715497"
    assert next(r for r in r3["written"] if r["kind"] == "entry")["position_id"] == "cik:1715497|2026-10-09"
    assert next(r for r in r3["written"] if r["kind"] == "run")["counts"]["identity_reconciled"] == 1

    # restart (fresh replay) and score: the original purchase is scored exactly once
    r4 = tracker.run_once(log, conn=None, now=et(2026, 12, 15, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    assert "alias" not in [r["kind"] for r in r4["written"]] and "entry" not in [r["kind"] for r in r4["written"]]
    exits30 = sorted(r["position_id"] for r in r4["written"] if r["kind"] == "exit" and r["horizon"] == 30)
    assert exits30 == ["cik:1715497|2026-10-09", "ticker:BORR|2026-10-08"]
    assert r4["scoreboard"]["tables"]["h30_large"]["all"]["n_closed"] == 2
    state = tracker.State(log.read_all())
    assert state.aliases == {"BORR": 1715497} and set(state.entries) == set(exits30)
    assert log.verify_chain()["ok"]


def test_enriched_same_day_filing_joins_the_recorded_position(tmp_path, fake_db):
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 8), 45)
    px = {d: 5.0 for d in days}
    prices = FakePrices(adj={"BORR": px, "SPY": px}, raw={"BORR": px})
    sec = FakeSec({"acc-2": _large_filing("acc-2", "BORR", "0001715497", "20261007080000")})
    fake_db["rows"] = [raw_row("acc-1", "BORR", et(2026, 10, 7, 16, 5))]
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)
    fake_db["rows"].append(raw_row("acc-2", "BORR", et(2026, 10, 7, 16, 6)))  # same entry day, now with a CIK
    r2 = tracker.run_once(log, conn=None, now=et(2026, 10, 7, 19, 0), code_sha="a" * 40, sec=sec, prices=prices)
    sig = [r for r in r2["written"] if r["kind"] == "signal"]
    assert [s["position_id"] for s in sig] == ["ticker:BORR|2026-10-08"] and sig[0]["revision"] == 2
    # one purchase reported twice: the enriched report (earlier known_at) is the kept report
    assert sig[0]["accessions"] == ["acc-2"] and sig[0]["n_purchases"] == 1
    assert sig[0]["issuer_alias"] == "cik:1715497" and sig[0]["entry_session"] == "2026-10-08"
    r3 = tracker.run_once(log, conn=None, now=et(2026, 12, 15, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    assert [r["position_id"] for r in r3["written"] if r["kind"] == "entry"] == ["ticker:BORR|2026-10-08"]
    assert sum(1 for r in r3["written"] if r["kind"] == "exit" and r["horizon"] == 30) == 1
    assert len(r3["admitted"]) == 1


def test_enrichment_that_moves_known_at_earlier_does_not_reopen_an_entered_position(tmp_path, fake_db):
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 8), 45)
    px = {d: 5.0 for d in days}
    prices = FakePrices(adj={"BORR": px, "SPY": px}, raw={"BORR": px})
    sec = FakeSec({"acc-2": _large_filing("acc-2", "BORR", "0001715497", "20261008090000")})
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)  # genesis

    def filed_1008(acc, ingest):
        row = raw_row(acc, "BORR", ingest)
        return {**row, "payload": {**row["payload"], "filing_date": "2026-10-08"}}

    # fallback report ingested Thursday 10:00 (filing date 10-08 -> 22:00 rule): entry Friday 10-09
    fake_db["rows"] = [filed_1008("acc-1", et(2026, 10, 8, 10, 0))]
    r1 = tracker.run_once(log, conn=None, now=et(2026, 10, 10, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    (first,) = [r for r in r1["written"] if r["kind"] == "entry"]
    assert first["position_id"] == "ticker:BORR|2026-10-09" and first["status"] == "opened"
    # the same purchase, enriched, accepted 09:00 and ingested 14:00 on 10-08: known_at 14:15 -> entry 10-08
    fake_db["rows"].append(filed_1008("acc-2", et(2026, 10, 8, 14, 0)))
    r2 = tracker.run_once(log, conn=None, now=et(2026, 10, 13, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    kinds = [r["kind"] for r in r2["written"]]
    assert kinds.count("alias") == 1 and "entry" not in kinds and "signal" not in kinds
    assert set(r2["admitted"]) == {"ticker:BORR|2026-10-09"}
    pos = r2["admitted"]["ticker:BORR|2026-10-09"]
    assert pos["issuer_alias"] == "cik:1715497" and pos["entry_session"] == "2026-10-09"
    r3 = tracker.run_once(log, conn=None, now=et(2026, 12, 15, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    assert [r["position_id"] for r in r3["written"] if r["kind"] == "exit" and r["horizon"] == 30] == ["ticker:BORR|2026-10-09"]
    assert r3["scoreboard"]["tables"]["h30_large"]["all"]["n_closed"] == 1


def test_look_blocks_on_a_delayed_earlier_exit_and_is_decided_once(tmp_path, fake_db, monkeypatch):
    from paper_log.trade_edge import scoreboard as sb
    monkeypatch.setattr(sb, "LOOKS", (2, 4, 6))
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 8), 60)
    spy = {d: 100.0 for d in days}
    paths = {"AAA": (5.0, 2.5), "BBB": (5.0, 3.0), "CCC": (5.0, 7.5)}  # -50%, -40%, +50% over the window
    adj = {t: {d: lo + (hi - lo) * i / 60 for i, d in enumerate(days)} for t, (lo, hi) in paths.items()}
    prices = FakePrices(adj={**adj, "SPY": spy}, raw=dict(adj))
    sec = FakeSec({f"acc-{t}": _large_filing(f"acc-{t}", t, f"000000000{i}", "20261007080000")
                   for i, t in enumerate(paths, start=1)})
    ingest = {"AAA": et(2026, 10, 7, 16, 5), "BBB": et(2026, 10, 8, 16, 5), "CCC": et(2026, 10, 9, 16, 5)}
    fake_db["rows"] = [raw_row(f"acc-{t}", t, ingest[t], url_cik=str(i)) for i, t in enumerate(paths, start=1)]
    exit_b = sessions_from(date(2026, 10, 9), 30)[-1]  # BBB's primary exit session
    spy_hole = spy.pop(exit_b)  # BBB's benchmark close is missing: its h30 exit waits
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)  # genesis
    r1 = tracker.run_once(log, conn=None, now=et(2026, 12, 15, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    closed = sorted(r["position_id"] for r in r1["written"] if r["kind"] == "exit" and r["horizon"] == 30)
    assert closed == ["cik:1|2026-10-08", "cik:3|2026-10-12"]  # AAA and CCC closed, BBB pending
    lab = r1["scoreboard"]["label"]
    assert lab["label"] == "UNPROVEN" and lab["look_pending"]["blocked_by_positions"] == 1
    assert "look" not in [r["kind"] for r in r1["written"]]

    spy[exit_b] = spy_hole
    r2 = tracker.run_once(log, conn=None, now=et(2026, 12, 16, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    (look,) = [r for r in r2["written"] if r["kind"] == "look"]
    assert look["n"] == 2 and look["members"] == ["cik:1|2026-10-08", "cik:2|2026-10-09"]  # not CCC
    assert look["decision"] == "CONTRARY" and look["terminal"] is True
    run = next(r for r in r2["written"] if r["kind"] == "run")
    assert run["label"] == "CONTRARY" and run["looks_done"] == 1

    r3 = tracker.run_once(log, conn=None, now=et(2026, 12, 17, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    assert [r["kind"] for r in r3["written"]] == ["run"]
    assert r3["scoreboard"]["label"]["label"] == "CONTRARY" and r3["scoreboard"]["label"]["basis"] == "journal"
    assert tracker.State(log.read_all()).looks[2]["members"] == look["members"]
    assert log.verify_chain()["ok"]


def test_journal_label_without_look_record_is_refused_not_rewritten(tmp_path, fake_db):
    log = make_log(tmp_path)
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=FakeSec({}),
                     prices=FakePrices({}, {}))
    records = log.read_all()
    legacy = {**records[-1], "label": "SUPPORTED_FORWARD"}  # a run decided under the re-selection rule
    with pytest.raises(tracker.LookPolicyError):
        tracker.State(records + [legacy])
    assert tracker.State(records).looks == {}  # UNPROVEN run labels remain compatible


# ── review R1: one position per canonical (issuer, recorded entry session) ──


def _filed_1008(acc, ingest):
    row = raw_row(acc, "BORR", ingest)
    return {**row, "payload": {**row["payload"], "filing_date": "2026-10-08"}}


def _moved_day_setup(tmp_path, fake_db):
    """Fallback acc-1 (entry 10-09); acc-2 = the same purchase enriched with an earlier known_at
    (entry 10-08); acc-3 = a distinct enriched purchase whose entry is the recorded 10-09."""
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 8), 45)
    px = {d: 5.0 for d in days}
    prices = FakePrices(adj={"BORR": px, "SPY": px}, raw={"BORR": px})
    sec = FakeSec({"acc-2": _large_filing("acc-2", "BORR", "0001715497", "20261008090000"),
                   "acc-3": _large_filing("acc-3", "BORR", "0001715497", "20261008170000",
                                          line=("2026-10-07", "P", "A", "150000", "5", ""))})
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)  # genesis
    fake_db["rows"] = [_filed_1008("acc-1", et(2026, 10, 8, 10, 0))]
    return log, sec, prices


def _enriched_reports_arrive(fake_db):
    fake_db["rows"] += [_filed_1008("acc-2", et(2026, 10, 8, 14, 0)), _filed_1008("acc-3", et(2026, 10, 8, 17, 0))]


def test_moved_day_distinct_purchase_folds_into_the_signalled_slot_before_entry(tmp_path, fake_db):
    log, sec, prices = _moved_day_setup(tmp_path, fake_db)
    r1 = tracker.run_once(log, conn=None, now=et(2026, 10, 8, 23, 0), code_sha="a" * 40, sec=sec, prices=prices)
    assert [r["position_id"] for r in r1["written"] if r["kind"] == "signal"] == ["ticker:BORR|2026-10-09"]
    assert "entry" not in [r["kind"] for r in r1["written"]]  # 10-09 is not completed yet

    _enriched_reports_arrive(fake_db)
    r2 = tracker.run_once(log, conn=None, now=et(2026, 10, 9, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    kinds = [r["kind"] for r in r2["written"]]
    assert kinds.count("alias") == 1 and "entry" not in kinds
    (sig,) = [r for r in r2["written"] if r["kind"] == "signal"]  # no cik:...|2026-10-09 signal
    assert sig["position_id"] == "ticker:BORR|2026-10-09" and sig["revision"] == 2
    assert sig["entry_session"] == "2026-10-09" and sig["issuer_alias"] == "cik:1715497"
    assert sig["accessions"] == ["acc-2", "acc-3"] and sig["n_purchases"] == 2
    assert sig["total_value"] == pytest.approx(1_000_000 + 750_000) and sig["largest_value"] == pytest.approx(1_000_000)
    assert set(r2["admitted"]) == {"ticker:BORR|2026-10-09"}
    assert next(r for r in r2["written"] if r["kind"] == "run")["counts"]["identity_reconciled"] == 2

    # restart (fresh replay): one entry for the slot with the registered aggregate, scored once
    r3 = tracker.run_once(log, conn=None, now=et(2026, 12, 15, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    assert "signal" not in [r["kind"] for r in r3["written"]]
    (ent,) = [r for r in r3["written"] if r["kind"] == "entry"]
    assert ent["position_id"] == "ticker:BORR|2026-10-09" and ent["status"] == "opened"
    assert ent["accessions"] == ["acc-2", "acc-3"] and ent["n_purchases"] == 2 and ent["stratum"] == "large"
    assert [r["position_id"] for r in r3["written"] if r["kind"] == "exit" and r["horizon"] == 30] == \
        ["ticker:BORR|2026-10-09"]
    assert r3["scoreboard"]["tables"]["h30_large"]["all"]["n_closed"] == 1
    r4 = tracker.run_once(log, conn=None, now=et(2026, 12, 16, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    assert set(r4["admitted"]) == {"ticker:BORR|2026-10-09"}
    assert not {"signal", "entry", "exit"} & {r["kind"] for r in r4["written"]}
    assert set(tracker.State(log.read_all()).entries) == {"ticker:BORR|2026-10-09"}
    assert log.verify_chain()["ok"]


def _body(rec):
    return {k: v for k, v in rec.items() if k != "prev_sha256"}  # the record without its chain link


def _entered_slot_runs(tmp_path, fake_db, late):
    """ticker:BORR|2026-10-09 is entered; then the enriched re-report acc-2 (and, with ``late``, the
    distinct purchase acc-3 of the same issuer/session) becomes visible; then a restart scores everything."""
    log, sec, prices = _moved_day_setup(tmp_path, fake_db)
    r1 = tracker.run_once(log, conn=None, now=et(2026, 10, 10, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    before = log.read_all()
    fake_db["rows"] += [_filed_1008("acc-2", et(2026, 10, 8, 14, 0))]
    if late:
        fake_db["rows"] += [_filed_1008("acc-3", et(2026, 10, 8, 17, 0))]
    r2 = tracker.run_once(log, conn=None, now=et(2026, 10, 13, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    r3 = tracker.run_once(log, conn=None, now=et(2026, 12, 15, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    return log, before, r1, r2, r3


def test_moved_day_distinct_purchase_on_an_entered_slot_is_noted_once_and_never_scored(tmp_path, fake_db):
    from paper_log.trade_edge.__main__ import status_from_records
    log, before, r1, r2, r3 = _entered_slot_runs(tmp_path / "late", fake_db, late=True)
    (first,) = [r for r in r1["written"] if r["kind"] == "entry"]
    assert first["position_id"] == "ticker:BORR|2026-10-09" and first["accessions"] == ["acc-1"]
    assert tracker.State(before).late_members == [] and status_from_records(before)["late_members"] == 0

    kinds = [r["kind"] for r in r2["written"]]
    assert kinds.count("alias") == 1 and kinds.count("late_member") == 1 and not {"signal", "entry"} & set(kinds)
    (note,) = [r for r in r2["written"] if r["kind"] == "late_member"]
    acc3 = next(r for r in r2["written"] if r["kind"] == "filing" and r["accession"] == "acc-3")
    assert _body(note) == {"kind": "late_member", "run_at": et(2026, 10, 13, 8, 30).isoformat(),
                           "position_id": "ticker:BORR|2026-10-09", "issuer_key": "cik:1715497",
                           "entry_session": "2026-10-09", "accession": "acc-3", "known_at": acc3["known_at"],
                           "purchase_keys": [["cik:1715497", "2026-10-07", 150000, 5.0]],
                           "reason": "moved_day_after_entry", "rebuilt_id": "cik:1715497|2026-10-09"}
    assert set(r2["admitted"]) == {"ticker:BORR|2026-10-09"}  # no second position for the slot
    assert next(r for r in r2["written"] if r["kind"] == "run")["counts"]["written"]["late_member"] == 1
    records = log.read_all()
    assert records[:len(before)] == before  # nothing historical is rewritten

    # restart (fresh replay): the note is replayed, not appended again; the entry is scored as recorded
    kinds3 = [r["kind"] for r in r3["written"]]
    assert "late_member" not in kinds3 and not {"signal", "entry"} & set(kinds3)
    state = tracker.State(log.read_all())
    assert state.late_members == [note] and state.entries == {first["position_id"]: first}
    assert status_from_records(log.read_all())["late_members"] == 1
    assert log.verify_chain()["ok"]

    # the late purchase never counts: exits and statistics equal a log where it never appeared
    fake_db["rows"] = []
    log_c, _, r1_c, r2_c, r3_c = _entered_slot_runs(tmp_path / "control", fake_db, late=False)
    assert "late_member" not in {r["kind"] for r in log_c.read_all()}
    assert [_body(r) for r in r1_c["written"] if r["kind"] == "entry"] == [_body(first)]
    assert [_body(r) for r in r3["written"] if r["kind"] == "exit"] == \
        [_body(r) for r in r3_c["written"] if r["kind"] == "exit"]
    assert r3["scoreboard"]["tables"] == r3_c["scoreboard"]["tables"]
    assert r3["scoreboard"]["label"] == r3_c["scoreboard"]["label"]
    assert r3["scoreboard"]["tables"]["h30_large"]["all"]["n_closed"] == 1


def test_same_group_late_purchase_on_an_entered_slot_is_noted_like_a_moved_one(tmp_path, fake_db):
    log = make_log(tmp_path)
    days = sessions_from(date(2026, 10, 8), 45)
    px = {d: 5.0 for d in days}
    prices = FakePrices(adj={"BORR": px, "SPY": px}, raw={"BORR": px})
    sec = FakeSec({"acc-5": _large_filing("acc-5", "BORR", "0001715497", "20261012090000",
                                          line=("2026-10-05", "S", "D", "100", "5", ""))})  # alias only, no purchase
    tracker.run_once(log, conn=None, now=et(2026, 10, 7, 18, 0), code_sha="a" * 40, sec=sec, prices=prices)  # genesis
    fake_db["rows"] = [_filed_1008("acc-1", et(2026, 10, 8, 10, 0))]  # grid_db fallback: entry 10-09
    r1 = tracker.run_once(log, conn=None, now=et(2026, 10, 10, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    (first,) = [r for r in r1["written"] if r["kind"] == "entry"]
    assert first["position_id"] == "ticker:BORR|2026-10-09"

    # a distinct fallback purchase ingested 10-08 11:00 becomes visible only now (ingest lag): same rebuilt group
    row = raw_row("acc-4", "BORR", et(2026, 10, 8, 11, 0), value_shares=100_000)
    fake_db["rows"].append({**row, "payload": {**row["payload"], "filing_date": "2026-10-08"}})
    r2 = tracker.run_once(log, conn=None, now=et(2026, 10, 13, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    kinds = [r["kind"] for r in r2["written"]]
    assert kinds.count("late_member") == 1 and not {"signal", "entry"} & set(kinds)
    (note,) = [r for r in r2["written"] if r["kind"] == "late_member"]
    assert (note["position_id"], note["accession"], note["reason"], note["rebuilt_id"], note["purchase_keys"]) == (
        "ticker:BORR|2026-10-09", "acc-4", "same_group_after_entry", "ticker:BORR|2026-10-09",
        [["ticker:BORR", "2026-10-06", 100000, 5.0]])

    # a later filing teaches BORR -> CIK 1715497: the noted purchase is matched canonically, not noted again
    fake_db["rows"].append(raw_row("acc-5", "BORR", et(2026, 10, 12, 9, 0)))
    r3 = tracker.run_once(log, conn=None, now=et(2026, 10, 14, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    kinds3 = [r["kind"] for r in r3["written"]]
    assert kinds3.count("alias") == 1 and "late_member" not in kinds3 and not {"signal", "entry"} & set(kinds3)
    r4 = tracker.run_once(log, conn=None, now=et(2026, 12, 15, 8, 30), code_sha="a" * 40, sec=sec, prices=prices)
    assert "late_member" not in [r["kind"] for r in r4["written"]]
    assert [r["position_id"] for r in r4["written"] if r["kind"] == "exit" and r["horizon"] == 30] == \
        ["ticker:BORR|2026-10-09"]
    state = tracker.State(log.read_all())
    assert state.late_members == [note] and state.entries == {first["position_id"]: first}
    assert first["accessions"] == ["acc-1"] and first["n_purchases"] == 1  # membership for scoring stays frozen
    assert r4["scoreboard"]["tables"]["h30_large"]["all"]["n_closed"] == 1
    assert log.verify_chain()["ok"]


# ── review R2: status reads the journal's look records (never decides) ──────


def _status_journal(nets, looks=(), run_label="UNPROVEN", looks_done=0):
    recs = [{"kind": "header", "run_at": "2026-10-07T18:00:00-04:00"}]
    for i, net in enumerate(nets):
        pid = f"cik:{i + 1}|2026-10-{8 + i:02d}"
        recs.append({"kind": "entry", "position_id": pid, "status": "opened", "stratum": "large",
                     "cap_bucket": ">=2B", "entry_session": f"2026-10-{8 + i:02d}"})
        recs.append({"kind": "exit", "position_id": pid, "horizon": 30, "status": "closed", "net_excess": net,
                     "gross_excess": net + 0.003, "exit_session": f"2026-11-{19 + i:02d}"})
    recs += [dict(rec) for rec in looks]
    recs.append({"kind": "run", "run_at": "2026-12-16T08:30:00-05:00", "label": run_label,
                 "looks_done": looks_done, "look_pending": None})
    return recs


def test_status_reads_terminal_and_completed_looks_and_keeps_undecided_interim(monkeypatch):
    from paper_log.trade_edge import scoreboard as sb
    from paper_log.trade_edge.__main__ import status_from_records
    monkeypatch.setattr(sb, "LOOKS", (2, 4, 6))
    nets = [-0.5, -0.4, 0.5]
    members = ["cik:1|2026-10-08", "cik:2|2026-10-09"]
    contrary_stats = sb.look_statistics([{"net_excess": n, "entry_session": s} for n, s in
                                         ((-0.5, "2026-10-08"), (-0.4, "2026-10-09"))])

    # terminal look: top-level label/banner are the journal's decision and agree with last_run
    terminal = {"kind": "look", "run_at": "2026-12-16T08:30:00-05:00", "n": 2, "boundary_exit_session": "2026-11-20",
                "members": members, "stats": contrary_stats, "decision": "CONTRARY", "terminal": True}
    recs = _status_journal(nets, [terminal], run_label="CONTRARY", looks_done=1)
    snapshot = [dict(r) for r in recs]
    st = status_from_records(recs)
    assert st["label"]["label"] == st["last_run"]["label"] == "CONTRARY"
    assert st["banner"].startswith("CONTRARY") and st["label"]["basis"] == "journal"
    assert st["label"]["looks_done"] == st["last_run"]["looks_done"] == 1 and st["label"]["decided_at_look"] == 2
    assert st["label"]["look_stats"] == contrary_stats and recs == snapshot  # read only
    for decision in ("SUPPORTED_FORWARD", "NOT_SUPPORTED"):
        other = status_from_records(_status_journal(nets, [{**terminal, "decision": decision}], decision, 1))
        assert other["label"]["label"] == decision and other["banner"].startswith(decision)

    # completed unsuccessful look: its membership/statistics stay; the next look is not decided by status
    unproven_stats = {**contrary_stats, "clustered_t": 0.5}
    done = {**terminal, "stats": unproven_stats, "decision": "UNPROVEN", "terminal": False}
    st2 = status_from_records(_status_journal(nets, [done], run_label="UNPROVEN", looks_done=1))
    assert st2["label"]["label"] == st2["last_run"]["label"] == "UNPROVEN"
    assert st2["label"]["looks_done"] == st2["last_run"]["looks_done"] == 1
    assert st2["label"]["look_stats"] == unproven_stats and st2["label"]["basis"] == "journal"
    assert st2["label"]["next_look_at_n_closed"] == 4

    # undecided journal: interim UNPROVEN, even where the tracker would decide this look
    undecided = _status_journal(nets)
    st3 = status_from_records(undecided)
    assert st3["label"]["label"] == "UNPROVEN" and st3["label"]["basis"] == "interim"
    assert st3["label"]["looks_done"] == 0 and st3["label"]["look_pending"]["n"] == 2
    assert st3["banner"].startswith("UNPROVEN") and "new_looks" not in st3
    entries = [r for r in undecided if r["kind"] == "entry"]
    exits = [r for r in undecided if r["kind"] == "exit"]
    assert sb.build_scoreboard(entries, exits, looks={}, decide=True)["label"]["label"] == "CONTRARY"
