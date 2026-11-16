"""3pacs/GRID#1180 slice 1: filing-URL citations, 10-K only (10-Q measured, dropped), validated-derived gate."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from ingestion.altdata import supply_chain_parser as scp
from intelligence import chain_contagion as cc

CIK = "0000080424"


def meta(form, day, acc):
    return {"accession": acc, "primary_doc": f"{form.lower()}.htm", "filing_date": day, "form": form}


@pytest.fixture
def parser(monkeypatch):
    p = scp.SupplyChain10KParser(db_engine=MagicMock())
    monkeypatch.setattr(p, "_resolve_cik", lambda t: CIK)
    return p


def test_only_the_10k_is_read(parser, monkeypatch):
    metas = {"10-K": meta("10-K", date(2026, 8, 5), "k"), "10-Q": meta("10-Q", date(2026, 10, 24), "q")}
    asked = []
    monkeypatch.setattr(parser, "_latest_filing_meta", lambda cik, form: asked.append(form) or metas[form])
    assert [m["form"] for m in parser._filings_to_read(CIK)] == ["10-K"]
    assert asked == ["10-K"] and scp.FORMS_READ == ("10-K",)


def test_filings_are_read_newest_first_when_several_forms_are_enabled(parser, monkeypatch):
    metas = {"10-K": meta("10-K", date(2026, 8, 5), "k"), "10-Q": meta("10-Q", date(2026, 10, 24), "q")}
    monkeypatch.setattr(scp, "FORMS_READ", ("10-K", "10-Q"))
    monkeypatch.setattr(parser, "_latest_filing_meta", lambda cik, form: metas[form])
    assert [m["form"] for m in parser._filings_to_read(CIK)] == ["10-Q", "10-K"]


def test_filing_url_points_at_the_sec_archive():
    url = scp.SupplyChain10KParser._filing_url(CIK, meta("10-Q", date(2026, 10, 24), "0000080424-26-000002"))
    assert url == "https://www.sec.gov/Archives/edgar/data/80424/000008042426000002/10-q.htm"


def test_edges_cite_the_newest_filing(parser, monkeypatch):
    monkeypatch.setattr(scp, "FORMS_READ", ("10-K", "10-Q"))
    metas = {"10-K": meta("10-K", date(2026, 8, 5), "0000080424-26-000001"),
             "10-Q": meta("10-Q", date(2026, 10, 24), "0000080424-26-000002")}
    docs = {
        "10-K": "Sales to Walmart Inc. and its affiliates represented approximately 15% of total net sales.",
        "10-Q": "Sales to Walmart Inc. and its affiliates represented approximately 16% of total net sales.",
    }
    monkeypatch.setattr(parser, "_latest_filing_meta", lambda cik, form: metas[form])
    monkeypatch.setattr(parser, "_fetch_10k_text", lambda cik, m: docs[m["form"]])
    conn = MagicMock()
    conn.execute.return_value.rowcount = 1
    parser.engine.begin.return_value.__enter__.return_value = conn

    assert parser.process_ticker("PG") == 1
    edge_params = [c.args[1] for c in conn.execute.call_args_list
                   if "INSERT INTO supply_chain_edges" in str(c.args[0])]
    assert len(edge_params) == 1  # one relationship, deduped across the two filings
    e = edge_params[0]
    assert e["src"].startswith("10-Q 2026 PG ")
    assert e["src"].endswith("https://www.sec.gov/Archives/edgar/data/80424/000008042426000002/10-q.htm")
    assert e["as_of"] == date(2026, 10, 24)
    assert e["conf"] == "derived"
    assert parser.stats.filings_fetched == 2


def test_a_failed_fetch_of_one_filing_still_reads_the_other(parser, monkeypatch):
    monkeypatch.setattr(scp, "FORMS_READ", ("10-K", "10-Q"))
    metas = {"10-K": meta("10-K", date(2026, 8, 5), "a"), "10-Q": meta("10-Q", date(2026, 10, 24), "b")}
    monkeypatch.setattr(parser, "_latest_filing_meta", lambda cik, form: metas[form])
    monkeypatch.setattr(parser, "_fetch_10k_text", lambda cik, m: None if m["form"] == "10-Q" else
                        "Sales to Walmart Inc. represented approximately 15% of net sales.")
    conn = MagicMock()
    conn.execute.return_value.rowcount = 1
    parser.engine.begin.return_value.__enter__.return_value = conn
    assert parser.process_ticker("PG") == 1
    assert parser.stats.filings_failed == 1 and parser.stats.filings_fetched == 1


# --- contagion gate ---------------------------------------------------------------


def test_contagion_uses_derived_edges_only_after_validation():
    engine = create_engine("sqlite://")
    k8 = "8-K 2026-11-12 TSM Item 1.01 https://www.sec.gov/Archives/edgar/data/1046179/000104617926000001/a.htm"
    k10 = "10-K 2026 TSM concentration https://www.sec.gov/Archives/edgar/data/1046179/000104617926000002/b.htm"
    with engine.begin() as conn:
        conn.execute(text(
            "create table supply_chain_edges (upstream_id text, downstream_id text, relationship text,"
            " input_type text, annual_usd real, pct_downstream_cogs real, chokepoint_score real,"
            " confidence text, last_validation_at text, relationship_weak boolean, source text)"))
        conn.execute(text("create table supply_chain_nodes (id text, type text)"))
        for nid, typ in [("tsmc", "ticker"), ("aapl", "ticker"), ("openai", "private_company"),
                         ("cargill", "private_company")]:
            conn.execute(text("insert into supply_chain_nodes values (:i, :t)"), {"i": nid, "t": typ})
        rows = [
            ("confirmed_unchecked", "confirmed", None, None, ""),
            ("derived_unchecked", "derived", None, None, ""),
            ("derived_checked", "derived", "2026-11-15", False, ""),
            ("derived_weak", "derived", "2026-11-15", True, ""),
            ("derived_checked_null_weak", "derived", "2026-11-15", None, ""),
            ("backtest_tier", "derived_from_backtest", None, None, ""),
            # 8-K edge to a private company: the validator cannot price it; it joins on its citation
            ("openai", "derived", None, None, k8),
            # 8-K edge between two listed nodes: waits for the validator like any derived edge
            ("aapl", "derived", None, None, k8),
            # 10-K regex edge to a private company: still out
            ("cargill", "derived", None, None, k10),
        ]
        for down, conf, checked, weak, src in rows:
            conn.execute(text(
                "insert into supply_chain_edges values ('tsmc', :d, 'supplier', null, null, null, null,"
                " :c, :v, :w, :s)"), {"d": down, "c": conf, "v": checked, "w": weak, "s": src})
    with engine.connect() as conn:
        got = sorted(e["downstream_id"] for e in cc._fetch_downstream_edges(conn, "tsmc"))
    assert got == ["backtest_tier", "confirmed_unchecked", "derived_checked", "derived_checked_null_weak",
                   "openai"]
