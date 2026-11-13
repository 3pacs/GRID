"""3pacs/GRID#1180 slice 2: supplier/customer links from 8-K Item 1.01 (fixture filings, fake model)."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

import pytest

from ingestion.altdata import supply_chain_8k as s8
from ingestion.altdata import supply_chain_parser as scp

DAL_SECTION = (
    "Entry into a Material Definitive Agreement. On January 12, 2026, Delta Air Lines, Inc. (“Delta”) "
    "entered into a definitive agreement with The Boeing Company to acquire 30 Boeing 787-10 aircraft, with an "
    "option to purchase up to an additional 30 of the same aircraft. The 787-10 aircraft will include GEnx "
    "engines manufactured by GE Aerospace."
)
LOAN_SECTION = (
    "Entry into a Material Definitive Agreement. On November 9, 2026, the Company entered into a purchase "
    "agreement with J.P. Morgan Securities LLC relating to the sale of 1.00% Convertible Senior Notes due 2031 "
    "under an indenture with U.S. Bank Trust Company."
)
AKAM_SECTION = (
    "Entry into a Material Definitive Agreement. On September 18, 2026, Akamai Technologies, Inc. (the "
    "“Company”) and Anthropic, PBC (“Anthropic”) entered into Project Plan 2 under the existing "
    "Master Services Agreement, under which the Company will provide cloud infrastructure services to Anthropic."
)
INDEX = """Form Type   Company Name                                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
10-Q             ACME CORP                                                     1234567     20260112    edgar/data/1234567/0001234567-26-000001.txt
8-K              DELTA AIR LINES, INC.                                         27904       20260112    edgar/data/27904/0000027904-26-000002.txt
8-K/A            DELTA AIR LINES, INC.                                         27904       20260112    edgar/data/27904/0000027904-26-000003.txt
8-K              SMALLCO INC                                                   999         20260112    edgar/data/999/0000000999-26-000004.txt
"""


def reply(*links):
    return "<think>reading</think>" + json.dumps({"links": [dict(zip(
        ("supplier", "customer", "provides", "quote", "kind"), lk)) for lk in links]})


# --- pure pieces -----------------------------------------------------------------


def test_daily_index_keeps_8k_lines_only():
    got = s8.parse_index(INDEX)
    assert got == [("0000027904", "DELTA AIR LINES, INC.", "0000027904-26-000002"),
                   ("0000000999", "SMALLCO INC", "0000000999-26-000004")]


def test_item_101_section_stops_at_the_next_item():
    body = "Cover page. Item 1.01 " + DAL_SECTION + " Item 9.01 Financial Statements and Exhibits. Exhibit 10.1"
    sec = s8.item_101_section(body)
    assert sec.startswith("Entry into a Material") and "Exhibit 10.1" not in sec and "9.01" not in sec


@pytest.mark.parametrize("section, kind", [
    (DAL_SECTION, "commercial"),
    (AKAM_SECTION, "commercial"),
    (LOAN_SECTION, "financing"),
    (DAL_SECTION + " The Company also amended its revolving credit agreement.", "mixed"),
    ("Entry into a Material Definitive Agreement. The information set forth in Item 2.03 is incorporated.",
     "other"),
])
def test_keyword_screen(section, kind):
    assert s8.classify_section(section) == kind


def test_model_reply_parsing():
    links = s8.parse_links(reply(("The Boeing Company", "FILER", "787-10 aircraft", "x", "Supply")))
    assert links == [s8.Link("The Boeing Company", "FILER", "787-10 aircraft", "x", kind="supply")]
    assert s8.parse_links('{"links": []}') == []
    assert s8.parse_links("no json here") is None
    assert s8.parse_links('{"links": "none"}') is None
    assert s8.parse_links(None) is None


QUOTE_DAL = ("Delta Air Lines, Inc. (“Delta”) entered into a definitive agreement with The Boeing Company "
             "to acquire 30 Boeing 787-10 aircraft")


@pytest.mark.parametrize("link, want", [
    (s8.Link("The Boeing Company", "FILER", "787-10 aircraft", QUOTE_DAL), ("Boeing Company", "filer_buys")),
    # curly quotes in the filing, straight quotes from the model: still verbatim
    (s8.Link("The Boeing Company", "FILER", "aircraft", QUOTE_DAL.replace("“", '"').replace("”", '"')),
     ("Boeing Company", "filer_buys")),
    (s8.Link("FILER", "FILER", "", QUOTE_DAL), (None, "filer_not_one_side")),
    (s8.Link("The Boeing Company", "Airbus", "", QUOTE_DAL), (None, "filer_not_one_side")),
    (s8.Link("The Boeing Company", "FILER", "", "Delta agreed to buy 300 aircraft"), (None, "quote_not_in_filing")),
    (s8.Link("Airbus S.A.S.", "FILER", "", QUOTE_DAL), (None, "counterparty_not_in_filing")),
    (s8.Link("J.P. Morgan Securities LLC", "FILER", "", QUOTE_DAL), (None, "financial_counterparty")),
    (s8.Link("the", "FILER", "", QUOTE_DAL), (None, "counterparty_noise")),
])
def test_link_validation(link, want):
    assert s8.validate_link(link, DAL_SECTION) == want


def test_local_providers_are_never_paid():
    from llm.router import _PAID_PROVIDERS, _RETIRED_PROVIDERS

    assert not set(s8.LOCAL_PROVIDERS) & (_PAID_PROVIDERS | _RETIRED_PROVIDERS)


def test_days_skip_weekends_and_today(monkeypatch, tmp_path):
    monkeypatch.setattr(s8, "CHECKPOINT_PATH", tmp_path / "ck.json")
    job = s8.EightKSupplyLinks(MagicMock(), parser=MagicMock())
    fri = date(2026, 11, 13)
    assert job.days_to_read(fri, date(2026, 11, 6)) == [date(2026, 11, 6), date(2026, 11, 9), date(2026, 11, 10),
                                                         date(2026, 11, 11), date(2026, 11, 12)]
    assert job.days_to_read(fri, None)[0] == date(2026, 11, 6)  # first run: seven days back
    s8.EightKSupplyLinks.save_checkpoint(date(2026, 11, 11), ["a"])
    assert job.days_to_read(fri, None) == [date(2026, 11, 12)]


# --- the pass, with SEC and the model faked ------------------------------------------


class FakeLLM:
    def __init__(self, raw):
        self.raw, self.calls = raw, 0

    def chat(self, messages, **kw):
        self.calls += 1
        assert kw["temperature"] == 0.0 and "FILER" in messages[0]["content"]
        return self.raw


@pytest.fixture
def job(monkeypatch, tmp_path):
    monkeypatch.setattr(s8, "CHECKPOINT_PATH", tmp_path / "ck.json")
    parser = scp.SupplyChain10KParser.__new__(scp.SupplyChain10KParser)
    parser.seed_node_ids = {"dal", "ba", "akam"}
    parser.name_to_node_id = {}
    parser._ticker_to_node = {"DAL": "dal", "BA": "ba", "AKAM": "akam"}
    parser._upsert_node = MagicMock(return_value=True)
    parser._upsert_edge = MagicMock(return_value=True)
    engine = MagicMock()

    def make(llm, sections, universe=None, items="1.01,9.01"):
        j = s8.EightKSupplyLinks(engine, parser=parser, llm=llm)
        universe = universe or {"0000027904": ("DAL", "dal", True), "0000000999": ("SMCO", "smco", False)}
        monkeypatch.setattr(j, "filer_universe", lambda: universe)
        j._universe, j._graph_names = universe, {"boeing": "ba"}
        monkeypatch.setattr(j, "index_8ks", lambda day: s8.parse_index(INDEX))

        def details(f):
            f.items = items
            f.url = f"https://www.sec.gov/Archives/edgar/data/{int(f.cik)}/{f.accession.replace('-', '')}/d.htm"
            return True

        monkeypatch.setattr(j, "filing_details", details)
        monkeypatch.setattr(j, "filing_text", lambda f: "Item 1.01 " + sections[f.cik] + " Item 9.01 Exhibits")
        return j
    make.parser, make.engine = parser, engine
    return make


def test_dry_run_finds_the_link_and_writes_nothing(job):
    llm = FakeLLM(reply(("The Boeing Company", "FILER", "787-10 aircraft", QUOTE_DAL)))
    j = job(llm, {"0000027904": DAL_SECTION, "0000000999": LOAN_SECTION})
    s = j.run(write=False, since=date(2026, 1, 12), today=date(2026, 1, 13))
    assert s.listed_8ks == 2 and s.graph_8ks == 1 and s.item_101 == 2
    assert llm.calls == 1 and s.skipped == {"financing": 1}  # the notes deal never reaches the model
    (link,) = s.links
    assert (link["upstream_id"], link["downstream_id"], link["relationship"]) == ("ba", "dal", "component")
    assert link["confidence"] == "derived" and link["as_of"] == "2026-01-12"
    assert link["source"].startswith("8-K 2026-01-12 DAL Item 1.01 https://www.sec.gov/Archives/edgar/data/27904/")
    assert link["new_node"] is False and link["new_filer_node"] is False
    assert not job.parser._upsert_edge.called and not job.engine.begin.called


def test_write_adds_a_filer_new_to_the_graph_as_a_ticker_node(job):
    llm = FakeLLM(reply(("FILER", "Anthropic, PBC", "cloud infrastructure services",
                         "the Company will provide cloud infrastructure services to Anthropic", "services")))
    j = job(llm, {"0000000999": AKAM_SECTION, "0000027904": LOAN_SECTION})
    s = j.run(write=True, since=date(2026, 1, 12), today=date(2026, 1, 13))
    (link,) = s.links
    assert (link["upstream_id"], link["downstream_id"], link["relationship"]) == ("smco", "anthropic_pbc", "component")
    nodes = [c.args[1] for c in job.parser._upsert_node.call_args_list]
    assert [(n.id, n.type) for n in nodes] == [("smco", "ticker"), ("anthropic_pbc", "private_company")]
    assert nodes[0].name == "Smallco Inc" and "edgar/data/999/" in nodes[0].notes
    assert s.edges_written == 1 and s.nodes_written == 2


def test_rejected_links_and_seen_filings(job):
    llm = FakeLLM(reply(("J.P. Morgan Securities LLC", "FILER", "", QUOTE_DAL),
                        ("Airbus S.A.S.", "FILER", "", "made up quote")))
    j = job(llm, {"0000027904": DAL_SECTION, "0000000999": DAL_SECTION})
    s = j.run(write=True, since=date(2026, 1, 12), today=date(2026, 1, 13))
    assert s.links == [] and s.rejected == {"financial_counterparty": 2, "quote_not_in_filing": 2}
    assert not job.parser._upsert_edge.called
    ck = json.loads(s8.CHECKPOINT_PATH.read_text())
    assert ck["last_day"] == "2026-01-12" and len(ck["seen"]) == 2
    again = j.run(write=True, since=date(2026, 1, 12), today=date(2026, 1, 13))
    assert again.listed_8ks == 0 and llm.calls == 2  # already-seen accessions are not re-read


def test_no_local_model_means_no_edges(job, monkeypatch):
    j = job(None, {"0000027904": DAL_SECTION, "0000000999": LOAN_SECTION})
    monkeypatch.setattr(j, "_client", lambda: None)
    s = j.run(write=True, since=date(2026, 1, 12), today=date(2026, 1, 13))
    assert s.rejected == {"no_model": 1} and s.links == [] and s.model_calls == 0


def test_filings_without_item_101_are_not_fetched(job):
    llm = FakeLLM(reply())
    j = job(llm, {}, items="2.02,9.01")
    s = j.run(write=False, since=date(2026, 1, 12), today=date(2026, 1, 13))
    assert s.skipped == {"no_item_101": 2} and s.item_101 == 0 and llm.calls == 0


def universe_job():
    listing = [("TSM", "0001046179", "TAIWAN SEMICONDUCTOR MANUFACTURING CO LTD"),
               ("BA", "0000012927", "BOEING CO"),
               ("BKR", "0001701605", "Baker Hughes Co"),
               ("DAL", "0000027904", "DELTA AIR LINES, INC."),
               ("BRK.B", "0001067983", "BERKSHIRE HATHAWAY INC"),
               ("XLABW", "0002109869", "Exascale Labs Holdings Inc."),
               ("XLAB", "0002109869", "Exascale Labs Holdings Inc.")]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [
        ("tsmc", "ticker", "Taiwan Semiconductor Manufacturing Company"), ("ba", "ticker", "Boeing"),
        ("openai", "private_company", "OpenAI")]
    parser = MagicMock()
    parser._resolve_counterparty.side_effect = lambda n: (scp._slug(n), scp.DerivedNode(scp._slug(n), n))
    j = s8.EightKSupplyLinks(engine, parser=parser, llm=FakeLLM(""))
    j.sec_listing = lambda: listing
    return j


def test_universe_puts_graph_nodes_first_and_takes_the_main_symbol():
    u = universe_job().filer_universe()
    assert u["0001046179"] == ("TSM", "tsmc", True)
    assert u["0000012927"] == ("BA", "ba", True)
    assert u["0000027904"] == ("DAL", "dal", False)
    assert u["0001067983"] == ("BRK-B", "brk-b", False)
    assert u["0002109869"] == ("XLAB", "xlab", False)  # the shortest symbol for a CIK, not its warrant


@pytest.mark.parametrize("a, b", [
    ("The Boeing Company", "BOEING CO"),
    ("Delta Air Lines, Inc.", "DELTA AIR LINES INC"),
    ("Anthropic, PBC", "Anthropic"),
    ("Airbus S.A.S.", "Airbus SE"),
])
def test_company_key_ignores_legal_form(a, b):
    assert s8.company_key(a) == s8.company_key(b)


def test_counterparties_resolve_to_graph_listed_or_private_nodes():
    j = universe_job()
    j.filer_universe()
    assert j.resolve_counterparty("The Boeing Company") == ("ba", None)  # graph node by name
    assert j.resolve_counterparty("Taiwan Semiconductor Manufacturing Co., Ltd.") == ("tsmc", None)
    nid, node = j.resolve_counterparty("Delta Air Lines, Inc.")  # listed, not yet in the graph
    assert nid == "dal" and (node.id, node.type, node.name) == ("dal", "ticker", "Delta Air Lines, Inc.")
    nid, node = j.resolve_counterparty("Caney Fork Farms, LLC")  # not listed: a private node
    assert nid == "caney_fork_farms_llc" and node.type == "private_company"


def test_a_budget_stops_between_filings_and_the_next_run_resumes(job, monkeypatch):
    llm = FakeLLM(reply())
    j = job(llm, {"0000027904": DAL_SECTION, "0000000999": DAL_SECTION})
    clock = iter([0.0, 0.0, 999.0, 999.0])
    monkeypatch.setattr(s8.time, "monotonic", lambda: next(clock, 999.0))
    first = j.run(since=date(2026, 1, 12), today=date(2026, 1, 13), budget_seconds=10)
    assert first.stopped_early and first.listed_8ks == 1 and first.days == []
    ck = json.loads(s8.CHECKPOINT_PATH.read_text())
    assert ck["last_day"] == "2026-01-11" and ck["seen"] == ["0000027904-26-000002"]
    monkeypatch.setattr(s8.time, "monotonic", lambda: 0.0)
    second = j.run(today=date(2026, 1, 13), budget_seconds=10)  # from the checkpoint
    assert second.listed_8ks == 1 and second.days == ["2026-01-12"] and not second.stopped_early
    assert json.loads(s8.CHECKPOINT_PATH.read_text())["last_day"] == "2026-01-12"


def test_a_missing_index_is_a_holiday_once_a_later_day_has_one(job, monkeypatch):
    j = job(FakeLLM(reply()), {}, items="2.02")
    missing = {date(2026, 11, 11), date(2026, 11, 13)}  # Veterans Day; then a day not yet published
    monkeypatch.setattr(j, "index_8ks", lambda day: None if day in missing else [])
    s = j.run(since=date(2026, 11, 10), today=date(2026, 11, 14))
    assert s.days == ["2026-11-10", "2026-11-12"] and s.skipped == {"no_index": 2}
    assert json.loads(s8.CHECKPOINT_PATH.read_text())["last_day"] == "2026-11-12"  # waits for the 13th
    s = j.run(today=date(2026, 11, 18))  # the 13th still has no index, now more than 3 days old
    assert json.loads(s8.CHECKPOINT_PATH.read_text())["last_day"] == "2026-11-17"
