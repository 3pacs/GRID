"""Shared fixtures for the 13F holdings-observation path (writer -> display).

``FakeEdgar`` answers the three EDGAR requests the real 13F writer makes
(submissions JSON, filing ``index.json``, infotable XML), so
``InstitutionalFlowsPuller.pull_13f_only`` parses, compares and inserts rows
exactly as it does in production. Only the network is replaced. Every CIK,
CUSIP, ticker and manager name here is synthetic.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from xml.sax.saxutils import escape

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))
# Typed reads (PARSE_DECLTYPES) so readers that call .isoformat() on DATE /
# DATETIME columns see the same Python types psycopg2 returns.
sqlite3.register_converter("DATE", lambda b: date.fromisoformat(b.decode()[:10]))
sqlite3.register_converter("DATETIME", lambda b: datetime.fromisoformat(b.decode()))

# Synthetic identities.
CIK_ALPHA = "9000000001"
CIK_BETA = "9000000002"
ALPHA = "Fixture Capital Alpha LP"
BETA = "Fixture Partners Beta LLC"
FILERS = {CIK_ALPHA: ALPHA, CIK_BETA: BETA}

ROOT_CUSIP, ROOT = "FXROOT001", "ROOTX"
PEER_A_CUSIP, PEER_A = "FXPEERA02", "PEERA"
PEER_B_CUSIP, PEER_B = "FXPEERB03", "PEERB"
UNMAPPED_CUSIP = "FXUNMAP04"
IDENTIFIERS = {ROOT_CUSIP: ROOT, PEER_A_CUSIP: PEER_A, PEER_B_CUSIP: PEER_B}


# EDGAR's current namespaces (information table; 13F-HR cover page).
INFOTABLE_NS = "http://www.sec.gov/edgar/document/thirteenf/informationtable"
COVER_NS = "http://www.sec.gov/edgar/thirteenffiler"


@dataclass
class Filing:
    accession: str
    filing_date: date
    report_date: date
    # cusip -> (issuer name, value in whole USD, shares)
    holdings: dict[str, tuple[str, int, int]] = field(default_factory=dict)
    form: str = "13F-HR"
    # Negative-control knobs: "ok" | "missing" | "malformed" | "truncated" (infotable)
    infotable: str = "ok"
    cover: str = "ok"  # "ok" | "missing" | "malformed"
    amendment_type: str = "RESTATEMENT"  # what the cover page says for a /A form
    cover_entry_total: int | None = None  # override the cover's tableEntryTotal


class _Resp:
    def __init__(self, *, payload: Any = None, content: bytes = b"", status: int = 200) -> None:
        self._payload = payload
        self.content = content
        self.status_code = status

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeEdgar:
    """Serves registered filings; ``today`` hides filings not yet filed."""

    def __init__(self) -> None:
        self.filings: dict[str, list[Filing]] = {}
        self.today: date = date.today()

    def add(self, cik: str, filing: Filing) -> None:
        self.filings.setdefault(cik, []).append(filing)

    def _visible(self, cik: str) -> list[Filing]:
        rows = [f for f in self.filings.get(cik, []) if f.filing_date <= self.today]
        return sorted(rows, key=lambda f: f.filing_date, reverse=True)

    def get(self, url: str, headers: Any = None, timeout: Any = None) -> _Resp:
        if url.startswith("https://data.sec.gov/submissions/CIK"):
            cik = url.rsplit("CIK", 1)[1].split(".json", 1)[0].lstrip("0")
            match = next((c for c in self.filings if c.lstrip("0") == cik), None)
            if match is None:
                return _Resp(status=404)
            rows = self._visible(match)
            return _Resp(payload={"filings": {"recent": {
                "form": [f.form for f in rows],
                "accessionNumber": [f.accession for f in rows],
                "filingDate": [f.filing_date.isoformat() for f in rows],
                "reportDate": [f.report_date.isoformat() for f in rows],
                "primaryDocument": ["xslForm13F_X02/primary_doc.xml" for _ in rows],
            }}})
        for cik, rows in self.filings.items():
            for f in rows:
                base = f"https://www.sec.gov/Archives/edgar/data/{cik}/{f.accession.replace('-', '')}"
                if url == f"{base}/index.json":
                    items = [{"name": f"{f.accession}-index.html"}]
                    if f.cover != "missing":
                        items.append({"name": "primary_doc.xml"})
                    if f.infotable != "missing":
                        items.append({"name": "infotable.xml"})
                    return _Resp(payload={"directory": {"item": items}})
                if url == f"{base}/primary_doc.xml" and f.cover != "missing":
                    return _Resp(content=b"<edgarSubmission><headerData>" if f.cover == "malformed"
                                 else _cover_xml(f))
                if url == f"{base}/infotable.xml" and f.infotable != "missing":
                    if f.infotable == "malformed":
                        return _Resp(content=b"<informationTable><infoTable><cusip>X")
                    listed = dict(list(f.holdings.items())[:-1]) if f.infotable == "truncated" else f.holdings
                    return _Resp(content=_infotable_xml(listed))
        return _Resp(status=404)


def _cover_date(d: date) -> str:
    return d.strftime("%m-%d-%Y")


def _cover_xml(f: Filing) -> bytes:
    is_amendment = f.form.endswith("/A")
    total = len(f.holdings) if f.cover_entry_total is None else f.cover_entry_total
    amendment = (f"<amendmentInfo><amendmentType>{f.amendment_type}</amendmentType></amendmentInfo>"
                 if is_amendment else "")
    return (
        f'<edgarSubmission xmlns="{COVER_NS}" xmlns:com="http://www.sec.gov/edgar/common">'
        f"<headerData><submissionType>{f.form}</submissionType>"
        f"<filerInfo><periodOfReport>{_cover_date(f.report_date)}</periodOfReport></filerInfo></headerData>"
        f"<formData><coverPage><reportCalendarOrQuarter>{_cover_date(f.report_date)}</reportCalendarOrQuarter>"
        f"<isAmendment>{'true' if is_amendment else 'false'}</isAmendment>{amendment}</coverPage>"
        f"<summaryPage><otherIncludedManagersCount>0</otherIncludedManagersCount>"
        f"<tableEntryTotal>{total}</tableEntryTotal>"
        f"<tableValueTotal>{sum(v for _n, v, _s in f.holdings.values())}</tableValueTotal>"
        f"<isConfidentialOmitted>false</isConfidentialOmitted></summaryPage></formData>"
        "</edgarSubmission>"
    ).encode()


def _infotable_xml(holdings: dict[str, tuple[str, int, int]]) -> bytes:
    parts = [f'<informationTable xmlns="{INFOTABLE_NS}">']
    for cusip, (name, value, shares) in holdings.items():
        parts.append(
            "<infoTable>"
            f"<nameOfIssuer>{escape(name)}</nameOfIssuer>"
            f"<cusip>{cusip}</cusip>"
            f"<value>{value}</value>"
            f"<shrsOrPrnAmt><sshPrnamt>{shares}</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt>"
            "</infoTable>"
        )
    parts.append("</informationTable>")
    return "".join(parts).encode()


def run_writer(engine: Engine, monkeypatch: Any, edgar: FakeEdgar, filers: dict[str, str] = FILERS) -> list[dict]:
    """Run the real 13F writer against ``edgar`` (network replaced only)."""
    from ingestion.altdata import institutional_flows

    monkeypatch.setattr(institutional_flows.requests, "get", edgar.get)
    monkeypatch.setattr(institutional_flows.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(institutional_flows, "TOP_13F_FILERS", dict(filers))
    institutional_flows._DEAD_CIKS.clear()
    puller = institutional_flows.InstitutionalFlowsPuller(engine)
    return puller.pull_13f_only()


def quarter_dates(quarters_back: int) -> tuple[date, date]:
    """``(report_date, filing_date)`` for a synthetic quarter ``quarters_back`` ago.

    The newest (0) was filed 10 days ago for a period that ended 55 days ago,
    so it is inside the reader's freshness window whenever the test runs.
    """
    report = date.today() - timedelta(days=55 + 91 * quarters_back)
    return report, report + timedelta(days=45)


# ── Engines ────────────────────────────────────────────────────────────────

_DDL_SQLITE = (
    "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE raw_series (id INTEGER PRIMARY KEY AUTOINCREMENT, series_id TEXT NOT NULL, "
    "source_id INTEGER NOT NULL, obs_date DATE NOT NULL, "
    "pull_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, value REAL NOT NULL, "
    "raw_payload TEXT, pull_status TEXT NOT NULL)",
    "CREATE TABLE signal_sources (id INTEGER PRIMARY KEY AUTOINCREMENT, source_type TEXT NOT NULL, "
    "source_id TEXT NOT NULL, ticker TEXT, signal_date DATE NOT NULL, signal_type TEXT NOT NULL, "
    "signal_value TEXT, trust_score REAL DEFAULT 0.5, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE dollar_flows (id INTEGER PRIMARY KEY AUTOINCREMENT, source_type TEXT NOT NULL, "
    "actor_name TEXT, ticker TEXT, amount_usd REAL NOT NULL, direction TEXT NOT NULL, "
    "confidence TEXT DEFAULT 'estimated', evidence TEXT, flow_date DATE NOT NULL, "
    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE security_identifiers (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT NOT NULL, "
    "id_scheme TEXT NOT NULL, id_value TEXT NOT NULL, valid_from DATE NOT NULL, valid_to DATE, "
    "is_primary BOOLEAN NOT NULL DEFAULT 1, source TEXT NOT NULL DEFAULT 'fixture')",
)

DDL_PG = (
    "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, "
    "source_id INTEGER NOT NULL REFERENCES source_catalog(id), obs_date DATE NOT NULL, "
    "pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), value DOUBLE PRECISION NOT NULL, "
    "raw_payload JSONB, pull_status TEXT NOT NULL)",
    "CREATE TABLE signal_sources (id SERIAL PRIMARY KEY, source_type TEXT NOT NULL, "
    "source_id TEXT NOT NULL, ticker TEXT, signal_date DATE NOT NULL, signal_type TEXT NOT NULL, "
    "signal_value JSONB, trust_score NUMERIC DEFAULT 0.5, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    "CREATE TABLE dollar_flows (id SERIAL PRIMARY KEY, source_type TEXT NOT NULL, actor_name TEXT, "
    "ticker TEXT, amount_usd NUMERIC NOT NULL, direction TEXT NOT NULL, "
    "confidence TEXT DEFAULT 'estimated', evidence JSONB, flow_date DATE NOT NULL, "
    "created_at TIMESTAMPTZ DEFAULT NOW())",
    "CREATE TABLE security_identifiers (id BIGSERIAL PRIMARY KEY, entity_id TEXT NOT NULL, "
    "id_scheme TEXT NOT NULL, id_value TEXT NOT NULL, valid_from DATE NOT NULL, valid_to DATE, "
    "is_primary BOOLEAN NOT NULL DEFAULT TRUE, source TEXT NOT NULL DEFAULT 'fixture')",
)


def seed_base(engine: Engine, identifiers: dict[str, str] = IDENTIFIERS) -> None:
    """Source catalog row for the writer plus CUSIP -> ticker identities."""
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (7, 'INSTITUTIONAL_FLOWS')"))
        for i, (cusip, ticker) in enumerate(sorted(identifiers.items())):
            entity = f"FXENT{i}"
            for scheme, value in (("cusip", cusip), ("ticker", ticker)):
                conn.execute(
                    text(
                        "INSERT INTO security_identifiers (entity_id, id_scheme, id_value, valid_from) "
                        "VALUES (:e, :s, :v, :f)"
                    ),
                    {"e": entity, "s": scheme, "v": value, "f": date(2000, 1, 1)},
                )


def make_sqlite_engine() -> Engine:
    engine = create_engine("sqlite://", connect_args={"detect_types": sqlite3.PARSE_DECLTYPES})
    with engine.begin() as conn:
        for ddl in _DDL_SQLITE:
            conn.execute(text(ddl))
    seed_base(engine)
    return engine


def seed_upstream_layers(engine: Engine, root: str, connected: list[str]) -> float:
    """Two rows that make the real layers 5 -> 7 reach ``connected`` from ``root``.

    A campaign-finance row (layer 5: politician) and a ``LEGISLATION:`` row
    the politician sponsors whose affected tickers are ``connected`` (layers
    6 and 7). Returns the campaign amount, the only real dollar flow.
    """
    amount = 2_500.0
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES ('campaign_finance', 'fixture_pac', :t, :d, 'CONTRIBUTION', :v)"
            ),
            {"t": root, "d": date.today() - timedelta(days=30),
             "v": json.dumps({"recipient_name": "Rep Fixture", "amount": amount})},
        )
        conn.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                "VALUES ('LEGISLATION:HR-FIX-1', 7, :d, 1, :p, 'SUCCESS')"
            ),
            {"d": date.today() - timedelta(days=20),
             "p": json.dumps({"bill_id": "HR-FIX-1", "title": "Fixture Act", "sponsor": "Rep Fixture",
                              "affected_tickers": connected})},
        )
    return amount


def two_quarter_scenario() -> FakeEdgar:
    """Alpha holds ROOT (new) and PEER_A (increased), closed PEER_B, and
    newly holds an unmapped CUSIP. Beta newly holds ROOT and closed PEER_A,
    so Beta holds no connected company."""
    prev_report, prev_filed = quarter_dates(1)
    curr_report, curr_filed = quarter_dates(0)
    edgar = FakeEdgar()
    edgar.add(CIK_ALPHA, Filing("9000000001-26-000001", prev_filed, prev_report, {
        PEER_A_CUSIP: ("PEER A CORP", 50_000, 500),
        PEER_B_CUSIP: ("PEER B INC", 30_000, 300),
    }))
    edgar.add(CIK_ALPHA, Filing("9000000001-26-000002", curr_filed, curr_report, {
        ROOT_CUSIP: ("ROOT HOLDINGS INC", 260_000, 2_000),
        PEER_A_CUSIP: ("PEER A CORP", 120_000, 1_000),
        UNMAPPED_CUSIP: ("APPLE INC", 10_000, 100),
    }))
    edgar.add(CIK_BETA, Filing("9000000002-26-000001", prev_filed, prev_report, {
        PEER_A_CUSIP: ("PEER A CORP", 40_000, 400),
    }))
    edgar.add(CIK_BETA, Filing("9000000002-26-000002", curr_filed, curr_report, {
        ROOT_CUSIP: ("ROOT HOLDINGS INC", 75_000, 600),
    }))
    return edgar


def set_receipt(engine: Engine, obs_date: date, received_at: datetime) -> int:
    """Move every 13F change row and filing receipt filed on ``obs_date`` to a
    known receipt instant. Returns the number of change rows moved."""
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE raw_series SET pull_timestamp = :ts WHERE series_id LIKE '13F-FILING:%' AND obs_date = :d"),
            {"ts": received_at, "d": obs_date},
        )
        return conn.execute(
            text("UPDATE raw_series SET pull_timestamp = :ts WHERE series_id LIKE '13F:%' AND obs_date = :d"),
            {"ts": received_at, "d": obs_date},
        ).rowcount


def series_rows(engine: Engine, pattern: str) -> list[tuple[str, Any]]:
    """``(series_id, payload)`` of every raw_series row matching ``pattern``."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT series_id, raw_payload FROM raw_series WHERE series_id LIKE :p ORDER BY id"),
            {"p": pattern},
        ).fetchall()
    return [(r[0], r[1] if isinstance(r[1], dict) else json.loads(r[1])) for r in rows]
