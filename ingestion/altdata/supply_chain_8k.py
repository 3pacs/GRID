"""Supplier/customer links from 8-K Item 1.01 filings by US-listed companies (3pacs/GRID#1180 slice 2).

Measured before building (read-only, 2026-11-13): over 365 days the graph's
211 SEC-registered tradable companies filed 2,360 8-Ks, 230 with Item 1.01
("Entry into a Material Definitive Agreement"); 189 of those were loans,
notes, mergers or other financing, and about six named a real supplier or
customer (Delta and Boeing, Delta and Airbus, Venture Global and GE Vernova,
Akamai and Anthropic). GRID's own ``news_articles`` 8-K feed carries about a
fifth of EDGAR's 8-Ks and none of its 30-day Item 1.01 filings touched the
graph. Watching only the graph's own filers would also mean the graph can
never gain a company. So this watches every US-listed SEC filer (the SEC's
own ticker list, about 10,000 companies): EDGAR's daily form index is one
request a day whatever the universe, so cost grows with the filings opened,
not with the companies watched. A filer new to the graph becomes a
``ticker`` node; an unlisted counterparty a ``private_company`` node.

Dry run 2026-10-14..11-13 (21 business days, read-only, gridz4's 27B): 7,987
listed filers, 6,488 8-Ks, 762 with Item 1.01, 444 financing-only and 210
empty or cross-referenced (no model call), 107 model calls, 52 links kept,
about 71 s per day. Read by hand, about 40 of the 52 were real supply,
customer, distribution or licence deals; the misses (sponsor office space, a
paid promoter, patent purchases, transition services to a sold business) are
now excluded in the prompt. Only 3 touched an existing graph node: the graph
grows mostly at its edges.

Daily pass, per business day since the checkpoint:

1. one request for EDGAR's daily form index; keep 8-Ks whose filer CIK has a
   US-listed symbol (graph companies first, then the SEC ticker list; each
   symbol passes :func:`intelligence.supply_chain_symbols.symbol_for`);
2. the filer's submissions JSON gives the filing's items and primary
   document; only Item 1.01 filings are read;
3. the Item 1.01 section is cut out and classified by keywords; financing-only
   and empty sections stop here (no model call);
4. a local GPU model (:data:`LOCAL_PROVIDERS`, never a paid or cloud
   provider, whatever ``GRID_ALLOW_PAID_LLM`` says) names who supplies whom,
   with a verbatim quote;
5. a link is kept only if the quote is in the filing, one side is the filer
   and the counterparty is named in the filing and is not a lender, investor
   or bank;
6. kept links upsert as confidence ``derived`` edges citing the filing URL;
   contagion uses them only after the edge validator has measured them
   (``chain_contagion.VALIDATED_DERIVED_GATE``).

Writes nothing unless ``write=True``. The checkpoint (last day read, the
accessions whose outcome is final, and retry counts) is a JSON file written
atomically, and only by a write run: a dry run starts from it and leaves it
as it was, so a dry run never hides filings from the next write run.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata import supply_chain_parser as scp
from intelligence.supply_chain_symbols import symbol_for

FORM_INDEX_URL = "https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/form.{ymd}.idx"
QUARTER_LISTING_URL = "https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/index.json"
CHECKPOINT_PATH = Path(os.environ.get(
    "GRID_SUPPLY_CHAIN_8K_CHECKPOINT",
    str(Path(__file__).resolve().parents[2] / "outputs" / "supply_chain" / "8k_checkpoint.json"),
))
FIRST_RUN_LOOKBACK_DAYS = 7
MAX_DAYS_PER_RUN = 30
SEEN_KEEP = 5000
MAX_ATTEMPTS = 3  # a filing that cannot be read is retried on this many runs, then given up (counted)
SECTION_CHARS = 8000
# GPU nodes only, in router order. Paid providers are never in this list, so
# no setting can send a filing to a cloud model from here.
LOCAL_PROVIDERS: tuple[str, ...] = ("llamacpp_z4", "llamacpp_oracle", "llamacpp", "llamacpp_x99", "ollama")

_INDEX_LINE = re.compile(r"^(8-K(?:/A)?)\s+(.+?)\s+(\d{1,10})\s+(\d{8})\s+(edgar/data/\S+\.txt)\s*$")
_ITEM_101 = re.compile(r"item\s*1\.01\b(.*?)(?=item\s*\d\.\d\d\b|signatures?\b|$)", re.I | re.S)
COMMERCIAL = re.compile(
    r"\b(supply|supplier|manufactur\w*|offtake|off-take|customer|collaboration|licen[cs]e agreement|"
    r"master services|services agreement|development agreement|sourcing|procurement|reseller|wafer|"
    r"capacity reservation|purchase commitment|long-term agreement|purchase \d[\d,]* \w+ aircraft|"
    r"to (?:purchase|acquire) \d+ \w+)\b", re.I)
FINANCING = re.compile(
    r"\b(credit agreement|indenture|notes due|senior notes|term loan|loan agreement|revolving|underwriting|"
    r"merger|equity distribution|at-the-market|securities purchase|warrant|employment agreement|separation|"
    r"commercial paper|share repurchase|accelerated share|receivables purchase|registration rights|"
    r"stockholders agreement|support agreement|cooperation agreement)\b", re.I)
_FINANCIAL_PARTY = re.compile(
    r"\b(bank|bancorp|securities|capital|lenders?|investors?|underwriters?|trust company|"
    r"agent|holders?|purchasers?|fund|partners lp|financial)\b", re.I)
KIND_RELATIONSHIP = {"supply": "component", "services": "component", "manufacturing": "contract_mfg",
                     "distribution": "distribution", "license": "licensor"}
_THINK = re.compile(r"<think>.*?</think>", re.S | re.I)
_LEGAL_SUFFIX = re.compile(
    r"\b(inc|incorporated|corp|corporation|co|company|companies|ltd|limited|plc|llc|lp|l p|holdings?|group|"
    r"s a s|sas|s a|sa|n v|nv|ag|se|pbc|the)\b")

PROMPT_SYSTEM = (
    "You read the Item 1.01 section of an SEC 8-K. Reply with ONE JSON object and nothing else: "
    '{"links": [{"supplier": "...", "customer": "...", "kind": "...", "provides": "...", "quote": "..."}]}. '
    "A link is a commercial agreement in which one party sells goods or services to the other: "
    "supply, purchase of products or equipment, manufacturing, services, licensing of a product. "
    "kind is one of: supply, manufacturing, distribution, services, license. The supplier is the party "
    "that sells, makes, distributes for, serves or licenses to the other. "
    "Write FILER for the company that filed the 8-K or any of its subsidiaries. One side of every link "
    "must be FILER and the other side a named company. "
    "Do NOT report loans, notes, credit facilities, underwriting, share sales, mergers, acquisitions of a "
    "company or business, purchases or assignments of patents or other assets, leases, employment, office "
    "space or administrative services, transition services to a business being sold or spun off, investor "
    "relations, promotion, M&A or capital-markets advice, or agreements with investors, lenders or banks. "
    "provides: what the supplier sells, at most eight words. quote: an exact sentence copied from the text "
    'that shows the link. If there is no link, reply {"links": []}.'
)


@dataclass
class Filing:
    cik: str
    symbol: str
    node_id: str
    company: str
    accession: str
    filing_date: date
    url: str = ""
    items: str = ""
    in_graph: bool = True


@dataclass
class Link:
    supplier: str
    customer: str
    provides: str
    quote: str
    kind: str = ""


@dataclass
class RunSummary:
    days: list[str] = field(default_factory=list)
    universe: int = 0
    listed_8ks: int = 0
    graph_8ks: int = 0
    item_101: int = 0
    skipped: dict[str, int] = field(default_factory=dict)
    model_calls: int = 0
    provider: str | None = None
    rejected: dict[str, int] = field(default_factory=dict)
    links: list[dict[str, Any]] = field(default_factory=list)
    edges_written: int = 0
    nodes_written: int = 0
    write: bool = False
    stopped_early: bool = False
    checkpoint_advanced: bool = False
    gave_up: int = 0  # filings in the checkpoint's gave_up list after this run

    def bump(self, bucket: dict[str, int], key: str) -> None:
        bucket[key] = bucket.get(key, 0) + 1


class IndexRefused(RuntimeError):
    """EDGAR did not give a usable answer for a daily index: not proof of a holiday."""


class UniverseUnavailable(RuntimeError):
    """The SEC ticker list could not be read, so no filer can be recognised."""


def parse_ticker_list(resp: Any) -> list[tuple[str, str, str]]:
    """``[(symbol, cik10, title)]`` from a company_tickers.json response; raises on anything odd.

    The payload must be a non-empty object whose every value has a non-empty
    ``ticker`` and a positive integer ``cik_str``. An empty, null or malformed
    body raises :class:`UniverseUnavailable`, so the pass stops instead of
    reading days with no recognisable filer.
    """
    try:
        payload = resp.json()
    except ValueError as exc:
        raise UniverseUnavailable(f"SEC ticker list: not JSON ({exc})") from exc
    if not isinstance(payload, dict) or not payload:
        raise UniverseUnavailable(f"SEC ticker list: empty or not an object ({type(payload).__name__})")
    out = []
    for key, e in payload.items():
        ticker = e.get("ticker") if isinstance(e, dict) else None
        cik = e.get("cik_str") if isinstance(e, dict) else None
        if not isinstance(ticker, str) or not ticker.strip() or isinstance(cik, bool) \
                or not str(cik).isdigit() or int(cik) <= 0:
            raise UniverseUnavailable(f"SEC ticker list: malformed entry {key!r}")
        out.append((ticker.strip().upper(), str(int(cik)).zfill(10), str(e.get("title") or "")))
    return out


def company_key(name: str) -> str:
    """Comparable company name: ``The Boeing Company`` and ``BOEING CO`` both give ``boeing``."""
    s = re.sub(r"[^a-z0-9&]+", " ", (name or "").lower().replace("\u2019", "'").replace("'s", "s"))
    return re.sub(r"\s+", " ", _LEGAL_SUFFIX.sub(" ", s)).strip()


def _norm(s: str) -> str:
    s = (s or "").replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    return re.sub(r"\s+", " ", s).strip().lower()


def item_101_section(body: str) -> str:
    """The Item 1.01 section of a filing's text ("" when absent)."""
    m = _ITEM_101.search(body or "")
    return (m.group(1) if m else "").strip()[:SECTION_CHARS]


def classify_section(section: str) -> str:
    """``commercial`` / ``mixed`` / ``financing`` / ``other``; only the first two reach the model."""
    com, fin = bool(COMMERCIAL.search(section)), bool(FINANCING.search(section))
    if com:
        return "mixed" if fin else "commercial"
    return "financing" if fin else "other"


def parse_index(raw: str) -> list[tuple[str, str, str]]:
    """``[(cik10, company, accession)]`` for 8-K lines of an EDGAR daily form index."""
    out = []
    for line in (raw or "").splitlines():
        m = _INDEX_LINE.match(line.rstrip())
        if not m or m.group(1) != "8-K":
            continue
        acc = Path(m.group(5)).stem
        out.append((m.group(3).zfill(10), m.group(2).strip(), acc))
    return out


def parse_links(raw: str | None) -> list[Link] | None:
    """Model reply -> links; ``None`` when the reply is not the JSON asked for."""
    if not raw:
        return None
    body = _THINK.sub("", raw)
    start, end = body.find("{"), body.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        data = json.loads(body[start:end + 1])
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict) or not isinstance(data.get("links"), list):
        return None
    links = []
    for item in data["links"]:
        if not isinstance(item, dict):
            continue
        vals = [str(item.get(k) or "").strip() for k in ("supplier", "customer", "provides", "quote", "kind")]
        links.append(Link(*vals[:4], kind=vals[4].lower()))
    return links


def validate_link(link: Link, section: str) -> tuple[str | None, str]:
    """``(counterparty, "filer_supplies" | "filer_buys")`` or ``(None, reject_reason)``."""
    sup, cus = link.supplier.upper() == "FILER", link.customer.upper() == "FILER"
    if sup == cus:
        return None, "filer_not_one_side"
    other = re.sub(r"^the\s+", "", scp._clean_name(link.customer if sup else link.supplier), flags=re.I)
    if not other or scp._is_noise_name(other):
        return None, "counterparty_noise"
    if _FINANCIAL_PARTY.search(other):
        return None, "financial_counterparty"
    text_n = _norm(section)
    if not link.quote or _norm(link.quote) not in text_n:
        return None, "quote_not_in_filing"
    if _norm(other) not in text_n:
        return None, "counterparty_not_in_filing"
    return other, "filer_supplies" if sup else "filer_buys"


class EightKSupplyLinks:
    """One daily pass over EDGAR 8-Ks by US-listed companies (see the module docstring)."""

    def __init__(self, engine: Engine, parser: scp.SupplyChain10KParser | None = None,
                 llm: Any | None = None) -> None:
        self.engine = engine
        self.parser = parser or scp.SupplyChain10KParser(db_engine=engine)
        self._llm = llm
        self._provider_name: str | None = "injected" if llm is not None else None
        self._submissions: dict[str, dict[str, Any]] = {}
        self._universe: dict[str, tuple[str, str, bool]] = {}
        self._titles: dict[str, str] = {}  # cik -> SEC company title
        self._by_name: dict[str, str] = {}  # company_key(title) -> cik
        self._graph_names: dict[str, str] = {}  # company_key(node name) -> node id

    # ── universe and EDGAR reads ──────────────────────────────────────────

    def sec_listing(self) -> list[tuple[str, str, str]]:
        """``[(symbol, cik10, title)]`` from the SEC ticker list, in the SEC's order."""
        resp = self._get(scp._COMPANY_TICKERS_URL)
        if resp is None:
            raise UniverseUnavailable(f"SEC ticker list: status {getattr(self, '_last_status', None)}")
        return parse_ticker_list(resp)

    def filer_universe(self) -> dict[str, tuple[str, str, bool]]:
        """``{cik10: (symbol, node_id, in_graph)}``: graph companies, then every SEC-listed symbol.

        A graph company keeps its node id (``tsmc``); any other filer's node id
        is its lower-cased symbol, the seed convention for ``ticker`` nodes.
        A CIK with several symbols (share classes, warrants, units) takes its
        shortest, earliest-listed one: ``XLAB`` over ``XLABW``.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT id, type, name FROM supply_chain_nodes")).fetchall()
        listing = self.sec_listing()
        cik_of = {sym.replace(".", "-"): cik for sym, cik, _ in listing}
        out: dict[str, tuple[str, str, bool]] = {}
        known = set()
        for node_id, node_type, name in rows:
            nid = str(node_id).lower()
            known.add(nid)
            if name:
                self._graph_names.setdefault(company_key(name), nid)
            sym = symbol_for(nid, node_type).symbol
            if sym and cik_of.get(sym):
                out.setdefault(cik_of[sym], (sym, nid, True))
        listed: dict[str, str] = {}
        for tkr, cik, title in listing:
            self._titles.setdefault(cik, title)
            self._by_name.setdefault(company_key(title), cik)
            sym = symbol_for(tkr.lower().replace(".", "-"), "ticker").symbol
            if sym and cik not in out and (cik not in listed or len(sym) < len(listed[cik])):
                listed[cik] = sym
        for cik, sym in listed.items():
            out[cik] = (sym, sym.lower(), sym.lower() in known)
        if not out:
            raise UniverseUnavailable("no filer could be recognised from the SEC ticker list")
        self._universe = out
        return out

    def resolve_counterparty(self, name: str) -> tuple[str, scp.DerivedNode | None]:
        """Graph node by name, else an SEC-listed company (its node, or a new ``ticker`` node), else private."""
        key = company_key(name)
        node = self._graph_names.get(key) or _parent(key, self._graph_names)
        if node:
            return node, None
        cik = self._by_name.get(key) or _parent(key, self._by_name)
        if cik and cik in self._universe:
            sym, nid, in_graph = self._universe[cik]
            return nid, None if in_graph else scp.DerivedNode(id=nid, name=_display(self._titles.get(cik, name)),
                                                              type="ticker")
        return self.parser._resolve_counterparty(name)

    def _get(self, url: str) -> Any | None:
        self._last_status = None  # None: no answer at all
        try:
            resp = self.parser.session.get(url, timeout=scp._REQUEST_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 - network errors end this read, not the run
            log.warning("supply_chain_8k: GET failed {u}: {e}", u=url, e=str(exc))
            return None
        finally:
            time.sleep(scp._RATE_LIMIT_SLEEP)
        self._last_status = resp.status_code
        if resp.status_code in (403, 404):  # weekends and holidays have no index
            return None
        if resp.status_code >= 400:
            log.warning("supply_chain_8k: HTTP {s} {u}", s=resp.status_code, u=url)
            return None
        return resp

    def index_8ks(self, day: date) -> list[tuple[str, str, str]] | None:
        """The day's 8-K lines; None when the request got 403/404.

        Any other failure raises :class:`IndexRefused`. A 403 is also what a
        rate-limit refusal looks like, so None is not proof of a holiday:
        :meth:`index_absent` decides that from the quarter's listing.
        """
        q = (day.month - 1) // 3 + 1
        resp = self._get(FORM_INDEX_URL.format(y=day.year, q=q, ymd=day.strftime("%Y%m%d")))
        if resp is not None:
            return parse_index(resp.text)
        if getattr(self, "_last_status", None) in (403, 404):
            return None
        raise IndexRefused(f"{day.isoformat()}: status {getattr(self, '_last_status', None)}")

    def quarter_listing(self, y: int, q: int) -> set[str] | None:
        """File names in EDGAR's daily-index directory for a quarter; None if not fetched (200 only)."""
        key = (y, q)
        cache = self.__dict__.setdefault("_listings", {})
        if key not in cache:
            resp = self._get(QUARTER_LISTING_URL.format(y=y, q=q))
            if resp is None:
                return None  # not cached: a failed fetch is asked again later
            items = ((resp.json() or {}).get("directory", {}) or {}).get("item", []) or []
            cache[key] = {str(it.get("name", "")) for it in items}
        return cache[key]

    def index_absent(self, day: date) -> bool:
        """True only on positive evidence that ``day`` has no index: a 200 quarter
        listing without ``form.<day>.idx`` that shows a later day's form index
        (EDGAR publishes in order). False means wait: the 403/404 may be a refusal,
        or the day is not published yet. A listing that could not be read raises.
        """
        q = (day.month - 1) // 3 + 1
        names = self.quarter_listing(day.year, q)
        if names is None:
            raise IndexRefused(f"{day.isoformat()}: quarter listing unavailable")
        if f"form.{day:%Y%m%d}.idx" in names:
            return False  # listed: the 403/404 was a refusal
        later = [n for n in names if n.startswith("form.") and n.endswith(".idx") and n[5:13] > f"{day:%Y%m%d}"]
        if later:
            return True
        ny, nq = (day.year + 1, 1) if q == 4 else (day.year, q + 1)
        nxt = self.quarter_listing(ny, nq)  # last weekday of a quarter: the next quarter is the evidence
        return bool(nxt and any(n.startswith("form.") and n.endswith(".idx") for n in nxt))

    def filing_details(self, filing: Filing) -> bool:
        """Fill items and URL from the filer's submissions JSON; False when not listed."""
        sub = self._submissions.get(filing.cik)
        if sub is None:
            resp = self._get(scp._SUBMISSIONS_URL.format(cik=filing.cik))
            if resp is None:
                return False  # not cached: a failed fetch is retried, not remembered
            sub = resp.json().get("filings", {}).get("recent", {})
            self._submissions[filing.cik] = sub
        accs = sub.get("accessionNumber", [])
        if filing.accession not in accs:
            return False
        i = accs.index(filing.accession)
        filing.items = (sub.get("items") or [""] * len(accs))[i] or ""
        doc = (sub.get("primaryDocument") or [""] * len(accs))[i]
        filing.url = self.parser._filing_url(filing.cik, {"accession": filing.accession, "primary_doc": doc})
        return bool(doc)

    def filing_text(self, filing: Filing) -> str | None:
        doc = filing.url.rsplit("/", 1)[-1]
        return self.parser._fetch_10k_text(filing.cik, {"accession": filing.accession, "primary_doc": doc})

    # ── model ─────────────────────────────────────────────────────────────

    def _client(self) -> Any | None:
        if self._llm is not None:
            return self._llm
        from llm.router import get_provider_client

        for name in LOCAL_PROVIDERS:
            client = get_provider_client(name)
            if client is not None:
                self._llm, self._provider_name = client, name
                return client
        return None

    def extract(self, filing: Filing, section: str) -> list[Link] | None:
        client = self._client()
        if client is None:
            return None
        prompt = f"Filer: {filing.company} ({filing.symbol}).\n\nItem 1.01 text:\n{section}"
        raw = client.chat(
            [{"role": "system", "content": PROMPT_SYSTEM}, {"role": "user", "content": prompt}],
            temperature=0.0, num_predict=700,
        )
        return parse_links(raw)

    # ── edges ─────────────────────────────────────────────────────────────

    def edge_for(self, filing: Filing, counterparty: str, direction: str,
                 link: Link) -> tuple[scp.DerivedEdge, scp.DerivedNode | None] | None:
        other_id, new_node = self.resolve_counterparty(counterparty)
        if not other_id or other_id == filing.node_id:
            return None
        if direction == "filer_supplies":
            upstream, downstream = filing.node_id, other_id
        else:
            upstream, downstream = other_id, filing.node_id
        # what flows upstream -> downstream, whichever party filed, so a deal
        # both parties report lands on the same edge key
        rel = KIND_RELATIONSHIP.get(link.kind) or scp._infer_relationship(link.provides, None, "")
        edge = scp.DerivedEdge(
            upstream_id=upstream, downstream_id=downstream, relationship=rel,
            source=f"8-K {filing.filing_date.isoformat()} {filing.symbol} Item 1.01 {filing.url}",
            as_of=filing.filing_date, input_type=(link.provides or None) and link.provides[:80],
            raw_name=counterparty,
        )
        return edge, new_node

    # ── checkpoint ────────────────────────────────────────────────────────

    @staticmethod
    def load_checkpoint() -> dict[str, Any]:
        try:
            return json.loads(CHECKPOINT_PATH.read_text())
        except (OSError, ValueError):
            return {}

    @staticmethod
    def save_checkpoint(last_day: date, seen: list[str], attempts: dict[str, int] | None = None,
                        gave_up: list[dict[str, Any]] | None = None) -> None:
        payload = {"last_day": last_day.isoformat(), "seen": seen[-SEEN_KEEP:],
                   "attempts": dict(attempts or {}), "gave_up": list(gave_up or []),  # never truncated: each is a recoverable failure
                   "updated_at": datetime.now(timezone.utc).isoformat()}
        CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = CHECKPOINT_PATH.with_name(CHECKPOINT_PATH.name + ".tmp")
        tmp.write_text(json.dumps(payload, indent=1))
        tmp.replace(CHECKPOINT_PATH)

    def days_to_read(self, today: date, since: date | None) -> list[date]:
        if since is None:
            last = self.load_checkpoint().get("last_day")
            since = (date.fromisoformat(last) + timedelta(days=1)) if last else today - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)
        days, d = [], since
        while d < today and len(days) < MAX_DAYS_PER_RUN:  # today's index is not published until evening
            if d.weekday() < 5:
                days.append(d)
            d += timedelta(days=1)
        return days

    # ── run ───────────────────────────────────────────────────────────────

    def run(self, write: bool = False, since: date | None = None, today: date | None = None,
            checkpoint: bool = True, budget_seconds: float | None = None,
            retry_gave_up: bool = False) -> RunSummary:
        """Read each business day in order; the checkpoint advances one finished day at a time.

        Only a write run saves the checkpoint; a dry run reads it and leaves it.
        If the SEC ticker list cannot be read the pass stops before any day:
        an empty universe would otherwise skip every filing and finish the day.

        A day whose index request got 403/404 passes only on positive evidence
        (:meth:`index_absent`: the quarter's listing, fetched with a 200, omits
        the day and shows a later one). Otherwise the pass stops with the
        checkpoint before that day; any other index failure stops it too.

        A filing joins ``seen`` only once its outcome is final (no Item 1.01, not
        a commercial section, links checked). One that could not be read
        (submissions or text unavailable, a bad model reply) leaves its day
        unfinished, so the next write run asks again. After ``MAX_ATTEMPTS`` runs
        it is recorded in the checkpoint's ``gave_up`` list (accession, CIK,
        company, day, reason) so the day can pass, and ``retry_gave_up`` reads
        those filings again. With no local model the pass stops and counts
        nothing as tried. With a budget the pass stops between filings.
        """
        started = time.monotonic()
        summary = RunSummary(write=write)
        today = today or datetime.now(timezone.utc).date()
        days = self.days_to_read(today, since)
        try:
            filers = self.filer_universe()
        except UniverseUnavailable as exc:
            summary.bump(summary.skipped, "universe_unavailable")
            log.warning("supply_chain_8k: {e}; nothing read, checkpoint untouched", e=str(exc))
            return summary
        summary.universe = len(filers)
        saved = self.load_checkpoint() if checkpoint else {}
        advance = checkpoint and write
        seen = list(saved.get("seen", []))
        seen_set = set(seen)
        attempts: dict[str, int] = {k: int(v) for k, v in (saved.get("attempts") or {}).items()}
        gave_up: list[dict[str, Any]] = [dict(g) for g in saved.get("gave_up") or []]

        def final(acc: str) -> None:
            seen.append(acc)
            seen_set.add(acc)
            attempts.pop(acc, None)

        def read(cik: str, company: str, acc: str, day: date) -> str | None:
            sym, node_id, in_graph = filers[cik]
            filing = Filing(cik, sym, node_id, company, acc, day, in_graph=in_graph)
            summary.listed_8ks += 1
            summary.graph_8ks += in_graph
            return self._read_filing(filing, summary, write)

        if retry_gave_up:
            kept = []
            for k, g in enumerate(gave_up):
                if g["cik"] not in filers:
                    kept.append(g)
                    continue
                why = read(g["cik"], g["company"], g["accession"], date.fromisoformat(g["day"]))
                if why == "no_model":
                    kept.extend(gave_up[k:])  # untouched: no model, nothing tried
                    break
                if why is None:
                    summary.bump(summary.skipped, "gave_up_recovered")
                else:
                    kept.append({**g, "reason": why, "attempts": int(g.get("attempts", 0)) + 1,
                                 "at": datetime.now(timezone.utc).isoformat()})
            gave_up = kept

        last_saved: date | None = None
        for day in days:
            try:
                listed = self.index_8ks(day)
                if listed is None:
                    if not self.index_absent(day):
                        summary.bump(summary.skipped, "index_not_yet")
                        break  # not published, or a refusal: the checkpoint stays before it
                    summary.bump(summary.skipped, "holiday")
                    listed = []
            except IndexRefused as exc:
                summary.bump(summary.skipped, "index_refused")
                log.warning("supply_chain_8k: index unusable, stopping before {d}: {e}", d=day, e=str(exc))
                break
            finished, stop = True, False
            for cik, company, acc in listed:
                if cik not in filers or acc in seen_set:
                    continue
                if budget_seconds is not None and time.monotonic() - started > budget_seconds:
                    summary.stopped_early, finished = True, False
                    break
                retry = read(cik, company, acc, day)
                if retry is None:
                    final(acc)
                elif retry == "no_model":
                    finished, stop = False, True
                    break
                else:
                    attempts[acc] = attempts.get(acc, 0) + 1
                    if attempts[acc] >= MAX_ATTEMPTS:
                        summary.bump(summary.skipped, f"gave_up_{retry}")
                        gave_up.append({"accession": acc, "cik": cik, "company": company,
                                        "day": day.isoformat(), "reason": retry,
                                        "attempts": attempts[acc],
                                        "at": datetime.now(timezone.utc).isoformat()})
                        final(acc)
                    else:
                        finished = False  # the next run asks for this filing again
            if finished:
                summary.days.append(day.isoformat())
            last_saved = day if finished else day - timedelta(days=1)
            if advance:
                self.save_checkpoint(last_saved, seen, attempts, gave_up)
                summary.checkpoint_advanced = True
            if not finished or stop:
                break
        if advance and last_saved is None and retry_gave_up:
            prior = saved.get("last_day")
            if prior:
                self.save_checkpoint(date.fromisoformat(prior), seen, attempts, gave_up)
                summary.checkpoint_advanced = True
        summary.gave_up = len(gave_up)
        summary.provider = self._provider_name
        return summary

    def _read_filing(self, filing: Filing, summary: RunSummary, write: bool) -> str | None:
        """None when the filing's outcome is final; else why it should be read again."""
        if not self.filing_details(filing):
            summary.bump(summary.skipped, "not_in_submissions")
            return "not_in_submissions"
        if "1.01" not in filing.items:
            summary.bump(summary.skipped, "no_item_101")
            return None
        summary.item_101 += 1
        body = self.filing_text(filing)
        if body is None:
            summary.bump(summary.skipped, "text_unavailable")
            return "text_unavailable"
        section = item_101_section(body)
        kind = classify_section(section) if section else "no_section"
        if kind not in ("commercial", "mixed"):
            summary.bump(summary.skipped, kind)
            return None
        links = self.extract(filing, section)
        if links is None:
            reason = "no_model" if self._llm is None else "bad_reply"
            summary.bump(summary.rejected, reason)
            return reason
        summary.model_calls += 1
        for link in links:
            other, why = validate_link(link, section)
            if other is None:
                summary.bump(summary.rejected, why)
                continue
            made = self.edge_for(filing, other, why, link)
            if made is None:
                summary.bump(summary.rejected, "self_or_unresolved")
                continue
            edge, new_node = made
            summary.links.append({**asdict(edge), "as_of": edge.as_of.isoformat() if edge.as_of else None,
                                  "quote": link.quote[:300], "new_node": new_node is not None})
            filer_node = None if filing.in_graph else scp.DerivedNode(
                id=filing.node_id, name=_display(filing.company), type="ticker", notes=f"added from 8-K {filing.url}")
            summary.links[-1]["new_filer_node"] = filer_node is not None
            if write:
                with self.engine.begin() as conn:
                    for node in (filer_node, new_node):
                        if node is not None and self.parser._upsert_node(conn, node):
                            summary.nodes_written += 1
                    if self.parser._upsert_edge(conn, edge):
                        summary.edges_written += 1


def _parent(key: str, known: dict[str, str]) -> str | None:
    """A subsidiary's parent: the longest known key of two or more words that starts ``key``.

    ``baker hughes energy services`` -> ``baker hughes``. One-word keys never
    match this way (``apple`` must not claim ``apple hospitality reit``).
    """
    words = key.split()
    for n in range(len(words) - 1, 1, -1):
        hit = known.get(" ".join(words[:n]))
        if hit:
            return hit
    return None


def _display(name: str) -> str:
    return name.title() if name.isupper() else name


DAILY_BUDGET_SECONDS = 150  # Hermes gives a daily task 180 s; stop between filings, resume next run


def run_daily(db_engine: Engine, write: bool = True) -> dict[str, Any]:
    """Hermes entry point: read the business days since the checkpoint, within the daily budget."""
    summary = EightKSupplyLinks(db_engine).run(write=write, budget_seconds=DAILY_BUDGET_SECONDS)
    log.info("supply_chain_8k: {n} listed 8-Ks ({g} by graph companies), {i} Item 1.01, {m} model calls, {e} edges",
             n=summary.listed_8ks, g=summary.graph_8ks, i=summary.item_101, m=summary.model_calls,
             e=summary.edges_written)
    return asdict(summary)


def main(argv: list[str] | None = None) -> int:
    import argparse

    from db import get_engine

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--since", type=date.fromisoformat, help="first day to read (default: checkpoint)")
    ap.add_argument("--write", action="store_true", help="upsert edges and advance the checkpoint (default: dry run, checkpoint untouched)")
    ap.add_argument("--no-checkpoint", action="store_true", help="neither read nor advance the checkpoint")
    ap.add_argument("--retry-gave-up", action="store_true",
                    help="read the checkpoint's given-up filings again before the daily pass")
    args = ap.parse_args(argv)
    summary = EightKSupplyLinks(get_engine()).run(write=args.write, since=args.since,
                                                  checkpoint=not args.no_checkpoint,
                                                  retry_gave_up=args.retry_gave_up)
    print(json.dumps(asdict(summary), indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
