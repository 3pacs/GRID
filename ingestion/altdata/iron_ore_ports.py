"""Chinese iron ore port stocks + daily throughput puller (CAT-52).

Why this matters
----------------
Chinese iron ore **port stocks** — the inventory of iron ore sitting at
the 45 survey ports tracked by Mysteel — are the cleanest real-time
read on Chinese steel-production pace. The survey is published weekly
(Thursdays) and is the single most-watched number in the Chinese
ferrous complex. Dalian Commodity Exchange also publishes daily
deliverable stocks under the ``i`` (iron ore) contract.

* **Rising stocks (throughput < imports)** → mills are cutting runs →
  steel demand slowing → bearish BHP / RIO / VALE and the China-
  sensitivity basket. Typical lead-time to commodity spot price:
  5-15 days. Lead-time to sell-side earnings revisions on the three
  majors: 3-8 weeks.
* **Falling stocks (throughput > imports)** → mills tightening → steel
  production accelerating → bullish iron ore, coking coal, HRC, and
  eventually Caterpillar / Komatsu through the construction cycle.

Downstream consumers
--------------------
This puller feeds three canonical downstream modules:

* ``intelligence/sector_networks/commodities_agriculture.yaml`` —
  the ferrous edge weights in the commodities sector_network use the
  ``iron_ore:port_stocks_mt:aggregate`` series as the "China tightness"
  anchor.
* ``intelligence/global_growth_impulse`` classifier — the 4-week
  week-over-week delta of the 45-port stocks is one of eight leading
  indicators in the global industrial impulse model.
* ``intelligence/supply_chain_chokepoint`` — the port-stock series is
  the China-exposure anchor for the BHP / RIO / VALE basket in the
  chokepoint detector.

Data strategy
-------------
akshare wraps several Chinese-commodity data vendors (including the
DCE + some Mysteel re-exposures) and is tried first through a
fallback ladder. If every akshare candidate is missing, the puller
falls back to an HTML scrape of the public Mysteel 45-port survey
page. Every source failure is caught and logged — the puller returns
zero rows and a single warning rather than crashing the scheduler.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

import requests
from bs4 import BeautifulSoup
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Top-10 Chinese iron ore ports by volume (cold-start subset of the
#: Mysteel 45-port survey). The full 45 require an authenticated Mysteel
#: feed; this subset covers ~80% of aggregate throughput and is the set
#: every sell-side China steel desk tracks on a day-to-day basis.
CHINESE_PORTS_45: tuple[str, ...] = (
    "Qingdao",
    "Rizhao",
    "Tianjin",
    "Caofeidian",
    "Lianyungang",
    "Ningbo",
    "Zhanjiang",
    "Jingtang",
    "Beilun",
    "Fangchenggang",
)

#: akshare function names to probe in order. Names are stable-ish across
#: akshare versions but do churn; each candidate is tried via ``getattr``
#: so missing functions are soft failures, not crashes.
AKSHARE_FUNCTION_CANDIDATES: list[str] = [
    "futures_inventory_em",          # DCE/Em inventory feed — includes i (iron ore)
    "futures_inventory_99",          # 99-metals alt inventory feed
    "macro_china_iron_ore_port",     # probable name if akshare re-wraps Mysteel
    "macro_china_iron_ore_stock",    # alt probe
    "iron_ore_port_stock_em",        # alt probe
    "iron_ore_inventory_em",         # alt probe
    "futures_hist_em",               # DCE iron ore price fallback (tightness proxy)
]

#: Public Mysteel 45-port weekly survey URL. Falls back here if akshare
#: has no iron-ore function exposed.
MYSTEEL_URL: str = "https://ihuangye.mysteel.com/"

# Series ID namespaces
SERIES_PORT_STOCKS_AGG: str = "iron_ore:port_stocks_mt:aggregate"
SERIES_PORT_STOCKS_PREFIX: str = "iron_ore:port_stocks_mt:"
SERIES_THROUGHPUT_AGG: str = "iron_ore:daily_throughput_mt:aggregate"
SERIES_DELTA_WOW_AGG: str = "iron_ore:stocks_delta_wow_mt:aggregate"

_REQUEST_TIMEOUT: int = 30
_USER_AGENT: str = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Sentinels that Mysteel / akshare sometimes emit in numeric cells.
_NULL_SENTINELS: frozenset[str] = frozenset(
    {"", "-", "--", "—", "N/A", "n/a", "NA", "None", "null", "nan", "NaN"}
)

_NUMBER_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class IronOrePortSnapshot:
    """One port (or the 45-port aggregate) iron-ore stock observation.

    ``port=None`` represents the aggregate 45-port survey line. All
    tonnage is in **metric tons (mt)**. ``daily_throughput_mt`` is the
    daily discharge/load volume reported alongside the stock level —
    ``None`` when the source only emits the stock figure.
    ``delta_wow_mt`` is the week-over-week change in stocks; computed
    downstream when a prior observation is available.
    """

    date: date
    port: str | None
    total_stocks_mt: float
    daily_throughput_mt: float | None
    delta_wow_mt: float | None


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _parse_float(raw: Any) -> float | None:
    """Coerce a Mysteel / akshare cell to float. Return None on sentinels."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if isinstance(raw, float) and (raw != raw):  # NaN
            return None
        return float(raw)
    txt = str(raw).strip()
    if txt in _NULL_SENTINELS:
        return None
    # Extract first numeric token (handles "12,345.6 万吨" style cells)
    match = _NUMBER_RE.search(txt.replace(" ", ""))
    if match is None:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _parse_date(raw: Any) -> date | None:
    """Parse a date from str / datetime / Timestamp. None on failure."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    txt = str(raw).strip()
    if not txt or txt in _NULL_SENTINELS:
        return None
    fmts = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%d %b %Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    # Chinese-formatted dates: "2026年04月10日"
    m = re.match(r"(\d{4})[年\-/.](\d{1,2})[月\-/.](\d{1,2})", txt)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def compute_wow_delta(
    current_mt: float,
    prior_mt: float | None,
) -> float | None:
    """Return week-over-week stock delta in metric tons.

    Returns ``None`` when the prior observation is missing or zero —
    a zero prior is almost always a data gap rather than a real value
    for the 45-port aggregate, so we suppress it to avoid poisoning the
    downstream delta series.
    """
    if prior_mt is None:
        return None
    if prior_mt == 0:
        return None
    return current_mt - prior_mt


def _load_akshare_function(name: str) -> Callable[..., Any] | None:
    """Return an akshare function by name, or None on any failure.

    Function-local import of ``akshare`` wrapped in
    ``try/except ImportError`` — the module must import cleanly on
    hosts where akshare is not installed. Any attribute-miss or import
    failure returns ``None``, never raises.
    """
    try:
        import akshare as ak  # type: ignore
    except ImportError:
        return None
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("iron_ore_ports: akshare import exploded: {e}", e=str(exc))
        return None
    fn = getattr(ak, name, None)
    if fn is None:
        return None
    if not callable(fn):
        return None
    return fn


# ---------------------------------------------------------------------------
# Mysteel HTML parser
# ---------------------------------------------------------------------------


# Chinese + English header synonyms, lowercased.
_HEADER_SYNONYMS: dict[str, str] = {
    # Port name column
    "港口": "port",
    "port": "port",
    "港名": "port",
    # Stock level column
    "库存": "stocks",
    "港口库存": "stocks",
    "total stocks": "stocks",
    "port stocks": "stocks",
    "stocks": "stocks",
    "iron ore stocks": "stocks",
    "现货库存": "stocks",
    # Throughput column
    "日均疏港量": "throughput",
    "疏港量": "throughput",
    "daily throughput": "throughput",
    "throughput": "throughput",
    "discharge": "throughput",
    # Date column
    "日期": "date",
    "date": "date",
    "week": "date",
    "report date": "date",
    "统计日期": "date",
}


def _normalise_header(raw: str) -> str | None:
    """Return the canonical header kind for a raw table header string.

    Canonical kinds: ``"date"``, ``"port"``, ``"stocks"``, ``"throughput"``
    or ``None`` when the header is unrecognised.
    """
    if raw is None:
        return None
    key = raw.strip().lower().replace("(", " ").replace(")", " ")
    key = re.sub(r"\s+", " ", key).strip()
    # Try full match first
    if key in _HEADER_SYNONYMS:
        return _HEADER_SYNONYMS[key]
    # Try synonym substring match
    for synonym, canonical in _HEADER_SYNONYMS.items():
        if synonym in key:
            return canonical
    return None


def _parse_mysteel_html(html: str) -> list[IronOrePortSnapshot]:
    """Parse the public Mysteel 45-port weekly survey page.

    Walks every ``<table>`` and picks the first one whose header row
    carries a recognisable "stocks" column. Aggregate rows (port name
    containing "45港" / "45-port" / "total" / "aggregate") are emitted
    with ``port=None`` to match the downstream namespace convention.
    Pure function — no network calls. Returns ``[]`` on any parse
    failure.
    """
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("iron_ore_ports: BeautifulSoup init failed: {e}", e=str(exc))
        return []

    # Try to locate a report date anywhere on the page (for tables that
    # don't repeat the date per row).
    page_date: date | None = None
    for span in soup.find_all(["span", "div", "p", "h1", "h2", "h3"]):
        text_val = span.get_text(strip=True)
        if not text_val:
            continue
        dt = _parse_date(text_val)
        if dt is None:
            # Also try Chinese regex embedded anywhere
            m = re.search(r"(\d{4})[年\-/.](\d{1,2})[月\-/.](\d{1,2})", text_val)
            if m:
                try:
                    dt = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                except ValueError:
                    dt = None
        if dt is not None:
            page_date = dt
            break

    snapshots: list[IronOrePortSnapshot] = []

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if header_row is None:
            continue
        header_cells = header_row.find_all(["th", "td"])
        if not header_cells:
            continue
        headers = [c.get_text(strip=True) for c in header_cells]
        col_kinds: list[str | None] = [_normalise_header(h) for h in headers]

        if "stocks" not in col_kinds:
            continue

        port_idx = col_kinds.index("port") if "port" in col_kinds else None
        stocks_idx = col_kinds.index("stocks")
        tp_idx = col_kinds.index("throughput") if "throughput" in col_kinds else None
        date_idx = col_kinds.index("date") if "date" in col_kinds else None

        # Process body rows
        body_rows = table.find_all("tr")[1:]
        for row in body_rows:
            cells = row.find_all(["td", "th"])
            if len(cells) <= stocks_idx:
                continue
            values = [c.get_text(strip=True) for c in cells]

            port_name: str | None = None
            if port_idx is not None and port_idx < len(values):
                raw_port = values[port_idx].strip()
                if raw_port:
                    # Aggregate row detection
                    low = raw_port.lower()
                    if (
                        "45" in raw_port
                        or "合计" in raw_port
                        or "aggregate" in low
                        or "total" in low
                    ):
                        port_name = None
                    else:
                        port_name = raw_port
                else:
                    port_name = None

            stocks = _parse_float(values[stocks_idx])
            if stocks is None:
                continue

            throughput: float | None = None
            if tp_idx is not None and tp_idx < len(values):
                throughput = _parse_float(values[tp_idx])

            row_date: date | None = None
            if date_idx is not None and date_idx < len(values):
                row_date = _parse_date(values[date_idx])
            if row_date is None:
                row_date = page_date
            if row_date is None:
                continue

            snapshots.append(
                IronOrePortSnapshot(
                    date=row_date,
                    port=port_name,
                    total_stocks_mt=stocks,
                    daily_throughput_mt=throughput,
                    delta_wow_mt=None,
                )
            )

        if snapshots:
            break  # first recognised table wins

    return snapshots


# ---------------------------------------------------------------------------
# akshare adapter — tolerates arbitrary DCE / Mysteel DataFrame shapes
# ---------------------------------------------------------------------------


def _pick_column(df_columns: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df_columns:
            return c
    # Loose match
    lowered = {col.lower(): col for col in df_columns}
    for cand in candidates:
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None


def _parse_akshare_df(df: Any) -> list[IronOrePortSnapshot]:
    """Parse a pandas-style DataFrame returned by an akshare candidate.

    Extremely tolerant of column naming since the probe list spans
    several unrelated akshare functions. Returns ``[]`` on anything
    that does not look like an iron-ore inventory payload.
    """
    if df is None:
        return []
    try:
        if getattr(df, "empty", False):
            return []
        records = df.to_dict(orient="records")
    except Exception:
        return []

    cols = list(df.columns) if hasattr(df, "columns") else (
        list(records[0].keys()) if records else []
    )
    if not cols:
        return []

    # Iron-ore relevance filter. DCE futures_inventory_em returns rows
    # keyed on contract name — we only want "iron ore" / "铁矿石" / "i".
    product_col = _pick_column(cols, ["品种", "product", "name", "合约", "symbol"])
    date_col = _pick_column(
        cols, ["date", "日期", "统计日期", "报告日期", "周期", "datetime"]
    )
    stock_col = _pick_column(
        cols,
        [
            "库存",
            "港口库存",
            "总库存",
            "stocks",
            "stock",
            "inventory",
            "库存量",
        ],
    )
    tp_col = _pick_column(
        cols, ["日均疏港量", "疏港量", "throughput", "daily_throughput", "discharge"]
    )
    port_col = _pick_column(cols, ["港口", "port", "港名"])

    if stock_col is None:
        return []

    snapshots: list[IronOrePortSnapshot] = []
    for rec in records:
        # Product relevance
        if product_col is not None:
            prod = str(rec.get(product_col, "")).lower()
            if not any(
                tok in prod
                for tok in ("铁矿", "iron", "i2", "i ", "ironore", "iron_ore")
            ):
                # If the df is keyed on product but none of the rows are
                # iron ore, skip the row. If product_col is absent, trust
                # the caller — the dataframe is presumed iron-ore-only.
                continue

        dt_raw = rec.get(date_col) if date_col else None
        dt = _parse_date(dt_raw) if dt_raw is not None else date.today()
        if dt is None:
            continue

        stocks = _parse_float(rec.get(stock_col))
        if stocks is None:
            continue

        throughput: float | None = None
        if tp_col is not None:
            throughput = _parse_float(rec.get(tp_col))

        port_name: str | None = None
        if port_col is not None:
            raw_port = str(rec.get(port_col, "")).strip()
            if raw_port:
                low = raw_port.lower()
                if (
                    "45" in raw_port
                    or "合计" in raw_port
                    or "aggregate" in low
                    or "total" in low
                ):
                    port_name = None
                else:
                    port_name = raw_port

        snapshots.append(
            IronOrePortSnapshot(
                date=dt,
                port=port_name,
                total_stocks_mt=stocks,
                daily_throughput_mt=throughput,
                delta_wow_mt=None,
            )
        )

    return snapshots


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------


def _http_get(url: str) -> str | None:
    """Minimal HTTP GET wrapper. Returns page text or None on failure.

    Kept as a top-level function so tests can patch it cleanly.
    """
    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        log.warning("iron_ore_ports HTTP GET failed for {u}: {e}", u=url, e=str(exc))
        return None


# ---------------------------------------------------------------------------
# Puller class
# ---------------------------------------------------------------------------


class IronOrePortsPuller(BasePuller):
    """CAT-52 — Chinese iron-ore 45-port stocks + daily throughput.

    Fallback ladder:
      1. akshare — walks ``AKSHARE_FUNCTION_CANDIDATES`` in order and
         parses the first non-empty DataFrame.
      2. Mysteel public HTML scrape — parses the 45-port survey table.

    On total failure returns zero rows and a single warning — never
    raises up to the scheduler.
    """

    SOURCE_NAME: str = "iron_ore_ports"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": MYSTEEL_URL,
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 22,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self.last_source: str = "none"
        log.info(
            "IronOrePortsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _try_akshare(self) -> list[IronOrePortSnapshot]:
        """Walk the akshare candidate list. Returns the first non-empty
        parsed snapshot list, or ``[]`` on total miss.
        """
        for name in AKSHARE_FUNCTION_CANDIDATES:
            fn = _load_akshare_function(name)
            if fn is None:
                continue
            try:
                # futures_inventory_em takes a symbol argument
                if name == "futures_inventory_em":
                    try:
                        df = fn(symbol="铁矿石")  # type: ignore[call-arg]
                    except TypeError:
                        df = fn()
                else:
                    df = fn()
            except Exception as exc:
                log.debug(
                    "iron_ore_ports: akshare.{n} raised: {e}",
                    n=name,
                    e=str(exc),
                )
                continue
            parsed = _parse_akshare_df(df)
            if parsed:
                self.last_source = f"akshare:{name}"
                log.info(
                    "iron_ore_ports: akshare.{n} returned {r} rows",
                    n=name,
                    r=len(parsed),
                )
                return parsed
        return []

    def _try_mysteel_html(self) -> list[IronOrePortSnapshot]:
        """Scrape the public Mysteel 45-port survey page."""
        html = _http_get(MYSTEEL_URL)
        if not html:
            return []
        parsed = _parse_mysteel_html(html)
        if parsed:
            self.last_source = "mysteel_html"
            log.info(
                "iron_ore_ports: mysteel HTML scrape returned {r} rows",
                r=len(parsed),
            )
        return parsed

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def pull(self) -> list[IronOrePortSnapshot]:
        """Run the fallback walk and return the first non-empty result.

        Sets ``self.last_source`` to ``akshare:<fn>``, ``mysteel_html``
        or ``none``.
        """
        ak_snaps = self._try_akshare()
        if ak_snaps:
            return ak_snaps

        html_snaps = self._try_mysteel_html()
        if html_snaps:
            return html_snaps

        self.last_source = "none"
        log.warning("iron_ore_ports: every source failed — 0 rows pulled")
        return []

    def save_to_db(
        self,
        snapshots: list[IronOrePortSnapshot],
    ) -> int:
        """Upsert snapshots into ``raw_series``. Returns rows inserted.

        Idempotent — skips any ``(series_id, obs_date)`` already present
        for this source with SUCCESS status, cached per-series to avoid
        N+1 lookups.
        """
        if not snapshots:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            for snap in snapshots:
                # Series ID for the stock level
                if snap.port is None:
                    stock_series_id = SERIES_PORT_STOCKS_AGG
                else:
                    slug = re.sub(r"[^a-z0-9]+", "_", snap.port.lower()).strip("_")
                    if not slug:
                        slug = "unknown"
                    stock_series_id = f"{SERIES_PORT_STOCKS_PREFIX}{slug}"

                if not self._row_exists(stock_series_id, snap.date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=stock_series_id,
                        obs_date=snap.date,
                        value=float(snap.total_stocks_mt),
                        raw_payload={
                            "source": self.last_source,
                            "task": "CAT-52",
                            "unit": "mt",
                            "port": snap.port,
                            "description": "Chinese iron ore port stocks",
                        },
                    )
                    inserted += 1

                if (
                    snap.port is None
                    and snap.daily_throughput_mt is not None
                    and not self._row_exists(SERIES_THROUGHPUT_AGG, snap.date, conn)
                ):
                    self._insert_raw(
                        conn=conn,
                        series_id=SERIES_THROUGHPUT_AGG,
                        obs_date=snap.date,
                        value=float(snap.daily_throughput_mt),
                        raw_payload={
                            "source": self.last_source,
                            "task": "CAT-52",
                            "unit": "mt/day",
                            "description": "Chinese iron ore daily throughput",
                        },
                    )
                    inserted += 1

                if (
                    snap.port is None
                    and snap.delta_wow_mt is not None
                    and not self._row_exists(SERIES_DELTA_WOW_AGG, snap.date, conn)
                ):
                    self._insert_raw(
                        conn=conn,
                        series_id=SERIES_DELTA_WOW_AGG,
                        obs_date=snap.date,
                        value=float(snap.delta_wow_mt),
                        raw_payload={
                            "source": self.last_source,
                            "task": "CAT-52",
                            "unit": "mt",
                            "description": "Chinese iron ore port stocks week-over-week delta",
                        },
                    )
                    inserted += 1

        return inserted


# ---------------------------------------------------------------------------
# Module entrypoint
# ---------------------------------------------------------------------------


def run_iron_ore_ports_puller(engine: Engine) -> dict[str, Any]:
    """Run the iron-ore port-stocks puller end-to-end.

    Returns a summary dict with keys:
        * ``fetched``  — snapshots returned from the winning source
        * ``inserted`` — raw_series rows actually written
        * ``source``   — ``"akshare:<fn>"`` / ``"mysteel_html"`` / ``"none"``

    Never raises — wraps the whole run in a broad except so a broken
    upstream can never crash the scheduler.
    """
    try:
        puller = IronOrePortsPuller(engine)
        snapshots = puller.pull()
        inserted = puller.save_to_db(snapshots)
        return {
            "fetched": len(snapshots),
            "inserted": inserted,
            "source": puller.last_source,
        }
    except Exception as exc:  # pragma: no cover — defensive
        log.error("iron_ore_ports: run_iron_ore_ports_puller crashed: {e}", e=str(exc))
        return {"fetched": 0, "inserted": 0, "source": "none"}
