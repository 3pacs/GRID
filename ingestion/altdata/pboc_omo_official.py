"""PBoC open-market-operation announcements, read from pbc.gov.cn (official).

Why this module exists
----------------------
``ingestion/altdata/pboc_omo.py`` (source ``pboc_omo``) was built on two
akshare functions -- ``macro_china_cb_operation`` and
``macro_china_mlf_rate`` -- that do not exist in the akshare release GRID
runs (checked 2026-09-29 on grid-svr, akshare 1.18.52). Every run therefore
fell through to ``repo_rate_hist`` (the FR007 interbank repo fixing, whose
history ends 2020-10-29) and ``macro_china_lpr`` (the Loan Prime Rate). Those
fallbacks were written under OMO/MLF series names with injection/withdrawal
defaulted to 0 -- i.e. the ``pboc_omo`` rows are mislabeled and the content
froze in 2020.

This module reads the PBoC's own "公开市场业务交易公告" (open market
operations announcement) column instead:

    index:  https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/index.html
    detail: .../125475/<id>/index.html  (one page per operating day)

Each announcement states the operation date, and a table of term /
operation rate / bid volume / winning volume for the rate-setting 7-day
reverse repo. The prose may also report other reverse-repo terms done the
same day (e.g. overnight or 14-day). Only what the announcement states is
stored -- no maturities/withdrawals are inferred, and no net figure is
computed.

It writes under its own ``source_catalog`` identity
(``pboc_omo_announcements``) so it never changes the meaning of the old
``pboc_omo`` rows.

Timestamps: ``obs_date`` is the operation date printed in the announcement.
``raw_series.pull_timestamp`` is left to its column default (NOW() at
insert), i.e. the time GRID actually fetched the announcement. Nothing here
claims earlier availability.

Failure contract: if the index or every detail page fails to fetch/parse,
the run returns ``status="FAILED"`` and writes nothing. Rows are only ever
written from a successfully parsed announcement.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable
from urllib.parse import urljoin

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

SOURCE_NAME: str = "pboc_omo_announcements"

PBOC_BASE_URL: str = "https://www.pbc.gov.cn"
PBOC_OMO_INDEX_URL: str = (
    "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/index.html"
)

USER_AGENT: str = "GRID/4.0 (research; stepdadfinance@gmail.com)"
REQUEST_TIMEOUT_S: int = 30
# Polite spacing between detail-page fetches.
DETAIL_FETCH_DELAY_S: float = 1.5
# Hard cap on detail pages fetched per run (the index lists 20).
MAX_DETAILS_PER_RUN: int = 20

SERIES_PREFIX: str = "pboc_omo_ann"

_TERM_MAP: dict[str, str] = {
    "隔夜": "on",
}

_ANNOUNCEMENT_TITLE = "公开市场业务交易公告"

# <a href="..." ... title="公开市场业务交易公告 [2026]第190号" ...>...</a> ... <span class="hui12">2026-09-28</span>
_INDEX_ITEM_RE = re.compile(
    r'<a[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>[^<]*'
    + _ANNOUNCEMENT_TITLE
    + r'[^<]*)</a>\s*</font>\s*<span[^>]*>(?P<pub>\d{4}-\d{2}-\d{2})</span>',
)

_OP_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
# "开展了1390亿元7天期逆回购操作", "开展了6610亿元隔夜逆回购操作",
# "3000亿元14天期逆回购操作". Excludes 买断式 (outright) reverse repos,
# which are a different instrument with their own announcements.
_PROSE_OP_RE = re.compile(
    r"(?P<amt>\d+(?:\.\d+)?)亿元(?P<term>隔夜|\d+天)期?(?P<kind>买断式)?逆回购"
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class IndexItem:
    """One row of the announcement index."""

    url: str
    title: str
    published: date


@dataclass(frozen=True)
class ReverseRepoOp:
    """One reverse-repo operation stated in an announcement."""

    term: str  # normalised: "7d", "14d", "on"
    amount_cny_bn: float
    rate_pct: float | None  # only when the announcement's table gives it


@dataclass(frozen=True)
class Announcement:
    """A parsed announcement page."""

    url: str
    title: str
    op_date: date
    operations: tuple[ReverseRepoOp, ...] = field(default_factory=tuple)
    no_operation: bool = False


# ---------------------------------------------------------------------------
# Pure parsing helpers (tested against recorded fixtures)
# ---------------------------------------------------------------------------


def _normalise_term(raw: str) -> str | None:
    raw = raw.strip()
    if raw in _TERM_MAP:
        return _TERM_MAP[raw]
    m = re.fullmatch(r"(\d+)天期?", raw)
    if m:
        return f"{int(m.group(1))}d"
    return None


def _yi_to_bn(raw: str) -> float | None:
    """'1390亿元' -> 139.0 (CNY bn). 1 亿 = 0.1 bn."""
    m = re.search(r"(\d+(?:\.\d+)?)\s*亿元", raw)
    if not m:
        return None
    return float(m.group(1)) / 10.0


def _pct(raw: str) -> float | None:
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", raw)
    return float(m.group(1)) if m else None


def parse_index(html: str, base_url: str = PBOC_BASE_URL) -> list[IndexItem]:
    """Return the announcements listed on the OMO index page, newest first."""
    items: list[IndexItem] = []
    seen: set[str] = set()
    for m in _INDEX_ITEM_RE.finditer(html):
        url = urljoin(base_url, m.group("href"))
        if url in seen:
            continue
        seen.add(url)
        try:
            pub = date.fromisoformat(m.group("pub"))
        except ValueError:
            continue
        items.append(IndexItem(url=url, title=m.group("title").strip(), published=pub))
    items.sort(key=lambda i: i.published, reverse=True)
    return items


def _article_text_and_rows(html: str) -> tuple[str, list[list[str]]]:
    """Return (plain text, table rows as cell-text lists) of the article body."""
    start = html.find('id="zoom"')
    body = html[start:] if start >= 0 else html
    end = body.find("打印本页")
    if end > 0:
        body = body[:end]

    rows: list[list[str]] = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", body, flags=re.S):
        cells = [
            _WS_RE.sub("", _TAG_RE.sub("", td))
            for td in re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.S)
        ]
        if any(cells):
            rows.append(cells)

    text = _WS_RE.sub("", _TAG_RE.sub("", body))
    return text, rows


def parse_announcement(html: str, url: str = "", title: str = "") -> Announcement | None:
    """Parse one announcement page. Returns None when it cannot be parsed."""
    text, rows = _article_text_and_rows(html)

    m = _OP_DATE_RE.search(text)
    if not m:
        return None
    try:
        op_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None

    # Table: header row contains 期限 (term); data rows follow.
    table_rates: dict[str, float | None] = {}
    table_amounts: dict[str, float] = {}
    header: list[str] | None = None
    for cells in rows:
        if any("期限" in c for c in cells):
            header = cells
            continue
        if header is None or len(cells) != len(header):
            continue
        rec = dict(zip(header, cells))
        term = _normalise_term(rec.get("期限", ""))
        if term is None:
            continue
        rate_cell = next((v for k, v in rec.items() if "利率" in k), "")
        amt_cell = next((v for k, v in rec.items() if "中标量" in k or "操作量" in k), "")
        table_rates[term] = _pct(rate_cell)
        amt = _yi_to_bn(amt_cell)
        if amt is not None:
            table_amounts[term] = amt

    ops: dict[str, ReverseRepoOp] = {}
    for pm in _PROSE_OP_RE.finditer(text):
        if pm.group("kind"):  # 买断式 outright reverse repo -- different instrument
            continue
        term = _normalise_term(pm.group("term"))
        if term is None or term in ops:
            continue
        ops[term] = ReverseRepoOp(
            term=term,
            amount_cny_bn=float(pm.group("amt")) / 10.0,
            rate_pct=table_rates.get(term),
        )
    # Table rows the prose did not mention (defensive).
    for term, amt in table_amounts.items():
        if term not in ops:
            ops[term] = ReverseRepoOp(term=term, amount_cny_bn=amt, rate_pct=table_rates.get(term))

    no_op = not ops and ("不开展" in text or "未开展" in text)
    if not ops and not no_op:
        return None

    return Announcement(
        url=url,
        title=title,
        op_date=op_date,
        operations=tuple(sorted(ops.values(), key=lambda o: o.term)),
        no_operation=no_op,
    )


def series_ids_for(op: ReverseRepoOp) -> tuple[str, str]:
    """(amount series, rate series) for one operation term."""
    return (
        f"{SERIES_PREFIX}:reverse_repo_{op.term}_amount_cny_bn",
        f"{SERIES_PREFIX}:reverse_repo_{op.term}_rate_pct",
    )


# ---------------------------------------------------------------------------
# Puller
# ---------------------------------------------------------------------------


class PBOCOmoAnnouncementsPuller(BasePuller):
    """Reads PBoC OMO announcements from pbc.gov.cn into raw_series."""

    SOURCE_NAME: str = SOURCE_NAME
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": PBOC_OMO_INDEX_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 30,
    }

    # Series checked for "what have we already stored" (bounded lookup).
    ANCHOR_SERIES: str = f"{SERIES_PREFIX}:reverse_repo_7d_amount_cny_bn"

    def __init__(self, db_engine: Engine, session: requests.Session | None = None) -> None:
        super().__init__(db_engine)
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

    def _get(self, url: str) -> str:
        resp = self._session.get(url, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        resp.encoding = resp.encoding if resp.encoding and resp.encoding.lower() != "iso-8859-1" else "utf-8"
        return resp.text

    def fetch(self, since: date | None) -> tuple[list[Announcement], list[str]]:
        """Fetch announcements published on/after ``since`` (all listed if None).

        Returns (parsed announcements, error strings). Raises if the index
        itself cannot be fetched or parsed.
        """
        index_html = self._get(PBOC_OMO_INDEX_URL)
        items = parse_index(index_html)
        if not items:
            raise ValueError("PBoC OMO index: no announcements parsed (layout change?)")

        wanted = [i for i in items if since is None or i.published >= since]
        wanted = wanted[:MAX_DETAILS_PER_RUN]

        parsed: list[Announcement] = []
        errors: list[str] = []
        for n, item in enumerate(wanted):
            if n:
                time.sleep(DETAIL_FETCH_DELAY_S)
            try:
                html = self._get(item.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{item.url}: fetch {exc}")
                continue
            ann = parse_announcement(html, url=item.url, title=item.title)
            if ann is None:
                errors.append(f"{item.url}: unparseable")
                continue
            parsed.append(ann)
        return parsed, errors

    def save(self, announcements: Iterable[Announcement]) -> int:
        """Insert rows for parsed announcements; skip (series, date) already stored."""
        anns = [a for a in announcements if a.operations]
        if not anns:
            return 0
        start = min(a.op_date for a in anns)
        fetched_at = datetime.now(timezone.utc).isoformat()
        inserted = 0
        existing: dict[str, set[date]] = {}
        with self.engine.begin() as conn:
            for ann in anns:
                for op in ann.operations:
                    amount_sid, rate_sid = series_ids_for(op)
                    payload = {
                        "announcement_url": ann.url,
                        "announcement_title": ann.title,
                        "term": op.term,
                        "amount_cny_bn": op.amount_cny_bn,
                        "rate_pct": op.rate_pct,
                        "fetched_at": fetched_at,
                    }
                    for sid, value in ((amount_sid, op.amount_cny_bn), (rate_sid, op.rate_pct)):
                        if value is None:
                            continue
                        if sid not in existing:
                            existing[sid] = self._get_existing_dates(sid, conn, start_date=start)
                        if ann.op_date in existing[sid]:
                            continue
                        self._insert_raw(conn, sid, ann.op_date, float(value), raw_payload=payload)
                        existing[sid].add(ann.op_date)
                        inserted += 1
        return inserted

    def pull(self) -> dict[str, Any]:
        """Fetch new announcements and store them. Never raises."""
        try:
            latest = self._get_latest_date(self.ANCHOR_SERIES)
        except Exception as exc:  # noqa: BLE001
            log.warning("pboc_omo_announcements: latest-date lookup failed: {e}", e=str(exc))
            latest = None

        try:
            announcements, errors = self.fetch(since=latest)
        except Exception as exc:  # noqa: BLE001
            log.warning("pboc_omo_announcements: index fetch failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}

        if errors and not announcements:
            log.warning("pboc_omo_announcements: every detail page failed: {e}", e=errors[:3])
            return {"status": "FAILED", "rows_inserted": 0, "error": "; ".join(errors)[:300]}

        try:
            inserted = self.save(announcements)
        except Exception as exc:  # noqa: BLE001
            log.warning("pboc_omo_announcements: save failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}

        status = "PARTIAL" if errors else "SUCCESS"
        log.info(
            "pboc_omo_announcements: {a} announcements parsed, {i} rows inserted, {e} errors",
            a=len(announcements), i=inserted, e=len(errors),
        )
        return {
            "status": status,
            "rows_inserted": inserted,
            "announcements": len(announcements),
            "latest_op_date": max((a.op_date for a in announcements), default=None),
            "errors": errors[:5],
        }


def run_pboc_omo_announcements_puller(engine: Engine) -> dict[str, Any]:
    """Scheduler entry point."""
    try:
        return PBOCOmoAnnouncementsPuller(engine).pull()
    except Exception as exc:  # noqa: BLE001
        log.error("pboc_omo_announcements: puller crashed: {e}", e=str(exc))
        return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}


__all__ = [
    "SOURCE_NAME",
    "PBOC_OMO_INDEX_URL",
    "IndexItem",
    "ReverseRepoOp",
    "Announcement",
    "parse_index",
    "parse_announcement",
    "series_ids_for",
    "PBOCOmoAnnouncementsPuller",
    "run_pboc_omo_announcements_puller",
]
