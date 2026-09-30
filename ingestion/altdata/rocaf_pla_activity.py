"""Daily PLA activity around Taiwan, from the ROC Air Force HQ (MND) site.

Why this module exists
----------------------
``taiwan_strait_osint`` scraped ``www.mnd.gov.tw/English/PublishTable.aspx``
pages that now 404 (MND moved to ``www.mnd.gov.tw/en/news/PlaactList``).
The new MND site's robots.txt is ``User-agent: *  Disallow: /`` (only
Googlebot is allowed), so GRID must not scrape www.mnd.gov.tw at all.

The same daily MND "Military News Update" is republished, in English, by
the ROC Air Force Command Headquarters (an MND unit) at::

    list:   https://air.mnd.gov.tw/EN/News/News_List.aspx?CID=214
    detail: https://air.mnd.gov.tw/EN/News/News_Detail.aspx?CID=214&ID=<id>

Each detail page carries e.g. "3 sorties of PLA aircraft, 6 PLAN ships
and 4 official ships operating around Taiwan were detected as of 6 a.m.
(UTC+8) today. 1 out of 3 sorties entered Taiwan's southwestern ADIZ."
and ends with "Source: Ministry of National Defense R.O.C - Military News
Update". air.mnd.gov.tw serves no robots.txt (the path redirects to the
home page), i.e. it states no crawl restriction. GRID fetches the list
plus only the new detail pages, spaced 1.5 s apart, once a day.

Identity and meaning
--------------------
Own ``source_catalog`` row ``rocaf_pla_activity`` and own series namespace
``pla_activity:`` -- deliberately NOT the old ``taiwan_strait:*`` series,
which hold placeholder "seed" rows written by the broken scraper.

* ``pla_activity:aircraft_sorties``   -- PLA aircraft sorties detected
* ``pla_activity:plan_ships``         -- PLAN ships detected
* ``pla_activity:official_ships``     -- PRC official (e.g. coast guard) ships
* ``pla_activity:adiz_entries``       -- sorties that entered Taiwan's ADIZ
  (including those that crossed the median line). MND omits the sentence
  when none entered; that case is stored as 0 with
  ``raw_payload.adiz_sentence_present = false`` so it stays auditable.

Only counts actually printed in a report are stored; a report whose
aircraft-sortie count cannot be parsed is skipped entirely. If ADIZ is
mentioned, a positive entry count must be parsed or the report is skipped;
zero is stored only when ADIZ is absent. ``obs_date`` is the report date
(counts as of 06:00 UTC+8 that day); ``raw_series.pull_timestamp`` keeps
its default = GRID's fetch time.

Failure contract: list fetch/parse failure, every new detail failing, or no
new rows after the incremental filter/deduplication -> ``status="FAILED"``.
Never report a zero-row pull as SUCCESS.
"""

from __future__ import annotations

import html as html_lib
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable
from urllib.parse import urljoin

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

SOURCE_NAME: str = "rocaf_pla_activity"

AF_BASE_URL: str = "https://air.mnd.gov.tw"
AF_LIST_URL: str = "https://air.mnd.gov.tw/EN/News/News_List.aspx?CID=214"
USER_AGENT: str = "GRID/4.0 (research; stepdadfinance@gmail.com)"
REQUEST_TIMEOUT_S: int = 30
DETAIL_FETCH_DELAY_S: float = 1.5
# First run: the most recent N reports only (no deep history crawl).
INITIAL_MAX_DETAILS: int = 10
MAX_DETAILS_PER_RUN: int = 20

SERIES_AIRCRAFT: str = "pla_activity:aircraft_sorties"
SERIES_PLAN_SHIPS: str = "pla_activity:plan_ships"
SERIES_OFFICIAL_SHIPS: str = "pla_activity:official_ships"
SERIES_ADIZ: str = "pla_activity:adiz_entries"

_LIST_ITEM_RE = re.compile(
    r'<a[^>]+href="(?P<href>[^"]*News_Detail\.aspx\?CID=214&(?:amp;)?ID=\d+)"[^>]*>\s*'
    r'<span class="Title">(?P<title>[^<]*)</span>\s*'
    r'<span class="Time">(?P<date>\d{4}/\d{2}/\d{2})</span>',
    re.S,
)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

_AIRCRAFT_RE = re.compile(r"(\d+)\s+sorties?\s+of\s+PLA\s+aircraft", re.I)
_AIRCRAFT_ALT_RE = re.compile(r"(\d+)\s+PLA\s+aircraft", re.I)
_PLAN_RE = re.compile(r"(\d+)\s+PLAN\s+(?:ships?|vessels?)", re.I)
_OFFICIAL_RE = re.compile(r"(\d+)\s+official\s+(?:ships?|vessels?)", re.I)
_ADIZ_DIRECTION = (
    r"(?:northern|southern|eastern|western|central|northeastern|"
    r"northwestern|southeastern|southwestern)"
)
# Only the printed affirmative subject/predicate is supported. Arbitrary
# spans can borrow a total from another clause or match "never entered".
_ADIZ_RE = re.compile(
    r"(?:All\s+(?P<all>\d+)|(?P<part>\d+)\s+(?:out\s+of|of)\s+"
    r"(?:the\s+)?(?P<total>\d+))\s+sorties?(?:\s+of\s+PLA\s+aircraft)?\s+"
    r"(?:crossed\s+the\s+median\s+line\s+and\s+)?entered\s+"
    r"(?:(?:Taiwan['’]s|the)\s+)?"
    rf"(?:{_ADIZ_DIRECTION}(?:\s*,\s*{_ADIZ_DIRECTION})*"
    rf"(?:\s*,?\s+and\s+{_ADIZ_DIRECTION})?\s+)?ADIZ",
    re.I,
)
_DATE_RE = re.compile(r"\b(20\d{2})/(\d{2})/(\d{2})\b")


@dataclass(frozen=True)
class ListItem:
    url: str
    title: str
    published: date


@dataclass(frozen=True)
class PLAActivityReport:
    url: str
    report_date: date
    aircraft_sorties: int
    plan_ships: int | None
    official_ships: int | None
    adiz_entries: int
    adiz_sentence_present: bool
    text: str


def parse_list(html: str, base_url: str = AF_BASE_URL) -> list[ListItem]:
    """Return the reports listed on the Air activities page, newest first."""
    items: list[ListItem] = []
    seen: set[str] = set()
    for m in _LIST_ITEM_RE.finditer(html):
        url = urljoin(base_url, html_lib.unescape(m.group("href")))
        if url in seen:
            continue
        seen.add(url)
        try:
            y, mo, d = (int(x) for x in m.group("date").split("/"))
            published = date(y, mo, d)
        except ValueError:
            continue
        title = html_lib.unescape(m.group("title")).strip()
        if "PLA activities" not in title:
            continue
        items.append(ListItem(url=url, title=title, published=published))
    items.sort(key=lambda i: i.published, reverse=True)
    return items


def _visible_text(html: str) -> str:
    body = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S | re.I)
    return _WS_RE.sub(" ", html_lib.unescape(_TAG_RE.sub(" ", body))).strip()


def parse_report(html: str, url: str = "", fallback_date: date | None = None) -> PLAActivityReport | None:
    """Skip reports with missing sorties or an ADIZ mention without a positive count."""
    text = _visible_text(html)
    start = text.find("PLA activities:")
    if start < 0:
        start = text.find("sorties of PLA aircraft")
        start = max(0, start - 200)
    body = text[start:start + 1500]

    m_air = _AIRCRAFT_RE.search(body) or _AIRCRAFT_ALT_RE.search(body)
    if not m_air:
        return None
    aircraft_sorties = int(m_air.group(1))

    report_date = fallback_date
    m_date = _DATE_RE.search(text[max(0, start - 200):start + 50]) or _DATE_RE.search(text)
    if m_date:
        try:
            report_date = date(int(m_date.group(1)), int(m_date.group(2)), int(m_date.group(3)))
        except ValueError:
            pass
    if report_date is None:
        return None

    m_plan = _PLAN_RE.search(body)
    m_off = _OFFICIAL_RE.search(body)
    adiz_entries = 0
    adiz_present = "adiz" in text.lower()
    if adiz_present:
        # A second/unsupported mention makes the report ambiguous, even if
        # one other clause has a parseable count. Check all visible text so
        # an out-of-window mention cannot silently become an omitted zero.
        # Keep semicolon-linked qualifications and sentence terminators. A
        # question must not become an assertion by losing its "?", including
        # punctuation separated by visible whitespace or HTML tags. Inspect
        # the complete sentence before applying the unchanged activity window,
        # so a qualifier just beyond that window cannot be silently dropped.
        sentences = [m for m in re.finditer(r"[^.!?]+(?:[.!?](?:\s*[.!?])*|$)", text[start:])
                     if "adiz" in m.group().lower()]
        if text.lower().count("adiz") != 1 or len(sentences) != 1:
            return None
        sentence = sentences[0]
        if sentence.end() > len(body):
            return None
        statement = sentence.group().strip()
        # A single declarative period is supported; ?, !, mixed/repeated
        # punctuation and semicolon qualifications remain for fullmatch to
        # reject. Unpunctuated complete statements retain prior support.
        if statement.endswith("."):
            statement = statement[:-1].rstrip()
        m_adiz = _ADIZ_RE.fullmatch(statement)
        if m_adiz is None:
            return None
        adiz_entries = int(m_adiz.group("all") or m_adiz.group("part"))
        stated_total = int(m_adiz.group("all") or m_adiz.group("total"))
        # The count belongs to the same printed aircraft total. Refuse
        # contradictions instead of choosing one of the reported numbers.
        if not 0 < adiz_entries <= aircraft_sorties or stated_total != aircraft_sorties:
            return None
    return PLAActivityReport(
        url=url,
        report_date=report_date,
        aircraft_sorties=aircraft_sorties,
        plan_ships=int(m_plan.group(1)) if m_plan else None,
        official_ships=int(m_off.group(1)) if m_off else None,
        adiz_entries=adiz_entries,
        adiz_sentence_present=adiz_present,
        text=body[:600],
    )


class ROCAFPLAActivityPuller(BasePuller):
    """Daily PLA activity counts from air.mnd.gov.tw into raw_series."""

    SOURCE_NAME: str = SOURCE_NAME
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": AF_LIST_URL,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 40,
    }

    def __init__(self, db_engine: Engine, session: requests.Session | None = None) -> None:
        super().__init__(db_engine)
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

    def _get(self, url: str) -> str:
        resp = self._session.get(url, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()
        return resp.text

    def fetch(self, since: date | None) -> tuple[list[PLAActivityReport], list[str]]:
        items = parse_list(self._get(AF_LIST_URL))
        if not items:
            raise ValueError("ROCAF Air activities list: no reports parsed (layout change?)")
        if since is None:
            wanted = items[:INITIAL_MAX_DETAILS]
        else:
            wanted = [i for i in items if i.published > since][:MAX_DETAILS_PER_RUN]

        reports: list[PLAActivityReport] = []
        errors: list[str] = []
        for n, item in enumerate(wanted):
            if n:
                time.sleep(DETAIL_FETCH_DELAY_S)
            try:
                html = self._get(item.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{item.url}: fetch {exc}")
                continue
            rep = parse_report(html, url=item.url, fallback_date=item.published)
            if rep is None:
                errors.append(f"{item.url}: unparseable")
                continue
            reports.append(rep)
        return reports, errors

    def save(self, reports: Iterable[PLAActivityReport]) -> int:
        reps = list(reports)
        if not reps:
            return 0
        start = min(r.report_date for r in reps)
        fetched_at = datetime.now(timezone.utc).isoformat()
        inserted = 0
        existing: dict[str, set[date]] = {}
        with self.engine.begin() as conn:
            for rep in reps:
                payload = {
                    "report_url": rep.url,
                    "adiz_sentence_present": rep.adiz_sentence_present,
                    "text": rep.text,
                    "publisher": "ROC Air Force Command HQ (MND Military News Update)",
                    "fetched_at": fetched_at,
                }
                for sid, value in (
                    (SERIES_AIRCRAFT, rep.aircraft_sorties),
                    (SERIES_PLAN_SHIPS, rep.plan_ships),
                    (SERIES_OFFICIAL_SHIPS, rep.official_ships),
                    (SERIES_ADIZ, rep.adiz_entries),
                ):
                    if value is None:
                        continue
                    if sid not in existing:
                        existing[sid] = self._get_existing_dates(sid, conn, start_date=start)
                    if rep.report_date in existing[sid]:
                        continue
                    self._insert_raw(conn, sid, rep.report_date, float(value), raw_payload=payload)
                    existing[sid].add(rep.report_date)
                    inserted += 1
        return inserted

    def pull(self) -> dict[str, Any]:
        """Fetch new reports and store them. Never raises."""
        try:
            latest = self._get_latest_date(SERIES_AIRCRAFT)
        except Exception as exc:  # noqa: BLE001
            log.warning("rocaf_pla_activity: latest-date lookup failed: {e}", e=str(exc))
            latest = None
        try:
            reports, errors = self.fetch(since=latest)
        except Exception as exc:  # noqa: BLE001
            log.warning("rocaf_pla_activity: list fetch failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}
        if errors and not reports:
            return {"status": "FAILED", "rows_inserted": 0, "error": "; ".join(errors)[:300]}
        if not reports:
            return {"status": "FAILED", "rows_inserted": 0, "error": "no new ROCAF activity reports"}
        try:
            inserted = self.save(reports)
        except Exception as exc:  # noqa: BLE001
            log.warning("rocaf_pla_activity: save failed: {e}", e=str(exc))
            return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}
        if inserted == 0:
            return {
                "status": "FAILED",
                "rows_inserted": 0,
                "reports": len(reports),
                "error": "ROCAF activity reports produced no new rows",
            }
        latest_rep = max(reports, key=lambda r: r.report_date) if reports else None
        return {
            "status": "PARTIAL" if errors else "SUCCESS",
            "rows_inserted": inserted,
            "reports": len(reports),
            "latest_report_date": latest_rep.report_date.isoformat() if latest_rep else None,
            "latest_aircraft_sorties": latest_rep.aircraft_sorties if latest_rep else None,
            "errors": errors[:5],
        }


def run_rocaf_pla_activity_puller(engine: Engine) -> dict[str, Any]:
    """Scheduler entry point."""
    try:
        return ROCAFPLAActivityPuller(engine).pull()
    except Exception as exc:  # noqa: BLE001
        log.error("rocaf_pla_activity: puller crashed: {e}", e=str(exc))
        return {"status": "FAILED", "rows_inserted": 0, "error": str(exc)[:300]}


__all__ = [
    "SOURCE_NAME",
    "AF_LIST_URL",
    "SERIES_AIRCRAFT",
    "SERIES_PLAN_SHIPS",
    "SERIES_OFFICIAL_SHIPS",
    "SERIES_ADIZ",
    "ListItem",
    "PLAActivityReport",
    "parse_list",
    "parse_report",
    "ROCAFPLAActivityPuller",
    "run_rocaf_pla_activity_puller",
]
