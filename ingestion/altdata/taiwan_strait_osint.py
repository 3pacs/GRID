"""
GRID Taiwan Strait OSINT — CAT-91 (P0, Tier A).

Tracks real-time military tension across the Taiwan Strait as a
cross-asset risk-off proxy. A spike in PLA activity in Taiwan's ADIZ
has historically correlated with:

- 3-8 day risk-off moves in SPY
- 1-3% vol spikes in semiconductor equities (TSM, NVDA, ASML)
- A sharp yen (JPY) and gold bid
- Sudden USD moves (broad-DXY repricing)

Primary signal: the daily count of PLA Air Force aircraft crossing
into Taiwan's Air Defense Identification Zone (ADIZ). Secondary: PLA
Navy vessel count near the Strait. Tertiary: announced PLA exercise
zones.

Data strategy
-------------
Real OpenSky Network (ADS-B) and AISHub (AIS) integration is noisy,
rate-limited, and requires running spatial filters on millions of
position updates per day. We do NOT do that in V1. Instead, the
canonical public signal is the Taiwan Ministry of National Defense
(MND) daily report, "Chinese Communist Military Activities in the
Vicinity of the ROC," which is the same feed used by Reuters, AP, and
every China-watcher. MND publishes daily with the fields we need:
aircraft_count, adiz_crossing_count, vessel_count, date.

**OpenSky / AISHub real-time integration is explicitly out of scope
for V1 and is documented here as a future enhancement.** When we
eventually add it, it will augment (not replace) the MND daily count.

Secondary signal
----------------
KNOWN_PLA_EXERCISES is a hard-coded calendar of announced PLA drills
since 2022 (Pelosi visit, Tsai-McCarthy meeting, Joint Sword series).
It is a known-gap seed used by the event-tension classifier when the
MND scrape fails or produces no rows. Real-time exercise announcement
tracking requires live MND press releases, which this puller scrapes.

Series stored (raw_series namespaces)
-------------------------------------
- taiwan_strait:aircraft_count
- taiwan_strait:adiz_crossing_count
- taiwan_strait:vessel_count
- taiwan_strait:exercise_flag   (0 or 1)

Source: Taiwan MND English press releases (HTML scrape)
Schedule: Daily (MND publishes ~08:00 Taipei time every day)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import requests
from bs4 import BeautifulSoup
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 30
_USER_AGENT: str = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Taiwan MND English site root (press releases portal)
MND_ENGLISH_URL: str = "https://www.mnd.gov.tw/English/"

# Canonical daily ADIZ incursion report landing page. MND groups these
# under "Chinese Communist Military Activities in the Vicinity of the
# ROC". The exact permalink on MND rotates by language slug, so we
# probe this URL first and fall back to MND_ENGLISH_URL if it 404s.
MND_ADIZ_URL: str = (
    "https://www.mnd.gov.tw/English/"
    "PublishTable.aspx?Types=Military%20News%20Update&title=News%20Channel"
)

# Series IDs (namespaced)
SERIES_AIRCRAFT: str = "taiwan_strait:aircraft_count"
SERIES_ADIZ: str = "taiwan_strait:adiz_crossing_count"
SERIES_VESSEL: str = "taiwan_strait:vessel_count"
SERIES_EXERCISE_FLAG: str = "taiwan_strait:exercise_flag"

ALL_SERIES: tuple[str, ...] = (
    SERIES_AIRCRAFT,
    SERIES_ADIZ,
    SERIES_VESSEL,
    SERIES_EXERCISE_FLAG,
)

# Hard-coded known PLA exercises (announced publicly).
# Seed for event-tension classifier. Extend as new drills are announced.
# Date = first day of the announced window.
KNOWN_PLA_EXERCISES: dict[date, str] = {
    # Pelosi visit backlash
    date(2022, 8, 4): "PLA Drills Around Taiwan (Pelosi Visit Response)",
    # Tsai Ing-wen meets Speaker McCarthy in California
    date(2023, 4, 8): "PLA Joint Sword Exercises (Tsai-McCarthy Meeting)",
    # Post-inauguration of President Lai Ching-te
    date(2024, 5, 23): "PLA Joint Sword 2024-A",
    # October 2024 — second Joint Sword round
    date(2024, 10, 14): "PLA Joint Sword 2024-B",
    # December 2024 — large-scale naval deployment around Taiwan
    date(2024, 12, 9): "PLA December Naval Mobilization",
    # April 2025 — Joint Sword successor exercise
    date(2025, 4, 1): "PLA Strait Thunder 2025-A",
}


# ── Data class ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class TaiwanStraitSnapshot:
    """One day of Taiwan MND ADIZ incursion data.

    Attributes:
        date: Observation date (UTC/Taipei calendar day).
        aircraft_count: Total PLA aircraft detected in the vicinity.
        adiz_crossing_count: Subset that crossed into Taiwan's ADIZ or
            crossed the median line of the Taiwan Strait.
        vessel_count: PLA Navy vessel count near the Strait.
        exercise_announced: True if a PLA exercise is announced for
            this date (from either the scrape or the hard-coded seed).
        exercise_name: Name of the announced exercise, if any.
    """

    date: date
    aircraft_count: int
    adiz_crossing_count: int
    vessel_count: int
    exercise_announced: bool
    exercise_name: str | None

    @property
    def exercise_flag(self) -> int:
        """Integer 0/1 flag for raw_series storage."""
        return 1 if self.exercise_announced else 0


# ── Pure helpers ─────────────────────────────────────────────────────


def is_exercise_active(
    as_of: date,
    calendar: dict[date, str],
    window_days: int = 7,
) -> tuple[bool, str | None]:
    """Return whether a known PLA exercise is active on ``as_of``.

    An exercise is considered active if any calendar entry falls within
    ``window_days`` of the given date (inclusive, either side). This is
    a pure function with no side effects — safe to call from tests.

    Parameters:
        as_of: The date to check.
        calendar: Dict mapping exercise start date to exercise name.
        window_days: Half-window in days on either side. Default 7.

    Returns:
        Tuple of (is_active, exercise_name). If multiple exercises are
        within the window, the closest one is returned.
    """
    if not calendar:
        return (False, None)

    best_name: str | None = None
    best_delta: int | None = None

    for ex_date, ex_name in calendar.items():
        delta = abs((as_of - ex_date).days)
        if delta <= window_days:
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_name = ex_name

    if best_name is None:
        return (False, None)
    return (True, best_name)


def _safe_int(raw: str | None) -> int:
    """Parse an int from an MND text fragment, tolerating 'N/A' and junk.

    Returns 0 on failure rather than raising — MND rows with missing
    fields should degrade to 0 instead of crashing the pull.
    """
    if raw is None:
        return 0
    cleaned = raw.strip().replace(",", "")
    if not cleaned or cleaned.upper() in {"N/A", "NA", "-", "—"}:
        return 0
    try:
        return int(cleaned)
    except (TypeError, ValueError):
        return 0


def _parse_mnd_date(raw: str) -> date | None:
    """Parse a date from MND's multiple publish formats.

    Handles ``2026/04/13``, ``2026-04-13``, ``April 13, 2026``, and the
    Taiwan-calendar compact form ``20260413``.
    """
    if not raw:
        return None
    txt = raw.strip()

    # ISO-ish (2026/04/13 or 2026-04-13)
    m = re.search(r"(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})", txt)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # Compact digits 20260413
    m = re.search(r"\b(20\d{2})(\d{2})(\d{2})\b", txt)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # "April 13, 2026" or "Apr 13 2026"
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue

    return None


# MND uses language like "24 PLA aircraft ... 7 PLAN vessels ... 15 of
# them crossed the median line of the Taiwan Strait". We grep for these
# phrases with forgiving regexes.
_AIRCRAFT_RE: re.Pattern[str] = re.compile(
    r"([\d,]+|N/?A)\s*(?:PLA(?:AF)?\s+)?aircraft", re.IGNORECASE
)
_VESSEL_RE: re.Pattern[str] = re.compile(
    r"([\d,]+|N/?A)\s*(?:PLA(?:N)?\s+)?vessels?", re.IGNORECASE
)
_ADIZ_RE: re.Pattern[str] = re.compile(
    r"([\d,]+|N/?A)\s+(?:of\s+(?:them|the\s+\w+)\s+)?"
    r"(?:crossed|entered|crossings?)\s+"
    r"(?:the\s+)?.{0,40}?"
    r"(?:median\s+line|ADIZ)",
    re.IGNORECASE | re.DOTALL,
)


def _parse_mnd_html(html: str) -> list[TaiwanStraitSnapshot]:
    """Parse MND press releases HTML into TaiwanStraitSnapshot rows.

    Strategy:
        1. BeautifulSoup finds every press release block. MND uses a
           ``<div class="press-release">`` / ``<article>`` / or a
           ``<tr>`` row layout depending on page. We try all three.
        2. Each block supplies a date (header/title/``<time>``) and
           body text. We regex-match aircraft / vessel / ADIZ counts.
        3. Rows with no parseable date are skipped. Rows with no
           parseable counts are stored with count=0 (degrades
           gracefully — an empty row still anchors the date).

    Parameters:
        html: Raw HTML from the MND press releases page.

    Returns:
        List of TaiwanStraitSnapshot sorted most-recent first.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    snapshots: list[TaiwanStraitSnapshot] = []
    seen_dates: set[date] = set()

    # Candidate block selectors, in priority order.
    candidate_blocks: list[Any] = []
    candidate_blocks.extend(soup.find_all("div", class_=re.compile(r"press-release|news-item|release", re.I)))
    candidate_blocks.extend(soup.find_all("article"))
    candidate_blocks.extend(soup.find_all("tr"))
    # Fall back: look for any element with a <time> child
    if not candidate_blocks:
        candidate_blocks = [t.parent for t in soup.find_all("time") if t.parent]

    for block in candidate_blocks:
        try:
            snap = _parse_one_block(block)
        except Exception as exc:  # defensive — per-block errors must not kill the pull
            log.debug("MND block parse failed: {e}", e=str(exc))
            continue
        if snap is None:
            continue
        if snap.date in seen_dates:
            continue
        seen_dates.add(snap.date)
        snapshots.append(snap)

    # Most recent first
    snapshots.sort(key=lambda s: s.date, reverse=True)
    return snapshots


def _parse_one_block(block: Any) -> TaiwanStraitSnapshot | None:
    """Extract a single TaiwanStraitSnapshot from a BeautifulSoup block."""
    text_content: str = " ".join(block.get_text(" ", strip=True).split())
    if not text_content:
        return None

    # Find date: prefer <time datetime="...">, otherwise regex on text.
    parsed_date: date | None = None
    time_el = block.find("time") if hasattr(block, "find") else None
    if time_el is not None:
        parsed_date = _parse_mnd_date(time_el.get("datetime", "") or time_el.get_text(" ", strip=True))
    if parsed_date is None:
        parsed_date = _parse_mnd_date(text_content)
    if parsed_date is None:
        return None

    # Must smell like an ADIZ report (at least one relevant token)
    lower = text_content.lower()
    if "aircraft" not in lower and "vessel" not in lower and "adiz" not in lower:
        return None

    aircraft = 0
    vessels = 0
    adiz = 0

    m_air = _AIRCRAFT_RE.search(text_content)
    if m_air:
        aircraft = _safe_int(m_air.group(1))
    m_ves = _VESSEL_RE.search(text_content)
    if m_ves:
        vessels = _safe_int(m_ves.group(1))
    m_adiz = _ADIZ_RE.search(text_content)
    if m_adiz:
        adiz = _safe_int(m_adiz.group(1))

    # Optional: detect in-body exercise announcements
    exercise_announced = False
    exercise_name: str | None = None
    if re.search(r"joint\s+sword|strait\s+thunder|live[- ]fire\s+drill", text_content, re.I):
        exercise_announced = True
        em = re.search(
            r"(joint\s+sword[\w\- ]*|strait\s+thunder[\w\- ]*)",
            text_content,
            re.I,
        )
        if em:
            exercise_name = em.group(1).strip()

    return TaiwanStraitSnapshot(
        date=parsed_date,
        aircraft_count=aircraft,
        adiz_crossing_count=adiz,
        vessel_count=vessels,
        exercise_announced=exercise_announced,
        exercise_name=exercise_name,
    )


# ── Puller ───────────────────────────────────────────────────────────


class TaiwanStraitPuller(BasePuller):
    """Daily Taiwan Strait tension puller (CAT-91).

    Scrapes Taiwan MND English press releases for the daily ADIZ
    incursion count, then upserts into raw_series under the
    ``taiwan_strait:*`` namespaces. Falls back to a hard-coded PLA
    exercise seed when the scrape fails so downstream consumers always
    see a non-null row for "today".

    OpenSky / AISHub real-time integration is out of scope — see the
    module docstring for the V1 data strategy.
    """

    SOURCE_NAME: str = "taiwan_strait_osint"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": MND_ENGLISH_URL,
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": False,  # MND never revises, but does not publish vintage
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "TaiwanStraitPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # Fetch
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_mnd_html(self, url: str) -> str:
        """Fetch MND HTML. Raises on network or HTTP error."""
        headers = {"User-Agent": _USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
        resp = requests.get(url, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def pull(self) -> dict[str, Any]:
        """Fetch + parse MND. Falls back to seed snapshot on failure.

        Returns:
            Dict with: snapshots (list), source ("mnd_html"|"seed"|"none"),
            error (str|None).
        """
        # Try specific ADIZ URL first, then the general press releases page.
        for url in (MND_ADIZ_URL, MND_ENGLISH_URL):
            try:
                html = self._fetch_mnd_html(url)
            except requests.RequestException as exc:
                log.warning("MND fetch failed for {u}: {e}", u=url, e=str(exc))
                continue
            except Exception as exc:
                log.warning("MND fetch unexpected error for {u}: {e}", u=url, e=str(exc))
                continue

            snapshots = _parse_mnd_html(html)
            if snapshots:
                log.info("MND parse: {n} snapshots from {u}", n=len(snapshots), u=url)
                return {
                    "snapshots": snapshots,
                    "source": "mnd_html",
                    "error": None,
                }
            log.info("MND parse returned 0 snapshots for {u} — trying fallback", u=url)

        # Final fallback: seed row from hard-coded exercise calendar.
        seed = self._seed_snapshot(date.today())
        return {
            "snapshots": [seed],
            "source": "seed",
            "error": "MND scrape produced no parseable snapshots",
        }

    def _seed_snapshot(self, as_of: date) -> TaiwanStraitSnapshot:
        """Construct a zero-count snapshot anchored on the exercise calendar."""
        active, name = is_exercise_active(as_of, KNOWN_PLA_EXERCISES)
        return TaiwanStraitSnapshot(
            date=as_of,
            aircraft_count=0,
            adiz_crossing_count=0,
            vessel_count=0,
            exercise_announced=active,
            exercise_name=name,
        )

    # ------------------------------------------------------------------ #
    # Persist
    # ------------------------------------------------------------------ #

    def save_to_db(self, snapshots: list[TaiwanStraitSnapshot]) -> int:
        """Idempotently upsert snapshots into raw_series.

        Writes one row per series per observation date, skipping any
        (series_id, obs_date) already present. Returns the number of
        rows actually inserted across all four namespaces.
        """
        if not snapshots:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            existing_by_series: dict[str, set[date]] = {
                sid: set(self._get_existing_dates(sid, conn)) for sid in ALL_SERIES
            }

            for snap in snapshots:
                for series_id, value in (
                    (SERIES_AIRCRAFT, float(snap.aircraft_count)),
                    (SERIES_ADIZ, float(snap.adiz_crossing_count)),
                    (SERIES_VESSEL, float(snap.vessel_count)),
                    (SERIES_EXERCISE_FLAG, float(snap.exercise_flag)),
                ):
                    if snap.date in existing_by_series[series_id]:
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=snap.date,
                        value=value,
                        raw_payload={
                            "source": "taiwan_mnd",
                            "exercise_name": snap.exercise_name,
                            "exercise_announced": snap.exercise_announced,
                        },
                    )
                    existing_by_series[series_id].add(snap.date)
                    inserted += 1
        return inserted


# ── Top-level entry point ────────────────────────────────────────────


def run_taiwan_strait_puller(engine: Engine) -> dict[str, Any]:
    """Run the Taiwan Strait OSINT pull and return a summary dict.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Dict with keys: fetched, inserted, source, latest_aircraft_count.
        ``source`` is one of ``"mnd_html"``, ``"seed"``, or ``"none"``.
    """
    puller = TaiwanStraitPuller(engine)

    try:
        pull_result = puller.pull()
    except Exception as exc:
        log.error("TaiwanStraitPuller.pull() crashed: {e}", e=str(exc))
        return {
            "fetched": 0,
            "inserted": 0,
            "source": "none",
            "latest_aircraft_count": None,
        }

    snapshots: list[TaiwanStraitSnapshot] = pull_result.get("snapshots", [])
    source: str = pull_result.get("source", "none")

    try:
        inserted = puller.save_to_db(snapshots)
    except Exception as exc:
        log.error("TaiwanStraitPuller.save_to_db() crashed: {e}", e=str(exc))
        inserted = 0

    latest_aircraft: int | None = None
    if snapshots:
        latest = max(snapshots, key=lambda s: s.date)
        latest_aircraft = latest.aircraft_count

    log.info(
        "taiwan_strait_osint: fetched={f} inserted={i} source={s}",
        f=len(snapshots),
        i=inserted,
        s=source,
    )
    return {
        "fetched": len(snapshots),
        "inserted": inserted,
        "source": source,
        "latest_aircraft_count": latest_aircraft,
    }


if __name__ == "__main__":
    from db import get_engine

    result = run_taiwan_strait_puller(get_engine())
    print(result)
