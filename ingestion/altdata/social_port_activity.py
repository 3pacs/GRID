"""
Ground-truth social-feed observation layer for port activity.

Cross-checks reported shipping statistics (CAT-51 LME warehouse,
CAT-52 iron ore port stocks, CAT-82 Drewry/SCFI container freight)
against Reddit + YouTube + nitter + Bilibili post-velocity per port.
Paired with ``ais_ground_truth.py`` (AIS physical presence) to feed
the downstream ``intelligence/shipping_fudge_detector.py``. A
sustained divergence between the statistical feed delta and the
social/AIS delta is the fudge signal.

Why this puller exists
----------------------
Reported port throughput and warehouse inventories are *self-reported*
by the same entities that have a direct interest in making the numbers
look good (exchanges, port authorities, industry indices). When the
statistical feed diverges from what is actually happening on the
ground, the gap is the fudge — and the gap only becomes measurable
when we have an independent, unreportable signal.

Port-spotter communities are that independent signal. Every major
container terminal has a dedicated audience of people who post videos,
photos and short notes about ship arrivals, crane movements, truck
queues, and unusual activity. A sustained **spike** in port-spotter
video uploads (either because something is going on, or because the
port suddenly became interesting) is a lead indicator. A sudden
**drop to zero** — especially at ports with normally-dense coverage
like Los Angeles, Rotterdam, Shanghai — is a lag indicator that
activity has stalled, that access has been restricted, or that the
local community has been told to stop posting.

Data strategy
-------------
Four candidate free public sources, each independent and
graceful-degradable. The puller never blocks on any one failing:

1. **Reddit public JSON API** — `https://www.reddit.com/search.json`
   with a User-Agent header. No auth needed. 60 req/min rate limit.
   We search each port name across a curated subreddit list and count
   posts in the trailing 7 days. This is the MOST RELIABLE free source
   and it is the first one we try for every port.

2. **YouTube Data API v3** — requires ``YOUTUBE_API_KEY`` env var.
   Graceful-degrades to ``None`` when the key is unset. 10K queries
   per day free quota is plenty for a daily 15-port pull.

3. **Nitter public instances** — free Twitter mirrors. We keep a
   fallback list of public instances and rotate on failure. HTML is
   scraped, not parsed via API — nitter is stateless and instance
   availability fluctuates.

4. **Bilibili public search** — `https://search.bilibili.com/all`
   HTML scrape. Only fires for Chinese-named ports
   (Qingdao/青岛, Shanghai/上海, Ningbo/宁波, Tianjin/天津). This is
   the ONLY source that actually sees Chinese social video, which is
   where the Mysteel cross-check bite lives.

Politeness & rate limiting
--------------------------
Every external call is followed by a short 0.5–1.0 second sleep
(`_POLITE_SLEEP_SECONDS`). This keeps us well below Reddit's 60 req/min
and well below nitter's effective per-instance limits. We are
explicitly NOT trying to maximise throughput — this is a daily-cadence
puller with ~45-60 total calls per run, and courtesy matters for free
public sources we do not own.

Series stored (raw_series namespaces)
-------------------------------------
- ``social_port:reddit_posts:<slug>``       — integer post count (7d)
- ``social_port:youtube_videos:<slug>``     — integer video count (7d)
- ``social_port:nitter_tweets:<slug>``      — integer tweet count (7d)
- ``social_port:bilibili_videos:<slug>``    — integer video count (7d)
- ``social_port:composite_velocity:<slug>`` — weighted composite score

Composite weights (see ``compute_composite_velocity``):
- Reddit: 1.0 (baseline, always-available, broadest community)
- YouTube: 2.0 (higher-effort posts, more signal per unit)
- Nitter: 0.5 (cheap to post, noisiest)
- Bilibili: 3.0 (only meaningful channel into Chinese port activity)

Sources contributing ``None`` (unavailable) contribute zero to the
composite. Zero-source days still produce a snapshot with
``composite_velocity == 0``, so the downstream detector always sees a
daily heartbeat row — the absence of signal is itself a signal.
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 20
_POLITE_SLEEP_SECONDS: tuple[float, float] = (0.5, 1.0)

REDDIT_USER_AGENT: str = (
    "grid-social-port-activity/1.0 "
    "(GRID quantitative intelligence; contact: ops@stepdad.finance)"
)

REDDIT_SEARCH_URL: str = "https://www.reddit.com/search.json"
YOUTUBE_SEARCH_URL: str = "https://www.googleapis.com/youtube/v3/search"
BILIBILI_SEARCH_URL: str = "https://search.bilibili.com/all"

# Curated subreddits that consistently host port-spotter content. Order
# matters: we union the search across all of these with the port name.
REDDIT_SUBREDDITS: tuple[str, ...] = (
    "maritime",
    "ShipSpotting",
    "LogisticsAndSupplyChain",
    "WarshipPorn",
    "ports",
)

# Public nitter instances, rotated on failure. These fluctuate over
# time — keep the list conservative and known-stable.
NITTER_INSTANCES: tuple[str, ...] = (
    "https://nitter.net",
    "https://nitter.cz",
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
)

# Trailing window for all four sources.
SEARCH_WINDOW_DAYS: int = 7

# Composite velocity weights — documented, deterministic, testable.
COMPOSITE_WEIGHTS: dict[str, float] = {
    "reddit": 1.0,
    "youtube": 2.0,
    "nitter": 0.5,
    "bilibili": 3.0,
}


# ── Port spec ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SocialPortSpec:
    """Metadata for a single tracked port.

    Attributes:
        slug: Stable snake_case identifier used in series_id namespaces.
        display_name: Human-readable port name.
        chinese_name: Simplified Chinese name, or None for non-Chinese ports.
            Only ports with a non-None ``chinese_name`` fire the Bilibili
            search path — the others short-circuit to ``None``.
        search_keywords: Tuple of alias phrases used for fuzzy searches
            across all four sources. Must be non-empty.
    """

    slug: str
    display_name: str
    chinese_name: str | None
    search_keywords: tuple[str, ...]


# Mirrors CAT-52 top-10 iron-ore ports + CAT-82 container hubs + Taiwan.
# Slugs are lowercase snake_case and are the canonical namespace key.
PORT_SPECS: tuple[SocialPortSpec, ...] = (
    SocialPortSpec(
        "qingdao", "Qingdao", "青岛",
        ("qingdao port", "qingdao terminal", "port of qingdao"),
    ),
    SocialPortSpec(
        "shanghai", "Shanghai", "上海",
        ("shanghai port", "port of shanghai", "yangshan port"),
    ),
    SocialPortSpec(
        "ningbo", "Ningbo-Zhoushan", "宁波",
        ("ningbo port", "ningbo zhoushan", "ningbo terminal"),
    ),
    SocialPortSpec(
        "tianjin", "Tianjin", "天津",
        ("tianjin port", "port of tianjin"),
    ),
    SocialPortSpec(
        "la", "Los Angeles", None,
        ("port of los angeles", "san pedro bay", "pier 400"),
    ),
    SocialPortSpec(
        "long_beach", "Long Beach", None,
        ("port of long beach", "lbct terminal"),
    ),
    SocialPortSpec(
        "ny_nj", "NY/NJ", None,
        ("port of new york", "newark container", "bayonne port"),
    ),
    SocialPortSpec(
        "rotterdam", "Rotterdam", None,
        ("port of rotterdam", "rotterdam maasvlakte", "europoort"),
    ),
    SocialPortSpec(
        "antwerp", "Antwerp", None,
        ("port of antwerp", "antwerp bruges port"),
    ),
    SocialPortSpec(
        "hamburg", "Hamburg", None,
        ("hamburg port", "hamburger hafen", "port of hamburg"),
    ),
    SocialPortSpec(
        "singapore", "Singapore", None,
        ("port of singapore", "psa terminal", "tuas mega port"),
    ),
    SocialPortSpec(
        "port_klang", "Port Klang", None,
        ("port klang", "pelabuhan klang", "westports malaysia"),
    ),
    SocialPortSpec(
        "jebel_ali", "Jebel Ali", None,
        ("jebel ali port", "dp world dubai"),
    ),
    SocialPortSpec(
        "jeddah", "Jeddah", None,
        ("jeddah islamic port", "king abdullah port"),
    ),
    SocialPortSpec(
        "kaohsiung", "Kaohsiung", None,
        ("kaohsiung port", "port of kaohsiung", "high port taiwan"),
    ),
)


# ── Snapshot ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SocialActivitySnapshot:
    """One day of per-port social activity across four sources.

    Any source can be ``None`` (unavailable). A ``None`` source does not
    crash the snapshot — it contributes 0 to ``composite_velocity``.

    Attributes:
        date: Observation date (UTC calendar day the pull ran).
        port_slug: Matches ``SocialPortSpec.slug``.
        reddit_post_count: Reddit search post count over window.
        youtube_video_count: YouTube Data API v3 video count, or None.
        nitter_tweet_count: Nitter HTML tweet count, or None.
        bilibili_video_count: Bilibili HTML video count, or None.
            Always None for non-Chinese ports.
        composite_velocity: Weighted sum (see COMPOSITE_WEIGHTS).
            Always >= 0.
    """

    date: date
    port_slug: str
    reddit_post_count: int
    youtube_video_count: int | None
    nitter_tweet_count: int | None
    bilibili_video_count: int | None
    composite_velocity: float


# ── Series IDs ───────────────────────────────────────────────────────


SERIES_PREFIX: str = "social_port"
SERIES_REDDIT: str = "reddit_posts"
SERIES_YOUTUBE: str = "youtube_videos"
SERIES_NITTER: str = "nitter_tweets"
SERIES_BILIBILI: str = "bilibili_videos"
SERIES_COMPOSITE: str = "composite_velocity"

ALL_SERIES_SUFFIXES: tuple[str, ...] = (
    SERIES_REDDIT,
    SERIES_YOUTUBE,
    SERIES_NITTER,
    SERIES_BILIBILI,
    SERIES_COMPOSITE,
)


def _series_id(suffix: str, slug: str) -> str:
    """Build a namespaced series_id for raw_series insertion."""
    return f"{SERIES_PREFIX}:{suffix}:{slug}"


# ── Pure helpers ─────────────────────────────────────────────────────


def compute_composite_velocity(
    reddit: int,
    youtube: int | None,
    nitter: int | None,
    bilibili: int | None,
) -> float:
    """Weighted sum of the four source counts, treating None as zero.

    Weights are fixed and documented in ``COMPOSITE_WEIGHTS``:
        reddit=1.0, youtube=2.0, nitter=0.5, bilibili=3.0

    The function is deterministic, pure, and never raises. Negative
    inputs are clamped to 0 (a defensive guard — the underlying
    fetchers never return negatives, but callers may pass arbitrary
    ints).

    Parameters:
        reddit: Reddit post count (int, always available — the floor).
        youtube: YouTube video count, or None if API key unset / source
            failed.
        nitter: Nitter tweet count, or None if all instances failed.
        bilibili: Bilibili video count, or None if not a Chinese port or
            the scrape failed.

    Returns:
        Non-negative weighted sum.
    """
    def _clean(v: int | None) -> float:
        if v is None:
            return 0.0
        f = float(v)
        return f if f > 0 else 0.0

    score = (
        _clean(reddit) * COMPOSITE_WEIGHTS["reddit"]
        + _clean(youtube) * COMPOSITE_WEIGHTS["youtube"]
        + _clean(nitter) * COMPOSITE_WEIGHTS["nitter"]
        + _clean(bilibili) * COMPOSITE_WEIGHTS["bilibili"]
    )
    return max(score, 0.0)


def _polite_sleep() -> None:
    """Sleep 0.5–1.0 seconds between external calls. Never raises."""
    lo, hi = _POLITE_SLEEP_SECONDS
    try:
        time.sleep(random.uniform(lo, hi))
    except Exception:
        pass


def _build_reddit_query(port: SocialPortSpec) -> str:
    """Compose the reddit search q= parameter for a port.

    Uses the primary display name plus the first alias, joined with
    subreddit filters. Reddit's search accepts multiple ``subreddit:``
    qualifiers ORed together via the `q` parameter.
    """
    primary = f'"{port.display_name}"'
    alias = f'"{port.search_keywords[0]}"' if port.search_keywords else ""
    subreddit_clause = " OR ".join(f"subreddit:{sr}" for sr in REDDIT_SUBREDDITS)
    if alias:
        return f"({primary} OR {alias}) ({subreddit_clause})"
    return f"{primary} ({subreddit_clause})"


# ── Per-source fetchers — ALL return int | None, NEVER raise ────────


def _fetch_reddit_counts(
    port: SocialPortSpec,
    window_days: int = SEARCH_WINDOW_DAYS,
) -> int | None:
    """Fetch recent Reddit post count for a port.

    Uses the public reddit.com/search.json endpoint with a polite
    User-Agent. Returns an integer post count, or None on any failure
    (rate limit, malformed JSON, network error). Never raises.

    Parameters:
        port: SocialPortSpec to search for.
        window_days: Lookback window. Reddit's `t=` maps week/month/year,
            so we round up to week for <=7d and month otherwise.
    """
    query = _build_reddit_query(port)
    params: dict[str, str | int] = {
        "q": query,
        "sort": "new",
        "limit": 50,
        "t": "week" if window_days <= 7 else "month",
    }
    headers = {"User-Agent": REDDIT_USER_AGENT, "Accept": "application/json"}

    try:
        resp = requests.get(
            REDDIT_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
    except Exception as exc:
        log.warning("reddit fetch network error for {p}: {e}", p=port.slug, e=str(exc))
        return None

    status = getattr(resp, "status_code", None)
    if status is None or status >= 400:
        log.warning(
            "reddit fetch HTTP {s} for {p}", s=status, p=port.slug,
        )
        return None

    try:
        payload = resp.json()
    except (ValueError, json.JSONDecodeError) as exc:
        log.warning("reddit fetch bad JSON for {p}: {e}", p=port.slug, e=str(exc))
        return None
    except Exception as exc:
        log.warning("reddit fetch json() raised for {p}: {e}", p=port.slug, e=str(exc))
        return None

    if not isinstance(payload, dict):
        return None

    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    children = data.get("children")
    if not isinstance(children, list):
        return None

    # Reddit returns whatever matched the query; filter to the window
    # if the post has a created_utc field.
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=window_days)
    ).timestamp()
    count = 0
    for item in children:
        if not isinstance(item, dict):
            continue
        entry = item.get("data")
        if not isinstance(entry, dict):
            continue
        created = entry.get("created_utc")
        if isinstance(created, (int, float)) and created < cutoff:
            continue
        count += 1
    return count


def _fetch_youtube_counts(
    port: SocialPortSpec,
    api_key: str | None,
    window_days: int = SEARCH_WINDOW_DAYS,
) -> int | None:
    """Fetch recent YouTube video count for a port via Data API v3.

    Graceful-degrades to None when ``api_key`` is None — no network call
    is made in that case. Any other failure also returns None. Never
    raises.
    """
    if not api_key:
        return None

    published_after = (
        datetime.now(timezone.utc) - timedelta(days=window_days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    q = f'"{port.display_name}" (port OR terminal OR harbor)'
    params: dict[str, str | int] = {
        "part": "snippet",
        "q": q,
        "type": "video",
        "publishedAfter": published_after,
        "maxResults": 50,
        "key": api_key,
    }
    try:
        resp = requests.get(
            YOUTUBE_SEARCH_URL, params=params, timeout=_REQUEST_TIMEOUT
        )
    except Exception as exc:
        log.warning("youtube fetch network error for {p}: {e}", p=port.slug, e=str(exc))
        return None

    status = getattr(resp, "status_code", None)
    if status is None or status >= 400:
        log.warning("youtube fetch HTTP {s} for {p}", s=status, p=port.slug)
        return None

    try:
        payload = resp.json()
    except Exception as exc:
        log.warning("youtube fetch bad JSON for {p}: {e}", p=port.slug, e=str(exc))
        return None

    if not isinstance(payload, dict):
        return None
    items = payload.get("items")
    if isinstance(items, list):
        return len(items)
    total_results = payload.get("pageInfo", {}).get("totalResults")
    if isinstance(total_results, int):
        return total_results
    return None


# Nitter HTML contains one <div class="timeline-item"> per tweet. Count
# them rather than fully parse.
_NITTER_TWEET_RE: re.Pattern[str] = re.compile(
    r'class="timeline-item"', re.IGNORECASE
)


def _fetch_nitter_counts(
    port: SocialPortSpec,
    instances: list[str] | None = None,
) -> int | None:
    """Fetch tweet count via a fallback list of public nitter instances.

    Rotates through ``instances`` on any failure. Returns the first
    successful count, or None if every instance fails. Never raises.
    """
    instance_list = list(instances) if instances else list(NITTER_INSTANCES)
    if not instance_list:
        return None

    query = f'"{port.display_name}" port'
    params: dict[str, str] = {"f": "tweets", "q": query}
    headers = {"User-Agent": REDDIT_USER_AGENT, "Accept": "text/html"}

    for base in instance_list:
        url = f"{base.rstrip('/')}/search"
        try:
            resp = requests.get(
                url, params=params, headers=headers, timeout=_REQUEST_TIMEOUT
            )
        except Exception as exc:
            log.info(
                "nitter {b} failed for {p}: {e}",
                b=base, p=port.slug, e=str(exc),
            )
            continue

        status = getattr(resp, "status_code", None)
        if status is None or status >= 400:
            log.info(
                "nitter {b} HTTP {s} for {p}",
                b=base, s=status, p=port.slug,
            )
            continue

        html = getattr(resp, "text", "") or ""
        if not html:
            continue

        matches = _NITTER_TWEET_RE.findall(html)
        return len(matches)

    return None


_BILIBILI_VIDEO_RE: re.Pattern[str] = re.compile(
    r'class="bili-video-card|/video/BV[0-9A-Za-z]+', re.IGNORECASE
)


def _fetch_bilibili_counts(port: SocialPortSpec) -> int | None:
    """Fetch Bilibili video count for a Chinese-named port.

    Short-circuits to None for ports with ``chinese_name is None``.
    Never raises.
    """
    if port.chinese_name is None:
        return None

    params: dict[str, str] = {"keyword": port.chinese_name}
    headers = {"User-Agent": REDDIT_USER_AGENT, "Accept": "text/html"}

    try:
        resp = requests.get(
            BILIBILI_SEARCH_URL,
            params=params,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
    except Exception as exc:
        log.warning("bilibili fetch network error for {p}: {e}", p=port.slug, e=str(exc))
        return None

    status = getattr(resp, "status_code", None)
    if status is None or status >= 400:
        log.warning("bilibili fetch HTTP {s} for {p}", s=status, p=port.slug)
        return None

    html = getattr(resp, "text", "") or ""
    if not html:
        return None

    hits = _BILIBILI_VIDEO_RE.findall(html)
    # Each video card is typically matched twice (class + URL). Halve
    # to approximate a distinct count. Never return <0.
    return max(len(hits) // 2, 0)


# ── Puller ───────────────────────────────────────────────────────────


class SocialPortActivityPuller(BasePuller):
    """Daily social-feed port activity puller.

    Iterates over every port in ``PORT_SPECS``, runs four independent
    source fetchers, and emits one ``SocialActivitySnapshot`` per port.
    Every source failure is trapped locally — no single failure can
    stop the pull. Even a zero-source day produces a snapshot with
    ``composite_velocity == 0`` so the downstream detector always gets
    a daily heartbeat.
    """

    SOURCE_NAME: str = "social_port_activity"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.reddit.com",
        "cost_tier": "FREE",
        "latency_class": "DAILY",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 55,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._youtube_api_key: str | None = os.environ.get("YOUTUBE_API_KEY") or None
        log.info(
            "SocialPortActivityPuller initialised — source_id={sid} "
            "youtube_enabled={yt}",
            sid=self.source_id,
            yt=self._youtube_api_key is not None,
        )

    # ------------------------------------------------------------------ #
    # Fetch
    # ------------------------------------------------------------------ #

    def pull(self) -> dict[str, Any]:
        """Run all four source fetchers for every port.

        Returns:
            Dict with:
              - snapshots: list[SocialActivitySnapshot]
              - source_mix: dict counting successful-source firings
              - error: str | None
        """
        today = date.today()
        snapshots: list[SocialActivitySnapshot] = []
        source_mix: dict[str, int] = {
            "reddit": 0,
            "youtube": 0,
            "nitter": 0,
            "bilibili": 0,
        }

        for port in PORT_SPECS:
            reddit_count: int | None = None
            youtube_count: int | None = None
            nitter_count: int | None = None
            bilibili_count: int | None = None

            # Reddit — always fires
            try:
                reddit_count = _fetch_reddit_counts(port, SEARCH_WINDOW_DAYS)
            except Exception as exc:
                log.warning("reddit fetcher raised for {p}: {e}", p=port.slug, e=str(exc))
                reddit_count = None
            _polite_sleep()

            # YouTube — graceful-degraded when key unset
            try:
                youtube_count = _fetch_youtube_counts(
                    port, self._youtube_api_key, SEARCH_WINDOW_DAYS
                )
            except Exception as exc:
                log.warning("youtube fetcher raised for {p}: {e}", p=port.slug, e=str(exc))
                youtube_count = None
            if self._youtube_api_key is not None:
                _polite_sleep()

            # Nitter — rotates instances internally
            try:
                nitter_count = _fetch_nitter_counts(port, list(NITTER_INSTANCES))
            except Exception as exc:
                log.warning("nitter fetcher raised for {p}: {e}", p=port.slug, e=str(exc))
                nitter_count = None
            _polite_sleep()

            # Bilibili — only for Chinese ports
            try:
                bilibili_count = _fetch_bilibili_counts(port)
            except Exception as exc:
                log.warning("bilibili fetcher raised for {p}: {e}", p=port.slug, e=str(exc))
                bilibili_count = None
            if port.chinese_name is not None:
                _polite_sleep()

            if reddit_count is not None:
                source_mix["reddit"] += 1
            if youtube_count is not None:
                source_mix["youtube"] += 1
            if nitter_count is not None:
                source_mix["nitter"] += 1
            if bilibili_count is not None:
                source_mix["bilibili"] += 1

            reddit_for_composite = reddit_count if reddit_count is not None else 0
            composite = compute_composite_velocity(
                reddit_for_composite, youtube_count, nitter_count, bilibili_count
            )

            snap = SocialActivitySnapshot(
                date=today,
                port_slug=port.slug,
                reddit_post_count=reddit_for_composite,
                youtube_video_count=youtube_count,
                nitter_tweet_count=nitter_count,
                bilibili_video_count=bilibili_count,
                composite_velocity=composite,
            )
            snapshots.append(snap)

        return {
            "snapshots": snapshots,
            "source_mix": source_mix,
            "error": None,
        }

    # ------------------------------------------------------------------ #
    # Persist
    # ------------------------------------------------------------------ #

    def save_to_db(self, snapshots: list[SocialActivitySnapshot]) -> int:
        """Idempotently upsert snapshots into raw_series.

        Writes five rows per port per observation date (reddit, youtube,
        nitter, bilibili, composite). Skips any (series_id, obs_date)
        already present. Returns the number of rows actually inserted.

        Null source counts are stored as 0.0 in raw_series (the
        downstream detector reads the raw_payload JSON for the
        "was-it-null?" flag, which we set here).
        """
        if not snapshots:
            return 0

        inserted = 0
        with self.engine.begin() as conn:
            # Pre-fetch existing dates for every series/port combo.
            existing_cache: dict[str, set[date]] = {}
            for snap in snapshots:
                for suffix in ALL_SERIES_SUFFIXES:
                    sid = _series_id(suffix, snap.port_slug)
                    if sid not in existing_cache:
                        existing_cache[sid] = set(
                            self._get_existing_dates(sid, conn)
                        )

            for snap in snapshots:
                rows: tuple[tuple[str, float, dict[str, Any]], ...] = (
                    (
                        _series_id(SERIES_REDDIT, snap.port_slug),
                        float(snap.reddit_post_count),
                        {
                            "source": "reddit_public_json",
                            "available": True,
                        },
                    ),
                    (
                        _series_id(SERIES_YOUTUBE, snap.port_slug),
                        float(snap.youtube_video_count or 0),
                        {
                            "source": "youtube_data_api_v3",
                            "available": snap.youtube_video_count is not None,
                        },
                    ),
                    (
                        _series_id(SERIES_NITTER, snap.port_slug),
                        float(snap.nitter_tweet_count or 0),
                        {
                            "source": "nitter_html",
                            "available": snap.nitter_tweet_count is not None,
                        },
                    ),
                    (
                        _series_id(SERIES_BILIBILI, snap.port_slug),
                        float(snap.bilibili_video_count or 0),
                        {
                            "source": "bilibili_html",
                            "available": snap.bilibili_video_count is not None,
                        },
                    ),
                    (
                        _series_id(SERIES_COMPOSITE, snap.port_slug),
                        float(snap.composite_velocity),
                        {
                            "source": "composite",
                            "weights": COMPOSITE_WEIGHTS,
                        },
                    ),
                )

                for series_id, value, payload in rows:
                    if snap.date in existing_cache.get(series_id, set()):
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=snap.date,
                        value=value,
                        raw_payload=payload,
                    )
                    existing_cache.setdefault(series_id, set()).add(snap.date)
                    inserted += 1
        return inserted


# ── Top-level entry point ────────────────────────────────────────────


def run_social_port_activity_puller(engine: Engine) -> dict[str, Any]:
    """Run the social-port-activity pull and return a summary dict.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Dict with keys:
          - fetched: int — snapshots produced (one per port)
          - inserted: int — rows inserted into raw_series
          - ports_done: int — number of ports that produced a snapshot
          - source_mix: dict[str, int] — per-source successful firings
    """
    puller = SocialPortActivityPuller(engine)

    try:
        pull_result = puller.pull()
    except Exception as exc:
        log.error("SocialPortActivityPuller.pull() crashed: {e}", e=str(exc))
        return {
            "fetched": 0,
            "inserted": 0,
            "ports_done": 0,
            "source_mix": {"reddit": 0, "youtube": 0, "nitter": 0, "bilibili": 0},
        }

    snapshots: list[SocialActivitySnapshot] = pull_result.get("snapshots", [])
    source_mix: dict[str, int] = pull_result.get(
        "source_mix", {"reddit": 0, "youtube": 0, "nitter": 0, "bilibili": 0}
    )

    try:
        inserted = puller.save_to_db(snapshots)
    except Exception as exc:
        log.error("SocialPortActivityPuller.save_to_db() crashed: {e}", e=str(exc))
        inserted = 0

    log.info(
        "social_port_activity: fetched={f} inserted={i} ports={p} mix={m}",
        f=len(snapshots),
        i=inserted,
        p=len(snapshots),
        m=source_mix,
    )
    return {
        "fetched": len(snapshots),
        "inserted": inserted,
        "ports_done": len(snapshots),
        "source_mix": source_mix,
    }


if __name__ == "__main__":
    from db import get_engine

    print(run_social_port_activity_puller(get_engine()))
