"""
GRID legislative tracker — bills, hearings, and votes from Congress.gov.

Pulls data from the free Congress.gov API (v3):
  - Bills introduced in the last 7 days
  - Upcoming committee hearings
  - Recent roll-call votes

For each bill/hearing, topics are extracted and mapped to affected
sectors/tickers via keyword matching.  This enables the key intelligence
function: detecting committee members trading in sectors their committee
is actively legislating.

Series pattern: LEGISLATION:{bill_id}:{action}
Fields: title, sponsor, committee, status, date, subjects, affected_tickers

API key: set CONGRESS_GOV_API_KEY in environment.  If absent the puller
logs a warning and gracefully skips (no crash).
"""

from __future__ import annotations

import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── API configuration ────────────────────────────────────────────────────

_API_KEY_ENV: str = "CONGRESS_GOV_API_KEY"
_BASE_URL: str = "https://api.congress.gov/v3"

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.0  # Congress.gov asks for <=1 req/s

# ── Topic-to-sector/ticker mapping ───────────────────────────────────────

TOPIC_SECTOR_MAP: dict[str, list[str]] = {
    "artificial intelligence": ["NVDA", "MSFT", "GOOGL", "META", "XLK"],
    "technology": ["XLK", "AAPL", "MSFT", "GOOGL"],
    "semiconductor": ["NVDA", "AMD", "INTC", "SMH"],
    "defense": ["RTX", "LMT", "NOC", "GD", "ITA"],
    "military": ["RTX", "LMT", "NOC", "GD", "ITA"],
    "armed forces": ["RTX", "LMT", "NOC", "GD", "ITA"],
    "healthcare": ["XLV", "UNH", "JNJ", "PFE"],
    "health": ["XLV", "UNH", "JNJ", "PFE"],
    "pharmaceutical": ["XLV", "PFE", "MRK", "LLY", "ABBV"],
    "drug pricing": ["XLV", "PFE", "MRK", "LLY"],
    "medicare": ["XLV", "UNH", "HUM", "CNC"],
    "energy": ["XLE", "XOM", "CVX"],
    "oil": ["XLE", "XOM", "CVX", "OXY"],
    "natural gas": ["XLE", "EQT", "LNG"],
    "renewable energy": ["TAN", "ICLN", "ENPH", "FSLR"],
    "solar": ["TAN", "ENPH", "FSLR"],
    "banking": ["XLF", "JPM", "BAC", "GS"],
    "financial services": ["XLF", "JPM", "BAC", "GS", "MS"],
    "insurance": ["XLF", "BRK-B", "MET", "AIG"],
    "cannabis": ["TLRY", "CGC"],
    "marijuana": ["TLRY", "CGC"],
    "crypto": ["BTC", "COIN", "MSTR"],
    "cryptocurrency": ["BTC", "COIN", "MSTR"],
    "digital assets": ["BTC", "COIN", "MSTR"],
    "blockchain": ["BTC", "COIN", "MSTR"],
    "agriculture": ["DBA", "ADM", "DE", "MOS"],
    "farming": ["DBA", "ADM", "DE", "MOS"],
    "transportation": ["XTN", "UNP", "CSX", "DAL", "UAL"],
    "aviation": ["BA", "DAL", "UAL", "LUV"],
    "airline": ["DAL", "UAL", "LUV", "AAL"],
    "infrastructure": ["PAVE", "CAT", "VMC", "MLM"],
    "telecommunications": ["XLC", "T", "VZ", "TMUS"],
    "cybersecurity": ["HACK", "CRWD", "PANW", "ZS"],
    "housing": ["XHB", "LEN", "DHI", "TOL"],
    "real estate": ["XLRE", "VNQ", "AMT", "PLD"],
    "education": ["LOPE", "CHGG"],
    "student loan": ["SLM", "NAVI"],
    "tax": ["XLF", "INTU", "HRB"],
    "trade": ["EEM", "FXI", "EWJ"],
    "tariff": ["EEM", "FXI", "EWJ"],
    "china": ["FXI", "BABA", "JD", "PDD"],
    "climate": ["ICLN", "TAN", "ENPH"],
    "environment": ["ICLN", "TAN", "ENPH"],
    "social media": ["META", "SNAP", "PINS"],
    "privacy": ["META", "GOOGL", "MSFT"],
    "antitrust": ["GOOGL", "META", "AMZN", "AAPL", "MSFT"],
    "space": ["LMT", "BA", "RKLB", "SPCE"],
    "nuclear": ["CCJ", "UEC", "NNE"],
    "uranium": ["CCJ", "UEC", "NNE"],
    "water": ["PHO", "AWK", "WTR"],
    "lithium": ["ALB", "SQM", "LTHM"],
    "electric vehicle": ["TSLA", "RIVN", "LCID", "LI"],
    "autonomous vehicle": ["TSLA", "GOOGL", "GM"],
    "quantum computing": ["IONQ", "RGTI", "QUBT"],
}

# Committees that oversee specific sectors (for legislative trading detection)
COMMITTEE_SECTOR_MAP: dict[str, list[str]] = {
    "financial services": ["XLF", "JPM", "BAC", "GS", "COIN"],
    "banking": ["XLF", "JPM", "BAC", "GS"],
    "finance": ["XLF", "JPM", "BAC", "GS"],
    "energy and commerce": ["XLE", "XLV", "XLC", "XOM", "CVX", "UNH"],
    "energy and natural resources": ["XLE", "XOM", "CVX"],
    "armed services": ["ITA", "RTX", "LMT", "NOC", "GD"],
    "commerce": ["XLC", "XLY", "AMZN", "META"],
    "health": ["XLV", "UNH", "JNJ", "PFE"],
    "agriculture": ["DBA", "ADM", "DE", "MOS"],
    "judiciary": ["GOOGL", "META", "AMZN", "AAPL"],
    "intelligence": ["XLK", "PANW", "CRWD"],
    "transportation": ["XTN", "DAL", "UAL", "BA"],
    "ways and means": ["XLF", "INTU"],
    "appropriations": [],  # broad — affects everything
    "science": ["XLK", "NVDA", "IONQ"],
    "homeland security": ["PANW", "CRWD", "LMT"],
}


def _match_topics(text_blob: str) -> tuple[list[str], list[str]]:
    """Match text against TOPIC_SECTOR_MAP, returning (topics, tickers).

    Parameters:
        text_blob: Combined title + subjects + description text.

    Returns:
        Tuple of (matched_topics, unique_affected_tickers).
    """
    text_lower = text_blob.lower()
    matched_topics: list[str] = []
    tickers: set[str] = set()

    for topic, topic_tickers in TOPIC_SECTOR_MAP.items():
        if topic in text_lower:
            matched_topics.append(topic)
            tickers.update(topic_tickers)

    return matched_topics, sorted(tickers)


def _tickers_for_committee(committee_name: str) -> list[str]:
    """Return tickers under a committee's jurisdiction.

    Parameters:
        committee_name: Committee name string.

    Returns:
        List of tickers the committee oversees.
    """
    name_lower = committee_name.lower()
    tickers: set[str] = set()
    for key, key_tickers in COMMITTEE_SECTOR_MAP.items():
        if key in name_lower:
            tickers.update(key_tickers)
    return sorted(tickers)


class LegislationPuller(BasePuller):
    """Pulls bills, hearings, and votes from the Congress.gov API.

    Stores each bill/hearing as a raw_series row with topic-to-ticker
    mapping in the payload.  Emits signal_sources rows for downstream
    trust scoring and legislative-trading detection.

    Series pattern: LEGISLATION:{bill_id}:{action}
    Value: 1.0 for introduced, 2.0 for passed committee, 3.0 for passed
           chamber, 4.0 for enacted.  Hearings use 0.5.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for CONGRESS_GOV.
        api_key: Congress.gov API key from environment.
    """

    SOURCE_NAME: str = "CONGRESS_GOV"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.congress.gov/v3",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 35,
    }

    # Map bill status to a numeric score for value column
    STATUS_SCORES: dict[str, float] = {
        "introduced": 1.0,
        "referred": 1.0,
        "reported": 2.0,
        "passed_committee": 2.0,
        "passed_house": 3.0,
        "passed_senate": 3.0,
        "enacted": 4.0,
        "hearing": 0.5,
        "vote": 1.5,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the legislation puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.api_key: str = os.environ.get(_API_KEY_ENV, "")
        if not self.api_key:
            log.warning(
                "LegislationPuller: {env} not set — Congress.gov API "
                "calls will be skipped.  Get a free key at api.congress.gov",
                env=_API_KEY_ENV,
            )
        super().__init__(db_engine)
        log.info(
            "LegislationPuller initialised — source_id={sid}, api_key={'SET' if self.api_key else 'MISSING'}",
            sid=self.source_id,
        )

    def _has_api_key(self) -> bool:
        """Check if API key is available, log warning if not."""
        if not self.api_key:
            log.warning(
                "LegislationPuller: no Congress.gov API key — skipping pull"
            )
            return False
        return True

    # ── API helpers ──────────────────────────────────────────────────────

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _api_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make an authenticated GET request to Congress.gov API.

        Parameters:
            endpoint: API path relative to base URL (e.g. '/bill').
            params: Additional query parameters.

        Returns:
            Parsed JSON response dict.

        Raises:
            requests.RequestException: On HTTP errors after retries.
        """
        url = f"{_BASE_URL}{endpoint}"
        req_params = {"api_key": self.api_key, "format": "json"}
        if params:
            req_params.update(params)

        headers = {
            "Accept": "application/json",
            "User-Agent": "GRID-DataPuller/1.0",
        }

        resp = requests.get(url, params=req_params, headers=headers, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()

        time.sleep(_RATE_LIMIT_DELAY)
        return resp.json()

    # ── Bills ────────────────────────────────────────────────────────────

    def _fetch_recent_bills(self, days_back: int = 7) -> list[dict[str, Any]]:
        """Fetch bills introduced in the last N days.

        Parameters:
            days_back: How many days back to search.

        Returns:
            List of bill dicts from the API.
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()
        data = self._api_get("/bill", params={
            "fromDateTime": f"{cutoff}T00:00:00Z",
            "sort": "updateDate+desc",
            "limit": 250,
        })
        bills = data.get("bills", [])
        log.info("Congress.gov: fetched {n} bills from last {d} days", n=len(bills), d=days_back)
        return bills

    def _parse_bill(self, bill: dict[str, Any]) -> dict[str, Any] | None:
        """Parse a bill API record into a normalised dict.

        Parameters:
            bill: Raw bill dict from Congress.gov API.

        Returns:
            Normalised bill dict, or None if unparseable.
        """
        try:
            bill_type = bill.get("type", "").lower()
            bill_number = bill.get("number", "")
            congress = bill.get("congress", "")
            bill_id = f"{congress}-{bill_type}{bill_number}"

            title = bill.get("title", "")
            # Latest action for status
            latest_action = bill.get("latestAction", {})
            action_text = latest_action.get("text", "")
            action_date_str = latest_action.get("actionDate", "")

            # Determine status from action text
            status = "introduced"
            action_lower = action_text.lower()
            if "enacted" in action_lower or "became public law" in action_lower:
                status = "enacted"
            elif "passed house" in action_lower:
                status = "passed_house"
            elif "passed senate" in action_lower:
                status = "passed_senate"
            elif "reported" in action_lower or "ordered to be reported" in action_lower:
                status = "reported"
            elif "referred to" in action_lower:
                status = "referred"

            # Parse date
            try:
                obs_date = date.fromisoformat(action_date_str[:10]) if action_date_str else date.today()
            except (ValueError, TypeError):
                obs_date = date.today()

            # Sponsor
            sponsors = bill.get("sponsors", [])
            sponsor_name = ""
            sponsor_party = ""
            sponsor_state = ""
            if sponsors:
                sp = sponsors[0] if isinstance(sponsors, list) else sponsors
                sponsor_name = sp.get("fullName", sp.get("name", ""))
                sponsor_party = sp.get("party", "")
                sponsor_state = sp.get("state", "")

            # Committees
            committees_list = bill.get("committees", {})
            committee_names: list[str] = []
            if isinstance(committees_list, dict):
                for c in committees_list.get("committees", []):
                    committee_names.append(c.get("name", ""))
            elif isinstance(committees_list, list):
                for c in committees_list:
                    committee_names.append(c.get("name", ""))

            # Subjects / policy area
            policy_area = bill.get("policyArea", {})
            policy_name = policy_area.get("name", "") if isinstance(policy_area, dict) else ""
            subjects_list = bill.get("subjects", {})
            subject_names: list[str] = []
            if isinstance(subjects_list, dict):
                for s in subjects_list.get("legislativeSubjects", []):
                    subject_names.append(s.get("name", ""))

            # Build combined text for topic matching
            text_blob = " ".join([
                title,
                policy_name,
                " ".join(subject_names),
                " ".join(committee_names),
            ])

            matched_topics, affected_tickers = _match_topics(text_blob)

            # Also add tickers from committee jurisdiction
            for cname in committee_names:
                affected_tickers = sorted(
                    set(affected_tickers) | set(_tickers_for_committee(cname))
                )

            return {
                "bill_id": bill_id,
                "title": title,
                "type": bill_type,
                "congress": congress,
                "sponsor": sponsor_name,
                "sponsor_party": sponsor_party,
                "sponsor_state": sponsor_state,
                "committees": committee_names,
                "status": status,
                "action_text": action_text,
                "obs_date": obs_date,
                "policy_area": policy_name,
                "subjects": subject_names,
                "matched_topics": matched_topics,
                "affected_tickers": affected_tickers,
            }

        except Exception as exc:
            log.debug("Failed to parse bill: {e}", e=str(exc))
            return None

    # ── Hearings ─────────────────────────────────────────────────────────

    def _fetch_hearings(self, days_ahead: int = 14) -> list[dict[str, Any]]:
        """Fetch upcoming committee hearings.

        Parameters:
            days_ahead: How many days ahead to look for hearings.

        Returns:
            List of hearing dicts from the API.
        """
        data = self._api_get("/hearing", params={
            "limit": 250,
            "sort": "date+desc",
        })
        hearings = data.get("hearings", [])
        log.info("Congress.gov: fetched {n} hearings", n=len(hearings))

        # Filter to recent/upcoming hearings
        cutoff_past = date.today() - timedelta(days=7)
        cutoff_future = date.today() + timedelta(days=days_ahead)
        filtered = []
        for h in hearings:
            date_str = h.get("date", "")
            try:
                h_date = date.fromisoformat(date_str[:10])
                if cutoff_past <= h_date <= cutoff_future:
                    filtered.append(h)
            except (ValueError, TypeError):
                filtered.append(h)  # keep if we can't parse date

        return filtered

    def _parse_hearing(self, hearing: dict[str, Any]) -> dict[str, Any] | None:
        """Parse a hearing API record into a normalised dict.

        Parameters:
            hearing: Raw hearing dict from Congress.gov API.

        Returns:
            Normalised hearing dict, or None if unparseable.
        """
        try:
            chamber = hearing.get("chamber", "")
            congress = hearing.get("congress", "")
            number = hearing.get("number", hearing.get("jacketNumber", ""))
            hearing_id = f"{congress}-hearing-{chamber}-{number}"

            title = hearing.get("title", "")
            date_str = hearing.get("date", "")

            try:
                obs_date = date.fromisoformat(date_str[:10]) if date_str else date.today()
            except (ValueError, TypeError):
                obs_date = date.today()

            # Committee info
            committees = hearing.get("committees", [])
            committee_names: list[str] = []
            if isinstance(committees, list):
                for c in committees:
                    committee_names.append(c.get("name", ""))
            elif isinstance(committees, dict):
                committee_names.append(committees.get("name", ""))

            # Topic matching
            text_blob = " ".join([title] + committee_names)
            matched_topics, affected_tickers = _match_topics(text_blob)

            for cname in committee_names:
                affected_tickers = sorted(
                    set(affected_tickers) | set(_tickers_for_committee(cname))
                )

            return {
                "hearing_id": hearing_id,
                "title": title,
                "chamber": chamber,
                "congress": congress,
                "committees": committee_names,
                "obs_date": obs_date,
                "matched_topics": matched_topics,
                "affected_tickers": affected_tickers,
            }

        except Exception as exc:
            log.debug("Failed to parse hearing: {e}", e=str(exc))
            return None

    # ── Votes ────────────────────────────────────────────────────────────

    def _fetch_recent_votes(self, chamber: str = "house", days_back: int = 7) -> list[dict[str, Any]]:
        """Fetch recent roll-call votes.

        Parameters:
            chamber: 'house' or 'senate'.
            days_back: How many days back to search.

        Returns:
            List of vote dicts from the API.
        """
        # Congress.gov vote endpoint uses current congress
        current_congress = 119  # 2025-2027
        data = self._api_get(
            f"/vote/{current_congress}/{chamber}",
            params={"limit": 50, "sort": "date+desc"},
        )
        votes = data.get("votes", [])
        log.info(
            "Congress.gov: fetched {n} {c} votes",
            n=len(votes),
            c=chamber,
        )

        cutoff = date.today() - timedelta(days=days_back)
        filtered = []
        for v in votes:
            date_str = v.get("date", "")
            try:
                v_date = date.fromisoformat(date_str[:10])
                if v_date >= cutoff:
                    filtered.append(v)
            except (ValueError, TypeError):
                filtered.append(v)

        return filtered

    # ── Per-member vote records (hypocrisy detector) ──────────────────────

    def _fetch_vote_members(
        self, congress: int, chamber: str, roll_call: int | str,
    ) -> list[dict[str, Any]]:
        """Fetch member-level breakdown for a single roll-call vote.

        Uses Congress.gov API: /vote/{congress}/{chamber}/{rollCall}

        Parameters:
            congress: Congress number (e.g. 119).
            chamber: 'house' or 'senate'.
            roll_call: Roll call number.

        Returns:
            List of member vote dicts with name, party, state, vote position.
        """
        data = self._api_get(
            f"/vote/{congress}/{chamber}/{roll_call}",
        )
        vote_data = data.get("vote", {})
        members_block = vote_data.get("members", [])

        members: list[dict[str, Any]] = []
        for m in members_block:
            member_name = m.get("fullName", m.get("name", ""))
            vote_position = m.get("votePosition", m.get("vote", ""))
            party = m.get("party", "")
            state = m.get("state", "")
            bioguide = m.get("bioguideId", m.get("member_id", ""))

            if member_name and vote_position:
                members.append({
                    "member_name": member_name,
                    "bioguide_id": bioguide,
                    "party": party,
                    "state": state,
                    "vote_position": vote_position,
                })

        return members

    def pull_member_votes(self, days_back: int = 30) -> dict[str, Any]:
        """Pull per-member vote records for recent roll-call votes.

        For each vote on a tracked bill, fetches the member-level breakdown
        and stores as VOTE:{bill_id}:{member}:{vote} series rows.
        This enables the vote-vs-trade hypocrisy detector.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            dict with status, rows_inserted, votes_processed.
        """
        import json

        if not self._has_api_key():
            return {"status": "SKIPPED", "reason": "no_api_key", "rows_inserted": 0}

        current_congress = 119  # 2025-2027
        total_inserted = 0
        total_votes_processed = 0

        for chamber in ("house", "senate"):
            try:
                raw_votes = self._fetch_recent_votes(chamber=chamber, days_back=days_back)
            except Exception as exc:
                log.warning(
                    "LegislationPuller: {c} member vote fetch failed: {e}",
                    c=chamber, e=str(exc),
                )
                continue

            with self.engine.begin() as conn:
                for vote in raw_votes:
                    roll_call = vote.get("rollNumber", vote.get("number", ""))
                    if not roll_call:
                        continue

                    vote_date_str = vote.get("date", "")
                    try:
                        obs_date = date.fromisoformat(vote_date_str[:10]) if vote_date_str else date.today()
                    except (ValueError, TypeError):
                        obs_date = date.today()

                    # Build a bill_id from vote context if available
                    bill_ref = vote.get("bill", {})
                    if isinstance(bill_ref, dict) and bill_ref.get("number"):
                        bill_id = f"{bill_ref.get('congress', current_congress)}-{bill_ref.get('type', '').lower()}{bill_ref['number']}"
                    else:
                        bill_id = f"{chamber}-vote-{roll_call}"

                    # Fetch member-level breakdown
                    try:
                        members = self._fetch_vote_members(
                            congress=current_congress,
                            chamber=chamber,
                            roll_call=roll_call,
                        )
                    except Exception as exc:
                        log.debug(
                            "Member vote detail fetch failed for {c} roll {r}: {e}",
                            c=chamber, r=roll_call, e=str(exc),
                        )
                        continue

                    total_votes_processed += 1

                    for member in members:
                        # Normalise member name for series ID (remove special chars)
                        member_slug = re.sub(
                            r"[^A-Za-z0-9]", "_",
                            member["member_name"].strip(),
                        )[:40]
                        vote_pos = member["vote_position"]

                        series_id = f"VOTE:{bill_id}:{member_slug}:{vote_pos}"

                        if self._row_exists(series_id, obs_date, conn, dedup_hours=168):
                            continue

                        # Encode vote position as numeric: Yea=1, Nay=-1, Not Voting=0, Present=0.5
                        vote_val = {
                            "Yea": 1.0, "Aye": 1.0, "Yes": 1.0,
                            "Nay": -1.0, "No": -1.0,
                            "Not Voting": 0.0, "Present": 0.5,
                        }.get(vote_pos, 0.0)

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=obs_date,
                            value=vote_val,
                            raw_payload={
                                "bill_id": bill_id,
                                "chamber": chamber,
                                "roll_call": roll_call,
                                "member_name": member["member_name"],
                                "bioguide_id": member.get("bioguide_id", ""),
                                "party": member.get("party", ""),
                                "state": member.get("state", ""),
                                "vote_position": vote_pos,
                                "question": vote.get("question", ""),
                                "result": vote.get("result", ""),
                            },
                        )
                        total_inserted += 1

                        # Emit signal for cross-referencing with trades
                        try:
                            conn.execute(
                                text(
                                    "INSERT INTO signal_sources "
                                    "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                                    "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                                    "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                                    "DO NOTHING"
                                ),
                                {
                                    "stype": "member_vote",
                                    "sid": bill_id,
                                    "ticker": member["member_name"],  # ticker field re-used for member name
                                    "sdate": obs_date,
                                    "stype2": "MEMBER_VOTE",
                                    "sval": json.dumps({
                                        "vote_position": vote_pos,
                                        "party": member.get("party", ""),
                                        "state": member.get("state", ""),
                                        "bill_id": bill_id,
                                        "chamber": chamber,
                                    }),
                                },
                            )
                        except Exception as exc:
                            log.debug(
                                "Member vote signal emit failed for {m}: {e}",
                                m=member["member_name"], e=str(exc),
                            )

        log.info(
            "LegislationPuller: {ins} member vote rows from {n} roll-call votes",
            ins=total_inserted, n=total_votes_processed,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "votes_processed": total_votes_processed,
        }

    # ── Signal emission ──────────────────────────────────────────────────

    def _emit_signal(
        self,
        conn: Any,
        record: dict[str, Any],
        signal_type: str,
    ) -> None:
        """Emit signal_sources rows for each affected ticker.

        Parameters:
            conn: Active database connection (within a transaction).
            record: Normalised bill or hearing dict.
            signal_type: 'LEGISLATION_NEW', 'LEGISLATION_HEARING', etc.
        """
        import json

        tickers = record.get("affected_tickers", [])
        if not tickers:
            return

        obs_date = record.get("obs_date", date.today())
        record_id = record.get("bill_id", record.get("hearing_id", "unknown"))

        for ticker in tickers:
            try:
                conn.execute(
                    text(
                        "INSERT INTO signal_sources "
                        "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                        "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                        "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                        "DO NOTHING"
                    ),
                    {
                        "stype": "legislative",
                        "sid": record_id,
                        "ticker": ticker,
                        "sdate": obs_date,
                        "stype2": signal_type,
                        "sval": json.dumps({
                            "title": record.get("title", ""),
                            "committees": record.get("committees", []),
                            "status": record.get("status", ""),
                            "matched_topics": record.get("matched_topics", []),
                            "sponsor": record.get("sponsor", ""),
                        }),
                    },
                )
            except Exception as exc:
                log.debug(
                    "Signal emit failed for {t}/{r}: {e}",
                    t=ticker,
                    r=record_id,
                    e=str(exc),
                )

    # ── Main pull methods ────────────────────────────────────────────────

    def pull_bills(self, days_back: int = 30) -> dict[str, Any]:
        """Pull recent bills and store in raw_series.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            dict with status, rows_inserted, bills_found.
        """
        if not self._has_api_key():
            return {"status": "SKIPPED", "reason": "no_api_key", "rows_inserted": 0}

        try:
            raw_bills = self._fetch_recent_bills(days_back=days_back)
        except Exception as exc:
            log.error("LegislationPuller: bill fetch failed: {e}", e=str(exc))
            return {"status": "FAILED", "error": str(exc), "rows_inserted": 0}

        bills = [self._parse_bill(b) for b in raw_bills]
        bills = [b for b in bills if b is not None]

        if not bills:
            log.info("LegislationPuller: no bills found in last {d} days", d=days_back)
            return {"status": "SUCCESS", "rows_inserted": 0, "bills_found": 0}

        rows_inserted = 0

        with self.engine.begin() as conn:
            for bill in bills:
                series_id = f"LEGISLATION:{bill['bill_id']}:{bill['status']}"
                obs_date = bill["obs_date"]

                if self._row_exists(series_id, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=self.STATUS_SCORES.get(bill["status"], 1.0),
                    raw_payload={
                        "bill_id": bill["bill_id"],
                        "title": bill["title"],
                        "type": bill["type"],
                        "congress": bill["congress"],
                        "sponsor": bill["sponsor"],
                        "sponsor_party": bill["sponsor_party"],
                        "sponsor_state": bill["sponsor_state"],
                        "committees": bill["committees"],
                        "status": bill["status"],
                        "action_text": bill["action_text"],
                        "policy_area": bill["policy_area"],
                        "subjects": bill["subjects"],
                        "matched_topics": bill["matched_topics"],
                        "affected_tickers": bill["affected_tickers"],
                    },
                )
                rows_inserted += 1

                try:
                    self._emit_signal(conn, bill, "LEGISLATION_NEW")
                except Exception as exc:
                    log.warning(
                        "LegislationPuller: signal emit failed for {b}: {e}",
                        b=bill["bill_id"],
                        e=str(exc),
                    )

        log.info(
            "LegislationPuller: {ins} bill rows inserted from {total} bills",
            ins=rows_inserted,
            total=len(bills),
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "bills_found": len(bills),
        }

    def pull_hearings(self, days_ahead: int = 14) -> dict[str, Any]:
        """Pull upcoming hearings and store in raw_series.

        Parameters:
            days_ahead: Number of days ahead to look for hearings.

        Returns:
            dict with status, rows_inserted, hearings_found.
        """
        if not self._has_api_key():
            return {"status": "SKIPPED", "reason": "no_api_key", "rows_inserted": 0}

        try:
            raw_hearings = self._fetch_hearings(days_ahead=days_ahead)
        except Exception as exc:
            log.error("LegislationPuller: hearing fetch failed: {e}", e=str(exc))
            return {"status": "FAILED", "error": str(exc), "rows_inserted": 0}

        hearings = [self._parse_hearing(h) for h in raw_hearings]
        hearings = [h for h in hearings if h is not None]

        if not hearings:
            log.info("LegislationPuller: no hearings found")
            return {"status": "SUCCESS", "rows_inserted": 0, "hearings_found": 0}

        rows_inserted = 0

        with self.engine.begin() as conn:
            for hearing in hearings:
                series_id = f"LEGISLATION:{hearing['hearing_id']}:hearing"
                obs_date = hearing["obs_date"]

                if self._row_exists(series_id, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=self.STATUS_SCORES["hearing"],
                    raw_payload={
                        "hearing_id": hearing["hearing_id"],
                        "title": hearing["title"],
                        "chamber": hearing["chamber"],
                        "congress": hearing["congress"],
                        "committees": hearing["committees"],
                        "matched_topics": hearing["matched_topics"],
                        "affected_tickers": hearing["affected_tickers"],
                    },
                )
                rows_inserted += 1

                try:
                    self._emit_signal(conn, hearing, "LEGISLATION_HEARING")
                except Exception as exc:
                    log.warning(
                        "LegislationPuller: signal emit failed for {h}: {e}",
                        h=hearing["hearing_id"],
                        e=str(exc),
                    )

        log.info(
            "LegislationPuller: {ins} hearing rows inserted from {total} hearings",
            ins=rows_inserted,
            total=len(hearings),
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": rows_inserted,
            "hearings_found": len(hearings),
        }

    def pull_votes(self, days_back: int = 7) -> dict[str, Any]:
        """Pull recent votes from both chambers.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            dict with status, rows_inserted, votes_found.
        """
        if not self._has_api_key():
            return {"status": "SKIPPED", "reason": "no_api_key", "rows_inserted": 0}

        total_inserted = 0
        total_found = 0

        for chamber in ("house", "senate"):
            try:
                raw_votes = self._fetch_recent_votes(chamber=chamber, days_back=days_back)
            except Exception as exc:
                log.warning(
                    "LegislationPuller: {c} vote fetch failed: {e}",
                    c=chamber,
                    e=str(exc),
                )
                continue

            total_found += len(raw_votes)

            with self.engine.begin() as conn:
                for vote in raw_votes:
                    vote_id = vote.get("rollNumber", vote.get("number", ""))
                    vote_date_str = vote.get("date", "")

                    try:
                        obs_date = date.fromisoformat(vote_date_str[:10]) if vote_date_str else date.today()
                    except (ValueError, TypeError):
                        obs_date = date.today()

                    series_id = f"LEGISLATION:{chamber}-vote-{vote_id}:vote"

                    if self._row_exists(series_id, obs_date, conn):
                        continue

                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=obs_date,
                        value=self.STATUS_SCORES["vote"],
                        raw_payload={
                            "chamber": chamber,
                            "vote_number": vote_id,
                            "question": vote.get("question", ""),
                            "result": vote.get("result", ""),
                            "description": vote.get("description", ""),
                        },
                    )
                    total_inserted += 1

        log.info(
            "LegislationPuller: {ins} vote rows inserted from {total} votes",
            ins=total_inserted,
            total=total_found,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted": total_inserted,
            "votes_found": total_found,
        }

    def pull_all(self, days_back: int = 30) -> dict[str, Any]:
        """Pull bills, hearings, and votes.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            Combined summary dict.
        """
        results: dict[str, Any] = {
            "status": "SUCCESS",
            "bills": {},
            "hearings": {},
            "votes": {},
            "member_votes": {},
        }

        results["bills"] = self.pull_bills(days_back=days_back)
        results["hearings"] = self.pull_hearings(days_ahead=14)
        results["votes"] = self.pull_votes(days_back=days_back)
        results["member_votes"] = self.pull_member_votes(days_back=days_back)

        # Overall status
        statuses = [
            results["bills"].get("status", "FAILED"),
            results["hearings"].get("status", "FAILED"),
            results["votes"].get("status", "FAILED"),
            results["member_votes"].get("status", "FAILED"),
        ]
        if all(s == "SKIPPED" for s in statuses):
            results["status"] = "SKIPPED"
        elif any(s == "FAILED" for s in statuses):
            results["status"] = "PARTIAL"

        return results

    def pull_recent(self, days_back: int = 30) -> dict[str, Any]:
        """Alias for pull_all — always incremental.

        Parameters:
            days_back: Number of days of history to pull.

        Returns:
            Combined summary dict.
        """
        return self.pull_all(days_back=days_back)
