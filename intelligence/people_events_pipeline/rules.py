"""Per-channel rules for canonical people-events: known_at, keys, direction, confidence.

Every function here is pure (no I/O, no clock reads) so the same inputs always
give the same canonical rows -- the reproducibility half of the E1 gates.

known_at: "the earliest instant the public could see this act", always stored
as a *valid upper bound* of that instant. A rule may be loose (later than the
truth) but never early. When several valid upper bounds exist for one act,
their minimum is also a valid upper bound, which is how the merge step combines
sources. A value that is not a guaranteed bound (for example the STOCK Act
45-day deadline: late filers exist) is never used as known_at.

Conventions (design doc section 3):

* Form 3/4/5, filing date only: 22:00 America/New_York on the filing date, in
  UTC. Reg S-T Rule 13(a)(4): a Section 16 form submitted by 22:00 ET is deemed
  filed that business day, and EDGAR disseminates on acceptance, so a filing
  dated D was public by 22:00 ET on D. This is exactly VS1's
  ``analysis.panel_insider_density.filing_known_at`` (decisions are 16:00 ET
  closes, so a filing dated D first counts at the D+1 close).
* Any other disclosure known only as a date (13F, PTR report date, LDA posting
  date): 09:30 America/New_York on the next trading day after that date (the
  plan's "rounded up to the next session open" rule), DST-aware.
* first_seen: a timestamp at which GRID had already stored the payload. Only
  valid when the stored payload cannot have been replaced since (see
  ``SourceSpec.overwritable`` in ``adapters``).
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd

from ingestion.market_calendar import next_trading_day

NEW_YORK = ZoneInfo("America/New_York")
SECTION16_FILING_CUTOFF = time(22, 0)  # Reg S-T 13(a)(4), Forms 3/4/5
SESSION_OPEN = time(9, 30)

KEY_VERSION = {
    "form4": "f4v2",
    "congress": "cgv1",
    "thirteen_f": "13fv1",
    "gov_contract": "gcv1",
    "gov_contract_qq_aggregate": "qqgcv1",
    "lobbying": "lbv1",
    "fara": "farav1",
}

# known_at_basis values, strongest first. Used only to break ties when two
# sources give the identical known_at instant.
BASIS_RANK = {"filing": 0, "publish": 1, "qq_last_modified": 2, "statutory_bound": 3, "first_seen": 4}


# --- known_at -------------------------------------------------------------------------


def section16_filing_known_at(filing_date: date) -> datetime:
    """22:00 America/New_York on ``filing_date``, as an aware UTC datetime."""
    local = datetime.combine(filing_date, SECTION16_FILING_CUTOFF, tzinfo=NEW_YORK)
    return local.astimezone(timezone.utc)


def next_session_open_after(d: date) -> datetime:
    """09:30 America/New_York on the first trading day strictly after ``d``, in UTC."""
    session = next_trading_day(d + timedelta(days=1))
    return datetime.combine(session, SESSION_OPEN, tzinfo=NEW_YORK).astimezone(timezone.utc)


def section16_filing_known_at_series(filing_dates: pd.Series) -> pd.Series:
    """Vectorized :func:`section16_filing_known_at` over a datetime64 (naive date) series."""
    days = pd.to_datetime(filing_dates).dt.normalize()
    local = days + pd.Timedelta(hours=SECTION16_FILING_CUTOFF.hour, minutes=SECTION16_FILING_CUTOFF.minute)
    return local.dt.tz_localize(NEW_YORK).dt.tz_convert("UTC")


def next_session_open_after_series(dates: pd.Series) -> pd.Series:
    """Vectorized :func:`next_session_open_after`; NaT stays NaT."""
    days = pd.to_datetime(dates).dt.normalize()
    uniq = days.dropna().unique()
    mapping = {pd.Timestamp(d): pd.Timestamp(next_session_open_after(pd.Timestamp(d).date())) for d in uniq}
    out = days.map(mapping)
    return pd.to_datetime(out, utc=True)


def to_utc(value: Any) -> datetime | None:
    """Parse a timestamp-like value to an aware UTC datetime; naive input is taken as UTC."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
        return None
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError):
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").to_pydatetime()


def parse_date(value: Any) -> date | None:
    """ISO ``YYYY-MM-DD[...]``, SEC ``DD-MON-YYYY``, date or datetime; None otherwise."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt, width in (("%Y-%m-%d", 10), ("%d-%b-%Y", 11)):
        try:
            return datetime.strptime(text[:width].upper() if fmt == "%d-%b-%Y" else text[:width], fmt).date()
        except ValueError:
            continue
    return None


# --- normalization ---------------------------------------------------------------------

# Honorifics and generational suffixes are dropped from name keys. A source
# that writes "Timothy D. Cook" and one that writes "COOK TIMOTHY D" must fold
# to the same key; so must "Hon. Nancy Pelosi" and "Pelosi, Nancy". Single
# letters (middle initials) are dropped too because sources disagree on them.
# The cost -- two people at one issuer differing only by initial or suffix,
# trading the same day, code and share count -- is reported by the merge
# step's near-duplicate counter rather than silently assumed away.
_NAME_DROP = frozenset({
    "MR", "MRS", "MS", "DR", "HON", "HONORABLE", "REP", "SEN", "SENATOR", "REPRESENTATIVE",
    "JR", "SR", "II", "III", "IV", "ESQ", "PHD", "MD", "CPA",
})


def name_tokens(name: Any) -> str:
    """Order-independent name key: upper-case alphanumeric tokens, sorted, joined by '_'."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    tokens = re.sub(r"[^A-Za-z0-9]+", " ", str(name)).upper().split()
    kept = sorted({t for t in tokens if len(t) > 1 and t not in _NAME_DROP})
    return "_".join(kept)


def name_tokens_series(names: pd.Series) -> pd.Series:
    uniq = names.dropna().unique()
    mapping = {n: name_tokens(n) for n in uniq}
    return names.map(mapping).fillna("")


_INVALID_TICKERS = frozenset({"", "NONE", "NA", "N/A", "NULL", "NAN", "-", "--", "0"})


def normalize_ticker(value: Any) -> str | None:
    """Upper-case ticker with class separators removed (BRK.B, BRK-B, BRK/B -> BRKB); None if not a ticker."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip().upper()
    if raw in _INVALID_TICKERS:
        return None
    folded = re.sub(r"[.\-/\s]", "", raw)
    if not folded or len(folded) > 10 or not folded.isalnum() or folded in _INVALID_TICKERS:
        return None
    return folded


def normalize_ticker_series(values: pd.Series) -> pd.Series:
    uniq = values.dropna().unique()
    mapping = {v: normalize_ticker(v) for v in uniq}
    return values.map(mapping)


def cik_int(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return int(text) if text.isdigit() and int(text) > 0 else None


def cik_text(value: int | None) -> str | None:
    """10-digit zero-padded CIK text, the form SEC publishes."""
    return None if value is None else f"{int(value):010d}"


# --- dedup keys ------------------------------------------------------------------------


def _shares_key(shares: Any) -> str:
    if shares is None or (isinstance(shares, float) and pd.isna(shares)):
        return "NA"
    return str(int(round(abs(float(shares)))))


def issuer_key(ticker: str | None, cik: int | None) -> str:
    """Form 4 issuer component: the filed ticker when it is a ticker, else the issuer CIK.

    The ticker is what every Form 4 source carries (QuiverQuant and the EDGAR
    native feed carry no issuer CIK), and it is the ticker *as of the filing*,
    so two sources describing the same act agree on it. CIK is the fallback
    for SEC rows with no usable ISSUERTRADINGSYMBOL.
    """
    if ticker:
        return f"t:{ticker}"
    if cik is not None:
        return f"c:{cik}"
    return "?"


def form4_dedup_key(issuer: str, owner_tokens: str, trans_date: date, code: str | None, shares: Any) -> str:
    """Plan section 2.1: (issuer, reporting owner, transaction_date, transaction_code, round(shares))."""
    return "|".join([
        KEY_VERSION["form4"], issuer, owner_tokens or "?", trans_date.isoformat(),
        (code or "").strip().upper()[:1] or "?", _shares_key(shares),
    ])


def form4_loose_key(issuer: str, owner_tokens: str, trans_date: date, code: str | None) -> str:
    """The dedup key without shares: used only to *count* possible duplicates, never to merge."""
    return "|".join([issuer, owner_tokens or "?", trans_date.isoformat(), (code or "").strip().upper()[:1] or "?"])


def amount_band_low(text: Any) -> str:
    """Lower bound of a disclosed range ("$1,001 - $15,000" -> "1001"); "NA" if none."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return "NA"
    if isinstance(text, (int, float)):
        return str(int(text))
    nums = re.findall(r"\d[\d,]*", str(text))
    return str(int(nums[0].replace(",", ""))) if nums else "NA"


def congress_dedup_key(member_tokens: str, ticker: str | None, trans_date: date, direction: str | None,
                       amount_low: str) -> str:
    """Plan section 2.1: (member, ticker, transaction_date, direction, amount band)."""
    return "|".join([
        KEY_VERSION["congress"], member_tokens or "?", ticker or "?", trans_date.isoformat(),
        direction or "?", amount_low or "NA",
    ])


def thirteen_f_dedup_key(filer: str, security: str, report_date: date) -> str:
    """Plan section 2.1: (filer CIK, CUSIP, report_date)."""
    return "|".join([KEY_VERSION["thirteen_f"], filer, security, report_date.isoformat()])


def gov_contract_dedup_key(award_id: str, action_date: date) -> str:
    """Plan section 2.1: USASpending award_id (+ modification, approximated by the action date)."""
    return "|".join([KEY_VERSION["gov_contract"], award_id.strip().upper(), action_date.isoformat()])


def qq_gov_aggregate_dedup_key(ticker: str, year: int, qtr: int, amount: Any) -> str:
    amt = "NA" if amount is None else str(int(round(float(amount))))
    return "|".join([KEY_VERSION["gov_contract_qq_aggregate"], ticker, f"{year}Q{qtr}", amt])


def lobbying_dedup_key(ticker: str, registrant_tokens: str, filed: date, amount: Any) -> str:
    amt = "NA" if amount in (None, "") or (isinstance(amount, float) and pd.isna(amount)) else str(int(round(float(amount))))
    return "|".join([KEY_VERSION["lobbying"], ticker, registrant_tokens or "?", filed.isoformat(), amt])


def fara_dedup_key(registrant_tokens: str, principal_tokens: str, activity_date: date, activity_type: str) -> str:
    return "|".join([
        KEY_VERSION["fara"], registrant_tokens or "?", principal_tokens or "?",
        activity_date.isoformat(), (activity_type or "?").strip().upper(),
    ])


# --- direction -------------------------------------------------------------------------

FORM4_CODE_DIRECTIONS = {"P": "buy", "S": "sell", "A": "award"}


def form4_direction(code: Any, acquired_disposed: Any = None) -> str | None:
    """TransactionCode first (P/S/A); AcquiredDisposedCode only when no code at all.

    Same rule as ``intelligence.people_events_materializer.form4_transaction_direction``:
    exercises, tax withholding, gifts and other codes are not buys or sells.
    """
    c = ("" if code is None or (isinstance(code, float) and pd.isna(code)) else str(code)).strip().upper()[:1]
    if c:
        return FORM4_CODE_DIRECTIONS.get(c)
    a = ("" if acquired_disposed is None else str(acquired_disposed)).strip().upper()[:1]
    return {"A": "buy", "D": "sell"}.get(a)


def congress_direction(text: Any) -> str | None:
    t = ("" if text is None else str(text)).strip().lower()
    if not t:
        return None
    if "purchase" in t or t in ("buy", "p"):
        return "buy"
    if "sale" in t or "sell" in t or t in ("s", "s (partial)", "s (full)"):
        return "sell"
    return None  # exchanges and unknown text are not buys or sells


# --- confidence ------------------------------------------------------------------------

STRONG_ACTOR_BASES = frozenset({"owner_cik", "bioguide", "filer_cik", "registrant_id"})


def confidence(known_at_basis: str, actor_id_basis: str, security_matched: bool, channel: str) -> str:
    """Closed rubric (design doc section 2.4).

    high:   a filing/publish timestamp, a stable actor id, and a resolved issuer.
    medium: a filing/publish timestamp but a name-keyed actor or an unresolved issuer.
    low:    any first_seen / last_modified / statutory basis, and every aggregate channel.
    """
    if channel in ("gov_contract_qq_aggregate", "fara"):
        return "low"
    if known_at_basis not in ("filing", "publish"):
        return "low"
    if actor_id_basis in STRONG_ACTOR_BASES and security_matched:
        return "high"
    return "medium"


# --- hashing ---------------------------------------------------------------------------


def stable_hash(obj: Any) -> str:
    """sha256 of canonical JSON (sorted keys, no whitespace); dates as ISO strings."""
    def default(o: Any) -> Any:
        if isinstance(o, (datetime, date, pd.Timestamp)):
            return o.isoformat()
        if isinstance(o, (set, frozenset)):
            return sorted(o)
        raise TypeError(f"not JSON-serializable: {type(o)!r}")

    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=default).encode()).hexdigest()


def first_present(d: dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None
