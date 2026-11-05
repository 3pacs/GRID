"""Pure event logic: known_at, entry session, qualifying lines, positions.

No I/O. Pre-registration §2-§3. The session calendar is the repository's
(``ingestion.market_calendar`` via ``paper_log.gex_levels.sessions``, which adds
the recurring 13:00 early closes), so an entry session's close instant is
never assumed to be 16:00 on a day the market shut at 13:00.

``entry_positions`` in ``analysis.panel_insider_density`` is the VS1 harness
form of the same key (one position per issuer and entry session); it works on
DataFrames with a 16:00 close for every session. This module is the forward
(record-at-a-time) form with early closes; tests check the two agree on
ordinary sessions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from ingestion.market_calendar import is_market_open
from paper_log.gex_levels.sessions import expected_close_time
from paper_log.trade_edge.config import (
    BUCKET_LARGE,
    BUCKET_MICRO,
    BUCKET_MID,
    BUCKET_UNKNOWN,
    CAP_MID_MAX,
    CAP_SMALL_MAX,
    DATA_READY_AFTER_CLOSE,
    EASTERN,
    INGEST_VISIBILITY_MARGIN,
    LARGE_LINE_USD,
    MAX_FILING_LAG_DAYS,
    MIN_SHARES,
    MIN_TRADE_USD,
    PUBLIC_FALLBACK_LOCAL,
    PURCHASE_CODE,
    PURCHASE_FORM,
    STRATUM_LARGE,
    STRATUM_SMALL,
)

# ── sessions ────────────────────────────────────────────────────────────────


def close_instant(d: date) -> datetime:
    """The session's close as an aware America/New_York datetime."""
    return datetime.combine(d, expected_close_time(d), tzinfo=EASTERN)


def _aware(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts


def entry_session(known_at: datetime) -> date:
    """First session whose close instant is strictly after ``known_at`` (§3)."""
    known_at = _aware(known_at)
    d = known_at.astimezone(EASTERN).date()
    while True:
        if is_market_open(d) and close_instant(d) > known_at:
            return d
        d += timedelta(days=1)


def shift_sessions(d: date, n: int) -> date:
    """The n-th session after session ``d`` (n >= 0; n = 0 returns ``d``)."""
    if n < 0:
        raise ValueError("n must be >= 0")
    out = d
    for _ in range(n):
        out += timedelta(days=1)
        while not is_market_open(out):
            out += timedelta(days=1)
    return out


def sessions_after(start: date, end: date) -> int:
    """Number of sessions s with start < s <= end."""
    count = 0
    d = start + timedelta(days=1)
    while d <= end:
        if is_market_open(d):
            count += 1
        d += timedelta(days=1)
    return count


def last_completed_session(now: datetime) -> date:
    """Latest session whose data are available at ``now`` (close + 30 min)."""
    now = _aware(now)
    d = now.astimezone(EASTERN).date()
    while True:
        if is_market_open(d) and close_instant(d) + DATA_READY_AFTER_CLOSE <= now:
            return d
        d -= timedelta(days=1)


# ── known_at (§2.4) ─────────────────────────────────────────────────────────


def public_time(filing_date: date, acceptance_at: datetime | None) -> datetime:
    """EDGAR acceptance time when known, else the filing date at 22:00 ET."""
    if acceptance_at is not None:
        return _aware(acceptance_at)
    return datetime.combine(filing_date, PUBLIC_FALLBACK_LOCAL, tzinfo=EASTERN)


def compute_known_at(
    filing_date: date, acceptance_at: datetime | None, first_ingest_at: datetime
) -> datetime:
    """Later of the public time and GRID's first ingest (+ visibility margin), in UTC."""
    ingest = _aware(first_ingest_at) + INGEST_VISIBILITY_MARGIN
    known = max(public_time(filing_date, acceptance_at), ingest)
    return known.astimezone(timezone.utc)


# ── qualifying lines (§2.2) ─────────────────────────────────────────────────

EXCL_NOT_FORM_4 = "not_form_4"
EXCL_NOT_PURCHASE = "not_code_p"
EXCL_DERIVATIVE = "derivative"
EXCL_NOT_ACQUIRED = "not_acquired"
EXCL_EQUITY_SWAP = "equity_swap"
EXCL_10B5_1 = "rule_10b5_1"
EXCL_LATE_FILING = "late_filing"
EXCL_SMALL = "small_or_unpriced"


def line_exclusion(line: dict[str, Any], filing_date: date, submission_type: str) -> str | None:
    """The first rule a line fails, or None when it qualifies."""
    if (submission_type or "").strip().upper() != PURCHASE_FORM:
        return EXCL_NOT_FORM_4
    if (line.get("code") or "").strip().upper() != PURCHASE_CODE:
        return EXCL_NOT_PURCHASE
    if line.get("is_derivative"):
        return EXCL_DERIVATIVE
    if (line.get("acq_disp") or "").strip().upper() != "A":
        return EXCL_NOT_ACQUIRED
    if line.get("equity_swap"):
        return EXCL_EQUITY_SWAP
    if line.get("is_10b5_1"):
        return EXCL_10B5_1
    trans = _as_date(line.get("trans_date"))
    if trans is None:
        return EXCL_LATE_FILING
    lag = (filing_date - trans).days
    if lag < 0 or lag > MAX_FILING_LAG_DAYS:
        return EXCL_LATE_FILING
    shares = line.get("shares")
    price = line.get("price")
    if shares is None or price is None or price <= 0 or shares < MIN_SHARES:
        return EXCL_SMALL
    if shares * price < MIN_TRADE_USD:
        return EXCL_SMALL
    return None


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _as_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


# ── tickers / buckets ───────────────────────────────────────────────────────

_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}([.\-][A-Z0-9]{1,2})?$")
_UNRESOLVED = frozenset({"", "NONE", "N/A", "NA", "NULL", "-"})


def is_resolvable_ticker(ticker: str | None) -> bool:
    t = (ticker or "").strip().upper()
    return t not in _UNRESOLVED and bool(_TICKER_RE.match(t))


def yahoo_symbol(ticker: str) -> str:
    """EDGAR class-share dots (BRK.B) are dashes on Yahoo (BRK-B)."""
    return ticker.strip().upper().replace(".", "-")


def cap_bucket(market_cap_usd: float | None) -> str:
    if market_cap_usd is None or not market_cap_usd > 0:
        return BUCKET_UNKNOWN
    if market_cap_usd < CAP_SMALL_MAX:
        return BUCKET_MICRO
    if market_cap_usd < CAP_MID_MAX:
        return BUCKET_MID
    return BUCKET_LARGE


def stratum(largest_value: float) -> str:
    return STRATUM_LARGE if largest_value >= LARGE_LINE_USD else STRATUM_SMALL


# ── purchases and positions (§2.3, §3) ──────────────────────────────────────


class IdentityPolicyError(ValueError):
    """The journal already holds two scored identities for one economic event,
    or two recorded positions for one (issuer, entry session) slot.

    Raised instead of merging or rewriting: which record is canonical is an
    owner decision for a new log, never a code path. (A purchase that joins an
    already entered slot is not a conflict: it is noted append-only as a
    :data:`LATE_MEMBER_KIND` record, see :func:`reconcile_positions`.)
    """


class MembershipProofError(IdentityPolicyError):
    """A journal record's original membership cannot be proved from the ordered
    journal prefix before it (missing, ambiguous or inconsistent history).

    The message names the failing record (its 0-based index in the journal),
    or the prefix before it, and the reason. Raised before any append: nothing
    is reset, rewritten, backdated or inferred from a provider, and continuing
    an unproved history is an owner decision.
    """


@dataclass
class Witness:
    """Recorded membership proved from the journal's ordered prefixes (in memory only, never stored).

    ``recorded`` is each position id's latest proved ``signal``/``entry`` record;
    ``members`` the :func:`purchase_key` of every de-duplicated purchase that
    record aggregated (a signal's latest revision is candidate membership, an
    entry's is frozen); ``late`` the proved :data:`LATE_MEMBER_KIND` records,
    each binding its purchases to its entered slot for good.
    """

    recorded: dict[str, dict] = field(default_factory=dict)
    members: dict[str, list[tuple]] = field(default_factory=dict)
    late: list[dict] = field(default_factory=list)


# A purchase visible only after its (issuer, recorded entry session) slot was
# entered (owner decision (c), 2026-10-08). Appended once per accession and
# slot; never scored, never changes the entry. ``reason`` is how it reached the
# slot: in the rebuilt group the entered id resolves to, or moved onto the slot
# by canonical (issuer, entry session) occupancy. Once journaled, the note binds
# its purchases to that entered slot for good (see :func:`reconcile_positions`).
LATE_MEMBER_KIND = "late_member"
LATE_SAME_GROUP = "same_group_after_entry"
LATE_MOVED_DAY = "moved_day_after_entry"


def learn_aliases(filings: Iterable[dict], aliases: dict[str, int] | None = None) -> list[dict]:
    """New ticker -> CIK associations in ``filings`` not yet in ``aliases`` (first wins).

    Returned in filing order as ``{"ticker", "cik", "accession"}``; the caller
    appends them as ``alias`` journal records so the association is append-only
    and survives restarts.
    """
    known = dict(aliases or {})
    out: list[dict] = []
    for f in filings:
        if f.get("issuer_cik") and is_resolvable_ticker(f.get("ticker")):
            t = f["ticker"].strip().upper()
            if t not in known:
                known[t] = int(f["issuer_cik"])
                out.append({"ticker": t, "cik": known[t], "accession": f["accession"]})
    return out


def _ticker_ciks(filings: list[dict], aliases: dict[str, int] | None) -> dict[str, int]:
    """ticker -> CIK: the journal's ``aliases`` first, then what the filings carry."""
    by_ticker: dict[str, int] = {t.strip().upper(): int(c) for t, c in (aliases or {}).items()}
    for f in filings:
        if f.get("issuer_cik") and is_resolvable_ticker(f.get("ticker")):
            by_ticker.setdefault(f["ticker"].strip().upper(), int(f["issuer_cik"]))
    return by_ticker


def canonical_issuer(issuer_key: str, by_ticker: dict[str, int]) -> str:
    """``ticker:T`` is ``cik:N`` once T's CIK is known; any other key is already canonical."""
    kind, _, rest = issuer_key.partition(":")
    if kind == "ticker" and rest in by_ticker:
        return f"cik:{by_ticker[rest]}"
    return issuer_key


def issuer_keys(filings: Iterable[dict], aliases: dict[str, int] | None = None) -> dict[str, str]:
    """accession -> issuer key: ``cik:N`` when known, else ``ticker:T``.

    A ``grid_db`` accession (no CIK) whose ticker some CIK-keyed accession also
    carries is mapped to that CIK so one issuer never yields two keys. The
    journal's append-only ``aliases`` (ticker -> CIK, first association wins)
    take precedence over anything a later filing claims for the same ticker.
    """
    filings = list(filings)
    by_ticker = _ticker_ciks(filings, aliases)
    out: dict[str, str] = {}
    for f in filings:
        if f.get("issuer_cik"):
            out[f["accession"]] = f"cik:{int(f['issuer_cik'])}"
        else:
            t = (f.get("ticker") or "").strip().upper()
            out[f["accession"]] = f"cik:{by_ticker[t]}" if t in by_ticker else f"ticker:{t or '?'}"
    return out


def actor_sort_key(actor: str) -> tuple:
    """Smallest actor = smallest numeric CIK; name-keyed actors sort after CIKs."""
    kind, _, rest = actor.partition(":")
    if kind == "cik" and rest.isdigit():
        return (0, int(rest), "")
    return (1, 0, actor)


def build_purchases(filings: Iterable[dict], aliases: dict[str, int] | None = None) -> list[dict]:
    """Qualifying purchases, de-duplicated across accessions (VS1 §2.1 (b)).

    ``filings`` are ``filing`` records (§2.1). Each line already carries its
    ``exclusion``; only lines with none are purchases. ``aliases`` are the
    journal's ticker -> CIK associations (see :func:`issuer_keys`).
    """
    filings = list(filings)
    keys = issuer_keys(filings, aliases)
    rows: list[dict] = []
    for f in filings:
        known_at = _as_dt(f["known_at"])
        for line in f.get("lines", []):
            if line.get("exclusion"):
                continue
            rows.append(
                {
                    "issuer_key": keys[f["accession"]],
                    "issuer_cik": f.get("issuer_cik"),
                    "issuer_name": f.get("issuer_name") or "",
                    "ticker": (f.get("ticker") or "").strip().upper(),
                    "accession": f["accession"],
                    "actor": f["actor"],
                    "owner_names": [o.get("name") or "" for o in f.get("owners", [])],
                    "trans_date": str(line["trans_date"])[:10],
                    "shares": float(line["shares"]),
                    "price": float(line["price"]),
                    "value": float(line["shares"]) * float(line["price"]),
                    "filing_date": str(f["filing_date"])[:10],
                    "acceptance_at": f.get("acceptance_at"),
                    "first_ingest_at": f.get("first_ingest_at"),
                    "known_at": known_at,
                    "source": f.get("source"),
                }
            )
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        k = purchase_key(r["issuer_key"], r["trans_date"], r["shares"], r["price"])
        groups.setdefault(k, []).append(r)
    out = []
    for members in groups.values():
        members.sort(key=lambda r: (r["known_at"], r["accession"]))
        keep = dict(members[0])
        keep["actor"] = min((m["actor"] for m in members), key=actor_sort_key)
        keep["n_reports"] = len({m["accession"] for m in members})
        out.append(keep)
    out.sort(key=lambda r: (r["issuer_key"], r["known_at"], r["actor"], r["trans_date"]))
    return out


def position_id(issuer_key: str, entry: date) -> str:
    return f"{issuer_key}|{entry.isoformat()}"


def purchase_key(issuer_key: str, trans_date: Any, shares: float, price: float) -> tuple:
    """The economic-purchase identity used to de-duplicate reports (VS1 §2.1 (b))."""
    return (issuer_key, str(trans_date)[:10], round(float(shares)), round(float(price), 2))


def _canonical_purchase_key(key: Iterable, by_ticker: dict[str, int]) -> tuple:
    """A :func:`purchase_key` (or its JSON list form) with its issuer made canonical."""
    issuer, trans, shares, price = key
    return (canonical_issuer(str(issuer), by_ticker), str(trans)[:10], round(float(shares)), round(float(price), 2))


def _key_of(p: dict) -> tuple:
    return purchase_key(p["issuer_key"], p["trans_date"], p["shares"], p["price"])


def _refused(detail: str) -> IdentityPolicyError:
    return IdentityPolicyError(f"{detail}; refusing to merge or rewrite (owner decision for a new log)")


def reconcile_positions(positions: dict[str, dict], filings: dict[str, dict], witness: Witness,
                        aliases: dict[str, int]
                        ) -> tuple[dict[str, dict], list[dict], list[dict], dict[str, list[tuple]]]:
    """Keep recorded identities: one binding per purchase, one position per (issuer, entry session).

    ``positions`` are ``group_positions(build_purchases(filings, aliases))``.
    Their membership is the exact de-duplicated purchases (earliest report per
    economic :func:`purchase_key`) that each one aggregates -- never every raw
    line under its accessions. ``witness`` is the recorded membership proved
    from the journal's ordered prefixes (``tracker.prove_membership``): each
    recorded id's latest ``signal``/``entry`` record, the purchase keys it
    consumed, and the proved :data:`LATE_MEMBER_KIND` notes. Purchase keys
    compare canonically (:func:`canonical_issuer`), so a ``ticker:T`` fallback
    and its later ``cik:N`` enrichment are one purchase.

    0. Owner sets. Every canonical purchase has at most one binding: one
       recorded owner (a signal's revisions are one candidate owner, an entry
       is the frozen owner) or one late-note target (an entered id). Two
       recorded owners, two late targets, or a purchase both consumed and
       noted late are refused, whatever the current rebuilt id, entry day or
       iteration order. A recorded owner's purchases must still rebuild.
    1. Purchase identity. A purchase noted late stays with its late target:
       it is never routed again, so it can never open or join a scored
       position, even after an alias or an earlier duplicate moves its
       rebuilt day. The rest of a rebuilt position follows the one recorded
       owner its purchases (or its own recorded id) name.
    2. Slot occupancy. Every other rebuilt position whose canonical issuer
       and entry session equal a recorded position's is that slot too.
    3. Rebuilt positions that land on one recorded id are recombined from
       their purchases with the :func:`group_positions` rules under the
       recorded id/session (``issuer_alias`` names the rebuilt key when it
       differs). Before entry this is the slot's next signal revision; after
       entry only the purchases the entry consumed are aggregated and the
       caller never revises the entry.
    4. Late membership (owner decision (c)). A purchase routed to an entered
       slot that its entry did not consume is late: the entry, its identity,
       membership, stratum/size and cap cost stay as recorded and no second
       position is opened. It is returned as a late-member note, one per
       accession, with ``reason`` :data:`LATE_SAME_GROUP` (its rebuilt group
       resolves to the entered id) or :data:`LATE_MOVED_DAY` (it reached the
       slot by step 2). Once the note is journaled the purchase is bound by
       step 1 and never noted again.

    Also refused with :class:`IdentityPolicyError` (never merged or
    rewritten): a rebuilt position whose purchases name two recorded owners,
    and two recorded positions on one canonical (issuer, entry session).

    Returns the reconciled positions, the associations applied, the new
    late-member notes (the caller appends them as :data:`LATE_MEMBER_KIND`
    records) and each reconciled position's membership (its purchase keys).
    """
    flist = list(filings.values())
    by_ticker = _ticker_ciks(flist, aliases)
    recorded = witness.recorded

    def canon(key: Iterable) -> tuple:
        return _canonical_purchase_key(key, by_ticker)

    by_pid: dict[str, list[dict]] = {}
    for p in build_purchases(flist, aliases):
        by_pid.setdefault(position_id(p["issuer_key"], entry_session(p["known_at"])), []).append(p)
    if set(by_pid) != set(positions):
        raise ValueError("positions were not rebuilt from these filings and aliases")

    # 0. complete canonical owner sets, before any routing
    owners: dict[tuple, set[str]] = {}
    for rid, keys in witness.members.items():
        for k in keys:
            owners.setdefault(canon(k), set()).add(rid)
    for k in sorted(owners):
        if len(owners[k]) > 1:
            raise _refused(f"purchase {list(k)} is consumed by the recorded positions {sorted(owners[k])}")
    rebuilt = {canon(_key_of(p)) for ps in by_pid.values() for p in ps}
    for k in sorted(owners):
        if k not in rebuilt:
            raise _refused(f"recorded position {sorted(owners[k])[0]!r} consumed purchase {list(k)}, "
                           "which these filings no longer rebuild")
    late_to: dict[tuple, str] = {}
    for note in witness.late:
        tid = note["position_id"]
        if (recorded.get(tid) or {}).get("kind") != "entry":
            raise _refused(f"late note of accession {note.get('accession')!r} targets {tid!r}, "
                           "which is not an entered position")
        for key in note["purchase_keys"]:
            k = canon(key)
            if late_to.setdefault(k, tid) != tid:
                raise _refused(f"purchase {list(k)} is noted late on two positions {sorted({late_to[k], tid})}")
            if k in owners:
                raise _refused(f"purchase {list(k)} is consumed by {sorted(owners[k])} and also noted late "
                               f"on {tid!r}")

    # 1. purchase identity (late-noted purchases stay bound to their late target)
    rest: dict[str, list[dict]] = {}
    target: dict[str, str] = {}
    for pid in sorted(positions):
        free = [p for p in by_pid[pid] if canon(_key_of(p)) not in late_to]
        if not free:
            continue
        rest[pid] = free
        hits = {o for p in free for o in owners.get(canon(_key_of(p)), ())}
        if pid in recorded:
            if hits - {pid}:
                raise _refused(f"rebuilt position {pid!r}, itself recorded, holds purchases consumed by "
                               f"{sorted(hits - {pid})}")
            target[pid] = pid
        elif len(hits) > 1:
            raise _refused(f"rebuilt position {pid!r} holds purchases of two recorded positions {sorted(hits)}")
        else:
            target[pid] = next(iter(hits)) if hits else pid

    # 2. one position per canonical (issuer, entry session)
    slots: dict[tuple[str, str], str] = {}
    for rid in sorted(recorded):
        rec = recorded[rid]
        slot = (canonical_issuer(rec["issuer_key"], by_ticker), rec["entry_session"])
        holder = slots.setdefault(slot, rid)
        if holder != rid:
            raise _refused(f"journal records {holder!r} and {rid!r} for one issuer/entry session {slot[0]}|{slot[1]}")
    moved: set[str] = set()
    for pid in rest:
        if target[pid] != pid or pid in recorded:
            continue
        rid = slots.get((canonical_issuer(positions[pid]["issuer_key"], by_ticker), positions[pid]["entry_session"]))
        if rid is not None:
            target[pid] = rid
            moved.add(pid)

    # 3. one reconciled position per target id
    groups: dict[str, list[str]] = {}
    for pid in positions:
        if pid in rest:
            groups.setdefault(target[pid], []).append(pid)
    out: dict[str, dict] = {}
    applied: list[dict] = []
    members: dict[str, list[tuple]] = {}
    took: dict[str, set[tuple]] = {}
    for tid, pids in groups.items():
        rec = recorded.get(tid)
        ps = [p for pid in pids for p in rest[pid]]
        if rec is not None and rec.get("kind") == "entry":
            took[tid] = {canon(k) for k in witness.members.get(tid, ())}
            ps = [p for p in ps if canon(_key_of(p)) in took[tid]]
            if not ps:
                raise _refused(f"entered position {tid!r} rebuilds none of the purchases it consumed")
        if pids == [tid] and ps == by_pid[tid]:
            pos = positions[tid]
        elif rec is None:  # a new position less its late-noted purchases
            pos = _position(tid, positions[tid]["issuer_key"], positions[tid]["entry_session"],
                            positions[tid]["entry_close_at"], ps)
        else:
            ps.sort(key=lambda r: (r["issuer_key"], r["known_at"], r["actor"], r["trans_date"]))
            pos = _position(tid, rec["issuer_key"], rec["entry_session"], rec["entry_close_at"], ps)
            rebuilt_keys = sorted({positions[pid]["issuer_key"] for pid in pids} - {rec["issuer_key"]})
            if rebuilt_keys:
                pos["issuer_alias"] = rebuilt_keys[0]
        for pid in pids:
            if pid != tid:
                applied.append({"position_id": tid, "superseded_id": pid,
                                "issuer_key": positions[pid]["issuer_key"],
                                "entry_session": positions[pid]["entry_session"]})
        out[tid] = pos
        members[tid] = [_key_of(p) for p in ps]

    # 4. late membership of entered slots: noted once, never scored
    late: list[dict] = []
    for tid in sorted(took):
        rec = recorded[tid]
        notes: dict[str, dict] = {}
        for pid in groups[tid]:
            for p in rest[pid]:
                k = _key_of(p)
                if canon(k) in took[tid]:
                    continue
                note = notes.setdefault(p["accession"], {
                    "position_id": tid, "issuer_key": canonical_issuer(rec["issuer_key"], by_ticker),
                    "entry_session": rec["entry_session"], "accession": p["accession"],
                    "known_at": filings[p["accession"]]["known_at"], "purchase_keys": [],
                    "reason": LATE_MOVED_DAY if pid in moved else LATE_SAME_GROUP, "rebuilt_id": pid})
                note["purchase_keys"].append(list(k))
        late += [notes[a] for a in sorted(notes)]
    return out, applied, late, members


def _position(pid: str, issuer_key: str, entry: str, entry_close_at: str, ps: list[dict]) -> dict:
    """One position over its purchases ``ps`` (registered aggregation, §3)."""
    pos = {
        "position_id": pid,
        "issuer_key": issuer_key,
        "issuer_cik": ps[0]["issuer_cik"],
        "issuer_name": ps[0]["issuer_name"],
        "ticker": ps[0]["ticker"],
        "entry_session": entry,
        "entry_close_at": entry_close_at,
    }
    for p in ps:
        if not pos["issuer_name"] and p["issuer_name"]:
            pos["issuer_name"] = p["issuer_name"]
        if not is_resolvable_ticker(pos["ticker"]) and is_resolvable_ticker(p["ticker"]):
            pos["ticker"] = p["ticker"]
    values = [p["value"] for p in ps]
    names = sorted({n for p in ps for n in p["owner_names"] if n})
    pos.update(
        {
            "accessions": sorted({p["accession"] for p in ps}),
            "actors": sorted({p["actor"] for p in ps}),
            "n_actors": len({p["actor"] for p in ps}),
            "insider_names": names,
            "n_purchases": len(ps),
            "total_value": round(sum(values), 2),
            "largest_value": round(max(values), 2),
            "stratum": stratum(max(values)),
            "filing_dates": sorted({p["filing_date"] for p in ps}),
            "acceptance_at": sorted({p["acceptance_at"] for p in ps if p["acceptance_at"]}),
            "first_known_at": min(p["known_at"] for p in ps).isoformat(),
            "last_known_at": max(p["known_at"] for p in ps).isoformat(),
            "sources": sorted({p["source"] or "?" for p in ps}),
            "ticker_resolved": is_resolvable_ticker(pos["ticker"]),
        }
    )
    return pos


def group_positions(purchases: Iterable[dict]) -> dict[str, dict]:
    """One position per (issuer, entry session), never per insider-day line."""
    groups: dict[str, tuple[str, date, list[dict]]] = {}
    for p in purchases:
        entry = entry_session(p["known_at"])
        pid = position_id(p["issuer_key"], entry)
        groups.setdefault(pid, (p["issuer_key"], entry, []))[2].append(p)
    return {pid: _position(pid, key, entry.isoformat(), close_instant(entry).isoformat(), ps)
            for pid, (key, entry, ps) in groups.items()}
