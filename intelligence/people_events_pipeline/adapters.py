"""Source rows -> canonical candidate rows, one adapter per source.

Every adapter returns ``(candidates, skips)``: a DataFrame with exactly
``CANDIDATE_COLUMNS`` and a ``Counter`` of ``"<source>:<reason>"`` -> rows
dropped. A row is dropped -- counted, never guessed -- when it has no
defensible known_at, no event date, no actor, or is not an act at all
(derived cluster rows, aggregate echoes).

Sources and why each is (or is not) read
-----------------------------------------
Read (primary records):

* SEC Form 3/4/5 structured data set, ``derived/nonderiv_transactions.parquet``
  (``form4_from_form345``) -- the authoritative Form 4 history, 2006 onward.
* ``signal_sources`` rows by ``source_type`` (``SOURCE_SPECS``).
* ``institutional_holdings`` (``thirteen_f_changes``) -- 13F positions, turned
  into position *changes* between a filer's consecutive filed quarters.

Never read (copies of the above; plan section 1.3, ``NEVER_A_CHANNEL``):
``insider_trades``, ``congressional_trades``, ``signal_data``, ``wealth_flows``,
``dollar_flows``, ``actor_connections``, ``lever_pullers``, ``influence_loops``,
``sector_health_snapshots``. Counting one of these alongside its source would
double every act.

Overwritable sources
---------------------
Several ``signal_sources`` writers upsert with ``ON CONFLICT ... DO UPDATE SET
signal_value = EXCLUDED.signal_value`` under a key that does not identify one
act (QuiverQuant uses a constant ``source_id`` per endpoint, so the key is
effectively ``(ticker, signal_date, signal_type)``). Two acts that share that
key overwrite each other, and the row's ``created_at`` then belongs to the
*first* payload, not the one now stored. For those sources ``created_at`` is
not a valid known_at bound; ``SourceSpec.overwritable`` makes the adapter use
the payload's own disclosure timestamp, else the materializer's own
observation time (``observed_at``), never ``created_at``.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

from intelligence.people_events_pipeline import rules as R

CANDIDATE_COLUMNS = [
    "channel", "dedup_key", "loose_key",
    "event_date", "known_at", "known_at_basis",
    "actor_id", "actor_id_basis", "actor_type", "actor_name", "co_actor_ids",
    "entity_ticker", "entity_cik", "entity_kind",
    "direction", "transaction_code", "size_usd",
    "source", "source_type", "source_record_id", "precedence",
    "accession", "document_type", "amended", "attrs",
]

# Lower = more authoritative for the act's descriptive fields (who, what,
# how much). known_at never uses precedence: it is the minimum valid bound
# across all sources of the act.
PRECEDENCE = {
    "sec_form345": 0,
    "edgar_native": 1,
    "sec_13f": 1,
    "congress_native": 1,
    "usaspending": 1,
    "lda": 1,
    "fara": 1,
    "quiverquant": 2,
}

NEVER_A_CHANNEL = frozenset({
    "insider_trades", "congressional_trades", "signal_data", "wealth_flows", "dollar_flows",
    "actor_connections", "lever_pullers", "influence_loops", "sector_health_snapshots",
})


@dataclass(frozen=True)
class SourceSpec:
    """How one ``signal_sources.source_type`` maps onto people_events."""

    source_type: str
    channel: str
    source: str
    overwritable: bool  # DO UPDATE writer: created_at is NOT a valid first_seen bound
    adapter: str  # function name in this module


SOURCE_SPECS: dict[str, SourceSpec] = {
    s.source_type: s
    for s in (
        SourceSpec("quiverquant:insider", "form4", "quiverquant", True, "form4_from_qq_insider"),
        SourceSpec("insider", "form4", "edgar_native", False, "form4_from_edgar_native"),
        SourceSpec("quiverquant:house", "congress", "quiverquant", True, "congress_from_qq"),
        SourceSpec("quiverquant:senate", "congress", "quiverquant", True, "congress_from_qq"),
        SourceSpec("congressional", "congress", "congress_native", False, "congress_from_native"),
        SourceSpec("gov_contract", "gov_contract", "usaspending", False, "gov_contract_from_usaspending"),
        SourceSpec("quiverquant:gov_contracts", "gov_contract_qq_aggregate", "quiverquant", True,
                   "gov_contract_qq_aggregate"),
        SourceSpec("lobbying", "lobbying", "lda", False, "lobbying_from_lda"),
        SourceSpec("quiverquant:lobbying", "lobbying", "quiverquant", True, "lobbying_from_qq"),
        SourceSpec("foreign_lobbying", "fara", "fara", False, "fara_from_signal_sources"),
    )
}

SIGNAL_SOURCE_COLUMNS = ["id", "source_type", "source_id", "ticker", "signal_date", "signal_type",
                         "signal_value", "created_at"]


def empty_candidates() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in CANDIDATE_COLUMNS})


def _finish(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return empty_candidates()
    frame = pd.DataFrame(rows)
    for col in CANDIDATE_COLUMNS:
        if col not in frame.columns:
            frame[col] = None
    frame["known_at"] = pd.to_datetime(frame["known_at"], utc=True)
    frame["event_date"] = pd.to_datetime(frame["event_date"]).dt.date
    return frame[CANDIDATE_COLUMNS]


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _min_bound(bounds: Iterable[tuple[datetime | None, str]]) -> tuple[datetime, str] | None:
    """Minimum of several valid known_at upper bounds; ties go to the stronger basis."""
    valid = [(t, b) for t, b in bounds if t is not None]
    if not valid:
        return None
    return min(valid, key=lambda tb: (tb[0], R.BASIS_RANK.get(tb[1], 9)))


def _float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(str(v).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None
    return None if np.isnan(f) else f


def _size(shares: float | None, price: float | None) -> float | None:
    if shares is None or price is None or price <= 0:
        return None
    return abs(shares * price)


# --- Form 4: SEC structured data set (GD3 history) -----------------------------------------

FORM345_TRANSACTION_TYPES = frozenset({"4", "4/A", "5", "5/A"})


def form4_from_form345(frame: pd.DataFrame) -> tuple[pd.DataFrame, Counter]:
    """``derived/nonderiv_transactions.parquet`` rows -> one candidate per transaction line.

    The derived file fans a joint filing out once per reporting owner. One
    transaction line is one act, so owners are folded back: the actor is the
    lowest owner CIK (VS1's convention) and the others go to ``co_actor_ids``.
    Vectorized: the full file is about 8.5M rows.
    """
    skips: Counter = Counter()
    src = "sec_form345"
    if frame.empty:
        return empty_candidates(), skips
    f = frame.copy()
    doc = f["document_type"].astype("string").str.strip().str.upper()
    keep = doc.isin(FORM345_TRANSACTION_TYPES)
    skips[f"{src}:not_a_transaction_form"] += int((~keep).sum())
    f, doc = f[keep.to_numpy()], doc[keep.to_numpy()]

    filed = pd.to_datetime(f["filing_date"].astype("string").str.slice(0, 10), format="%Y-%m-%d", errors="coerce")
    traded = pd.to_datetime(f["transaction_date"].astype("string").str.slice(0, 10), format="%Y-%m-%d",
                            errors="coerce")
    if "transaction_date_raw" in f.columns:
        raw = pd.to_datetime(f["transaction_date_raw"].astype("string").str.strip().str.upper(), format="%d-%b-%Y",
                             errors="coerce")
        traded = traded.fillna(raw)
    accession = f["accession_number"].astype("string").str.strip()
    code = f["transaction_code"].astype("string").str.strip().str.upper().str.slice(0, 1)
    issuer_cik = pd.to_numeric(f["issuer_cik"].astype("string").str.strip(), errors="coerce")
    issuer_tkr = R.normalize_ticker_series(f["issuer_ticker"].astype("object"))
    owner_cik = pd.to_numeric(f["owner_cik"].astype("string").str.strip(), errors="coerce")

    checks = [
        ("missing_accession", accession.isna() | (accession == "")),
        ("missing_filing_date", filed.isna()),
        ("missing_transaction_date", traded.isna()),
        ("missing_transaction_code", code.isna() | (code == "")),
        ("missing_issuer", issuer_cik.isna() & issuer_tkr.isna()),
        ("missing_owner", owner_cik.isna() & f["owner_name"].isna()),
        # A transaction cannot postdate the filing that reports it: a data
        # error, and if kept it would be an event known before it happened.
        ("transaction_after_filing", traded > filed),
    ]
    bad = pd.Series(False, index=f.index)
    for reason, mask in checks:
        mask = mask.fillna(False) & ~bad
        skips[f"{src}:{reason}"] += int(mask.sum())
        bad |= mask
    ok = (~bad).to_numpy()

    base = pd.DataFrame({
        "accession": accession[ok],
        "line": f["nonderiv_trans_sk"].astype("string").str.strip()[ok],
        "document_type": doc[ok],
        "amended": f["amended"].astype(bool)[ok] if "amended" in f.columns else False,
        "filed": filed[ok],
        "traded": traded[ok],
        "code": code[ok],
        "acq_disp": f["acquired_disposed_code"].astype("string").str.strip().str.upper()[ok],
        "shares": pd.to_numeric(f["shares"], errors="coerce")[ok],
        "price": pd.to_numeric(f["price_per_share"], errors="coerce")[ok],
        "issuer_cik": issuer_cik[ok],
        "issuer_tkr": issuer_tkr[ok],
        "owner_cik": owner_cik[ok],
        "owner_name": f["owner_name"].astype("object")[ok],
        "is_director": f["is_director"][ok] if "is_director" in f.columns else None,
        "is_officer": f["is_officer"][ok] if "is_officer" in f.columns else None,
        "is_ten_pct_owner": f["is_ten_pct_owner"][ok] if "is_ten_pct_owner" in f.columns else None,
    })
    if base.empty:
        return empty_candidates(), skips

    # Fold joint filers: one row per (accession, line); actor = lowest owner CIK.
    base["_owner_sort"] = base["owner_cik"].fillna(np.inf)
    base = base.sort_values(["accession", "line", "_owner_sort", "owner_name"], kind="mergesort")
    line_key = base["accession"] + "#" + base["line"].fillna("")
    sizes = line_key.map(line_key.value_counts())
    first = ~line_key.duplicated()
    co_actor = pd.Series("", index=base.index, dtype="object")
    multi = base[(sizes > 1).to_numpy()]
    if not multi.empty:
        mk = line_key[multi.index]
        others = (
            multi.assign(_k=mk, _o=multi["owner_cik"].map(lambda v: R.cik_text(int(v)) if pd.notna(v) else None))
            .groupby("_k")["_o"].agg(lambda s: ",".join(sorted({x for x in list(s)[1:] if x})))
        )
        co_actor.loc[multi.index] = mk.map(others).fillna("")
    skips[f"{src}:joint_filer_rows_folded"] += int((~first).sum())
    base = base[first.to_numpy()].copy()
    base["co_actor_ids"] = co_actor.loc[base.index]

    owner_tok = R.name_tokens_series(base["owner_name"])
    issuer_part = np.where(
        base["issuer_tkr"].notna(), "t:" + base["issuer_tkr"].fillna(""),
        "c:" + base["issuer_cik"].astype("Int64").astype("string").fillna("?"),
    )
    shares_key = base["shares"].abs().round().astype("Int64").astype("string").fillna("NA")
    date_s = base["traded"].dt.strftime("%Y-%m-%d")
    code_s = base["code"].fillna("?")
    tok = owner_tok.where(owner_tok != "", "?")
    loose = pd.Series(issuer_part, index=base.index) + "|" + tok + "|" + date_s + "|" + code_s
    dedup = R.KEY_VERSION["form4"] + "|" + loose + "|" + shares_key

    # The key carries the owner's *name* (live feeds have no owner CIK), so two
    # different reporting owners whose names fold alike (initials dropped)
    # could collide. Where one key spans several owner CIKs, every member gets
    # its CIK appended: never merge two known-different people.
    n_owners = base.groupby(dedup)["owner_cik"].transform("nunique")
    split = (n_owners > 1).to_numpy()
    skips[f"{src}:owner_name_collision_split"] += int(split.sum())
    if split.any():
        dedup = dedup.where(~split, dedup + "|o" + base["owner_cik"].astype("Int64").astype("string").fillna("?"))

    # Two lines of the SAME accession with an identical key are two acts (e.g.
    # two separate purchases of the same size): number the extras so they do
    # not collapse. Across accessions an identical key is the same act (an
    # amendment repeating a line), which is exactly what should merge.
    order = base.assign(_dk=dedup, _p=base["price"].fillna(-1.0),
                        _l=pd.to_numeric(base["line"], errors="coerce").fillna(-1))
    order = order.sort_values(["_dk", "accession", "_p", "_l"], kind="mergesort")
    rank = order.groupby(["_dk", "accession"], sort=False).cumcount()
    suffix = pd.Series(np.where(rank > 0, "|n" + (rank + 1).astype(str), ""), index=order.index)
    skips[f"{src}:same_accession_same_key_numbered"] += int((rank > 0).sum())
    dedup = dedup + suffix.reindex(base.index).fillna("")

    owner_text = base["owner_cik"].map(lambda v: R.cik_text(int(v)) if pd.notna(v) else None)
    has_cik = owner_text.notna()
    direction = base["code"].map(R.FORM4_CODE_DIRECTIONS).astype(object)
    direction = direction.where(direction.notna(), None)  # M/F/G/J...: no direction, as None not NaN
    size = (base["shares"] * base["price"]).abs().where(base["price"] > 0)
    attrs = pd.Series(
        [
            # The derived Form 3/4/5 file has no AFF10B5ONE column: unknown (D5).
            {"is_director": _b(d), "is_officer": _b(o), "is_ten_pct_owner": _b(t), "is_10b5_1": None}
            for d, o, t in zip(base["is_director"], base["is_officer"], base["is_ten_pct_owner"])
        ] if "is_director" in base.columns else [{}] * len(base),
        index=base.index,
    )
    out = pd.DataFrame({
        "channel": "form4",
        "dedup_key": dedup,
        "loose_key": loose,
        "event_date": base["traded"].dt.date,
        "known_at": R.section16_filing_known_at_series(base["filed"]),
        "known_at_basis": "filing",
        "actor_id": owner_text.where(has_cik, tok),
        "actor_id_basis": np.where(has_cik, "owner_cik", "normalized_name"),
        "actor_type": "insider",
        "actor_name": base["owner_name"],
        "co_actor_ids": base["co_actor_ids"],
        "entity_ticker": base["issuer_tkr"],
        "entity_cik": base["issuer_cik"].astype("Int64"),
        "entity_kind": "issuer",
        "direction": direction,
        "transaction_code": base["code"],
        "size_usd": size,
        "source": src,
        "source_type": "sec_form345:nonderiv",
        "source_record_id": base["accession"] + ":" + base["line"].fillna(""),
        "precedence": PRECEDENCE[src],
        "accession": base["accession"],
        "document_type": base["document_type"],
        "amended": base["amended"],
        "attrs": attrs,
    })
    return out[CANDIDATE_COLUMNS].reset_index(drop=True), skips


def _flag(v: Any) -> bool | None:
    """A source boolean that may be absent: missing/blank/unparseable -> None (unknown), never False."""
    if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, bool):
        return v
    text = str(v).strip().lower()
    if text in ("true", "t", "1", "y", "yes"):
        return True
    if text in ("false", "f", "0", "n", "no"):
        return False
    return None


def _b(v: Any) -> bool | None:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA:
        return None
    return bool(v)


# --- signal_sources adapters ---------------------------------------------------------------


def _rows(frame: pd.DataFrame) -> Iterable[dict[str, Any]]:
    for rec in frame.to_dict("records"):
        rec["signal_value"] = _payload(rec.get("signal_value"))
        yield rec


def _observed(spec: SourceSpec, observed_at: datetime | None) -> tuple[datetime | None, str]:
    return (R.to_utc(observed_at) if observed_at is not None else None), "first_seen"


def _created(spec: SourceSpec, rec: dict[str, Any]) -> tuple[datetime | None, str]:
    """created_at as a first_seen bound -- only for writers that never rewrite a stored payload."""
    if spec.overwritable:
        return None, "first_seen"
    return R.to_utc(rec.get("created_at")), "first_seen"


_QQ_OWNER = ("Name", "Insider", "InsiderName", "Reporter", "Owner", "OwnerName")
_QQ_CODE = ("TransactionCode", "transactionCode")
_QQ_ACQ = ("AcquiredDisposedCode", "acquiredDisposedCode")
_QQ_SHARES = ("Shares", "shares")
_QQ_PRICE = ("PricePerShare", "Price", "price", "pricePerShare")
_QQ_FILED = ("fileDate", "FileDate", "FilingDate")
_QQ_UPLOADED = ("uploaded", "Uploaded")


def form4_from_qq_insider(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                          ) -> tuple[pd.DataFrame, Counter]:
    """``quiverquant:insider``: signal_date is the transaction date; ``fileDate`` the filing date."""
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        sid = f"{spec.source_type}:{rec['id']}"
        traded = R.parse_date(rec.get("signal_date"))
        ticker = R.normalize_ticker(rec.get("ticker"))
        owner = R.first_present(p, _QQ_OWNER)
        code = R.first_present(p, _QQ_CODE)
        code = str(code).strip().upper()[:1] if code else None
        if traded is None or ticker is None or not owner:
            skips[f"{spec.source_type}:missing_date_ticker_or_owner"] += 1
            continue
        filed = R.parse_date(R.first_present(p, _QQ_FILED))
        if filed is not None and filed < traded:
            skips[f"{spec.source_type}:filing_before_transaction"] += 1
            continue
        bound = _min_bound([
            (R.section16_filing_known_at(filed) if filed else None, "filing"),
            (R.vendor_time_bound(R.first_present(p, _QQ_UPLOADED)), "first_seen"),
            _observed(spec, observed_at),
        ])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        if not code:
            skips[f"{spec.source_type}:missing_transaction_code_kept"] += 1
        shares = _float(R.first_present(p, _QQ_SHARES))
        price = _float(R.first_present(p, _QQ_PRICE))
        tok = R.name_tokens(owner)
        issuer = R.issuer_key(ticker, None)
        rows.append({
            "channel": "form4",
            "dedup_key": R.form4_dedup_key(issuer, tok, traded, code, shares),
            "loose_key": R.form4_loose_key(issuer, tok, traded, code),
            "event_date": traded, "known_at": bound[0], "known_at_basis": bound[1],
            "actor_id": tok, "actor_id_basis": "normalized_name", "actor_type": "insider",
            "actor_name": str(owner), "co_actor_ids": "",
            "entity_ticker": ticker, "entity_cik": None, "entity_kind": "issuer",
            "direction": R.form4_direction(code, R.first_present(p, _QQ_ACQ)),
            "transaction_code": code, "size_usd": _size(shares, price),
            "source": spec.source, "source_type": spec.source_type, "source_record_id": sid,
            "precedence": PRECEDENCE[spec.source], "accession": None, "document_type": None, "amended": None,
            # QuiverQuant's insider payload carries no Rule 10b5-1 flag: unknown,
            # never "not planned" (a density feature must not count planned
            # sales as discretionary because the field was absent).
            "attrs": {"is_10b5_1": None},
        })
    return _finish(rows), skips


def form4_from_edgar_native(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                            ) -> tuple[pd.DataFrame, Counter]:
    """``insider`` (ingestion/altdata/insider_filings.py): one row per (insider, ticker, date, type).

    ``CLUSTER_BUY`` rows are derived from other rows (not acts) and derivative
    lines are outside the non-derivative channel the SEC history defines.
    """
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        stype = str(rec.get("signal_type") or "").upper()
        if stype == "CLUSTER_BUY":
            skips[f"{spec.source_type}:derived_cluster_row"] += 1
            continue
        if bool(p.get("is_derivative")):
            skips[f"{spec.source_type}:derivative_line"] += 1
            continue
        traded = R.parse_date(rec.get("signal_date"))
        ticker = R.normalize_ticker(rec.get("ticker"))
        owner = rec.get("source_id")
        if traded is None or ticker is None or not owner:
            skips[f"{spec.source_type}:missing_date_ticker_or_owner"] += 1
            continue
        filed = R.parse_date(p.get("filing_date"))
        if filed is not None and filed < traded:
            skips[f"{spec.source_type}:filing_before_transaction"] += 1
            continue
        bound = _min_bound([
            (R.section16_filing_known_at(filed) if filed else None, "filing"),
            _created(spec, rec),
        ])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        code = str(p.get("transaction_code") or "").strip().upper()[:1] or None
        if code is None:
            skips[f"{spec.source_type}:missing_transaction_code_kept"] += 1
        shares = _float(p.get("shares"))
        price = _float(p.get("price"))
        tok = R.name_tokens(owner)
        issuer = R.issuer_key(ticker, None)
        acq = {"BUY": "A", "SELL": "D"}.get(stype.replace("UNUSUAL_", ""))
        accession = str(p.get("accession") or "").strip() or None
        rows.append({
            "channel": "form4",
            "dedup_key": R.form4_dedup_key(issuer, tok, traded, code, shares),
            "loose_key": R.form4_loose_key(issuer, tok, traded, code),
            "event_date": traded, "known_at": bound[0], "known_at_basis": bound[1],
            "actor_id": tok, "actor_id_basis": "normalized_name", "actor_type": "insider",
            "actor_name": str(owner), "co_actor_ids": "",
            "entity_ticker": ticker, "entity_cik": None, "entity_kind": "issuer",
            "direction": R.form4_direction(code, acq),
            "transaction_code": code, "size_usd": _size(shares, price),
            "source": spec.source, "source_type": spec.source_type,
            "source_record_id": f"{spec.source_type}:{rec['id']}",
            "precedence": PRECEDENCE[spec.source], "accession": accession, "document_type": None, "amended": None,
            "attrs": {"is_10b5_1": _flag(p.get("is_10b5_1"))},
        })
    return _finish(rows), skips


# --- congress ------------------------------------------------------------------------------

_QQ_MEMBER = ("Representative", "Senator", "Name", "Politician", "Member")
_QQ_BIOGUIDE = ("BioGuideID", "BioguideID", "bioguide_id", "bioguide")
_QQ_TXN = ("Transaction", "Type", "TransactionType")
_QQ_RANGE = ("Range", "Amount", "amount_range")
_QQ_TRADED = ("TransactionDate", "Traded", "transaction_date")
_QQ_DISCLOSED = ("ReportDate", "DisclosureDate", "Filed", "disclosure_date", "Disclosed")
_QQ_LAST_MODIFIED = ("last_modified", "LastModified")


def congress_from_qq(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                     ) -> tuple[pd.DataFrame, Counter]:
    """``quiverquant:house`` / ``quiverquant:senate``.

    known_at = min over valid bounds: a disclosure/report date in the payload
    (next session open after it; basis ``filing``), QuiverQuant
    ``last_modified`` when it is on/after the trade (an upper bound: an edit
    can only come after the record existed; basis ``qq_last_modified``), and
    the materializer's own observation time (``first_seen``). The source is
    overwritable, so ``created_at`` is never used.
    """
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        traded = R.parse_date(R.first_present(p, _QQ_TRADED)) or R.parse_date(rec.get("signal_date"))
        ticker = R.normalize_ticker(rec.get("ticker"))
        member = R.first_present(p, _QQ_MEMBER)
        if traded is None or ticker is None or not member:
            skips[f"{spec.source_type}:missing_date_ticker_or_member"] += 1
            continue
        disclosed = R.parse_date(R.first_present(p, _QQ_DISCLOSED))
        if disclosed is not None and disclosed < traded:
            disclosed = None
            skips[f"{spec.source_type}:disclosure_before_trade_ignored"] += 1
        raw_modified = R.first_present(p, _QQ_LAST_MODIFIED)
        modified_day = R.parse_date(raw_modified)
        modified = R.vendor_time_bound(raw_modified) if modified_day is not None and modified_day >= traded else None
        bound = _min_bound([
            (R.next_session_open_after(disclosed) if disclosed else None, "filing"),
            (modified, "qq_last_modified"),
            _observed(spec, observed_at),
        ])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        direction = R.congress_direction(R.first_present(p, _QQ_TXN))
        tok = R.name_tokens(member)
        bioguide = R.first_present(p, _QQ_BIOGUIDE)
        band = R.amount_band_low(R.first_present(p, _QQ_RANGE))
        rows.append(_congress_row(spec, rec, tok, member, bioguide, ticker, traded, direction, band, bound,
                                  {"chamber": "senate" if spec.source_type.endswith("senate") else "house"}))
    return _finish(rows), skips


def congress_from_native(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                         ) -> tuple[pd.DataFrame, Counter]:
    """``congressional`` (ingestion/altdata/congressional.py, ``DO NOTHING`` writer).

    ``disclosure_basis == 'statutory_bound'`` means the writer put trade + 45
    days in ``disclosure_date``. Late PTRs exist, so that is not an upper
    bound and is ignored; only a ``reported`` date or ``created_at`` count.
    """
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        traded = R.parse_date(rec.get("signal_date"))
        ticker = R.normalize_ticker(rec.get("ticker"))
        member = rec.get("source_id")
        if traded is None or ticker is None or not member:
            skips[f"{spec.source_type}:missing_date_ticker_or_member"] += 1
            continue
        # Only an explicit ``reported`` basis is a disclosure date. Rows written
        # before GD-FIX (#694) carry no basis and stored the *transaction*
        # date as the disclosure date ("lag 0 is not real"), so a missing
        # basis means "no disclosure date", not "reported".
        basis = p.get("disclosure_basis")
        disclosed = R.parse_date(p.get("disclosure_date")) if basis == "reported" else None
        if basis == "statutory_bound":
            skips[f"{spec.source_type}:statutory_bound_not_used"] += 1
        elif basis is None and p.get("disclosure_date"):
            skips[f"{spec.source_type}:pre_gdfix_disclosure_date_not_used"] += 1
        if disclosed is not None and disclosed <= traded:
            disclosed = None
        bound = _min_bound([
            (R.next_session_open_after(disclosed) if disclosed else None, "filing"),
            _created(spec, rec),
        ])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        direction = {"BUY": "buy", "SELL": "sell"}.get(str(rec.get("signal_type") or "").upper())
        band = R.amount_band_low(p.get("amount_range"))
        rows.append(_congress_row(spec, rec, R.name_tokens(member), member, None, ticker, traded, direction, band,
                                  bound, {"chamber": p.get("chamber")}))
    return _finish(rows), skips


def _congress_row(spec, rec, tok, member, bioguide, ticker, traded, direction, band, bound, attrs):
    return {
        "channel": "congress",
        "dedup_key": R.congress_dedup_key(tok, ticker, traded, direction, band),
        "loose_key": "|".join([tok or "?", ticker, traded.isoformat()]),
        "event_date": traded, "known_at": bound[0], "known_at_basis": bound[1],
        "actor_id": str(bioguide) if bioguide else tok,
        "actor_id_basis": "bioguide" if bioguide else "normalized_name",
        "actor_type": "congress_member", "actor_name": str(member), "co_actor_ids": "",
        "entity_ticker": ticker, "entity_cik": None, "entity_kind": "issuer",
        "direction": direction, "transaction_code": None, "size_usd": None,
        "source": spec.source, "source_type": spec.source_type, "source_record_id": f"{spec.source_type}:{rec['id']}",
        "precedence": PRECEDENCE[spec.source], "accession": None, "document_type": None, "amended": None,
        "attrs": {**attrs, "amount_band_low": band},
    }


# --- government contracts ------------------------------------------------------------------


def gov_contract_from_usaspending(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                                  ) -> tuple[pd.DataFrame, Counter]:
    """``gov_contract`` (USASpending). Event date = Action Date; never the Start Date.

    USASpending publishes awards with a lag (DoD about 90 days) and there is
    no publication timestamp in the payload, so the only valid bound is
    ``created_at`` (``DO NOTHING`` writer). Rows whose event date is the
    ingestion-date fallback (``award_date_basis == 'first_seen'``) have no act
    date at all and are dropped.
    """
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        if p.get("award_date_basis", "first_seen") != "action_date":
            skips[f"{spec.source_type}:no_action_date"] += 1
            continue
        acted = R.parse_date(rec.get("signal_date"))
        ticker = R.normalize_ticker(rec.get("ticker"))
        award_id = str(p.get("award_id") or "").strip()
        if acted is None or ticker is None or not award_id:
            skips[f"{spec.source_type}:missing_date_ticker_or_award_id"] += 1
            continue
        bound = _min_bound([_created(spec, rec)])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        agency = str(rec.get("source_id") or "")
        amount = _float(p.get("amount"))
        rows.append({
            "channel": "gov_contract",
            "dedup_key": R.gov_contract_dedup_key(award_id, acted),
            "loose_key": "|".join([ticker, agency.upper(), acted.isoformat()]),
            "event_date": acted, "known_at": bound[0], "known_at_basis": bound[1],
            "actor_id": R.name_tokens(agency) or "?", "actor_id_basis": "normalized_name", "actor_type": "agency",
            "actor_name": agency, "co_actor_ids": "",
            "entity_ticker": ticker, "entity_cik": None, "entity_kind": "issuer",
            "direction": "award", "transaction_code": None, "size_usd": abs(amount) if amount is not None else None,
            "source": spec.source, "source_type": spec.source_type, "source_record_id": f"{spec.source_type}:{rec['id']}",
            "precedence": PRECEDENCE[spec.source], "accession": None, "document_type": None, "amended": None,
            "attrs": {"award_id": award_id, "recipient_name": p.get("recipient_name")},
        })
    return _finish(rows), skips


# The quarter-end date GD-FIX's writer (quiverquant._gov_contract_period_date)
# stores as signal_date. It reads (Year, Qtr) as a CALENDAR quarter; used here
# only to recognise rows written under that post-fix key.
_WRITER_QUARTER_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


def federal_fiscal_quarter(year: int, qtr: int) -> tuple[date, date]:
    """(start, end) of US federal fiscal quarter ``qtr`` of fiscal ``year``.

    QuiverQuant's gov-contract (Year, Qtr) is the federal fiscal quarter: the
    E1 PIT canary found 2,947 aggregates "known" before the calendar quarter
    they were mapped to had begun (e.g. 2026 Q4 seen 2026-09-11), which only
    the fiscal reading explains (FY2026 Q4 = 2026-07-01..2026-09-30). FY Y
    starts 1 October of Y-1.
    """
    if qtr == 1:
        return date(year - 1, 10, 1), date(year - 1, 12, 31)
    start = date(year, 3 * qtr - 5, 1)
    end = {2: date(year, 3, 31), 3: date(year, 6, 30), 4: date(year, 9, 30)}[qtr]
    return start, end


def gov_contract_qq_aggregate(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                              ) -> tuple[pd.DataFrame, Counter]:
    """``quiverquant:gov_contracts``: a quarterly per-ticker aggregate, never an award.

    Before GD-FIX (#694) each daily pull inserted a new row dated the pull
    day; those rows were only rewritten within that same day, so each is a
    valid "seen by the end of created_at's day" observation. Rows dated the
    quarter end (the post-fix key) are rewritten in place across days, so
    they only support the materializer's own observation time. The merge
    step then takes the earliest bound across all rows with the same
    (ticker, quarter, amount).
    """
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        ticker = R.normalize_ticker(rec.get("ticker"))
        try:
            year = int(R.first_present(p, ("Year", "year")))
            qtr = int(R.first_present(p, ("Qtr", "qtr", "Quarter", "quarter")))
            if qtr not in (1, 2, 3, 4):
                raise ValueError(qtr)
            start, end = federal_fiscal_quarter(year, qtr)
            writer_end = date(year, *_WRITER_QUARTER_END[qtr])
        except (TypeError, ValueError, KeyError):
            skips[f"{spec.source_type}:missing_year_or_quarter"] += 1
            continue
        amount = _float(R.first_present(p, ("Amount", "amount")))
        if ticker is None or amount is None:
            skips[f"{spec.source_type}:missing_ticker_or_amount"] += 1
            continue
        stored = R.parse_date(rec.get("signal_date"))
        created = R.to_utc(rec.get("created_at"))
        snapshot_bound = None
        if stored is not None and stored != writer_end and created is not None:
            snapshot_bound = R.next_session_open_after(created.date())  # pre-fix daily snapshot
        bound = _min_bound([(snapshot_bound, "first_seen"), _observed(spec, observed_at)])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        if bound[0].date() <= end:
            skips[f"{spec.source_type}:partial_quarter_aggregate_kept"] += 1
        rows.append({
            "channel": "gov_contract_qq_aggregate",
            "dedup_key": R.qq_gov_aggregate_dedup_key(ticker, year, qtr, amount),
            "loose_key": "|".join([ticker, f"{year}Q{qtr}"]),
            # The aggregate covers a whole quarter and QuiverQuant publishes it
            # while the quarter is still running, so the act's date is the
            # period start (period end goes to attrs).
            "event_date": start, "known_at": bound[0], "known_at_basis": bound[1],
            "actor_id": "USG", "actor_id_basis": "agency_code", "actor_type": "agency_aggregate",
            "actor_name": "US federal government (aggregate)", "co_actor_ids": "",
            "entity_ticker": ticker, "entity_cik": None, "entity_kind": "issuer",
            "direction": "award", "transaction_code": None, "size_usd": abs(amount),
            "source": spec.source, "source_type": spec.source_type, "source_record_id": f"{spec.source_type}:{rec['id']}",
            "precedence": PRECEDENCE[spec.source], "accession": None, "document_type": None, "amended": None,
            "attrs": {"fiscal_year": year, "fiscal_qtr": qtr, "period_end": end.isoformat()},
        })
    return _finish(rows), skips


# --- lobbying / FARA -----------------------------------------------------------------------


def lobbying_from_lda(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                      ) -> tuple[pd.DataFrame, Counter]:
    """``lobbying`` (Senate LDA): signal_date = ``dt_posted`` date, source_id = filing_uuid."""
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        posted = R.parse_date(rec.get("signal_date"))
        ticker = R.normalize_ticker(rec.get("ticker"))
        registrant = p.get("registrant_name")
        if posted is None or ticker is None or not registrant:
            skips[f"{spec.source_type}:missing_date_ticker_or_registrant"] += 1
            continue
        bound = _min_bound([(R.next_session_open_after(posted), "filing"), _created(spec, rec)])
        created = R.to_utc(rec.get("created_at"))
        attrs = {"client_name": p.get("client_name"), "filing_uuid": rec.get("source_id")}
        if created is not None and created.date() == posted:
            # lobbying.py falls back to the ingestion date when dt_posted is
            # missing; a same-day posted date cannot be told apart from that.
            attrs["posted_date_may_be_ingestion_date"] = True
        rows.append(_lobby_row(spec, rec, ticker, registrant, posted, p.get("amount"), bound, attrs))
    return _finish(rows), skips


def lobbying_from_qq(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                     ) -> tuple[pd.DataFrame, Counter]:
    """``quiverquant:lobbying``. The payload has no verified filing timestamp: first_seen only."""
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        filed = R.parse_date(rec.get("signal_date"))
        ticker = R.normalize_ticker(rec.get("ticker"))
        registrant = R.first_present(p, ("Registrant", "registrant"))
        if filed is None or ticker is None or not registrant:
            skips[f"{spec.source_type}:missing_date_ticker_or_registrant"] += 1
            continue
        bound = _min_bound([_observed(spec, observed_at)])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        rows.append(_lobby_row(spec, rec, ticker, registrant, filed, R.first_present(p, ("Amount", "amount")), bound,
                               {"client_name": R.first_present(p, ("Client", "client"))}))
    return _finish(rows), skips


def _lobby_row(spec, rec, ticker, registrant, filed, amount, bound, attrs):
    amt = _float(amount)
    tok = R.name_tokens(registrant)
    return {
        "channel": "lobbying",
        "dedup_key": R.lobbying_dedup_key(ticker, tok, filed, amt),
        "loose_key": "|".join([ticker, tok or "?", filed.isoformat()]),
        "event_date": filed, "known_at": bound[0], "known_at_basis": bound[1],
        "actor_id": tok or "?", "actor_id_basis": "normalized_name", "actor_type": "lobbying_registrant",
        "actor_name": str(registrant), "co_actor_ids": "",
        "entity_ticker": ticker, "entity_cik": None, "entity_kind": "issuer",
        "direction": None, "transaction_code": None, "size_usd": abs(amt) if amt is not None else None,
        "source": spec.source, "source_type": spec.source_type, "source_record_id": f"{spec.source_type}:{rec['id']}",
        "precedence": PRECEDENCE[spec.source], "accession": None, "document_type": None, "amended": None,
        "attrs": attrs,
    }


def fara_from_signal_sources(frame: pd.DataFrame, spec: SourceSpec, observed_at: datetime | None
                             ) -> tuple[pd.DataFrame, Counter]:
    """``foreign_lobbying`` (DOJ FARA). Its ``ticker`` is a *sector proxy*, not an issuer."""
    skips: Counter = Counter()
    rows = []
    for rec in _rows(frame):
        p = rec["signal_value"]
        acted = R.parse_date(rec.get("signal_date"))
        registrant = rec.get("source_id")
        if acted is None or not registrant:
            skips[f"{spec.source_type}:missing_date_or_registrant"] += 1
            continue
        bound = _min_bound([_created(spec, rec)])
        if bound is None:
            skips[f"{spec.source_type}:no_known_at"] += 1
            continue
        tok = R.name_tokens(registrant)
        rows.append({
            "channel": "fara",
            "dedup_key": R.fara_dedup_key(tok, R.name_tokens(p.get("principal_name")), acted,
                                          str(p.get("activity_type") or "")),
            "loose_key": "|".join([tok or "?", acted.isoformat()]),
            "event_date": acted, "known_at": bound[0], "known_at_basis": bound[1],
            "actor_id": tok or "?", "actor_id_basis": "normalized_name", "actor_type": "foreign_agent",
            "actor_name": str(registrant), "co_actor_ids": "",
            "entity_ticker": R.normalize_ticker(rec.get("ticker")), "entity_cik": None, "entity_kind": "sector_proxy",
            "direction": None, "transaction_code": None, "size_usd": _float(p.get("compensation")),
            "source": spec.source, "source_type": spec.source_type, "source_record_id": f"{spec.source_type}:{rec['id']}",
            "precedence": PRECEDENCE[spec.source], "accession": None, "document_type": None, "amended": None,
            "attrs": {"country": p.get("country"), "principal_name": p.get("principal_name")},
        })
    return _finish(rows), skips


# --- 13F position changes ------------------------------------------------------------------

MAX_QUARTER_GAP_DAYS = 100


def thirteen_f_changes(holdings: pd.DataFrame) -> tuple[pd.DataFrame, Counter]:
    """``institutional_holdings`` -> one event per (filer, security, quarter) position change.

    A change needs the filer's previous *filed* quarter (at most
    ``MAX_QUARTER_GAP_DAYS`` earlier): a filer's first quarter in the table
    would otherwise make every position look "new" -- a coverage artifact,
    not an act. known_at = next session open after the current quarter's
    ``filed_date`` (basis ``filing``). The writer rewrites rows in place on
    amendment, so a row's ``filed_date`` always belongs to its current
    content; rows without one are dropped (``created_at`` is not a valid
    bound for an overwritable row).
    """
    skips: Counter = Counter()
    src = "sec_13f"
    if holdings.empty:
        return empty_candidates(), skips
    h = holdings.copy()
    h["report_date"] = pd.to_datetime(h["report_date"]).dt.date
    h["filed"] = pd.to_datetime(h["filed_date"], errors="coerce").dt.date
    no_filed = h["filed"].isna()
    skips[f"{src}:no_filed_date"] += int(no_filed.sum())
    h = h[~no_filed.to_numpy()]
    h["filer"] = h["cik"].map(lambda v: f"cik:{int(str(v).strip())}" if v is not None and str(v).strip().isdigit()
                              else None)
    h["filer"] = h["filer"].fillna("name:" + R.name_tokens_series(h["holder_name"]))
    h["security"] = h["cusip"].astype("string").str.strip().str.upper()
    h["security"] = ("cusip:" + h["security"]).where(h["security"].notna() & (h["security"] != ""),
                                                    "tkr:" + R.normalize_ticker_series(h["ticker"]).fillna("?"))
    h["shares"] = pd.to_numeric(h["shares_held"], errors="coerce").fillna(0.0)
    h["value"] = pd.to_numeric(h["value_usd"], errors="coerce")
    h = h.groupby(["filer", "security", "report_date"], as_index=False).agg(
        shares=("shares", "sum"), value=("value", "sum"), filed=("filed", "max"),
        ticker=("ticker", "first"), holder_name=("holder_name", "first"), row_id=("id", "min"),
    )
    rows = []
    for filer, g in h.groupby("filer", sort=True):
        quarters = sorted(g["report_date"].unique())
        filed_by_q = g.groupby("report_date")["filed"].max().to_dict()
        by_q = {q: gq.set_index("security") for q, gq in g.groupby("report_date")}
        skips[f"{src}:first_quarter_coverage_start"] += int(len(by_q[quarters[0]]))
        for prev_q, q in zip(quarters, quarters[1:]):
            cur, prev = by_q[q], by_q[prev_q]
            if (q - prev_q).days > MAX_QUARTER_GAP_DAYS:
                skips[f"{src}:non_consecutive_quarter"] += int(len(cur))
                continue
            # A change compares two filings; it is knowable only once BOTH are
            # public (the earlier one can be late, or amended in place).
            known = R.next_session_open_after(max(filed_by_q[q], filed_by_q[prev_q]))
            for security in sorted(set(cur.index) | set(prev.index)):
                now_sh = float(cur.loc[security, "shares"]) if security in cur.index else 0.0
                was_sh = float(prev.loc[security, "shares"]) if security in prev.index else 0.0
                delta = now_sh - was_sh
                if delta == 0:
                    continue
                if security in cur.index:
                    ref = cur.loc[security]
                else:
                    ref = prev.loc[security]
                code = "NEW" if was_sh == 0 else ("EXIT" if now_sh == 0 else ("INC" if delta > 0 else "DEC"))
                per_share = (float(ref["value"]) / float(ref["shares"])) if ref["shares"] and pd.notna(ref["value"]) else None
                ticker = R.normalize_ticker(ref["ticker"])
                rows.append({
                    "channel": "thirteen_f",
                    "dedup_key": R.thirteen_f_dedup_key(filer, security, q),
                    "loose_key": "|".join([filer, ticker or security, q.isoformat()]),
                    "event_date": q, "known_at": known, "known_at_basis": "filing",
                    "actor_id": filer.split(":", 1)[1] if filer.startswith("cik:") else filer,
                    "actor_id_basis": "filer_cik" if filer.startswith("cik:") else "normalized_name",
                    "actor_type": "institution", "actor_name": str(ref["holder_name"]), "co_actor_ids": "",
                    "entity_ticker": ticker, "entity_cik": None, "entity_kind": "issuer",
                    "direction": "buy" if delta > 0 else "sell", "transaction_code": code,
                    "size_usd": abs(delta) * per_share if per_share is not None else None,
                    "source": src, "source_type": "institutional_holdings",
                    "source_record_id": f"institutional_holdings:{int(ref['row_id'])}",
                    "precedence": PRECEDENCE[src], "accession": None, "document_type": "13F-HR", "amended": None,
                    "attrs": {"shares_delta": delta, "security": security},
                })
    return _finish(rows), skips


# --- dispatch ------------------------------------------------------------------------------

ADAPTERS: dict[str, Callable[..., tuple[pd.DataFrame, Counter]]] = {
    "form4_from_qq_insider": form4_from_qq_insider,
    "form4_from_edgar_native": form4_from_edgar_native,
    "congress_from_qq": congress_from_qq,
    "congress_from_native": congress_from_native,
    "gov_contract_from_usaspending": gov_contract_from_usaspending,
    "gov_contract_qq_aggregate": gov_contract_qq_aggregate,
    "lobbying_from_lda": lobbying_from_lda,
    "lobbying_from_qq": lobbying_from_qq,
    "fara_from_signal_sources": fara_from_signal_sources,
}


def from_signal_sources(frame: pd.DataFrame, observed_at: datetime | None) -> tuple[pd.DataFrame, Counter]:
    """Dispatch ``signal_sources`` rows to their adapters by ``source_type``; unknown types are counted."""
    skips: Counter = Counter()
    parts = []
    if frame.empty:
        return empty_candidates(), skips
    for stype, group in frame.groupby("source_type", sort=True):
        spec = SOURCE_SPECS.get(stype)
        if spec is None:
            skips[f"{stype}:not_a_people_channel"] += int(len(group))
            continue
        cands, s = ADAPTERS[spec.adapter](group, spec, observed_at)
        skips.update(s)
        parts.append(cands)
    parts = [p for p in parts if not p.empty]
    return (pd.concat(parts, ignore_index=True) if parts else empty_candidates()), skips
