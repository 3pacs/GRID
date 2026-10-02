#!/usr/bin/env python3
"""Build the all-issuers ``security_master`` seed artifact (files only, no database).

GD1's seed (``scripts/seed_security_master_technology.py``) put 100 Technology
entities into ``security_master``, so only ~7% of Form 4 events resolve onto an
entity (``intelligence.people_events_pipeline.security``). This builder widens
the proposed rows to every SEC issuer CIK that appears in the local SEC data,
and freezes them in an artifact that ``scripts/apply_security_master_seed.py``
loads later. It never opens a database connection and never calls EDGAR.

Inputs (all local files):

``--company-tickers``   SEC ``company_tickers.json`` (repeatable; first file wins on a
                        repeated CIK). Gives the current name and the current ticker(s).
``--submissions``       Form 3/4/5 ``submissions.parquet`` (``accession_number, filing_date,
                        issuer_cik, issuer_ticker, ...``). The point-in-time ticker history is
                        derived from it.
``--nonderiv``          optional Form 4 ``nonderiv_transactions.parquet``; only used as the
                        fallback name source (the latest ``issuer_name`` per issuer CIK).
``--sic-map``           optional ``issuer_sic_map.jsonl`` (one SEC submissions record per
                        line). Gives ``security_master.sic``. It is the issuer's CURRENT
                        classification at fetch time, not point-in-time, and is recorded as such
                        in ``provenance``. Without it ``sic`` stays NULL.

Output (``--out-dir``; refuses to overwrite):

``security_master_seed.jsonl``  one JSON object per line, ``{"t": "sm", ...}`` for a
                               ``security_master`` row and ``{"t": "si", ...}`` for a
                               ``security_identifiers`` row. Sorted, so a rebuild from the same
                               inputs is byte-identical (idempotent).
``conflicts.json``             every ticker claimed by two CIKs over overlapping windows.
``ticker_noise.json``          the raw ``issuer_ticker`` values the cleaner rejected, with counts.
``receipt.json``               input sha256s, the output sha256, the code SHA and the counts.

Rows (see the PR body for the rationale):

* ``security_master``: one row per issuer CIK, ``entity_id = sm_<10-digit CIK>``.
* ``cik`` identifier: ``id_value`` is the unpadded CIK, ``valid_from`` the first filing date seen.
* ``ticker`` identifiers, dated from the filings. For each ``(issuer_cik, ticker)``:
  ``valid_from`` is the first filing date naming that ticker; ``valid_to`` is the day before the
  first LATER filing date by that issuer that names a ticker outside this ticker's share-class family
  (see ``ticker_families``: ISCA/ISCB, GOOG/GOOGL), or NULL when the issuer's latest ticker-bearing
  filing is still in the family. Several tickers can be open at once: dual-class issuers file under
  either class, so a class missing from the latest filing has not stopped trading.
* ``company_tickers`` supplies a ticker that no filing named, dated ``--tickers-as-of`` (the
  snapshot day) with ``valid_to`` NULL.
* An issuer that went silent while its ticker was still "open" (its last filing naming it is older than
  another CIK's first filing naming it) is closed the day before the other CIK starts: the ticker was
  handed over, not contested (``apply_handoff_clamp``; ``--no-handoff-clamp`` keeps the literal rule).
  The row keeps ``conflict_detail.kind = ticker_reuse_handoff`` and ``conflict_flag`` false.
* A ticker claimed by two different CIKs over overlapping windows (both still filing under it) is NOT
  resolved here: both rows keep ``conflict_flag`` true, ``is_primary`` false, and the pair is written to
  ``conflicts.json``.

Not touched: ``security_sector_membership`` (non-Technology sector membership needs a SIC-to-sector
mapping, a later step) and every existing seed row (the loader only ever INSERTs).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess  # nosec B404
import sys
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd  # noqa: E402

from intelligence.people_events_pipeline.rules import normalize_ticker  # noqa: E402
from intelligence.security_master import (  # noqa: E402
    ID_SCHEME_CIK,
    ID_SCHEME_TICKER,
    entity_id_for_cik,
    evaluate_delisting_candidate,
)

BUILDER_VERSION = "all_issuers_v1"
ENTITY_SOURCE = BUILDER_VERSION
SOURCE_FILINGS = f"{BUILDER_VERSION}:sec_form345"
SOURCE_COMPANY_TICKERS = f"{BUILDER_VERSION}:sec_company_tickers"
SEED_FILE = "security_master_seed.jsonl"

# --- ticker cleaning ------------------------------------------------------------------------
#
# ``normalize_ticker`` (the consumer's rule) turns "BRK.B" into BRKB and rejects NONE/N/A, but it
# folds whitespace and punctuation away, so a multi-ticker field ("ISCA, ISCB") would become the one
# bogus ticker ISCAISCB-or-None and "NYSE: KRC" the bogus NYSEKRC. The filer noise measured in
# submissions.parquet (2006-2026) is handled here first; each cleaned token still goes through
# ``normalize_ticker`` so the stored value is exactly what the consumer will compare against.

_PLACEHOLDERS = frozenset({
    "", "NONE", "N/A", "NA", "N.A.", "N.A", "NULL", "NAN", "NIL", "-", "--", "0", "NO SYMBOL",
    "NO TICKER", "NO TRADING SYMBOL", "NOT APPLICABLE", "NOT LISTED", "NOT TRADED", "NOT AVAILABLE",
    "NOT PUBLICLY TRADED", "NONE.", "UNKNOWN", "PRIVATE", "TBD", "TBA", "N A", "N/A.",
})
_POST_PLACEHOLDERS = frozenset({
    "NONE", "NA", "NULL", "NAN", "NIL", "NOSYMBOL", "NOTICKER", "NOTAPPLICABLE", "NOTLISTED",
    "NOTTRADED", "UNKNOWN", "PRIVATE", "TBD", "TBA",
})
_EXCHANGE_WORDS = frozenset({
    "NYSE", "NASDAQ", "NASD", "AMEX", "NYSEMKT", "NYSEARCA", "NYSEAMERICAN", "OTC", "OTCBB", "OTCQB",
    "OTCQX", "NMS", "NASDAQGS", "NASDAQGM", "NASDAQCM", "NASDAQNM", "PINK",
})
# Short market tags ("NWIN (OB)"): dropped beside a ticker, kept when alone because NM, PK and NQ are real tickers.
_MARKET_TAGS = frozenset({"OB", "PK", "OQ", "NM", "NQ"})
# A ticker on another country's exchange is not a US ticker: it can never match a US-ticker feed, and keeping
# it would only add collisions, so a field carrying one of these tags is rejected, not cleaned.
_FOREIGN_EXCHANGES = frozenset({"ASX", "AX", "TSX", "TSXV", "TO", "LSE", "LN", "HK", "GR", "OSE", "SI", "NZ", "JP", "TYO"})
_EXCHANGE_PREFIX = re.compile(
    r"^(?:NYSE(?:\s*(?:MKT|AMERICAN|ARCA|AMEX))?|NASDAQ(?:\s*(?:GS|GM|CM|NM|NMS))?|NASD|AMEX|NYSEMKT|"
    r"NYSEARCA|OTCBB|OTCQB|OTCQX|OTC(?:\s*MARKETS)?|NMS|TSX|TSXV|LSE|PINK(?:\s*SHEETS)?)\s*[:\-]\s*"
)
_EXCHANGE_SUFFIX = re.compile(r"[.\-](?:PK|OB|OQ|OTC|NYSE|NASDAQ)$")
# "BDG/BDGA", "KELYA/KELYB", "BRK.A/BRK.B": a slash between two 3+ character halves separates two tickers,
# while "BRK/B" and "BF/B" (a 1-2 character half) are one ticker with a class separator.
_SLASH_PAIR = re.compile(r"([A-Z0-9.\-]+)/([A-Z0-9.\-]+)")
_MAX_TICKER_LEN = 6
_CLASS_SHORTHAND = re.compile(r"^([A-Z0-9.\-]+)\s+/\s+([A-Z])$")  # "BWINA / B" = BWINA and BWINB
_FIELD_SPLIT = re.compile(r"\s*(?:[,;&|_]|\s/\s)\s*|\s+AND\s+")
_BRACKETS = str.maketrans({c: " " for c in "()[]{}\"'`"})


def _common_prefix_len(parts: list[str]) -> int:
    n = 0
    for chars in zip(*parts):
        if len(set(chars)) != 1:
            break
        n += 1
    return n


def _slash_pairs_to_commas(s: str) -> str:
    return _SLASH_PAIR.sub(lambda m: f"{m.group(1)},{m.group(2)}" if min(len(m.group(1)), len(m.group(2))) >= 3 else m.group(0), s)


def _split_whitespace(token: str) -> tuple[list[str], Optional[str]]:
    """A token that still holds spaces: ``N O G`` -> NOG, ``BIO BIOB`` -> BIO, BIOB, else ambiguous."""
    words = token.split()
    parts = [p for p in words if p not in _EXCHANGE_WORDS and not (len(words) > 1 and p in _MARKET_TAGS)]
    if len(parts) <= 1:
        return parts, None
    if all(len(p) == 1 for p in parts):
        return ["".join(parts)], None
    if _common_prefix_len(parts) >= min(2, min(len(p) for p in parts)):
        return parts, None
    return [], "ambiguous_whitespace"


def parse_ticker_field(raw: Any) -> tuple[tuple[str, ...], Optional[str]]:
    """``issuer_ticker`` as filed -> ``(normalized tickers, reject_reason)``.

    Placeholders (NONE, N/A, NO SYMBOL ...), exchange decorations (``NYSE: KRC``, ``(SIRI)``,
    ``SWWI.PK``) and multiple tickers in one field (``ISCA, ISCB``, ``Z AND ZG``, ``BWINA / B``)
    are handled; anything that cannot be read with confidence is rejected, never guessed.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return (), "blank"
    s = re.sub(r"\s+", " ", str(raw).upper().translate(_BRACKETS)).strip()
    if s in _PLACEHOLDERS:
        return (), "placeholder"
    s = _slash_pairs_to_commas(s)
    shorthand = _CLASS_SHORTHAND.match(s)
    if shorthand:
        first = shorthand.group(1)
        tokens = [first, first[:-1] + shorthand.group(2)] if len(first) > 1 else [first]
    else:
        tokens = [t for t in _FIELD_SPLIT.split(s) if t]
    out: list[str] = []
    reason: Optional[str] = None
    only_placeholders = True
    for tok in tokens:
        tok = tok.replace("*", "").strip().lstrip("$").strip()
        tok = _EXCHANGE_PREFIX.sub("", tok).strip()
        if ":" in tok:  # "DYSL:OB", "OB:KRED", "ASX:CRN": a ticker tagged with its exchange
            sides = [p.strip() for p in tok.split(":") if p.strip()]
            if any(p in _FOREIGN_EXCHANGES for p in sides):
                only_placeholders, reason = False, reason or "foreign_exchange"
                continue
            sides = [p for p in sides if p not in _EXCHANGE_WORDS and p not in _MARKET_TAGS]
            if len(sides) != 1:
                only_placeholders, reason = False, reason or "unparseable"
                continue
            tok = sides[0]
        tok = _EXCHANGE_SUFFIX.sub("", tok)
        if tok in _PLACEHOLDERS:
            continue
        only_placeholders = False
        parts, why = _split_whitespace(tok)
        if why:
            reason = why
            continue
        for part in parts:
            part = _EXCHANGE_SUFFIX.sub("", part)
            norm = normalize_ticker(part)
            if norm is None or norm in _POST_PLACEHOLDERS:
                continue
            if norm.isdigit():  # "000": no US-listed ticker is all digits; this is filer noise
                reason = reason or "numeric"
                continue
            if len(norm) > _MAX_TICKER_LEN or (len(norm) >= 6 and any(c.isdigit() for c in norm)):
                reason = reason or "too_long_or_id_like"  # CUSIPs, run-together tickers, free text
                continue
            if norm not in out:
                out.append(norm)
    if out:
        return tuple(out), None
    if reason:
        return (), reason
    return (), "placeholder" if only_placeholders else "unparseable"


# --- inputs ---------------------------------------------------------------------------------


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_company_tickers(data: Any) -> dict[int, dict[str, Any]]:
    """``{cik: {"name": str|None, "tickers": [normalized...]}}`` from a decoded ``company_tickers.json``."""
    out: dict[int, dict[str, Any]] = {}
    entries = data.values() if isinstance(data, dict) else (data or [])
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            cik = int(str(entry.get("cik_str")).strip())
        except (TypeError, ValueError):
            continue
        if cik <= 0:
            continue
        rec = out.setdefault(cik, {"name": None, "tickers": []})
        title = str(entry.get("title") or "").strip()
        if title and not rec["name"]:
            rec["name"] = title
        for tkr in parse_ticker_field(entry.get("ticker"))[0]:
            if tkr not in rec["tickers"]:
                rec["tickers"].append(tkr)
    return out


def company_tickers_without_cik(data: Any) -> set[str]:
    """Cleaned tickers of ``company_tickers.json`` entries whose CIK is missing or not a positive integer."""
    out: set[str] = set()
    entries = data.values() if isinstance(data, dict) else (data or [])
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            ok = int(str(entry.get("cik_str")).strip()) > 0
        except (TypeError, ValueError):
            ok = False
        if not ok:
            out.update(parse_ticker_field(entry.get("ticker"))[0])
    return out


def merge_company_tickers(parts: Iterable[dict[int, dict[str, Any]]]) -> dict[int, dict[str, Any]]:
    """First file wins the name; tickers are the union (a later snapshot never removes an earlier one)."""
    merged: dict[int, dict[str, Any]] = {}
    for part in parts:
        for cik, rec in part.items():
            cur = merged.setdefault(cik, {"name": None, "tickers": []})
            if rec["name"] and not cur["name"]:
                cur["name"] = rec["name"]
            for t in rec["tickers"]:
                if t not in cur["tickers"]:
                    cur["tickers"].append(t)
    return merged


def load_company_tickers(paths: list[Path]) -> dict[int, dict[str, Any]]:
    return merge_company_tickers(parse_company_tickers(json.loads(Path(p).read_text(encoding="utf-8"))) for p in paths)


def load_submissions(path: Path) -> pd.DataFrame:
    import pyarrow.parquet as pq

    return pq.read_table(path, columns=["accession_number", "filing_date", "issuer_cik", "issuer_ticker"]).to_pandas()


def load_latest_names(path: Path) -> dict[int, str]:
    """Latest ``issuer_name`` per issuer CIK in the Form 4 file (latest filing date, ties by name)."""
    import pyarrow.parquet as pq

    df = pq.read_table(path, columns=["issuer_cik", "issuer_name", "filing_date"]).to_pandas()
    df = df[df["issuer_name"].fillna("").str.strip() != ""]
    df["cik"] = pd.to_numeric(df["issuer_cik"], errors="coerce")
    df = df.dropna(subset=["cik"]).drop_duplicates(["cik", "filing_date", "issuer_name"])
    df = df.sort_values(["cik", "filing_date", "issuer_name"], kind="mergesort").drop_duplicates("cik", keep="last")
    return {int(c): str(n).strip() for c, n in zip(df["cik"], df["issuer_name"])}


def load_sic_map(path: Path) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("http_status", 200) != 200 or not rec.get("cik"):
                continue
            sic = rec.get("sic")
            out[int(rec["cik"])] = {
                "sic": int(sic) if sic not in (None, "", 0, "0") else None,
                "name": (rec.get("name") or "").strip() or None,
                "fetched_at": rec.get("fetched_at"),
            }
    return out


# --- ticker history -------------------------------------------------------------------------


def _prev_day(iso: str) -> str:
    return (date.fromisoformat(iso) - timedelta(days=1)).isoformat()


def _same_stem_other_class(a: str, b: str) -> bool:
    """``GOOG``/``GOOGL``, ``BIO``/``BIOB``, ``BRKA``/``BRKB``: one stem of 3+ characters plus a class letter.

    A digit suffix (``SEQ`` -> ``SEQ2``) or a different stem (``OLD`` -> ``NEW``) is a new ticker, not a class.
    """
    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    if len(long_) == len(short) + 1:
        return len(short) >= 3 and long_.startswith(short) and long_[-1].isalpha()
    if len(long_) == len(short):
        return len(short) >= 4 and short[:-1] == long_[:-1] and short[-1].isalpha() and long_[-1].isalpha()
    return False


def ticker_families(tickers: list[str], co_named: Iterable[tuple[str, ...]]) -> list[list[str]]:
    """Group one issuer's tickers into share-class families.

    Dual-class issuers file under either class (GOOG / GOOGL, ISCA / ISCB), so one class not appearing in
    the latest filing does not mean it stopped trading. Two tickers are in one family when a single filing
    field named both (``ISCA, ISCB``), or when they differ only by a final class letter on a 3+ character
    stem (``_same_stem_other_class``: GOOG/GOOGL, BRKA/BRKB). A family's window ends only when a later filing names a ticker outside the family.
    """
    parent = {t: t for t in tickers}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    for names in co_named:
        for other in names[1:]:
            if names[0] in parent and other in parent:
                union(names[0], other)
    for i, a in enumerate(tickers):
        for b in tickers[i + 1:]:
            if _same_stem_other_class(a, b):
                union(a, b)
    groups: dict[str, list[str]] = defaultdict(list)
    for t in tickers:
        groups[find(t)].append(t)
    return [sorted(g) for _, g in sorted(groups.items())]


def derive_ticker_history(submissions: pd.DataFrame) -> dict[str, Any]:
    """Per-issuer dated ticker windows from filings.

    Returns ``{"issuers": {cik: {"first": iso, "last": iso, "filings": n}},
    "windows": [{cik, ticker, valid_from, valid_to, last_seen}], "noise": {...}}``.
    """
    sub = submissions[["accession_number", "filing_date", "issuer_cik", "issuer_ticker"]].copy()
    sub["cik"] = pd.to_numeric(sub["issuer_cik"], errors="coerce")
    sub["fdate"] = pd.to_datetime(sub["filing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    stats: Counter = Counter()
    stats["rows"] = int(len(sub))
    bad_cik = sub["cik"].isna() | (sub["cik"] <= 0)
    stats["rows_bad_cik"] = int(bad_cik.sum())
    no_cik_tickers = parse_many(sub.loc[bad_cik, "issuer_ticker"])
    bad_date = sub["fdate"].isna()
    stats["rows_bad_filing_date"] = int(bad_date.sum())
    sub = sub[~bad_cik & ~bad_date]
    sub = sub.assign(cik=sub["cik"].astype("int64"))

    # Distinct (issuer, date, raw ticker): everything below is per distinct triple, never per row.
    triples = sub.groupby(["cik", "fdate", sub["issuer_ticker"].fillna("")], sort=True).size().reset_index(name="n")
    triples = triples.rename(columns={"issuer_ticker": "raw"})
    raw_map = {raw: parse_ticker_field(raw) for raw in triples["raw"].unique()}

    issuers: dict[int, dict[str, Any]] = {}
    for cik, grp in sub.groupby("cik", sort=True):
        issuers[int(cik)] = {"first": grp["fdate"].min(), "last": grp["fdate"].max(), "filings": int(grp["accession_number"].nunique())}

    rejected: Counter = Counter()
    multi_fields = 0
    seen: dict[tuple[int, str], list[str]] = {}
    bearing: dict[int, set[str]] = defaultdict(set)
    co_named: dict[int, list[tuple[str, ...]]] = defaultdict(list)
    for cik, fdate, raw, n in zip(triples["cik"], triples["fdate"], triples["raw"], triples["n"]):
        tickers, why = raw_map[raw]
        if not tickers:
            rejected[(raw, why)] += int(n)
            stats[f"rows_ticker_rejected_{why}"] += int(n)
            continue
        if len(tickers) > 1:
            multi_fields += int(n)
            co_named[int(cik)].append(tickers)
        bearing[int(cik)].add(fdate)
        for t in tickers:
            span = seen.get((int(cik), t))
            if span is None:
                seen[(int(cik), t)] = [fdate, fdate]
            else:
                span[0] = min(span[0], fdate)
                span[1] = max(span[1], fdate)
    stats["rows_multi_ticker_field"] = multi_fields

    windows: list[dict[str, Any]] = []
    bearing_sorted = {cik: sorted(ds) for cik, ds in bearing.items()}
    by_issuer: dict[int, list[str]] = defaultdict(list)
    for cik, tkr in seen:
        by_issuer[cik].append(tkr)
    sibling_groups = 0
    for cik, tkrs in sorted(by_issuer.items()):
        ds = bearing_sorted[cik]
        for group in ticker_families(sorted(tkrs), co_named.get(cik, [])):
            sibling_groups += len(group) > 1
            group_last = max(seen[(cik, t)][1] for t in group)
            idx = bisect_right(ds, group_last)
            valid_to = _prev_day(ds[idx]) if idx < len(ds) else None
            for t in group:
                first, last = seen[(cik, t)]
                windows.append({"cik": cik, "ticker": t, "valid_from": first, "valid_to": valid_to, "last_seen": last})
    stats["issuer_ticker_families_with_siblings"] = sibling_groups
    noise = {
        "stats": dict(sorted(stats.items())),
        "rejected": [{"raw": raw, "reason": why, "rows": n} for (raw, why), n in sorted(rejected.items(), key=lambda kv: (-kv[1], kv[0][0]))],
        "tickers_without_cik": {"rows": int(bad_cik.sum()), "distinct_tickers": len(no_cik_tickers), "tickers": sorted(no_cik_tickers)[:200]},
    }
    return {"issuers": issuers, "windows": windows, "noise": noise}


def parse_many(values: pd.Series) -> set[str]:
    out: set[str] = set()
    for raw in values.dropna().unique():
        out.update(parse_ticker_field(raw)[0])
    return out


# --- conflicts ------------------------------------------------------------------------------


def _overlap(a: dict[str, Any], b: dict[str, Any]) -> Optional[tuple[str, Optional[str]]]:
    start = max(a["valid_from"], b["valid_from"])
    ends = [e for e in (a["valid_to"], b["valid_to"]) if e is not None]
    end = min(ends) if ends else None
    if end is not None and start > end:
        return None
    return start, end


def apply_handoff_clamp(rows: list[dict[str, Any]]) -> int:
    """Close an open window whose issuer went silent before another CIK took the ticker.

    Evidence rule: if CIK A's window for T is still open but A's last filing naming T is earlier than
    CIK B's first filing naming T, A stopped using T before B started, so A's window ends the day
    before B's starts. Windows that are still contested (A filed T after B began) stay open and are
    flagged by ``flag_conflicts``. Returns the number of windows closed.
    """
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_ticker[r["id_value"]].append(r)
    closed = 0
    for group in by_ticker.values():
        if len({r["entity_id"] for r in group}) < 2:
            continue
        for a in group:
            if a["valid_to"] is not None or a["_last_seen"] is None:
                continue
            later = [b["valid_from"] for b in group if b["entity_id"] != a["entity_id"] and b["valid_from"] > a["_last_seen"]]
            if later:
                a["valid_to"] = _prev_day(min(later))
                a["conflict_detail"] = {"kind": "ticker_reuse_handoff", "closed_by_first_filing_of_other_cik": min(later)}
                closed += 1
    return closed


def flag_conflicts(rows: list[dict[str, Any]], max_listed: int = 25) -> list[dict[str, Any]]:
    """Flag every ticker row that overlaps another CIK's row for the same ticker; return the conflict groups."""
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_ticker[r["id_value"]].append(r)
    groups: list[dict[str, Any]] = []
    for ticker in sorted(by_ticker):
        grp = sorted(by_ticker[ticker], key=lambda r: (r["valid_from"], r["entity_id"]))
        if len({r["entity_id"] for r in grp}) < 2:
            continue
        others: dict[int, set[str]] = defaultdict(set)
        pairs: list[dict[str, Any]] = []
        for i, a in enumerate(grp):
            for j in range(i + 1, len(grp)):
                b = grp[j]
                if a["entity_id"] == b["entity_id"]:
                    continue
                ov = _overlap(a, b)
                if ov is None:
                    continue
                others[i].add(b["entity_id"])
                others[j].add(a["entity_id"])
                pairs.append({"a": a["entity_id"], "b": b["entity_id"], "overlap_from": ov[0], "overlap_to": ov[1]})
        if not pairs:
            continue
        for i, r in enumerate(grp):
            if i in others:
                r["conflict_flag"] = True
                r["is_primary"] = False
                r["conflict_detail"] = {
                    "kind": "overlapping_ticker_claim",
                    "other_entities": sorted(others[i])[:max_listed],
                    "n_other_entities": len(others[i]),
                }
        groups.append({
            "ticker": ticker,
            "entities": sorted({e for p in pairs for e in (p["a"], p["b"])}),
            "n_pairs": len(pairs),
            "pairs": pairs[:max_listed],
        })
    return groups


# --- assembly -------------------------------------------------------------------------------


def build_rows(
    submissions: pd.DataFrame,
    company_tickers: dict[int, dict[str, Any]],
    *,
    tickers_as_of: str,
    nonderiv_names: Optional[dict[int, str]] = None,
    sic_map: Optional[dict[int, dict[str, Any]]] = None,
    handoff_clamp: bool = True,
    company_tickers_no_cik: Optional[Iterable[str]] = None,
) -> dict[str, Any]:
    """Pure: frames and dicts in, proposed rows and the report out."""
    date.fromisoformat(tickers_as_of)  # validates the snapshot day
    nonderiv_names = nonderiv_names or {}
    sic_map = sic_map or {}
    hist = derive_ticker_history(submissions)
    issuers: dict[int, dict[str, Any]] = hist["issuers"]
    ciks = sorted(set(issuers) | set(company_tickers))

    name_source: Counter = Counter()
    sic_counts: Counter = Counter()
    sm_rows: list[dict[str, Any]] = []
    id_rows: list[dict[str, Any]] = []
    for cik in ciks:
        ct = company_tickers.get(cik)
        info = issuers.get(cik)
        if ct and ct["name"]:
            name, nsrc = ct["name"], "sec_company_tickers"
        elif nonderiv_names.get(cik):
            name, nsrc = nonderiv_names[cik], "form4_latest_issuer_name"
        elif sic_map.get(cik, {}).get("name"):
            name, nsrc = sic_map[cik]["name"], "sec_submissions_name"
        else:
            name, nsrc = f"CIK {cik}", "placeholder_no_name"
        name_source[nsrc] += 1
        sic_rec = sic_map.get(cik)
        sic = sic_rec["sic"] if sic_rec else None
        sic_counts["with_sic" if sic else "without_sic"] += 1
        delisting = evaluate_delisting_candidate(has_live_cik=ct is not None)
        entity_id = entity_id_for_cik(cik)
        provenance: dict[str, Any] = {
            "builder": BUILDER_VERSION,
            "name_source": nsrc,
            "in_company_tickers": ct is not None,
            "form345_first_filing": info["first"] if info else None,
            "form345_last_filing": info["last"] if info else None,
            "form345_filings": info["filings"] if info else 0,
        }
        if sic is not None:
            provenance["sic_source"] = "sec_submissions_current_not_point_in_time"
        sm_rows.append({
            "entity_id": entity_id,
            "cik": cik,
            "name": name,
            "security_type": "equity",
            "is_active": delisting.is_active,
            "delisted_at": None,
            "delisted_reason": delisting.delisted_reason,
            "delisted_basis": delisting.delisted_basis,
            "sic": sic,
            "source": ENTITY_SOURCE,
            "provenance": provenance,
        })
        id_rows.append(_identifier(entity_id, ID_SCHEME_CIK, str(cik), info["first"] if info else tickers_as_of,
                                   None, True, SOURCE_FILINGS if info else SOURCE_COMPANY_TICKERS))

    ticker_rows: list[dict[str, Any]] = []
    open_by_entity: dict[tuple[int, str], bool] = {}
    for w in hist["windows"]:
        row = _identifier(entity_id_for_cik(w["cik"]), ID_SCHEME_TICKER, w["ticker"], w["valid_from"], w["valid_to"], True, SOURCE_FILINGS)
        row["_last_seen"] = w["last_seen"]
        ticker_rows.append(row)
        open_by_entity[(w["cik"], w["ticker"])] = open_by_entity.get((w["cik"], w["ticker"]), False) or w["valid_to"] is None
    added_from_company_tickers = 0
    for cik, ct in sorted(company_tickers.items()):
        for tkr in ct["tickers"]:
            if open_by_entity.get((cik, tkr)):
                continue  # a filing already names it and the window is open: the filings win
            row = _identifier(entity_id_for_cik(cik), ID_SCHEME_TICKER, tkr, tickers_as_of, None, True, SOURCE_COMPANY_TICKERS)
            row["_last_seen"] = None
            ticker_rows.append(row)
            added_from_company_tickers += 1

    clamped = apply_handoff_clamp(ticker_rows) if handoff_clamp else 0
    conflicts = flag_conflicts(ticker_rows)
    for r in ticker_rows:
        r.pop("_last_seen", None)
    id_rows.extend(ticker_rows)

    sm_rows.sort(key=lambda r: r["entity_id"])
    id_rows.sort(key=lambda r: (r["entity_id"], r["id_scheme"], r["id_value"], r["valid_from"]))
    flagged = [r for r in id_rows if r["conflict_flag"]]
    report = {
        "entities": len(sm_rows),
        "entities_in_filings": len(issuers),
        "entities_company_tickers_only": len(set(company_tickers) - set(issuers)),
        "identifiers": len(id_rows),
        "cik_identifiers": sum(1 for r in id_rows if r["id_scheme"] == ID_SCHEME_CIK),
        "ticker_identifiers": len(ticker_rows),
        "ticker_identifiers_open": sum(1 for r in ticker_rows if r["valid_to"] is None),
        "distinct_tickers": len({r["id_value"] for r in ticker_rows}),
        "ticker_identifiers_from_company_tickers": added_from_company_tickers,
        "conflict_tickers": len(conflicts),
        "conflict_identifier_rows": len(flagged),
        "conflict_entities": len({r["entity_id"] for r in flagged}),
        "handoff_clamp": handoff_clamp,
        "handoff_windows_closed": clamped,
        "name_source": dict(sorted(name_source.items())),
        "sic": dict(sorted(sic_counts.items())),
        "is_active_candidates_sec_absence_only": sum(1 for r in sm_rows if r["delisted_basis"]),
        "tickers_without_cik": {
            "form345_rows_with_a_ticker_but_no_valid_cik": hist["noise"]["tickers_without_cik"]["rows"],
            "form345_distinct_tickers": hist["noise"]["tickers_without_cik"]["distinct_tickers"],
            "company_tickers_distinct_tickers": len(set(company_tickers_no_cik or ())),
            "examples": sorted(set(hist["noise"]["tickers_without_cik"]["tickers"]) | set(company_tickers_no_cik or ()))[:50],
        },
        "noise_stats": hist["noise"]["stats"],
    }
    return {"security_master": sm_rows, "security_identifiers": id_rows, "conflicts": conflicts, "noise": hist["noise"], "report": report}


def _identifier(entity_id: str, scheme: str, value: str, valid_from: str, valid_to: Optional[str],
                is_primary: bool, source: str) -> dict[str, Any]:
    return {
        "entity_id": entity_id, "id_scheme": scheme, "id_value": value, "valid_from": valid_from,
        "valid_to": valid_to, "is_primary": is_primary, "source": source, "conflict_flag": False,
        "conflict_detail": None,
    }


# --- artifact -------------------------------------------------------------------------------


def seed_lines(built: dict[str, Any]) -> Iterable[str]:
    for r in built["security_master"]:
        yield json.dumps({"t": "sm", **r}, sort_keys=True, separators=(",", ":"))
    for r in built["security_identifiers"]:
        yield json.dumps({"t": "si", **r}, sort_keys=True, separators=(",", ":"))


def _code_sha(explicit: Optional[str]) -> dict[str, Any]:
    out: dict[str, Any] = {"builder_file_sha256": sha256_file(Path(__file__).resolve()), "git_head": explicit}
    if explicit is None:
        try:
            # Fixed argv, no shell, no user input: only records the commit this code was built from.
            res = subprocess.run(  # nosec B603 B607
                ["git", "rev-parse", "HEAD"], cwd=_PROJECT_ROOT, capture_output=True, text=True, timeout=10, check=False)
            out["git_head"] = res.stdout.strip() or None
        except (OSError, subprocess.SubprocessError):
            out["git_head"] = None
    return out


def write_artifact(built: dict[str, Any], out_dir: Path, *, inputs: list[dict[str, Any]], params: dict[str, Any],
                   code_sha: Optional[str] = None) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    seed_path = out_dir / SEED_FILE
    for name in (SEED_FILE, "receipt.json", "conflicts.json", "ticker_noise.json"):
        if (out_dir / name).exists():
            raise FileExistsError(f"refusing to overwrite {out_dir / name}")
    h = hashlib.sha256()
    n_lines = 0
    with open(seed_path, "w", encoding="utf-8", newline="\n") as fh:
        for line in seed_lines(built):
            data = (line + "\n")
            fh.write(data)
            h.update(data.encode("utf-8"))
            n_lines += 1
    (out_dir / "conflicts.json").write_text(json.dumps(built["conflicts"], indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / "ticker_noise.json").write_text(json.dumps(built["noise"], indent=1, sort_keys=True) + "\n", encoding="utf-8")
    receipt = {
        "builder": "scripts/build_security_master_all_issuers.py",
        "builder_version": BUILDER_VERSION,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "code": _code_sha(code_sha),
        "params": params,
        "inputs": inputs,
        "output": {"name": SEED_FILE, "lines": n_lines, "sha256": h.hexdigest(), "bytes": seed_path.stat().st_size},
        "counts": built["report"],
        "writes_to_database": False,
    }
    (out_dir / "receipt.json").write_text(json.dumps(receipt, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--company-tickers", type=Path, action="append", required=True)
    ap.add_argument("--submissions", type=Path, required=True)
    ap.add_argument("--nonderiv", type=Path, help="Form 4 nonderiv_transactions.parquet (fallback name source)")
    ap.add_argument("--sic-map", type=Path, help="issuer_sic_map.jsonl (current SIC, not point-in-time)")
    ap.add_argument("--tickers-as-of", required=True, help="YYYY-MM-DD snapshot day of company_tickers.json")
    ap.add_argument("--no-handoff-clamp", action="store_true",
                    help="keep a silent issuer's last ticker open forever (the literal reading of the window rule); "
                         "by default its window is closed the day before another CIK first names the ticker")
    ap.add_argument("--code-sha", help="git commit of this code when it is not run from a git checkout")
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args(argv)

    inputs = []
    for label, path in ([("company_tickers", p) for p in args.company_tickers]
                        + [("submissions", args.submissions)]
                        + ([("nonderiv", args.nonderiv)] if args.nonderiv else [])
                        + ([("sic_map", args.sic_map)] if args.sic_map else [])):
        inputs.append({"label": label, "path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    built = build_rows(
        load_submissions(args.submissions),
        load_company_tickers(args.company_tickers),
        tickers_as_of=args.tickers_as_of,
        nonderiv_names=load_latest_names(args.nonderiv) if args.nonderiv else None,
        sic_map=load_sic_map(args.sic_map) if args.sic_map else None,
        handoff_clamp=not args.no_handoff_clamp,
        company_tickers_no_cik=set().union(*(company_tickers_without_cik(json.loads(Path(p).read_text(encoding="utf-8")))
                                             for p in args.company_tickers)),
    )
    receipt = write_artifact(
        built, args.out_dir, inputs=inputs, code_sha=args.code_sha,
        params={"tickers_as_of": args.tickers_as_of, "handoff_clamp": not args.no_handoff_clamp},
    )
    print(json.dumps({"out_dir": str(args.out_dir), "output_sha256": receipt["output"]["sha256"], **receipt["counts"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
