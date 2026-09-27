"""VS1 v2 panel harness: SIC-expanded Technology insider open-market-buy density (research only).

Pre-registration: ``docs/paper_log/vs1-insider-density-v2-preregistration.md``.
Its body sha256 is pinned in :data:`PREREG_BODY_SHA256`; every stage refuses to
run when the repository copy no longer hashes to it.

Relation to v1 (``analysis.panel_insider_density``)
---------------------------------------------------
v1 failed its Stage-0 power gate (2026-09-27: primary power 0.02 vs 0.50 on 84
sector-map tickers) before any price was read, and the owner chose v1 §10
option (a): a new pre-registration with an SIC-expanded Technology universe.
v1's module, pre-registration, registry and witness are left untouched and
still pinned; this module is a separate pinned registry (its own chain, its
own anchor file, its own off-host witness path) and imports only v1's
version-free building blocks:

* the Form 4 event rules (:func:`analysis.panel_insider_density.build_events`),
  the feature (:func:`density`), known_at, decision instants, horizon-spaced
  split-first labels, the rank-IC statistic, the block sign-flip null, the
  sensitivity nulls, the magnitude, missing-label and momentum reports, the
  planted-power simulation and the Stage-0 settings -- all unchanged (v2 §2-§10);
* the price reader contract (``store.observations.read_window`` with an explicit
  source, admitted manifest, frozen ``as_of_ts``).

What v2 changes (each item is a v2 pre-registration section):

* universe: sector-map Technology members (v1 rules) **plus** every issuer whose
  current SEC SIC code is in 3570-3579, 3660-3679 or 7370-7379, both restricted
  to CIKs with a current ticker in the pinned ``company_tickers.json``
  (:func:`v2_universe`);
* admission at each decision: at least two Form 4 accessions in the trailing
  730 days (Form 4 history) and the ticker rule -- the issuer's own latest
  Section 16 filing must name one of its current tickers (review item 5 of
  #697, candidate amendment C1) (:func:`build_admission`);
* reported only: delisting-return bounds (C2), an SIC-group-neutral IC;
* the calibration alarm (C3): CONTRARY only from Holm-adjusted negative
  one-sided p-values, and the "absent while powered" alarm replaced by evidence
  against the expected sign; v1's rule outcome is reported alongside;
* the registry refuses to open a v2 discovery once v1 has opened one (v1's
  off-host witness must still cover only its 2 registration records).

Boundaries are v1's: no DB writes, no migrations, no timers; prices only through
``store.observations.read_window`` behind a registry key; nothing here is a
trading signal (``promotion_allowed`` is false everywhere).
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from analysis import panel_insider_density as v1
from analysis.offline_research_proof import (
    MIN_BLOCKS,
    corrected_p,
    digest,
    holm_adjusted,
    stamp,
    write_once,
)
from store import observations

REPO = Path(__file__).resolve().parent.parent

# --- pinned pre-registration and inputs ----------------------------------------------

PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v2-preregistration.md")
BODY_START, BODY_END = v1.BODY_START, v1.BODY_END
# sha256 of the LF bytes strictly between the two markers. Changing the body is
# a new pre-registration (v3): re-pin only before any VS1 price is read.
PREREG_BODY_SHA256 = "159f69da278cd23a3f55e8249e795c59a23235b9d5b1aacacce118de067e2d2b"
VERSION = "vs1-v2"
ORIGIN = "form345_panel_research"

#: SEC ``company_tickers.json`` (ticker -> CIK), the file VS1 v1's Stage-0 used,
#: fetched 2026-09-27T21:05:28Z; the universe is computed from exactly this file.
ISSUER_MAP_SHA256 = "016ae8ffe06c0f8f8bed5aff9af1bb69ae12b197a3441851c712f88a5d7f64f1"
#: ``issuer_sic_map.jsonl`` built by ``scripts/fetch_sec_issuer_sic.py`` (current SIC per CIK).
#: Built on grid-svr at /data/sec/vs2/issuer_sic_map.jsonl from 20,760 submissions JSON bodies
#: fetched 2026-09-27T21:38Z..22:33Z (fetch log sha256 057a7b2f...).
SIC_MAP_SHA256 = "4200acd05c9fbf563dd681acfedefd8ccd3a3ff46330fe742edf7159dd883f08"
#: Pre-registered SIC ranges (inclusive). 3672 (printed circuit boards) lies inside 3660-3679.
SIC_RANGES: tuple[tuple[int, int], ...] = ((3570, 3579), (3660, 3679), (7370, 7379))
SIC_GROUPS: tuple[str, ...] = ("3570-3579", "3660-3679", "7370-7379", "other")

#: Admission (v2 §2.2): Form 4 accessions of the issuer known in (t - 730 d, t].
FORM4_HISTORY_DAYS = 730
FORM4_HISTORY_MIN = 2
FORM4_TYPES = frozenset({"4", "4/A"})
#: Symbols that name no ticker (ignored by the ticker rule).
NULL_SYMBOLS = frozenset({"", "NONE", "NA", "NAN", "NULL", "N", "NOTICKER", "NOSYMBOL", "NOTAPPLICABLE",
                          "NOTLISTED", "PRIVATE", "NOTPUBLIC", "UNLISTED", "TBD", "0"})

#: C2 reported-only delisting bounds.
DELISTING_BOUNDS: tuple[str, ...] = ("pessimistic", "neutral", "optimistic")
#: C3: the "evidence against the expected sign" alarm on the primary trial.
PRIMARY_AGAINST_P = 0.05

#: Frozen inputs (v1's plus the SIC map).
FROZEN_INPUT_KEYS: tuple[str, ...] = (*v1.FROZEN_INPUT_KEYS, "sic_map_sha256")
OBSERVED_INPUT_KEYS: tuple[str, ...] = tuple(
    k for k in FROZEN_INPUT_KEYS if k not in ("as_of_ts", "accept_underpowered")
)


def prereg_body_sha256(path: Path) -> str:
    return v1.prereg_body_sha256(path)


def check_prereg(repo_root: Path = REPO) -> str:
    """The repository v2 pre-registration must still hash to the pinned body hash."""
    actual = prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != PREREG_BODY_SHA256:
        raise ValueError(
            f"v2 pre-registration body hashes to {actual[:12]}, pinned {PREREG_BODY_SHA256[:12]}: "
            "the spec changed after registration (a change is a new version)"
        )
    return actual


# --- universe ---------------------------------------------------------------------------


def sic_in_ranges(sic: Any) -> bool:
    try:
        value = int(sic)
    except (TypeError, ValueError):
        return False
    return any(lo <= value <= hi for lo, hi in SIC_RANGES)


def sic_group(sic: Any) -> str:
    try:
        value = int(sic)
    except (TypeError, ValueError):
        return "other"
    for lo, hi in SIC_RANGES:
        if lo <= value <= hi:
            return f"{lo}-{hi}"
    return "other"


_PINNED = "pinned"


def load_sic_map(path: Path, *, pinned: str | None = _PINNED) -> pd.DataFrame:
    """``issuer_sic_map.jsonl`` as a frame (cik, sic, name, tickers, former_names, http_status).

    Refused when the file does not hash to ``pinned`` (default: the
    pre-registered :data:`SIC_MAP_SHA256`); tests pass ``pinned=None``.
    """
    path = Path(path)
    pinned = SIC_MAP_SHA256 if pinned == _PINNED else pinned
    if pinned is not None and v1.data_sha256(path) != pinned:
        raise ValueError("SIC map differs from the pre-registered issuer_sic_map.jsonl")
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    frame = pd.DataFrame(rows, columns=["cik", "sic", "name", "tickers", "former_names", "http_status"])
    frame["cik"] = frame["cik"].astype("int64")
    if frame["cik"].duplicated().any():
        raise ValueError("SIC map has duplicate CIKs")
    return frame


def load_issuer_map(path: Path, *, pinned: str | None = _PINNED) -> pd.DataFrame:
    """SEC ``company_tickers.json`` (refused unless it is the pinned file; tests pass ``pinned=None``)."""
    pinned = ISSUER_MAP_SHA256 if pinned == _PINNED else pinned
    if pinned is not None and v1.data_sha256(Path(path)) != pinned:
        raise ValueError("issuer map differs from the pre-registered company_tickers.json")
    return v1.load_issuer_map(Path(path))


def canonical_symbol(value: str) -> str:
    """Upper-case alphanumerics only: ``BRK-B``, ``BRK.B`` and ``BRK/B`` all become ``BRKB``."""
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def filed_symbols(raw: Any) -> frozenset[str]:
    """Canonical ticker candidates named by an ``ISSUERTRADINGSYMBOL`` as filed.

    Handles lists (``ISCA, ISCB``, ``MOGA/MOGB``, ``Z AND ZG``), exchange prefixes
    (``NYSE: KRC``, ``(NASDAQ:FBC)``) and quoting; the whole string is a candidate
    too (``BRK/B``). Null tokens (``NONE``, ``N/A``) and all-digit strings name no
    ticker and yield the empty set.
    """
    if raw is None or (isinstance(raw, float) and np.isnan(raw)) or raw is pd.NA:
        return frozenset()
    text = re.sub(r"[\"'()\[\]{}]", " ", str(raw).upper())
    if canonical_symbol(text) in NULL_SYMBOLS:  # N/A, N.A., [NONE]
        return frozenset()
    parts = [p.split(":")[-1] for p in re.split(r"[,;&/]|\bAND\b|\s+", text)]
    whole = text.split(":")[-1]
    out = set()
    for candidate in [*parts, whole]:
        key = canonical_symbol(candidate)
        if key and key not in NULL_SYMBOLS and not key.isdigit() and len(key) <= 10:
            out.add(key)
    return frozenset(out)


def _price_ticker(tickers: Iterable[str]) -> str:
    """The representative current ticker: plain letters first, then shortest, then alphabetical."""
    return sorted(set(tickers), key=lambda t: (not t.isalpha(), len(t), t))[0]


def v2_universe(
    sector_map: Mapping[str, Any], issuer_map: pd.DataFrame, sic_map: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """VS1 v2 universe: one row per issuer CIK.

    Members are (a) the v1 sector-map Technology members that resolve to a CIK
    (v1 §2.2 rules 1-2, ticker = the member ticker, alphabetically first on a
    share-class tie) and (b) every CIK with a current ticker in the issuer map
    whose current SIC is in :data:`SIC_RANGES`. Columns: ``ticker`` (the price
    ticker), ``cik``, ``source`` (``sector_map``, ``sic`` or ``both``), ``sic``,
    ``sic_group`` and ``current_tickers`` (every current ticker of the CIK,
    canonical, for the ticker rule).
    """
    members, info = v1.sector_universe(v1.VS1_SECTOR, sector_map, issuer_map)
    by_cik = issuer_map.groupby("cik")["ticker"].apply(lambda s: sorted(set(s)))
    sic = sic_map.set_index("cik")["sic"]
    sic_ciks = sorted(int(c) for c, value in sic.items() if sic_in_ranges(value) and int(c) in by_cik.index)
    rows = {}
    for row in members.itertuples(index=False):
        rows[int(row.cik)] = {"ticker": row.ticker, "cik": int(row.cik), "source": "sector_map"}
    for cik in sic_ciks:
        if cik in rows:
            rows[cik]["source"] = "both"
        else:
            rows[cik] = {"ticker": _price_ticker(by_cik[cik]), "cik": cik, "source": "sic"}
    for cik, row in rows.items():
        value = sic.get(cik)
        row["sic"] = int(value) if value is not None and not pd.isna(value) else None
        row["sic_group"] = sic_group(row["sic"])
        row["current_tickers"] = sorted({canonical_symbol(t) for t in by_cik.get(cik, [])})
    frame = pd.DataFrame(sorted(rows.values(), key=lambda r: (r["ticker"], r["cik"])),
                         columns=["ticker", "cik", "source", "sic", "sic_group", "current_tickers"], dtype=object)
    frame["cik"] = frame["cik"].astype("int64")
    shared = frame["ticker"].duplicated(keep="first")  # rows are sorted by (ticker, cik): lowest CIK kept
    dropped = frame[shared]
    frame = frame[~shared].reset_index(drop=True)  # one price series, one issuer
    sic_known = sic_map[sic_map["sic"].notna()]
    in_range = sic_known[sic_known["sic"].map(sic_in_ranges)]
    info = {
        **info,
        "rule": "v1 sector-map Technology members with a CIK, plus CIKs with a current ticker whose current "
                "SEC SIC is in 3570-3579, 3660-3679 or 7370-7379",
        "sic_ranges": [list(r) for r in SIC_RANGES],
        "sector_map_members_with_cik": int(len(members)),
        "sic_map_ciks": int(len(sic_map)),
        "sic_map_ciks_in_ranges": int(len(in_range)),
        "sic_in_ranges_without_current_ticker": int((~in_range["cik"].isin(by_cik.index)).sum()),
        "price_ticker_shared_dropped": dropped[["ticker", "cik"]].to_dict("records"),
        "universe": int(len(frame)),
        "by_source": {k: int(v) for k, v in frame["source"].value_counts().sort_index().items()},
        "by_sic_group": {k: int(v) for k, v in frame["sic_group"].value_counts().sort_index().items()},
    }
    return frame, info


# --- admission (Form 4 history and the ticker rule), from filings only --------------------

SUBMISSION_V2_COLUMNS = ("accession_number", "filing_date", "issuer_cik", "document_type", "issuer_ticker")


def read_submissions(path: Path, issuers: Iterable[int] | None = None) -> pd.DataFrame:
    """The derived SUBMISSION table with the columns v2 needs (every value as text)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(path)
    if ".parquet" in [s.lower() for s in path.suffixes]:
        names = pq.ParquetFile(path).schema_arrow.names
        wanted = [n for n in names if n.lower() in SUBMISSION_V2_COLUMNS]
        table = pq.read_table(path, columns=wanted)
        if issuers is not None:
            ciks = pd.to_numeric(table.column("issuer_cik").to_pandas().astype("string").str.strip(), errors="coerce")
            table = table.filter(pa.array(ciks.isin({int(i) for i in issuers}).to_numpy()))
        frame = table.to_pandas().astype("string")
    else:
        frame = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    frame.columns = [c.strip().upper() for c in frame.columns]
    return frame


@dataclass(frozen=True)
class Admission:
    """Point-in-time admission inputs built from the SUBMISSION table (no price, no outcome)."""

    form4: pd.DataFrame  # issuer_cik, known_at: one row per Form 4 accession
    tickers: pd.DataFrame  # issuer_cik, known_at, match: one row per (issuer, filing instant) naming a symbol
    receipt: dict

    @property
    def receipt_sha256(self) -> str:
        return digest(self.receipt)


def build_admission(submissions: pd.DataFrame, universe: pd.DataFrame) -> Admission:
    """Form 4 history and ticker-rule inputs for the universe's issuers.

    Ticker rule (v2 §2.2 rule 4): at decision ``t`` the issuer's latest filing
    instant (known_at <= t) whose accessions name any ticker must name one of
    the issuer's current tickers; accessions naming no ticker (``NONE``, blank)
    are ignored. Several accessions at one filing instant pass if any of them
    names a current ticker.
    """
    frame = submissions.rename(columns=lambda c: str(c).strip().upper())
    need = [c.upper() for c in SUBMISSION_V2_COLUMNS]
    missing = [c for c in need if c not in frame.columns]
    if missing:
        raise ValueError(f"SUBMISSION table lacks {missing} (v2 needs ISSUERTRADINGSYMBOL as issuer_ticker)")
    wanted = {int(c) for c in universe["cik"]}
    cik = pd.to_numeric(frame["ISSUER_CIK"].astype("string").str.strip(), errors="coerce")
    frame = frame[cik.isin(wanted).to_numpy()].copy()
    frame["cik"] = pd.to_numeric(frame["ISSUER_CIK"].astype("string").str.strip(), errors="coerce").astype("int64")
    frame["filed"] = v1.parse_dates(frame["FILING_DATE"])
    frame["accession"] = frame["ACCESSION_NUMBER"].astype("string").str.strip()
    frame = frame[frame["filed"].notna() & frame["accession"].notna()]
    frame = frame.drop_duplicates("accession")
    frame["known_at"] = v1.filing_known_at(frame["filed"])
    kind = frame["DOCUMENT_TYPE"].astype("string").str.strip().str.upper()
    form4 = frame[kind.isin(FORM4_TYPES).to_numpy()][["cik", "known_at"]].rename(columns={"cik": "issuer_cik"})
    current = {int(r.cik): set(r.current_tickers) for r in universe.itertuples(index=False)}
    symbols = frame["ISSUER_TICKER"].map(filed_symbols)
    named = frame[symbols.map(bool).to_numpy()].assign(symbols=symbols[symbols.map(bool)])
    named["match"] = [bool(s & current.get(c, set())) for s, c in zip(named["symbols"], named["cik"])]
    tickers = (
        named.groupby(["cik", "known_at"], as_index=False)["match"].any()
        .rename(columns={"cik": "issuer_cik"}).sort_values(["issuer_cik", "known_at"]).reset_index(drop=True)
    )
    receipt = {
        "version": VERSION,
        "rules": {
            "form4_history": f">= {FORM4_HISTORY_MIN} Form 4 accessions (4, 4/A) known in (t - "
                             f"{FORM4_HISTORY_DAYS} d, t]",
            "ticker": "latest filing instant <= t naming a ticker names a current ticker of the issuer",
        },
        "counts": {
            "accessions": int(len(frame)),
            "form4_accessions": int(len(form4)),
            "accessions_naming_no_ticker": int(len(frame) - len(named)),
            "filing_instants_naming_a_ticker": int(len(tickers)),
            "filing_instants_not_matching_current": int((~tickers["match"]).sum()),
            "issuers_never_naming_a_current_ticker": int(
                len(wanted) - tickers.loc[tickers["match"], "issuer_cik"].nunique()
            ),
        },
        "form4_sha256": digest(v1._records(form4.sort_values(["issuer_cik", "known_at"]))),
        "tickers_sha256": digest(v1._records(tickers)),
    }
    return Admission(form4=form4.reset_index(drop=True), tickers=tickers, receipt=receipt)


def form4_history_mask(admission: Admission, issuers: Iterable[int], decisions: pd.DatetimeIndex) -> pd.DataFrame:
    """At least :data:`FORM4_HISTORY_MIN` Form 4 accessions known in (t - 730 d, t]."""
    issuers = [int(i) for i in issuers]
    t = decisions.tz_convert("UTC").as_unit("ns").asi8
    lo = t - np.int64(FORM4_HISTORY_DAYS) * np.int64(86_400_000_000_000)
    out = np.zeros((len(decisions), len(issuers)), dtype=bool)
    by_issuer = {c: g for c, g in admission.form4.groupby("issuer_cik")}
    for j, cik in enumerate(issuers):
        events = by_issuer.get(cik)
        if events is None:
            continue
        known = np.sort(v1._ns(events["known_at"]))
        count = np.searchsorted(known, t, side="right") - np.searchsorted(known, lo, side="right")
        out[:, j] = count >= FORM4_HISTORY_MIN
    return pd.DataFrame(out, index=decisions, columns=issuers)


def ticker_mask(admission: Admission, issuers: Iterable[int], decisions: pd.DatetimeIndex) -> pd.DataFrame:
    """The latest ticker-naming filing instant <= t names a current ticker of the issuer."""
    issuers = [int(i) for i in issuers]
    t = decisions.tz_convert("UTC").as_unit("ns").asi8
    out = np.zeros((len(decisions), len(issuers)), dtype=bool)
    by_issuer = {c: g for c, g in admission.tickers.groupby("issuer_cik")}
    for j, cik in enumerate(issuers):
        rows = by_issuer.get(cik)
        if rows is None:
            continue
        known = v1._ns(rows["known_at"])
        match = rows["match"].to_numpy(dtype=bool)
        idx = np.searchsorted(known, t, side="right") - 1
        out[:, j] = (idx >= 0) & match[np.clip(idx, 0, None)]
    return pd.DataFrame(out, index=decisions, columns=issuers)


def admitted_mask(events: v1.Form4Events, admission: Admission, issuers: Iterable[int],
                  decisions: pd.DatetimeIndex) -> pd.DataFrame:
    """v2 §2.2 rules 3-4: v1's Section 16 activity, the Form 4 history and the ticker rule."""
    issuers = list(issuers)
    return (
        v1.active_mask(events.activity, issuers, decisions)
        & form4_history_mask(admission, issuers, decisions)
        & ticker_mask(admission, issuers, decisions)
    )


def feature_panel(events: v1.Form4Events, admission: Admission, issuers: Iterable[int],
                  decisions: pd.DatetimeIndex, feature: str) -> pd.DataFrame:
    """The declared feature (v1 §4, unchanged), NaN where the issuer is not admitted at ``t``."""
    window, tau = v1.FEATURES[feature]
    issuers = list(issuers)
    values = v1.density(events.purchases, issuers, decisions, window, tau)
    return values.where(admitted_mask(events, admission, issuers, decisions))


def load_inputs(form4: Path, submissions: Path, universe: pd.DataFrame,
                owners: Path | None = None) -> tuple[v1.Form4Events, Admission]:
    """Events (v1 rules) and admission inputs for the universe's issuers."""
    issuers = sorted({int(i) for i in universe["cik"]})
    inputs = {
        "transactions": {"name": Path(form4).name, "sha256": v1.data_sha256(form4)},
        "submissions": {"name": Path(submissions).name, "sha256": v1.data_sha256(submissions)},
        "issuer_filter_sha256": digest(issuers),
    }
    owner_frame = None
    if owners is not None:
        owner_frame = v1.read_table(owners)
        inputs["owners"] = {"name": Path(owners).name, "sha256": v1.data_sha256(owners)}
    subs = read_submissions(submissions, issuers)
    events = v1.build_events(v1.read_table(form4, issuers), owner_frame, submissions=subs, inputs=inputs,
                             issuers=issuers)
    return events, build_admission(subs, universe)


# --- prices ------------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceManifest(v1.PriceManifest):
    """v1's admitted-price contract plus optional per-ticker source listing starts.

    ``listed_from``: ``((ticker, "YYYY-MM-DD"), ...)`` from the admitted source's
    own metadata; an issuer-date before its ticker's listing start abstains
    (the C1 cross-check with N = 0 sessions).
    """

    listed_from: tuple[tuple[str, str], ...] = ()

    def validate(self) -> None:
        super().validate()
        for ticker, start in self.listed_from:
            if ticker not in self.admitted:
                raise ValueError(f"listed_from names a ticker that is not admitted: {ticker}")
            date.fromisoformat(start)

    @classmethod
    def from_file(cls, path: Path) -> "PriceManifest":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        listed = raw.get("listed_from") or {}
        items = listed.items() if isinstance(listed, dict) else listed
        manifest = cls(**{**raw, "admitted": tuple(sorted(raw["admitted"])),
                          "listed_from": tuple(sorted((str(t), str(d)) for t, d in items))})
        manifest.validate()
        return manifest

    def digest(self) -> str:
        return digest({**asdict(self), "admitted": list(self.admitted),
                       "listed_from": [list(x) for x in self.listed_from]})


_KEY_TOKEN = object()
_HOLDOUT_TOKEN = object()


class DiscoveryKey:
    """Proof that v2's ``discovery_opened`` is in the pinned v2 chain and witnessed off-host."""

    window = "discovery"

    def __init__(self, token: object, inputs_frozen_sha256: str, inputs: dict, *,
                 log_dir: Path | None = None, witness_tip: str | None = None) -> None:
        if token is not _KEY_TOKEN:
            raise TypeError("a v2 DiscoveryKey is issued only by resume_discovery")
        if log_dir is None or witness_tip is None:
            raise TypeError("a DiscoveryKey needs its registry and off-host witness")
        self.inputs_frozen_sha256 = inputs_frozen_sha256
        self.inputs = dict(inputs)
        self.as_of_ts = stamp(inputs["as_of_ts"])
        self.log_dir = Path(log_dir)
        self.witness_tip = witness_tip


class HoldoutKey:
    """Proof that v2's ``holdout_opened`` is in the pinned v2 chain and witnessed off-host."""

    window = "holdout"

    def __init__(self, token: object, frozen_sha256: str, inputs: dict, *,
                 log_dir: Path | None = None, witness_tip: str | None = None) -> None:
        if token is not _HOLDOUT_TOKEN:
            raise TypeError("a v2 HoldoutKey is issued only by resume_holdout")
        if log_dir is None or witness_tip is None:
            raise TypeError("a HoldoutKey needs its registry and off-host witness")
        self.frozen_sha256 = frozen_sha256
        self.inputs = dict(inputs)
        self.as_of_ts = stamp(inputs["as_of_ts"])
        self.log_dir = Path(log_dir)
        self.witness_tip = witness_tip


def load_price_panel(conn, manifest: PriceManifest, tickers: Iterable[str], *, start: date, as_of: date,
                     window: str, key: DiscoveryKey | HoldoutKey) -> v1.PricePanel:
    """v1's bounded, admitted, source-filtered read, behind a v2 registry key."""
    manifest.validate()
    if window == "discovery":
        if not isinstance(key, DiscoveryKey):
            raise PermissionError("discovery prices need a v2 DiscoveryKey from resume_discovery")
        if as_of >= stamp(v1.SPLIT).date():
            raise PermissionError("discovery reads stop before the split date")
    elif window == "holdout":
        if not isinstance(key, HoldoutKey):
            raise PermissionError("holdout prices need a v2 HoldoutKey from resume_holdout")
        if as_of >= stamp(v1.END).date():
            raise PermissionError("holdout reads stop before the end of the frozen window")
    else:
        raise ValueError("window must be discovery or holdout")
    if manifest.digest() != key.inputs.get("price_manifest_sha256"):
        raise PermissionError("price manifest differs from the one in inputs_frozen")
    if manifest.probe_report_sha256 != key.inputs.get("probe_report_sha256"):
        raise PermissionError("price manifest's probe report differs from the one in inputs_frozen")
    wanted = sorted(set(tickers) | {manifest.benchmark})
    refused = [t for t in wanted if t not in manifest.admitted]
    if refused:
        raise PermissionError(f"tickers not in the admitted-price manifest: {refused[:10]}")
    data = {
        ticker: tuple(observations.read_window(
            conn, manifest.series_template.format(ticker=ticker), source=manifest.source,
            start=start, as_of=as_of, as_of_ts=key.as_of_ts,
        ))
        for ticker in wanted
    }
    if not data[manifest.benchmark]:
        raise ValueError("no benchmark closes in the read window")
    panel = v1.PricePanel(token=v1._PRICE_LOADER, manifest=manifest, start=start, as_of=as_of,
                          as_of_ts=key.as_of_ts, window=window, data=data)
    record_prices_read(key, panel.receipt_sha)
    return panel


# --- trial panels ------------------------------------------------------------------------


@dataclass
class TrialPanel(v1.TrialPanel):
    """v1's trial panel plus each entity's SIC group (reported-only industry-neutral IC)."""

    groups: list[str] = field(default_factory=list)


def build_trial_panels(events: v1.Form4Events, admission: Admission, universe: pd.DataFrame,
                       prices: v1.PricePanel, window: str) -> dict[str, TrialPanel]:
    """Every declared trial's panel for one window (v1 construction, v2 admission)."""
    prices.verify()
    if prices.window != window:
        raise ValueError("price panel window differs")
    closes = prices.closes()
    benchmark = prices.manifest.benchmark
    universe = universe[universe["ticker"].isin(closes.columns)]
    tickers = list(universe["ticker"])
    ciks = list(universe["cik"].astype(int))
    groups = list(universe["sic_group"])
    listed = dict(getattr(prices.manifest, "listed_from", ()) or ())
    panels = {}
    for h in v1.HORIZONS:
        positions, labels, momentum = v1.relative_labels(closes, benchmark, tickers, h, window)
        sessions = [closes.index[i].date() for i in positions]
        decided = v1.decision_instants(sessions)
        ends = v1.decision_instants([closes.index[i + h].date() for i in positions])
        trading = (np.isfinite(closes[tickers].to_numpy(dtype=float)[positions]) if positions
                   else np.zeros((0, len(tickers)), bool))
        after_listing = np.ones_like(trading)
        for j, ticker in enumerate(tickers):
            if ticker in listed:
                after_listing[:, j] = [s >= date.fromisoformat(listed[ticker]) for s in sessions]
        for name in v1.FEATURES:
            feature = feature_panel(events, admission, ciks, decided, name).to_numpy(dtype=float)
            feature = np.where(trading & after_listing, feature, np.nan)
            largest = v1.largest_value(events.purchases, ciks, decided, v1.FEATURES[name][0])
            trial = f"{name}|fwd{h}"
            panels[trial] = TrialPanel(
                trial=trial, window=window, horizon=h,
                decision_at=[d.isoformat() for d in decided], label_end=[d.isoformat() for d in ends],
                entities=tickers, feature=feature, label=labels, momentum=momentum,
                largest=np.where(np.isfinite(feature), largest.to_numpy(dtype=float), np.nan),
                groups=groups,
            )
    return panels


# --- reported-only statistics (C2 delisting bounds, SIC-group-neutral IC) ------------------


def _mean_ic(feature: np.ndarray, label: np.ndarray) -> float | None:
    ic, _ = v1.rank_ic_series(feature, label)
    return float(np.nanmean(ic)) if np.isfinite(ic).any() else None


def delisting_sensitivity(panel: v1.TrialPanel) -> dict:
    """C2 (reported only): the mean IC and buyer-minus-non-buyer with missing labels imputed.

    A missing label is an issuer-date with a feature but no label (no end
    close). *pessimistic*: a missing buyer label (A > 0) takes the date's worst
    observed relative return, a missing non-buyer label the date's best;
    *neutral*: 0; *optimistic*: the mirror of pessimistic.
    """
    has_feature = np.isfinite(panel.feature)
    missing = has_feature & ~np.isfinite(panel.label)
    buyer = has_feature & (np.nan_to_num(panel.feature) > 0)
    with np.errstate(all="ignore"):
        worst = np.nanmin(np.where(np.isfinite(panel.label), panel.label, np.nan), axis=1, initial=np.inf)
        best = np.nanmax(np.where(np.isfinite(panel.label), panel.label, np.nan), axis=1, initial=-np.inf)
    worst = np.where(np.isfinite(worst), worst, np.nan)[:, None]
    best = np.where(np.isfinite(best), best, np.nan)[:, None]
    out = {"missing_labels": int(missing.sum()), "buyer_missing_labels": int((missing & buyer).sum())}
    for bound in DELISTING_BOUNDS:
        if bound == "neutral":
            fill_buyer = fill_other = np.zeros_like(worst)
        elif bound == "pessimistic":
            fill_buyer, fill_other = worst, best
        else:
            fill_buyer, fill_other = best, worst
        label = panel.label.copy()
        label = np.where(missing & buyer, np.broadcast_to(fill_buyer, label.shape), label)
        label = np.where(missing & ~buyer, np.broadcast_to(fill_other, label.shape), label)
        imputed = v1.TrialPanel(trial=panel.trial, window=panel.window, horizon=panel.horizon,
                                decision_at=panel.decision_at, label_end=panel.label_end,
                                entities=panel.entities, feature=panel.feature, label=label)
        ic, _ = v1.rank_ic_series(panel.feature, label)
        rows = np.flatnonzero(np.isfinite(ic))
        out[bound] = {
            "mean_ic": float(ic[rows].mean()) if len(rows) else None,
            "buyer_minus_nonbuyer": v1.buyer_excess(imputed, rows)["buyer_minus_nonbuyer"],
        }
    observed = _mean_ic(panel.feature, panel.label)
    pessimistic = out["pessimistic"]["mean_ic"]
    out["sign_survives_pessimistic"] = bool(
        observed is not None and pessimistic is not None and np.sign(observed) == np.sign(pessimistic)
    )
    out["note"] = "reported only; never selects"
    return out


def industry_neutral_ic(panel: TrialPanel) -> dict:
    """Reported only: mean rank IC with labels demeaned within SIC group on each date."""
    groups = np.asarray(getattr(panel, "groups", None) or [], dtype=object)
    if len(groups) != len(panel.entities):
        return {"mean_ic": None, "note": "no SIC groups on this panel"}
    label = panel.label.copy()
    for g in set(groups.tolist()):
        cols = groups == g
        block = label[:, cols]
        seen = np.isfinite(block)
        total = np.where(seen, block, 0.0).sum(axis=1, keepdims=True)
        count = seen.sum(axis=1, keepdims=True)
        centre = np.divide(total, count, out=np.zeros_like(total), where=count > 0)
        label[:, cols] = block - centre
    return {"mean_ic": _mean_ic(panel.feature, label),
            "groups": {g: int((groups == g).sum()) for g in sorted(set(groups.tolist()))},
            "note": "labels demeaned within SIC group per date; reported only"}


def measure_trial(panel: v1.TrialPanel, *, block: int | None = None, perms: int = v1.PERMS,
                  seed: int = v1.SEED, min_n: int = v1.MIN_N, sensitivity: bool = True) -> dict:
    """v1's measurement plus the negative one-sided p (same draws) and the v2 reported items."""
    out = v1.measure_trial(panel, block=block, perms=perms, seed=seed, min_n=min_n, sensitivity=sensitivity)
    if out["status"] != "tested":
        return {**out, "p_one_sided_negative": 1.0}
    ic, _ = v1.rank_ic_series(panel.feature, panel.label)
    series = ic[np.isfinite(ic)]
    _, _, p_negative = v1.signflip_pvalues(series, out["block"], perms, seed, -1)
    out["p_one_sided_negative"] = p_negative
    if sensitivity:
        out["delisting_sensitivity"] = delisting_sensitivity(panel)
        out["industry_neutral"] = industry_neutral_ic(panel)
    return out


# --- discovery, calibration, holdout, verdict -----------------------------------------------


def run_spec(run_id: str | None = None) -> v1.RunSpec:
    """The v2 run: the VS1 sector, k = 1 of the ledger, the 4 declared trials (v1 §5-§7 unchanged)."""
    return v1.RunSpec(run_id=run_id or f"{VERSION}:{v1.VS1_SECTOR}", sector=v1.VS1_SECTOR,
                      run_k=v1.VS1_RUN_K, trials=v1.trial_names())


def calibration(ledger: list[dict], alpha: float = v1.run_alpha(v1.VS1_RUN_K)) -> dict:
    """v2 §11 (C3): CONTRARY only when a negative one-sided p survives Holm at the run alpha.

    ``primary_against_expectation``: the primary trial's mean IC is below 0 with
    one-sided p <= 0.05 in the negative direction (evidence against the expected
    sign). v1's rule outcome is reported under ``v1_rule``.
    """
    by = {t["trial"]: t for t in ledger}
    primary = by[v1.PRIMARY_TRIAL]
    negative = [t.get("p_one_sided_negative", 1.0) if t["status"] == "tested" else 1.0 for t in ledger]
    holm_negative = holm_adjusted(negative)
    contrary = [
        t["trial"] for t, h in zip(ledger, holm_negative)
        if t["status"] == "tested" and t["mean_ic"] is not None and t["mean_ic"] < 0 and h <= alpha
    ]
    against = bool(
        primary["status"] == "tested" and primary["mean_ic"] < 0
        and primary.get("p_one_sided_negative", 1.0) <= PRIMARY_AGAINST_P
    )
    consistent = (
        primary["status"] == "tested" and primary["mean_ic"] > 0 and primary["p_one_sided_positive"] <= 0.10
    ) or any(t["selected"] and t["mean_ic"] > 0 for t in ledger)
    if contrary:
        state = "CONTRARY"
    elif consistent:
        state = "CONSISTENT"
    elif primary["status"] == "tested" and primary["mean_ic"] > 0:
        state = "WEAK_POSITIVE"
    else:
        state = "ABSENT"
    return {
        "state": state,
        "primary_trial": v1.PRIMARY_TRIAL,
        "contrary_trials": contrary,
        "holm_adjusted_negative_p": {t["trial"]: h for t, h in zip(ledger, holm_negative)},
        "primary_against_expectation": against,
        "v1_rule": v1.calibration(ledger),
    }


def discover_panel(spec: v1.RunSpec, panels: Mapping[str, v1.TrialPanel], *, inputs: dict,
                   repo_root: Path = REPO, sensitivity: bool = True) -> dict:
    """Freeze the v2 discovery ledger (v1 statistic and selection; v2 calibration)."""
    prereg = check_prereg(repo_root)
    spec.validate()
    if set(panels) != set(spec.trials):
        raise ValueError("panels must be exactly the declared trials")
    ledger = []
    for trial in spec.trials:
        panel = panels[trial]
        if panel.window != "discovery":
            raise ValueError("discovery received a non-discovery panel")
        v1.validate_panel(panel)
        result = measure_trial(panel, perms=spec.perms, seed=spec.seed, min_n=spec.min_n, sensitivity=sensitivity)
        ledger.append({"trial_id": digest([spec.run_id, spec.sector, trial]), "trial": trial,
                       "sector": spec.sector, **result})
    pvalues = [t["p"] for t in ledger]
    for trial, holm, bh in zip(ledger, holm_adjusted(pvalues), v1.bh_adjusted(pvalues)):
        trial["holm_adjusted_p"] = holm
        trial["bh_adjusted_p"] = bh
        trial["selected"] = trial["status"] == "tested" and holm <= spec.alpha
    payload = {
        "version": VERSION,
        "origin": ORIGIN,
        "prereg_sha256": prereg,
        "spec": asdict(spec),
        "windows": {"discovery_start": v1.DISCOVERY_START, "split": v1.SPLIT, "end": v1.END},
        "selection": f"Holm at ledger run alpha {spec.alpha:.6g} (q={spec.ledger_q}, k={spec.run_k}) "
                     "over every declared trial incl. untestable; BH-adjusted p reported only",
        "null": "block sign-flip of the per-date rank-IC series; block from discovery IC acf1 "
                f"(autocorrelation_block, >= {MIN_BLOCKS} blocks)",
        "inputs": inputs,
        "discovery_sha256": digest({t: panels[t].as_record() for t in spec.trials}),
        "ledger": ledger,
        "calibration": calibration(ledger, spec.alpha),
        "state": "DISCOVERY_FROZEN",
        "promotion_allowed": False,
    }
    return {"payload": payload, "sha256": digest(payload)}


def verdict(payload: dict, checks: list[dict], power: dict | None) -> dict:
    """v2 §11 verdict; v1's rule outcome is reported as ``v1_rule_state``."""
    calib = payload["calibration"]
    survivors = [c for c in checks if c["retrospective_survivor"]]
    positive = [c for c in survivors if c["mean_ic"] > 0]
    powered = bool(power and power.get("gate_passed"))
    notes = []
    if calib["state"] == "CONTRARY" or any(c["mean_ic"] < 0 for c in survivors):
        state = "MACHINERY_SUSPECT"
        notes.append("a Holm-significant negative insider-buy IC contradicts the published prior: audit the "
                     "event parse, dates, universe and prices before reading anything else")
    elif calib.get("primary_against_expectation"):
        state = "MACHINERY_SUSPECT"
        notes.append("the primary discovery IC is negative with one-sided p <= 0.05: evidence against the "
                     "expected sign")
    elif positive:
        state = "HOLDOUT_SURVIVOR_FORWARD_PENDING"
    else:
        state = "NO_SURVIVOR"
        if not powered:
            notes.append("UNDERPOWERED: the Stage-0 power gate did not pass; a null here is not "
                         "evidence against the published effect")
    primary_rows = [t for t in [*payload.get("ledger", []), *checks] if t.get("trial") == v1.PRIMARY_TRIAL]
    if any(t.get("labels", {}).get("buyer_missing_share", 0.0) > v1.MISSING_LABEL_WARNING for t in primary_rows):
        notes.append(f"SURVIVORSHIP_WARNING: more than {v1.MISSING_LABEL_WARNING:.0%} of the primary trial's "
                     "buyer issuer-dates have no label (possible delistings; no delisting return)")
    if any((t.get("delisting_sensitivity") or {}).get("sign_survives_pessimistic") is False for t in primary_rows):
        notes.append("DELISTING_SENSITIVE: the primary trial's IC changes sign under the pessimistic "
                     "delisting bound (reported only)")
    v1_state = v1.verdict({"calibration": {"state": (calib.get("v1_rule") or {}).get("state", "ABSENT")},
                           "ledger": payload.get("ledger", [])}, checks, power)["state"]
    return {"state": state, "calibration": calib["state"], "notes": notes, "v1_rule_state": v1_state,
            "survivors": [c["trial"] for c in positive], "promotion_allowed": False,
            "statement": "Nothing here is a trading signal."}


def check_holdout_request(frozen: dict, *, allow_holdout: bool, prereg_sha256: str, repo_root: Path = REPO) -> dict:
    if allow_holdout is not True:
        raise PermissionError("holdout evaluation needs an explicit allow_holdout=True")
    if prereg_sha256 != PREREG_BODY_SHA256:
        raise PermissionError("the pre-registration hash given does not match the pinned v2 one")
    check_prereg(repo_root)
    payload = frozen.get("payload") or {}
    if digest(payload) != frozen.get("sha256"):
        raise PermissionError("frozen discovery manifest changed")
    if payload.get("prereg_sha256") != PREREG_BODY_SHA256 or payload.get("version") != VERSION:
        raise PermissionError("the discovery was not run under this pre-registration")
    if payload.get("state") != "DISCOVERY_FROZEN":
        raise PermissionError("no frozen discovery to evaluate")
    return payload


def evaluate_panel_holdout(frozen: dict, panels: Mapping[str, v1.TrialPanel], key: HoldoutKey, *,
                           power: dict | None = None) -> dict:
    """Frozen selections (and the primary trial) on the holdout, once (v1 §8 unchanged)."""
    if not isinstance(key, HoldoutKey) or key.frozen_sha256 != frozen.get("sha256"):
        raise PermissionError("holdout needs the v2 HoldoutKey opened for this frozen discovery")
    payload = frozen["payload"]
    spec = v1.RunSpec(**{**payload["spec"], "trials": tuple(payload["spec"]["trials"])})
    ledger = {t["trial"]: t for t in payload["ledger"]}
    selected = [t for t in payload["ledger"] if t["selected"]]
    evaluated = sorted({t["trial"] for t in selected} | {v1.PRIMARY_TRIAL})
    checks = []
    for trial in evaluated:
        panel = panels[trial]
        if panel.window != "holdout":
            raise ValueError("holdout received a non-holdout panel")
        v1.validate_panel(panel)
        ic, _ = v1.rank_ic_series(panel.feature, panel.label)
        n = int(np.isfinite(ic).sum())
        block = max(1, min(ledger[trial]["block"] or 1, max(1, n // MIN_BLOCKS)))
        result = measure_trial(panel, block=block, perms=spec.perms, seed=spec.seed, min_n=spec.min_n)
        is_selected = ledger[trial]["selected"]
        adjusted = corrected_p(result["p"], len(selected)) if is_selected else None
        survives = bool(is_selected and result["status"] == "tested" and adjusted <= v1.HOLDOUT_ALPHA
                        and result["mean_ic"] * ledger[trial]["mean_ic"] > 0)
        checks.append({"trial_id": ledger[trial]["trial_id"], "trial": trial, "selected_in_discovery": is_selected,
                       **result, "bonferroni_p": adjusted, "retrospective_survivor": survives,
                       "primary_one_sided_p": result["p_one_sided_positive"] if trial == v1.PRIMARY_TRIAL else None})
    result = {"discovery_manifest": frozen["sha256"], "prereg_sha256": payload["prereg_sha256"],
              "holdout_sha256": digest({t: panels[t].as_record() for t in evaluated}),
              "holdout_checks": checks, "promotion_allowed": False}
    result["verdict"] = verdict(payload, checks, power)
    return result


# --- Stage 0 (v1 §10 settings, v2 universe and admission) ------------------------------------


def power_features(events: v1.Form4Events, admission: Admission, universe: pd.DataFrame,
                   window: str = "discovery") -> dict[str, np.ndarray]:
    """Feature panels on proxy sessions (no price read) for the Stage-0 gate."""
    lo, hi = v1.window_bounds(window)
    sessions = v1.proxy_sessions(lo.date(), hi.date())
    ciks = list(universe["cik"].astype(int))
    out = {}
    for h in v1.HORIZONS:
        decided = v1.decision_instants(sessions[::h])
        for name in v1.FEATURES:
            out[f"{name}|fwd{h}"] = feature_panel(events, admission, ciks, decided, name).to_numpy(dtype=float)
    return out


def stage0_power(features: Mapping[str, np.ndarray]) -> dict:
    """v1's Stage-0 power at v1's pinned settings (same run alpha, threshold and gate)."""
    return {**v1.stage0_power(features), "version": VERSION}


def verify_power(power: Mapping[str, Any]) -> None:
    if power.get("version") != VERSION:
        raise ValueError("power file was not computed by the v2 harness")
    v1.verify_power(power)


def admission_report(events: v1.Form4Events, admission: Admission, universe: pd.DataFrame,
                     window: str = "discovery") -> dict:
    """Stage-0 description of the admitted panel (Form 4 data only): issuers and events per year."""
    lo, hi = v1.window_bounds(window)
    sessions = v1.proxy_sessions(lo.date(), hi.date())
    ciks = list(universe["cik"].astype(int))
    decisions = v1.decision_instants(sessions)
    rules = {
        "rule1_section16_filer": v1.active_mask(events.activity, ciks, decisions),
        "rule2_form4_history": form4_history_mask(admission, ciks, decisions),
        "rule3_ticker": ticker_mask(admission, ciks, decisions),
    }
    daily = rules["rule1_section16_filer"] & rules["rule2_form4_history"] & rules["rule3_ticker"]
    ever = daily.any(axis=0)
    purchases = events.purchases[events.purchases["issuer_cik"].isin(ciks)].copy()
    in_window = purchases[(purchases["known_at"] >= lo) & (purchases["known_at"] < hi)].copy()
    instants = pd.DatetimeIndex(sorted(in_window["known_at"].unique()))
    if len(instants):
        at_filing = admitted_mask(events, admission, ciks, instants)
        rows = instants.get_indexer(pd.DatetimeIndex(in_window["known_at"]))
        cols = pd.Index(ciks).get_indexer(in_window["issuer_cik"])
        in_window["admitted"] = at_filing.to_numpy()[rows, cols]
    else:
        in_window["admitted"] = np.zeros(0, dtype=bool)
    in_window["year"] = in_window["known_at"].dt.tz_convert(v1.NEW_YORK).dt.year
    by_source = universe.set_index("cik")["source"]
    admitted_ciks = [c for c in ciks if ever.get(c, False)]
    per_date = daily.sum(axis=1)
    return {
        "window": window,
        "proxy_sessions": len(sessions),
        "issuer_dates": int(daily.size),
        "issuer_dates_admitted": int(daily.to_numpy().sum()),
        "issuer_dates_failing": {name: int((~mask.to_numpy()).sum()) for name, mask in rules.items()},
        "universe_issuers": len(ciks),
        "admitted_issuers": len(admitted_ciks),
        "admitted_issuers_by_source": {k: int(v) for k, v in
                                       by_source.loc[admitted_ciks].value_counts().sort_index().items()},
        "admitted_issuers_per_proxy_session": {"median": float(per_date.median()), "min": int(per_date.min()),
                                               "max": int(per_date.max())},
        "purchase_events_by_year": {int(y): int(n) for y, n in in_window.groupby("year").size().items()},
        "admitted_purchase_events_by_year": {int(y): int(n) for y, n in
                                             in_window[in_window["admitted"]].groupby("year").size().items()},
        "purchase_events": int(len(in_window)),
        "admitted_purchase_events": int(in_window["admitted"].sum()),
        "issuers_with_admitted_purchase": int(in_window.loc[in_window["admitted"], "issuer_cik"].nunique()),
        "admitted_buyer_pairs": int(
            in_window.loc[in_window["admitted"], ["issuer_cik", "actor"]].drop_duplicates().shape[0]
        ),
    }


# --- v2 registry (hash-chained, research_forward_log mechanism; separate from v1) -----------

REGISTRY_LOG = "granular_panel_prereg_v2.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v2.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v2.lock"

#: The one real v2 registration (header + ``preregistration``), pinned once registered.
REGISTERED_AT: datetime | None = None
REGISTERED_CODE_SHA: str | None = None
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_PREREG_SHA256 = PREREG_BODY_SHA256

#: The off-host witness, pinned: this path on ``main`` of the GitHub vault.
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = "05-GRID/Paper-Log/vs1/granular_panel_prereg_v2.anchors.jsonl"
WITNESS_REF = "refs/vs1-v2-witness/main"
REGISTERED_ANCHOR_LINE: bytes | None = None
_WITNESS_TOKEN = object()

#: v1's registration stays at exactly its 2 records if v1 never opened a discovery.
V1_REGISTRATION_RECORDS = 2


def registry(log_dir: Path, prereg_sha256: str = PREREG_BODY_SHA256):
    from analysis.research_forward_log import ForwardLog

    return ForwardLog(log_dir, log_filename=REGISTRY_LOG, anchor_filename=REGISTRY_ANCHORS,
                      lock_filename=REGISTRY_LOCK, prereg_sha256=prereg_sha256)


def registration_records(now: datetime, code_sha: str, prereg_sha256: str = PREREG_BODY_SHA256) -> list[dict]:
    """The header and ``preregistration`` records (before chaining), as ``register`` writes them."""
    header = {
        "kind": "header",
        "version": VERSION,
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "promotion_allowed": False,
    }
    record = {
        "kind": "preregistration",
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "sector_map_sha256": v1.SECTOR_MAP_SHA256,
        "issuer_map_sha256": ISSUER_MAP_SHA256,
        "sic_map_sha256": SIC_MAP_SHA256,
        "sic_ranges": [list(r) for r in SIC_RANGES],
        "ledger_id": v1.LEDGER_ID,
        "runs": {
            "vs1": {"sector": v1.VS1_SECTOR, "k": v1.VS1_RUN_K, "alpha": v1.run_alpha(v1.VS1_RUN_K),
                    "trials": list(v1.trial_names())},
            "other_sectors": {"sectors": list(v1.OTHER_SECTORS), "k": v1.OTHER_SECTORS_RUN_K,
                              "alpha": v1.run_alpha(v1.OTHER_SECTORS_RUN_K), "trials": list(v1.trial_names()),
                              "rules": "vs1-v1 (v1 pre-registration section 13, unchanged)"},
        },
        "supersedes": {
            "version": v1.VERSION,
            "prereg_sha256": v1.PREREG_BODY_SHA256,
            "registry_head_sha256": v1.REGISTERED_RECORD_SHA256[1],
            "status": "superseded before any price read (Stage-0 power gate failed; owner option (a))",
        },
        "windows": {"discovery_start": v1.DISCOVERY_START, "split": v1.SPLIT, "end": v1.END},
        "promotion_allowed": False,
    }
    return [header, record]


def register(log_dir: Path, now: datetime, code_sha: str, *, prereg_sha256: str = PREREG_BODY_SHA256) -> list[dict]:
    """Write the v2 registration into an empty registry directory.

    Before the pins exist (:data:`REGISTERED_RECORD_SHA256` is None) this is the
    one real registration; afterwards it only re-materialises that exact
    registration (a copy is witnessed like any copy) and refuses anything else.
    """
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    if prereg_sha256 != PREREG_BODY_SHA256:
        raise PermissionError("only the pinned v2 pre-registration can be registered")
    records = registration_records(now, code_sha, prereg_sha256)
    if REGISTERED_RECORD_SHA256 is not None and tuple(v1.chained_sha256(records)) != REGISTERED_RECORD_SHA256:
        raise PermissionError(
            "VS1 v2 is already registered (chain head "
            f"{REGISTERED_RECORD_SHA256[1][:12]}... at 2 records); a registration with another "
            "time, code or body starts a fork and is refused"
        )
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        if log.read_all():
            raise ValueError("this pre-registration is already registered in this directory")
        return log.append_locked(records)


def _chain(log, prereg_sha256: str) -> list[dict]:
    """Verified records of the pinned v2 registry (a fresh or recreated registry is a fork)."""
    if REGISTERED_RECORD_SHA256 is None:
        raise PermissionError("VS1 v2 is not registered yet (no pinned registration in code)")
    if prereg_sha256 != REGISTERED_PREREG_SHA256:
        raise PermissionError("only the pinned VS1 v2 pre-registration has a registry")
    check = log.verify_chain()
    if not check["ok"]:
        raise RuntimeError(f"registry chain is broken: {check['detail']}")
    heads = v1._line_sha256(log)
    if tuple(heads[:2]) != REGISTERED_RECORD_SHA256:
        if not heads:
            raise PermissionError("this pre-registration is not registered here (empty registry)")
        raise PermissionError(
            "registry is not the pinned VS1 v2 registration (its first records do not hash to "
            f"{REGISTERED_RECORD_SHA256[1][:12]}... at 2 records): a fork, refused"
        )
    return log.read_all()


class OffhostWitness:
    """The v2 witness file as committed on the pinned remote's ``main`` (issued by :func:`check_offhost`)."""

    def __init__(self, token: object, repo: Path, tip: str, content: bytes, versions: list[dict],
                 remote_url: str) -> None:
        if token is not _WITNESS_TOKEN:
            raise TypeError("a v2 OffhostWitness is issued only by check_offhost")
        self.repo, self.tip, self.content, self.versions = Path(repo), tip, content, versions
        self.remote_url = remote_url

    @property
    def lines(self) -> list[bytes]:
        return [line for line in self.content.split(b"\n") if line]

    def descends_from(self, commit: str) -> bool:
        import subprocess

        result = subprocess.run(["git", "-C", str(self.repo), "merge-base", "--is-ancestor", commit, self.tip],
                                capture_output=True, check=False)
        return result.returncode == 0

    def witnessing_commit(self, records: int) -> str | None:
        for version in self.versions:
            if version["covered_records"] >= records:
                return version["commit"]
        return None

    def receipt(self) -> dict:
        return {"remote_url": self.remote_url, "branch": WITNESS_BRANCH, "path": WITNESS_PATH,
                "tip": self.tip, "versions": self.versions}


def _walk_witness(repo: Path, ref: str, path: str, first_line: bytes, url: str) -> tuple[list[dict], list[bytes]]:
    """Every committed version of ``path`` on ``ref``: at that path, pinned first line, append-only."""
    log = v1._git(repo, "log", "--first-parent", "-m", "--follow", "--name-status", "--format=%x00%H", ref, "--", path)
    entries = []
    for chunk in log.split("\x00")[1:]:
        head, *rest = chunk.strip("\n").split("\n")
        entries.append((head.strip(), [line.split("\t") for line in rest if line.strip()]))
    if not entries:
        raise PermissionError(f"{path} is not on {WITNESS_BRANCH} of {url}")
    versions, previous = [], None
    for commit, changes in reversed(entries):
        for change in changes:
            status, paths = change[0], change[1:]
            if status.startswith(("R", "C")) or any(p != path for p in paths):
                raise PermissionError(f"{commit[:12]}: the witness file came from another path ({paths})")
            if status.startswith("D"):
                raise PermissionError(f"{commit[:12]}: the witness file was deleted (not append-only)")
        content = v1._git(repo, "show", f"{commit}:{path}", binary=True).replace(b"\r\n", b"\n")
        lines = [line for line in content.split(b"\n") if line]
        if not lines or lines[0] != first_line:
            raise PermissionError(f"{commit[:12]}: the witness file does not start with the pinned registration")
        if previous is not None and not (len(lines) > len(previous) and lines[: len(previous)] == previous):
            raise PermissionError(
                f"{commit[:12]}: the witness file is not a strict line-prefix extension of its previous "
                "version (truncated, edited or unchanged): not append-only"
            )
        versions.append({"commit": commit, "lines": len(lines), "covered_records": json.loads(lines[-1]).get("records", 0)})
        previous = lines
    return versions, previous


def check_offhost(vault_repo: Path, *, remote_url: str | None = None) -> OffhostWitness:
    """Fetch the pinned vault ``main`` and return the v2 witness file as committed there (v1 rules)."""
    if REGISTERED_ANCHOR_LINE is None:
        raise PermissionError("VS1 v2 is not registered yet (no pinned anchor line in code)")
    url = WITNESS_REMOTE_URL if remote_url is None else remote_url
    repo = Path(vault_repo)
    v1._git(repo, "rev-parse", "--git-dir")
    v1._git(repo, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", url,
            f"+refs/heads/{WITNESS_BRANCH}:{WITNESS_REF}")
    tip = v1._git(repo, "rev-parse", "--verify", f"{WITNESS_REF}^{{commit}}").strip()
    versions, previous = _walk_witness(repo, WITNESS_REF, WITNESS_PATH, REGISTERED_ANCHOR_LINE, url)
    at_tip = v1._git(repo, "show", f"{tip}:{WITNESS_PATH}", binary=True).replace(b"\r\n", b"\n")
    if [line for line in at_tip.split(b"\n") if line] != previous:
        raise PermissionError("the witness file at the fetched tip is not its last walked version")
    return OffhostWitness(_WITNESS_TOKEN, repo, tip, b"\n".join(previous) + b"\n", versions, url)


def require_v1_unopened(v1_witness: v1.OffhostWitness | None) -> dict:
    """v2 takes v1's ledger slot (k = 1): v1's off-host witness must still cover only its registration.

    A v1 discovery needs its ``discovery_opened`` head on v1's witness file
    before any v1 price read; a v1 witness beyond the 2 registration records
    means v1 was opened, and then v2 is refused (both would spend alpha_1).
    """
    if not isinstance(v1_witness, v1.OffhostWitness):
        raise PermissionError("v1's pinned off-host witness (panel_insider_density.check_offhost) is required")
    covered = json.loads(v1_witness.lines[-1])["records"]
    if covered != V1_REGISTRATION_RECORDS or len(v1_witness.lines) != 1:
        raise PermissionError(
            f"VS1 v1's off-host witness covers {covered} records: v1 was opened after its registration, so "
            "v2 cannot take ledger run k=1 (owner decision needed)"
        )
    return {"v1_witness_tip": v1_witness.tip, "v1_witnessed_records": covered}


def require_witness(log_dir: Path, witness: OffhostWitness | None, records_needed: int, *,
                    prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """The pinned v2 off-host anchor log must already witness this chain up to ``records_needed``."""
    import tempfile

    if not isinstance(witness, OffhostWitness):
        raise PermissionError("the pinned v2 off-host anchor log (check_offhost) is required")
    log = registry(log_dir, prereg_sha256)
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / Path(WITNESS_PATH).name
        path.write_bytes(witness.content)
        with log.locked():
            records = _chain(log, prereg_sha256)
            check = log.verify_chain(external_anchors=path)
    if not check["ok"]:
        raise PermissionError(f"off-host anchor log does not witness this registry: {check['detail']}")
    covered = json.loads(witness.lines[-1])["records"]
    if covered < records_needed:
        raise PermissionError(
            f"off-host anchor log covers {covered} records; it must already contain the chain head "
            f"at {records_needed} records (commit and push the registry's anchor lines to "
            f"{WITNESS_BRANCH}:{WITNESS_PATH} first)"
        )
    for record in v1._kind(records, "prices_read"):
        if not witness.descends_from(record["witness_tip"]):
            raise PermissionError(
                f"the pinned {WITNESS_BRANCH} no longer contains the witness commit "
                f"{record['witness_tip'][:12]} an earlier price read saw (history rewritten)"
            )
    return {"records": len(records), "witnessed_records": covered, "tip": witness.tip,
            "witnessing_commit": witness.witnessing_commit(records_needed)}


def export_anchors(log_dir: Path, vault_worktree: Path, *, prereg_sha256: str = PREREG_BODY_SHA256) -> list[str]:
    """Append to ``<vault_worktree>/WITNESS_PATH`` the v2 registry anchor lines it lacks."""
    from analysis.research_forward_log import _lines

    log = registry(log_dir, prereg_sha256)
    with log.locked():
        _chain(log, prereg_sha256)
        local = list(_lines(log.anchor_path))
    path = Path(vault_worktree) / WITNESS_PATH
    existing = [line.rstrip(b"\r") for line in _lines(path)] if path.exists() else []
    if existing != local[: len(existing)]:
        raise PermissionError("the off-host anchor log witnesses another registry chain; not appending")
    new = local[len(existing):]
    if new:
        path.parent.mkdir(parents=True, exist_ok=True)
        tail = path.read_bytes() if path.exists() else b""
        with open(path, "ab") as stream:
            if tail and not tail.endswith(b"\n"):
                stream.write(b"\n")
            for line in new:
                stream.write(line + b"\n")
    return [line.decode("utf-8") for line in new]


def record_prices_read(key: DiscoveryKey | HoldoutKey, price_receipt_sha256: str) -> dict:
    """Append ``prices_read``; a resumed run's receipt must equal the first read's."""
    if not isinstance(key, (DiscoveryKey, HoldoutKey)):
        raise PermissionError("recording a price read needs its v2 key")
    log = registry(key.log_dir)
    with log.locked():
        records = _chain(log, PREREG_BODY_SHA256)
        earlier = [r for r in v1._kind(records, "prices_read") if r["window"] == key.window]
        differ = [r for r in earlier if r["price_receipt_sha256"] != price_receipt_sha256]
        if differ:
            raise PermissionError(
                f"{key.window} prices differ from the first read under this registry "
                f"(receipt {differ[0]['price_receipt_sha256'][:12]} then {price_receipt_sha256[:12]}): refused"
            )
        return log.append_locked([{
            "kind": "prices_read", "run_at": datetime.now(timezone.utc).isoformat(),
            "prereg_sha256": PREREG_BODY_SHA256, "window": key.window,
            "price_receipt_sha256": price_receipt_sha256, "witness_tip": key.witness_tip,
            "promotion_allowed": False,
        }])[0]


def _check_observed(frozen_inputs: Mapping[str, Any], observed: Mapping[str, Any]) -> None:
    missing = [k for k in OBSERVED_INPUT_KEYS if k not in observed]
    if missing:
        raise ValueError(f"observed inputs lack {missing}")
    differ = sorted(k for k in set(OBSERVED_INPUT_KEYS) | set(observed) if frozen_inputs.get(k) != observed.get(k))
    if differ:
        raise PermissionError(f"inputs differ from the inputs_frozen record: {differ}")


def freeze_inputs(log_dir: Path, now: datetime, inputs: Mapping[str, Any], *,
                  prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """Append ``inputs_frozen`` (every input hash and ``as_of_ts``) before any price read."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    missing = [k for k in FROZEN_INPUT_KEYS if k not in inputs]
    if missing:
        raise ValueError(f"inputs_frozen needs {missing}")
    if inputs["sector"] != v1.VS1_SECTOR:
        raise ValueError("only the VS1 sector runs under this registry")
    for k in FROZEN_INPUT_KEYS:
        if k.endswith("_sha256") and not v1._is_hex64(inputs[k]):
            raise ValueError(f"{k} must be a sha256 hex digest")
    if not isinstance(inputs["accept_underpowered"], bool):
        raise ValueError("accept_underpowered must be a boolean")
    if stamp(inputs["as_of_ts"]) > now:
        raise ValueError("as_of_ts cannot be later than the freeze")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        if v1._kind(records, "discovery_opened"):
            raise PermissionError("a discovery was already opened: its inputs cannot be re-frozen")
        previous = v1._kind(records, "inputs_frozen")
        return log.append_locked([{
            "kind": "inputs_frozen", "run_at": now.isoformat(), "prereg_sha256": prereg_sha256,
            "inputs": dict(inputs), "supersedes": v1._record_sha256(previous[-1]) if previous else None,
            "promotion_allowed": False,
        }])[0]


def latest_frozen_inputs(log_dir: Path, *, prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        frozen_inputs = v1._kind(_chain(log, prereg_sha256), "inputs_frozen")
    if not frozen_inputs:
        raise PermissionError("no inputs_frozen record: run freeze-inputs before any price read")
    return dict(frozen_inputs[-1]["inputs"])


def open_discovery(log_dir: Path, now: datetime, observed: Mapping[str, Any], v1_witness: v1.OffhostWitness | None,
                   *, prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """One-shot v2 discovery, step 1: append ``discovery_opened`` (no price is read).

    Also requires v1's off-host witness to show v1 never opened (:func:`require_v1_unopened`).
    """
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    superseded = require_v1_unopened(v1_witness)
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        if v1._kind(records, "discovery_opened") or v1._kind(records, "discovery_frozen"):
            raise PermissionError("discovery already ran under this pre-registration (one shot)")
        frozen_inputs = v1._kind(records, "inputs_frozen")
        if not frozen_inputs:
            raise PermissionError("no inputs_frozen record: run freeze-inputs before any price read")
        current = frozen_inputs[-1]
        _check_observed(current["inputs"], observed)
        log.append_locked([{
            "kind": "discovery_opened", "run_at": now.isoformat(), "prereg_sha256": prereg_sha256,
            "inputs_frozen_sha256": v1._record_sha256(current), "v1_superseded": superseded,
            "promotion_allowed": False,
        }])
        heads = v1._line_sha256(log)
    return {"kind": "discovery_opened", "records": len(heads), "head_sha256": heads[-1]}


def resume_discovery(log_dir: Path, observed: Mapping[str, Any], witness: OffhostWitness | None,
                     v1_witness: v1.OffhostWitness | None, *, prereg_sha256: str = PREREG_BODY_SHA256) -> DiscoveryKey:
    """One-shot v2 discovery, step 2: the price key, once the v2 off-host log witnesses the opening."""
    require_v1_unopened(v1_witness)
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
    if v1._kind(records, "discovery_frozen"):
        raise PermissionError("a discovery is already frozen (one shot)")
    position = v1._position(records, "discovery_opened")
    opened = records[position - 1]
    matching = [r for r in v1._kind(records, "inputs_frozen") if v1._record_sha256(r) == opened["inputs_frozen_sha256"]]
    if len(matching) != 1:
        raise PermissionError("discovery_opened does not name an inputs_frozen record of this chain")
    _check_observed(matching[0]["inputs"], observed)
    require_witness(log_dir, witness, position, prereg_sha256=prereg_sha256)
    return DiscoveryKey(_KEY_TOKEN, opened["inputs_frozen_sha256"], matching[0]["inputs"],
                        log_dir=log_dir, witness_tip=witness.tip)


def seal_discovery(log_dir: Path, now: datetime, key: DiscoveryKey, frozen: dict, *,
                   prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """Append ``discovery_frozen`` carrying the frozen discovery manifest's sha256."""
    if not isinstance(key, DiscoveryKey):
        raise PermissionError("sealing a discovery needs its v2 DiscoveryKey")
    payload = frozen.get("payload") or {}
    if digest(payload) != frozen.get("sha256"):
        raise ValueError("frozen discovery manifest does not hash to its sha256")
    if (payload.get("inputs") or {}).get("inputs_frozen_sha256") != key.inputs_frozen_sha256:
        raise PermissionError("the discovery manifest does not carry this key's inputs_frozen hash")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        opened = v1._kind(records, "discovery_opened")
        if len(opened) != 1 or opened[0]["inputs_frozen_sha256"] != key.inputs_frozen_sha256:
            raise PermissionError("no discovery_opened record for this key")
        if v1._kind(records, "discovery_frozen"):
            raise PermissionError("a discovery is already frozen (one shot)")
        return log.append_locked([{
            "kind": "discovery_frozen", "run_at": now.isoformat(), "prereg_sha256": prereg_sha256,
            "inputs_frozen_sha256": key.inputs_frozen_sha256, "discovery_sha256": frozen["sha256"],
            "calibration": payload["calibration"]["state"],
            "selected": [t["trial"] for t in payload["ledger"] if t["selected"]],
            "promotion_allowed": False,
        }])[0]


def open_holdout(frozen: dict, *, allow_holdout: bool, prereg_sha256: str, log_dir: Path, now: datetime,
                 observed: Mapping[str, Any], repo_root: Path = REPO) -> dict:
    """One-shot v2 holdout, step 1: flag + pinned hash + the chain, then ``holdout_opened``."""
    payload = check_holdout_request(frozen, allow_holdout=allow_holdout, prereg_sha256=prereg_sha256,
                                    repo_root=repo_root)
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        inputs = v1._holdout_inputs(records, frozen, payload)
        if v1._kind(records, "holdout_opened"):
            raise PermissionError("the holdout was already opened (evaluated once)")
        _check_observed(inputs, observed)
        log.append_locked([{
            "kind": "holdout_opened", "run_at": now.isoformat(), "prereg_sha256": prereg_sha256,
            "discovery_sha256": frozen["sha256"], "promotion_allowed": False,
        }])
        heads = v1._line_sha256(log)
    return {"kind": "holdout_opened", "records": len(heads), "head_sha256": heads[-1]}


def resume_holdout(frozen: dict, *, allow_holdout: bool, prereg_sha256: str, log_dir: Path,
                   observed: Mapping[str, Any], witness: OffhostWitness | None, repo_root: Path = REPO) -> HoldoutKey:
    """One-shot v2 holdout, step 2: the price key, once the v2 off-host log witnesses the opening."""
    payload = check_holdout_request(frozen, allow_holdout=allow_holdout, prereg_sha256=prereg_sha256,
                                    repo_root=repo_root)
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
    inputs = v1._holdout_inputs(records, frozen, payload)
    if v1._kind(records, "holdout_result"):
        raise PermissionError("a holdout result is already recorded (evaluated once)")
    position = v1._position(records, "holdout_opened")
    if records[position - 1]["discovery_sha256"] != frozen["sha256"]:
        raise PermissionError("holdout_opened names another discovery")
    _check_observed(inputs, observed)
    require_witness(log_dir, witness, position, prereg_sha256=prereg_sha256)
    return HoldoutKey(_HOLDOUT_TOKEN, frozen["sha256"], inputs, log_dir=log_dir, witness_tip=witness.tip)


def seal_holdout(log_dir: Path, now: datetime, key: HoldoutKey, result: dict, *,
                 prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """Append ``holdout_result`` (the result's sha256 and verdict state)."""
    if not isinstance(key, HoldoutKey) or result.get("discovery_manifest") != key.frozen_sha256:
        raise PermissionError("the holdout result does not belong to this key")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        if not any(r.get("discovery_sha256") == key.frozen_sha256 for r in v1._kind(records, "holdout_opened")):
            raise PermissionError("no holdout_opened record for this discovery")
        if v1._kind(records, "holdout_result"):
            raise PermissionError("a holdout result is already recorded")
        return log.append_locked([{
            "kind": "holdout_result", "run_at": now.isoformat(), "prereg_sha256": prereg_sha256,
            "discovery_sha256": key.frozen_sha256, "result_sha256": digest(result),
            "verdict": result["verdict"]["state"], "promotion_allowed": False,
        }])[0]


def write_frozen(output: Path, name: str, value: Any) -> None:
    Path(output).mkdir(parents=True, exist_ok=True)
    write_once(Path(output) / name, value)
