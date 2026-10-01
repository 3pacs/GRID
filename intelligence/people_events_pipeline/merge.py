"""Candidates -> canonical events: one row per (channel, dedup_key).

Rules (design doc section 2.3):

* ``known_at`` = the minimum across the act's candidates. Each candidate's
  known_at is a valid upper bound on the public-knowable instant, so their
  minimum is too, and it is the tightest one available. ``known_at_basis``
  follows the candidate that supplied it (ties: the stronger basis).
* Descriptive fields (actor, entity, direction, size, event date) come from
  an original filing before any amendment (``/A``), then the most
  authoritative source (``adapters.PRECEDENCE``), ties broken by
  ``source_record_id`` so the result never depends on input order. An
  amendment's content is never shown at the original's known_at: an
  amendment that repeats the line only adds a source; one that changes it
  lands under a different key (flagged near-duplicate; supersession by
  amendment is owner decision D6).
* ``n_sources`` counts distinct source *systems* (``sec_form345``,
  ``edgar_native``, ``quiverquant`` ...), not rows: an original Form 4 and its
  amendment repeating the same line are one source. ``n_source_rows`` keeps
  the row count and ``source_refs`` every contributing row.
* ``near_duplicate`` is a batch-level audit flag computed with hindsight
  (siblings known later count); it must never be used as a point-in-time
  filter.
* Near-duplicates -- events sharing a ``loose_key`` (the dedup key minus its
  size component; stored as the act group) but not a dedup key, and coming
  from more than one filing or record -- are flagged and counted, never
  merged: two purchases by one insider on one day can be two acts.
* Echoes (``link_echoes``): a row in an echo channel (news) that matches an
  earlier-known event of another channel on the same entity inside a window
  is linked ``echo_of`` and does not count as an independent event.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from intelligence.people_events_pipeline import rules as R
from intelligence.people_events_pipeline.adapters import CANDIDATE_COLUMNS

EVENT_COLUMNS = [
    "channel", "dedup_key", "loose_key", "event_date", "known_at", "known_at_basis",
    "actor_id", "actor_id_basis", "actor_type", "actor_name", "co_actor_ids",
    "entity_ticker", "entity_cik", "entity_kind", "direction", "transaction_code", "size_usd",
    "source", "source_record_id", "source_refs", "sources", "n_sources", "n_source_rows",
    "accession", "document_type", "near_duplicate", "content_hash", "attrs",
]

# What a correction must change to be a new version. Identity fields
# (actor_id, entity_cik) are excluded: a better source naming the same person
# by CIK instead of by name is an identity enrichment of the same act, not a
# correction. Size is part of the act only where the key does not already pin
# it (13F position changes, contract awards); for Form 4 the key carries the
# share count and sources legitimately differ on price precision.
CONTENT_FIELDS = ("channel", "dedup_key", "event_date", "entity_ticker", "direction", "transaction_code")
SIZE_IN_CONTENT = frozenset({"thirteen_f", "gov_contract"})


@dataclass
class MergeResult:
    events: pd.DataFrame
    stats: dict[str, Any] = field(default_factory=dict)


def _ref(frame: pd.DataFrame) -> pd.Series:
    return frame["source"].astype(str) + "|" + frame["source_record_id"].astype(str)


def content_hash_series(events: pd.DataFrame) -> pd.Series:
    """sha256 over the act's content (``CONTENT_FIELDS``). A change means a new version, not an edit."""
    size = events["size_usd"].map(lambda v: "" if v is None or pd.isna(v) else f"{float(v):.2f}")
    size = size.where(events["channel"].isin(SIZE_IN_CONTENT), "")
    parts = [
        events["channel"].astype(str), events["dedup_key"].astype(str), events["event_date"].astype(str),
        events["entity_ticker"].fillna("").astype(str), events["direction"].fillna("").astype(str),
        events["transaction_code"].fillna("").astype(str), size,
    ]
    joined = parts[0]
    for p in parts[1:]:
        joined = joined + "\x1f" + p
    return joined.map(lambda s: hashlib.sha256(s.encode()).hexdigest())


def merge_candidates(candidates: pd.DataFrame) -> MergeResult:
    """Collapse candidates to canonical events. Deterministic for any input row order."""
    if candidates.empty:
        return MergeResult(pd.DataFrame(columns=EVENT_COLUMNS), {"total": {"candidates": 0, "events": 0}})
    c = candidates[CANDIDATE_COLUMNS].copy()
    c["_ref"] = _ref(c)
    c["_basis_rank"] = c["known_at_basis"].map(R.BASIS_RANK).fillna(9)
    c["_amend"] = c["document_type"].fillna("").astype(str).str.endswith("/A").astype(int)
    keys = ["channel", "dedup_key"]

    # known_at: earliest valid bound, then stronger basis, then a stable tiebreak.
    by_known = c.sort_values(keys + ["known_at", "_basis_rank", "precedence", "_ref"], kind="mergesort")
    known = by_known.drop_duplicates(keys)[keys + ["known_at", "known_at_basis"]]

    # Descriptive fields: most authoritative source first.
    by_prec = c.sort_values(keys + ["_amend", "precedence", "_ref"], kind="mergesort")
    rep = by_prec.drop_duplicates(keys).drop(columns=["known_at", "known_at_basis"])

    counts = c.groupby(keys, sort=False).agg(n_source_rows=("_ref", "size"), n_sources=("source", "nunique"))
    events = rep.merge(known, on=keys, how="left").merge(counts.reset_index(), on=keys, how="left")

    events["source_refs"] = events["_ref"].map(lambda r: [r])
    events["sources"] = events["source"].map(lambda s: [s])
    multi_keys = counts[counts["n_source_rows"] > 1].reset_index()[keys]
    if not multi_keys.empty:
        m = c.merge(multi_keys, on=keys, how="inner")
        refs = m.groupby(keys)["_ref"].agg(lambda s: sorted(set(s)))
        srcs = m.groupby(keys)["source"].agg(lambda s: sorted(set(s)))
        co = m.groupby(keys)["co_actor_ids"].agg(
            lambda s: ",".join(sorted({x for v in s if isinstance(v, str) and v for x in v.split(",")})))
        idx = pd.MultiIndex.from_frame(events[keys])
        has = idx.isin(refs.index)
        events.loc[has, "source_refs"] = pd.Series(refs.reindex(idx[has]).to_list(), index=events.index[has])
        events.loc[has, "sources"] = pd.Series(srcs.reindex(idx[has]).to_list(), index=events.index[has])
        events.loc[has, "co_actor_ids"] = co.reindex(idx[has]).to_numpy()
        # A flag one source knows and another lacks (the SEC data set carries no
        # 10b5-1 column) must not be lost to the representative's None: take the
        # first known value, preferring the most authoritative source.
        flags = (m.sort_values(keys + ["precedence", "_ref"], kind="mergesort")
                 .assign(_f=lambda d: d["attrs"].map(lambda a: a.get("is_10b5_1") if isinstance(a, dict) else None))
                 .dropna(subset=["_f"]).drop_duplicates(keys).set_index(keys)["_f"])
        if not flags.empty:
            for pos in [i for i, k in enumerate(idx) if k in flags.index]:
                row = events.index[pos]
                attrs = dict(events.at[row, "attrs"] or {})
                if attrs.get("is_10b5_1") is None:
                    attrs["is_10b5_1"] = bool(flags.loc[idx[pos]])
                    events.at[row, "attrs"] = attrs
        conflicts = m.groupby(keys).agg(d=("direction", lambda s: s.dropna().nunique()),
                                        e=("event_date", "nunique"))
    else:
        conflicts = pd.DataFrame({"d": [], "e": []})

    # Near-duplicate: same loose key (act group: issuer, actor, date, code)
    # under more than one dedup key AND from more than one filing/record.
    # Several lines of ONE filing (a sale executed in lots at different
    # prices) are separate acts of one act group, not suspected duplicates.
    origin = events["accession"].where(events["accession"].notna(), events["source_record_id"])
    grp = events.assign(_o=origin).groupby(["channel", "loose_key"])
    events["near_duplicate"] = (grp["dedup_key"].transform("nunique") > 1) & (grp["_o"].transform("nunique") > 1)
    events["content_hash"] = content_hash_series(events)
    events = events.sort_values(keys, kind="mergesort").reset_index(drop=True)
    events = events[EVENT_COLUMNS]
    return MergeResult(events, _stats(c, events, conflicts))


def _stats(c: pd.DataFrame, events: pd.DataFrame, conflicts: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for channel, ev in events.groupby("channel", sort=True):
        cand = c[c["channel"] == channel]
        n_c, n_e = int(len(cand)), int(len(ev))
        pairs = Counter("+".join(s) for s in ev.loc[ev["n_sources"] > 1, "sources"])
        out[channel] = {
            "candidates": n_c,
            "events": n_e,
            "dedup_rate": round(1 - n_e / n_c, 6) if n_c else 0.0,
            "candidates_by_source": {k: int(v) for k, v in cand["source"].value_counts().sort_index().items()},
            "events_multi_row": int((ev["n_source_rows"] > 1).sum()),
            "events_multi_source": int((ev["n_sources"] > 1).sum()),
            "multi_source_combinations": dict(sorted(pairs.items())),
            "n_sources_hist": {str(k): int(v) for k, v in ev["n_sources"].value_counts().sort_index().items()},
            "known_at_basis": {k: int(v) for k, v in ev["known_at_basis"].value_counts().sort_index().items()},
            "near_duplicate_events": int(ev["near_duplicate"].sum()),
            "near_duplicate_groups_with_amendment": _amended_near_dup_groups(ev),
            "by_transaction_code": {str(k): int(v) for k, v in
                                    ev["transaction_code"].fillna("none").value_counts().head(15).items()},
            "by_direction": {str(k): int(v) for k, v in ev["direction"].fillna("none").value_counts().items()},
            "events_by_known_year": {str(k): int(v) for k, v in
                                     pd.to_datetime(ev["known_at"], utc=True).dt.year.value_counts().sort_index().items()},
            "event_date_min": str(min(ev["event_date"])) if n_e else None,
            "event_date_max": str(max(ev["event_date"])) if n_e else None,
            "known_at_min": ev["known_at"].min().isoformat() if n_e else None,
            "known_at_max": ev["known_at"].max().isoformat() if n_e else None,
        }
    out["total"] = {
        "candidates": int(len(c)),
        "events": int(len(events)),
        "dedup_rate": round(1 - len(events) / len(c), 6) if len(c) else 0.0,
        "direction_conflicts": int((conflicts["d"] > 1).sum()) if len(conflicts) else 0,
        "event_date_conflicts": int((conflicts["e"] > 1).sum()) if len(conflicts) else 0,
    }
    return out


def _amended_near_dup_groups(ev: pd.DataFrame) -> dict[str, int]:
    """Near-duplicate act groups, split by whether any member came from an amendment (``/A``).

    An amendment that changes a line's share count produces a second act
    under a new key; these groups are the candidates for a future
    amendment-supersession rule (design doc section 2.3, owner decision).
    """
    nd = ev[ev["near_duplicate"]]
    if nd.empty:
        return {"groups": 0, "with_amendment": 0}
    amended = nd["document_type"].fillna("").astype(str).str.contains("/A", regex=False)
    g = amended.groupby(nd["loose_key"]).any()
    return {"groups": int(len(g)), "with_amendment": int(g.sum())}


def pit_violations(events: pd.DataFrame, observed_at: pd.Timestamp | None = None) -> dict[str, int]:
    """Invariants every canonical event must satisfy (E1 look-ahead canary, data side).

    * ``known_before_event``: known_at earlier than the start of the act's own
      date. An act cannot be public before it happened.
    * ``known_after_observation``: known_at later than the time this run
      observed its inputs -- a bound in the future is not "known".
    * ``missing_known_at``: should be impossible (adapters drop such rows).
    """
    if events.empty:
        return {"known_before_event": 0, "known_before_event_by_channel": {}, "known_after_observation": 0,
                "missing_known_at": 0}
    start = pd.to_datetime(events["event_date"]).dt.tz_localize("UTC")
    known = pd.to_datetime(events["known_at"], utc=True)
    early = known < start
    out = {
        "known_before_event": int(early.sum()),
        "known_before_event_by_channel": {str(k): int(v) for k, v in
                                          events.loc[early.to_numpy(), "channel"].value_counts().items()},
        "missing_known_at": int(known.isna().sum()),
        "known_after_observation": 0,
    }
    if observed_at is not None:
        out["known_after_observation"] = int((known > pd.Timestamp(observed_at)).sum())
    return out


def link_echoes(events: pd.DataFrame, echo_channel: str = "news", window: pd.Timedelta = pd.Timedelta(days=3)
                ) -> pd.Series:
    """For rows of ``echo_channel``: the dedup key of the earliest-known event of another channel
    on the same entity whose known_at is in ``[echo.known_at - window, echo.known_at]``.

    Returns a Series aligned to ``events`` (None where not an echo). The
    candidate primary must be known *no later than* the echo: an echo
    cannot precede the thing it repeats.
    """
    out = pd.Series([None] * len(events), index=events.index, dtype="object")
    if events.empty:
        return out
    echoes = events[(events["channel"] == echo_channel) & events["entity_ticker"].notna()]
    primary = events[(events["channel"] != echo_channel) & events["entity_ticker"].notna()]
    if echoes.empty or primary.empty:
        return out
    prim = primary.sort_values(["known_at", "dedup_key"], kind="mergesort")
    for idx, row in echoes.iterrows():
        cand = prim[(prim["entity_ticker"] == row["entity_ticker"])
                    & (prim["known_at"] <= row["known_at"])
                    & (prim["known_at"] >= row["known_at"] - window)]
        if not cand.empty:
            out.loc[idx] = cand.iloc[0]["dedup_key"]
    return out


def numpy_safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value
