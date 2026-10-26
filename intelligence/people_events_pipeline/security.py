"""Point-in-time resolution of event issuers onto ``security_master.entity_id`` (GD1).

``people_events.security_id`` is BIGINT today while ``security_master.entity_id``
is TEXT (``sm_0000320193`` / ``sm_tkr_AAPL``). The ``people_events_v2``
migration (owner approval) turns ``security_id`` into TEXT with a foreign key
onto ``security_master(entity_id)``; until then this module only *reports*
what it would write.

Resolution order, per event (``event_date`` is the as-of date):

1. issuer CIK -> a ``cik`` identifier. A CIK is never reassigned to another
   registrant, so a CIK match is accepted regardless of the identifier's
   ``valid_from`` (the GD1 seed dates every row at its own seed date, which
   would otherwise reject all history).
2. ticker -> a ``ticker`` identifier valid on ``event_date``
   (``valid_from <= event_date <= valid_to``). Tickers *are* reused, so the
   window is enforced. A ticker that only matches outside its window is
   reported as ``ticker_outside_validity`` (the survivorship/backcast case)
   and NOT written.

Ties (more than one entity) prefer ``is_primary``, then the latest
``valid_from``, then the smallest ``entity_id``: deterministic, as
``intelligence.security_master.resolve_entity`` orders them. Identifiers with
``conflict_flag`` still resolve but are counted.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from intelligence.people_events_pipeline import rules as R

IDENTIFIER_COLUMNS = ["entity_id", "id_scheme", "id_value", "valid_from", "valid_to", "is_primary", "conflict_flag"]

MATCH_BASES = ("cik", "ticker", "ticker_outside_validity", "not_an_issuer", "unmatched")


def _prepare(identifiers: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ids = identifiers[IDENTIFIER_COLUMNS].copy()
    ids["valid_from"] = pd.to_datetime(ids["valid_from"]).dt.date
    ids["valid_to"] = pd.to_datetime(ids["valid_to"]).dt.date
    ids["is_primary"] = ids["is_primary"].fillna(False).astype(bool)
    ids["conflict_flag"] = ids["conflict_flag"].fillna(False).astype(bool)
    ciks = ids[ids["id_scheme"] == "cik"].copy()
    ciks["cik"] = ciks["id_value"].map(R.cik_int)
    ciks = ciks.dropna(subset=["cik"])
    tkrs = ids[ids["id_scheme"] == "ticker"].copy()
    tkrs["tkr"] = R.normalize_ticker_series(tkrs["id_value"])
    tkrs = tkrs.dropna(subset=["tkr"])
    return ciks, tkrs


def _pick(matches: pd.DataFrame) -> pd.DataFrame:
    """One entity per event row: primary first, latest valid_from, smallest entity_id."""
    m = matches.assign(_p=~matches["is_primary"], _vf=matches["valid_from"].map(lambda d: -d.toordinal()))
    m = m.sort_values(["_row", "_p", "_vf", "entity_id"], kind="mergesort")
    return m.drop_duplicates("_row")


def resolve_securities(events: pd.DataFrame, identifiers: pd.DataFrame) -> pd.DataFrame:
    """Return ``events`` with ``security_id``, ``security_match_basis`` and ``security_conflict`` added."""
    out = events.copy()
    out["security_id"] = None
    out["security_match_basis"] = "unmatched"
    out["security_conflict"] = False
    if out.empty:
        return out
    issuer = out["entity_kind"].fillna("issuer") == "issuer"
    out.loc[~issuer, "security_match_basis"] = "not_an_issuer"
    if identifiers is None or identifiers.empty:
        return out
    ciks, tkrs = _prepare(identifiers)
    rows = out[issuer].assign(_row=out.index[issuer])

    # 1. CIK, validity not enforced (CIKs are permanent).
    by_cik = rows.dropna(subset=["entity_cik"])[["_row", "entity_cik"]]
    if not by_cik.empty and not ciks.empty:
        by_cik = by_cik.assign(cik=by_cik["entity_cik"].astype("int64"))
        hit = _pick(by_cik.merge(ciks, on="cik", how="inner"))
        out.loc[hit["_row"], "security_id"] = hit["entity_id"].to_numpy()
        out.loc[hit["_row"], "security_match_basis"] = "cik"
        out.loc[hit["_row"], "security_conflict"] = hit["conflict_flag"].to_numpy()

    # 2. Ticker within its validity window.
    todo = rows[out.loc[rows.index, "security_id"].isna().to_numpy()]
    by_tkr = todo.dropna(subset=["entity_ticker"])[["_row", "entity_ticker", "event_date"]]
    if not by_tkr.empty and not tkrs.empty:
        cand = by_tkr.merge(tkrs, left_on="entity_ticker", right_on="tkr", how="inner")
        on = pd.to_datetime(cand["event_date"])
        vt = pd.to_datetime(cand["valid_to"])
        inside = (pd.to_datetime(cand["valid_from"]) <= on) & (vt.isna() | (on <= vt))
        hit = _pick(cand[inside.to_numpy()]) if inside.any() else cand.iloc[0:0]
        if not hit.empty:
            out.loc[hit["_row"], "security_id"] = hit["entity_id"].to_numpy()
            out.loc[hit["_row"], "security_match_basis"] = "ticker"
            out.loc[hit["_row"], "security_conflict"] = hit["conflict_flag"].to_numpy()
        outside = set(cand["_row"]) - set(hit["_row"])
        if outside:
            out.loc[sorted(outside), "security_match_basis"] = "ticker_outside_validity"
    return out


def match_summary(resolved: pd.DataFrame) -> dict[str, Any]:
    """Match rate per channel: share of issuer events with a written security_id, plus the basis mix."""
    out: dict[str, Any] = {}
    for channel, g in resolved.groupby("channel", sort=True):
        issuers = g[g["security_match_basis"] != "not_an_issuer"]
        n = int(len(issuers))
        matched = int(issuers["security_id"].notna().sum())
        out[channel] = {
            "issuer_events": n,
            "matched": matched,
            "match_rate": round(matched / n, 6) if n else None,
            "by_basis": {k: int(v) for k, v in g["security_match_basis"].value_counts().sort_index().items()},
            "conflict_flagged": int(g["security_conflict"].sum()),
            "distinct_entities": int(g["security_id"].dropna().nunique()),
        }
    return out
