"""Canonical events vs. rows already stored -> an append-only write plan.

Storage model (design doc section 5; schema in the ``people_events_v2``
migration): ``people_events`` is append-only. A stored row's content is never
edited in place. What a run may do:

``insert``            a new act (no current row for ``(channel, dedup_key)``).
``add_sources``       another source row describing an act already stored:
                      ``source_refs``/``n_sources`` grow. Content unchanged.
``tighten_known_at``  a new source gives an *earlier* valid upper bound for an
                      act already stored. known_at only ever moves earlier.
``enrich_identity``   a stronger identity for the same act (a name-keyed actor
                      now known by CIK, an issuer CIK now known). Content and
                      known_at unchanged.
``supersede``         the same key now describes different content (a source
                      corrected the act). The stored row gets
                      ``superseded_at = observed_at`` and a new version row is
                      inserted with ``known_at = observed_at`` (basis
                      ``first_seen``).
``retract``           the act vanished from a source scope that was scanned
                      in full (``complete_channels``). ``retracted_at =
                      observed_at``; the row stays.
``actor_conflict``    the same key now names a *different* strongly-identified
                      actor (two owner CIKs). Nothing is written; counted for
                      review.
``unchanged``         nothing to do -- the idempotency case.

The version floor (the one-visible-version invariant)
------------------------------------------------------
For a key with earlier versions (superseded or retracted), no current row may
become visible before the last of those ended:
``floor = max(superseded_at, retracted_at)`` over the key's non-current rows.
``insert`` (a re-appearing act) and ``tighten_known_at`` (a superseding
version later matched by an earlier source) are clamped to the floor, so at
every ``as_of`` at most one version of an act is visible. The database
enforces the same rule by trigger (``people_events_v2``).

Running the same plan twice gives only ``unchanged`` the second time.

A materializer *rule* change that alters content is not a supersession: it
bumps ``rules.KEY_VERSION`` (a new key namespace) and is rebuilt side by side,
so history built under the old rules stays reproducible.
"""

from __future__ import annotations

import json
from collections import Counter
from typing import Any, Iterable

import pandas as pd

from intelligence.people_events_pipeline.rules import STRONG_ACTOR_BASES

OPS = ("insert", "add_sources", "tighten_known_at", "enrich_identity", "supersede", "retract", "actor_conflict",
       "report_only", "unchanged")

# Channels the dry run reports but no run writes. QuiverQuant's quarterly
# contract totals are published while the quarter runs and grow: keyed with the
# amount, every snapshot would stay current and a sum would double count;
# keyed without it, a later amount would show at an earlier known_at. They
# stay out of the write path until amounts are versioned (owner decision D13).
REPORT_ONLY_CHANNELS = frozenset({"gov_contract_qq_aggregate"})

STORED_COLUMNS = ["channel", "dedup_key", "known_at", "known_at_basis", "source_refs", "content_hash",
                  "actor_id", "actor_id_basis", "entity_cik", "superseded_at", "retracted_at"]


def _refs(value: Any) -> frozenset[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return frozenset()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return frozenset({value})
    out = set()
    for v in value:
        if isinstance(v, dict):
            out.add(f"{v.get('source')}|{v.get('source_record_id')}")
        else:
            out.add(str(v))
    return frozenset(out)


def _ts(value: Any) -> pd.Timestamp | None:
    if value is None or (not isinstance(value, (list, tuple)) and pd.isna(value)):
        return None
    t = pd.Timestamp(value)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _normalize(stored: pd.DataFrame) -> pd.DataFrame:
    s = stored.copy()
    for col in STORED_COLUMNS:
        if col not in s.columns:
            s[col] = None
    return s


def current_rows(stored: pd.DataFrame) -> pd.DataFrame:
    """The current version of every key: not superseded, not retracted."""
    if stored.empty:
        return stored
    s = _normalize(stored)
    return s[s["superseded_at"].isna() & s["retracted_at"].isna()]


def version_floors(stored: pd.DataFrame) -> dict[tuple[str, str], pd.Timestamp]:
    """Per key, the instant its last non-current version stopped being visible."""
    if stored.empty:
        return {}
    s = _normalize(stored)
    floors: dict[tuple[str, str], pd.Timestamp] = {}
    for r in s.to_dict("records"):
        ends = [t for t in (_ts(r["superseded_at"]), _ts(r["retracted_at"])) if t is not None]
        if not ends:
            continue
        key = (r["channel"], r["dedup_key"])
        end = max(ends)
        floors[key] = max(floors.get(key, end), end)
    return floors


def _strong(basis: Any) -> bool:
    return isinstance(basis, str) and basis in STRONG_ACTOR_BASES


def build_write_plan(events: pd.DataFrame, stored: pd.DataFrame, observed_at: pd.Timestamp,
                     complete_channels: Iterable[str] = ()) -> pd.DataFrame:
    """One row per affected key: ``op`` plus the values the writer needs.

    ``stored`` may lack ``content_hash`` (today's schema has none); such rows
    are treated as content-equal so a first run never supersedes legacy rows
    on a hash it cannot compute.

    ``complete_channels`` asserts that ``events`` holds the FULL source scope
    of those channels (no ``--since`` window, no row limit, no skipped
    quarter); only then is an absent key a retraction. Never pass it from a
    windowed or limited read.
    """
    observed_at = _ts(observed_at)
    report_only = events["channel"].isin(REPORT_ONLY_CHANNELS) if not events.empty else None
    held = pd.DataFrame()
    if report_only is not None and report_only.any():
        held = pd.DataFrame({"op": "report_only", "channel": events.loc[report_only, "channel"].to_numpy(),
                             "dedup_key": events.loc[report_only, "dedup_key"].to_numpy()})
        events = events[~report_only.to_numpy()]
    out = _build(events, stored, observed_at, complete_channels)
    return pd.concat([out, held], ignore_index=True) if not held.empty else out


def _build(events: pd.DataFrame, stored: pd.DataFrame, observed_at: pd.Timestamp,
           complete_channels: Iterable[str]) -> pd.DataFrame:
    if stored.empty:
        # Fast path (a first load, e.g. the GD3 backfill): every event is an insert.
        if events.empty:
            return pd.DataFrame(columns=["op", "channel", "dedup_key"])
        return pd.DataFrame({
            "op": "insert", "channel": events["channel"].to_numpy(), "dedup_key": events["dedup_key"].to_numpy(),
            "known_at": events["known_at"].to_numpy(), "known_at_basis": events["known_at_basis"].to_numpy(),
            "content_hash": events["content_hash"].to_numpy(),
            "source_refs": [sorted(set(r)) for r in events["source_refs"]], "prev_known_at": None,
            "actor_id": events["actor_id"].to_numpy(), "actor_id_basis": events["actor_id_basis"].to_numpy(),
            "entity_cik": events["entity_cik"].to_numpy(),
        })
    cur_by_key = {(r["channel"], r["dedup_key"]): r for r in current_rows(stored).to_dict("records")}
    floors = version_floors(stored)
    plan: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for ev in events.to_dict("records"):
        key = (ev["channel"], ev["dedup_key"])
        seen.add(key)
        new_refs = frozenset(ev["source_refs"])
        ev_known = _ts(ev["known_at"])
        floor = floors.get(key)
        old = cur_by_key.get(key)
        if old is None:
            known, basis = ev_known, ev["known_at_basis"]
            if floor is not None and known < floor:
                known, basis = floor, "first_seen"  # a re-appearing act is new to readers from the floor on
            plan.append({"op": "insert", "channel": key[0], "dedup_key": key[1], "known_at": known,
                         "known_at_basis": basis, "content_hash": ev["content_hash"],
                         "source_refs": sorted(new_refs), "prev_known_at": None,
                         "act_known_at": ev_known if known != ev_known else None,
                         "actor_id": ev.get("actor_id"), "actor_id_basis": ev.get("actor_id_basis"),
                         "entity_cik": ev.get("entity_cik")})
            continue
        if _strong(old.get("actor_id_basis")) and _strong(ev.get("actor_id_basis")) \
                and str(old.get("actor_id")) != str(ev.get("actor_id")):
            plan.append({"op": "actor_conflict", "channel": key[0], "dedup_key": key[1],
                         "stored_actor_id": old.get("actor_id"), "new_actor_id": ev.get("actor_id")})
            continue
        old_hash = old.get("content_hash")
        if isinstance(old_hash, str) and old_hash and old_hash != ev["content_hash"]:
            plan.append({"op": "supersede", "channel": key[0], "dedup_key": key[1], "known_at": observed_at,
                         "known_at_basis": "first_seen", "content_hash": ev["content_hash"],
                         "source_refs": sorted(new_refs | _refs(old.get("source_refs"))),
                         "prev_known_at": old["known_at"], "act_known_at": ev_known,
                         "actor_id": ev.get("actor_id"), "actor_id_basis": ev.get("actor_id_basis"),
                         "entity_cik": ev.get("entity_cik")})
            continue
        old_refs = _refs(old.get("source_refs"))
        old_known = _ts(old["known_at"])
        target = min(ev_known, old_known)
        if floor is not None:
            target = max(target, floor)
        ops = []
        if target < old_known:
            ops.append("tighten_known_at")
        if not new_refs <= old_refs:
            ops.append("add_sources")
        if not _strong(old.get("actor_id_basis")) and _strong(ev.get("actor_id_basis")):
            ops.append("enrich_identity")
        if not ops:
            plan.append({"op": "unchanged", "channel": key[0], "dedup_key": key[1]})
            continue
        for op in ops:
            plan.append({
                "op": op, "channel": key[0], "dedup_key": key[1], "known_at": target,
                # A floor-clamped tighten is the floor instant, not the source's own
                # bound, so it is labelled first_seen, never the source's basis.
                "known_at_basis": (ev["known_at_basis"] if target == ev_known else "first_seen")
                if op == "tighten_known_at" else old["known_at_basis"],
                "content_hash": ev["content_hash"], "source_refs": sorted(new_refs | old_refs),
                "prev_known_at": old_known, "actor_id": ev.get("actor_id"),
                "actor_id_basis": ev.get("actor_id_basis"), "entity_cik": ev.get("entity_cik"),
            })
    complete = set(complete_channels)
    for key, old in sorted(cur_by_key.items()):
        if key[0] in complete and key not in seen:
            plan.append({"op": "retract", "channel": key[0], "dedup_key": key[1], "known_at": old["known_at"],
                         "retracted_at": observed_at})
    frame = pd.DataFrame(plan)
    if frame.empty:
        frame = pd.DataFrame(columns=["op", "channel", "dedup_key"])
    return frame


def plan_counts(plan: pd.DataFrame) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    if plan.empty:
        return out
    for (channel, op), n in Counter(zip(plan["channel"], plan["op"])).items():
        out.setdefault(channel, {o: 0 for o in OPS})[op] = int(n)
    return dict(sorted(out.items()))


def apply_in_memory(stored: pd.DataFrame, plan: pd.DataFrame, observed_at: pd.Timestamp) -> pd.DataFrame:
    """What the database would hold after applying ``plan``: the writer's contract, in pandas.

    Used by tests (idempotency, append-only, one visible version per key)
    and mirrored statement-for-statement by the writer that ships with the
    migration.
    """
    observed_at = _ts(observed_at)
    rows = [] if stored.empty else _normalize(stored).to_dict("records")

    def current(channel: str, key: str) -> dict[str, Any] | None:
        for r in rows:
            if r["channel"] == channel and r["dedup_key"] == key and _ts(r["superseded_at"]) is None \
                    and _ts(r["retracted_at"]) is None:
                return r
        return None

    def new_row(p: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
        row = {"channel": p["channel"], "dedup_key": p["dedup_key"], "known_at": p["known_at"],
               "known_at_basis": p["known_at_basis"], "source_refs": list(p["source_refs"]),
               "content_hash": p["content_hash"], "actor_id": p.get("actor_id"),
               "actor_id_basis": p.get("actor_id_basis"), "entity_cik": p.get("entity_cik"),
               "superseded_at": None, "retracted_at": None}
        row.update(extra or {})
        return row

    for p in plan.to_dict("records"):
        op = p["op"]
        if op in ("unchanged", "actor_conflict", "report_only"):
            continue
        if op == "insert":
            if current(p["channel"], p["dedup_key"]) is not None:
                raise ValueError("insert for a key that already has a current row")
            rows.append(new_row(p))
            continue
        cur = current(p["channel"], p["dedup_key"])
        if cur is None:
            raise ValueError(f"plan op {op} for a key with no current row: {p['channel']} {p['dedup_key']}")
        if op == "supersede":
            cur["superseded_at"] = observed_at
            rows.append(new_row(p))
        elif op == "tighten_known_at":
            if _ts(p["known_at"]) > _ts(cur["known_at"]):
                raise ValueError("known_at may only move earlier")
            cur["known_at"], cur["known_at_basis"] = p["known_at"], p["known_at_basis"]
        elif op == "add_sources":
            cur["source_refs"] = sorted(set(cur["source_refs"]) | set(p["source_refs"]))
        elif op == "enrich_identity":
            if _strong(cur.get("actor_id_basis")):
                raise ValueError("identity is already strong")
            cur["actor_id"], cur["actor_id_basis"] = p["actor_id"], p["actor_id_basis"]
            if cur.get("entity_cik") is None or pd.isna(cur.get("entity_cik")):
                cur["entity_cik"] = p.get("entity_cik")
        elif op == "retract":
            cur["retracted_at"] = observed_at
    return pd.DataFrame(rows)


def visible_at(stored: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    """PIT read of the append-only store: the version a reader at ``as_of`` may use.

    ``known_at <= as_of`` and not yet superseded/retracted at ``as_of``. This
    is the predicate ``store.people_events.read_events`` adopts with the
    migration.
    """
    if stored.empty:
        return stored
    s = _normalize(stored)
    as_of = _ts(as_of)
    known = pd.to_datetime(s["known_at"], utc=True)
    sup = pd.to_datetime(s["superseded_at"], utc=True)
    ret = pd.to_datetime(s["retracted_at"], utc=True)
    mask = (known <= as_of) & (sup.isna() | (sup > as_of)) & (ret.isna() | (ret > as_of))
    return stored[mask.to_numpy()]


def max_visible_versions(stored: pd.DataFrame, instants: Iterable[pd.Timestamp]) -> int:
    """The largest number of versions of any one act visible at any of ``instants`` (must be <= 1)."""
    worst = 0
    for t in instants:
        v = visible_at(stored, t)
        if not v.empty:
            worst = max(worst, int(v.groupby(["channel", "dedup_key"]).size().max()))
    return worst
