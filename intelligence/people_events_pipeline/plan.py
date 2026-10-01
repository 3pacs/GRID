"""Canonical events vs. rows already stored -> an append-only write plan.

Storage model (design doc section 4; schema in the ``people_events_v2``
migration): ``people_events`` is append-only. A stored row's descriptive
content is never edited in place. What a run may do:

``insert``            a new act (new ``(channel, dedup_key)``).
``add_sources``       another source row describing an act already stored:
                      ``source_refs``/``n_sources`` grow. Content unchanged.
``tighten_known_at``  a new source gives an *earlier* valid upper bound for an
                      act already stored. known_at only ever moves earlier
                      (the minimum of valid bounds is a valid bound); the old
                      value goes to the revision log.
``supersede``         the same key now describes different content (a source
                      corrected the act). The stored row gets
                      ``superseded_at = observed_at`` and a new version row is
                      inserted with ``known_at = observed_at`` (basis
                      ``first_seen``): the corrected content was not knowable
                      to GRID before this run, and exactly one version is
                      visible at any ``as_of``.
``retract``           the act vanished from a source scope that was scanned
                      in full (``complete_channels``). ``retracted_at =
                      observed_at``; the row stays.
``unchanged``         nothing to do -- the idempotency case.

Running the same plan twice gives only ``unchanged`` the second time
(``tests/test_people_events_pipeline.py`` proves it with ``apply_in_memory``).

A materializer *rule* change that alters content is not a supersession: it
bumps ``rules.KEY_VERSION`` (a new key namespace) and is rebuilt side by side,
so history built under the old rules stays reproducible.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

import pandas as pd

OPS = ("insert", "add_sources", "tighten_known_at", "supersede", "retract", "unchanged")

STORED_COLUMNS = ["channel", "dedup_key", "known_at", "known_at_basis", "source_refs", "content_hash",
                  "superseded_at", "retracted_at"]


def _refs(value: Any) -> frozenset[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return frozenset()
    if isinstance(value, str):
        import json

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


def current_rows(stored: pd.DataFrame) -> pd.DataFrame:
    """The visible version of every key: not superseded, not retracted."""
    if stored.empty:
        return stored
    s = stored
    for col in ("superseded_at", "retracted_at"):
        if col not in s.columns:
            s = s.assign(**{col: pd.NaT})
    return s[s["superseded_at"].isna() & s["retracted_at"].isna()]


def build_write_plan(events: pd.DataFrame, stored: pd.DataFrame, observed_at: pd.Timestamp,
                     complete_channels: Iterable[str] = ()) -> pd.DataFrame:
    """One row per affected key: ``op`` plus the values the writer needs.

    ``stored`` may lack ``content_hash`` (today's schema has none); such rows
    are treated as content-equal so a first run never supersedes legacy rows
    on a hash it cannot compute.
    """
    observed_at = pd.Timestamp(observed_at)
    if stored.empty:
        # Fast path (a first load, e.g. the GD3 backfill): every event is an insert.
        if events.empty:
            return pd.DataFrame(columns=["op", "channel", "dedup_key"])
        return pd.DataFrame({
            "op": "insert", "channel": events["channel"].to_numpy(), "dedup_key": events["dedup_key"].to_numpy(),
            "known_at": events["known_at"].to_numpy(), "known_at_basis": events["known_at_basis"].to_numpy(),
            "content_hash": events["content_hash"].to_numpy(),
            "source_refs": [sorted(set(r)) for r in events["source_refs"]], "prev_known_at": None,
        })
    cur = current_rows(stored)
    cur_by_key = {(r["channel"], r["dedup_key"]): r for r in cur.to_dict("records")}
    plan: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for ev in events.to_dict("records"):
        key = (ev["channel"], ev["dedup_key"])
        seen.add(key)
        new_refs = frozenset(ev["source_refs"])
        old = cur_by_key.get(key)
        if old is None:
            plan.append({"op": "insert", "channel": key[0], "dedup_key": key[1], "known_at": ev["known_at"],
                         "known_at_basis": ev["known_at_basis"], "content_hash": ev["content_hash"],
                         "source_refs": sorted(new_refs), "prev_known_at": None})
            continue
        old_hash = old.get("content_hash")
        if isinstance(old_hash, str) and old_hash and old_hash != ev["content_hash"]:
            plan.append({"op": "supersede", "channel": key[0], "dedup_key": key[1], "known_at": observed_at,
                         "known_at_basis": "first_seen", "content_hash": ev["content_hash"],
                         "source_refs": sorted(new_refs | _refs(old.get("source_refs"))),
                         "prev_known_at": old["known_at"], "act_known_at": ev["known_at"]})
            continue
        old_refs = _refs(old.get("source_refs"))
        old_known = pd.Timestamp(old["known_at"])
        ops = []
        if pd.Timestamp(ev["known_at"]) < old_known:
            ops.append("tighten_known_at")
        if not new_refs <= old_refs:
            ops.append("add_sources")
        if not ops:
            plan.append({"op": "unchanged", "channel": key[0], "dedup_key": key[1]})
            continue
        for op in ops:
            plan.append({
                "op": op, "channel": key[0], "dedup_key": key[1],
                "known_at": min(pd.Timestamp(ev["known_at"]), old_known),
                "known_at_basis": ev["known_at_basis"] if op == "tighten_known_at" else old["known_at_basis"],
                "content_hash": ev["content_hash"], "source_refs": sorted(new_refs | old_refs),
                "prev_known_at": old_known,
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
    observed_at = pd.Timestamp(observed_at)
    rows = [] if stored.empty else stored.to_dict("records")
    for r in rows:
        r.setdefault("superseded_at", pd.NaT)
        r.setdefault("retracted_at", pd.NaT)

    def current(channel: str, key: str) -> dict[str, Any] | None:
        for r in rows:
            if r["channel"] == channel and r["dedup_key"] == key and pd.isna(r["superseded_at"]) \
                    and pd.isna(r["retracted_at"]):
                return r
        return None

    for p in plan.to_dict("records"):
        op = p["op"]
        if op == "unchanged":
            continue
        if op == "insert":
            rows.append({"channel": p["channel"], "dedup_key": p["dedup_key"], "known_at": p["known_at"],
                         "known_at_basis": p["known_at_basis"], "source_refs": list(p["source_refs"]),
                         "content_hash": p["content_hash"], "superseded_at": pd.NaT, "retracted_at": pd.NaT})
            continue
        cur = current(p["channel"], p["dedup_key"])
        if cur is None:
            raise ValueError(f"plan op {op} for a key with no current row: {p['channel']} {p['dedup_key']}")
        if op == "supersede":
            cur["superseded_at"] = observed_at
            rows.append({"channel": p["channel"], "dedup_key": p["dedup_key"], "known_at": p["known_at"],
                         "known_at_basis": p["known_at_basis"], "source_refs": list(p["source_refs"]),
                         "content_hash": p["content_hash"], "superseded_at": pd.NaT, "retracted_at": pd.NaT})
        elif op == "tighten_known_at":
            if pd.Timestamp(p["known_at"]) > pd.Timestamp(cur["known_at"]):
                raise ValueError("known_at may only move earlier")
            cur["known_at"], cur["known_at_basis"] = p["known_at"], p["known_at_basis"]
        elif op == "add_sources":
            cur["source_refs"] = sorted(set(cur["source_refs"]) | set(p["source_refs"]))
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
    as_of = pd.Timestamp(as_of)
    known = pd.to_datetime(stored["known_at"], utc=True)
    sup = pd.to_datetime(stored.get("superseded_at"), utc=True)
    ret = pd.to_datetime(stored.get("retracted_at"), utc=True)
    mask = (known <= as_of) & (sup.isna() | (sup > as_of)) & (ret.isna() | (ret > as_of))
    return stored[mask.to_numpy()]
