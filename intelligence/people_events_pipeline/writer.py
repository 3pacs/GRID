"""Apply a write plan to ``people_events`` (requires migration ``people_events_v2_20261001``).

NOT SCHEDULED, NOT CALLED BY ANYTHING. Running it against production is an
owner-approved activation (design doc section 7): the GD3 backfill and the
incremental timer each need their own OK.

Contract (mirrors ``plan.apply_in_memory`` statement for statement):

* ``insert``           INSERT a new current row.
* ``add_sources`` /    UPDATE the current row: source_refs grow, known_at only
  ``tighten_known_at`` moves earlier (``LEAST``), n_sources/n_source_rows recount.
* ``supersede``        UPDATE the current row (``superseded_at``,
                       ``superseded_by`` = the new id, a deferred FK), then
                       INSERT the new version with ``known_at = observed_at``.
* ``retract``          UPDATE ``retracted_at``/``retraction_reason``.

Every UPDATE is logged into ``people_event_revisions`` by trigger, and the
table's guard trigger refuses anything else (deletes, content edits, a later
known_at). Work is committed in chunks so a crash leaves a prefix; a re-run
recomputes the plan against what was stored and continues (idempotent).
Each run is recorded in ``people_events_runs``; a run that wrote nothing is
``NO_NEW_ROWS``, never ``SUCCESS``.
"""

from __future__ import annotations

import json
from datetime import datetime, time, timezone
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from intelligence.people_events_pipeline import PIPELINE_VERSION
from intelligence.people_events_pipeline import rules as R

CHUNK_ROWS = 20_000

_INSERT = text("""
    INSERT INTO people_events (
        id, channel, dedup_key, loose_key, event_time, known_at, known_at_basis,
        actor_id, actor_id_basis, actor_type, co_actor_ids, entity_ticker, entity_cik, security_id,
        direction, transaction_code, size_usd, source, source_record_id, source_refs, n_sources,
        n_source_rows, confidence, content_hash, materializer_version, run_id, provenance
    ) VALUES (
        COALESCE(:id, nextval('people_events_id_seq')), :channel, :dedup_key, :loose_key, :event_time,
        :known_at, :known_at_basis, :actor_id, :actor_id_basis, :actor_type, :co_actor_ids,
        :entity_ticker, :entity_cik, :security_id, :direction, :transaction_code, :size_usd, :source,
        :source_record_id, CAST(:source_refs AS jsonb), :n_sources, :n_source_rows, :confidence,
        :content_hash, :materializer_version, :run_id, CAST(:provenance AS jsonb)
    )
""")

_MERGE = text("""
    UPDATE people_events SET
        known_at = LEAST(known_at, :known_at),
        known_at_basis = CASE WHEN :known_at < known_at THEN :known_at_basis ELSE known_at_basis END,
        source_refs = (SELECT jsonb_agg(DISTINCT e) FROM jsonb_array_elements(source_refs || CAST(:source_refs AS jsonb)) e),
        n_sources = (SELECT count(DISTINCT e->>'source') FROM jsonb_array_elements(source_refs || CAST(:source_refs AS jsonb)) e),
        n_source_rows = (SELECT count(DISTINCT e) FROM jsonb_array_elements(source_refs || CAST(:source_refs AS jsonb)) e)
    WHERE channel = :channel AND dedup_key = :dedup_key AND superseded_at IS NULL AND retracted_at IS NULL
""")

_SUPERSEDE = text("""
    UPDATE people_events SET superseded_at = :observed_at, superseded_by = :new_id
    WHERE channel = :channel AND dedup_key = :dedup_key AND superseded_at IS NULL AND retracted_at IS NULL
""")

_RETRACT = text("""
    UPDATE people_events SET retracted_at = :observed_at, retraction_reason = :reason
    WHERE channel = :channel AND dedup_key = :dedup_key AND superseded_at IS NULL AND retracted_at IS NULL
""")


def _refs_json(refs: list[str]) -> str:
    out = []
    for r in sorted(set(refs)):
        source, _, record = r.partition("|")
        out.append({"source": source, "source_record_id": record})
    return json.dumps(out)


def event_row(ev: dict[str, Any], *, run_id: str, known_at: Any = None, basis: str | None = None) -> dict[str, Any]:
    """One resolved canonical event (``merge`` + ``security`` output) -> INSERT parameters."""
    matched = ev.get("security_id") is not None and not pd.isna(ev.get("security_id"))
    cik = ev.get("entity_cik")
    known = ev["known_at"] if known_at is None else known_at
    known_basis = ev["known_at_basis"] if basis is None else basis
    refs = list(ev["source_refs"])
    provenance = {
        "pipeline_version": PIPELINE_VERSION,
        "attrs": ev.get("attrs") or {},
        "accession": ev.get("accession"),
        "document_type": ev.get("document_type"),
        "near_duplicate": bool(ev.get("near_duplicate", False)),
        "security_match_basis": ev.get("security_match_basis"),
        "sources": list(ev.get("sources") or []),
    }
    if known_at is not None:
        provenance["act_known_at"] = pd.Timestamp(ev["known_at"]).isoformat()
    return {
        "id": None,
        "channel": ev["channel"], "dedup_key": ev["dedup_key"], "loose_key": ev.get("loose_key"),
        "event_time": datetime.combine(ev["event_date"], time.min, tzinfo=timezone.utc),
        "known_at": pd.Timestamp(known).to_pydatetime(), "known_at_basis": known_basis,
        "actor_id": ev["actor_id"], "actor_id_basis": ev["actor_id_basis"], "actor_type": ev["actor_type"],
        "co_actor_ids": [x for x in str(ev.get("co_actor_ids") or "").split(",") if x],
        "entity_ticker": ev.get("entity_ticker"),
        "entity_cik": None if cik is None or pd.isna(cik) else R.cik_text(int(cik)),
        "security_id": ev.get("security_id") if matched else None,
        "direction": ev.get("direction"), "transaction_code": ev.get("transaction_code"),
        "size_usd": None if ev.get("size_usd") is None or pd.isna(ev.get("size_usd")) else float(ev["size_usd"]),
        "source": ev["source"], "source_record_id": ev.get("source_record_id"),
        "source_refs": _refs_json(refs),
        "n_sources": int(ev.get("n_sources", 1)), "n_source_rows": int(ev.get("n_source_rows", len(refs) or 1)),
        "confidence": R.confidence(known_basis, ev["actor_id_basis"], matched, ev["channel"]),
        "content_hash": ev["content_hash"], "materializer_version": PIPELINE_VERSION, "run_id": run_id,
        "provenance": json.dumps(provenance, default=str),
    }


def _start_run(conn: Connection, run_id: str, mode: str, inputs: dict[str, Any]) -> None:
    conn.execute(text(
        "INSERT INTO people_events_runs (run_id, mode, materializer_version, inputs) "
        "VALUES (:run_id, :mode, :v, CAST(:inputs AS jsonb))"
    ), {"run_id": run_id, "mode": mode, "v": PIPELINE_VERSION, "inputs": json.dumps(inputs, default=str)})


def _finish_run(conn: Connection, run_id: str, counts: dict[str, int], error: str | None = None) -> str:
    written = sum(v for k, v in counts.items() if k != "unchanged")
    status = "FAILED" if error else ("SUCCESS" if written > 0 else "NO_NEW_ROWS")
    conn.execute(text(
        "UPDATE people_events_runs SET finished_at = clock_timestamp(), status = :status, "
        "counts = CAST(:counts AS jsonb), error = :error WHERE run_id = :run_id"
    ), {"status": status, "counts": json.dumps({**counts, "written": written}), "error": error, "run_id": run_id})
    return status


def apply_write_plan(engine: Engine, events: pd.DataFrame, plan: pd.DataFrame, *, run_id: str, mode: str,
                     observed_at: datetime, inputs: dict[str, Any] | None = None) -> dict[str, Any]:
    """Apply ``plan`` (from ``plan.build_write_plan``) for the resolved ``events``. Returns counts + status."""
    by_key = {(e["channel"], e["dedup_key"]): e for e in events.to_dict("records")}
    counts = {op: 0 for op in ("insert", "add_sources", "tighten_known_at", "supersede", "retract", "unchanged")}
    with engine.begin() as conn:
        _start_run(conn, run_id, mode, inputs or {})
    ops = plan.to_dict("records")
    try:
        merged_keys: set[tuple[str, str]] = set()
        for start in range(0, len(ops), CHUNK_ROWS):
            chunk = ops[start:start + CHUNK_ROWS]
            with engine.begin() as conn:
                inserts = []
                for p in chunk:
                    key = (p["channel"], p["dedup_key"])
                    op = p["op"]
                    if op == "unchanged":
                        counts[op] += 1
                    elif op == "insert":
                        inserts.append(event_row(by_key[key], run_id=run_id))
                        counts[op] += 1
                    elif op in ("add_sources", "tighten_known_at"):
                        counts[op] += 1
                        if key in merged_keys:
                            continue  # both ops for one key are applied by one UPDATE
                        merged_keys.add(key)
                        ev = by_key[key]
                        conn.execute(_MERGE, {"channel": key[0], "dedup_key": key[1],
                                              "known_at": pd.Timestamp(ev["known_at"]).to_pydatetime(),
                                              "known_at_basis": ev["known_at_basis"],
                                              "source_refs": _refs_json(list(ev["source_refs"]))})
                    elif op == "supersede":
                        new_id = conn.execute(text("SELECT nextval('people_events_id_seq')")).scalar()
                        conn.execute(_SUPERSEDE, {"observed_at": observed_at, "new_id": new_id,
                                                  "channel": key[0], "dedup_key": key[1]})
                        row = event_row(by_key[key], run_id=run_id, known_at=observed_at, basis="first_seen")
                        row["id"] = new_id
                        conn.execute(_INSERT, row)
                        counts[op] += 1
                    elif op == "retract":
                        conn.execute(_RETRACT, {"observed_at": observed_at, "reason": "absent_from_complete_scan",
                                                "channel": key[0], "dedup_key": key[1]})
                        counts[op] += 1
                if inserts:
                    conn.execute(_INSERT, inserts)
    except Exception as exc:
        with engine.begin() as conn:
            _finish_run(conn, run_id, counts, error=f"{type(exc).__name__}: {exc}"[:2000])
        raise
    with engine.begin() as conn:
        status = _finish_run(conn, run_id, counts)
    return {"run_id": run_id, "status": status, "counts": counts}
