"""Research findings: what the Research Desk and the GEX searches have tested, loaded into GRID.

The desk keeps one record per tested question in its own repository (schema ``desk-findings/1``): the rule as
tested, the window and sample, the effect with its interval, the verdict, the unread data the look used up, and the
written result it came from. Every number in a record is checked against a quote from that result before the
catalogue is exported. This module loads an exported catalogue into GRID and answers two questions for GRID's code:

* **Has this been tested, and how did it go?** ``current_findings`` (and ``GET /api/v1/research/findings``)
  filters by family, area, verdict or words, so a search can count an earlier look before spending a new one
  and a reader can cite the result.
* **What should the chat know?** ``context_for_prompt`` turns the findings that match a question into short lines
  with their verdicts and numbers, for the LLM's system context next to the knowledge tree.

Rules
-----
* **Insert-only, by catalogue version.** A load inserts every record of one catalogue under its
  ``records_sha256`` (``research_findings``) and one row in ``research_findings_loads``. Rows are never updated
  or deleted (triggers refuse it). "Current" is the latest load; earlier versions stay readable.
* **The catalogue must hash to what it claims.** ``records_sha256`` is recomputed over the records exactly as
  the exporter wrote them; a mismatch, a wrong schema, an unknown verdict or a duplicate id refuses the whole file.
* **Nothing is scored or promoted here.** A finding is a record of a past result, not a signal.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

SCHEMA = "desk-findings/1"
KINDS = ("confirmation_look", "development", "description", "forward_record", "exploratory", "registration")
VERDICTS = ("CONFIRMED", "NOT_CONFIRMED", "UNDERPOWERED", "FAILED_BAR", "PASSED_BAR", "DESCRIPTION", "PENDING")
STATUSES = ("closed", "forward_live", "open", "retired")
AREAS = ("cross_section", "index_0dte", "index_daily", "market_state", "flows", "events", "execution", "data_quality")
REQUIRED = ("id", "family", "area", "question", "rule", "kind", "verdict", "status", "source", "recorded_at")
MAX_BYTES = 20_000_000
_WORD = re.compile(r"[a-z0-9][a-z0-9-]{3,}")
_STOP = frozenset({"what", "when", "with", "that", "this", "does", "have", "from", "into", "will", "would", "about",
                   "there", "their", "which", "should", "could", "market", "stocks", "today"})


_FILTERS = {"family": "family = :family", "area": "area = :area", "verdict": "verdict = :verdict"}


class CatalogueError(ValueError):
    """The file is not a sound desk-findings/1 catalogue."""


def records_sha256(records: list[dict]) -> str:
    body = json.dumps(records, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def _date_or_none(v: Any) -> str | None:
    return v if isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v) else None


def _num(v: Any) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def parse_catalogue(raw: bytes) -> dict:
    """The catalogue as a dict, or CatalogueError naming what is wrong."""
    if len(raw) > MAX_BYTES:
        raise CatalogueError("catalogue too large")
    try:
        cat = json.loads(raw)
    except ValueError as exc:
        raise CatalogueError(f"not JSON: {exc}") from exc
    if not isinstance(cat, dict) or cat.get("schema") != SCHEMA:
        raise CatalogueError(f"schema must be {SCHEMA}")
    recs = cat.get("records")
    if not isinstance(recs, list) or not recs:
        raise CatalogueError("records must be a non-empty list")
    if cat.get("count") != len(recs):
        raise CatalogueError("count does not match the records")
    if records_sha256(recs) != cat.get("records_sha256"):
        raise CatalogueError("records_sha256 does not match the records")
    seen: set[str] = set()
    for r in recs:
        missing = [k for k in REQUIRED if not r.get(k)]
        if missing:
            raise CatalogueError(f"record {r.get('id', '?')} lacks {missing}")
        for k, allowed in (("kind", KINDS), ("verdict", VERDICTS), ("status", STATUSES), ("area", AREAS)):
            if r[k] not in allowed:
                raise CatalogueError(f"record {r['id']}: {k} {r[k]!r} not allowed")
        if not _date_or_none(r["recorded_at"]):
            raise CatalogueError(f"record {r['id']}: recorded_at must be YYYY-MM-DD")
        if not isinstance(r["source"], dict) or not r["source"].get("path"):
            raise CatalogueError(f"record {r['id']}: source.path required")
        if r["id"] in seen:
            raise CatalogueError(f"duplicate id {r['id']}")
        seen.add(r["id"])
    return cat


def _row(r: dict, sha: str) -> dict:
    w, s, e = r.get("window") or {}, r.get("sample") or {}, r.get("effect") or {}
    ci = e.get("ci95") if isinstance(e.get("ci95"), list) and len(e.get("ci95")) == 2 else [None, None]
    return {"finding_id": r["id"], "catalogue_sha256": sha, "family": r["family"], "area": r["area"], "kind": r["kind"],
            "verdict": r["verdict"], "status": r["status"], "question": r["question"], "rule": r["rule"],
            "window_from": _date_or_none(w.get("from")), "window_to": _date_or_none(w.get("to")),
            "sample_n": _num(s.get("n")), "sample_unit": s.get("unit"), "effect_value": _num(e.get("value")),
            "effect_unit": e.get("unit"), "ci_low": _num(ci[0]), "ci_high": _num(ci[1]), "t_stat": _num(e.get("t")),
            "data_spent": json.dumps(r.get("data_spent") or []), "record": json.dumps(r, sort_keys=True),
            "source_path": r["source"]["path"], "source_sha256": r["source"].get("sha256"), "recorded_at": r["recorded_at"]}


INSERT_ROW = text(
    "INSERT INTO research_findings (finding_id, catalogue_sha256, family, area, kind, verdict, status, question, rule, "
    " window_from, window_to, sample_n, sample_unit, effect_value, effect_unit, ci_low, ci_high, t_stat, data_spent, "
    " record, source_path, source_sha256, recorded_at) VALUES (:finding_id, :catalogue_sha256, :family, :area, :kind, "
    " :verdict, :status, :question, :rule, CAST(:window_from AS date), CAST(:window_to AS date), :sample_n, :sample_unit, "
    " :effect_value, :effect_unit, :ci_low, :ci_high, :t_stat, CAST(:data_spent AS jsonb), CAST(:record AS jsonb), "
    " :source_path, :source_sha256, CAST(:recorded_at AS date))")


def load_catalogue(engine: Engine, raw: bytes, source: str) -> dict:
    """Insert one catalogue version. Loading the same version twice is a no-op."""
    cat = parse_catalogue(raw)
    sha = cat["records_sha256"]
    with engine.begin() as conn:
        if conn.execute(text("SELECT 1 FROM research_findings_loads WHERE catalogue_sha256 = :s"), {"s": sha}).fetchone():
            return {"status": "already loaded", "catalogue_sha256": sha, "count": cat["count"]}
        conn.execute(text("INSERT INTO research_findings_loads (catalogue_sha256, record_count, repo_commit, source) "
                          "VALUES (:s, :n, :c, :src)"), {"s": sha, "n": cat["count"], "c": cat.get("repo_commit"), "src": source})
        conn.execute(INSERT_ROW, [_row(r, sha) for r in cat["records"]])
    return {"status": "loaded", "catalogue_sha256": sha, "count": cat["count"]}


def current_findings(engine: Engine, *, family: str | None = None, area: str | None = None, verdict: str | None = None,
                     q: str | None = None, limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
    """Findings of the latest catalogue, newest first, with the filtered total."""
    where, params = [], {}
    for col, val in (("family", family), ("area", area), ("verdict", verdict)):
        if val:
            where.append(_FILTERS[col])
            params[col] = val
    if q:
        where.append("(question ILIKE :q OR rule ILIKE :q OR family ILIKE :q)")
        params["q"] = f"%{q}%"
    clause = (" AND " + " AND ".join(where)) if where else ""
    base = "FROM research_findings_current WHERE TRUE" + clause
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) " + base), params).scalar() or 0
        rows = conn.execute(text("SELECT finding_id, family, area, kind, verdict, status, question, rule, window_from, "
                                 "window_to, sample_n, sample_unit, effect_value, effect_unit, ci_low, ci_high, t_stat, "
                                 "data_spent, source_path, recorded_at, catalogue_sha256 " + base
                                 + " ORDER BY recorded_at DESC, finding_id LIMIT :limit OFFSET :offset"),
                            dict(params, limit=limit, offset=offset)).fetchall()
    out = []
    for row in rows:
        d = dict(row._mapping)
        for k in ("window_from", "window_to", "recorded_at"):
            d[k] = d[k].isoformat() if d.get(k) is not None else None
        out.append(d)
    return out, int(total)


def _terms(topic: str) -> set[str]:
    return {w for w in _WORD.findall(topic.lower()) if w not in _STOP}


def rank_for_topic(findings: list[dict], topic: str, limit: int = 5) -> list[dict]:
    """Findings sharing the most words with the topic (family, question, rule); ties go to the newest."""
    terms = _terms(topic)
    if not terms:
        return []
    scored = []
    for f in findings:
        words = _terms(" ".join(str(f.get(k) or "") for k in ("family", "question", "rule")))
        hit = len(terms & words)
        if hit:
            scored.append((hit, str(f.get("recorded_at") or ""), f))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [f for _, _, f in scored[:limit]]


def line(f: dict) -> str:
    """One plain line: verdict, question, effect with its interval, sample, window."""
    s = f"[{f['verdict']}] {f['question']}"
    if f.get("effect_value") is not None:
        s += f" Effect {f['effect_value']:+g} {f.get('effect_unit') or ''}".rstrip()
        if f.get("ci_low") is not None and f.get("ci_high") is not None:
            s += f" (95% {f['ci_low']:+g} to {f['ci_high']:+g})"
        if f.get("t_stat") is not None:
            s += f", t {f['t_stat']:g}"
    if f.get("sample_n") is not None:
        s += f"; n {f['sample_n']:g} {f.get('sample_unit') or ''}".rstrip()
    if f.get("window_from") or f.get("window_to"):
        s += f"; {f.get('window_from') or '?'}..{f.get('window_to') or '?'}"
    return s + f"; status {f['status']}."


def context_for_prompt(engine: Engine, topic: str, limit: int = 5) -> str:
    """The findings that match a question, for the LLM's system context. Empty when none match or on error."""
    try:
        rows, _ = current_findings(engine, limit=1000)
    except Exception as exc:  # noqa: BLE001 - the chat works without it
        log.debug("research findings context skipped: {e}", e=str(exc))
        return ""
    hits = rank_for_topic(rows, topic, limit)
    if not hits:
        return ""
    return "\n".join(["--- Research Desk findings (tested; verdicts as recorded) ---", *(line(f) for f in hits),
                      "--- End findings ---"])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Load or list Research Desk findings")
    sub = ap.add_subparsers(dest="cmd", required=True)
    ld = sub.add_parser("load")
    ld.add_argument("--path", required=True)
    ls = sub.add_parser("list")
    ls.add_argument("--family")
    ls.add_argument("--verdict")
    a = ap.parse_args(argv)
    from db import get_engine
    engine = get_engine()
    if a.cmd == "load":
        with open(a.path, "rb") as f:
            raw = f.read()
        print(json.dumps(load_catalogue(engine, raw, a.path)))
        return 0
    rows, total = current_findings(engine, family=a.family, verdict=a.verdict, limit=1000)
    for r in rows:
        print(line(r))
    print(f"{total} findings")
    return 0


if __name__ == "__main__":
    sys.exit(main())
