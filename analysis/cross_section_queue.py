"""Hypothesis queue from published effects (#1212): the file the cross-section factory takes its next run from.

Each entry in ``docs/research/cross-section-queue.json`` is a documented cross-sectional effect. It records the paper,
the paper's sample, the size the paper states and how that size was checked, the mechanism, the data it needs, how
this test departs from the paper, and the signal that tests it. The factory (``analysis/cross_section_factory.py``)
runs it on the development days, pooled and inside each sector, and its card says by year, by sector and by the
state recorded the evening before where the effect paid after cost and where it did not.

The order is the queue's: ``next`` is the first entry whose status is ``queued`` and whose run the ledger has not
declared. An entry's run id carries its version (``xsq-<id>-v<version>``) and the factory refuses a run id or run file
a ledger declared before, so an entry gets one look per version; a changed entry is a new version, never an edit.

    python -m analysis.cross_section_queue status [--queue Q] [--ledger L --anchor A]
    python -m analysis.cross_section_queue run --data-dir DIR --state-db state.db --ledger L --anchor A \\
        --out-root ROOT [--queue Q] [--genesis LEDGER_ID]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from analysis import cross_section_factory as X
from analysis import ledger_steered_exploration as lse
from analysis.offline_research_proof import write_once

SCHEMA = "cross-section-effect-queue/1"
DEFAULT_QUEUE = Path(__file__).resolve().parent.parent / "docs" / "research" / "cross-section-queue.json"
STATUSES = ("queued", "blocked", "retired")
CHECKED = ("paper", "secondary", "not_checked")
ENTRY_KEYS = {"id", "version", "status", "blocked_reason", "retired_reason", "paper", "paper_sample", "stated_effect",
              "mechanism", "needs", "deviation", "signals", "rankings", "fractions", "prior_dev_looks"}
RUN_DEFAULTS = {"rankings", "fractions", "cost_bp_per_side", "alpha", "perms", "seed", "cards", "null_block_sessions",
                "first_session", "last_session", "min_ranked", "min_session_bars", "min_ranked_gated"}


class QueueError(ValueError):
    """A queue file the reader refuses."""


def run_id(entry: dict) -> str:
    return f"xsq-{entry['id']}-v{entry['version']}"


def run_file(queue: dict, entry: dict) -> dict:
    """The factory run file for one entry: the queue's defaults, the entry's own rankings and fractions if it names
    them, its signals and its earlier development looks. Deterministic, so the same entry gives the same file."""
    body = {k: queue["defaults"][k] for k in sorted(queue["defaults"])}
    for k in ("rankings", "fractions"):
        if k in entry:
            body[k] = entry[k]
    body.update(run_id=run_id(entry), signals=entry["signals"], prior_dev_looks=list(entry.get("prior_dev_looks", [])))
    return body


def _entry_problem(e: dict) -> str | None:
    if not isinstance(e, dict) or set(e) - ENTRY_KEYS:
        return f"unknown keys {sorted(set(e) - ENTRY_KEYS) if isinstance(e, dict) else e}"
    if not (isinstance(e.get("id"), str) and e["id"] and all(c.isalnum() or c == "-" for c in e["id"])):
        return "id is letters, digits and '-'"
    if type(e.get("version")) is not int or e["version"] < 1:
        return "version is a whole number from 1"
    if e.get("status") not in STATUSES:
        return f"status is one of {STATUSES}"
    for need in ("paper", "paper_sample", "stated_effect", "mechanism", "needs"):
        if not e.get(need):
            return f"{need} is required"
    if not (isinstance(e["paper"], dict) and e["paper"].get("authors") and e["paper"].get("year") and e["paper"].get("title")):
        return "paper needs authors, year and title"
    if e["stated_effect"].get("checked") not in CHECKED or not e["stated_effect"].get("text"):
        return f"stated_effect needs text and checked in {CHECKED}"
    if e["stated_effect"]["checked"] != "not_checked" and not e["stated_effect"].get("source"):
        return "a stated effect checked anywhere names its source"
    if e["status"] == "blocked" and not e.get("blocked_reason"):
        return "a blocked entry says why"
    if e["status"] == "retired" and not e.get("retired_reason"):
        return "a retired entry says why"
    if e["status"] == "queued" and not (e.get("signals") and e.get("deviation")):
        return "a queued entry has signals and says how the test departs from the paper"
    return None


def load_queue(path: Path = DEFAULT_QUEUE) -> dict:
    """The queue, checked: every entry well formed, (id, version) unique, and every queued entry's run file one the
    factory accepts."""
    q = json.loads(Path(path).read_text())
    if q.get("schema") != SCHEMA or not isinstance(q.get("entries"), list) or not isinstance(q.get("defaults"), dict):
        raise QueueError(f"not a {SCHEMA} file")
    if set(q["defaults"]) - RUN_DEFAULTS:
        raise QueueError(f"defaults may set {sorted(RUN_DEFAULTS)}")
    seen = set()
    for e in q["entries"]:
        why = _entry_problem(e)
        if why:
            raise QueueError(f"entry {e.get('id') if isinstance(e, dict) else e}: {why}")
        if (e["id"], e["version"]) in seen:
            raise QueueError(f"entry {e['id']} v{e['version']} appears twice")
        seen.add((e["id"], e["version"]))
        if e["status"] == "queued":
            try:
                X.RunFile.from_dict(run_file(q, e))
            except X.RunFileError as exc:
                raise QueueError(f"entry {e['id']}: the factory refuses its run file ({exc})") from exc
    return q


def declared(ledger: lse.Ledger | None) -> dict:
    """run id -> 'result' | 'abandoned' | 'open' for every factory run the ledger declared."""
    if ledger is None:
        return {}
    out, last = {}, None
    for rec in ledger.records:
        kind = rec.get("kind")
        if kind == lse.FACTORY_DECLARED:
            last = rec.get("run_id")
            out[last] = "open"
        elif kind == lse.FACTORY_RESULT and rec.get("run_id") in out:
            out[rec["run_id"]] = "result"
        elif kind == lse.FACTORY_ABANDONED and last in out and out[last] == "open":
            out[last] = "abandoned"
    return out


def status(queue: dict, ledger: lse.Ledger | None = None) -> list[dict]:
    runs = declared(ledger)
    return [{"id": e["id"], "version": e["version"], "status": e["status"], "run_id": run_id(e),
             "run": runs.get(run_id(e)), "checked": e["stated_effect"]["checked"],
             "why": e.get("blocked_reason") or e.get("retired_reason")} for e in queue["entries"]]


def next_entry(queue: dict, ledger: lse.Ledger | None) -> dict | None:
    runs = declared(ledger)
    return next((e for e in queue["entries"] if e["status"] == "queued" and run_id(e) not in runs), None)


def run_next(queue_path: Path, data_dir: Path, state_db: Path, ledger: lse.Ledger, out_root: Path) -> dict:
    """Run the next queued entry through the factory. Writes ``<run id>.run.json`` once, then the factory's output in
    ``<out_root>/<run id>/``. Returns the factory's report."""
    q = load_queue(queue_path)
    e = next_entry(q, ledger)
    if e is None:
        raise QueueError("no queued entry is left that this ledger has not run")
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    rf = out_root / f"{run_id(e)}.run.json"
    write_once(rf, run_file(q, e))
    return X.run(rf, data_dir, state_db, ledger, out_root / run_id(e))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("status")
    s.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    s.add_argument("--ledger", type=Path)
    s.add_argument("--anchor", type=Path)
    r = sub.add_parser("run")
    r.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    for name in ("--data-dir", "--state-db", "--ledger", "--anchor", "--out-root"):
        r.add_argument(name, required=True, type=Path)
    r.add_argument("--genesis", metavar="LEDGER_ID", help="create the factory ledger first (once)")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "status":
            ledger = X.open_ledger(a.ledger, a.anchor, None) if a.ledger else None
            print(json.dumps(status(load_queue(a.queue), ledger), indent=1))
            return 0
        rep = run_next(a.queue, a.data_dir, a.state_db, X.open_ledger(a.ledger, a.anchor, a.genesis), a.out_root)
    except (QueueError, X.RunFileError) as e:
        print(json.dumps({"ok": False, "refused": str(e)}))
        return 2
    print(json.dumps({"ok": True, "run_id": rep["run_id"], "tested": rep["tested"], "passed_step_down":
                      sum(bool(t["passed_step_down"]) for t in rep["ranking"]), "selected": len(rep["selected"]),
                      "cards": len(rep["cards"])}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
