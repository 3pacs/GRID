"""Staged EDGAR 8-K acceptance-datetime backfill (gap G2 of the D0 research contract, section 5.5).

D1 needs, for every 8-K in the development and holdout years, the instant EDGAR accepted it. GRID has
none before 2026-02 (``earnings_events`` starts there). This job collects it for free from the SEC and
writes **hashed files outside griddb**. There is no table and no DDL here: a table is a separate decision.

Stages (each is resumable and writes only under ``--out``)::

    plan     quarterly form index (form.gz) -> every 8-K / 8-K/A accession filed in the window, and its CIK
    fetch    one submissions history per planned CIK (plus its older pages) -> raw gzip + sha256 manifest
    extract  raw -> acceptance_8k.jsonl (UTC instants) + reconciliation.json against the plan
    verify   a seeded sample of accessions re-read from the filing's own header; every mismatch is reported

Why submissions histories and not one header per filing (the contract estimated about 1 M requests):
``acceptanceDateTime`` in the submissions record is the same acceptance instant and costs one request per
company, not per filing. That claim is not trusted: ``verify`` compares a sample against the filing header and
the job reports the mismatch count instead of asserting agreement.

Honest limits, recorded in every output rather than assumed away:
- ``retrieved_at`` is the collection time (2026), not the historical publication time. These rows prove when
  EDGAR *accepted* a filing, not when GRID could have seen it. Historical use stays PROXY / DIAGNOSTIC_ONLY
  under contract section 5.1, and the manifest says so.
- A planned accession with no acceptance time is listed in ``reconciliation.json``; it is never dropped.
- Nothing here touches the network except through the injected ``fetch`` callable, which tests replace.

The SEC identity is the repo's existing one (``ingestion.edgar.resolve_sec_identity``). Default rate is half
the SEC's 10 requests per second.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
SCHEMA = "edgar-acceptance-8k/1"
FORMS = ("8-K", "8-K/A")
INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{y}/QTR{q}/form.gz"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/{name}"
HEADER_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{nodash}/{acc}.hdr.sgml"
DEFAULT_INTERVAL_S = 0.2  # 5 requests/s, half the SEC limit

Fetch = Callable[[str], bytes]


class BackfillError(Exception):
    """The job cannot continue honestly (blocked by the SEC, corrupt state, bad input)."""


class Blocked(BackfillError):
    """HTTP 403 from the SEC: stop, do not retry around it."""


# ---------------------------------------------------------------------------------------------- plumbing


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# Same value as ``ingestion.edgar.DEFAULT_SEC_IDENTITY``; copied because importing that module needs the
# heavy ``edgar`` package, which a collection box should not have to install. A test pins the two together.
_DEFAULT_IDENTITY = "GRID Intelligence ops@stepdad.finance"


def sec_identity(identity: str | None = None) -> str:
    """Explicit identity, then the SEC_USER_AGENT environment variable, then the repo default.

    A value without a deliverable-looking address is replaced by the default (the SEC returns 403 otherwise).
    """
    import re

    for cand in (identity, os.environ.get("SEC_USER_AGENT")):
        if cand and re.search(r"@[A-Za-z0-9-]+\.[A-Za-z]{2,}", cand) and "@localhost" not in cand:
            return cand.strip()
    return _DEFAULT_IDENTITY


def make_http_fetch(identity: str | None = None, *, interval_s: float = DEFAULT_INTERVAL_S, retries: int = 4,
                    sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> Fetch:
    """The real fetcher: declared SEC identity, a floor on the gap between requests, bounded retries."""
    import requests

    headers = {"User-Agent": sec_identity(identity), "Accept-Encoding": "gzip, deflate"}
    last = [0.0]

    def fetch(url: str) -> bytes:
        for attempt in range(retries + 1):
            wait = interval_s - (clock() - last[0])
            if wait > 0:
                sleep(wait)
            last[0] = clock()
            try:
                r = requests.get(url, headers=headers, timeout=60)
            except requests.RequestException as e:  # network error: back off and retry
                if attempt == retries:
                    raise BackfillError(f"{url}: {e}") from e
                sleep(2.0 * (attempt + 1))
                continue
            if r.status_code == 403:
                raise Blocked(f"{url}: HTTP 403 (SEC fair-access block); stopping")
            if r.status_code == 404:
                raise FileNotFoundError(url)
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt == retries:
                    raise BackfillError(f"{url}: HTTP {r.status_code} after {retries} retries")
                sleep(5.0 * (attempt + 1))
                continue
            if r.status_code != 200:
                raise BackfillError(f"{url}: HTTP {r.status_code}")
            return r.content
        raise BackfillError(f"{url}: exhausted retries")  # pragma: no cover

    return fetch


# ---------------------------------------------------------------------------------------------- plan


@dataclass(frozen=True)
class Planned:
    """One accession. ``ciks`` lists every co-registrant (one filing, several CIKs, is normal)."""

    accession: str
    ciks: tuple[str, ...]  # 10 digits each, sorted
    form: str
    filed: str  # YYYY-MM-DD


def quarters(start_year: int, end_year: int) -> list[tuple[int, int]]:
    if not (1994 <= start_year <= end_year):
        raise BackfillError(f"bad window {start_year}..{end_year}")
    return [(y, q) for y in range(start_year, end_year + 1) for q in (1, 2, 3, 4)]


_ROW = re.compile(r"^(8-K|8-K/A)\s+(.*\S)\s+(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\S+)\s*$")


def parse_form_idx(text: str) -> list[Planned]:
    """Read a quarterly form index. Only 8-K and 8-K/A rows are kept.

    The SEC's data columns are not aligned with its header line, so a row is read from its right-hand
    tokens (file name, date, CIK), never by column offsets. A row that starts with an 8-K form but does not
    parse is an error, not a skip. 8-K12B, 8-K15D5 and every other form are different forms and are ignored.
    """
    lines = text.splitlines()
    head = next((i for i, ln in enumerate(lines) if ln.startswith("Form Type")), None)
    if head is None:
        raise BackfillError("form index has no 'Form Type' header")
    out: list[Planned] = []
    for ln in lines[head + 1:]:
        if not (ln.startswith("8-K ") or ln.startswith("8-K/A ")):
            continue
        m = _ROW.match(ln)
        if not m:
            raise BackfillError(f"unreadable 8-K index row: {ln!r}")
        form, _name, cik, filed, fname = m.groups()
        acc = fname.rsplit("/", 1)[-1]
        acc = acc[:-4] if acc.endswith(".txt") else acc
        if not (len(acc) == 20 and acc[10] == "-" and acc[13] == "-"):
            raise BackfillError(f"unreadable accession in index row: {ln!r}")
        date.fromisoformat(filed)  # raises on a bad date
        out.append(Planned(acc, (cik.zfill(10),), form, filed))
    return out


def stage_plan(out: Path, fetch: Fetch, start_year: int, end_year: int, *, today: date | None = None) -> dict:
    """Fetch every quarterly index in the window (raw bytes kept) and write plan.jsonl.

    A quarter that has not finished (or has not started) is refused: a planned set must be final.
    """
    today = today or datetime.now(UTC).date()
    planned: dict[str, Planned] = {}
    manifest = []
    for y, q in quarters(start_year, end_year):
        if date(y, 3 * q - 2, 1) > today:
            raise BackfillError(f"{y}Q{q} has not started; the window must end before today")
        url = INDEX_URL.format(y=y, q=q)
        raw = fetch(url)
        _atomic_write(out / "raw" / "index" / f"{y}Q{q}.gz", raw)
        text = (gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw).decode("latin-1")
        rows = parse_form_idx(text)
        for r in rows:
            prev = planned.get(r.accession)
            if prev and (prev.form, prev.filed) != (r.form, r.filed):
                raise BackfillError(f"accession {r.accession} appears twice with a different form or date")
            ciks = tuple(sorted(set(prev.ciks) | set(r.ciks))) if prev else r.ciks
            planned[r.accession] = Planned(r.accession, ciks, r.form, r.filed)
        manifest.append({"quarter": f"{y}Q{q}", "url": url, "sha256": sha256_bytes(raw), "bytes": len(raw), "rows": len(rows)})
    lines = [json.dumps({**p.__dict__, "ciks": list(p.ciks)}, sort_keys=True) for p in sorted(planned.values(), key=lambda p: p.accession)]
    body = ("\n".join(lines) + "\n").encode()
    _atomic_write(out / "plan.jsonl", body)
    summary = {"schema": SCHEMA, "window": [start_year, end_year], "planned_accessions": len(planned),
               "planned_ciks": len({c for p in planned.values() for c in p.ciks}), "plan_sha256": sha256_bytes(body), "quarters": manifest}
    _atomic_write(out / "plan.summary.json", (json.dumps(summary, indent=1, sort_keys=True) + "\n").encode())
    return summary


def load_plan(out: Path) -> list[Planned]:
    p = out / "plan.jsonl"
    if not p.exists():
        raise BackfillError("no plan.jsonl: run the plan stage first")
    out = []
    for ln in p.read_text().splitlines():
        if ln:
            d = json.loads(ln)
            out.append(Planned(d["accession"], tuple(d["ciks"]), d["form"], d["filed"]))
    return out


# ---------------------------------------------------------------------------------------------- fetch


def _manifest_path(out: Path) -> Path:
    return out / "raw" / "submissions.manifest.jsonl"


def _load_manifest(out: Path) -> dict[str, dict]:
    p = _manifest_path(out)
    seen: dict[str, dict] = {}
    if p.exists():
        for ln in p.read_text().splitlines():
            if ln:
                e = json.loads(ln)
                seen[e["name"]] = e
    return seen


def _store_raw(out: Path, fetch: Fetch, name: str, seen: dict[str, dict], clock: Callable[[], datetime]) -> bytes | None:
    """Fetch one submissions file unless a verified copy is already stored; append its manifest line."""
    path = out / "raw" / "submissions" / (name + ".gz")
    if name in seen and path.exists():
        data = gzip.decompress(path.read_bytes())
        if sha256_bytes(data) == seen[name]["sha256"]:
            return data
    try:
        data = fetch(SUBMISSIONS_URL.format(name=name))
    except FileNotFoundError:
        entry = {"name": name, "status": "NOT_FOUND", "retrieved_at": _utc_iso(clock())}
        with _manifest_path(out).open("a") as fh:
            fh.write(json.dumps(entry, sort_keys=True) + "\n")
        seen[name] = entry
        return None
    _atomic_write(path, gzip.compress(data, mtime=0))
    entry = {"name": name, "status": "OK", "sha256": sha256_bytes(data), "bytes": len(data), "retrieved_at": _utc_iso(clock())}
    _manifest_path(out).parent.mkdir(parents=True, exist_ok=True)
    with _manifest_path(out).open("a") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")
    seen[name] = entry
    return data


def stage_fetch(out: Path, fetch: Fetch, *, limit_ciks: int | None = None, start_year: int | None = None,
                end_year: int | None = None, clock: Callable[[], datetime] = lambda: datetime.now(UTC)) -> dict:
    """One submissions history per planned CIK, plus each older page that overlaps the window."""
    plan = load_plan(out)
    ciks = sorted({c for p in plan for c in p.ciks})
    if limit_ciks is not None:
        ciks = ciks[:limit_ciks]
    lo = f"{start_year}-01-01" if start_year else "0000-00-00"
    hi = f"{end_year}-12-31" if end_year else "9999-99-99"
    seen = _load_manifest(out)
    fetched = 0
    for cik in ciks:
        top = _store_raw(out, fetch, f"CIK{cik}.json", seen, clock)
        if top is None:
            continue
        for f in json.loads(top).get("filings", {}).get("files", []) or []:
            if f.get("filingTo", "9999") >= lo and f.get("filingFrom", "0000") <= hi:
                _store_raw(out, fetch, f["name"], seen, clock)
        fetched += 1
    return {"ciks_requested": len(ciks), "ciks_stored": fetched, "files_in_manifest": len(seen)}


# ---------------------------------------------------------------------------------------------- extract


def _parse_accept(s: str) -> datetime:
    """acceptanceDateTime as the SEC serves it ('2024-01-30T16:05:23.000Z', UTC). Refuses anything else."""
    if not (isinstance(s, str) and s.endswith("Z")):
        raise ValueError(f"not a UTC instant: {s!r}")
    return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)


def _columnar(block: dict) -> Iterable[dict]:
    keys = ("accessionNumber", "filingDate", "form", "acceptanceDateTime", "items")
    cols = [block.get(k) for k in keys]
    if cols[0] is None:
        return
    n = len(cols[0])
    for k, c in zip(keys, cols):
        if c is None or len(c) != n:
            raise BackfillError(f"submissions block column {k!r} is missing or ragged")
    for i in range(n):
        yield dict(zip(keys, (c[i] for c in cols)))


def stage_extract(out: Path) -> dict:
    plan = {p.accession: p for p in load_plan(out)}
    seen = _load_manifest(out)
    candidates: dict[str, list[dict]] = {}  # every valid source row per accession; none is dropped
    problems: list[dict] = []
    for name, e in sorted(seen.items()):
        if e.get("status") != "OK":
            continue
        path = out / "raw" / "submissions" / (name + ".gz")
        data = gzip.decompress(path.read_bytes())
        if sha256_bytes(data) != e["sha256"]:
            raise BackfillError(f"{name}: stored bytes do not match the manifest hash")
        doc = json.loads(data)
        cik = (doc.get("cik") or name.split("-")[0].replace("CIK", "").replace(".json", "")).zfill(10)
        blocks = [doc["filings"]["recent"]] if "filings" in doc else [doc]
        for b in blocks:
            for r in _columnar(b):
                acc = r["accessionNumber"]
                if acc not in plan:
                    continue  # outside the planned window or not an 8-K
                try:
                    acc_utc = _parse_accept(r["acceptanceDateTime"])
                except (ValueError, TypeError) as ex:
                    problems.append({"accession": acc, "problem": f"acceptance unreadable: {ex}"})
                    continue
                if cik not in plan[acc].ciks:
                    problems.append({"accession": acc, "problem": f"cik {cik} not among the index's {list(plan[acc].ciks)}"})
                    continue
                if r["filingDate"] != plan[acc].filed:
                    problems.append({"accession": acc, "problem": f"filingDate {r['filingDate']} != index {plan[acc].filed}"})
                    continue
                row = {"accession": acc, "ciks": list(plan[acc].ciks), "form": r["form"], "filing_date": r["filingDate"],
                       "acceptance_utc": _utc_iso(acc_utc), "items": [i for i in (r.get("items") or "").split(",") if i],
                       "source_file": name, "source_retrieved_at": e["retrieved_at"], "source_sha256": e["sha256"]}
                candidates.setdefault(acc, []).append(row)
    keep = ("form", "filing_date", "acceptance_utc", "items")
    accepted: dict[str, dict] = {}
    conflicts: list[dict] = []
    for acc in sorted(candidates):
        srcs = candidates[acc]
        if len({tuple(json.dumps(r[k]) for k in keep) for r in srcs}) > 1:
            # sources disagree: quarantined, never emitted as accepted; every source row is kept as evidence
            conflicts.append({"accession": acc, "ciks": srcs[0]["ciks"], "sources": srcs})
        else:
            accepted[acc] = {**srcs[0], "n_sources": len(srcs)}
    conflicted = {c["accession"] for c in conflicts}
    missing = sorted(set(plan) - set(accepted) - conflicted)

    def jl(items) -> bytes:
        items = list(items)
        return ("\n".join(json.dumps(i, sort_keys=True) for i in items) + ("\n" if items else "")).encode()

    files = {"acceptance_8k.jsonl": jl(accepted[a] for a in sorted(accepted)), "conflicts.jsonl": jl(conflicts),
             "missing.jsonl": jl({"accession": a} for a in missing), "problems.jsonl": jl(problems)}
    for fname, body in files.items():
        _atomic_write(out / fname, body)
    assert len(plan) == len(accepted) + len(conflicted) + len(missing)
    rec = {"schema": SCHEMA, "planned": len(plan), "accepted": len(accepted), "conflicted": len(conflicted),
           "missing": len(missing), "problem_count": len(problems),
           "rows_sha256": sha256_bytes(files["acceptance_8k.jsonl"]),
           "detail_files": {f: {"sha256": sha256_bytes(b), "lines": b.count(b"\n")}
                            for f, b in files.items() if f != "acceptance_8k.jsonl"},
           "counts_note": "planned = accepted + conflicted + missing. problem_count counts record-level defects and is a "
                          "separate diagnostic: one accession can have a problem and still be accepted from another source. "
                          "Full lists are in the detail files; nothing is truncated.",
           "limits": ["acceptance_utc is EDGAR's acceptance instant, not GRID's historical visibility",
                      "source_retrieved_at is the 2026 collection time; historical use is PROXY / DIAGNOSTIC_ONLY (contract 5.1)",
                      "a planned accession with no acceptance time is listed in missing.jsonl, never dropped",
                      "an accession whose co-registrant sources disagree is quarantined in conflicts.jsonl with every source row, "
                      "and is excluded from acceptance_8k.jsonl until resolved against the authoritative filing header"]}
    _atomic_write(out / "reconciliation.json", (json.dumps(rec, indent=1, sort_keys=True) + "\n").encode())
    return {k: rec[k] for k in ("planned", "accepted", "conflicted", "missing", "problem_count", "rows_sha256")}


# ---------------------------------------------------------------------------------------------- verify


def parse_header_acceptance(sgml: str) -> datetime:
    """ACCEPTANCE-DATETIME in a filing header is Eastern wall time 'YYYYMMDDHHMMSS'."""
    for ln in sgml.splitlines():
        if ln.startswith("<ACCEPTANCE-DATETIME>"):
            v = ln.split(">", 1)[1].strip()
            return datetime.strptime(v[:14], "%Y%m%d%H%M%S")
    raise ValueError("no ACCEPTANCE-DATETIME in header")


def _header_matches(naive_et: datetime, api_utc: datetime) -> bool:
    # a wall time in the repeated November hour has two instants; accept either (EDGAR is closed then anyway)
    return any(naive_et.replace(tzinfo=ET, fold=f).astimezone(UTC) == api_utc for f in (0, 1))


def stage_verify(out: Path, fetch: Fetch, *, sample: int, seed: int = 20261126) -> dict:
    p, rp = out / "acceptance_8k.jsonl", out / "reconciliation.json"
    if not (p.exists() and rp.exists()):
        raise BackfillError("no acceptance_8k.jsonl / reconciliation.json: run the extract stage first")
    actual = sha256_bytes(p.read_bytes())
    if actual != json.loads(rp.read_text())["rows_sha256"]:
        raise BackfillError("acceptance_8k.jsonl does not match the hash in reconciliation.json: re-run extract")
    rows = [json.loads(ln) for ln in p.read_text().splitlines() if ln]
    pick = random.Random(seed).sample(rows, min(sample, len(rows)))
    results, bad = [], 0
    for r in sorted(pick, key=lambda r: r["accession"]):
        url = HEADER_URL.format(cik=int(r["ciks"][0]), nodash=r["accession"].replace("-", ""), acc=r["accession"])
        try:
            hdr = parse_header_acceptance(fetch(url).decode("latin-1"))
            ok = _header_matches(hdr, _parse_accept(r["acceptance_utc"]))
            res = {"accession": r["accession"], "match": ok, "header_et": hdr.isoformat(), "api_utc": r["acceptance_utc"]}
        except (FileNotFoundError, ValueError) as e:
            ok, res = False, {"accession": r["accession"], "match": False, "error": str(e)}
        bad += not ok
        results.append(res)
    rec = {"schema": SCHEMA, "seed": seed, "extracted_rows_sha256": actual, "population": len(rows), "requested_sample": sample,
           "achieved_sample": len(results), "sampled": len(results), "mismatches": bad, "results": results,
           "scope": "sampled accepted (non-conflicted) rows only; quarantined conflicts are not verified here",
           "note": "a mismatch means the submissions record is not trusted for that accession; zero mismatches is evidence, not proof"}
    _atomic_write(out / "verify.json", (json.dumps(rec, indent=1, sort_keys=True) + "\n").encode())
    return {"sampled": len(results), "mismatches": bad}


# ---------------------------------------------------------------------------------------------- CLI


def main(argv: list[str] | None = None, *, fetch: Fetch | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("stage", choices=("plan", "fetch", "extract", "verify"))
    ap.add_argument("--out", required=True, type=Path, help="output directory; must be outside the repo and griddb")
    ap.add_argument("--start-year", type=int, default=2012)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--limit-ciks", type=int, default=None, help="pilot: only the first N planned CIKs")
    ap.add_argument("--sample", type=int, default=200, help="verify: number of accessions to re-read from headers")
    ap.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_S, help="seconds between requests (>= 0.1)")
    a = ap.parse_args(argv)
    if a.interval < 0.1:
        print("refusing: the SEC limit is 10 requests/s; --interval must be at least 0.1", file=sys.stderr)
        return 2
    out = a.out.resolve()
    repo = Path(__file__).resolve().parents[1]
    if repo == out or repo in out.parents:
        print("refusing: --out is inside the repository", file=sys.stderr)
        return 2
    fetch = fetch or make_http_fetch(interval_s=a.interval)
    try:
        if a.stage == "plan":
            res = stage_plan(out, fetch, a.start_year, a.end_year)
            res = {k: v for k, v in res.items() if k != "quarters"}
        elif a.stage == "fetch":
            res = stage_fetch(out, fetch, limit_ciks=a.limit_ciks, start_year=a.start_year, end_year=a.end_year)
        elif a.stage == "extract":
            res = stage_extract(out)
        else:
            res = stage_verify(out, fetch, sample=a.sample)
    except Blocked as e:
        print(f"BLOCKED: {e}", file=sys.stderr)
        return 3
    except BackfillError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    print(json.dumps(res, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
