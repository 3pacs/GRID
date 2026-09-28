"""Fetch SEC EDGAR submissions JSON per issuer CIK and derive a CIK -> SIC map (VS1 v2 input).

Why: the VS1 v2 pre-registration
(``docs/paper_log/vs1-insider-density-v2-preregistration.md``) widens the
Technology universe to issuers whose SEC Standard Industrial Classification is
in 3570-3579, 3660-3679 or 7370-7379. ``company_tickers.json`` has no SIC; the
per-CIK submissions JSON (``https://data.sec.gov/submissions/CIK##########.json``)
does, together with the entity's current name, tickers, exchanges and its
``formerNames`` history.

Caveat carried into the pre-registration: ``sic`` in that JSON is the issuer's
**current** classification, not a point-in-time one.

Two steps, files only (no database connection, no DB writes, no price data):

    # 1. fetch (resumable; <= 8 requests/s; the SEC fair-access User-Agent)
    python -m scripts.fetch_sec_issuer_sic fetch \\
        --submissions /data/sec/form345/derived/submissions.parquet \\
        --out-dir /data/sec/vs2

    # 2. derive the map from the fetched files (write-once output)
    python -m scripts.fetch_sec_issuer_sic derive --out-dir /data/sec/vs2

``fetch`` reads the distinct ``issuer_cik`` of the SUBMISSION table and, for each
CIK without a final entry in ``<out-dir>/fetch_log.jsonl``, GETs its submissions
JSON, stores the exact response body gzip-compressed at
``<out-dir>/submissions_json/CIK##########.json.gz`` and appends one log line
(cik, url, HTTP status, fetched_at UTC, byte count, sha256 of the body). 200 and
404 are final; anything else is retried with backoff and, if still failing,
logged as non-final so a re-run picks it up.

``derive`` writes ``<out-dir>/issuer_sic_map.jsonl`` (one row per CIK: cik, name,
sic, sic_description, entity_type, tickers, exchanges, former_names,
fetched_at, body_sha256, http_status) and ``issuer_sic_map.receipt.json``
(file sha256, counts, the SUBMISSION table's sha256).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

USER_AGENT = "GRID Intelligence ops@stepdad.finance"
URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
MAX_RATE = 8.0  # SEC fair access: at most 10 requests/s; we stay at <= 8
FINAL_STATUSES = frozenset({200, 404})
LOG_NAME = "fetch_log.jsonl"
MAP_NAME = "issuer_sic_map.jsonl"
RECEIPT_NAME = "issuer_sic_map.receipt.json"
JSON_DIR = "submissions_json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def issuer_ciks(submissions: Path) -> list[int]:
    """Distinct issuer CIKs of the derived SUBMISSION table."""
    import pandas as pd
    import pyarrow.parquet as pq

    column = pq.read_table(submissions, columns=["issuer_cik"]).column(0).to_pandas()
    ciks = pd.to_numeric(column.astype("string").str.strip(), errors="coerce").dropna().astype("int64")
    return sorted(int(c) for c in ciks.unique() if c > 0)


def done_ciks(log_path: Path) -> set[int]:
    """CIKs with a final (200/404) entry in the fetch log."""
    done: set[int] = set()
    if not log_path.exists():
        return done
    with open(log_path, encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue  # a torn last line from an interrupted run
            if row.get("status") in FINAL_STATUSES:
                done.add(int(row["cik"]))
    return done


def _get(url: str, timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip", "Host": "data.sec.gov"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return response.status, body
    except urllib.error.HTTPError as error:
        return error.code, b""


class _RateLimiter:
    """At most ``rate`` request starts per second across every worker thread."""

    def __init__(self, rate: float) -> None:
        self.interval = 1.0 / rate
        self.next_at = time.monotonic()
        self.lock = threading.Lock()

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_at)
            self.next_at = start + self.interval
        if start > now:
            time.sleep(start - now)


def _fetch_one(cik: int, out_dir: Path, limiter: _RateLimiter, timeout: float, retries: int) -> dict:
    url = URL.format(cik=cik)
    status, body = 0, b""
    for attempt in range(retries + 1):
        limiter.wait()
        try:
            status, body = _get(url, timeout)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            status, body = -1, b""
        if status in FINAL_STATUSES:
            break
        time.sleep(min(60.0, 2.0 ** (attempt + 1)))  # 429/5xx/network: back off
    row = {"cik": cik, "url": url, "status": status, "fetched_at": _now(), "bytes": len(body),
           "sha256": hashlib.sha256(body).hexdigest() if status == 200 else None}
    if status == 200:
        target = out_dir / JSON_DIR / f"CIK{cik:010d}.json.gz"
        tmp = target.with_suffix(".gz.tmp")
        tmp.write_bytes(gzip.compress(body, 6))
        tmp.replace(target)
    return row


def fetch(submissions: Path, out_dir: Path, *, rate: float = 7.0, limit: int | None = None,
          timeout: float = 30.0, retries: int = 4, workers: int = 3) -> dict:
    """Fetch every issuer CIK's submissions JSON (resumable; ``rate`` caps request starts/s)."""
    from concurrent.futures import ThreadPoolExecutor

    if rate > MAX_RATE:
        raise SystemExit(f"rate {rate}/s exceeds the {MAX_RATE}/s ceiling")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / JSON_DIR).mkdir(exist_ok=True)
    log_path = out_dir / LOG_NAME
    ciks = issuer_ciks(submissions)
    done = done_ciks(log_path)
    todo = [c for c in ciks if c not in done]
    if limit is not None:
        todo = todo[:limit]
    limiter = _RateLimiter(rate)
    counts = {"issuer_ciks": len(ciks), "todo": len(todo), "200": 0, "404": 0, "other": 0}
    with open(log_path, "a", encoding="utf-8") as log, ThreadPoolExecutor(max_workers=workers) as pool:
        rows = pool.map(lambda c: _fetch_one(c, out_dir, limiter, timeout, retries), todo)
        for i, row in enumerate(rows):
            key = str(row["status"]) if row["status"] in FINAL_STATUSES else "other"
            counts[key] += 1
            log.write(json.dumps(row, sort_keys=True) + "\n")
            log.flush()
            if (i + 1) % 500 == 0:
                print(json.dumps({"progress": i + 1, "of": len(todo), **counts}), flush=True)
    return counts


def _latest_log(log_path: Path) -> dict[int, dict]:
    latest: dict[int, dict] = {}
    with open(log_path, encoding="utf-8") as stream:
        for line in stream:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            latest[int(row["cik"])] = row
    return latest


def map_row(cik: int, body: bytes | None, log_row: dict) -> dict:
    """One CIK's derived map row (fields straight from the JSON; nothing inferred)."""
    row = {"cik": cik, "http_status": log_row.get("status"), "fetched_at": log_row.get("fetched_at"),
           "body_sha256": log_row.get("sha256"), "name": None, "sic": None, "sic_description": None,
           "entity_type": None, "tickers": [], "exchanges": [], "former_names": []}
    if body is None:
        return row
    raw = json.loads(body)
    sic = str(raw.get("sic") or "").strip()
    row.update({
        "name": raw.get("name"),
        "sic": int(sic) if sic.isdigit() else None,
        "sic_description": raw.get("sicDescription") or None,
        "entity_type": raw.get("entityType") or None,
        "tickers": [str(t).strip().upper() for t in (raw.get("tickers") or []) if t],
        "exchanges": [e for e in (raw.get("exchanges") or []) if e],
        "former_names": [
            {"name": f.get("name"), "from": (f.get("from") or "")[:10] or None, "to": (f.get("to") or "")[:10] or None}
            for f in (raw.get("formerNames") or [])
        ],
    })
    return row


def derive(out_dir: Path, submissions: Path | None = None) -> dict:
    target = out_dir / MAP_NAME
    if target.exists():
        raise SystemExit(f"{target} exists (write-once)")
    latest = _latest_log(out_dir / LOG_NAME)
    rows = []
    for cik in sorted(latest):
        log_row = latest[cik]
        body = None
        if log_row.get("status") == 200:
            data = gzip.decompress((out_dir / JSON_DIR / f"CIK{cik:010d}.json.gz").read_bytes())
            if hashlib.sha256(data).hexdigest() != log_row.get("sha256"):
                raise SystemExit(f"CIK {cik}: stored body does not hash to its fetch-log sha256")
            body = data
        rows.append(map_row(cik, body, log_row))
    tmp = target.with_suffix(".tmp")
    with open(tmp, "x", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")
    tmp.replace(target)
    counts = {
        "ciks": len(rows),
        "status_200": sum(r["http_status"] == 200 for r in rows),
        "status_404": sum(r["http_status"] == 404 for r in rows),
        "status_other": sum(r["http_status"] not in FINAL_STATUSES for r in rows),
        "with_sic": sum(r["sic"] is not None for r in rows),
        "with_ticker": sum(bool(r["tickers"]) for r in rows),
    }
    receipt = {
        "built_at": _now(),
        "map": {"name": MAP_NAME, "sha256": file_sha256(target), "bytes": target.stat().st_size},
        "fetch_log_sha256": file_sha256(out_dir / LOG_NAME),
        "fetched_at_range": [min(r["fetched_at"] for r in rows if r["fetched_at"]),
                             max(r["fetched_at"] for r in rows if r["fetched_at"])] if rows else None,
        "source": "https://data.sec.gov/submissions/CIK##########.json",
        "user_agent": USER_AGENT,
        "caveat": "sic is the issuer's CURRENT classification at fetch time, not point-in-time",
        "counts": counts,
    }
    if submissions is not None:
        receipt["submissions_sha256"] = file_sha256(submissions)
    (out_dir / RECEIPT_NAME).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return receipt


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("fetch")
    p.add_argument("--submissions", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--rate", type=float, default=7.0, help=f"requests per second (<= {MAX_RATE})")
    p.add_argument("--limit", type=int)
    p.add_argument("--workers", type=int, default=3)
    p = sub.add_parser("derive")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--submissions")
    args = parser.parse_args(argv)
    if args.command == "fetch":
        print(json.dumps(fetch(Path(args.submissions), Path(args.out_dir), rate=args.rate, limit=args.limit,
                               workers=args.workers)))
    else:
        submissions = Path(args.submissions) if args.submissions else None
        print(json.dumps(derive(Path(args.out_dir), submissions), indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
