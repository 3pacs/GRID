"""Vendor fetches for the VS1 v6 price-admission probe: files only, never the database.

VS1 v6 §2.3 (``docs/paper_log/vs1-insider-density-v6-preregistration.md``) needs two
things from outside the database, both saved to files (gzip) with a per-request
sha256 and fetch time in an append-only fetch log:

* **TwelveData** ``time_series`` (``interval=1day``, the window 2011-11-02..2019-12-31
  inclusive, requested as ``start_date=2011-11-02, end_date=2020-01-01`` because TwelveData's
  ``end_date`` is exclusive and 2020-01-01 is a market holiday), once with ``adjust=all`` and
  once with ``adjust=none``, per ticker, for the return cross-check and the splice corroboration.
  Any returned row dated on or after 2020-01-01 fails closed. Key: env
  ``TWELVEDATA_API_KEY``. The Basic plan allows 8 requests/minute and 800/day;
  the fetcher spaces requests, reads ``/api_usage`` to keep a daily reserve for the
  production pullers sharing the key, and resumes where it stopped.
* **Tiingo** ``/tiingo/daily/{T}`` metadata (``startDate``, ``endDate``, ``name``,
  ``exchangeCode``) for the C1 listing cross-check and the entity check. Key: env
  ``TIINGO_API_KEY``.

Keys travel only in the ``Authorization`` header; no URL, log line or error message
carries them. Nothing here computes a return or touches a Form 4 event, and no
request reaches 2020-01-01 or later.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from analysis import panel_insider_density_v6 as v6

HOLDOUT_START = date(2020, 1, 1)
TD_URL = "https://api.twelvedata.com/time_series"
TD_USAGE_URL = "https://api.twelvedata.com/api_usage"
TIINGO_META_URL = "https://api.tiingo.com/tiingo/daily/{ticker}"
TD_ADJUST_MODES = ("all", "none")
#: The pinned window (v6 §2.3), inclusive: the discovery read window, both adjust modes.
TD_START, TD_WINDOW_END = v6.CROSSCHECK.discovery_window
#: TwelveData's ``end_date`` is exclusive, so the inclusive window is requested with the bound
#: 2020-01-01, a market holiday: the response ends at 2019-12-31 and carries no 2020 row.
TD_END_EXCLUSIVE = HOLDOUT_START.isoformat()
#: Receipts made earlier with ``end_date=2019-12-31`` are completed by this short request (not a refetch).
TD_SUPPLEMENT_START = "2019-12-20"
TD_WINDOW_NOTE = ("window implemented inclusively via an exclusive end bound "
                  "(end_date=2020-01-01, a market holiday)")
#: TwelveData returns at most 5000 points; the window has about 2,050 sessions.
TD_OUTPUTSIZE = 5000
TD_KEY_ENV = "TWELVEDATA_API_KEY"
TIINGO_KEY_ENV = "TIINGO_API_KEY"

#: Seconds between TwelveData requests: 6/min, under the plan's 8/min, leaving room for the
#: production pullers that share the key.
TD_SPACING_S = 10.0
#: Stop (or wait for the UTC-midnight reset) when the day's usage reaches plan limit minus this.
TD_DAILY_RESERVE = 250
TD_USAGE_EVERY = 40
TIINGO_SPACING_S = 1.0

#: One HTTP GET: (url, params, headers) -> (status, body bytes). Tests inject a fake.
HttpGet = Callable[[str, Mapping[str, Any], Mapping[str, str]], "tuple[int, bytes]"]


class FetchStopped(RuntimeError):
    """The fetch stopped before finishing (daily cap, plan cannot serve the window)."""


def requests_get(url: str, params: Mapping[str, Any], headers: Mapping[str, str]) -> tuple[int, bytes]:
    """The default transport. A network error is re-raised without its message (it may echo the URL)."""
    import requests

    try:
        resp = requests.get(url, params=dict(params), headers=dict(headers), timeout=60)
    except requests.RequestException as exc:  # never propagate a message that could carry a header or URL
        raise ConnectionError(type(exc).__name__) from None
    return resp.status_code, resp.content


def api_key(env_name: str) -> str:
    key = os.environ.get(env_name, "").strip()
    if not key:
        raise FetchStopped(f"environment variable {env_name} is not set")
    return key


def _now() -> datetime:
    return datetime.now(timezone.utc)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_gz(path: Path, body: bytes) -> str:
    """Deterministic gzip (mtime 0) written atomically; returns the file's sha256."""
    data = gzip.compress(body, mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    with open(tmp, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    return sha256_bytes(data)


def read_gz(path: Path) -> bytes:
    return gzip.decompress(Path(path).read_bytes())


class FetchLog:
    """Append-only JSONL receipts. The last final entry per key wins (a rerun resumes after it)."""

    FINAL = frozenset({"ok", "unavailable"})

    def __init__(self, path: Path):
        self.path = Path(path)

    def entries(self) -> list[dict]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def final(self) -> dict[str, dict]:
        """key -> the latest final entry whose file still hashes to its receipt."""
        out: dict[str, dict] = {}
        for entry in self.entries():
            if entry.get("outcome") not in self.FINAL:
                continue
            f = self.path.parent / entry["file"]
            if f.exists() and sha256_bytes(f.read_bytes()) == entry["file_sha256"]:
                out[entry["key"]] = entry
        return out

    def append(self, entry: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(dict(entry), sort_keys=True, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())


def check_request_window(start: str, end_exclusive: str) -> None:
    """A TwelveData request's ``end_date`` is an exclusive bound: it may be at most 2020-01-01."""
    if (date.fromisoformat(end_exclusive) > HOLDOUT_START
            or date.fromisoformat(start) >= date.fromisoformat(end_exclusive)):
        raise FetchStopped(f"refused: a vendor request may not reach past {HOLDOUT_START.isoformat()} (exclusive "
                           "bound; holdout period)")


def td_symbol(ticker: str) -> str:
    """TwelveData writes share classes with a dot (BRK.B); SEC/Tiingo tickers use a dash."""
    return ticker.replace("-", ".")


def td_params(ticker: str, adjust: str, *, start: str = TD_START,
              end_exclusive: str = TD_END_EXCLUSIVE) -> dict[str, Any]:
    if adjust not in TD_ADJUST_MODES:
        raise ValueError(adjust)
    check_request_window(start, end_exclusive)
    return {"symbol": td_symbol(ticker), "interval": "1day", "start_date": start, "end_date": end_exclusive,
            "adjust": adjust, "outputsize": TD_OUTPUTSIZE}


def td_summary(body: bytes) -> dict:
    """Status, meta and date coverage of a TwelveData body (dates only; no price leaves this function)."""
    try:
        doc = json.loads(body)
    except ValueError:
        return {"td_status": "unparseable"}
    values = doc.get("values") if isinstance(doc, dict) else None
    dates = sorted(str(v.get("datetime", ""))[:10] for v in values or [] if isinstance(v, dict))
    meta = doc.get("meta") if isinstance(doc, dict) and isinstance(doc.get("meta"), dict) else {}
    return {"td_status": doc.get("status") if isinstance(doc, dict) else None,
            "td_code": doc.get("code") if isinstance(doc, dict) else None,
            "td_message": str(doc.get("message", ""))[:200] if isinstance(doc, dict) else "",
            "values": len(dates), "first": dates[0] if dates else None, "last": dates[-1] if dates else None,
            "holdout_rows": sum(1 for d in dates if d >= HOLDOUT_START.isoformat()),
            "meta": {k: meta.get(k) for k in ("symbol", "exchange", "mic_code", "type", "currency",
                                              "exchange_timezone")}}


@dataclass
class TdUsage:
    daily_usage: int
    plan_daily_limit: int
    plan_limit: int
    plan_category: str


def td_usage(http_get: HttpGet, key: str) -> TdUsage:
    status, body = http_get(TD_USAGE_URL, {}, {"Authorization": f"apikey {key}"})
    doc = json.loads(body) if status == 200 else {}
    if "plan_daily_limit" not in doc:
        raise FetchStopped(f"TwelveData /api_usage unavailable (HTTP {status})")
    return TdUsage(int(doc.get("daily_usage", 0)), int(doc["plan_daily_limit"]), int(doc.get("plan_limit", 0)),
                   str(doc.get("plan_category", "")))


def _seconds_to_utc_reset(now: datetime) -> float:
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
    return max(60.0, (nxt - now).total_seconds())


def needs_supplement(entry: Mapping[str, Any] | None) -> bool:
    """A window receipt requested with ``end_date=2019-12-31`` (exclusive at TwelveData) lacks 2019-12-31."""
    return (bool(entry) and entry.get("outcome") == "ok"
            and (entry.get("params") or {}).get("end_date") == TD_WINDOW_END)


@dataclass(frozen=True)
class _Job:
    key: str
    ticker: str
    adjust: str
    params: dict
    file: str
    kind: str  # "window" or "supplement"


def _jobs(order: Sequence[str], done: Mapping[str, dict]) -> list[_Job]:
    jobs = []
    for t in order:
        for a in TD_ADJUST_MODES:
            main = done.get(f"{t}|{a}")
            if main is None:
                jobs.append(_Job(f"{t}|{a}", t, a, td_params(t, a), f"{t}/{a}.json.gz", "window"))
            elif needs_supplement(main) and f"{t}|{a}|supplement" not in done:
                jobs.append(_Job(f"{t}|{a}|supplement", t, a, td_params(t, a, start=TD_SUPPLEMENT_START),
                                 f"{t}/{a}.supplement.json.gz", "supplement"))
    return jobs


def fetch_twelvedata(
    tickers: Sequence[str],
    out_dir: Path,
    *,
    benchmark: str,
    key: str,
    http_get: HttpGet = requests_get,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = _now,
    spacing_s: float = TD_SPACING_S,
    daily_reserve: int = TD_DAILY_RESERVE,
    usage_every: int = TD_USAGE_EVERY,
    wait_for_reset: bool = False,
    max_retries: int = 4,
    progress: Callable[[str], None] | None = None,
) -> dict:
    """Fetch ``adjust=all`` and ``adjust=none`` for every ticker, benchmark first; resumable.

    The window 2011-11-02..2019-12-31 is requested inclusively through the exclusive bound
    ``end_date=2020-01-01`` (a market holiday). A receipt made earlier with ``end_date=2019-12-31``
    is completed by a supplementary ``2019-12-20..2020-01-01`` request instead of a refetch. A
    response carrying any row dated on or after 2020-01-01 is not saved and stops the fetch (fail
    closed). The benchmark is the plan check (v6 §2.3): if TwelveData does not return both modes
    from the window start to 2019-12-31, the fetch stops and the owner decides.
    """
    out_dir = Path(out_dir)
    log = FetchLog(out_dir / "fetch_log.jsonl")
    done = log.final()
    order = [benchmark] + sorted(set(tickers) - {benchmark})
    todo = _jobs(order, done)
    say = progress or (lambda _m: None)
    counts = {"skipped_done": len(order) * len(TD_ADJUST_MODES) - sum(1 for j in todo if j.kind == "window"),
              "ok": 0, "unavailable": 0, "error": 0, "supplements": sum(1 for j in todo if j.kind == "supplement")}
    usage = td_usage(http_get, key)
    say(f"TwelveData plan {usage.plan_category}: {usage.daily_usage}/{usage.plan_daily_limit} today, "
        f"{len(todo)} requests to go ({counts['supplements']} supplements)")
    since_usage = 0
    last_request = 0.0

    def budget_ok() -> bool:
        return usage.daily_usage + 1 <= usage.plan_daily_limit - daily_reserve

    for i, job in enumerate(todo):
        while not budget_ok():
            if not wait_for_reset:
                log.append({"key": "_stop", "outcome": "daily_cap", "at": now().isoformat(),
                            "daily_usage": usage.daily_usage, "plan_daily_limit": usage.plan_daily_limit,
                            "reserve": daily_reserve})
                return {"stopped": "daily_cap", "remaining": len(todo) - i, **counts}
            wait = _seconds_to_utc_reset(now())
            say(f"daily budget reached ({usage.daily_usage}/{usage.plan_daily_limit}, reserve {daily_reserve}); "
                f"waiting {int(wait)} s for the UTC reset")
            sleep(wait)
            usage = td_usage(http_get, key)
            since_usage = 0
        entry: dict[str, Any] = {}
        for attempt in range(max_retries + 1):
            entry = {}
            gap = spacing_s - (time.monotonic() - last_request)
            if gap > 0:
                sleep(gap)
            fetched_at = now()
            last_request = time.monotonic()
            try:
                status, body = http_get(TD_URL, job.params, {"Authorization": f"apikey {key}"})
            except ConnectionError as exc:
                status, body = None, b""
                entry = {"error": str(exc)}
            usage.daily_usage += 1
            since_usage += 1
            summary = td_summary(body) if body else {}
            if summary.get("holdout_rows"):
                # fail closed: a row dated 2020-01-01 or later is never written to disk
                log.append({"key": job.key, "ticker": job.ticker, "adjust": job.adjust, "kind": job.kind,
                            "outcome": "holdout_rows", "url": TD_URL, "params": job.params,
                            "fetched_at": fetched_at.isoformat(), "http_status": status,
                            **{k: v for k, v in summary.items() if k != "meta"}})
                raise FetchStopped(f"TwelveData returned {summary['holdout_rows']} row(s) dated on or after "
                                   f"{HOLDOUT_START.isoformat()} for {job.key}; not saved, fetch stopped")
            if status == 200 and summary.get("td_status") == "ok" and summary.get("values"):
                outcome = "ok"
            elif (status in (200, 400, 404) and summary.get("td_status") == "error"
                  and summary.get("td_code") in (400, 404)):
                outcome = "unavailable"  # symbol unknown to TwelveData / no data for the dates (HTTP 200 or 400)
            elif status == 429 or summary.get("td_code") == 429:
                entry = {}
                sleep(65.0)  # the minute window is full (shared key): wait it out and retry
                continue
            else:
                outcome = "error"
            entry = {"key": job.key, "ticker": job.ticker, "adjust": job.adjust, "kind": job.kind,
                     "outcome": outcome, "url": TD_URL, "params": job.params, "fetched_at": fetched_at.isoformat(),
                     "http_status": status, "attempt": attempt, **summary,
                     **({"error": entry["error"]} if "error" in entry else {})}
            if body:
                entry.update({"file": job.file, "body_sha256": sha256_bytes(body), "body_bytes": len(body),
                              "file_sha256": write_gz(out_dir / job.file, body)})
            if outcome != "error" or attempt == max_retries:
                break
            log.append(entry)  # a transient failure is receipted too, then retried
            sleep(min(120.0, 5.0 * 2 ** attempt))
        if entry.get("outcome") is None:  # every attempt hit the rate limit
            entry = {"key": job.key, "ticker": job.ticker, "adjust": job.adjust, "kind": job.kind,
                     "outcome": "error", "url": TD_URL, "params": job.params, "fetched_at": now().isoformat(),
                     "error": "rate_limited"}
        log.append(entry)
        counts[entry["outcome"]] += 1
        if job.ticker == benchmark:
            _benchmark_plan_check(entry, job.kind, stop_log=log)
        if since_usage >= usage_every:
            usage = td_usage(http_get, key)
            since_usage = 0
        if (i + 1) % 20 == 0 or i + 1 == len(todo):
            say(f"TwelveData {i + 1}/{len(todo)} ({counts})")
    return {"stopped": None, "remaining": 0, **counts}


def _benchmark_plan_check(entry: Mapping[str, Any], kind: str, stop_log: FetchLog) -> None:
    """Both adjust modes must come back for the benchmark from the window's first to its last session."""
    ok = entry.get("outcome") == "ok" and entry.get("last") == TD_WINDOW_END
    if kind == "window":
        ok = ok and entry.get("first") == TD_START
    if ok:
        return
    stop_log.append({"key": "_stop", "outcome": "plan_cannot_serve_window", "benchmark_entry": entry.get("key"),
                     "first": entry.get("first"), "last": entry.get("last"), "td_code": entry.get("td_code")})
    raise FetchStopped(f"TwelveData did not return {entry.get('key')} over {TD_START}..{TD_WINDOW_END}: the "
                       "cross-check cannot run as specified (v6 §2.3); stop, the owner decides")


def fetch_tiingo_meta(
    tickers: Sequence[str],
    out_dir: Path,
    *,
    key: str,
    http_get: HttpGet = requests_get,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = _now,
    spacing_s: float = TIINGO_SPACING_S,
    max_retries: int = 3,
    progress: Callable[[str], None] | None = None,
) -> dict:
    """``/tiingo/daily/{T}`` per ticker to files (metadata only: no price is requested); resumable."""
    out_dir = Path(out_dir)
    log = FetchLog(out_dir / "fetch_log.jsonl")
    done = log.final()
    todo = [t for t in sorted(set(tickers)) if t not in done]
    counts = {"skipped_done": len(set(tickers)) - len(todo), "ok": 0, "unavailable": 0, "error": 0}
    say = progress or (lambda _m: None)
    for i, ticker in enumerate(todo):
        url = TIINGO_META_URL.format(ticker=ticker)
        entry: dict[str, Any] = {}
        for attempt in range(max_retries + 1):
            if i or attempt:
                sleep(spacing_s)
            fetched_at = now()
            try:
                status, body = http_get(url, {}, {"Authorization": f"Token {key}", "Content-Type": "application/json"})
            except ConnectionError as exc:
                status, body, err = None, b"", str(exc)
            else:
                err = None
            doc = _json_or_none(body)
            if status == 200 and isinstance(doc, dict) and doc.get("ticker"):
                outcome = "ok"
            elif status == 404:
                outcome = "unavailable"
            else:
                outcome = "error"
            entry = {"key": ticker, "ticker": ticker, "outcome": outcome, "url": url,
                     "fetched_at": fetched_at.isoformat(), "http_status": status, "attempt": attempt,
                     **({"error": err} if err else {})}
            if isinstance(doc, dict):
                entry["meta"] = {k: doc.get(k) for k in ("ticker", "name", "exchangeCode", "startDate", "endDate")}
            if body:
                name = f"{ticker}.json.gz"
                entry.update({"file": name, "body_sha256": sha256_bytes(body), "body_bytes": len(body),
                              "file_sha256": write_gz(out_dir / name, body)})
            if outcome != "error" or attempt == max_retries:
                break
            log.append(entry)
            sleep(min(60.0, 5.0 * 2 ** attempt))
        log.append(entry)
        counts[entry["outcome"]] += 1
        if (i + 1) % 50 == 0 or i + 1 == len(todo):
            say(f"Tiingo meta {i + 1}/{len(todo)} ({counts})")
    return {"stopped": None, **counts}


def _json_or_none(body: bytes) -> Any:
    try:
        return json.loads(body) if body else None
    except ValueError:
        return None


# --- reading the saved files back (the probe) ---------------------------------------------------------


def _closes(out_dir: Path, entry: Mapping[str, Any]) -> tuple[dict[str, float], int]:
    """Window closes of one saved body, and the number of rows dated on or after 2020-01-01 (never returned)."""
    doc = json.loads(read_gz(Path(out_dir) / entry["file"]))
    closes, holdout = {}, 0
    for v in doc.get("values") or []:
        d = str(v.get("datetime", ""))[:10]
        if not d:
            continue
        if d >= HOLDOUT_START.isoformat():
            holdout += 1
            continue
        if d < TD_START:
            continue
        try:
            closes[d] = float(v["close"])
        except (KeyError, TypeError, ValueError):
            continue
    return closes, holdout


_RECEIPT_KEYS = ("outcome", "fetched_at", "body_sha256", "file_sha256", "file", "values", "first", "last", "meta",
                 "td_code", "params")


def load_td_closes(out_dir: Path, ticker: str, done: Mapping[str, dict] | None = None) -> dict:
    """``{"all": {iso: close}, "none": {...}, "state": ..., "receipts": {...}}`` from verified files.

    Per mode: the window file, merged with its 2019-12-20..2020-01-01 supplement when the window
    file was requested with ``end_date=2019-12-31`` (overlapping dates must agree exactly; only
    the dates the window file lacks are added). Fail closed: a mode whose files carry any row
    dated on or after 2020-01-01, whose supplement is missing, or whose supplement disagrees on
    an overlapping date is dropped, and ``state`` names why.
    """
    done = FetchLog(Path(out_dir) / "fetch_log.jsonl").final() if done is None else done
    out: dict[str, Any] = {"receipts": {}, "holdout_rows": 0, "supplemented": [], "problems": []}
    unavailable = 0
    for adjust in TD_ADJUST_MODES:
        entry = done.get(f"{ticker}|{adjust}")
        if entry is None:
            continue
        out["receipts"][adjust] = {k: entry.get(k) for k in _RECEIPT_KEYS}
        if entry["outcome"] != "ok":
            unavailable += 1
            continue
        closes, holdout = _closes(out_dir, entry)
        supp = done.get(f"{ticker}|{adjust}|supplement")
        if supp is not None:
            out["receipts"][f"{adjust}_supplement"] = {k: supp.get(k) for k in _RECEIPT_KEYS}
        if needs_supplement(entry):
            if supp is None:
                out["problems"].append(f"{adjust}:supplement_missing")
                continue
            if supp["outcome"] == "ok":
                extra, extra_holdout = _closes(out_dir, supp)
                holdout += extra_holdout
                if any(d in closes and extra[d] != closes[d] for d in extra):
                    out["problems"].append(f"{adjust}:supplement_mismatch")
                    continue
                added = sorted(set(extra) - set(closes))
                closes.update({d: extra[d] for d in added})
                out["supplemented"].append({"adjust": adjust, "added_dates": added})
        if holdout:
            out["holdout_rows"] += holdout
            out["problems"].append(f"{adjust}:holdout_rows")
            continue
        out[adjust] = closes
    if all(a in out for a in TD_ADJUST_MODES):
        out["state"] = "ok"
    elif out["holdout_rows"]:
        out["state"] = "holdout_rows"
    elif out["problems"]:
        out["state"] = out["problems"][0].split(":", 1)[1]
    elif unavailable:
        out["state"] = "unavailable"
    else:
        out["state"] = "not_fetched"
    return out


def load_tiingo_meta(out_dir: Path, ticker: str, done: Mapping[str, dict] | None = None) -> dict | None:
    """The saved ``/tiingo/daily/{T}`` metadata with its receipt; None when never fetched to a final receipt."""
    done = FetchLog(Path(out_dir) / "fetch_log.jsonl").final() if done is None else done
    entry = done.get(ticker)
    if entry is None:
        return None
    receipt = {k: entry.get(k) for k in ("outcome", "fetched_at", "body_sha256", "file_sha256", "file")}
    if entry["outcome"] != "ok":
        return {"receipt": receipt, "meta": None}
    doc = json.loads(read_gz(Path(out_dir) / entry["file"]))
    meta = {k: doc.get(k) for k in ("ticker", "name", "exchangeCode", "startDate", "endDate")}
    return {"receipt": receipt, "meta": meta}


def fetch_log_sha256(out_dir: Path) -> str | None:
    path = Path(out_dir) / "fetch_log.jsonl"
    return sha256_bytes(path.read_bytes()) if path.exists() else None


def tickers_from_file(path: Path) -> list[str]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    items: Iterable[Any] = raw["issuers"] if isinstance(raw, dict) else raw
    out = [str(x["ticker"] if isinstance(x, dict) else x).strip().upper() for x in items]
    if not out or any(not t for t in out):
        raise ValueError("tickers file has no tickers or an empty ticker")
    return sorted(set(out))
