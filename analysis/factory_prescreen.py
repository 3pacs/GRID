"""Older-data pre-screen for a module-factory candidate: one registered look before a live slot.

Why it exists (owner's decision, 2026-11-20). A factory candidate's evidence
is its live slot: one look at 100 resolved fires. Measured on the ledger, the
rules the factory ranks fire on 5 to 40% of sessions, so a slot takes one to
five years to reach that look, and there are ten slots. This module gives a
selected candidate a first, fast test on sessions the factory has never read:
the store behind edge search 9, 2018-05-01 to 2022-11-11, older than the
development window. Only a candidate that passes takes a slot. The live look
stays the test.

What it is not. A pass is "held on older data", not "works now": the window
is 2019's rise, the 2020 crash and recovery, 2021's rise and the 2022 fall.
The store has been looked at before: edge search 9 spent one look on it for
three other rules. So a pass is a screen on previously exposed data and is
recorded as that (``evidence``), never as independent evidence. Every
registration must say what the store was used for (``prior_use``); a trial
is registered once; the store takes ten registrations, ever
(:data:`analysis.ledger_steered_exploration.PRESCREEN_MAX_LOOKS`).

Two steps, two ledger records
-----------------------------
1. :func:`register` appends ``factory_prescreen_registered`` for one
   registration file. It reads the ledger and the file, and no older data.
   The hash chain puts the registration before the look; a timestamp inside
   a file could not.
2. :func:`prescreen` runs the look for a registration already on the ledger
   and appends ``factory_prescreen_outcome``. It needs an independent witness
   that the registration existed before the look: the nightly Desk source
   receipt, which reads the factory ledger and records its head in a log of
   its own (:func:`ledger_witness`). The look is told where that log is, where
   an anchor copy held outside it is, and which session. It opens and verifies
   them itself and takes the ledger head from the receipt's payload; it
   accepts no head, count or place stated by the caller. The look's time is
   this module's clock, not an argument. What code cannot check is said on
   the record and not claimed: that the anchor copy is the one held off-host.

How a look is computed
----------------------
* **The factory's own arithmetic.** The rows come from
  :func:`analysis.module_factory.build_rows`, run for the one registered
  module with the settings of the run that selected it (z history, tape
  history, cost): the same features, the same eligibility (prior session
  complete, every minute from 09:30 to the decision), the same entry (open of
  the minute after the decision) and exit (close of the horizon's last
  minute). The target unit is dollars a share, as in the live look.
* **One window, no skipped windows.** The declared run's ``skip_windows``
  only remove whole (decision time, horizon) windows from that run. A window
  that was skipped has no trial, so a registered rule's window was never one
  of them; the reduced run therefore carries none. A registration whose
  window is in the declared ``skip_windows`` is refused. A skipped window
  changes no other window's eligibility, history or labels (tested).
* **A pinned calendar.** XNYS sessions for 2018-04-02 to 2022-11-11
  (:data:`PINS`), built with ``scripts/build_factory_calendar_manifest.py``
  from the library version the factory's other manifests name. No observed-day
  fallback.
* **Warm-up.** Sessions before :data:`FIRES_FROM` (2019-01-02) only fill the
  histories; no fire before it counts (edge search 9's split).
* **The test.** PASS needs all three:

  1. the lower bound of the mean net of the fires is above zero;
  2. the lower bound of that mean minus the mean net of taking the same side
     at the same time on every eligible session is above zero, so a long rule
     cannot pass on a rising market alone;
  3. the mean net of the fires without the five largest is above zero, so a
     pass cannot rest on a handful of sessions.

  Both bounds are at 0.05 / 10 and come from one seeded circular block
  bootstrap over the eligible sessions in date order (blocks of
  :data:`BLOCK` sessions), so fires on neighbouring days are resampled
  together and not treated as independent. Fewer than 40 fires is
  UNDERPOWERED. Anything else is FAIL. Means by year and the share of the
  total net from the five largest fires are reported, not tested.

Guards
------
* :func:`load_older` reads the data file's bytes once, checks their sha256
  against the registration, and queries those same bytes (an in-memory copy):
  what is hashed is what is read. A ``-wal``, ``-shm`` or ``-journal`` file
  beside it, or a file in WAL mode, is refused. So is a file holding any
  session on or after the development window, or two rows for one minute.
  The factory's own loader refuses the opposite; neither can read the other's
  data.
* Only SPY-only price features and tape features are supported: the older
  store has no complete bars for the other symbols and no event, options or
  macro history.
* The trial, the registration and the witness are checked before the data
  file is opened; the ledger re-checks every record on every reopen.

Registrations name a selected rule, so they are kept out of this public
repository; this module reads one by path and records its sha256.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import random
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from analysis import desk_receipt_witness
from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from analysis.factory_session_calendar import FactoryCalendar
from ingestion.market_calendar import ManifestCalendar

VERSION = "factory-prescreen/2"
SCHEMA = "factory-prescreen-registration/1"
STORE_ID = "edge9-older-2018-2022"
FIRST_SESSION = "2018-05-01"
LAST_SESSION = "2022-11-11"
FIRES_FROM = "2019-01-02"
BOOT_B = 10000
BOOT_SEED = 20261120
BLOCK = 5    # eligible sessions resampled together: fires on neighbouring days are not independent
TRIM = 5     # the largest fires left out of the concentration check
PINS = (
    ("prescreen-xnys-2018-04-02-2020-12-31.json", "88312f9a547c809737a8c94b5d9b51e0e5f38117cb51ecaec2bd639ced7138ba"),
    ("prescreen-xnys-2021-01-01-2022-11-11.json", "aee287ed567cf53c072d2378aa6437fe9bfcbbe8b6186fcc8bf4ab6109bc4dc5"),
)
CLASSES = ("price", "tape")
# The terms every registration must carry unchanged. A different test is a new version of this module.
TERMS = {
    "version": VERSION,
    "store": STORE_ID,
    "first_session": FIRST_SESSION,
    "last_session": LAST_SESSION,
    "fires_from": FIRES_FROM,
    "alpha": lse.PRESCREEN_ALPHA,
    "looks": lse.PRESCREEN_MAX_LOOKS,
    "min_fires": lse.PRESCREEN_MIN_FIRES,
    "boot_b": BOOT_B,
    "boot_seed": BOOT_SEED,
    "block_sessions": BLOCK,
    "trim_fires": TRIM,
    "pass": ("block-bootstrap bound(mean net of fires) > 0 and bound(mean net of fires - mean net of always taking "
             "the side) > 0 and mean net of fires without the largest trim_fires > 0"),
    "evidence": lse.PRESCREEN_EVIDENCE,
}
TERMS_SHA256 = hashlib.sha256(lse.canonical(TERMS)).hexdigest()
REGISTRATION_FIELDS = ("schema", "candidate", "data_sha256", "terms", "prior_use", "registered_at")
CANDIDATE_FIELDS = ("run_id", "trial_id", "result_sha256", "family", "feature")


class PrescreenError(ValueError):
    """A look that is refused. Nothing was appended."""


class OlderCalendar(FactoryCalendar):
    """The pinned XNYS schedule of the older store. Same reader as the factory's calendar, other manifests."""

    def __init__(self, root: Path | None = None) -> None:
        root = root or Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
        self.parts = tuple(ManifestCalendar(Path(root) / name, sha) for name, sha in PINS)
        self.sessions = tuple(s for p in self.parts for s in p.sessions)
        self.by_day = {s.key.split(":")[1]: s for s in self.sessions}
        self.index = {s.key.split(":")[1]: i for i, s in enumerate(self.sessions)}
        self.version = "pinned XNYS: " + ",".join(sha for _, sha in PINS)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_registration(path: Path) -> tuple[dict, str]:
    """(registration, sha256 of the file). Refuses any shape or terms other than this version's."""
    raw = Path(path).read_bytes()
    try:
        reg = json.loads(raw)
    except ValueError as exc:
        raise PrescreenError(f"the registration is not JSON: {exc}") from None
    if not isinstance(reg, dict) or set(reg) != set(REGISTRATION_FIELDS) or reg.get("schema") != SCHEMA:
        raise PrescreenError(f"a registration has exactly {REGISTRATION_FIELDS} with schema {SCHEMA}")
    candidate = reg["candidate"]
    if not isinstance(candidate, dict) or set(candidate) != set(CANDIDATE_FIELDS) \
            or not all(isinstance(candidate[k], str) and candidate[k] for k in CANDIDATE_FIELDS):
        raise PrescreenError(f"the candidate names exactly {CANDIDATE_FIELDS}")
    if reg["terms"] != TERMS:
        raise PrescreenError("the registration's terms are not this version's terms, unchanged")
    if not lse._hex64(reg["data_sha256"]):
        raise PrescreenError("data_sha256 is the sha256 of the older-data file")
    if not isinstance(reg["prior_use"], str) or len(reg["prior_use"].strip()) < 40:
        raise PrescreenError("prior_use states, in words, what this store has already been looked at for")
    lse.stamp(reg["registered_at"])
    return reg, hashlib.sha256(raw).hexdigest()


def candidate_run(ledger: lse.Ledger, candidate: dict) -> tuple[mf.RunFile, str, str]:
    """(run file for the one module on the older window, module name, family) from the ledger's own records."""
    result = lse.factory_result_index(ledger).get(candidate["result_sha256"])
    if result is None or result["run_id"] != candidate["run_id"]:
        raise PrescreenError("the candidate's result is not on this ledger")
    trial = next((t for t in result["trials"] if t["trial_id"] == candidate["trial_id"]), None)
    if trial is None:
        raise PrescreenError("the candidate's trial is not in that result")
    if (trial["family"], trial["feature"]) != (candidate["family"], candidate["feature"]):
        raise PrescreenError("the registered rule is not the rule of that trial")
    declared = next(r for r in ledger.of_kind(lse.FACTORY_DECLARED) if r["run_id"] == result["run_id"])
    family, module = trial["family"], trial["feature"]
    parts, window = module.split("|"), family.split("|")[-1].split("+")
    if len(parts) != 3 or len(window) != 2 or not family.startswith("SPY|change|"):
        raise PrescreenError(f"{family} {module} is not a SPY intraday module: the pre-screen cannot compute it")
    (feature, rule, side), (decision, horizon) = parts, window
    if mf.FEATURE_CLASS.get(feature) not in CLASSES:
        raise PrescreenError(f"{feature} is a {mf.FEATURE_CLASS.get(feature)} feature: the older store cannot supply it")
    body = dict(declared["run_file"])
    if f"{decision}+{horizon}" in (body.get("skip_windows") or ()):
        raise PrescreenError(f"{decision}+{horizon} is a skipped window of its run: it has no trial to pre-screen")
    for key in ("class_shares", "oi_archive_sha256", "macro_sha256"):
        body.pop(key, None)
    body.update(run_id=f"prescreen-{candidate['trial_id'][:12]}", features=[feature], decision_times=[decision],
                horizons=[horizon], sides=[side], skip_windows=[], target_unit="dollars")
    if rule != "on":
        body["z_thresholds"] = [float(rule[3:])]
    try:
        run = mf.RunFile.from_dict(body)   # every check of the factory, on the run's own development window
    except mf.RunFileError as exc:
        raise PrescreenError(f"the candidate's run file does not reduce to one module: {exc}") from None
    if run.symbols() != (run.symbol,):
        raise PrescreenError(f"{feature} reads {run.symbols()}: the older store holds complete bars for SPY only")
    if family not in run.families() or module not in run.modules():
        raise PrescreenError("the reduced run file does not hold the registered module")
    # The one deliberate step outside the factory's window check: this module's whole purpose is the older window.
    return dataclasses.replace(run, first_session=FIRST_SESSION, last_session=LAST_SESSION), module, family


SIDECARS = ("-wal", "-shm", "-journal")


def _sealed(db_path: Path, data_sha256: str) -> sqlite3.Connection:
    """A connection onto the exact bytes whose sha256 is registered: what is hashed is what is queried."""
    path = Path(db_path)
    present = [suffix for suffix in SIDECARS if Path(str(path) + suffix).exists()]
    if present:
        raise PrescreenError(f"a sidecar file sits beside the data file ({', '.join(present)}): refused")
    raw = path.read_bytes()   # the only read of the path
    if hashlib.sha256(raw).hexdigest() != data_sha256:
        raise PrescreenError("the data file is not the registered one (sha256 differs)")
    if raw[:16] != b"SQLite format 3\x00" or raw[18:20] != b"\x01\x01":
        raise PrescreenError("the data file is not a rollback-journal SQLite file (WAL mode or another format): refused")
    con = sqlite3.connect(":memory:")
    if hasattr(con, "deserialize"):
        con.deserialize(raw)
        return con
    con.close()   # Python 3.10: a private copy of the same bytes, opened immutable, removed once loaded
    with tempfile.TemporaryDirectory(prefix="prescreen-sealed-") as scratch:
        copy = Path(scratch) / "bars.db"
        copy.write_bytes(raw)
        disk = sqlite3.connect(f"file:{copy}?mode=ro&immutable=1", uri=True)
        memory = sqlite3.connect(":memory:")
        disk.backup(memory)
        disk.close()
    return memory


def load_older(db_path: Path, data_sha256: str, symbol: str = "SPY") -> dict[str, mf.Session]:
    """{day: Session} of the older store. Refuses another file, a sidecar, a development session, a repeated minute."""
    con = _sealed(db_path, data_sha256)
    try:
        hi = con.execute("select max(day) from bars where sym = ?", (symbol,)).fetchone()[0]
        if hi is None or hi >= mf.DEV_FIRST:
            raise PrescreenError(f"the data file holds {symbol} sessions up to {hi}: development sessions refused")
        twice = con.execute(
            "select day, hm from bars where sym = ? group by day, hm having count(*) > 1 limit 1", (symbol,)
        ).fetchone()
        if twice:
            raise PrescreenError(f"the data file holds two rows for {symbol} {twice[0]} {twice[1]}: refused, not overwritten")
        rows = con.execute(
            "select day, hm, o, h, l, c, v from bars where sym = ? and day between ? and ? order by day, hm",
            (symbol, FIRST_SESSION, LAST_SESSION),
        ).fetchall()
    except sqlite3.Error as exc:
        raise PrescreenError(f"the data file could not be read ({type(exc).__name__})") from None
    finally:
        con.close()
    by_day: dict[str, dict] = {}
    for day, hm, o, h, low, c, v in rows:
        by_day.setdefault(day, {})[hm] = (o, h, low, c, v)
    if not by_day:
        raise PrescreenError("the data file holds no sessions in the registered window")
    return {day: mf.Session(bars) for day, bars in by_day.items()}


def session_rows(run: mf.RunFile, module: str, family: str, sessions: dict[str, mf.Session],
                 calendar: FactoryCalendar) -> tuple[list[dict], dict]:
    """One row per eligible session from :data:`FIRES_FROM`: ``{day, fired, net}``, and the builder's coverage.

    ``net`` is what taking the rule's side at that time made that session, after the cost, whether or not the rule
    fired: the fires are the rule's record, all rows together are the always-on baseline.
    """
    families, coverage = mf.build_rows(sessions, run, calendar=calendar)
    side = mf.SIDES[module.split("|")[2]]
    rows = []
    for row in families[family]:
        day = row["decision_at"][:10]
        if day < FIRES_FROM:
            continue
        rows.append({"day": day, "fired": row["features"][module]["value"] is not None,
                     "net": side * row["target_dollars"] - run.cost_per_share})
    return rows, coverage


def block_bounds(rows: list[dict], alpha: float, b: int = BOOT_B, seed: int = BOOT_SEED,
                 block: int = BLOCK) -> tuple[float, float]:
    """(lower bound of the mean net of the fires, lower bound of that mean minus the mean net of every session).

    One circular moving-block bootstrap over the eligible sessions in date order: each resample is
    ``ceil(n / block)`` runs of ``block`` consecutive sessions. A resample without a fire counts as minus
    infinity for both: it cannot help a pass. The block starts come from Python's seeded generator.
    """
    n = len(rows)
    blocks = -(-n // block)
    net = np.array([r["net"] for r in rows], dtype=float)
    fired = np.array([1.0 if r["fired"] else 0.0 for r in rows])

    def prefix(x: np.ndarray) -> np.ndarray:   # prefix sums of the series wrapped once, so a block may run past the end
        return np.concatenate(([0.0], np.cumsum(np.concatenate((x, x[:block])))))

    rnd = random.Random(seed)
    starts = np.array([rnd.randrange(n) for _ in range(b * blocks)], dtype=np.int64).reshape(b, blocks)
    total, fired_total, fired_count = ((c[starts + block] - c[starts]).sum(axis=1)
                                       for c in (prefix(net), prefix(net * fired), prefix(fired)))
    some = fired_count > 0.5
    fired_mean = np.where(some, fired_total / np.where(some, fired_count, 1.0), -np.inf)
    excess = np.where(some, fired_mean - total / (blocks * block), -np.inf)
    q = max(0, int(alpha * b) - 1)
    return float(np.sort(fired_mean)[q]), float(np.sort(excess)[q])


def look(rows: list[dict], b: int = BOOT_B) -> dict:
    """The verdict of one look on :func:`session_rows`. Pure and seeded: the same rows give the same numbers."""
    alpha = lse.PRESCREEN_ALPHA / lse.PRESCREEN_MAX_LOOKS
    nets = [r["net"] for r in rows if r["fired"]]
    n = len(nets)
    by_year: dict[str, list[float]] = {}
    for r in rows:
        if r["fired"]:
            by_year.setdefault(r["day"][:4], []).append(r["net"])
    out = {
        "n": n,
        "alpha_per_test": alpha,
        "sessions_eligible": len(rows),
        "mean_net": round(sum(nets) / n, 6) if n else None,
        "baseline": {"n": len(rows), "mean_net": round(sum(r["net"] for r in rows) / len(rows), 6) if rows else None},
        "by_year": {y: {"n": len(v), "mean_net": round(sum(v) / len(v), 6)} for y, v in sorted(by_year.items())},
        "first_fire": min((r["day"] for r in rows if r["fired"]), default=None),
        "last_fire": max((r["day"] for r in rows if r["fired"]), default=None),
        "lower_bound": None,
        "excess_mean": None,
        "excess_lower_bound": None,
        "trimmed_mean_net": None,
        "top_share": None,
        "evidence": lse.PRESCREEN_EVIDENCE,
    }
    if n < lse.PRESCREEN_MIN_FIRES:
        return {**out, "verdict": "underpowered"}
    lower, excess = block_bounds(rows, alpha, b)
    kept = sorted(nets)[:-TRIM]
    total = sum(nets)
    out.update(
        lower_bound=round(lower, 6),
        excess_mean=round(sum(nets) / n - sum(r["net"] for r in rows) / len(rows), 6),
        excess_lower_bound=round(excess, 6),
        trimmed_mean_net=round(sum(kept) / len(kept), 6),
        # share of the total net from the TRIM largest fires; None when the total is not positive
        top_share=round(sum(sorted(nets)[-TRIM:]) / total, 4) if total > 0 else None,
    )
    passed = out["lower_bound"] > 0 and out["excess_lower_bound"] > 0 and out["trimmed_mean_net"] > 0
    return {**out, "verdict": "pass" if passed else "fail"}


def _target(reg: dict, reg_sha: str, module: str, family: str) -> dict:
    candidate = reg["candidate"]
    return {
        "run_id": candidate["run_id"],
        "trial_id": candidate["trial_id"],
        "result_sha256": candidate["result_sha256"],
        "registration_sha256": reg_sha,
        "rule": {"family": family, "feature": module},
        "store": {"id": STORE_ID, "data_sha256": reg["data_sha256"], "first_session": FIRST_SESSION,
                  "last_session": LAST_SESSION, "fires_from": FIRES_FROM, "previously_exposed": True},
        "version": VERSION,
        "terms_sha256": TERMS_SHA256,
    }


def register(registration: Path, ledger: lse.Ledger, *, recorded_at: str | None = None) -> dict:
    """Append the registration to ``ledger`` before any older data is read. Returns ``{record, sha256}``.

    It opens the registration file and the ledger, and nothing else: no data path is taken.
    """
    if not ledger.is_factory:
        raise PrescreenError("pre-screen registrations belong on a factory ledger")
    reg, reg_sha = load_registration(registration)
    _run, module, family = candidate_run(ledger, reg["candidate"])
    record = {**_target(reg, reg_sha, module, family), "prior_use": reg["prior_use"], "registered_at": reg["registered_at"]}
    return lse.record_factory_prescreen_registered(ledger, record, recorded_at=recorded_at)


def _clock() -> str:
    """The time of a look: one clock read. It is not an argument of :func:`prescreen`."""
    return datetime.now(timezone.utc).isoformat()


def ledger_witness(ledger: lse.Ledger, receipts: Path, external_anchors: Path, session: str) -> dict:
    """A head of ``ledger`` that a verified Desk receipt read, as the ``registration_witness`` of a look.

    The receipt log is opened and verified against the anchor copy (``desk_receipt_witness.verified_receipt``), and
    the head and record count are taken from the receipt's ``factory_ledger`` payload, never from the caller. The
    payload must be about this ledger and name one of its own lines. Whether that head is at or after the
    registration is the ledger's rule.
    """
    receipt = desk_receipt_witness.verified_receipt(Path(receipts), Path(external_anchors), session, "factory_ledger")
    body = json.loads(receipt["payload"])
    summary = body.get("summary") if isinstance(body, dict) else None
    records = summary.get("records") if isinstance(summary, dict) else None
    if not isinstance(body, dict) or body.get("ledger_id") != ledger.genesis["ledger_id"] or type(records) is not int \
            or not 0 < records <= len(ledger.records) or ledger.line_sha(records - 1) != body.get("head_sha256"):
        raise PrescreenError("that receipt's factory-ledger read is not a head of this ledger")
    return {
        "kind": "desk-source-receipt", "ledger_records": records, "ledger_head_sha256": body["head_sha256"],
        "receipt": {k: receipt[k] for k in ("session", "head_sha256", "log_records", "read_finished_at", "payload_sha256",
                                             "external_anchor")},
        "checked": {"by_code": [*receipt["checked"]["by_code"],
                                "the receipt's factory-ledger payload names a line of this ledger as its head"],
                    "by_a_person": receipt["checked"]["by_a_person"]},
    }


def prescreen(registration: Path, data: Path, ledger: lse.Ledger, witness: dict, *,
              calendar: FactoryCalendar | None = None) -> dict:
    """Run the look of a registration that is already on ``ledger`` and append it. Returns ``{record, sha256}``.

    ``witness``: where the independent witness is, ``{receipts, external_anchors, session}``: the Desk receipt
    log's folder, an anchor copy held outside it, and the session whose receipt read this ledger at or after the
    registration. Nothing else is accepted in it: a head, a count or a place stated by the caller is refused.
    Order: the registration record, the trial and the witness are checked before the data file is opened.
    A refusal raises :class:`PrescreenError` or ``ValueError`` and appends nothing.
    """
    if not ledger.is_factory:
        raise PrescreenError("pre-screen looks belong on a factory ledger")
    reg, reg_sha = load_registration(registration)
    run, module, family = candidate_run(ledger, reg["candidate"])
    registrations = lse.factory_prescreen_registrations(ledger)
    registered_sha = next((sha for sha, r in registrations.items() if r["registration_sha256"] == reg_sha), None)
    if registered_sha is None:
        raise PrescreenError("this registration is not on the ledger: register it first, before any look")
    if not isinstance(witness, dict) or set(witness) != {"receipts", "external_anchors", "session"}:
        raise PrescreenError("a look is told where its witness is and nothing else: receipts, external_anchors, session")
    seen = ledger_witness(ledger, witness["receipts"], witness["external_anchors"], witness["session"])
    looked_at = _clock()
    target = {**_target(reg, reg_sha, module, family), "registered_sha256": registered_sha, "registration_witness": seen}
    # every rule of the ledger that does not need the data, before the data: a provisional underpowered look
    provisional = {**target, "verdict": "underpowered", "n": 0, "alpha_per_test": lse.PRESCREEN_ALPHA / lse.PRESCREEN_MAX_LOOKS,
                   "evidence": lse.PRESCREEN_EVIDENCE, "looked_at": looked_at}
    lse.validate_factory_prescreen(provisional, lse.factory_result_index(ledger), registrations,
                                   ledger.of_kind(lse.FACTORY_PRESCREEN), [ledger.line_sha(r["seq"]) for r in ledger.records])
    calendar = calendar or OlderCalendar()
    sessions = load_older(Path(data), reg["data_sha256"], run.symbol)
    rows, coverage = session_rows(run, module, family, sessions, calendar)
    record = {
        **target,
        **look(rows),
        "boot": {"b": BOOT_B, "seed": BOOT_SEED, "block_sessions": BLOCK},
        "calendar": calendar.version,
        "coverage": {k: coverage[k] for k in ("sessions_read", "sessions_eligible", "first_eligible", "last_eligible")},
        "looked_at": looked_at,
    }
    return lse.record_factory_prescreen(ledger, record, recorded_at=looked_at)
