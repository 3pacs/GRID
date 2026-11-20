"""Older-data pre-screen for a module-factory candidate: one registered look before a live slot.

Why it exists (owner's decision, 2026-11-20). A factory candidate's evidence
is its live slot: one look at 100 resolved fires. Measured on the ledger, the
rules the factory ranks fire on 5 to 30% of sessions, so a slot takes one to
five years to reach that look, and there are ten slots. This module gives a
selected candidate a first, fast test on sessions the factory has never read:
the store behind edge search 9, 2018-05-01 to 2022-11-11, older than the
development window. Only a candidate that passes takes a slot. The live look
stays the test.

What it is not. A pass is "held on older data", not "works now": the window
is 2019's rise, the 2020 crash and recovery, 2021's rise and the 2022 fall.
The store is not untouched either: edge search 9 spent one look on it for
three other rules. Every registration must say so (``prior_use``), a trial
takes one look, and the store takes ten looks, ever
(:data:`analysis.ledger_steered_exploration.PRESCREEN_MAX_LOOKS`).

How a look is computed
----------------------
* **The factory's own arithmetic.** The rows come from
  :func:`analysis.module_factory.build_rows`, run for the one registered
  module with the settings of the run that selected it (z history, tape
  history, cost): the same features, the same eligibility (prior session
  complete, every minute from 09:30 to the decision), the same entry (open of
  the minute after the decision) and exit (close of the horizon's last
  minute). The target unit is dollars a share, as in the live look.
* **A pinned calendar.** XNYS sessions for 2018-04-02 to 2022-11-11
  (:data:`PINS`), built with ``scripts/build_factory_calendar_manifest.py``
  from the library version the factory's other manifests name. No observed-day
  fallback.
* **Warm-up.** Sessions before :data:`FIRES_FROM` (2019-01-02) only fill the
  histories; no fire before it counts (edge search 9's split).
* **The test.** PASS needs both, each at 0.05 / 10:
  the seeded bootstrap lower bound of the mean net of the fires is above zero
  (the live look's statistic), and so is the bound of that mean minus the mean
  net of taking the same side at the same time on every eligible session, so
  a long rule cannot pass on a rising market alone. Fewer than 40 fires is
  UNDERPOWERED. Anything else is FAIL. Means by year are reported, not tested.

Guards
------
* :func:`load_older` refuses a data file whose sha256 is not the registered
  one and a file holding any session on or after the development window. The
  factory's own loader refuses the opposite; neither can read the other's
  data.
* Only SPY-only price features and tape features are supported: the older
  store has no complete bars for the other symbols and no event, options or
  macro history.
* The trial, the store's remaining looks and the registration are checked
  before the data file is opened; the outcome is appended to the factory
  ledger (``factory_prescreen_outcome``), which re-checks it on every reopen.

Registrations name a selected rule, so they are kept out of this public
repository; this module reads one by path and records its sha256.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import random
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from analysis.factory_session_calendar import FactoryCalendar
from ingestion.market_calendar import ManifestCalendar

VERSION = "factory-prescreen/1"
SCHEMA = "factory-prescreen-registration/1"
STORE_ID = "edge9-older-2018-2022"
FIRST_SESSION = "2018-05-01"
LAST_SESSION = "2022-11-11"
FIRES_FROM = "2019-01-02"
BOOT_B = 10000
BOOT_SEED = 20261120
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
    "pass": "bound(mean net of fires) > 0 and bound(mean net of fires - mean net of always taking the side) > 0",
}
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
    feature, rule, side = module.split("|")
    window = family.split("|")[-1]
    decision, horizon = window.split("+")
    if mf.FEATURE_CLASS.get(feature) not in CLASSES:
        raise PrescreenError(f"{feature} is a {mf.FEATURE_CLASS.get(feature)} feature: the older store cannot supply it")
    body = dict(declared["run_file"])
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


def load_older(db_path: Path, data_sha256: str, symbol: str = "SPY") -> dict[str, mf.Session]:
    """{day: Session} of the older store, read-only. Refuses another file, and any development session in it."""
    if sha256_file(db_path) != data_sha256:
        raise PrescreenError("the data file is not the registered one (sha256 differs)")
    con = sqlite3.connect(f"file:{quote(str(Path(db_path).absolute()))}?mode=ro", uri=True)
    try:
        hi = con.execute("select max(day) from bars where sym = ?", (symbol,)).fetchone()[0]
        if hi is None or hi >= mf.DEV_FIRST:
            raise PrescreenError(f"the data file holds {symbol} sessions up to {hi}: development sessions refused")
        rows = con.execute(
            "select day, hm, o, h, l, c, v from bars where sym = ? and day between ? and ? order by day, hm",
            (symbol, FIRST_SESSION, LAST_SESSION),
        ).fetchall()
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


def _quantile(values: list[float], alpha: float) -> float:
    values.sort()
    return values[max(0, int(alpha * len(values)) - 1)]


def boot_lower(nets: list[float], alpha: float, b: int = BOOT_B, seed: int = BOOT_SEED) -> float:
    """One-sided lower bound of the mean at ``alpha``. The live look's bound (gex-signals ``factory_slots.boot_lower``)."""
    rnd, n = random.Random(seed), len(nets)
    return _quantile([sum(nets[rnd.randrange(n)] for _ in range(n)) / n for _ in range(b)], alpha)


def boot_excess_lower(rows: list[dict], alpha: float, b: int = BOOT_B, seed: int = BOOT_SEED + 1) -> float:
    """Lower bound of (mean net of the fires) - (mean net of every eligible session), resampling whole sessions.

    A resample without a fire counts as minus infinity: it cannot help a pass.
    """
    rnd, n = random.Random(seed), len(rows)
    pairs = [(r["net"], r["fired"]) for r in rows]
    draws = []
    for _ in range(b):
        total = fired_total = 0.0
        fired = 0
        for _ in range(n):
            net, f = pairs[rnd.randrange(n)]
            total += net
            if f:
                fired_total += net
                fired += 1
        draws.append(fired_total / fired - total / n if fired else float("-inf"))
    return _quantile(draws, alpha)


def look(rows: list[dict], b: int = BOOT_B) -> dict:
    """The verdict of one look on :func:`session_rows`. Pure and seeded: the same rows give the same bytes."""
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
        "mean_net": sum(nets) / n if n else None,
        "baseline": {"n": len(rows), "mean_net": sum(r["net"] for r in rows) / len(rows) if rows else None},
        "by_year": {y: {"n": len(v), "mean_net": round(sum(v) / len(v), 6)} for y, v in sorted(by_year.items())},
        "first_fire": min((r["day"] for r in rows if r["fired"]), default=None),
        "last_fire": max((r["day"] for r in rows if r["fired"]), default=None),
        "lower_bound": None,
        "excess_mean": None,
        "excess_lower_bound": None,
    }
    if n < lse.PRESCREEN_MIN_FIRES:
        return {**out, "verdict": "underpowered"}
    out["lower_bound"] = round(boot_lower(nets, alpha, b), 6)
    out["excess_mean"] = round(out["mean_net"] - out["baseline"]["mean_net"], 6)
    out["excess_lower_bound"] = round(boot_excess_lower(rows, alpha, b), 6)
    out["mean_net"] = round(out["mean_net"], 6)
    out["baseline"]["mean_net"] = round(out["baseline"]["mean_net"], 6)
    passed = out["lower_bound"] > 0 and out["excess_lower_bound"] > 0
    return {**out, "verdict": "pass" if passed else "fail"}


def prescreen(registration: Path, data: Path, ledger: lse.Ledger, *, calendar: FactoryCalendar | None = None,
              looked_at: str | None = None) -> dict:
    """Run the one registered look and append it to ``ledger``. Returns ``{record, sha256}``.

    Order: the registration, the trial and the store's remaining looks are checked before the data file is opened.
    A refusal raises :class:`PrescreenError` or ``ValueError`` and appends nothing.
    """
    if not ledger.is_factory:
        raise PrescreenError("pre-screen looks belong on a factory ledger")
    reg, reg_sha = load_registration(registration)
    candidate = reg["candidate"]
    target = {
        "run_id": candidate["run_id"],
        "trial_id": candidate["trial_id"],
        "result_sha256": candidate["result_sha256"],
        "registration_sha256": reg_sha,
        "store": {"id": STORE_ID, "data_sha256": reg["data_sha256"], "first_session": FIRST_SESSION,
                  "last_session": LAST_SESSION, "fires_from": FIRES_FROM},
    }
    result = lse.check_factory_prescreen_target(target, lse.factory_result_index(ledger),
                                                ledger.of_kind(lse.FACTORY_PRESCREEN))
    if lse.stamp(reg["registered_at"]) < lse.stamp(result["recorded_at"]):
        raise PrescreenError("the registration predates the result it names")
    run, module, family = candidate_run(ledger, candidate)
    calendar = calendar or OlderCalendar()
    sessions = load_older(Path(data), reg["data_sha256"], run.symbol)
    rows, coverage = session_rows(run, module, family, sessions, calendar)
    outcome = look(rows)
    record = {
        **target,
        **outcome,
        "rule": {"family": family, "feature": module},
        "version": VERSION,
        "boot": {"b": BOOT_B, "seed": BOOT_SEED},
        "calendar": calendar.version,
        "coverage": {k: coverage[k] for k in ("sessions_read", "sessions_eligible", "first_eligible", "last_eligible")},
        "looked_at": looked_at or datetime.now(timezone.utc).isoformat(),
    }
    return lse.record_factory_prescreen(ledger, record, recorded_at=record["looked_at"])
