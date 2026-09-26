"""Ledger-steered exploration for the offline research contract (S11).

The "self-improving" part of the hypothesis loop, and nothing more: a global,
append-only, hash-chained ledger of every trial ever declared decides which
hypothesis *families* (feature class x target x horizon) get the next run's
trial budget, while the error budget is kept honest across all runs.

Outputs are files and ledger records only. This module has no DB, provider,
route, timer or registry wiring. It never changes weights, never promotes and
never rescores; every record carries ``promotion_allowed: false``.

Flow (one run)
--------------
1. :func:`allocate` reads the ledger only (never data), scores each family by
   its ledger outcomes, and appends an ``allocation`` record that freezes the
   run's declared trial list, its alpha and its windows *before any data is
   touched*.
2. :func:`protocol_for_allocation` turns that record into the run's
   :class:`~analysis.offline_research_proof.Protocol`
   (``selection="ledger_holm"``, ``trials`` = the declared list,
   ``selection_alpha`` = the issued alpha, ``allocation_sha256`` = the record
   hash). The contract (``discover``/``evaluate_holdout``) runs unchanged.
3. :func:`record_run` checks the frozen discovery manifest and the holdout
   result against the open allocation and appends a ``run_result`` record
   with every declared trial's p-value, status and holdout outcome.
4. :func:`ingest_forward_outcomes` (later, S10) appends final forward-log
   verdicts for holdout survivors from an input file.

Allocation policy
-----------------
Each eligible family is a Beta-Bernoulli arm. A trial is a success when it was
selected in discovery *and* survived its holdout (or, later, passed its
forward log); every other declared trial of that family (not selected, failed
holdout, untestable, failed forward) is a failure. The next run's budget is
split by Thompson sampling's allocation probability -- the posterior
probability that each arm has the highest yield, computed by quadrature
(no sampling noise, so identical arms get identical shares) -- after an
exploration floor of ``floor`` trials for every eligible arm. Exact ties in
the largest-remainder split are broken by a seeded hash, never by name. Within an arm, features that succeeded before
are re-tested first (replication on fresh data), then the least-tested ones.

Ineligible, always: ``SELF_LAG`` families (a target predicting itself or a
declared near-copy), excluded telemetry features, and any family without a
fresh window (below).

Cross-run error control
-----------------------
The ledger's genesis freezes a global level ``q`` (<= 0.10). Run ``k``
(counting every allocation, including abandoned ones) is issued
``alpha_k = q / (k (k + 1))``; these sum to ``q k / (k + 1) < q`` for ever.
Inside a run, discovery selects by Holm's step-down at ``alpha_k`` over every
declared trial (untestable ones at p = 1). Holm controls the family-wise error
at ``alpha_k`` under arbitrary dependence, so by the union bound the
probability of *any* false discovery across every run the ledger will ever
record is at most ``sum_k alpha_k < q``. FWER <= q implies FDR <= q. Holdout
confirmation (Bonferroni over the run's selections) only removes selections.

Why not LORD / SAFFRON / alpha-investing or a cumulative global BH: the
p-values here are dependent inside a run (every feature of a family shares
the same target labels, features are correlated) and across runs (shared
targets and features), and the online-FDR guarantees of LORD/SAFFRON/
alpha-investing need independence or PRDS/local-dependence conditions that
cannot be checked here. A global BH over the cumulative ledger needs PRDS,
re-decides old trials every time the denominator grows (a discovery acted on
in run 3 can be revoked by run 9, after its holdout was consumed), and its
repeated looks are not covered by a single-look guarantee. Alpha spending
with Holm is valid under any dependence; its cost is power, which the
allocator pays back by concentrating the budget (fewer trials in a run means
a larger Holm level per trial).

Windows (single-use holdout registry)
-------------------------------------
Adaptive allocation is only honest if a re-tested family's null p-values are
still valid given the outcomes that steered it there. Every run declares one
window ``[start, end)`` (discovery ``[start, split)`` + holdout
``[split, end)``) on *label* time, and the ledger refuses to allocate a family
whose earlier runs touched any part of it. A holdout is therefore never
re-tested, and a family is never re-tested on outcomes it has already seen:
once a family has spent the history, it can only be re-tested on new data
(e.g. S10's forward log). Other families can still use the same history. The
registry is per family; a caveat stays: families that share a target share
its labels, so their conditional validity rests on the permutation null
being valid given the target sequence.

Forward-log input (S10 interface)
---------------------------------
:func:`ingest_forward_outcomes` reads a JSONL file whose lines carry
:data:`FORWARD_OUTCOME_FIELDS`. Only final verdicts are accepted (``pass`` or
``fail``, after the pre-registered stop rule), only for ledger trials that
survived their holdout and whose ``candidate_sha256`` matches, and at most
one per trial. The file's sha256 is recorded with each verdict.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import betainc, betaln

from analysis.offline_research_proof import (
    LEDGER_SELECTION,
    SELF_LAG,
    Protocol,
    build_family_rows,
    digest,
    excluded,
    protocol_from_payload,
    run_proof,
    stamp,
)

LEDGER_VERSION = 1
SELF_LAG_CLASS = "SELF_LAG"
KEY_SEPARATOR = "::"
SPENDING = "alpha_k = q / (k * (k + 1)), k = 1, 2, ... (sums to q * k / (k + 1) < q)"
WITHIN_RUN = "holm"
WINDOW_RULE = (
    "a family may not be allocated a run whose label window [start, end) overlaps "
    "any window an earlier run of that family touched (single-use holdout, fresh "
    "discovery)"
)
MAX_Q = 0.10
PERMS_CAP = 20000
STATUSES = ("tested", "untestable", SELF_LAG)
HOLDOUT_OUTCOMES = ("survived", "failed", "not_selected")
FORWARD_OUTCOMES = ("pass", "fail")
FORWARD_OUTCOME_FIELDS = (
    "trial_id",  # the ledger trial (digest([run_id, family, feature]))
    "candidate_sha256",  # the frozen candidate the forward log followed
    "outcome",  # final verdict after the pre-registered stop rule: pass | fail
    "n",  # forward observations evaluated
    "evaluated_through",  # tz-aware ISO timestamp of the last forward outcome
    "prereg_sha256",  # the forward log's pre-registration hash
)
# Feature class of a real-panel series: its publication source (research_real_panel).
SOURCE_CLASSES = {
    "FRB_H15": "rates",
    "FRED_H15_SPREAD": "curve",
    "ICE_BOFA": "credit",
    "CBOE_VIX": "volatility",
    "FRB_H10": "fx",
    "FRB_H41": "liquidity",
    "NYFED_RRP": "liquidity",
    "FREDDIE_PMMS": "housing",
    "AAII": "sentiment",
}


def run_alpha(k: int, q: float) -> float:
    """The alpha issued to the k-th run (1-based) out of the global level q."""
    if k < 1:
        raise ValueError("runs are counted from 1")
    return q / (k * (k + 1))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical(record: dict) -> bytes:
    return json.dumps(
        record, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def trial_id(run_id: str, family: str, feature: str) -> str:
    """Same id the contract's ``discover`` gives the trial."""
    return digest([run_id, family, feature])


# --- catalog ------------------------------------------------------------------------


@dataclass(frozen=True)
class Catalog:
    """The declared universe the allocator may draw trials from.

    ``families`` are contract target families (``TARGET|label|fwdH``: target x
    horizon). ``classes`` maps every feature to its feature class. A
    (family, feature) pair in ``self_lag`` belongs to the ``SELF_LAG`` class
    for that family, whatever the feature's class is elsewhere.
    """

    families: tuple[str, ...]
    features: tuple[str, ...]
    classes: tuple[tuple[str, str], ...]
    self_lag: tuple[tuple[str, str], ...] = ()

    def validate(self) -> None:
        mapping = dict(self.classes)
        if (
            not self.families
            or not self.features
            or len(set(self.families)) != len(self.families)
            or len(set(self.features)) != len(self.features)
            or len(mapping) != len(self.classes)
            or set(mapping) != set(self.features)
        ):
            raise ValueError("catalog needs unique families/features and one class each")
        if any(
            not cls or cls == SELF_LAG_CLASS or KEY_SEPARATOR in cls
            for cls in mapping.values()
        ):
            raise ValueError("feature classes must be named and may not be SELF_LAG")
        if any(
            pair[0] not in self.families or pair[1] not in self.features
            for pair in self.self_lag
        ) or len(set(self.self_lag)) != len(self.self_lag):
            raise ValueError("self_lag pairs must name catalog families and features")

    def sha256(self) -> str:
        return digest(asdict(self))

    def feature_class(self, family: str, feature: str) -> str:
        if (family, feature) in set(self.self_lag):
            return SELF_LAG_CLASS
        return dict(self.classes)[feature]

    def family_key(self, family: str, feature: str) -> str:
        return f"{self.feature_class(family, feature)}{KEY_SEPARATOR}{family}"

    def arms(self) -> dict[str, dict]:
        """Every family key with its feature pool and whether it may ever be allocated."""
        out: dict[str, dict] = {}
        for family in self.families:
            for feature in self.features:
                key = self.family_key(family, feature)
                arm = out.setdefault(
                    key,
                    {
                        "family": family,
                        "feature_class": key.split(KEY_SEPARATOR, 1)[0],
                        "pool": [],
                        "excluded": [],
                    },
                )
                (arm["excluded"] if excluded(feature) else arm["pool"]).append(feature)
        for arm in out.values():
            if arm["feature_class"] == SELF_LAG_CLASS:
                arm["never"] = "self_lag: a target or its declared near-copy"
            elif not arm["pool"]:
                arm["never"] = "no allocatable features (excluded telemetry only)"
        return dict(sorted(out.items()))


def catalog_from_specs(features, targets, horizons) -> Catalog:
    """A catalog over a real-panel universe (no DB): class = publication source.

    Feature and family names and the ``self_lag`` pairs are derived exactly as
    ``analysis.research_real_panel.LatestVintagePanel`` derives them.
    """
    from analysis.research_real_panel import FEATURE_SUFFIXES, self_lag_pairs

    names, classes = [], []
    for spec in features:
        for suffix in FEATURE_SUFFIXES:
            name = f"{spec.series_id}|{suffix}"
            names.append(name)
            classes.append((name, SOURCE_CLASSES[spec.source]))
    families = tuple(f"{t.series_id}|{t.label}|fwd{h}" for t in targets for h in horizons)
    catalog = Catalog(
        families=families,
        features=tuple(names),
        classes=tuple(classes),
        self_lag=self_lag_pairs(families, tuple(names)),
    )
    catalog.validate()
    return catalog


# --- ledger -------------------------------------------------------------------------


class Ledger:
    """Append-only, hash-chained JSONL ledger (one canonical JSON record per line).

    Every record carries ``seq`` and ``prev_sha256`` (the sha256 of the
    previous line's bytes; ``None`` for the genesis record). The chain is
    re-verified on load and the file head is re-checked before every append,
    so an edited, reordered, truncated or concurrently appended file is
    refused. The chain cannot see an edit of the *last* record on its own:
    pass ``expected_head`` (the head recorded elsewhere, e.g. in a run's
    ``summary.json``) to anchor it. ``path=None`` keeps the ledger in memory
    (simulations only).
    """

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        expected_head: str | None = None,
        _lines=None,
    ) -> None:
        self.path = Path(path) if path is not None else None
        if _lines is None:
            if self.path is None:
                raise ValueError("an in-memory ledger is made with Ledger.create")
            _lines = self._read_lines()
        self._lines: list[bytes] = list(_lines)
        self._records: list[dict] = verify_chain(self._lines)
        if expected_head is not None and expected_head not in {
            sha256_bytes(line) for line in self._lines
        }:
            raise ValueError("ledger does not contain the expected head")

    # --- construction ---------------------------------------------------------------

    @classmethod
    def create(
        cls,
        path: str | Path | None,
        *,
        ledger_id: str,
        q: float = MAX_Q,
        recorded_at: str | None = None,
    ) -> Ledger:
        if not ledger_id or not 0 < q <= MAX_Q:
            raise ValueError(f"ledger needs an id and 0 < q <= {MAX_Q}")
        genesis = {
            "kind": "genesis",
            "seq": 0,
            "prev_sha256": None,
            "ledger_version": LEDGER_VERSION,
            "ledger_id": ledger_id,
            "q": q,
            "spending": SPENDING,
            "within_run": WITHIN_RUN,
            "window_rule": WINDOW_RULE,
            "promotion_allowed": False,
            "recorded_at": recorded_at or now_iso(),
        }
        line = canonical(genesis)
        if path is not None:
            with Path(path).open("xb") as stream:  # a new ledger never overwrites one
                stream.write(line + b"\n")
        return cls(path, _lines=[line])

    def _read_lines(self) -> list[bytes]:
        data = self.path.read_bytes()
        if not data.endswith(b"\n"):
            raise ValueError("ledger file is truncated (no trailing newline)")
        return data[:-1].split(b"\n")

    # --- chain ----------------------------------------------------------------------

    @property
    def records(self) -> tuple[dict, ...]:
        return tuple(json.loads(line) for line in self._lines)

    @property
    def head(self) -> str:
        return sha256_bytes(self._lines[-1])

    @property
    def genesis(self) -> dict:
        return self._records[0]

    @property
    def q(self) -> float:
        return self.genesis["q"]

    def line_sha(self, seq: int) -> str:
        return sha256_bytes(self._lines[seq])

    def verify(self, full: bool = False) -> None:
        """The file still holds exactly this ledger's lines (``full``: re-walk the chain).

        The chain is verified on load, and ``_append`` only ever extends it
        canonically, so the per-append check is a byte comparison.
        """
        if full:
            verify_chain(self._lines)
        if self.path is not None and self.path.read_bytes() != self._file_bytes():
            raise ValueError("ledger file changed outside this ledger")

    def _file_bytes(self) -> bytes:
        return b"".join(line + b"\n" for line in self._lines)

    def _append(self, record: dict) -> tuple[dict, str]:
        self.verify()
        record = {**record, "seq": len(self._lines), "prev_sha256": self.head}
        record.setdefault("promotion_allowed", False)
        if record["promotion_allowed"] is not False:
            raise ValueError("the ledger never allows promotion")
        line = canonical(record)
        if self.path is not None:
            with self.path.open("ab") as stream:
                stream.write(line + b"\n")
        self._lines.append(line)
        self._records.append(json.loads(line))
        return self._records[-1], sha256_bytes(line)

    # --- state ----------------------------------------------------------------------

    def of_kind(self, kind: str) -> list[dict]:
        return [r for r in self._records if r["kind"] == kind]

    def allocations(self) -> list[dict]:
        return self.of_kind("allocation")

    def open_allocation(self) -> tuple[dict, str] | None:
        """The last allocation if no result or abandonment has closed it."""
        allocations = self.allocations()
        if not allocations:
            return None
        last = allocations[-1]
        closed = {
            r["allocation_sha256"]
            for r in self._records
            if r["kind"] in ("run_result", "abandoned")
        }
        sha = self.line_sha(last["seq"])
        return None if sha in closed else (last, sha)

    def alpha_spent(self) -> float:
        return float(sum(a["alpha"] for a in self.allocations()))

    def touched_windows(self) -> dict[str, list[tuple[str, str]]]:
        """Label windows every family key has touched (allocated = touched)."""
        out: dict[str, list[tuple[str, str]]] = {}
        for allocation in self.allocations():
            window = (allocation["windows"]["start"], allocation["windows"]["end"])
            for key in sorted({t["family_key"] for t in allocation["trials"]}):
                out.setdefault(key, []).append(window)
        return out

    def trial_results(self) -> list[dict]:
        return [t for r in self.of_kind("run_result") for t in r["trials"]]

    def outcomes(self) -> dict[str, dict]:
        """Per family key and per (family key, feature): successes and failures."""
        families: dict[str, dict] = {}
        features: dict[tuple[str, str], dict] = {}

        def tally(key, feature, success):
            for bucket, k in ((families, key), (features, (key, feature))):
                entry = bucket.setdefault(k, {"successes": 0, "failures": 0})
                entry["successes" if success else "failures"] += 1

        by_id = {}
        for trial in self.trial_results():
            by_id[trial["trial_id"]] = trial
            tally(
                trial["family_key"],
                trial["feature"],
                trial["holdout_outcome"] == "survived",
            )
        for verdict in self.of_kind("forward_outcome"):
            trial = by_id[verdict["trial_id"]]
            tally(trial["family_key"], trial["feature"], verdict["outcome"] == "pass")
        return {"families": families, "features": features}


def verify_chain(lines: list[bytes]) -> list[dict]:
    if not lines:
        raise ValueError("empty ledger")
    records, previous = [], None
    for seq, line in enumerate(lines):
        record = json.loads(line)
        if canonical(record) != line:
            raise ValueError(f"record {seq}: not canonical JSON")
        if record.get("seq") != seq or record.get("prev_sha256") != previous:
            raise ValueError(f"record {seq}: hash chain broken")
        if (seq == 0) != (record.get("kind") == "genesis"):
            raise ValueError(f"record {seq}: genesis must be first and only first")
        if record.get("promotion_allowed") is not False:
            raise ValueError(f"record {seq}: promotion is never allowed")
        records.append(record)
        previous = sha256_bytes(line)
    return records


# --- allocation ---------------------------------------------------------------------


@dataclass(frozen=True)
class Policy:
    """Thompson-sampling allocation over family arms with an exploration floor.

    ``prior`` is the Beta prior on every arm's yield (>= 1 keeps the densities
    bounded for the quadrature); ``grid`` is the quadrature resolution; ``seed``
    breaks exact ties and orders untested features.
    """

    budget: int
    floor: int = 1
    prior: tuple[float, float] = (1.0, 1.0)
    grid: int = 4096
    seed: int = 20260926

    def validate(self) -> None:
        if (
            self.budget < 1
            or self.floor < 1
            or self.grid < 256
            or len(self.prior) != 2
            or min(self.prior) < 1
        ):
            raise ValueError("invalid allocation policy")


def overlaps(a: tuple[str, str], b: tuple[str, str]) -> bool:
    return stamp(a[0]) < stamp(b[1]) and stamp(b[0]) < stamp(a[1])


def validate_windows(windows: dict) -> dict:
    if set(windows) != {"start", "split", "end"}:
        raise ValueError("a run declares start, split and end")
    if not stamp(windows["start"]) < stamp(windows["split"]) < stamp(windows["end"]):
        raise ValueError("windows must satisfy start < split < end")
    return {k: windows[k] for k in ("start", "split", "end")}


def probability_best(posteriors: dict[str, tuple[float, float]], grid: int) -> dict:
    """Thompson sampling's allocation probability: P(arm has the highest yield).

    ``P(i best) = integral pdf_i(x) prod_{j != i} cdf_j(x) dx`` over Beta
    posteriors, by the midpoint rule on ``grid`` points, normalised to 1.
    """
    keys = sorted(posteriors)
    x = (np.arange(grid) + 0.5) / grid
    a = np.array([posteriors[k][0] for k in keys], dtype=float)[:, None]
    b = np.array([posteriors[k][1] for k in keys], dtype=float)[:, None]
    pdf = np.exp((a - 1) * np.log(x) + (b - 1) * np.log1p(-x) - betaln(a, b))
    log_cdf = np.log(np.clip(betainc(a, b, x), 1e-300, 1.0))
    others = np.exp(log_cdf.sum(axis=0) - log_cdf)
    mass = (pdf * others).sum(axis=1)
    return {k: float(v) for k, v in zip(keys, mass / mass.sum())}


def split_budget(
    shares: dict[str, float],
    capacity: dict[str, int],
    budget: int,
    floor: int,
    tiebreak: dict[str, str] | None = None,
) -> dict[str, int]:
    """Floor for every arm, then the pot by share (largest remainder), under caps.

    ``budget`` is a maximum. Each arm wants ``min(room, pot * share)`` extra
    trials; the part of the pot owned by an arm that is already at its pool
    size is left unspent instead of being pushed onto arms the posterior does
    not favour (exploration is the floor's job). Fewer trials in a run means a
    larger Holm level per trial. Exact remainder ties go to the arm with the
    smallest ``tiebreak`` value (default: its name).
    """
    tiebreak = tiebreak or {k: k for k in shares}
    keys = sorted(shares)
    counts = {k: min(floor, capacity[k]) for k in keys}
    if sum(counts.values()) > budget:
        raise ValueError(
            f"budget {budget} is below the exploration floor of {len(counts)} arms"
        )
    pot = budget - sum(counts.values())
    total_share = sum(shares.values())
    want = {
        k: min(capacity[k] - counts[k], pot * shares[k] / total_share if total_share else 0.0)
        for k in keys
    }
    extra = {k: math.floor(want[k]) for k in keys}
    left = math.floor(sum(want.values()) + 0.5) - sum(extra.values())
    for k in sorted(keys, key=lambda k: (-(want[k] - extra[k]), tiebreak[k]))[:left]:
        extra[k] += 1
    return {k: counts[k] + extra[k] for k in keys}


def allocate(
    ledger: Ledger,
    catalog: Catalog,
    policy: Policy,
    *,
    run_id: str,
    windows: dict,
    recorded_at: str | None = None,
) -> dict:
    """Freeze the next run's declared trials, alpha and windows from the ledger alone.

    Returns ``{"record": ..., "sha256": ...}``; the sha256 is the allocation's
    ledger line hash, which the run's protocol must carry.
    """
    ledger.verify()
    catalog.validate()
    policy.validate()
    windows = validate_windows(windows)
    if ledger.open_allocation() is not None:
        raise ValueError("the previous allocation is still open: record or abandon it")
    if not run_id or any(a["run_id"] == run_id for a in ledger.allocations()):
        raise ValueError("run ids must be unique in the ledger")
    k = len(ledger.allocations()) + 1
    alpha = run_alpha(k, ledger.q)
    if ledger.alpha_spent() + alpha > ledger.q * (1 + 1e-12):
        raise ValueError("global alpha budget exhausted")
    touched = ledger.touched_windows()
    history = ledger.outcomes()
    window = (windows["start"], windows["end"])
    arms = {}
    for key, arm in catalog.arms().items():
        stats = history["families"].get(key, {"successes": 0, "failures": 0})
        reason = arm.get("never")
        if reason is None and any(overlaps(window, w) for w in touched.get(key, [])):
            reason = "no fresh window: an earlier run of this family touched it"
        arms[key] = {
            "family": arm["family"],
            "feature_class": arm["feature_class"],
            "pool": len(arm["pool"]),
            "successes": stats["successes"],
            "failures": stats["failures"],
            "posterior": [
                policy.prior[0] + stats["successes"],
                policy.prior[1] + stats["failures"],
            ],
            "eligible": reason is None,
            **({"ineligible": reason} if reason else {}),
        }
    eligible = {k2: a for k2, a in arms.items() if a["eligible"]}
    if not eligible:
        raise ValueError("no eligible family for this window")
    shares = probability_best(
        {k2: tuple(a["posterior"]) for k2, a in eligible.items()}, policy.grid
    )
    counts = split_budget(
        shares,
        {k2: a["pool"] for k2, a in eligible.items()},
        policy.budget,
        policy.floor,
        {k2: digest([policy.seed, run_id, k2]) for k2 in eligible},
    )
    pools = catalog.arms()
    trials = []
    for key in sorted(eligible):
        arms[key]["p_best"] = shares[key]
        arms[key]["count"] = counts[key]
        ranked = sorted(
            pools[key]["pool"],
            key=lambda feature: (
                -history["features"].get((key, feature), {}).get("successes", 0),
                sum(history["features"].get((key, feature), {}).values()),
                digest([policy.seed, run_id, key, feature]),
            ),
        )
        family = arms[key]["family"]
        for feature in ranked[: counts[key]]:
            if catalog.feature_class(family, feature) == SELF_LAG_CLASS:
                raise AssertionError("self_lag trial allocated")  # unreachable by arms()
            trials.append(
                {
                    "trial_id": trial_id(run_id, family, feature),
                    "family": family,
                    "feature": feature,
                    "family_key": key,
                }
            )
    m = len(trials)
    record = {
        "kind": "allocation",
        "run_id": run_id,
        "run_index": k,
        "alpha": alpha,
        "alpha_spent_after": ledger.alpha_spent() + alpha,
        "q": ledger.q,
        "spending": SPENDING,
        "within_run": WITHIN_RUN,
        "windows": windows,
        "policy": asdict(policy),
        "catalog_sha256": catalog.sha256(),
        "catalog": asdict(catalog),
        "arms": arms,
        "trials": trials,
        "trial_count": m,
        # smallest perms whose resolution 1/(perms+1) reaches Holm's first step
        "min_perms_for_first_step": math.ceil(m / alpha),
        "recorded_at": recorded_at or now_iso(),
    }
    record, sha = ledger._append(record)
    return {"record": record, "sha256": sha}


def abandon(ledger: Ledger, reason: str, recorded_at: str | None = None) -> dict:
    """Close an open allocation without results: its alpha stays spent, its windows touched."""
    open_allocation = ledger.open_allocation()
    if open_allocation is None:
        raise ValueError("no open allocation")
    if not reason:
        raise ValueError("an abandonment needs a reason")
    record, sha = ledger._append(
        {
            "kind": "abandoned",
            "run_id": open_allocation[0]["run_id"],
            "allocation_sha256": open_allocation[1],
            "reason": reason,
            "recorded_at": recorded_at or now_iso(),
        }
    )
    return {"record": record, "sha256": sha}


def protocol_for_allocation(allocation: dict, **settings) -> Protocol:
    """The run's frozen protocol: the allocation's trials, alpha, windows and hash.

    ``settings`` carries the remaining protocol fields (origin, perms, seed,
    sampling, statistic, min_n, alpha, read_receipt, ...). Features are the
    whole catalog universe (every row carries them); families are those with
    at least one declared trial; ``self_lag`` is the catalog's, restricted to
    those families.
    """
    record = allocation["record"]
    catalog = record["catalog"]
    declared = {t["family"] for t in record["trials"]}
    families = tuple(f for f in catalog["families"] if f in declared)
    for key in ("run_id", "features", "families", "trials", "self_lag", "selection",
                "selection_alpha", "allocation_sha256", "start", "split", "end"):
        if key in settings:
            raise ValueError(f"{key} is fixed by the allocation")
    protocol = Protocol(
        run_id=record["run_id"],
        features=tuple(catalog["features"]),
        families=families,
        split=record["windows"]["split"],
        end=record["windows"]["end"],
        start=record["windows"]["start"],
        self_lag=tuple(
            tuple(pair) for pair in catalog["self_lag"] if pair[0] in declared
        ),
        trials=tuple((t["family"], t["feature"]) for t in record["trials"]),
        selection=LEDGER_SELECTION,
        selection_alpha=record["alpha"],
        allocation_sha256=allocation["sha256"],
        **settings,
    )
    protocol.validate()
    return protocol


def default_perms(allocation: dict, floor: int = 999, cap: int = PERMS_CAP) -> int:
    """Enough permutations for Holm's first step (x2), within [floor, cap]."""
    return int(min(cap, max(floor, 2 * allocation["record"]["min_perms_for_first_step"])))


# --- results ------------------------------------------------------------------------


def status_of(raw: str) -> str:
    if raw == "tested":
        return "tested"
    if raw == SELF_LAG:
        return SELF_LAG
    return "untestable"


def record_run(
    ledger: Ledger, frozen: dict, holdout_result: dict, recorded_at: str | None = None
) -> dict:
    """Append the run's results after checking them against the open allocation.

    ``frozen`` is the persisted discovery manifest (``discovery-frozen.json``),
    ``holdout_result`` the contract's ``evaluate_holdout`` output.
    """
    open_allocation = ledger.open_allocation()
    if open_allocation is None:
        raise ValueError("no open allocation to record against")
    allocation, allocation_sha = open_allocation
    payload = frozen["payload"]
    if digest(payload) != frozen["sha256"]:
        raise ValueError("frozen discovery manifest changed")
    if holdout_result.get("discovery_manifest") != frozen["sha256"]:
        raise ValueError("holdout result is not for this discovery manifest")
    protocol = protocol_from_payload(payload["protocol"])
    windows = allocation["windows"]
    if (
        protocol.run_id != allocation["run_id"]
        or protocol.selection != LEDGER_SELECTION
        or protocol.allocation_sha256 != allocation_sha
        or protocol.selection_alpha != allocation["alpha"]
        or (protocol.start, protocol.split, protocol.end)
        != (windows["start"], windows["split"], windows["end"])
    ):
        raise ValueError("manifest protocol does not match the open allocation")
    declared = {(t["family"], t["feature"]): t for t in allocation["trials"]}
    if set(protocol.trials) != set(declared) or len(protocol.trials) != len(declared):
        raise ValueError("manifest trials differ from the allocation")
    measured = {(t["family"], t["feature"]): t for t in payload["ledger"]}
    if set(measured) != set(declared) or len(payload["ledger"]) != len(declared):
        raise ValueError("discovery ledger differs from the declared trials")
    checks = {c["trial_id"]: c for c in holdout_result["holdout_checks"]}
    candidates = {
        (c["specification"]["family"], c["specification"]["feature"]): c["sha256"]
        for c in holdout_result["candidates"]
    }
    trials = []
    for pair, allocated in declared.items():
        trial = measured[pair]
        if trial["trial_id"] != allocated["trial_id"]:
            raise ValueError("trial id differs from the allocation")
        if status_of(trial["status"]) == SELF_LAG:
            raise ValueError("a self_lag trial reached a ledger run")
        check = checks.get(trial["trial_id"])
        if trial["selected"] != (check is not None):
            raise ValueError("holdout checks differ from the frozen selections")
        outcome = (
            "not_selected"
            if check is None
            else "survived" if check["retrospective_survivor"] else "failed"
        )
        trials.append(
            {
                **allocated,
                "status": status_of(trial["status"]),
                "raw_status": trial["status"],
                "n": trial["n"],
                "r": trial["r"],
                "p": trial["p"],
                "adjusted_p": trial["adjusted_p"],
                "selected": trial["selected"],
                "holdout_outcome": outcome,
                "holdout_p": None if check is None else check["p"],
                "holdout_adjusted_p": None if check is None else check["adjusted_p"],
                "candidate_sha256": candidates.get(pair) if outcome == "survived" else None,
            }
        )
    return _append_run_result(
        ledger,
        allocation,
        allocation_sha,
        trials,
        {
            "discovery_manifest_sha256": frozen["sha256"],
            "holdout_sha256": holdout_result["holdout_sha256"],
            "state": holdout_result["state"],
            "perms": protocol.perms,
            "resolution_limited": 1 / (protocol.perms + 1)
            > allocation["alpha"] / max(1, len(trials)),
            "method": payload["method"],
        },
        recorded_at,
    )


def _append_run_result(ledger, allocation, allocation_sha, trials, extra, recorded_at):
    """Shared by :func:`record_run` and the simulations: trials must be the allocation's."""
    if {t["trial_id"] for t in trials} != {t["trial_id"] for t in allocation["trials"]}:
        raise ValueError("results must cover exactly the declared trials")
    for t in trials:
        if t["status"] not in STATUSES or t["holdout_outcome"] not in HOLDOUT_OUTCOMES:
            raise ValueError("unknown trial status or holdout outcome")
    order = {t["trial_id"]: i for i, t in enumerate(allocation["trials"])}
    trials = sorted(trials, key=lambda t: order[t["trial_id"]])
    record, sha = ledger._append(
        {
            "kind": "run_result",
            "run_id": allocation["run_id"],
            "run_index": allocation["run_index"],
            "allocation_sha256": allocation_sha,
            "alpha": allocation["alpha"],
            "trials": trials,
            "selected": sum(t["selected"] for t in trials),
            "survived": sum(t["holdout_outcome"] == "survived" for t in trials),
            **extra,
            "recorded_at": recorded_at or now_iso(),
        }
    )
    return {"record": record, "sha256": sha}


def ingest_forward_outcomes(
    ledger: Ledger, path: str | Path, recorded_at: str | None = None
) -> int:
    """Append final forward-log verdicts (S10 output) for holdout survivors.

    All lines are validated before any is appended. Returns the count appended.
    """
    data = Path(path).read_bytes()
    source_sha = sha256_bytes(data)
    lines = [line for line in data.decode("utf-8").splitlines() if line.strip()]
    trials = {t["trial_id"]: t for t in ledger.trial_results()}
    seen = {v["trial_id"] for v in ledger.of_kind("forward_outcome")}
    verdicts = []
    for number, line in enumerate(lines, 1):
        entry = json.loads(line)
        if set(entry) != set(FORWARD_OUTCOME_FIELDS):
            raise ValueError(f"line {number}: fields must be {FORWARD_OUTCOME_FIELDS}")
        trial = trials.get(entry["trial_id"])
        if trial is None or trial["holdout_outcome"] != "survived":
            raise ValueError(f"line {number}: not a ledger holdout survivor")
        if entry["candidate_sha256"] != trial["candidate_sha256"]:
            raise ValueError(f"line {number}: candidate hash differs from the ledger")
        if entry["outcome"] not in FORWARD_OUTCOMES:
            raise ValueError(f"line {number}: only final pass/fail verdicts are ingested")
        if not isinstance(entry["n"], int) or entry["n"] < 1:
            raise ValueError(f"line {number}: n must be a positive integer")
        stamp(entry["evaluated_through"])
        if entry["trial_id"] in seen:
            raise ValueError(f"line {number}: trial already has a forward verdict")
        seen.add(entry["trial_id"])
        verdicts.append(entry)
    for entry in verdicts:
        ledger._append(
            {
                "kind": "forward_outcome",
                **entry,
                "family_key": trials[entry["trial_id"]]["family_key"],
                "source_sha256": source_sha,
                "recorded_at": recorded_at or now_iso(),
            }
        )
    return len(verdicts)


def summarize(ledger: Ledger) -> dict:
    """Files-only status: runs, trials, alpha spent, discoveries, family yields."""
    ledger.verify()
    results = ledger.trial_results()
    outcomes = ledger.outcomes()["families"]
    return {
        "ledger_id": ledger.genesis["ledger_id"],
        "head_sha256": ledger.head,
        "records": len(ledger.records),
        "q": ledger.q,
        "runs_allocated": len(ledger.allocations()),
        "runs_recorded": len(ledger.of_kind("run_result")),
        "runs_abandoned": len(ledger.of_kind("abandoned")),
        "alpha_spent": ledger.alpha_spent(),
        "alpha_remaining": ledger.q - ledger.alpha_spent(),
        "trials": len(results),
        "trials_by_status": {s: sum(t["status"] == s for t in results) for s in STATUSES},
        "discoveries": sum(t["selected"] for t in results),
        "holdout_survivors": sum(t["holdout_outcome"] == "survived" for t in results),
        "forward_verdicts": {
            o: sum(v["outcome"] == o for v in ledger.of_kind("forward_outcome"))
            for o in FORWARD_OUTCOMES
        },
        "families": {
            key: {**stats, "trials": stats["successes"] + stats["failures"]}
            for key, stats in sorted(outcomes.items())
        },
        "error_control": (
            f"P(any false discovery over every run) <= sum_k alpha_k < q = {ledger.q} "
            "(Holm per run, arbitrary dependence)"
        ),
        "promotion_allowed": False,
    }


def allocation_counts(allocation: dict) -> dict[str, int]:
    return {
        key: arm.get("count", 0) for key, arm in allocation["record"]["arms"].items()
    }


# --- synthetic dry runs -------------------------------------------------------------

SYNTHETIC_TARGETS = ("T1", "T2")
SYNTHETIC_CLASSES = ("alpha", "beta", "gamma")
SYNTHETIC_OWN = "T1|chg1"  # T1's own change: a self_lag feature for T1's family


def synthetic_catalog(per_class: int = 6) -> Catalog:
    """Three feature classes x two targets at horizon 1, plus T1's own change."""
    features = [f"{cls}{i}|x" for cls in SYNTHETIC_CLASSES for i in range(1, per_class + 1)]
    classes = [(f, f.split("|")[0].rstrip("0123456789")) for f in features]
    features.append(SYNTHETIC_OWN)
    classes.append((SYNTHETIC_OWN, "own"))
    families = tuple(f"{t}|change|fwd1" for t in SYNTHETIC_TARGETS)
    catalog = Catalog(
        families=families,
        features=tuple(features),
        classes=tuple(classes),
        self_lag=(("T1|change|fwd1", SYNTHETIC_OWN),),
    )
    catalog.validate()
    return catalog


@dataclass(frozen=True)
class Epoch:
    features: pd.DataFrame
    levels: pd.DataFrame
    windows: dict


def synthetic_epoch(
    catalog: Catalog,
    epoch: int,
    *,
    seed: int = 20260926,
    planted: tuple[str, str] | None = ("alpha", "T1"),
    effect: float = 0.5,
    sessions: int = 300,
    discovery: int = 200,
) -> Epoch:
    """A fresh, disjoint block of business days (new data arriving for run ``epoch``).

    Every class's features share a class factor (so trials inside a run are
    dependent). With ``planted=(cls, target)``, that class's factor drives the
    target's next-session change; everything else is pure noise.
    """
    rng = np.random.default_rng([seed, epoch])
    first = pd.Timestamp("2000-01-03") + pd.offsets.BDay(epoch * (sessions + 10))
    index = pd.bdate_range(first, periods=sessions, tz="UTC")
    factors = {cls: rng.normal(size=sessions) for cls in SYNTHETIC_CLASSES}
    columns = {}
    for feature, cls in catalog.classes:
        if feature == SYNTHETIC_OWN:
            continue
        columns[feature] = factors[cls] + 0.5 * rng.normal(size=sessions)
    increments = {t: rng.normal(size=sessions) for t in SYNTHETIC_TARGETS}
    if planted is not None:
        cls, target = planted
        # the level moves from session t to t+1 by effect * factor_t + noise
        increments[target][1:] += effect * factors[cls][:-1]
    levels = pd.DataFrame(
        {t: 100 + np.cumsum(increments[t]) for t in SYNTHETIC_TARGETS}, index=index
    )
    own = levels["T1"].diff()
    own.iloc[0] = np.nan
    columns[SYNTHETIC_OWN] = own.to_numpy()
    features = pd.DataFrame(columns, index=index)[list(catalog.features)]
    windows = {
        "start": index[0].isoformat(),
        "split": index[discovery].isoformat(),
        "end": (index[-1] + pd.Timedelta(days=1)).isoformat(),
    }
    return Epoch(features=features, levels=levels, windows=windows)


def run_synthetic_step(
    ledger: Ledger,
    catalog: Catalog,
    policy: Policy,
    epoch: Epoch,
    output: str | Path,
    *,
    run_id: str,
    perms_cap: int = PERMS_CAP,
    seed: int = 20260926,
    recorded_at: str | None = None,
) -> dict:
    """allocate -> protocol -> rows -> contract run (files) -> ledger result.

    Permutations follow the run's Holm first step (:func:`default_perms`) up to
    ``perms_cap``; a capped run is recorded as ``resolution_limited``.
    """
    allocation = allocate(
        ledger, catalog, policy, run_id=run_id, windows=epoch.windows, recorded_at=recorded_at
    )
    protocol = protocol_for_allocation(
        allocation,
        origin="synthetic_fixture",
        sampling="horizon_spaced",
        step=1,
        perms=default_perms(allocation, cap=perms_cap),
        seed=seed,
        statistic="pearson",
    )
    rows = {
        window: {
            family: build_family_rows(
                protocol,
                epoch.features,
                epoch.levels[family.split("|")[0]],
                int(family.rsplit("fwd", 1)[1]),
                window,
                label="change",
            )
            for family in protocol.families
        }
        for window in ("discovery", "holdout")
    }
    output = Path(output)
    result = run_proof(protocol, rows["discovery"], rows["holdout"], output)
    frozen = json.loads((output / "discovery-frozen.json").read_text(encoding="utf-8"))
    recorded = record_run(ledger, frozen, result, recorded_at=recorded_at)
    return {"allocation": allocation, "result": recorded}


def run_synthetic_dry_runs(
    output: str | Path,
    *,
    runs: int = 2,
    planted: tuple[str, str] | None = ("alpha", "T1"),
    policy: Policy | None = None,
    q: float = MAX_Q,
    seed: int = 20260926,
    perms_cap: int = PERMS_CAP,
) -> dict:
    """Consecutive synthetic dry runs on one new ledger in a new directory."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    catalog = synthetic_catalog()
    policy = policy or Policy(budget=21, floor=1, seed=seed)
    ledger = Ledger.create(output / "ledger.jsonl", ledger_id=f"s11-synthetic-{seed}", q=q)
    steps = []
    for k in range(1, runs + 1):
        epoch = synthetic_epoch(catalog, k, seed=seed, planted=planted)
        step = run_synthetic_step(
            ledger,
            catalog,
            policy,
            epoch,
            output / f"run-{k:02d}",
            run_id=f"s11-synthetic-{seed}-run{k:02d}",
            perms_cap=perms_cap,
            seed=seed,
        )
        steps.append(
            {
                "run_id": step["allocation"]["record"]["run_id"],
                "alpha": step["allocation"]["record"]["alpha"],
                "counts": allocation_counts(step["allocation"]),
                "p_best": {
                    key: arm.get("p_best")
                    for key, arm in step["allocation"]["record"]["arms"].items()
                },
                "selected": step["result"]["record"]["selected"],
                "survived": step["result"]["record"]["survived"],
                "resolution_limited": step["result"]["record"]["resolution_limited"],
            }
        )
    summary = {"steps": steps, "ledger": summarize(ledger), "planted": planted}
    with (output / "summary.json").open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2, sort_keys=True, allow_nan=False)
    return summary
