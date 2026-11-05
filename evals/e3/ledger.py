"""E3 candidate ledger: every candidate counts, including abandoned ones.

S11 (``analysis.ledger_steered_exploration``) counts every *declared trial*
once it is inside an allocation. A hill-climber generates many candidates that
never reach one: ideas screened out on discovery data, variants an agent tried
and dropped, code changes rejected by E0/E1. If those vanish, the effective
number of tests is understated. This ledger records every candidate from the
moment it is proposed (before any data is read) through every stage.

Storage (files only, no DB)
---------------------------
One canonical JSON record per line, hash-chained (``seq``, ``prev_sha256``),
in ``$GRID_E3_LEDGER_DIR/candidates.jsonl``, with its external anchor in
``$GRID_E3_ANCHOR_DIR/candidates.anchor.jsonl`` (a separate directory; the
two may not be the same). Storage, chain and anchor are S11's own ``Ledger``,
``Anchor`` and ``verify_chain``, imported unchanged: a ledger that is shorter
or longer than its anchor, or whose lines do not hash to the anchored heads,
is refused on open, so an edited, truncated, reordered or recomputed file is
refused, and a second genesis against an existing anchor is refused. The off
host witness (EVAL-E0H2 pattern, vault path :data:`WITNESS_PATH`) is checked
by ``verify(external_anchors=...)``: the witnessed copy must be a line-for-line
prefix of the local anchor, which also catches a tamperer who rewrote both
local files. Every read and append runs under an exclusive ``O_EXCL`` lock
file (the ``research_forward_log`` pattern), and every append re-reads and
re-verifies both files and replays every invariant before writing.

A crash between the ledger write and the anchor write leaves one unanchored
line; the ledger then refuses to open (fail closed) until the owner reviews it.

Records
-------
``genesis``            ledger id, ``e3_version``, the S11 ledger id it binds to.
``proposed``           ``candidate_id`` (sha256 of the canonical proposal core),
                       ``family``, ``candidate_kind``, ``identity`` (S11
                       ``scientific_identity`` per declared trial) and
                       ``identity_sha256``, ``declared_trials``, ``proposer``,
                       ``engine_version``, ``spec_sha256``, ``expected_sign``,
                       ``rationale_sha256``, ``retest_of``, ``proposed_at``.
                       The spec and rationale enter only as hashes: no data
                       references, no free payload.
``stage_entered``      ``stage`` in :data:`STAGES`, strictly in order, once.
                       ``screen`` records the label window and the S11 head it
                       was checked against; ``holdout`` S11's *open* allocation
                       (which must declare the candidate's identities and may
                       pay for each identity's look only once) and its alpha.
``stage_result``       ``result`` in :data:`RESULTS`, ``p_values``,
                       ``alpha_spent`` (0 except holdout, which spends exactly
                       its allocation's alpha), ``s11_allocation_sha256``
                       (holdout) and the ``receipt_sha256`` of the stage artifact.
``abandoned``          any time before a terminal record; counts as a failure.
                       After an S11 allocation it stores the sha of the S11
                       record that closed that allocation (``abandoned``, or
                       ``run_result`` if the run had already been recorded).
``withdrawn_pre_data`` only before any ``stage_entered``; counted as proposed,
                       no alpha spent.
``promoted_research``, ``suspended``, ``retired``: written by E3B/E3C.

Invariants (enforced on every append and replayed on every open)
----------------------------------------------------------------
No stage without a prior ``proposed``; stages in order screen -> gates ->
holdout -> forward -> promotion, each entered at most once and only after the
previous stage passed; a re-test is a new candidate (``retest_of``) with a new
id, so it pays again; an identity is refused at ``screen`` if S11's window
registry says it already touched an overlapping window; nothing is ever
deleted or edited.

Who may write what
------------------
:class:`CandidateLedger` is the judge. :func:`client_for_proposer` returns a
:class:`ProposerClient` that exposes only ``propose``, and ``abandon`` /
``withdraw`` for that proposer's own candidates (abandon only before any S11
allocation). Appends carry a module-private capability (the VS1
``_WITNESS_TOKEN`` pattern) and the proposer capability is refused every
judge record kind. In-process Python is not a security boundary. In v1 the
client writes the files itself, so it only runs where the ledger is writable;
EVAL-E4A must put proposers behind an OS boundary (a separate user without
write access to the ledger and anchor directories) and route their records
through a judge-owned writer (spool or IPC) that exposes this same client API.
The judge accepts only the S11 file ledger with its anchor, never one in memory.

Every append and read re-verifies and replays the whole file under the lock:
O(n) per call, fine at hill-climb volumes; E3B/E4 may cache a verified head.
"""

from __future__ import annotations

import contextlib
import math
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator

from analysis.ledger_steered_exploration import (
    Anchor,
    Ledger,
    canonical,
    identity,
    now_iso,
    overlaps,
    scientific_identity,
    sha256_bytes,
    verify_chain,
)
from analysis.offline_research_proof import stamp
from analysis.research_forward_log import LOCK_STALE_AFTER_S, LOCK_TIMEOUT_S
from evals.e3 import VERSION

SCHEMA = 1
LEDGER_ID = "grid-e3-candidates"
LEDGER_FILENAME = "candidates.jsonl"
ANCHOR_FILENAME = "candidates.anchor.jsonl"
LOCK_FILENAME = "candidates.lock"
ENV_LEDGER_DIR = "GRID_E3_LEDGER_DIR"
ENV_ANCHOR_DIR = "GRID_E3_ANCHOR_DIR"
#: Off-host witness of the anchor file (EVAL-E0H2 pattern, cadence as EVAL-E2F2).
WITNESS_PATH = "05-GRID/Paper-Log/evals/e3/ledger.anchors.jsonl"

STAGES = ("screen", "gates", "holdout", "forward", "promotion")
RESULTS = ("pass", "fail", "untestable", "inconclusive")
CANDIDATE_KINDS = ("feature", "parameter", "machinery-change", "generator-change")
EXPECTED_SIGNS = (-1, 0, 1)  # 0: two-sided
JUDGE = "judge"
PROPOSER_PREFIX = "proposer:"
PROPOSER_KINDS = frozenset({"proposed", "abandoned", "withdrawn_pre_data"})

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_NAME = re.compile(r"^[^\s\x00-\x1f\x7f]{1,200}$")
_BASE = frozenset({"kind", "seq", "prev_sha256", "promotion_allowed", "actor", "recorded_at"})
FIELDS: dict[str, frozenset] = {
    "genesis": frozenset(
        {"kind", "seq", "prev_sha256", "promotion_allowed", "schema", "ledger_id",
         "e3_version", "s11_ledger_id", "stages", "created_at", "recorded_at"}
    ),
    "proposed": _BASE | {
        "candidate_id", "family", "candidate_kind", "identity", "identity_sha256",
        "declared_trials", "proposer", "engine_version", "spec_sha256", "expected_sign",
        "rationale_sha256", "retest_of", "proposed_at",
    },
    "stage_entered": _BASE | {
        "candidate_id", "stage", "window", "s11_ledger_id", "s11_head_sha256",
        "s11_allocation_sha256", "s11_alpha",
    },
    "stage_result": _BASE | {
        "candidate_id", "stage", "result", "p_values", "alpha_spent",
        "s11_allocation_sha256", "receipt_sha256",
    },
    "abandoned": _BASE | {
        "candidate_id", "reason", "s11_allocation_sha256", "s11_close_kind",
        "s11_close_sha256",
    },
    "withdrawn_pre_data": _BASE | {"candidate_id", "reason"},
    "promoted_research": _BASE | {"candidate_id", "receipt_sha256"},
    "suspended": _BASE | {"candidate_id", "reason"},
    "retired": _BASE | {"candidate_id", "reason", "receipt_sha256"},
}

# Module-private capabilities (VS1 ``_WITNESS_TOKEN`` pattern).
_JUDGE_CAP = object()
_PROPOSER_CAP = object()
_CLIENT_TOKEN = object()


# --- paths and lock -----------------------------------------------------------------


@dataclass(frozen=True)
class LedgerPaths:
    ledger: Path
    anchor: Path
    lock: Path

    @classmethod
    def from_dirs(
        cls, ledger_dir: str | Path | None = None, anchor_dir: str | Path | None = None
    ) -> LedgerPaths:
        """The ledger and anchor files from explicit dirs or ``GRID_E3_*_DIR``."""
        ledger_dir = ledger_dir or os.environ.get(ENV_LEDGER_DIR)
        anchor_dir = anchor_dir or os.environ.get(ENV_ANCHOR_DIR)
        if not ledger_dir or not anchor_dir:
            raise ValueError(f"set {ENV_LEDGER_DIR} and {ENV_ANCHOR_DIR} (separate directories)")
        ledger_dir, anchor_dir = Path(ledger_dir).resolve(), Path(anchor_dir).resolve()
        if ledger_dir == anchor_dir:
            raise ValueError("the ledger and its anchor must live in separate directories")
        return cls(
            ledger=ledger_dir / LEDGER_FILENAME,
            anchor=anchor_dir / ANCHOR_FILENAME,
            lock=ledger_dir / LOCK_FILENAME,
        )


@contextlib.contextmanager
def _locked(lock_path: Path) -> Iterator[None]:
    """Exclusive ``O_EXCL`` lock file; stale after 15 minutes (``research_forward_log``)."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + LOCK_TIMEOUT_S
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"pid={os.getpid()}".encode())
            os.close(fd)
            break
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
            except FileNotFoundError:
                continue
            if age > LOCK_STALE_AFTER_S:
                lock_path.unlink(missing_ok=True)
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"could not acquire {lock_path}") from None
            time.sleep(0.05)
    try:
        yield
    finally:
        lock_path.unlink(missing_ok=True)


def _fsync(path: Path) -> None:
    with open(path, "rb+") as stream:
        os.fsync(stream.fileno())


# --- replayed state -----------------------------------------------------------------


@dataclass
class Candidate:
    """A candidate's state, derived only by replaying the ledger."""

    proposed: dict
    proposed_seq: int
    entered: list[str] = field(default_factory=list)
    entered_seq: dict[str, int] = field(default_factory=dict)
    results: dict[str, dict] = field(default_factory=dict)
    allocation: str | None = None
    alpha: float | None = None
    terminal: str | None = None
    promoted: bool = False
    suspended: bool = False

    @property
    def candidate_id(self) -> str:
        return self.proposed["candidate_id"]

    @property
    def family(self) -> str:
        return self.proposed["family"]

    @property
    def failed(self) -> bool:
        """Closed by a non-pass stage result (fail, untestable or inconclusive)."""
        return any(r["result"] != "pass" for r in self.results.values())

    @property
    def open_stage(self) -> str | None:
        if self.entered and self.entered[-1] not in self.results:
            return self.entered[-1]
        return None


@dataclass
class State:
    genesis: dict
    candidates: dict[str, Candidate]
    #: S11 allocation sha -> identities whose holdout look it already paid for
    allocation_identities: dict[str, set] = field(default_factory=dict)


def _fail(seq: int | None, message: str) -> ValueError:
    where = "e3 append" if seq is None else f"e3 record {seq}"
    return ValueError(f"{where}: {message}")


def _hex(value) -> bool:
    return isinstance(value, str) and bool(_HEX64.fullmatch(value))


def _name(value) -> bool:
    return isinstance(value, str) and bool(_NAME.fullmatch(value))


def _timestamp(value, seq: int, what: str) -> None:
    try:
        stamp(value)
    except (TypeError, ValueError) as error:
        raise _fail(seq, f"{what} must be a tz-aware ISO timestamp") from error


def candidate_core(record: dict, e3_version: str = VERSION) -> dict:
    """The fields ``candidate_id`` is the sha256 of (canonical JSON)."""
    return {
        "e3_version": e3_version,
        "family": record["family"],
        "candidate_kind": record["candidate_kind"],
        "identity_sha256": record["identity_sha256"],
        "proposer": record["proposer"],
        "engine_version": record["engine_version"],
        "spec_sha256": record["spec_sha256"],
        "expected_sign": record["expected_sign"],
        "rationale_sha256": record["rationale_sha256"],
        "retest_of": record["retest_of"],
    }


def _apply(state: State | None, record: dict) -> State:
    """Validate one record against the replayed state and fold it in."""
    seq, kind = record.get("seq"), record.get("kind")
    if kind not in FIELDS:
        raise _fail(seq, f"unknown record kind {kind!r}")
    if set(record) != FIELDS[kind]:
        extra, missing = set(record) - FIELDS[kind], FIELDS[kind] - set(record)
        raise _fail(seq, f"{kind} fields differ (extra {sorted(extra)}, missing {sorted(missing)})")
    if record["promotion_allowed"] is not False:
        raise _fail(seq, "promotion_allowed must be false")
    _timestamp(record["recorded_at"], seq, "recorded_at")
    if kind == "genesis":
        if state is not None or seq != 0:
            raise _fail(seq, "genesis must be first and only first")
        if (
            record["schema"] != SCHEMA
            or not _name(record["ledger_id"])
            or not _name(record["s11_ledger_id"])
            or not isinstance(record["e3_version"], str)
            or record["e3_version"] not in ("e3-v1", "e3-v2")
            or list(record["stages"]) != list(STAGES)
        ):
            raise _fail(seq, "malformed genesis")
        _timestamp(record["created_at"], seq, "created_at")
        return State(genesis=record, candidates={})
    if state is None:
        raise _fail(seq, "the ledger must start with a genesis record")

    actor = record["actor"]
    by_proposer = isinstance(actor, str) and actor.startswith(PROPOSER_PREFIX)
    if actor != JUDGE and not (by_proposer and _name(actor[len(PROPOSER_PREFIX):])):
        raise _fail(seq, f"unknown actor {actor!r}")
    if by_proposer and kind not in PROPOSER_KINDS:
        raise _fail(seq, f"a proposer may not write {kind}")

    candidates = state.candidates
    cid = record["candidate_id"]
    if kind == "proposed":
        _check_proposed(state, record, seq, actor, by_proposer)
        candidates[cid] = Candidate(proposed=record, proposed_seq=seq)
        return state

    candidate = candidates.get(cid)
    if candidate is None:
        raise _fail(seq, f"{kind} for candidate {str(cid)[:12]} which was never proposed")
    if candidate.terminal is not None:
        raise _fail(seq, f"candidate {cid[:12]} is already {candidate.terminal}")
    if by_proposer and actor != PROPOSER_PREFIX + candidate.proposed["proposer"]:
        raise _fail(seq, "a proposer may only abandon or withdraw its own candidates")
    if kind in ("abandoned", "withdrawn_pre_data", "suspended", "retired"):
        if not isinstance(record["reason"], str) or not record["reason"].strip():
            raise _fail(seq, f"{kind} needs a reason")

    if kind == "stage_entered":
        _check_stage_entered(candidate, record, seq)
        if record["stage"] == "holdout":
            allocation = record["s11_allocation_sha256"]
            paid = state.allocation_identities.setdefault(allocation, set())
            identities = set(candidate.proposed["identity_sha256"])
            if paid & identities:
                raise _fail(seq, "this S11 allocation already paid for a holdout look at one of "
                                 "these identities: a re-test needs a new allocation")
            paid |= identities
            candidate.allocation = allocation
            candidate.alpha = record["s11_alpha"]
        candidate.entered.append(record["stage"])
        candidate.entered_seq[record["stage"]] = seq
    elif kind == "stage_result":
        _check_stage_result(candidate, record, seq)
        candidate.results[record["stage"]] = record
    elif kind == "abandoned":
        if by_proposer and candidate.allocation is not None:
            raise _fail(seq, "after an S11 allocation only the judge may abandon")
        close_kind, close_sha = record["s11_close_kind"], record["s11_close_sha256"]
        if record["s11_allocation_sha256"] != candidate.allocation:
            raise _fail(seq, "s11_allocation_sha256 differs from the candidate's allocation")
        if candidate.allocation is None:
            if close_kind is not None or close_sha is not None:
                raise _fail(seq, "no S11 allocation, so no S11 closing record")
        elif close_kind not in ("abandoned", "run_result") or not _hex(close_sha):
            raise _fail(seq, "abandoned after an allocation needs the S11 closing record's sha")
        candidate.terminal = "abandoned"
    elif kind == "withdrawn_pre_data":
        if candidate.entered:
            raise _fail(seq, "withdrawn_pre_data is only allowed before any stage_entered")
        candidate.terminal = "withdrawn_pre_data"
    elif kind == "promoted_research":
        if candidate.results.get("promotion", {}).get("result") != "pass":
            raise _fail(seq, "promoted_research needs a passed promotion stage")
        if candidate.promoted:
            raise _fail(seq, "already promoted")
        if not _hex(record["receipt_sha256"]):
            raise _fail(seq, "receipt_sha256 must be a sha256")
        candidate.promoted = True
    elif kind == "suspended":
        if "forward" not in candidate.entered:
            raise _fail(seq, "only a forward-admitted candidate can be suspended")
        if candidate.suspended:
            raise _fail(seq, "already suspended")
        candidate.suspended = True
    elif kind == "retired":
        if "forward" not in candidate.entered:
            raise _fail(seq, "only a forward-admitted candidate can be retired")
        if not _hex(record["receipt_sha256"]):
            raise _fail(seq, "receipt_sha256 must be a sha256")
        candidate.terminal = "retired"
    return state


def _check_proposed(state: State, record: dict, seq: int, actor: str, by_proposer: bool) -> None:
    if not _name(record["family"]) or not _name(record["proposer"]):
        raise _fail(seq, "family and proposer must be non-empty names without whitespace")
    if by_proposer and actor != PROPOSER_PREFIX + record["proposer"]:
        raise _fail(seq, "a proposer may only propose under its own id")
    if record["candidate_kind"] not in CANDIDATE_KINDS:
        raise _fail(seq, f"candidate_kind must be one of {CANDIDATE_KINDS}")
    if not _name(record["engine_version"]):
        raise _fail(seq, "engine_version must be a non-empty name")
    if record["expected_sign"] not in EXPECTED_SIGNS or isinstance(record["expected_sign"], bool):
        raise _fail(seq, f"expected_sign must be one of {EXPECTED_SIGNS}")
    if not _hex(record["spec_sha256"]) or not _hex(record["rationale_sha256"]):
        raise _fail(seq, "spec_sha256 and rationale_sha256 must be sha256 hex digests")
    identities, shas = record["identity"], record["identity_sha256"]
    if (
        not isinstance(identities, list)
        or not isinstance(shas, list)
        or not identities
        or len(identities) != len(shas)
        or record["declared_trials"] != len(identities)
        or isinstance(record["declared_trials"], bool)
    ):
        raise _fail(seq, "identity, identity_sha256 and declared_trials must agree (>= 1)")
    if len(set(shas)) != len(shas):
        raise _fail(seq, "a candidate declares each identity once")
    for ident, sha in zip(identities, shas):
        try:
            matches = isinstance(ident, dict) and _identity_sha(ident) == sha
        except (KeyError, TypeError, ValueError):
            matches = False
        if not matches:
            raise _fail(seq, "identity_sha256 is not the sha256 of its scientific identity")
    retest_of = record["retest_of"]
    if retest_of is not None and retest_of not in state.candidates:
        raise _fail(seq, "retest_of must name an earlier candidate")
    if record["candidate_id"] != sha256_bytes(
        canonical(candidate_core(record, e3_version=state.genesis["e3_version"]))
    ):
        raise _fail(seq, "candidate_id is not the sha256 of the canonical proposal")
    if record["candidate_id"] in state.candidates:
        raise _fail(seq, "candidate already proposed: a re-test is a new candidate (retest_of)")
    _timestamp(record["proposed_at"], seq, "proposed_at")
    if record["proposed_at"] != record["recorded_at"]:
        raise _fail(seq, "proposed_at must equal recorded_at")


def _identity_sha(ident: dict) -> str:
    """S11's ``identity()`` for a stored scientific identity (same digest)."""
    keys = ("target", "label", "horizon_sessions", "feature_series", "feature_suffix")
    if set(ident) != set(keys):
        raise ValueError("a scientific identity has exactly the S11 fields")
    family = f"{ident['target']}|{ident['label']}|fwd{ident['horizon_sessions']}"
    feature = (
        f"{ident['feature_series']}|{ident['feature_suffix']}"
        if ident["feature_suffix"]
        else ident["feature_series"]
    )
    if scientific_identity(family, feature) != ident:
        raise ValueError("not a canonical scientific identity")
    return identity(family, feature)


def _check_order(candidate: Candidate, stage: str, seq: int | None) -> None:
    """Strict order, no re-entry, previous stage passed (shared with the judge's pre-check)."""
    if stage not in STAGES:
        raise _fail(seq, f"stage must be one of {STAGES}")
    if candidate.terminal is not None:
        raise _fail(seq, f"candidate {candidate.candidate_id[:12]} is already {candidate.terminal}")
    if stage in candidate.entered:
        raise _fail(seq, f"stage {stage} re-entered: a re-test is a new candidate")
    expected = STAGES[len(candidate.entered)]
    if stage != expected:
        raise _fail(seq, f"stage {stage} out of order (next is {expected})")
    if candidate.entered:
        previous = candidate.entered[-1]
        if candidate.results.get(previous, {}).get("result") != "pass":
            raise _fail(seq, f"stage {stage} needs a passed {previous} result first")
    if candidate.suspended:
        raise _fail(seq, "candidate is suspended")


def _check_stage_entered(candidate: Candidate, record: dict, seq: int) -> None:
    stage = record["stage"]
    _check_order(candidate, stage, seq)
    window, s11_id, s11_head = record["window"], record["s11_ledger_id"], record["s11_head_sha256"]
    allocation = record["s11_allocation_sha256"]
    if stage == "screen":
        if not isinstance(window, dict) or set(window) != {"start", "end"}:
            raise _fail(seq, "screen declares its label window {start, end}")
        _timestamp(window["start"], seq, "window start")
        _timestamp(window["end"], seq, "window end")
        if not stamp(window["start"]) < stamp(window["end"]):
            raise _fail(seq, "screen window must satisfy start < end")
        if not _name(s11_id) or not _hex(s11_head):
            raise _fail(seq, "screen records the S11 ledger it was checked against")
    elif window is not None:
        raise _fail(seq, "only screen declares a window")
    s11_alpha = record["s11_alpha"]
    if stage == "holdout":
        if not _name(s11_id) or not _hex(s11_head) or not _hex(allocation):
            raise _fail(seq, "holdout spends alpha through an S11 allocation")
        if (
            isinstance(s11_alpha, bool)
            or not isinstance(s11_alpha, (int, float))
            or not 0 < s11_alpha <= 1
        ):
            raise _fail(seq, "holdout records the S11 allocation's alpha in (0, 1]")
    elif allocation is not None or s11_alpha is not None:
        raise _fail(seq, "only holdout names an S11 allocation and its alpha")
    if stage not in ("screen", "holdout") and (s11_id is not None or s11_head is not None):
        raise _fail(seq, f"stage {stage} does not consult S11")


def _check_stage_result(candidate: Candidate, record: dict, seq: int) -> None:
    stage = record["stage"]
    if candidate.open_stage != stage:
        raise _fail(seq, f"no open {stage} stage for this candidate")
    if record["result"] not in RESULTS:
        raise _fail(seq, f"result must be one of {RESULTS}")
    if not _hex(record["receipt_sha256"]):
        raise _fail(seq, "receipt_sha256 must be a sha256")
    alpha = record["alpha_spent"]
    if isinstance(alpha, bool) or not isinstance(alpha, (int, float)) or not math.isfinite(alpha):
        raise _fail(seq, "alpha_spent must be a number")
    expected_alpha = candidate.alpha if stage == "holdout" else 0
    if alpha != expected_alpha:
        raise _fail(seq, f"alpha_spent must be {expected_alpha}: only the holdout spends alpha, "
                         "and it spends exactly its S11 allocation's alpha")
    p_values = record["p_values"]
    if p_values is not None:
        if not isinstance(p_values, list) or len(p_values) != candidate.proposed["declared_trials"]:
            raise _fail(seq, "p_values: one per declared trial (or null)")
        for p in p_values:
            if p is not None and (
                isinstance(p, bool) or not isinstance(p, (int, float)) or not 0 <= p <= 1
            ):
                raise _fail(seq, "p_values must be in [0, 1] or null")
    expected_allocation = candidate.allocation if stage == "holdout" else None
    if record["s11_allocation_sha256"] != expected_allocation:
        raise _fail(seq, "s11_allocation_sha256 must be the holdout stage's allocation")


def replay(records) -> State:
    """Fold every record through the invariants (raises on the first violation)."""
    state = None
    for record in records:
        state = _apply(state, record)
    if state is None:
        raise ValueError("empty e3 ledger")
    return state


# --- storage ------------------------------------------------------------------------


def _open_storage(paths: LedgerPaths) -> Ledger:
    if not paths.ledger.exists():
        raise ValueError(f"no e3 ledger at {paths.ledger}: run genesis first")
    if not paths.anchor.exists():
        raise ValueError("the e3 anchor file is missing: refusing an unanchored ledger")
    return Ledger(paths.ledger, anchor=paths.anchor)  # chain + anchor verified here


def _external_check(paths: LedgerPaths, external_anchors: str | Path) -> int:
    """The witnessed anchor copy is a line-for-line prefix of the local anchor."""
    external = Path(external_anchors)
    if not external.is_file():
        raise ValueError("the external anchor copy is missing")
    witnessed = Anchor(external)
    witnessed.records()  # its own chain must verify
    local = Anchor(paths.anchor).lines()
    lines = witnessed.lines()
    if len(lines) > len(local):
        raise ValueError("the witnessed anchor is longer than the local one: truncated")
    for i, line in enumerate(lines):
        if line != local[i]:
            raise ValueError(f"anchor record {i} differs from its off-host witness")
    return len(lines)


def _load(paths: LedgerPaths) -> tuple[Ledger, State]:
    storage = _open_storage(paths)
    return storage, replay(storage.records)


def _write(
    paths: LedgerPaths,
    cap: object,
    build: Callable[[State], dict],
    recorded_at: str | None = None,
) -> dict:
    """Under the lock: re-verify, replay, build, validate and append one record."""
    if cap is not _JUDGE_CAP and cap is not _PROPOSER_CAP:
        raise PermissionError("appends need an e3 capability")
    with _locked(paths.lock):
        storage, state = _load(paths)
        record = build(state)
        if cap is _PROPOSER_CAP and (
            record["kind"] not in PROPOSER_KINDS
            or not str(record["actor"]).startswith(PROPOSER_PREFIX)
        ):
            raise PermissionError(f"a proposer may not write {record['kind']}")
        if cap is _JUDGE_CAP and record["actor"] != JUDGE:
            raise PermissionError("judge records carry actor 'judge'")
        when = recorded_at or now_iso()
        record = {**record, "recorded_at": when, "promotion_allowed": False}
        if record["kind"] == "proposed":
            record["proposed_at"] = when
            if record["candidate_id"] is None:
                record["candidate_id"] = sha256_bytes(
                    canonical(candidate_core(record, e3_version=state.genesis["e3_version"]))
                )
        full = {**record, "seq": len(storage.records), "prev_sha256": storage.head}
        _apply(state, full)  # the same invariants a later open replays
        appended, sha = storage._append(record)
        _fsync(paths.ledger)
        _fsync(paths.anchor)
    return {"record": appended, "sha256": sha}


# --- judge --------------------------------------------------------------------------


def _check_s11(s11: Ledger, genesis: dict) -> None:
    if not isinstance(s11, Ledger):
        raise TypeError("pass the S11 ledger (analysis.ledger_steered_exploration.Ledger)")
    if s11.path is None:
        raise ValueError("the bound S11 ledger must be the file ledger with its anchor, not in memory")
    s11.verify()
    if s11.genesis["ledger_id"] != genesis["s11_ledger_id"]:
        raise ValueError(
            f"this e3 ledger binds S11 ledger {genesis['s11_ledger_id']!r}, "
            f"not {s11.genesis['ledger_id']!r}"
        )


def _candidate(state: State, candidate_id: str) -> Candidate:
    candidate = state.candidates.get(candidate_id)
    if candidate is None:
        raise ValueError(f"candidate {str(candidate_id)[:12]} was never proposed")
    return candidate


class CandidateLedger:
    """The judge's handle on the E3 candidate ledger (see the module docstring)."""

    def __init__(self, paths: LedgerPaths, *, external_anchors: str | Path | None = None) -> None:
        self.paths = paths
        self.verify(external_anchors=external_anchors)

    @classmethod
    def open(
        cls,
        ledger_dir: str | Path | None = None,
        anchor_dir: str | Path | None = None,
        *,
        external_anchors: str | Path | None = None,
    ) -> CandidateLedger:
        return cls(LedgerPaths.from_dirs(ledger_dir, anchor_dir), external_anchors=external_anchors)

    @classmethod
    def genesis(
        cls,
        ledger_dir: str | Path | None = None,
        anchor_dir: str | Path | None = None,
        *,
        s11_ledger_id: str,
        ledger_id: str = LEDGER_ID,
        recorded_at: str | None = None,
    ) -> CandidateLedger:
        """Create the ledger. Refused if either file exists: one anchor pins one ledger."""
        paths = LedgerPaths.from_dirs(ledger_dir, anchor_dir)
        paths.anchor.parent.mkdir(parents=True, exist_ok=True)
        when = recorded_at or now_iso()
        record = {
            "kind": "genesis",
            "seq": 0,
            "prev_sha256": None,
            "promotion_allowed": False,
            "schema": SCHEMA,
            "ledger_id": ledger_id,
            "e3_version": VERSION,
            "s11_ledger_id": s11_ledger_id,
            "stages": list(STAGES),
            "created_at": when,
            "recorded_at": when,
        }
        _apply(None, record)
        with _locked(paths.lock):
            if Anchor(paths.anchor).lines():
                raise ValueError(
                    "the anchor already pins an e3 ledger: a second genesis is refused"
                )
            if paths.ledger.exists():
                raise ValueError("an e3 ledger file already exists: a second genesis is refused")
            line = canonical(record)
            verify_chain([line])
            with paths.ledger.open("xb") as stream:
                stream.write(line + b"\n")
            _fsync(paths.ledger)
            Anchor(paths.anchor).append(ledger_id, 0, sha256_bytes(line))
            _fsync(paths.anchor)
        return cls(paths)

    # --- reads ----------------------------------------------------------------------

    def verify(self, *, external_anchors: str | Path | None = None) -> dict:
        """Chain, anchor and every invariant (and the off-host witness, if given)."""
        with _locked(self.paths.lock):
            storage, state = _load(self.paths)
            witnessed = (
                _external_check(self.paths, external_anchors) if external_anchors else None
            )
        return {
            "ledger_id": state.genesis["ledger_id"],
            "records": len(storage.records),
            "head_sha256": storage.head,
            "witnessed_records": witnessed,
        }

    def state(self) -> State:
        with _locked(self.paths.lock):
            return _load(self.paths)[1]

    def records(self) -> tuple[dict, ...]:
        with _locked(self.paths.lock):
            return _load(self.paths)[0].records

    def families(self) -> list[str]:
        return sorted({c.family for c in self.state().candidates.values()})

    def family_counts(self, family: str) -> dict:
        """Honest denominators for one family: every candidate ever proposed counts.

        ``failures`` = ``abandoned`` + ``failed`` (closed by a fail, untestable or
        inconclusive stage result). ``withdrawn`` candidates stay in ``proposed``
        and spend no alpha.
        """
        state = self.state()
        members = [c for c in state.candidates.values() if c.family == family]
        abandoned = sum(c.terminal == "abandoned" for c in members)
        failed = sum(c.failed and c.terminal != "abandoned" for c in members)
        return {
            "family": family,
            "proposed": len(members),
            "declared_trials": sum(c.proposed["declared_trials"] for c in members),
            "withdrawn": sum(c.terminal == "withdrawn_pre_data" for c in members),
            "screened": sum("screen" in c.entered for c in members),
            "abandoned": abandoned,
            "failed": failed,
            "failures": abandoned + failed,
            "holdout_looks": sum("holdout" in c.entered for c in members),
            # once per distinct S11 allocation the family's holdout looks spent,
            # recorded at holdout entry (so abandoned looks still count)
            "alpha_spent": float(
                sum({c.allocation: c.alpha for c in members if c.allocation}.values())
            ),
            "forward_admitted": sum("forward" in c.entered for c in members),
            "promoted": sum(c.promoted for c in members),
            "suspended": sum(c.suspended for c in members),
            "retired": sum(c.terminal == "retired" for c in members),
        }

    # --- judge writes ---------------------------------------------------------------

    def stage_entered(
        self,
        candidate_id: str,
        stage: str,
        *,
        s11: Ledger | None = None,
        window: dict | None = None,
        s11_allocation_sha256: str | None = None,
        recorded_at: str | None = None,
    ) -> dict:
        """Enter the next stage. ``screen`` needs ``window`` + ``s11``; ``holdout`` the allocation."""

        def build(state: State) -> dict:
            candidate = _candidate(state, candidate_id)
            _check_order(candidate, stage, None)  # before consulting S11
            s11_id = s11_head = None
            if stage in ("screen", "holdout"):
                if s11 is None:
                    raise ValueError(f"{stage} is checked against the bound S11 ledger")
                _check_s11(s11, state.genesis)
                s11_id, s11_head = s11.genesis["ledger_id"], s11.head
            if stage == "screen":
                if not isinstance(window, dict) or set(window) != {"start", "end"}:
                    raise ValueError("screen declares its label window {start, end}")
                touched = s11.touched_windows()
                for sha in candidate.proposed["identity_sha256"]:
                    for used in touched.get(sha, []):
                        if overlaps((window["start"], window["end"]), used):
                            raise ValueError(
                                f"identity {sha[:12]} already touched an overlapping window "
                                f"in S11 ({used['source']}): refused at screen"
                            )
            s11_alpha = None
            if stage == "holdout":
                open_allocation = s11.open_allocation()
                if open_allocation is None or open_allocation[1] != s11_allocation_sha256:
                    raise ValueError(
                        "holdout must name S11's open allocation (not a closed or unknown one)"
                    )
                allocation = open_allocation[0]
                covered = {t["identity_sha256"] for t in allocation["trials"]}
                if not set(candidate.proposed["identity_sha256"]) <= covered:
                    raise ValueError("the S11 allocation does not declare this candidate's trials")
                s11_alpha = allocation["alpha"]
            return {
                "kind": "stage_entered",
                "actor": JUDGE,
                "candidate_id": candidate_id,
                "stage": stage,
                "window": dict(window) if stage == "screen" else window,
                "s11_ledger_id": s11_id,
                "s11_head_sha256": s11_head,
                "s11_allocation_sha256": s11_allocation_sha256,
                "s11_alpha": s11_alpha,
            }

        return _write(self.paths, _JUDGE_CAP, build, recorded_at)

    def stage_result(
        self,
        candidate_id: str,
        stage: str,
        result: str,
        *,
        receipt_sha256: str,
        alpha_spent: float | None = None,
        p_values: list | None = None,
        recorded_at: str | None = None,
    ) -> dict:
        def build(state: State) -> dict:
            candidate = _candidate(state, candidate_id)
            return {
                "kind": "stage_result",
                "actor": JUDGE,
                "candidate_id": candidate_id,
                "stage": stage,
                "result": result,
                "p_values": None if p_values is None else list(p_values),
                "alpha_spent": (
                    alpha_spent
                    if alpha_spent is not None
                    else candidate.alpha if stage == "holdout" else 0.0
                ),
                "s11_allocation_sha256": candidate.allocation if stage == "holdout" else None,
                "receipt_sha256": receipt_sha256,
            }

        return _write(self.paths, _JUDGE_CAP, build, recorded_at)

    def abandoned(
        self,
        candidate_id: str,
        reason: str,
        *,
        s11: Ledger | None = None,
        s11_abandoned_sha256: str | None = None,
        recorded_at: str | None = None,
    ) -> dict:
        """Abandon at any point; counts as a failure for the family.

        After an S11 allocation the allocation must be closed in S11 first: by
        S11 ``abandon()`` (pass its record sha as ``s11_abandoned_sha256``; alpha
        stays spent there), or by its ``run_result`` if the run was recorded.
        """

        def build(state: State) -> dict:
            candidate = _candidate(state, candidate_id)
            close_kind = close_sha = None
            if candidate.allocation is not None:
                if s11 is None:
                    raise ValueError("the candidate has an S11 allocation: pass the S11 ledger")
                _check_s11(s11, state.genesis)
                closing = [
                    (r["kind"], s11.line_sha(r["seq"]))
                    for r in s11.records
                    if r["kind"] in ("abandoned", "run_result")
                    and r.get("allocation_sha256") == candidate.allocation
                ]
                if not closing:
                    raise ValueError(
                        "the S11 allocation is still open: record S11 abandon() first"
                    )
                close_kind, close_sha = closing[0]
                if close_kind == "abandoned" and s11_abandoned_sha256 != close_sha:
                    raise ValueError("s11_abandoned_sha256 is not the S11 record that abandoned it")
                if close_kind == "run_result" and s11_abandoned_sha256 is not None:
                    raise ValueError("the S11 run was recorded, not abandoned")
            elif s11_abandoned_sha256 is not None:
                raise ValueError("no S11 allocation for this candidate")
            return {
                "kind": "abandoned",
                "actor": JUDGE,
                "candidate_id": candidate_id,
                "reason": reason,
                "s11_allocation_sha256": candidate.allocation,
                "s11_close_kind": close_kind,
                "s11_close_sha256": close_sha,
            }

        return _write(self.paths, _JUDGE_CAP, build, recorded_at)

    def withdrawn_pre_data(
        self, candidate_id: str, reason: str, *, recorded_at: str | None = None
    ) -> dict:
        return _write(
            self.paths,
            _JUDGE_CAP,
            lambda state: {
                "kind": "withdrawn_pre_data",
                "actor": JUDGE,
                "candidate_id": _candidate(state, candidate_id).candidate_id,
                "reason": reason,
            },
            recorded_at,
        )

    def promoted_research(
        self, candidate_id: str, *, receipt_sha256: str, recorded_at: str | None = None
    ) -> dict:
        return _write(
            self.paths,
            _JUDGE_CAP,
            lambda state: {
                "kind": "promoted_research",
                "actor": JUDGE,
                "candidate_id": _candidate(state, candidate_id).candidate_id,
                "receipt_sha256": receipt_sha256,
            },
            recorded_at,
        )

    def suspended(self, candidate_id: str, reason: str, *, recorded_at: str | None = None) -> dict:
        return _write(
            self.paths,
            _JUDGE_CAP,
            lambda state: {
                "kind": "suspended",
                "actor": JUDGE,
                "candidate_id": _candidate(state, candidate_id).candidate_id,
                "reason": reason,
            },
            recorded_at,
        )

    def retired(
        self, candidate_id: str, reason: str, *, receipt_sha256: str, recorded_at: str | None = None
    ) -> dict:
        return _write(
            self.paths,
            _JUDGE_CAP,
            lambda state: {
                "kind": "retired",
                "actor": JUDGE,
                "candidate_id": _candidate(state, candidate_id).candidate_id,
                "reason": reason,
                "receipt_sha256": receipt_sha256,
            },
            recorded_at,
        )


# --- proposers ----------------------------------------------------------------------


class ProposerClient:
    """The only write path a proposer gets: ``propose``, plus ``abandon``/``withdraw``
    of its own candidates. It holds no judge handle and no judge capability."""

    __slots__ = ("_paths", "_proposer")

    def __init__(self, token: object, paths: LedgerPaths, proposer: str) -> None:
        if token is not _CLIENT_TOKEN:
            raise TypeError("a ProposerClient is issued only by client_for_proposer()")
        if not _name(proposer):
            raise ValueError("proposer id must be a non-empty name without whitespace")
        object.__setattr__(self, "_paths", paths)
        object.__setattr__(self, "_proposer", proposer)

    def __setattr__(self, name: str, value) -> None:
        raise AttributeError("ProposerClient is read-only")

    @property
    def proposer_id(self) -> str:
        return self._proposer

    @property
    def _actor(self) -> str:
        return PROPOSER_PREFIX + self._proposer

    def propose(
        self,
        *,
        family: str,
        candidate_kind: str,
        trials: list[tuple[str, str]],
        spec: dict,
        rationale: str,
        engine_version: str,
        expected_sign: int = 0,
        retest_of: str | None = None,
    ) -> str:
        """Record a candidate before any data is read; returns its ``candidate_id``.

        ``trials`` are S11 ``(family, feature)`` pairs (``TARGET|label|fwdH``);
        each becomes a scientific identity. ``spec`` and ``rationale`` are
        hashed and never stored, so nothing but identifiers enters the ledger.
        """
        if not isinstance(spec, dict) or not spec:
            raise ValueError("spec must be a non-empty JSON object")
        if not isinstance(rationale, str) or not rationale.strip():
            raise ValueError("a candidate needs a rationale")
        pairs = [tuple(pair) for pair in trials]
        if not pairs or any(len(pair) != 2 for pair in pairs):
            raise ValueError("trials are (S11 family, feature) pairs")
        record = {
            "kind": "proposed",
            "actor": self._actor,
            "candidate_id": None,
            "family": family,
            "candidate_kind": candidate_kind,
            "identity": [scientific_identity(f, x) for f, x in pairs],
            "identity_sha256": [identity(f, x) for f, x in pairs],
            "declared_trials": len(pairs),
            "proposer": self._proposer,
            "engine_version": engine_version,
            "spec_sha256": sha256_bytes(canonical(spec)),
            "expected_sign": expected_sign,
            "rationale_sha256": sha256_bytes(rationale.encode("utf-8")),
            "retest_of": retest_of,
            "proposed_at": None,
        }
        return _write(self._paths, _PROPOSER_CAP, lambda state: record)["record"]["candidate_id"]

    def abandon(self, candidate_id: str, reason: str) -> dict:
        """Abandon an own candidate (before any S11 allocation); it counts as a failure."""
        return _write(
            self._paths,
            _PROPOSER_CAP,
            lambda state: {
                "kind": "abandoned",
                "actor": self._actor,
                "candidate_id": _candidate(state, candidate_id).candidate_id,
                "reason": reason,
                "s11_allocation_sha256": None,
                "s11_close_kind": None,
                "s11_close_sha256": None,
            },
        )

    def withdraw(self, candidate_id: str, reason: str) -> dict:
        """Withdraw an own candidate before any stage; it still counts as proposed."""
        return _write(
            self._paths,
            _PROPOSER_CAP,
            lambda state: {
                "kind": "withdrawn_pre_data",
                "actor": self._actor,
                "candidate_id": _candidate(state, candidate_id).candidate_id,
                "reason": reason,
            },
        )


def client_for_proposer(
    proposer_id: str,
    ledger_dir: str | Path | None = None,
    anchor_dir: str | Path | None = None,
) -> ProposerClient:
    """A proposer's client for an existing, verified ledger (``GRID_E3_*_DIR`` by default)."""
    paths = LedgerPaths.from_dirs(ledger_dir, anchor_dir)
    with _locked(paths.lock):
        _load(paths)
    return ProposerClient(_CLIENT_TOKEN, paths, proposer_id)
