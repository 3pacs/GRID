"""The ``grid-edge-looks`` ledger: insert-only registrations, looks and abandonments.

Storage reuses the S11 research ledger (:class:`analysis.ledger_steered_exploration.Ledger`
and its external :class:`~analysis.ledger_steered_exploration.Anchor`): canonical JSON
lines, ``prev_sha256`` hash chain, an anchor that refuses truncation, edits of the last
line and a restarted counter, ``promotion_allowed: false`` on every record. The stream
adds its own record kinds on a dedicated ledger (a placeholder S11 catalog satisfies
genesis; no S11 allocation ever runs on it):

``edge_policy``        seq 1, once: the frozen :func:`policy.policy_payload` and its sha256.
``edge_policy_amendment``  a later policy that only tightens the one before it
                       (:func:`policy.tightening_violations`); registrations after it are
                       sealed under its sha256.
``edge_registration``  sealed before the look's data is read; its line sha256 is the seal.
``edge_look``          the one look of a registration (verdicts + report sha); SPENT.
``edge_abandoned``     a registration closed without a look; its alpha stays spent.

:func:`load_state` re-derives and re-checks every invariant from the records, so a
ledger written by any other code path is refused rather than trusted.
"""

from __future__ import annotations

import signal
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from analysis.ledger_steered_exploration import Catalog, Ledger, sha256_bytes

from . import policy as P

POLICY_KIND = "edge_policy"
REGISTRATION_KIND = "edge_registration"
LOOK_KIND = "edge_look"
ABANDONED_KIND = "edge_abandoned"
AMENDMENT_KIND = "edge_policy_amendment"
STREAM_KINDS = (POLICY_KIND, REGISTRATION_KIND, LOOK_KIND, ABANDONED_KIND, AMENDMENT_KIND)

PLACEHOLDER_CATALOG = Catalog(
    families=("EDGE_LOOKS|change|fwd1",),
    features=("none|x",),
    classes=(("none|x", "edge_looks_only"),),
)

VERDICTS = ("PASS", "FAIL", "UNDERPOWERED")


class StreamRefused(ValueError):
    """A record the stream's rules do not allow (second look, budget, reused data...)."""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- construction -------------------------------------------------------------------


def create(path: str | Path | None, anchor: str | Path | None, recorded_at: str | None = None) -> Ledger:
    """Genesis + the policy record. Refuses an existing ledger or anchor (S11 rules)."""
    ledger = Ledger.create(
        path, anchor=anchor, catalog=PLACEHOLDER_CATALOG, ledger_id=P.LEDGER_ID, q=P.Q_YEAR,
        recorded_at=recorded_at,
    )
    _append(ledger, {
        "kind": POLICY_KIND,
        "policy": P.policy_payload(),
        "policy_sha256": P.policy_sha256(),
        "recorded_at": recorded_at or now_iso(),
    })
    load_state(ledger)
    return ledger


def memory(recorded_at: str | None = None) -> Ledger:
    """An in-memory ledger (dry runs and tests only; never written to disk)."""
    return create(None, None, recorded_at)


def open_ledger(path: str | Path, anchor: str | Path) -> Ledger:
    ledger = Ledger(path, anchor=anchor)
    load_state(ledger)
    return ledger


# --- state --------------------------------------------------------------------------


@dataclass
class StreamState:
    policy: dict
    policy_sha256: str
    registrations: dict[str, dict] = field(default_factory=dict)  # sha -> record (ledger order)
    looks: dict[str, dict] = field(default_factory=dict)  # registration sha -> look record
    abandoned: dict[str, dict] = field(default_factory=dict)  # registration sha -> record
    amendments: list[dict] = field(default_factory=list)  # edge_policy_amendment records, in order

    def closed(self, reg_sha: str) -> bool:
        return reg_sha in self.looks or reg_sha in self.abandoned

    def open_registrations(self) -> list[tuple[str, dict]]:
        return [(s, r) for s, r in self.registrations.items() if not self.closed(s)]

    def stage1_count(self, year: int) -> int:
        """Stage-1 registrations in a calendar year (abandoned ones included)."""
        return sum(
            1 for r in self.registrations.values()
            if r["stage"] == 1 and r["epoch"] == year
        )

    def registered_on(self, day: date) -> int:
        return sum(
            1 for r in self.registrations.values()
            if r["stage"] == 1 and r["recorded_at"][:10] == day.isoformat()
        )

    def proposal_ids(self) -> set[str]:
        return {r["proposal_id"] for r in self.registrations.values()}

    def consumption(self) -> list[dict]:
        """Every (group, horizon, period) already used: prior + every registration's window.

        A history registration consumes its window from the moment it is sealed. An open
        forward registration consumes ``[from, open)``; once looked, ``[from, last session]``.
        """
        out = [dict(c, source="prior") for c in self.policy["prior_consumption"]]
        for sha, reg in self.registrations.items():
            w = reg["window"]
            end = w.get("end")
            if reg["lane"] == "forward":
                if sha in self.looks:
                    end = self.looks[sha]["data"]["window_end"]
                elif sha in self.abandoned:
                    end = max(w["start"], self.abandoned[sha]["recorded_at"][:10])
                else:
                    end = None  # still accumulating
            out.append({"group": reg["group"], "horizon": reg["horizon"], "start": w["start"],
                        "end": end, "source": sha, "by": reg["proposal_id"]})
        return out


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise StreamRefused(msg)


def load_state(ledger: Ledger) -> StreamState:
    records = ledger.records
    _require(len(records) >= 2 and records[1].get("kind") == POLICY_KIND,
             "record 1 must be the edge_policy record")
    pol = records[1]
    _require(sha256_bytes(P.canonical_bytes(pol["policy"])) == pol["policy_sha256"],
             "edge_policy payload does not match its sha256")
    state = StreamState(policy=pol["policy"], policy_sha256=pol["policy_sha256"])
    for rec in records[2:]:
        kind = rec.get("kind")
        _require(kind in STREAM_KINDS and kind != POLICY_KIND,
                 f"record {rec['seq']}: kind {kind!r} does not belong on the edge-looks ledger")
        sha = ledger.line_sha(rec["seq"])
        if kind == AMENDMENT_KIND:
            _check_amendment(state, rec)
            state.policy, state.policy_sha256 = rec["policy"], rec["policy_sha256"]
            state.amendments.append(rec)
            continue
        if kind == REGISTRATION_KIND:
            _check_registration(state, rec)
            state.registrations[sha] = rec
        else:
            ref = rec.get("registration_sha256")
            _require(ref in state.registrations, f"record {rec['seq']}: unknown registration {ref}")
            _require(not state.closed(ref), f"record {rec['seq']}: registration {ref[:12]} already closed")
            if kind == LOOK_KIND:
                _check_look(rec)
                state.looks[ref] = rec
            else:
                _require(bool(rec.get("reason")), f"record {rec['seq']}: abandonment without a reason")
                state.abandoned[ref] = rec
    return state


def _overlap(a_start: str, a_end: str | None, b_start: str, b_end: str | None) -> bool:
    """Half-open [start, end) overlap; ``None`` end = open-ended."""
    return (b_end is None or a_start < b_end) and (a_end is None or b_start < a_end)


def conflicts(state: StreamState, group: str, horizon: str, start: str, end: str | None,
              lane: str, family: str | None = None) -> list[dict]:
    """Consumption entries a new window would reuse.

    History windows may not overlap anything already used for the same (group, horizon).
    A forward window starts after its registration and so cannot have been seen; it may
    run beside other forward registrations (each pays its own alpha) but not on a period
    a history registration has claimed.
    """
    hits = []
    for c in state.consumption():
        if c["group"] != group or c["horizon"] != horizon:
            continue
        if lane == "forward" and c["source"] != "prior":
            other = state.registrations.get(c["source"])
            # forward windows sealed before their data exists may run side by side, each paying
            # its own alpha (the union bound holds under any dependence) -- but never the same
            # family on the same group: that is one rule judged twice on the same sessions
            if other is not None and other["lane"] == "forward" and other["family"] != family:
                continue
        if _overlap(start, end, c["start"], c["end"]):
            hits.append(c)
    return hits


def _check_registration(state: StreamState, reg: dict) -> None:
    seq = reg.get("seq")
    _require(reg.get("policy_sha256") == state.policy_sha256, f"record {seq}: registration under another policy")
    _require(reg.get("stage") in (1, 2), f"record {seq}: stage must be 1 or 2")
    _require(reg.get("lane") in ("history", "forward"), f"record {seq}: bad lane")
    _require(reg.get("horizon") in P.HORIZONS, f"record {seq}: bad horizon")
    _require(reg["proposal_id"] not in state.proposal_ids(), f"record {seq}: proposal registered twice")
    trials = reg.get("trials") or []
    _require(1 <= len(trials) <= P.MAX_TRIALS_PER_LOOK and reg.get("k") == len(trials),
             f"record {seq}: k must equal the number of trials (1..{P.MAX_TRIALS_PER_LOOK})")
    sanity = reg.get("sanity") or {}
    _require(bool(sanity) and all(v == "ok" for v in sanity.values()),
             f"record {seq}: registration without a clean sanity report")
    if reg["stage"] == 1:
        _require(reg.get("confirms") is None, f"record {seq}: a stage-1 registration confirms nothing")
        _require(state.stage1_count(reg["epoch"]) < P.MAX_REGISTRATIONS_PER_YEAR,
                 f"record {seq}: the {reg['epoch']} budget of {P.MAX_REGISTRATIONS_PER_YEAR} looks is spent")
        _require(abs(reg["alpha_look"] - P.ALPHA_PER_LOOK) < 1e-15, f"record {seq}: wrong alpha for stage 1")
        _require(reg["epoch"] == int(reg["recorded_at"][:4]), f"record {seq}: epoch is the registration year")
    else:
        parent = state.registrations.get(reg.get("confirms"))
        _require(parent is not None and parent["stage"] == 1, f"record {seq}: a confirmation needs a stage-1 parent")
        look = state.looks.get(reg["confirms"])
        _require(look is not None and look["verdict"] == "PASS", f"record {seq}: only a PASS is confirmed")
        _require(reg["lane"] == "forward", f"record {seq}: confirmations are forward")
        _require(not any(r.get("confirms") == reg["confirms"] for r in state.registrations.values()),
                 f"record {seq}: confirmed twice")
        _require(abs(reg["alpha_look"] - P.CONFIRM_ALPHA) < 1e-15, f"record {seq}: wrong alpha for stage 2")
    _require(abs(reg["alpha_trial"] - reg["alpha_look"] / reg["k"]) < 1e-15, f"record {seq}: alpha_trial != alpha/k")
    w = reg["window"]
    if reg["lane"] == "history":
        _require(bool(w.get("end")) and w["start"] < w["end"] <= reg["recorded_at"][:10],
                 f"record {seq}: a history window must end before its registration")
    else:
        _require(w["start"] > reg["recorded_at"][:10], f"record {seq}: a forward window starts after registration")
    hits = conflicts(state, reg["group"], reg["horizon"], w["start"], w.get("end"), reg["lane"], reg["family"])
    _require(not hits, f"record {seq}: window reuses consumed data: "
                       + "; ".join(f"{h['start']}..{h['end']} by {h['by']}" for h in hits))


def _check_amendment(state: StreamState, rec: dict) -> None:
    seq = rec.get("seq")
    _require(isinstance(rec.get("policy"), dict)
             and sha256_bytes(P.canonical_bytes(rec["policy"])) == rec.get("policy_sha256"),
             f"record {seq}: amendment payload does not match its sha256")
    _require(rec.get("previous_policy_sha256") == state.policy_sha256,
             f"record {seq}: amendment does not follow the policy in force")
    _require(rec["policy_sha256"] != state.policy_sha256, f"record {seq}: amendment changes nothing")
    _require(bool(rec.get("reason")), f"record {seq}: amendment without a reason")
    bad = P.tightening_violations(state.policy, rec["policy"])
    _require(not bad, f"record {seq}: amendment loosens the policy: " + "; ".join(bad[:5]))


def _check_look(rec: dict) -> None:
    _require(rec.get("verdict") in VERDICTS, f"record {rec['seq']}: bad verdict")
    _require(all(t.get("verdict") in VERDICTS for t in rec.get("trials", [])),
             f"record {rec['seq']}: bad trial verdict")
    _require(len(rec.get("report_sha256", "")) == 64, f"record {rec['seq']}: look without its report sha256")


# --- appends ------------------------------------------------------------------------


def _append(ledger: Ledger, record: dict) -> tuple[dict, str]:
    """S11 append with SIGTERM/SIGINT held off: the ledger line and its anchor line are written
    as a pair (a kill between them would leave the ledger refusing to open; see the runbook)."""
    blocked = {signal.SIGTERM, signal.SIGINT, signal.SIGHUP}
    old = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
    try:
        return ledger._append(record)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, old)


def amend_policy(ledger: Ledger, reason: str, recorded_at: str | None = None) -> str | None:
    """Record the code's policy as an amendment when it differs from the one in force.

    Returns the amendment's sha256, or ``None`` when the ledger already carries this
    policy. A code policy that loosens anything is refused (:class:`StreamRefused`), and
    then no registration can be sealed until the code is put right.
    """
    state = load_state(ledger)
    new = P.policy_payload()
    new_sha = P.policy_sha256()
    if new_sha == state.policy_sha256:
        return None
    record = {"kind": AMENDMENT_KIND, "policy": new, "policy_sha256": new_sha,
              "previous_policy_sha256": state.policy_sha256, "reason": reason,
              "recorded_at": recorded_at or now_iso()}
    _check_amendment(state, {**record, "seq": len(ledger.records)})
    _, sha = _append(ledger, record)
    return sha


def append_registration(ledger: Ledger, registration: dict) -> str:
    state = load_state(ledger)
    _require(state.policy_sha256 == P.policy_sha256(),
             "the code's policy is not the policy in force on the ledger: amend_policy first")
    record = {**registration, "kind": REGISTRATION_KIND, "policy_sha256": state.policy_sha256}
    record.setdefault("recorded_at", now_iso())
    probe = {**record, "seq": len(ledger.records)}
    _check_registration(state, probe)
    _, sha = _append(ledger, record)
    return sha


def append_look(ledger: Ledger, reg_sha: str, look: dict) -> str:
    state = load_state(ledger)
    _require(reg_sha in state.registrations, f"unknown registration {reg_sha}")
    _require(not state.closed(reg_sha), f"registration {reg_sha[:12]} already has its look or was abandoned")
    record = {**look, "kind": LOOK_KIND, "registration_sha256": reg_sha}
    record.setdefault("recorded_at", now_iso())
    _check_look({**record, "seq": len(ledger.records)})
    _, sha = _append(ledger, record)
    return sha


def append_abandoned(ledger: Ledger, reg_sha: str, reason: str, recorded_at: str | None = None) -> str:
    state = load_state(ledger)
    _require(reg_sha in state.registrations, f"unknown registration {reg_sha}")
    _require(not state.closed(reg_sha), f"registration {reg_sha[:12]} already closed")
    _require(bool(reason), "an abandonment needs a reason")
    _, sha = _append(ledger, {
        "kind": ABANDONED_KIND, "registration_sha256": reg_sha, "reason": reason,
        "alpha_spent": state.registrations[reg_sha]["alpha_look"], "recorded_at": recorded_at or now_iso(),
    })
    return sha
