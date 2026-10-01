"""Panel mode: the VS1 panel statistic and one-shot custody for any registered construct.

GD6a (granular discovery, Route B). VS1 (``analysis.panel_insider_density``)
holds a reviewed cross-sectional statistic and one-shot custody, hard-wired to
one construct (Form 4 insider-buy density), one sector and the VS1
registries. This module lifts both into a construct-agnostic mode, by
**importing** the VS1 and S09 machinery read-only (it never edits them; they
are E0 ``MACHINERY_FILES`` and VS1 v8 pins them):

Statistic (unchanged, byte for byte; ``tests/test_panel_mode_e0_parity.py``)
---------------------------------------------------------------------------
* per decision date, the Spearman rank IC across a sector's issuers between the
  construct and the issuer's forward return minus the sector benchmark's
  (:func:`analysis.panel_insider_density.rank_ic_series`);
* the data-driven block (:func:`analysis.offline_research_proof.autocorrelation_block`,
  ``MIN_BLOCKS`` cap) and the block sign-flip null
  (:func:`analysis.panel_insider_density.signflip_pvalues`), at the run's seed;
* the two sensitivity nulls, reported only
  (``time_alignment_pvalue``, ``entity_shuffle_pvalue``);
* Holm at the ledger-issued run alpha over every declared trial (untestable
  ones at p = 1), BH reported; a frozen, write-once discovery;
* holdout: Bonferroni over the frozen selections, same sign required, frozen
  block; an optional entity-split second holdout; ``promotion_allowed`` is
  always false.

For a construct pre-registered with ``direction=+1`` and the
``positive_vs_zero`` magnitude, :func:`measure_panel_trial` returns exactly
``panel_insider_density.measure_trial``'s record (plus ``direction`` and
``p_one_sided``). A construct pre-registered as negative takes its one-sided p
from ``signflip_pvalues(..., direction=-1)``: never a post-hoc flip.

Custody (generic, :class:`PanelRegistry`)
------------------------------------------
A hash-pinned prereg body, a hash-chained registry
(:class:`analysis.research_forward_log.ForwardLog`, own file names) and an
off-host vault witness under ``05-GRID/Paper-Log/granular/`` come before any
price read. No price is read without a :class:`PanelDiscoveryKey` /
:class:`PanelHoldoutKey`; a second opening is refused. Witness paths are never
under ``05-GRID/Paper-Log/vs1/`` and never contain ``vs1``: VS1 v8's census
refuses on both.

Outcome-window guard (:class:`OutcomeWindowGuard`)
--------------------------------------------------
Standing rules R2/R3 in code: Technology / XLK / denylisted-ticker outcomes in
``[2007-11-02, 2026-07-01)`` need a :class:`V8TerminalWitness`; non-Technology
sector-relative outcomes in that window need a :class:`SectorsV6Witness`; the
sectors-v6 confirmatory trials are always refused. Both witnesses are issued
only against a reviewed pin of the terminal (v8) / registration (sectors-v6)
head; until those pins land the guard refuses, which is the intended state.

Nothing here reads a price or a label by itself, writes a DB, or is a trading
signal. Ledger (S11) integration and the CLI are GD6b.
"""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from analysis import panel_insider_density as v1
from analysis.offline_research_proof import (
    MIN_BLOCKS,
    autocorrelation_block,
    bh_adjusted,
    corrected_p,
    digest,
    holm_adjusted,
    stamp,
    write_once,
)
from analysis.research_forward_log import ForwardLog, canonical

REPO = Path(__file__).resolve().parent.parent
VERSION = "panel-mode-v1"
ORIGIN = "granular_panel_mode"

TrialPanel = v1.TrialPanel
HOLDOUT_ALPHA = v1.HOLDOUT_ALPHA
BH_Q = v1.BH_Q
MIN_N = v1.MIN_N
MIN_ENTITIES = v1.MIN_ENTITIES
MAGNITUDES = ("positive_vs_zero", "none")

# --- D-GD6-1: the S11 ledger a GD run spends from (owner decision, both implemented) ----------

#: (a) recommended: a separate ledger with its own q (cross-ledger FWER is NOT jointly controlled).
#: (b) share VS1's ``grid-granular-panel`` from k=3 (k=1 VS1 Technology, k=2 the other sectors).
LEDGER_OPTIONS: Mapping[str, Mapping[str, Any]] = {
    "separate": {"ledger_id": "grid-granular-families", "q": 0.10, "first_k": 1,
                 "disclosure": "separate ledger: FWER across grid-granular-panel and this ledger "
                               "is not jointly controlled"},
    "shared": {"ledger_id": v1.LEDGER_ID, "q": v1.LEDGER_Q, "first_k": v1.OTHER_SECTORS_RUN_K + 1,
               "disclosure": "shares grid-granular-panel with VS1 (k=1, k=2 reserved)"},
}
#: The owner's recorded D-GD6-1 pick ("separate" or "shared"). Unset: no real registration.
OWNER_LEDGER_DECISION: str | None = None


def run_alpha(k: int, q: float) -> float:
    return v1.run_alpha(k, q)


# --- construct and run specs -------------------------------------------------------------------

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_SECTOR = re.compile(r"^[A-Za-z][A-Za-z &.-]{0,63}$")
SECTORS_V6_CONSTRUCTS = frozenset({"A30", "A90"})
SECTORS_V6_WINDOWS = frozenset({30, 90})
SECTORS_V6_HORIZONS = frozenset({5, 20})


@dataclass(frozen=True)
class ConstructSpec:
    """A registered construct: what panel mode may test (frozen into the registration).

    ``name`` + ``window_days`` form the S11 feature ``{name}|W{window_days}``;
    ``feature_class`` prefixes the family key. ``scorer`` names the GD5 spec(s)
    or scorer id that produced the frozen artifact. ``direction`` is the
    pre-registered sign. ``confirmatory_horizons`` are the confirmatory trials,
    every other horizon is exploratory. ``contains_price`` marks constructs
    computed from prices (GD9 PX variants): their feature window is guarded as
    an outcome window too (``price_lookback_days`` before each decision).
    ``insider_buy_density`` declares a Form 4 insider-buy density construct (the
    sectors-v6 confirmatory family is refused whatever the construct is called;
    see :meth:`OutcomeWindowGuard.check_trial`).
    """

    name: str
    feature_class: str
    scorer: str
    window_days: int
    horizons: tuple[int, ...]
    direction: int
    confirmatory_horizons: tuple[int, ...]
    channels: tuple[str, ...] = ()
    event_filter: str = ""
    contains_price: bool = False
    price_lookback_days: int = 0
    artifact_kinds: tuple[str, ...] = ()
    magnitude: str = "none"
    insider_buy_density: bool = False

    def validate(self) -> None:
        if not _NAME.fullmatch(self.name) or not _NAME.fullmatch(self.feature_class) or not self.scorer:
            raise ValueError("construct name, feature_class and scorer are required ([A-Za-z0-9_.-])")
        if not isinstance(self.window_days, int) or self.window_days < 1:
            raise ValueError("window_days must be a positive integer")
        if not self.horizons or len(set(self.horizons)) != len(self.horizons) \
                or any(not isinstance(h, int) or h < 1 for h in self.horizons):
            raise ValueError("horizons must be distinct positive integers")
        if self.direction not in (1, -1):
            raise ValueError("direction is pre-registered as +1 or -1")
        if not set(self.confirmatory_horizons) <= set(self.horizons):
            raise ValueError("confirmatory horizons must be declared horizons")
        if self.magnitude not in MAGNITUDES:
            raise ValueError(f"magnitude must be one of {MAGNITUDES}")
        if self.contains_price and self.price_lookback_days < 1:
            raise ValueError("a construct that contains prices declares its price lookback")
        if not self.artifact_kinds:
            raise ValueError("declare the frozen artifact kinds the construct is read from")
        if not self.channels or any(not str(c).strip() for c in self.channels):
            raise ValueError("declare the construct's event channels")

    @property
    def feature(self) -> str:
        return f"{self.name}|W{self.window_days}"

    def trials(self) -> tuple[str, ...]:
        return tuple(f"{self.name}|fwd{h}" for h in self.horizons)

    def is_confirmatory(self, trial: str) -> bool:
        return trial_horizon(trial) in self.confirmatory_horizons

    def digest(self) -> str:
        return digest(asdict(self))


def trial_horizon(trial: str) -> int:
    name, _, fwd = trial.rpartition("|fwd")
    if not name or not fwd.isdigit():
        raise ValueError(f"not a panel trial name: {trial!r}")
    return int(fwd)


def family(sector: str, horizon: int) -> str:
    """S11 family of a sector-relative panel trial (``SECTOR:{sector}|rel_ret|fwd{h}``)."""
    return f"SECTOR:{sector}|rel_ret|fwd{horizon}"


def family_key(construct: ConstructSpec, sector: str, horizon: int) -> str:
    """``{feature_class}::SECTOR:{sector}|rel_ret|fwd{h}``: Thompson allocation steers across both."""
    return f"{construct.feature_class}::{family(sector, horizon)}"


@dataclass(frozen=True)
class PanelRunSpec:
    """A run's declared identity (frozen into the registration and the discovery manifest).

    ``benchmarks`` pairs each sector with its benchmark ETF. ``windows`` are
    ISO timestamps (discovery start, split, end). ``entity_split_salt``
    declares the optional entity-split second holdout: entities whose salted
    hash is odd are never seen in discovery and form the second holdout.
    ``supersedes`` names earlier granular registries that must have stayed at
    their 2-record registration (an older registry that grew is refused).
    """

    run_id: str
    registry_id: str
    ledger_option: str
    ledger_id: str
    ledger_q: float
    run_k: int
    sectors: tuple[str, ...]
    benchmarks: tuple[tuple[str, str], ...]
    discovery_start: str
    split: str
    end: str
    prereg_sha256: str
    perms: int = v1.PERMS
    seed: int = v1.SEED
    min_n: int = MIN_N
    entity_split_salt: str | None = None
    supersedes: tuple[str, ...] = ()

    def validate(self) -> None:
        check_registry_id(self.registry_id)
        if not self.run_id or not self.sectors or len(set(self.sectors)) != len(self.sectors):
            raise ValueError("a run declares an id and distinct sectors")
        if any(not _SECTOR.fullmatch(s) for s in self.sectors):
            raise ValueError("invalid sector name")
        bench = dict(self.benchmarks)
        if len(bench) != len(self.benchmarks) or set(bench) != set(self.sectors):
            raise ValueError("every sector has exactly one benchmark")
        for sector, etf in bench.items():
            if v1.EQUITY_SECTORS.get(sector) != etf:
                raise ValueError(f"{sector!r} is not one of the 11 equity sectors with its benchmark ETF "
                                 f"({etf!r}); see analysis.panel_insider_density.EQUITY_SECTORS")
        option = LEDGER_OPTIONS.get(self.ledger_option)
        if option is None:
            raise ValueError(f"ledger_option must be one of {sorted(LEDGER_OPTIONS)}")
        if self.ledger_id != option["ledger_id"] or self.ledger_q != option["q"] or self.run_k < option["first_k"]:
            raise ValueError(
                f"ledger option {self.ledger_option!r} spends from {option['ledger_id']} (q={option['q']}) "
                f"at k >= {option['first_k']}"
            )
        lo, split, hi = stamp(self.discovery_start), stamp(self.split), stamp(self.end)
        if not lo < split < hi:
            raise ValueError("windows must satisfy discovery_start < split < end")
        if not v1._is_hex64(self.prereg_sha256):
            raise ValueError("prereg_sha256 must be a sha256 hex digest")
        if self.perms < 99 or self.min_n < 1:
            raise ValueError("perms >= 99 and min_n >= 1")
        if self.entity_split_salt is not None and not self.entity_split_salt:
            raise ValueError("entity_split_salt, when declared, is non-empty")
        for rid in self.supersedes:
            check_registry_id(rid)

    @property
    def alpha(self) -> float:
        return run_alpha(self.run_k, self.ledger_q)

    def benchmark(self, sector: str) -> str:
        return dict(self.benchmarks)[sector]

    def window_bounds(self, window: str) -> tuple[pd.Timestamp, pd.Timestamp]:
        if window == "discovery":
            return pd.Timestamp(stamp(self.discovery_start)), pd.Timestamp(stamp(self.split))
        if window == "holdout":
            return pd.Timestamp(stamp(self.split)), pd.Timestamp(stamp(self.end))
        raise ValueError("window must be discovery or holdout")

    def as_record(self) -> dict:
        record = asdict(self)
        record["benchmarks"] = [list(pair) for pair in self.benchmarks]
        return record

    def digest(self) -> str:
        return digest(self.as_record())


def as_constructs(constructs: ConstructSpec | Sequence[ConstructSpec]) -> tuple[ConstructSpec, ...]:
    """One construct or a family of them (distinct names), each validated."""
    out = (constructs,) if isinstance(constructs, ConstructSpec) else tuple(constructs)
    if not out or any(not isinstance(c, ConstructSpec) for c in out):
        raise ValueError("a run declares one or more ConstructSpec")
    if len({c.name for c in out}) != len(out):
        raise ValueError("construct names must be distinct within a run")
    for c in out:
        c.validate()
    return out


def declared_trials(constructs: ConstructSpec | Sequence[ConstructSpec], run: PanelRunSpec) -> list[tuple[str, str]]:
    """Every (sector, trial) of the run, in declared order (sector, construct, horizon): the Holm denominator."""
    return [(sector, trial) for sector in run.sectors for c in as_constructs(constructs) for trial in c.trials()]


def construct_of(constructs: Sequence[ConstructSpec], trial: str) -> ConstructSpec:
    name = trial.rpartition("|fwd")[0]
    for c in constructs:
        if c.name == name:
            return c
    raise ValueError(f"no declared construct for trial {trial!r}")


def constructs_digest(constructs: Sequence[ConstructSpec]) -> str:
    return digest([asdict(c) for c in constructs])


# --- the outcome-window guard (R2 / R3 in code) ------------------------------------------------

QUARANTINE_START = date(2007, 11, 2)
QUARANTINE_END = date(2026, 7, 1)  # exclusive
TECHNOLOGY_SECTORS = frozenset({"technology", "information technology"})
TECHNOLOGY_BENCHMARK = "XLK"
DENYLIST_PATH = REPO / "evals" / "e0" / "data" / "vs1_technology_denylist.json"
#: VS1 v8 / sectors-v6 witness files on the vault (read, never written, here).
V8_WITNESS_PATH = v1.canonical_witness_path("vs1-v8")
SECTORS_V6_WITNESS_PATH = v1.canonical_witness_path("sectors-v6")
#: Reviewed pins, set by the PR that records each event (pattern of v8's pinned v7 STOP head):
#: ``{"head_sha256": <64 hex>, "records": <int>, "status": "holdout_result" | "STOP"}`` for the
#: v8 terminal record, ``{"head_sha256": <64 hex>, "records": 2}`` for the sectors-v6 registration.
V8_TERMINAL_PIN: Mapping[str, Any] | None = None
SECTORS_V6_PIN: Mapping[str, Any] | None = None
_GUARD_WITNESS_TOKEN = object()


def normalise_ticker(ticker: str) -> str:
    """Upper case, separators dropped (``BRK.B``, ``BRK-B`` and ``BRK/B`` are one ticker)."""
    return re.sub(r"[^A-Z0-9]", "", str(ticker).upper())


def load_technology_denylist(path: Path = DENYLIST_PATH) -> frozenset[str]:
    """E0's VS1 Technology denylist (the 782-candidate v2 universe, sector-map Technology, XLK), normalised."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    tickers = raw.get("tickers") if isinstance(raw, dict) else None
    if not isinstance(tickers, list) or len(tickers) < 782:
        raise ValueError("the Technology denylist is missing or truncated")
    return frozenset(normalise_ticker(t) for t in tickers) | {TECHNOLOGY_BENCHMARK}


class _GuardWitness:
    """A terminal/registration record on the pinned vault ``main`` (issued by an issuer below)."""

    kind = ""

    def __init__(self, token: object, *, tip: str, path: str, head_sha256: str, records: int,
                 status: str | None) -> None:
        if token is not _GUARD_WITNESS_TOKEN:
            raise TypeError(f"a {type(self).__name__} is issued only by its issuer")
        self.tip, self.path, self.head_sha256, self.records, self.status = tip, path, head_sha256, records, status

    def receipt(self) -> dict:
        return {"kind": self.kind, "tip": self.tip, "path": self.path, "head_sha256": self.head_sha256,
                "records": self.records, "status": self.status}


class V8TerminalWitness(_GuardWitness):
    """VS1 v8's chain ends in a witnessed ``holdout_result`` or STOP (lifts R2)."""

    kind = "vs1-v8-terminal"


class SectorsV6Witness(_GuardWitness):
    """The sectors-v6 two-record registration is witnessed on vault ``main`` (lifts R3)."""

    kind = "sectors-v6-registration"


def _as_day(value: Any) -> date:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("window instants must carry a timezone")
        return value.astimezone(timezone.utc).date()
    if isinstance(value, pd.Timestamp):
        return _as_day(value.to_pydatetime())
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 10:
            return date.fromisoformat(text)
        return _as_day(stamp(text))
    raise TypeError(f"not a date: {value!r}")


def _tokens(name: str) -> set[str]:
    return {t for t in re.split(r"[^A-Z0-9]+", str(name).upper()) if t}


def _is_form4(name: str) -> bool:
    return "form4" in name or "form345" in name or name in ("f4", "sec4")


_SELL_TOKENS = frozenset({"S", "SELL", "SELLS", "SALE", "SALES", "D", "DISPOSITION", "DISPOSE"})


def is_sectors_v6_family(construct: ConstructSpec) -> bool:
    """Form 4 insider-buy density at W30 / W90, however it is named (conservative).

    True when declared (``insider_buy_density``), when the name or scorer carries an
    ``A30`` / ``A90`` token, or when every channel is Form 4, the window is 30 or 90
    days and the event filter is not explicitly a sell / disposition filter
    (Form 4 = a channel or feature class naming form4 / form345 / f4 / sec4).
    """
    if construct.insider_buy_density:
        return True
    tokens = _tokens(construct.name) | _tokens(construct.scorer)
    if tokens & SECTORS_V6_CONSTRUCTS:
        return True
    channels = [re.sub(r"[^a-z0-9]", "", c.lower()) for c in construct.channels]
    form4_only = (bool(channels) and all(_is_form4(c) for c in channels)) or \
        _is_form4(re.sub(r"[^a-z0-9]", "", construct.feature_class.lower()))
    sell = bool(_tokens(construct.event_filter) & _SELL_TOKENS)
    return form4_only and construct.window_days in SECTORS_V6_WINDOWS and not sell


class OutcomeWindowGuard:
    """Refuses outcome windows that standing rules R2/R3 quarantine.

    ``check`` is called before any price read and again on every panel a
    discovery or holdout measures. ``window`` is ``(start, end)``, end
    exclusive (dates, tz-aware datetimes or ISO strings).
    """

    def __init__(self, *, v8_terminal: V8TerminalWitness | None = None,
                 sectors_v6: SectorsV6Witness | None = None,
                 extra_technology_tickers: Iterable[str] = ()) -> None:
        if v8_terminal is not None and type(v8_terminal) is not V8TerminalWitness:
            raise PermissionError("unknown witness: R2 lifts only on a V8TerminalWitness")
        if sectors_v6 is not None and type(sectors_v6) is not SectorsV6Witness:
            raise PermissionError("unknown witness: R3 lifts only on a SectorsV6Witness")
        self.v8_terminal, self.sectors_v6 = v8_terminal, sectors_v6
        # The E0 denylist is always in force; callers can only add to it.
        self.denylist = load_technology_denylist() | {normalise_ticker(t) for t in extra_technology_tickers}

    @staticmethod
    def in_quarantine(window: tuple[Any, Any]) -> bool:
        lo, hi = _as_day(window[0]), _as_day(window[1])
        if hi <= lo:
            raise ValueError("an outcome window must have start < end")
        return lo < QUARANTINE_END and hi > QUARANTINE_START

    def check(self, sector: str, tickers: Iterable[str], benchmark: str, window: tuple[Any, Any]) -> dict:
        inside = self.in_quarantine(window)
        tickers = [normalise_ticker(t) for t in tickers]
        hits = sorted({t for t in tickers if t in self.denylist})
        xlk = TECHNOLOGY_BENCHMARK in _tokens(benchmark) or normalise_ticker(benchmark) in self.denylist
        technology_sector = str(sector).strip().lower() in TECHNOLOGY_SECTORS
        technology = technology_sector or xlk or bool(hits)
        if inside and technology and self.v8_terminal is None:
            reason = ("sector Technology" if technology_sector else
                      f"benchmark {benchmark}" if xlk else f"denylisted tickers {hits[:5]}")
            raise PermissionError(
                f"R2: {reason} with an outcome window intersecting [{QUARANTINE_START}, {QUARANTINE_END}) "
                "needs VS1 v8's witnessed terminal record (V8TerminalWitness)"
            )
        if inside and not technology_sector and self.sectors_v6 is None:
            raise PermissionError(
                f"R3: a {sector} sector-relative outcome window intersecting "
                f"[{QUARANTINE_START}, {QUARANTINE_END}) needs the witnessed sectors-v6 registration "
                "(SectorsV6Witness)"
            )
        return {"sector": sector, "benchmark": benchmark,
                "window": [_as_day(window[0]).isoformat(), _as_day(window[1]).isoformat()],
                "in_quarantine": inside, "technology": technology,
                "witnesses": [w.receipt() for w in (self.v8_terminal, self.sectors_v6) if w is not None]}

    @staticmethod
    def check_trial(construct: ConstructSpec, sector: str, horizon: int) -> None:
        """The sectors-v6 confirmatory trials run only in the sectors-v6 harness (always refused)."""
        if (is_sectors_v6_family(construct) and str(sector).strip().lower() not in TECHNOLOGY_SECTORS
                and horizon in SECTORS_V6_HORIZONS):
            raise PermissionError(
                f"R3: {construct.name}|fwd{horizon} in {sector} is a sectors-v6 confirmatory trial "
                "(Form 4 A30/A90 insider-buy density, non-Technology, fwd5/fwd20); only the sectors-v6 "
                "harness runs it"
            )

    def check_panel(self, construct: ConstructSpec, sector: str, benchmark: str, panel: TrialPanel) -> dict:
        """Guard one measured panel: its decision-to-label-end span (and price lookback)."""
        self.check_trial(construct, sector, panel.horizon)
        if not panel.decision_at:
            raise ValueError("an empty panel has no outcome window")
        lo = _as_day(stamp(panel.decision_at[0])) - timedelta(days=construct.price_lookback_days)
        hi = _as_day(stamp(panel.label_end[-1])) + timedelta(days=1)
        return self.check(sector, panel.entities, benchmark, (lo, hi))


def require_guard(guard: Any) -> "OutcomeWindowGuard":
    """The guard itself (not a subclass that could relax ``check``)."""
    if type(guard) is not OutcomeWindowGuard:
        raise PermissionError("outcome reads need the OutcomeWindowGuard itself")
    return guard


def _pinned_anchor(pin: Mapping[str, Any] | None, *, records: int | None = None) -> tuple[str, int]:
    if not pin or not v1._is_hex64(pin.get("head_sha256")) or not isinstance(pin.get("records"), int):
        raise PermissionError("not pinned")
    if records is not None and pin["records"] != records:
        raise PermissionError("pin covers the wrong number of records")
    return pin["head_sha256"], pin["records"]


def _last_anchor_at_tip(repo: Path, tip: str, path: str, head: str, records: int) -> None:
    content = v1._git(repo, "show", f"{tip}:{path}", binary=True)
    if b"\r" in content or not content.endswith(b"\n"):
        raise PermissionError(f"{path} must have canonical LF line endings")
    lines = content.splitlines()
    try:
        last = json.loads(lines[-1])
    except (ValueError, IndexError) as exc:
        raise PermissionError(f"{path}: last anchor is not JSON") from exc
    if lines[-1] != canonical(last) or last.get("head_sha256") != head or last.get("records") != records:
        raise PermissionError(f"{path}: the last witnessed anchor differs from the pinned head")


def issue_v8_terminal_witness(vault_repo: Path, *, remote_url: str | None = None) -> V8TerminalWitness:
    """R2 lift: the v8 witness on the pinned vault ``main`` ends in the pinned terminal head."""
    try:
        head, records = _pinned_anchor(V8_TERMINAL_PIN)
    except PermissionError:
        raise PermissionError("VS1 v8 is not pinned terminal (V8_TERMINAL_PIN unset): R2 stays in force") from None
    status = V8_TERMINAL_PIN.get("status")
    if status not in ("holdout_result", "STOP"):
        raise PermissionError("the v8 terminal record is a holdout_result or a STOP")
    tip, _ = _walk_history(vault_repo, V8_WITNESS_PATH, remote_url=remote_url)
    _last_anchor_at_tip(Path(vault_repo), tip, V8_WITNESS_PATH, head, records)
    return V8TerminalWitness(_GUARD_WITNESS_TOKEN, tip=tip, path=V8_WITNESS_PATH, head_sha256=head,
                             records=records, status=status)


def issue_sectors_v6_witness(vault_repo: Path, *, remote_url: str | None = None) -> SectorsV6Witness:
    """R3 lift: the sectors-v6 witness on the pinned vault ``main`` holds the pinned 2-record registration."""
    try:
        head, records = _pinned_anchor(SECTORS_V6_PIN, records=2)
    except PermissionError:
        raise PermissionError("sectors-v6 is not pinned registered (SECTORS_V6_PIN unset): R3 stays in force") from None
    tip, _ = _walk_history(vault_repo, SECTORS_V6_WITNESS_PATH, remote_url=remote_url)
    content = v1._git(Path(vault_repo), "show", f"{tip}:{SECTORS_V6_WITNESS_PATH}", binary=True)
    first = content.splitlines()[0] if content else b""
    try:
        anchor = json.loads(first)
    except ValueError as exc:
        raise PermissionError("sectors-v6 registration anchor is not JSON") from exc
    if anchor.get("head_sha256") != head or anchor.get("records") != records:
        raise PermissionError("the sectors-v6 witness does not start with the pinned registration")
    return SectorsV6Witness(_GUARD_WITNESS_TOKEN, tip=tip, path=SECTORS_V6_WITNESS_PATH, head_sha256=head,
                            records=records, status="registered")


# --- statistics (VS1 primitives, composed; parity-tested) --------------------------------------


def measure_panel_trial(
    panel: TrialPanel,
    *,
    direction: int,
    block: int | None = None,
    perms: int = v1.PERMS,
    seed: int = v1.SEED,
    sensitivity_perms: int = v1.SENSITIVITY_PERMS,
    min_n: int = MIN_N,
    sensitivity: bool = True,
    magnitude: str = "positive_vs_zero",
) -> dict:
    """``measure_trial`` with a pre-registered direction.

    The first-pass record is ``panel_insider_density.measure_trial``'s,
    computed from the same primitives in the same order (two-sided ``p`` and
    ``p_one_sided_positive`` always in the +1 direction, as VS1 reports them).
    Added: ``direction`` and ``p_one_sided`` (one-sided in the pre-registered
    direction; equal to ``p_one_sided_positive`` when ``direction == 1``).
    """
    if direction not in (1, -1):
        raise ValueError("direction is +1 or -1")
    if magnitude not in MAGNITUDES:
        raise ValueError(f"magnitude must be one of {MAGNITUDES}")
    ic, counts = v1.rank_ic_series(panel.feature, panel.label)
    rows = np.flatnonzero(np.isfinite(ic))
    series = ic[rows]
    base = {
        "n": int(len(rows)),
        "decisions": len(panel.decision_at),
        "median_entities": int(np.median(counts[rows])) if len(rows) else 0,
        "labels": v1.missing_labels(panel),
    }
    if len(rows) < min_n:
        return {**base, "mean_ic": None, "p": 1.0, "p_one_sided_positive": 1.0,
                "status": "insufficient_data", "block": None, "block_basis": None,
                "direction": direction, "p_one_sided": 1.0}
    if block is None:
        block, basis = autocorrelation_block(series.tolist(), 0)
        basis = {**basis, "block": block}
    else:
        basis = {"rule": "frozen", "block": block}
    mean_ic, p, p_one = v1.signflip_pvalues(series, block, perms, seed, 1)
    p_directed = p_one if direction == 1 else v1.signflip_pvalues(series, block, perms, seed, direction)[2]
    out = {
        **base,
        "mean_ic": mean_ic,
        "ic_sd": float(series.std(ddof=1)) if len(series) > 1 else None,
        "ic_positive_share": float((series > 0).mean()),
        "p": p,
        "p_one_sided_positive": p_one,
        "status": "tested",
        "block": int(block),
        "block_basis": basis,
    }
    if sensitivity:
        ta_stat, ta_p = v1.time_alignment_pvalue(panel.feature, panel.label, rows, block, sensitivity_perms, seed)
        out["sensitivity"] = {
            "time_alignment_mean": ta_stat,
            "time_alignment_p": ta_p,
            "entity_shuffle_p": v1.entity_shuffle_pvalue(panel.feature, panel.label, rows, sensitivity_perms, seed),
            "note": "reported only; never selects",
        }
        out["magnitude"] = v1.buyer_excess(panel, rows) if magnitude == "positive_vs_zero" else None
        out["baseline"] = v1.momentum_baseline(panel, rows)
    out["direction"] = direction
    out["p_one_sided"] = p_directed
    return out


#: E0 benchmarks synthetic worlds on a calendar far outside every quarantine window.
E0_CALENDAR_START = date(2101, 1, 3)


def synthetic_decisions(n: int, horizon: int) -> tuple[list[str], list[str]]:
    """Horizon-spaced synthetic decision / label-end instants (business days from 2101-01-03)."""
    days = pd.bdate_range(E0_CALENDAR_START, periods=(n + 1) * horizon + 1)
    decided = v1.decision_instants([days[i * horizon].date() for i in range(n)])
    ends = v1.decision_instants([days[i * horizon + horizon].date() for i in range(n)])
    return [d.isoformat() for d in decided], [d.isoformat() for d in ends]


def e0_measure(feature: np.ndarray, label: np.ndarray, horizon: int, trial: str, *, perms: int,
               direction: int = 1) -> dict:
    """The panel-mode statistic on an E0 synthetic world (``evals.e0.machinery.measure``'s inputs).

    Same seed, min_n and entity floor as E0; sensitivity nulls off. E0 can
    therefore benchmark any panel construct through this path.
    """
    n = feature.shape[0]
    decided, ends = synthetic_decisions(n, horizon)
    tp = TrialPanel(trial=trial, window="discovery", horizon=horizon, decision_at=decided, label_end=ends,
                    entities=[str(j) for j in range(feature.shape[1])], feature=feature, label=label)
    return measure_panel_trial(tp, direction=direction, perms=perms, seed=v1.SEED, min_n=MIN_N,
                               sensitivity=False)


def validate_panel(panel: TrialPanel, run: PanelRunSpec) -> None:
    """VS1's ``validate_panel`` against the run's own windows."""
    lo, hi = run.window_bounds(panel.window)
    if len(panel.decision_at) != len(panel.label_end):
        raise ValueError("decisions and label ends differ in length")
    previous_end = None
    for decided, ended in zip(panel.decision_at, panel.label_end):
        d, e = stamp(decided), stamp(ended)
        if e <= d or d < lo or e >= hi:
            raise ValueError("decision or label outside its window")
        if previous_end is not None and d < previous_end:
            raise ValueError("overlapping outcome windows")
        previous_end = e
    shape = (len(panel.decision_at), len(panel.entities))
    for matrix in (panel.feature, panel.label):
        if matrix.shape != shape or np.isinf(matrix).any():
            raise ValueError("panel matrices must be decisions x entities, finite or NaN")


def entity_half(entity: str, salt: str) -> int:
    """0 = discovery entities, 1 = entity-split holdout entities (salted sha256, pre-registered)."""
    return int(hashlib.sha256(f"{salt}|{entity}".encode("utf-8")).hexdigest(), 16) & 1


def restrict_entities(panel: TrialPanel, salt: str, half: int) -> TrialPanel:
    """The panel with every entity outside ``half`` abstaining (feature and label NaN)."""
    keep = np.array([entity_half(e, salt) == half for e in panel.entities], dtype=bool)

    def mask(matrix):
        if matrix is None:
            return None
        out = np.array(matrix, dtype=float, copy=True)
        out[:, ~keep] = np.nan
        return out

    return TrialPanel(trial=panel.trial, window=panel.window, horizon=panel.horizon,
                      decision_at=list(panel.decision_at), label_end=list(panel.label_end),
                      entities=list(panel.entities), feature=mask(panel.feature), label=mask(panel.label),
                      momentum=mask(panel.momentum), largest=mask(panel.largest))


def panel_digest(panel: TrialPanel) -> str:
    """Content sha256 of a panel: its labels as canonical JSON, its matrices as float64 LE bytes (NaN canonical).

    Same role as VS1's ``digest(panel.as_record())`` (what was measured is pinned in the manifest) at a
    fraction of the cost on E0-sized panels.
    """
    h = hashlib.sha256()
    h.update(canonical({"trial": panel.trial, "window": panel.window, "horizon": panel.horizon,
                        "decision_at": list(panel.decision_at), "label_end": list(panel.label_end),
                        "entities": list(panel.entities)}))
    for matrix in (panel.feature, panel.label):
        values = np.ascontiguousarray(matrix, dtype="<f8").copy()
        values[np.isnan(values)] = np.nan  # one NaN bit pattern
        h.update(str(values.shape).encode("ascii"))
        h.update(values.tobytes())
    return h.hexdigest()


def _trial_id(run: PanelRunSpec, sector: str, trial: str) -> str:
    return digest([run.run_id, sector, trial])


def _check_panels(panels: Mapping[str, Mapping[str, TrialPanel]], wanted: Iterable[tuple[str, str]]) -> None:
    have = {(s, t) for s, by in panels.items() for t in by}
    if have != set(wanted):
        raise ValueError("panels must be exactly the declared (sector, trial) set")


def discover_panel(
    constructs: ConstructSpec | Sequence[ConstructSpec],
    run: PanelRunSpec,
    panels: Mapping[str, Mapping[str, TrialPanel]],
    *,
    inputs: dict,
    guard: OutcomeWindowGuard,
    sensitivity: bool = True,
) -> dict:
    """Freeze the discovery ledger over every declared (sector, trial). Never receives holdout rows."""
    constructs = as_constructs(constructs)
    run.validate()
    require_guard(guard)
    trials = declared_trials(constructs, run)
    _check_panels(panels, trials)
    guarded, ledger, measured, entities = [], [], {}, {}
    for sector, trial in trials:
        construct = construct_of(constructs, trial)
        panel = panels[sector][trial]
        if panel.window != "discovery" or panel.trial != trial or panel.horizon != trial_horizon(trial):
            raise ValueError("discovery received a non-discovery or mislabelled panel")
        if entities.setdefault(sector, list(panel.entities)) != list(panel.entities):
            raise ValueError(f"every {sector} panel must cover the same entities")
        validate_panel(panel, run)
        guarded.append(guard.check_panel(construct, sector, run.benchmark(sector), panel))
        if run.entity_split_salt is not None:
            panel = restrict_entities(panel, run.entity_split_salt, 0)
        measured[(sector, trial)] = panel
        result = measure_panel_trial(panel, direction=construct.direction, perms=run.perms, seed=run.seed,
                                     min_n=run.min_n, sensitivity=sensitivity, magnitude=construct.magnitude)
        h = trial_horizon(trial)
        ledger.append({"trial_id": _trial_id(run, sector, trial), "trial": trial, "sector": sector,
                       "family": family(sector, h), "feature": construct.feature,
                       "family_key": family_key(construct, sector, h),
                       "confirmatory": construct.is_confirmatory(trial), **result})
    pvalues = [t["p"] for t in ledger]
    for entry, holm, bh in zip(ledger, holm_adjusted(pvalues), bh_adjusted(pvalues)):
        entry["holm_adjusted_p"] = holm
        entry["bh_adjusted_p"] = bh
        entry["selected"] = entry["status"] == "tested" and holm <= run.alpha
    payload = {
        "version": VERSION,
        "origin": ORIGIN,
        "prereg_sha256": run.prereg_sha256,
        "constructs": [asdict(c) for c in constructs],
        "constructs_sha256": constructs_digest(constructs),
        "spec": run.as_record(),
        "spec_sha256": run.digest(),
        "windows": {"discovery_start": run.discovery_start, "split": run.split, "end": run.end},
        "selection": f"Holm at ledger run alpha {run.alpha:.6g} (ledger {run.ledger_id}, q={run.ledger_q}, "
                     f"k={run.run_k}) over every declared (sector, trial) incl. untestable; BH-adjusted p reported only",
        "null": "block sign-flip of the per-date rank-IC series; block from discovery IC acf1 "
                f"(autocorrelation_block, >= {MIN_BLOCKS} blocks)",
        "inputs": inputs,
        "guard": guarded,
        "entities": entities,
        "discovery_sha256": digest({f"{s}::{t}": panel_digest(measured[(s, t)]) for s, t in trials}),
        "ledger": ledger,
        "calibration": calibration(ledger),
        "state": "DISCOVERY_FROZEN",
        "promotion_allowed": False,
    }
    return {"payload": payload, "sha256": digest(payload)}


def calibration(ledger: list[dict]) -> dict:
    """Discovery-only check of every trial against its pre-registered direction (VS1's rule, sign-generic)."""
    tested = [t for t in ledger if t["status"] == "tested" and t["mean_ic"] is not None]
    contrary = [f"{t['sector']}::{t['trial']}" for t in tested
                if t["mean_ic"] * t["direction"] < 0 and t["p"] <= 0.05]
    aligned = [t for t in tested if t["confirmatory"] and t["mean_ic"] * t["direction"] > 0]
    consistent = any(t["p_one_sided"] <= 0.10 for t in aligned) or any(
        t["selected"] and t["mean_ic"] * t["direction"] > 0 for t in tested)
    if contrary:
        state = "CONTRARY"
    elif consistent:
        state = "CONSISTENT"
    elif aligned:
        state = "WEAK_ALIGNED"
    else:
        state = "ABSENT"
    return {"state": state, "contrary_trials": contrary,
            "confirmatory_trials": [f"{t['sector']}::{t['trial']}" for t in ledger if t["confirmatory"]]}


def _holdout_measure(panel: TrialPanel, entry: dict, construct: ConstructSpec, run: PanelRunSpec,
                     n_selected: int) -> tuple[dict, float | None, bool]:
    """VS1's holdout step for one panel: frozen block (MIN_BLOCKS cap), Bonferroni, same sign."""
    ic, _ = v1.rank_ic_series(panel.feature, panel.label)
    n = int(np.isfinite(ic).sum())
    block = max(1, min(entry["block"] or 1, max(1, n // MIN_BLOCKS)))
    result = measure_panel_trial(panel, direction=construct.direction, block=block, perms=run.perms,
                                 seed=run.seed, min_n=run.min_n, sensitivity=True, magnitude=construct.magnitude)
    is_selected = entry["selected"]
    adjusted = corrected_p(result["p"], n_selected) if is_selected else None
    survives = bool(is_selected and result["status"] == "tested" and adjusted <= HOLDOUT_ALPHA
                    and result["mean_ic"] * entry["mean_ic"] > 0)
    return result, adjusted, survives


def evaluate_panel_holdout(
    frozen: dict,
    panels: Mapping[str, Mapping[str, TrialPanel]],
    key: "PanelHoldoutKey",
    *,
    guard: OutcomeWindowGuard,
) -> dict:
    """Frozen selections and the confirmatory trials on the holdout, once (plus the entity split)."""
    if not isinstance(key, PanelHoldoutKey) or key.frozen_sha256 != frozen.get("sha256"):
        raise PermissionError("holdout needs the PanelHoldoutKey opened for this frozen discovery")
    require_guard(guard)
    payload = frozen["payload"]
    if digest(payload) != frozen["sha256"]:
        raise PermissionError("frozen discovery manifest changed")
    constructs = tuple(construct_from_record(c) for c in payload["constructs"])
    run = run_from_record(payload["spec"])
    ledger = {(t["sector"], t["trial"]): t for t in payload["ledger"]}
    selected = [k for k, t in ledger.items() if t["selected"]]
    evaluated = [k for k in declared_trials(constructs, run) if k in set(selected) or ledger[k]["confirmatory"]]
    checks, guarded, records = [], [], {}
    salt = run.entity_split_salt
    for sector, trial in evaluated:
        construct = construct_of(constructs, trial)
        panel = (panels.get(sector) or {}).get(trial)
        if panel is None:
            raise ValueError(f"the holdout lacks the panel {sector}::{trial}")
        if panel.window != "holdout" or panel.trial != trial or panel.horizon != trial_horizon(trial):
            raise ValueError("holdout received a non-holdout or mislabelled panel")
        if list(panel.entities) != payload["entities"][sector]:
            raise ValueError(f"the {sector} holdout panel is not on the discovery's entities")
        validate_panel(panel, run)
        guarded.append(guard.check_panel(construct, sector, run.benchmark(sector), panel))
        records[f"{sector}::{trial}"] = panel_digest(panel)
        entry = ledger[(sector, trial)]
        is_selected = entry["selected"]

        result, adjusted, survives = _holdout_measure(panel, entry, construct, run, len(selected))
        check = {
            "trial_id": entry["trial_id"], "trial": trial, "sector": sector,
            "family_key": entry["family_key"], "selected_in_discovery": is_selected,
            "confirmatory": entry["confirmatory"], **result,
            "bonferroni_p": adjusted, "retrospective_survivor": survives,
            "confirmatory_one_sided_p": result["p_one_sided"] if entry["confirmatory"] else None,
        }
        if salt is not None:
            split_result, split_adjusted, split_survives = _holdout_measure(
                restrict_entities(panel, salt, 1), entry, construct, run, len(selected))
            check["entity_split"] = {**split_result, "bonferroni_p": split_adjusted,
                                     "retrospective_survivor": split_survives}
        checks.append(check)
    result = {
        "discovery_manifest": frozen["sha256"],
        "prereg_sha256": payload["prereg_sha256"],
        "holdout_sha256": digest(records),
        "guard": guarded,
        "holdout_checks": checks,
        "promotion_allowed": False,
    }
    result["verdict"] = verdict(payload, checks, entity_split=salt is not None)
    return result


def verdict(payload: dict, checks: list[dict], *, entity_split: bool) -> dict:
    """NO_SURVIVOR, HOLDOUT_SURVIVOR_FORWARD_PENDING or CONTRARY_TO_PREREGISTERED_DIRECTION."""
    calib = payload["calibration"]["state"]
    survivors = [c for c in checks if c["retrospective_survivor"]]
    if entity_split:
        survivors = [c for c in survivors if c["entity_split"]["retrospective_survivor"]]
    aligned = [c for c in survivors if c["mean_ic"] * c["direction"] > 0]
    notes = []
    if calib == "CONTRARY" or any(c["mean_ic"] * c["direction"] < 0 for c in survivors):
        state = "CONTRARY_TO_PREREGISTERED_DIRECTION"
        notes.append("a significant IC against the pre-registered sign: audit the construct, its dates and "
                     "the prices before reading anything else")
    elif aligned:
        state = "HOLDOUT_SURVIVOR_FORWARD_PENDING"
    else:
        state = "NO_SURVIVOR"
    return {"state": state, "calibration": calib, "notes": notes,
            "survivors": [f"{c['sector']}::{c['trial']}" for c in aligned],
            "entity_split_required": entity_split, "promotion_allowed": False,
            "statement": "Nothing here is a trading signal."}


def construct_from_record(record: Mapping[str, Any]) -> ConstructSpec:
    tuples = ("horizons", "confirmatory_horizons", "channels", "artifact_kinds")
    return ConstructSpec(**{k: (tuple(v) if k in tuples else v) for k, v in record.items()})


def run_from_record(record: Mapping[str, Any]) -> PanelRunSpec:
    data = dict(record)
    data["sectors"] = tuple(data["sectors"])
    data["benchmarks"] = tuple(tuple(pair) for pair in data["benchmarks"])
    data["supersedes"] = tuple(data.get("supersedes") or ())
    return PanelRunSpec(**data)


def write_frozen(output: Path, name: str, value: Any) -> None:
    """Write-once artifact (hashed content: discovery / holdout payloads)."""
    Path(output).mkdir(parents=True, exist_ok=True)
    write_once(Path(output) / name, value)


def write_timing(output: Path, timing: Mapping[str, Any]) -> None:
    """Wall-clock facts go to an unhashed ``timing.json`` (never into a hashed payload)."""
    Path(output).mkdir(parents=True, exist_ok=True)
    write_once(Path(output) / "timing.json", dict(timing))


# --- generic custody: registry, off-host witness, keys ------------------------------------------

WITNESS_DIR = "05-GRID/Paper-Log/granular/"
WITNESS_DIR_ALLOWED = frozenset({"README.md", ".gitattributes"})
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = "main"
WITNESS_REF = "refs/granular-witness/main"
_REGISTRY_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
_WITNESS_FILE = re.compile(r"^05-GRID/Paper-Log/granular/([a-z0-9][a-z0-9._-]{2,63})\.anchors\.jsonl$")
FROZEN_INPUT_KEYS: tuple[str, ...] = ("artifacts", "price_manifest_sha256s",
                                      "as_of_ts")
OBSERVED_INPUT_KEYS: tuple[str, ...] = tuple(k for k in FROZEN_INPUT_KEYS if k != "as_of_ts")
_KEY_TOKEN = object()
_HOLDOUT_TOKEN = object()
_WITNESS_TOKEN = object()


def check_registry_id(registry_id: str) -> str:
    if not isinstance(registry_id, str) or not _REGISTRY_ID.fullmatch(registry_id):
        raise ValueError("registry id: 3-64 chars of [a-z0-9._-], starting alphanumeric")
    if "vs1" in registry_id.lower():
        raise PermissionError("a granular registry id may not contain 'vs1' (VS1 v8's census refuses it)")
    return registry_id


def witness_path(registry_id: str) -> str:
    return f"{WITNESS_DIR}{check_registry_id(registry_id)}.anchors.jsonl"


def check_witness_path(path: str) -> str:
    """Refuse any witness path outside ``05-GRID/Paper-Log/granular/`` or naming ``vs1``."""
    norm = str(path).replace("\\", "/")
    if norm.startswith(v1.WITNESS_DIR) or "vs1" in norm.lower():
        raise PermissionError(f"{path}: witness paths under {v1.WITNESS_DIR} or naming vs1 block VS1 v8's census")
    if not _WITNESS_FILE.fullmatch(norm):
        raise PermissionError(f"{path}: granular witnesses live at {WITNESS_DIR}<registry_id>.anchors.jsonl")
    return norm


@dataclass(frozen=True)
class PanelInputs:
    """The hashes ``inputs_frozen`` pins (re-freezable only while no discovery is open).

    ``artifacts``: (artifact sha256, receipt sha256) pairs of the frozen feature
    artifacts. ``price_manifest_sha256s``: (sector, admitted-price manifest
    digest) pairs, one per declared sector (the manifest digest covers its
    probe report and admitted universe).
    """

    artifacts: tuple[tuple[str, str], ...]
    price_manifest_sha256s: tuple[tuple[str, str], ...]
    as_of_ts: str

    def validate(self, run: "PanelRunSpec | None" = None) -> None:
        if not self.artifacts or any(len(a) != 2 or not all(v1._is_hex64(x) for x in a) for a in self.artifacts):
            raise ValueError("artifacts are (artifact sha256, receipt sha256) pairs")
        sectors = [pair[0] for pair in self.price_manifest_sha256s]
        if (not sectors or len(set(sectors)) != len(sectors)
                or any(len(pair) != 2 or not v1._is_hex64(pair[1]) for pair in self.price_manifest_sha256s)):
            raise ValueError("price_manifest_sha256s are distinct (sector, sha256) pairs")
        if run is not None and set(sectors) != set(run.sectors):
            raise ValueError("one admitted-price manifest per declared sector")
        stamp(self.as_of_ts)

    def manifest_sha256(self, sector: str) -> str | None:
        return dict(self.price_manifest_sha256s).get(sector)

    def as_record(self) -> dict:
        return {"artifacts": sorted([list(a) for a in self.artifacts]),
                "price_manifest_sha256s": sorted([list(p) for p in self.price_manifest_sha256s]),
                "as_of_ts": self.as_of_ts}

    def observed(self) -> dict:
        record = self.as_record()
        return {k: record[k] for k in OBSERVED_INPUT_KEYS}


@dataclass(frozen=True)
class PanelRegistry:
    """One granular registry: its directory, id and pinned prereg body hash."""

    log_dir: Path
    registry_id: str
    prereg_sha256: str

    def __post_init__(self) -> None:
        check_registry_id(self.registry_id)
        if not v1._is_hex64(self.prereg_sha256):
            raise ValueError("prereg_sha256 must be a sha256 hex digest")

    @property
    def witness_path(self) -> str:
        return witness_path(self.registry_id)

    def log(self) -> ForwardLog:
        stem = f"gd_panel_mode_{self.registry_id}"
        return ForwardLog(Path(self.log_dir), log_filename=f"{stem}.jsonl",
                          anchor_filename=f"{stem}.anchors.jsonl", lock_filename=f".{stem}.lock",
                          prereg_sha256=self.prereg_sha256)


def _record_sha256(record: dict) -> str:
    return hashlib.sha256(canonical(record)).hexdigest()


def _heads(log: ForwardLog) -> list[str]:
    from analysis.research_forward_log import _lines

    return [hashlib.sha256(line).hexdigest() for line in _lines(log.path)]


def _kind(records: list[dict], kind: str) -> list[dict]:
    return [r for r in records if r.get("kind") == kind]


def _position(records: list[dict], kind: str) -> int:
    positions = [i + 1 for i, r in enumerate(records) if r.get("kind") == kind]
    if len(positions) != 1:
        raise PermissionError(f"the registry holds {len(positions)} {kind} records, not one")
    return positions[0]


def _chain(reg: PanelRegistry) -> list[dict]:
    """Verified records: header + preregistration of exactly this registry id and prereg body."""
    log = reg.log()
    check = log.verify_chain()
    if not check["ok"]:
        raise RuntimeError(f"registry chain is broken: {check['detail']}")
    records = log.read_all()
    if len(records) < 2:
        raise PermissionError("this registry is not registered here")
    header, prereg = records[0], records[1]
    if header.get("registry_id") != reg.registry_id or header.get("version") != VERSION \
            or prereg.get("kind") != "preregistration" or prereg.get("prereg_sha256") != reg.prereg_sha256:
        raise PermissionError("registry does not start with this registry's registration")
    return records


def check_prereg(repo_root: Path, prereg_path: str, prereg_sha256: str) -> str:
    """The prereg body (between the VS1 markers) must still hash to the pinned value."""
    actual = v1.prereg_body_sha256(Path(repo_root) / prereg_path)
    if actual != prereg_sha256:
        raise PermissionError(
            f"pre-registration body hashes to {actual[:12]}, pinned {prereg_sha256[:12]}: a changed body is a "
            "new registration"
        )
    return actual


def registration_records(reg: PanelRegistry, now: datetime, code_sha: str, *,
                         constructs: ConstructSpec | Sequence[ConstructSpec], run: PanelRunSpec,
                         prereg_path: str) -> list[dict]:
    constructs = as_constructs(constructs)
    header = {
        "kind": "header", "version": VERSION, "registry_id": reg.registry_id, "run_at": now.isoformat(),
        "code_sha": code_sha, "prereg_path": prereg_path, "prereg_sha256": reg.prereg_sha256,
        "witness_path": check_witness_path(reg.witness_path), "promotion_allowed": False,
    }
    option = LEDGER_OPTIONS[run.ledger_option]
    trials = []
    for s, t in declared_trials(constructs, run):
        c = construct_of(constructs, t)
        trials.append({"sector": s, "trial": t, "family_key": family_key(c, s, trial_horizon(t)),
                       "direction": c.direction, "confirmatory": c.is_confirmatory(t)})
    record = {
        "kind": "preregistration", "run_at": now.isoformat(), "code_sha": code_sha,
        "prereg_path": prereg_path, "prereg_sha256": reg.prereg_sha256,
        "constructs": [asdict(c) for c in constructs], "constructs_sha256": constructs_digest(constructs),
        "run": run.as_record(), "run_sha256": run.digest(),
        "ledger": {"option": run.ledger_option, "ledger_id": run.ledger_id, "q": run.ledger_q,
                   "k": run.run_k, "alpha": run.alpha, "disclosure": option["disclosure"]},
        "trials": trials,
        "windows": {"discovery_start": run.discovery_start, "split": run.split, "end": run.end},
        "quarantine": {"start": QUARANTINE_START.isoformat(), "end_exclusive": QUARANTINE_END.isoformat()},
        "promotion_allowed": False,
    }
    return [header, record]


def register(reg: PanelRegistry, now: datetime, code_sha: str, *,
             constructs: ConstructSpec | Sequence[ConstructSpec], run: PanelRunSpec, prereg_path: str,
             repo_root: Path = REPO) -> list[dict]:
    """Write the header and ``preregistration`` into an empty registry (no data is touched).

    Refused without the owner's recorded D-GD6-1 ledger decision, on a ledger
    option other than the recorded one, a changed prereg body, a witness path
    under the VS1 directory, a run whose prereg / registry id differ from the
    registry, or a sectors-v6 confirmatory trial.
    """
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    if OWNER_LEDGER_DECISION not in LEDGER_OPTIONS:
        raise PermissionError("D-GD6-1 (separate vs shared ledger) is not recorded: no real registration")
    if run.ledger_option != OWNER_LEDGER_DECISION:
        raise PermissionError(f"the owner chose the {OWNER_LEDGER_DECISION!r} ledger, not {run.ledger_option!r}")
    constructs = as_constructs(constructs)
    run.validate()
    if run.registry_id != reg.registry_id or run.prereg_sha256 != reg.prereg_sha256:
        raise ValueError("the run's registry id / prereg hash differ from the registry's")
    check_witness_path(reg.witness_path)
    check_prereg(repo_root, prereg_path, reg.prereg_sha256)
    for sector, trial in declared_trials(constructs, run):
        OutcomeWindowGuard.check_trial(construct_of(constructs, trial), sector, trial_horizon(trial))
    records = registration_records(reg, now, code_sha, constructs=constructs, run=run, prereg_path=prereg_path)
    log = reg.log()
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        if log.read_all():
            raise ValueError("this registry is already registered in this directory")
        return log.append_locked(records)


def registered(reg: PanelRegistry) -> tuple[tuple[ConstructSpec, ...], PanelRunSpec, str]:
    """(constructs, run, prereg path) as registered."""
    with reg.log().locked():
        records = _chain(reg)
    prereg = records[1]
    return (tuple(construct_from_record(c) for c in prereg["constructs"]), run_from_record(prereg["run"]),
            prereg["prereg_path"])


class GranularWitness:
    """A granular registry's anchor file as committed on the pinned vault ``main`` (from :func:`check_offhost`)."""

    def __init__(self, token: object, *, repo: Path, tip: str, path: str, content: bytes,
                 versions: list[dict], remote_url: str, census: dict) -> None:
        if token is not _WITNESS_TOKEN:
            raise TypeError("a GranularWitness is issued only by check_offhost")
        self.repo, self.tip, self.path, self.content = Path(repo), tip, path, content
        self.versions, self.remote_url, self.census = versions, remote_url, census

    @property
    def lines(self) -> list[bytes]:
        return [line for line in self.content.split(b"\n") if line]

    def descends_from(self, commit: str) -> bool:
        import subprocess

        result = subprocess.run(["git", "-C", str(self.repo), "merge-base", "--is-ancestor", commit, self.tip],
                                capture_output=True, check=False)
        return result.returncode == 0

    def receipt(self) -> dict:
        return {"remote_url": self.remote_url, "branch": WITNESS_BRANCH, "path": self.path, "tip": self.tip,
                "versions": self.versions}


def _walk_history(vault_repo: Path, path: str, *, remote_url: str | None) -> tuple[str, list[dict]]:
    """Fetch pinned ``main`` and walk ``path``'s history: same path, no deletion, strictly append-only."""
    url = WITNESS_REMOTE_URL if remote_url is None else remote_url
    repo = Path(vault_repo)
    v1._git(repo, "rev-parse", "--git-dir")
    v1._git(repo, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", url,
            f"+refs/heads/{WITNESS_BRANCH}:{WITNESS_REF}")
    tip = v1._git(repo, "rev-parse", "--verify", f"{WITNESS_REF}^{{commit}}").strip()
    log = v1._git(repo, "log", "--first-parent", "-m", "--follow", "--name-status", "--format=%x00%H",
                  WITNESS_REF, "--", path)
    entries = []
    for chunk in log.split("\x00")[1:]:
        head, *rest = chunk.strip("\n").split("\n")
        entries.append((head.strip(), [line.split("\t") for line in rest if line.strip()]))
    if not entries:
        raise PermissionError(f"{path} is not on {WITNESS_BRANCH} of {url}")
    versions, previous = [], None
    for commit, changes in reversed(entries):
        for change in changes:
            status, paths = change[0], change[1:]
            if status.startswith(("R", "C")) or any(p != path for p in paths):
                raise PermissionError(f"{commit[:12]}: the witness file came from another path ({paths})")
            if status.startswith("D"):
                raise PermissionError(f"{commit[:12]}: the witness file was deleted (not append-only)")
        content = v1._git(repo, "show", f"{commit}:{path}", binary=True).replace(b"\r\n", b"\n")
        lines = [line for line in content.split(b"\n") if line]
        if not lines:
            raise PermissionError(f"{commit[:12]}: empty witness file")
        if previous is not None and not (len(lines) > len(previous) and lines[: len(previous)] == previous):
            raise PermissionError(f"{commit[:12]}: the witness file is not a strict line-prefix extension "
                                  "of its previous version: not append-only")
        try:
            covered = json.loads(lines[-1]).get("records", 0)
        except ValueError as exc:
            raise PermissionError(f"{commit[:12]}: witness line is not JSON") from exc
        versions.append({"commit": commit, "lines": len(lines), "covered_records": covered})
        previous = lines
    at_tip = v1._git(repo, "show", f"{tip}:{path}", binary=True).replace(b"\r\n", b"\n")
    if [line for line in at_tip.split(b"\n") if line] != previous:
        raise PermissionError("the witness file at the fetched tip is not its last walked version")
    return tip, versions


def granular_census(repo: Path, tip: str) -> dict:
    """Every granular witness at ``tip`` and the records its last anchor covers; unknown files listed."""
    files, unknown = {}, []
    for path in v1._git(repo, "ls-tree", "-r", "--name-only", tip, "--", WITNESS_DIR.rstrip("/")).splitlines():
        path = path.strip()
        if not path:
            continue
        match = _WITNESS_FILE.fullmatch(path)
        if match and "vs1" not in path.lower():
            files[match.group(1)] = path
        elif path[len(WITNESS_DIR):] not in WITNESS_DIR_ALLOWED:
            unknown.append(path)
    records = {}
    for rid, path in files.items():
        content = v1._git(repo, "show", f"{tip}:{path}", binary=True).replace(b"\r\n", b"\n")
        lines = [line for line in content.split(b"\n") if line]
        try:
            value = json.loads(lines[-1])["records"] if lines else None
        except (ValueError, KeyError, TypeError):
            value = None
        records[rid] = value if isinstance(value, int) else None
    return {"tip": tip, "files": dict(sorted(files.items())), "records": dict(sorted(records.items())),
            "unknown": sorted(unknown)}


def check_offhost(vault_repo: Path, registry_id: str, *, remote_url: str | None = None) -> GranularWitness:
    """Fetch the pinned vault ``main``; the registry's witness must be append-only at its one path."""
    path = check_witness_path(witness_path(registry_id))
    tip, versions = _walk_history(vault_repo, path, remote_url=remote_url)
    content = v1._git(Path(vault_repo), "show", f"{tip}:{path}", binary=True).replace(b"\r\n", b"\n")
    lines = [line for line in content.split(b"\n") if line]
    census = granular_census(Path(vault_repo), tip)
    return GranularWitness(_WITNESS_TOKEN, repo=Path(vault_repo), tip=tip, path=path,
                           content=b"\n".join(lines) + b"\n", versions=versions,
                           remote_url=WITNESS_REMOTE_URL if remote_url is None else remote_url, census=census)


def check_census(census: Mapping[str, Any], run: PanelRunSpec) -> None:
    """No unknown granular witness; every superseded registry still at its 2-record registration."""
    if census.get("unknown"):
        raise PermissionError(f"unknown files in {WITNESS_DIR}: {census['unknown'][:5]}; refused until accounted for")
    counts = census.get("records") or {}
    for rid in run.supersedes:
        if counts.get(rid) != 2:
            raise PermissionError(f"superseded registry {rid} is not exactly its witnessed 2-record "
                                  f"registration ({counts.get(rid)} records): an older registry grew")


def require_witness(reg: PanelRegistry, witness: GranularWitness | None, records_needed: int) -> dict:
    """The off-host log must witness this exact chain (no fork) up to ``records_needed`` records."""
    if not isinstance(witness, GranularWitness):
        raise PermissionError("the pinned off-host anchor log (check_offhost) is required")
    if witness.path != reg.witness_path:
        raise PermissionError("the witness belongs to another registry")
    log = reg.log()
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / Path(witness.path).name
        path.write_bytes(witness.content)
        with log.locked():
            records = _chain(reg)
            check = log.verify_chain(external_anchors=path)
    if not check["ok"]:
        raise PermissionError(f"off-host anchor log does not witness this registry (fork?): {check['detail']}")
    first = json.loads(witness.lines[0])
    if first.get("records") != 2 or first.get("prev_anchor_sha256") is not None:
        raise PermissionError("the off-host log does not start with the 2-record registration anchor")
    covered = json.loads(witness.lines[-1])["records"]
    if covered < records_needed:
        raise PermissionError(f"off-host anchor log covers {covered} records; commit and push the anchors up to "
                              f"{records_needed} records to {WITNESS_BRANCH}:{witness.path} first")
    for record in _kind(records, "prices_read"):
        if not witness.descends_from(record["witness_tip"]):
            raise PermissionError(f"the pinned {WITNESS_BRANCH} no longer contains witness commit "
                                  f"{record['witness_tip'][:12]} an earlier price read saw (history rewritten)")
    return {"records": len(records), "witnessed_records": covered, "tip": witness.tip}


def export_anchors(reg: PanelRegistry, vault_worktree: Path) -> list[str]:
    """Append to ``<vault_worktree>/<witness path>`` the anchor lines it lacks (operator commits + pushes)."""
    from analysis.research_forward_log import _lines

    log = reg.log()
    with log.locked():
        _chain(reg)
        local = list(_lines(log.anchor_path))
    path = Path(vault_worktree) / check_witness_path(reg.witness_path)
    existing = [line.rstrip(b"\r") for line in _lines(path)] if path.exists() else []
    if existing != local[: len(existing)]:
        raise PermissionError("the off-host anchor log witnesses another registry chain; not appending")
    new = local[len(existing):]
    if new:
        path.parent.mkdir(parents=True, exist_ok=True)
        tail = path.read_bytes() if path.exists() else b""
        with open(path, "ab") as stream:
            if tail and not tail.endswith(b"\n"):
                stream.write(b"\n")
            for line in new:
                stream.write(line + b"\n")
    return [line.decode("utf-8") for line in new]


class PanelDiscoveryKey:
    """``discovery_opened`` is in this registry's chain and witnessed off-host (from :func:`resume_discovery`).

    As in VS1, the issuing token is a module-private object: forging a key is
    prevented by convention and review (any caller could import the token), not
    by the type system. The registry chain and the off-host witness are the
    actual record of what was opened.
    """

    window = "discovery"

    def __init__(self, token: object, *, registry: PanelRegistry, inputs_frozen_sha256: str, inputs: dict,
                 witness_tip: str) -> None:
        if token is not _KEY_TOKEN:
            raise TypeError("a PanelDiscoveryKey is issued only by resume_discovery")
        self.registry, self.inputs_frozen_sha256 = registry, inputs_frozen_sha256
        self.inputs, self.witness_tip = dict(inputs), witness_tip
        self.as_of_ts = stamp(inputs["as_of_ts"])


class PanelHoldoutKey:
    """``holdout_opened`` (flag + pinned hash) is in the chain and witnessed (from :func:`resume_holdout`)."""

    window = "holdout"

    def __init__(self, token: object, *, registry: PanelRegistry, frozen_sha256: str, inputs: dict,
                 witness_tip: str) -> None:
        if token is not _HOLDOUT_TOKEN:
            raise TypeError("a PanelHoldoutKey is issued only by resume_holdout")
        self.registry, self.frozen_sha256 = registry, frozen_sha256
        self.inputs, self.witness_tip = dict(inputs), witness_tip
        self.as_of_ts = stamp(inputs["as_of_ts"])


def _check_observed(frozen_inputs: Mapping[str, Any], observed: PanelInputs) -> None:
    if not isinstance(observed, PanelInputs):
        raise TypeError("observed inputs are a PanelInputs")
    got = observed.observed()
    differ = sorted(k for k in OBSERVED_INPUT_KEYS if frozen_inputs.get(k) != got[k])
    if differ:
        raise PermissionError(f"inputs differ from the inputs_frozen record: {differ}")


def freeze_inputs(reg: PanelRegistry, now: datetime, inputs: PanelInputs) -> dict:
    """Append ``inputs_frozen`` before any price read; refused once a discovery was opened."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    _, run, _ = registered(reg)
    inputs.validate(run)
    if stamp(inputs.as_of_ts) > now:
        raise ValueError("as_of_ts cannot be later than the freeze")
    log = reg.log()
    with log.locked():
        records = _chain(reg)
        if _kind(records, "discovery_opened"):
            raise PermissionError("a discovery was already opened: its inputs cannot be re-frozen")
        previous = _kind(records, "inputs_frozen")
        return log.append_locked([{
            "kind": "inputs_frozen", "run_at": now.isoformat(), "prereg_sha256": reg.prereg_sha256,
            "inputs": inputs.as_record(), "supersedes": _record_sha256(previous[-1]) if previous else None,
            "promotion_allowed": False,
        }])[0]


def open_discovery(reg: PanelRegistry, now: datetime, observed: PanelInputs, *, repo_root: Path = REPO) -> dict:
    """One-shot discovery, step 1: append ``discovery_opened`` (no price read)."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    _, _, prereg_path = registered(reg)
    check_prereg(repo_root, prereg_path, reg.prereg_sha256)
    log = reg.log()
    with log.locked():
        records = _chain(reg)
        if _kind(records, "discovery_opened") or _kind(records, "discovery_frozen"):
            raise PermissionError("discovery already ran under this registry (one shot)")
        frozen_inputs = _kind(records, "inputs_frozen")
        if not frozen_inputs:
            raise PermissionError("no inputs_frozen record: freeze inputs before any price read")
        current = frozen_inputs[-1]
        _check_observed(current["inputs"], observed)
        log.append_locked([{
            "kind": "discovery_opened", "run_at": now.isoformat(), "prereg_sha256": reg.prereg_sha256,
            "inputs_frozen_sha256": _record_sha256(current), "promotion_allowed": False,
        }])
        heads = _heads(log)
    return {"kind": "discovery_opened", "records": len(heads), "head_sha256": heads[-1]}


def resume_discovery(reg: PanelRegistry, observed: PanelInputs, witness: GranularWitness | None, *,
                     repo_root: Path = REPO) -> PanelDiscoveryKey:
    """One-shot discovery, step 2: the price key, only once the off-host log witnesses the opening."""
    _, run, prereg_path = registered(reg)
    check_prereg(repo_root, prereg_path, reg.prereg_sha256)
    with reg.log().locked():
        records = _chain(reg)
    if _kind(records, "discovery_frozen"):
        raise PermissionError("a discovery is already frozen (one shot)")
    position = _position(records, "discovery_opened")
    opened = records[position - 1]
    matching = [r for r in _kind(records, "inputs_frozen") if _record_sha256(r) == opened["inputs_frozen_sha256"]]
    if len(matching) != 1:
        raise PermissionError("discovery_opened does not name an inputs_frozen record of this chain")
    _check_observed(matching[0]["inputs"], observed)
    if isinstance(witness, GranularWitness):
        check_census(witness.census, run)
    require_witness(reg, witness, position)
    return PanelDiscoveryKey(_KEY_TOKEN, registry=reg, inputs_frozen_sha256=opened["inputs_frozen_sha256"],
                             inputs=matching[0]["inputs"], witness_tip=witness.tip)


def record_prices_read(grant: PanelDiscoveryKey | PanelHoldoutKey, price_receipt_sha256: str,
                       manifest_sha256: str, now: datetime | None = None) -> dict:
    """Append ``prices_read`` for one sector's manifest; a resumed re-read must reproduce the first receipt."""
    if not isinstance(grant, (PanelDiscoveryKey, PanelHoldoutKey)):
        raise PermissionError("recording a price read needs its grant")
    if manifest_sha256 not in {sha for _, sha in grant.inputs.get("price_manifest_sha256s", [])}:
        raise PermissionError("the price manifest is not one inputs_frozen pinned")
    reg = grant.registry
    log = reg.log()
    with log.locked():
        records = _chain(reg)
        earlier = [r for r in _kind(records, "prices_read")
                   if r["window"] == grant.window and r["manifest_sha256"] == manifest_sha256]
        if any(r["price_receipt_sha256"] != price_receipt_sha256 for r in earlier):
            raise PermissionError(f"{grant.window} prices differ from the first read of this manifest under this "
                                  "registry: refused")
        return log.append_locked([{
            "kind": "prices_read", "run_at": (now or datetime.now(timezone.utc)).isoformat(),
            "prereg_sha256": reg.prereg_sha256, "window": grant.window, "manifest_sha256": manifest_sha256,
            "price_receipt_sha256": price_receipt_sha256, "witness_tip": grant.witness_tip,
            "promotion_allowed": False,
        }])[0]


def discovery_inputs(grant: PanelDiscoveryKey) -> dict:
    """The ``inputs`` a sealable discovery manifest carries: the grant's inputs_frozen record and hash."""
    if not isinstance(grant, PanelDiscoveryKey):
        raise PermissionError("discovery inputs come from a PanelDiscoveryKey")
    return {"inputs_frozen_sha256": grant.inputs_frozen_sha256, **grant.inputs}


def universe(manifest: Any) -> list[str]:
    """A sector manifest's issuer universe: admitted tickers minus its benchmark and calendar tickers."""
    return sorted(set(manifest.admitted) - {manifest.benchmark, manifest.calendar})


def seal_discovery(grant: PanelDiscoveryKey, now: datetime, frozen: dict, manifests: Mapping[str, Any]) -> dict:
    """Append ``discovery_frozen`` with the frozen manifest's sha256 -- only for the registered design.

    The discovery must have run exactly the registered constructs and run spec
    (same trials, directions, seed, perms, ledger k), on exactly this grant's
    frozen inputs, and every sector's panels on that sector's full admitted
    universe (``manifests``: sector -> the frozen admitted-price manifest).
    """
    if not isinstance(grant, PanelDiscoveryKey):
        raise PermissionError("sealing a discovery needs its PanelDiscoveryKey")
    payload = frozen.get("payload") or {}
    if digest(payload) != frozen.get("sha256"):
        raise ValueError("frozen discovery manifest does not hash to its sha256")
    if payload.get("inputs") != discovery_inputs(grant):
        raise PermissionError("the discovery manifest does not carry this grant's inputs_frozen record")
    reg = grant.registry
    if payload.get("prereg_sha256") != reg.prereg_sha256 or payload.get("spec", {}).get("registry_id") != reg.registry_id:
        raise PermissionError("the discovery manifest belongs to another registry")
    with reg.log().locked():
        prereg = _chain(reg)[1]
    try:
        constructs = tuple(construct_from_record(c) for c in payload["constructs"])
        recomputed = (constructs_digest(constructs), run_from_record(payload["spec"]).digest())
    except (KeyError, TypeError, ValueError) as exc:
        raise PermissionError("the discovery manifest's constructs / spec are malformed") from exc
    if {payload.get("constructs_sha256"), recomputed[0]} != {prereg["constructs_sha256"]} \
            or {payload.get("spec_sha256"), recomputed[1]} != {prereg["run_sha256"]}:
        raise PermissionError("the discovery did not run the registered constructs and run spec")
    if any(t.get("direction") != construct_of(constructs, t["trial"]).direction for t in payload.get("ledger", [])):
        raise PermissionError("a ledger entry's direction differs from its registered construct")
    pinned = dict(grant.inputs["price_manifest_sha256s"])
    if set(manifests) != set(pinned) or any(manifests[sec].digest() != pinned[sec] for sec in pinned):
        raise PermissionError("the price manifests are not the ones inputs_frozen pinned")
    entities = payload.get("entities") or {}
    if set(entities) != set(pinned) or any(entities[sec] != universe(manifests[sec]) for sec in pinned):
        raise PermissionError("a sector's panels are not its full frozen admitted universe")
    log = reg.log()
    with log.locked():
        records = _chain(reg)
        opened = _kind(records, "discovery_opened")
        if len(opened) != 1 or opened[0]["inputs_frozen_sha256"] != grant.inputs_frozen_sha256:
            raise PermissionError("no discovery_opened record for this grant")
        if _kind(records, "discovery_frozen"):
            raise PermissionError("a discovery is already frozen (one shot)")
        return log.append_locked([{
            "kind": "discovery_frozen", "run_at": now.isoformat(), "prereg_sha256": reg.prereg_sha256,
            "inputs_frozen_sha256": grant.inputs_frozen_sha256, "discovery_sha256": frozen["sha256"],
            "calibration": payload["calibration"]["state"],
            "selected": [f"{t['sector']}::{t['trial']}" for t in payload["ledger"] if t["selected"]],
            "promotion_allowed": False,
        }])[0]


def _holdout_request(reg: PanelRegistry, frozen: dict, *, allow_holdout: bool, prereg_sha256: str,
                     repo_root: Path) -> tuple[dict, dict]:
    if allow_holdout is not True:
        raise PermissionError("holdout evaluation needs an explicit allow_holdout=True")
    if prereg_sha256 != reg.prereg_sha256:
        raise PermissionError("the pre-registration hash given does not match the registry's")
    _, _, prereg_path = registered(reg)
    check_prereg(repo_root, prereg_path, reg.prereg_sha256)
    payload = frozen.get("payload") or {}
    if digest(payload) != frozen.get("sha256"):
        raise PermissionError("frozen discovery manifest changed")
    if payload.get("prereg_sha256") != reg.prereg_sha256 or payload.get("state") != "DISCOVERY_FROZEN":
        raise PermissionError("no frozen discovery of this registry to evaluate")
    with reg.log().locked():
        records = _chain(reg)
    sealed = _kind(records, "discovery_frozen")
    if len(sealed) != 1 or sealed[0]["discovery_sha256"] != frozen["sha256"]:
        raise PermissionError("the frozen discovery file is not the one the registry chain froze")
    matching = [r for r in _kind(records, "inputs_frozen") if _record_sha256(r) == sealed[0]["inputs_frozen_sha256"]]
    if len(matching) != 1 or (payload.get("inputs") or {}).get("inputs_frozen_sha256") != sealed[0]["inputs_frozen_sha256"]:
        raise PermissionError("the discovery's inputs_frozen record is not in the chain")
    return payload, matching[0]["inputs"]


def open_holdout(reg: PanelRegistry, frozen: dict, *, allow_holdout: bool, prereg_sha256: str, now: datetime,
                 observed: PanelInputs, repo_root: Path = REPO) -> dict:
    """One-shot holdout, step 1: explicit flag + pinned hash + the chain, then ``holdout_opened``."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    _, inputs = _holdout_request(reg, frozen, allow_holdout=allow_holdout, prereg_sha256=prereg_sha256,
                                 repo_root=repo_root)
    _check_observed(inputs, observed)
    log = reg.log()
    with log.locked():
        records = _chain(reg)
        if _kind(records, "holdout_opened"):
            raise PermissionError("the holdout was already opened (evaluated once)")
        log.append_locked([{
            "kind": "holdout_opened", "run_at": now.isoformat(), "prereg_sha256": reg.prereg_sha256,
            "discovery_sha256": frozen["sha256"], "promotion_allowed": False,
        }])
        heads = _heads(log)
    return {"kind": "holdout_opened", "records": len(heads), "head_sha256": heads[-1]}


def resume_holdout(reg: PanelRegistry, frozen: dict, *, allow_holdout: bool, prereg_sha256: str,
                   observed: PanelInputs, witness: GranularWitness | None,
                   repo_root: Path = REPO) -> PanelHoldoutKey:
    """One-shot holdout, step 2: the price key, only once the off-host log witnesses the opening."""
    _, inputs = _holdout_request(reg, frozen, allow_holdout=allow_holdout, prereg_sha256=prereg_sha256,
                                 repo_root=repo_root)
    with reg.log().locked():
        records = _chain(reg)
    if _kind(records, "holdout_result"):
        raise PermissionError("a holdout result is already recorded (evaluated once)")
    position = _position(records, "holdout_opened")
    if records[position - 1]["discovery_sha256"] != frozen["sha256"]:
        raise PermissionError("holdout_opened names another discovery")
    _check_observed(inputs, observed)
    _, run, _ = registered(reg)
    if isinstance(witness, GranularWitness):
        check_census(witness.census, run)
    require_witness(reg, witness, position)
    return PanelHoldoutKey(_HOLDOUT_TOKEN, registry=reg, frozen_sha256=frozen["sha256"], inputs=inputs,
                           witness_tip=witness.tip)


def seal_holdout(grant: PanelHoldoutKey, now: datetime, result: dict) -> dict:
    """Append ``holdout_result`` (the result's sha256 and verdict state)."""
    if not isinstance(grant, PanelHoldoutKey) or result.get("discovery_manifest") != grant.frozen_sha256:
        raise PermissionError("the holdout result does not belong to this grant")
    reg = grant.registry
    log = reg.log()
    with log.locked():
        records = _chain(reg)
        if not any(r.get("discovery_sha256") == grant.frozen_sha256 for r in _kind(records, "holdout_opened")):
            raise PermissionError("no holdout_opened record for this discovery")
        if _kind(records, "holdout_result"):
            raise PermissionError("a holdout result is already recorded")
        return log.append_locked([{
            "kind": "holdout_result", "run_at": now.isoformat(), "prereg_sha256": reg.prereg_sha256,
            "discovery_sha256": grant.frozen_sha256, "result_sha256": digest(result),
            "verdict": result["verdict"]["state"], "promotion_allowed": False,
        }])[0]


def verify(reg: PanelRegistry, witness: GranularWitness | None = None) -> dict:
    """Read-only summary: chain ok, record kinds, and (with a witness) how far it is witnessed."""
    with reg.log().locked():
        records = _chain(reg)
    out = {"registry_id": reg.registry_id, "records": len(records),
           "kinds": [r["kind"] for r in records], "promotion_allowed": False}
    if witness is not None:
        out["witness"] = require_witness(reg, witness, 2)
    return out


__all__ = [
    "ConstructSpec", "PanelRunSpec", "PanelInputs", "PanelRegistry", "OutcomeWindowGuard",
    "V8TerminalWitness", "SectorsV6Witness", "GranularWitness", "PanelDiscoveryKey", "PanelHoldoutKey",
    "measure_panel_trial", "e0_measure", "discover_panel", "evaluate_panel_holdout", "register",
    "freeze_inputs", "open_discovery", "resume_discovery", "seal_discovery", "open_holdout",
    "resume_holdout", "seal_holdout", "record_prices_read", "check_offhost", "verify", "family_key",
]
