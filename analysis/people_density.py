"""People-density features over ``people_events`` (GD5 of the granular-discovery plan).

What this is
------------
One deterministic, point-in-time module that turns canonical people events
(``store.people_events``) into the density constructs of
``GRID-GRANULAR-DISCOVERY-PLAN-20260927`` section 2.2:

* ``A(e, t)`` -- :func:`density_A`: recency-weighted count of **distinct
  actors** with a qualifying event on entity ``e`` whose ``known_at`` lies in
  ``(t - W, t]``. Each actor counts once, at the weight of its latest such
  event: ``exp(-(t - known_at) / tau)``.
* ``C(e, t)`` -- :func:`channel_count_C`: distinct channels with at least one
  qualifying event in the window.
* ``S(e, t)`` -- :func:`signed_S`: ``A`` over positive events minus ``A``
  over negative events (an actor counts once per sign). Planned (10b5-1)
  Form 4 sales are excluded; a Form 4 sale with no 10b5-1 determination makes
  ``S`` undefined (:class:`UndefinedFeature`), never silently counted or dropped.
* ``D_self`` -- :func:`d_self`: ``(A - median) / (MAD + eps)`` over the
  trailing 52 weekly observations (decision <= t); NaN with fewer than 52.
* ``D_peer`` -- :func:`d_peer`: average-rank percentile of ``A`` among the
  entity's sector peers, with membership as of ``t``.
* :func:`coverage_mask` / :func:`coverage_change_log`: an entity-date is
  scored only if every counted channel has covered the entity for at least
  ``W`` days; every coverage start and stop is logged so a coverage jump never
  reads as a density jump.
* :func:`sector_weekly_aggregates`: Friday 16:00 America/New_York sector sums,
  written by :func:`write_frozen_artifact` as a hashed parquet + receipt.

It computes **features only**. Nothing here reads a price, a return, a label
or any table other than ``people_events`` (and, for ticker-only rows, the
``security_identifiers`` resolver). ``event_time`` is carried for provenance
and never used by a feature.

Point in time
-------------
Every feature at decision ``t`` reads only events with ``known_at <= t``.
``known_at`` is consumed **as stored**; this module assumes no convention.
Two exist in the repo (``panel_insider_density.filing_known_at``: filing date
at 22:00 New York; the legacy ``people_events_materializer``: next session
14:30 UTC) and both put a filing first into the next session's 16:00
decision. The people-events design (GRID-PEOPLE-EVENTS-PIPELINE-DESIGN-20261001
section 3) picks the first for Form 4 (S16 = VS1's ``filing_known_at``) and
next-session-open for other date-only disclosures. :func:`load_events` is the
only database read and is a thin wrapper over
``store.people_events.read_events`` (``known_at <= as_of``, echoes excluded;
under the v2 store also versions superseded or retracted by ``as_of``).

The materializer contract (``EventContract``)
---------------------------------------------
Every assumption about how the materializer encodes a channel is isolated in
:class:`EventContract` (``PE_CONTRACT``) and recorded in every receipt, so a
materializer change is a new contract version here, not a ripple through the
feature code. Version ``pe-design-20261001`` follows
GRID-PEOPLE-EVENTS-PIPELINE-DESIGN-20261001 sections 2-4 (PRs #779/#780):

* C1 actor key: ``actor_id`` for every channel -- owner CIK (10-digit) or
  normalized name for Form 4, bioguide or name for congress, filer CIK for
  13F, awarding agency for contracts, registrant for lobbying -- prefixed
  with ``actor_id_basis`` so two id spaces never merge by accident. Known
  limit: an insider keyed by CIK in SEC history and by name in live
  QuiverQuant rows counts as two actors if both fall in one window.
* C2 10b5-1: read from ``provenance["attrs"]["is_10b5_1"]`` (the v2 writer's
  attrs), else a top-level ``is_10b5_1``. Absent means *undetermined*
  (None), never False.
* C3 13F "new or increased" = ``transaction_code`` NEW or INC (the design's
  position-change codes); congress buy/sell and contract awards follow
  ``direction``.
* C4 Form 4 codes match on their first character (P purchase, S sale).
* C5 entities key on security_master ``entity_id``: a TEXT ``security_id``
  (v2, resolved by the pipeline's PIT policy) wins; then ``entity_cik`` ->
  ``entity_id_for_cik``; then ticker-only rows through a caller-supplied
  resolver (``resolve_entity("ticker", ..., as_of=known_at date)`` on the DB
  path). Unresolved rows are dropped and counted; a v1 BIGINT
  ``security_id`` is ignored. Channels whose "ticker" is a sector proxy
  (``fara``) never resolve to an entity.
* C6 ``co_actor_ids`` never add actors (v1 rule).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy.stats import rankdata

from sqlalchemy import text

from intelligence.security_master import entity_id_for_cik
from store.people_events import CHANNELS, KNOWN_AT_BASES, PeopleEvent, read_events

NEW_YORK = ZoneInfo("America/New_York")
DECISION_HOUR_NY = 16  # decisions are 16:00 America/New_York
DAY_NS = 86_400e9  # float, exactly as analysis.panel_insider_density.density uses it

D_SELF_WEEKS = 52
#: MAD floor for D_self. 0.1 is the weight of one actor whose latest event is
#: ln(10) * tau old: a "one stale actor" scale, so an entity whose trailing
#: year is flat (MAD = 0) reads one fresh actor as ~10, not as infinity.
D_SELF_EPSILON = 0.1
#: D_peer is NaN when fewer scored sector peers than this exist at t.
MIN_PEER_COUNT = 5
#: Sector aggregate threshold ("count of entities with D_peer > 0.9").
D_PEER_HIGH = 0.9
#: known_at bases a declared spec admits, spelled out (not the store's
#: vocabulary) so a vocabulary change never re-hashes a frozen spec.
#: ``statutory_bound`` is never a valid bound (PE design F2: late PTRs) and
#: ``qq_last_modified`` is out under GD-INDEX R6 (Senate QQ last_modified);
#: a spec that wants either is a new name.
DECLARED_KNOWN_AT_BASES = ("filing", "first_seen", "publish")

POSITIVE_DIRECTIONS = frozenset({"buy", "award", "positive"})
NEGATIVE_DIRECTIONS = frozenset({"sell", "negative"})

EVENT_COLUMNS = (
    "channel", "dedup_key", "event_time", "known_at", "known_at_basis",
    "actor_id", "actor_id_basis", "actor_type", "entity_id", "entity_cik",
    "entity_ticker", "direction", "transaction_code", "size_usd", "plan_10b5_1",
    "visible_until",
)

AGGREGATE_COLUMNS = (
    "decision_at", "sector", "spec", "sum_A", "n_dpeer_gt_0p9", "n_covered", "n_undefined", "n_members",
)


class UndefinedFeature(ValueError):
    """The feature is not defined on these inputs (e.g. S with undetermined Form 4 sells)."""


class ImpossibleEvent(ValueError):
    """An event the declared coverage says cannot exist (e.g. first_seen before live start)."""


# --- materializer contract --------------------------------------------------------------


@dataclass(frozen=True)
class EventContract:
    """How the people-events materializer encodes a channel (see the module docstring)."""

    version: str = "pe-design-20261001"
    plan_flag_paths: tuple[tuple[str, ...], ...] = (("attrs", "is_10b5_1"), ("is_10b5_1",))
    non_entity_channels: tuple[str, ...] = ("fara",)
    true_tokens: tuple[str, ...] = ("1", "TRUE", "T", "Y", "YES")
    false_tokens: tuple[str, ...] = ("0", "FALSE", "F", "N", "NO")

    def plan_flag(self, provenance: Mapping[str, Any] | None) -> bool | None:
        """True / False when provenance carries a 10b5-1 determination, else None."""
        if not isinstance(provenance, Mapping):
            return None
        for path in self.plan_flag_paths:
            node: Any = provenance
            for key in path:
                node = node.get(key) if isinstance(node, Mapping) else None
            if node is not None:
                return self._coerce(node)
        return None

    def _coerce(self, value: Any) -> bool | None:
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        token = str(value).strip().upper()
        if token in self.true_tokens:
            return True
        if token in self.false_tokens:
            return False
        return None


PE_CONTRACT = EventContract()


# --- specs ------------------------------------------------------------------------------


@dataclass(frozen=True)
class ChannelRule:
    """Which events of one channel count, and who the actor is."""

    channel: str
    #: One-character codes match the code's first character (Form 4: P, S);
    #: longer ones match the whole code (13F: NEW, INC, DEC, EXIT).
    transaction_codes: tuple[str, ...] | None = None
    directions: tuple[str, ...] | None = None
    exclude_planned: bool = False  # drop rows whose 10b5-1 determination is True
    actor_field: str = "actor_id"

    def __post_init__(self) -> None:
        if self.channel not in CHANNELS:
            raise ValueError(f"unknown channel {self.channel!r}")


@dataclass(frozen=True)
class DensitySpec:
    """A frozen density construct. Changing one is a new name, never an edit."""

    name: str
    rules: tuple[ChannelRule, ...]
    window_days: int
    tau_days: float
    signed: bool = False
    allowed_known_at_bases: tuple[str, ...] = ("filing", "first_seen", "publish")

    def __post_init__(self) -> None:
        if self.window_days <= 0 or self.tau_days <= 0:
            raise ValueError("window_days and tau_days must be positive")
        unknown = set(self.allowed_known_at_bases) - set(KNOWN_AT_BASES)
        if unknown:
            raise ValueError(f"unknown known_at bases {sorted(unknown)}")
        channels = [r.channel for r in self.rules]
        if not channels or len(set(channels)) != len(channels):
            raise ValueError("a spec needs at least one rule and one rule per channel")

    @property
    def channels(self) -> tuple[str, ...]:
        return tuple(r.channel for r in self.rules)

    @property
    def sha256(self) -> str:
        return _canonical_sha256(asdict(self))


_ACTOR_CHANNELS = ("form4", "congress", "thirteen_f", "gov_contract", "lobbying")


def _declare() -> dict[str, DensitySpec]:
    insider_buy = ChannelRule("form4", transaction_codes=("P",), exclude_planned=True)
    congress_buy = ChannelRule("congress", directions=("buy",))
    families: dict[str, tuple[tuple[ChannelRule, ...], bool]] = {
        "A_insider_buy": ((insider_buy,), False),
        "A_congress": ((ChannelRule("congress", directions=("buy", "sell")),), False),
        "A_congress_buy": ((congress_buy,), False),
        "A_inst": ((ChannelRule("thirteen_f", transaction_codes=("NEW", "INC")),), False),
        "A_contract": ((ChannelRule("gov_contract"),), False),
        "A_lobby": ((ChannelRule("lobbying"),), False),
        "A_multi_mc1": ((insider_buy, congress_buy), False),
        "C_people": (tuple(ChannelRule(c) for c in _ACTOR_CHANNELS), False),
        "S_insider": ((ChannelRule("form4", transaction_codes=("P", "S"), exclude_planned=True),), True),
        "S_congress": ((ChannelRule("congress", directions=("buy", "sell")),), True),
    }
    specs = {}
    for base, (rules, signed) in families.items():
        for window in (30, 90):
            name = f"{base}_w{window}"
            specs[name] = DensitySpec(name, rules, window, window / 2.0, signed=signed,
                                      allowed_known_at_bases=DECLARED_KNOWN_AT_BASES)
    return specs


#: The frozen GD5 constructs (tau = W / 2, W in {30, 90}). Every scan that
#: uses them is pre-registered in its own slice (GD6-GD9).
DECLARED_SPECS: dict[str, DensitySpec] = _declare()


# --- events -----------------------------------------------------------------------------


_EXTRA_COLUMNS = ("echo_of", "security_id", "superseded_at", "retracted_at")


def _to_utc(values: pd.Series, name: str) -> pd.Series:
    """tz-aware UTC datetime64[ns]; a naive timestamp is refused, never assumed UTC."""
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        return values.dt.tz_convert("UTC").dt.as_unit("ns")
    if pd.api.types.is_datetime64_dtype(values.dtype):
        raise ValueError(f"{name} must be tz-aware; got naive {values.dtype}")
    out = []
    for v in values:
        if v is None or (isinstance(v, float) and np.isnan(v)) or v is pd.NaT:
            out.append(pd.NaT)
            continue
        ts = pd.Timestamp(v)
        if ts.tzinfo is None:
            raise ValueError(f"{name} must be tz-aware; got naive {v!r}")
        out.append(ts.tz_convert("UTC"))
    return pd.Series(pd.DatetimeIndex(out, tz="UTC").as_unit("ns"), index=values.index)


def _is_missing(v: Any) -> bool:
    return v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and np.isnan(v))


def _map_unique(values: pd.Series, fn: Callable[[Any], Any]) -> pd.Series:
    """``fn`` applied once per distinct value (missing values map to None)."""
    codes, uniques = pd.factorize(values, use_na_sentinel=True)
    mapped = np.array([fn(u) for u in uniques] + [None], dtype=object)
    return pd.Series(mapped[codes], index=values.index, dtype=object)


def _clean_text(values: pd.Series, upper: bool = False) -> pd.Series:
    """Stripped strings (optionally upper-cased) as object dtype; missing or blank -> None."""
    text = values.astype("string").str.strip()
    if upper:
        text = text.str.upper()
    missing = (text.isna() | (text == "")).to_numpy(dtype=bool)
    out = text.to_numpy(dtype=object, na_value=None)
    out[missing] = None
    return pd.Series(out, index=values.index, dtype=object)


def events_frame(
    events: Iterable[PeopleEvent] | pd.DataFrame,
    *,
    ticker_resolver: Callable[[str, date], str | None] | None = None,
    contract: EventContract = PE_CONTRACT,
) -> pd.DataFrame:
    """Normalise canonical people events to the frame every feature reads.

    Accepts ``PeopleEvent`` objects (the DB path) or a DataFrame with the same
    columns (the off-DB path, e.g. a frozen VS1 SEC Form 3/4/5 artifact, which
    may also carry ``plan_10b5_1`` / ``entity_id`` columns directly). Drops
    ``echo_of IS NOT NULL`` rows, normalises ``known_at`` / ``event_time`` to
    tz-aware UTC (naive timestamps are refused) and keys every row on a
    security_master ``entity_id`` (contract C5). Unresolvable rows are dropped
    and counted in ``frame.attrs["dropped"]``. ``visible_until`` is the
    earliest of ``visible_until`` / ``superseded_at`` / ``retracted_at`` when
    present (people_events v2 versioning); NaT means visible for ever.
    """
    if isinstance(events, pd.DataFrame):
        raw = events.copy()
        if "plan_10b5_1" not in raw.columns:
            prov = raw["provenance"] if "provenance" in raw.columns else pd.Series([None] * len(raw), index=raw.index)
            raw["plan_10b5_1"] = [contract.plan_flag(p if isinstance(p, Mapping) else None) for p in prov]
        else:
            raw["plan_10b5_1"] = _map_unique(raw["plan_10b5_1"], contract._coerce)
    else:
        rows = []
        for ev in events:
            row = {c: getattr(ev, c, None) for c in EVENT_COLUMNS if c not in ("entity_id", "plan_10b5_1")}
            row["echo_of"] = ev.echo_of
            row["security_id"] = getattr(ev, "security_id", None)
            row["superseded_at"] = getattr(ev, "superseded_at", None)  # v2 versioning, when the store exposes it
            row["retracted_at"] = getattr(ev, "retracted_at", None)
            row["plan_10b5_1"] = contract.plan_flag(ev.provenance)
            rows.append(row)
        raw = pd.DataFrame(rows, columns=[c for c in EVENT_COLUMNS if c != "entity_id"] + list(_EXTRA_COLUMNS))

    for col in EVENT_COLUMNS + _EXTRA_COLUMNS:
        if col not in raw.columns:
            raw[col] = None
    n_in = len(raw)
    echo = raw["echo_of"].notna()
    raw = raw.loc[~echo]

    bad_channel = ~raw["channel"].isin(CHANNELS)
    bad_basis = ~raw["known_at_basis"].isin(KNOWN_AT_BASES)
    if bad_channel.any() or bad_basis.any():
        raise ValueError("events carry an unknown channel or known_at_basis")
    known_at = _to_utc(raw["known_at"], "known_at")
    if known_at.isna().any():
        raise ValueError("known_at must never be null")
    actor = _clean_text(raw["actor_id"])
    if actor.isna().any():
        raise ValueError("actor_id must never be empty")

    out = pd.DataFrame(index=raw.index)
    out["channel"] = raw["channel"].astype(object)
    out["dedup_key"] = _clean_text(raw["dedup_key"])
    out["event_time"] = _to_utc(raw["event_time"], "event_time")
    out["known_at"] = known_at
    out["known_at_basis"] = raw["known_at_basis"].astype(object)
    out["actor_id"] = actor
    out["actor_id_basis"] = _clean_text(raw["actor_id_basis"])
    out["actor_type"] = _clean_text(raw["actor_type"])
    out["entity_cik"] = _clean_text(raw["entity_cik"])
    out["entity_ticker"] = _clean_text(raw["entity_ticker"], upper=True)
    out["direction"] = _map_unique(_clean_text(raw["direction"]), lambda d: d.lower())
    out["transaction_code"] = _clean_text(raw["transaction_code"], upper=True)
    out["size_usd"] = pd.to_numeric(raw["size_usd"], errors="coerce").astype(float)
    out["plan_10b5_1"] = pd.array(list(raw["plan_10b5_1"]), dtype="boolean")
    # A version is visible from known_at until it is superseded or retracted.
    ends = [_to_utc(raw[c], c) for c in ("visible_until", "superseded_at", "retracted_at")]
    out["visible_until"] = pd.concat(ends, axis=1).min(axis=1)

    entity = _clean_text(raw["entity_id"])
    # v2 security_id is the TEXT entity_id; a v1 BIGINT carries no identity.
    text_sid = _map_unique(raw["security_id"], lambda v: v.strip() if isinstance(v, str) and v.strip() else None)
    entity = entity.where(entity.notna(), text_sid)
    from_cik = _map_unique(out["entity_cik"], lambda c: entity_id_for_cik(c) if str(c).isdigit() else None)
    entity = entity.where(entity.notna(), from_cik)
    non_entity = out["channel"].isin(contract.non_entity_channels).to_numpy()
    entity = entity.where(~non_entity, None)
    todo = entity.isna() & out["entity_ticker"].notna() & ~non_entity
    if ticker_resolver is not None and todo.any():
        days = out.loc[todo, "known_at"].dt.tz_convert(NEW_YORK).dt.date
        keys = pd.Series(list(zip(out.loc[todo, "entity_ticker"], days, strict=True)), index=days.index)
        entity.loc[todo] = _map_unique(keys, lambda k: ticker_resolver(k[0], k[1]))
    out["entity_id"] = entity
    unresolved = out["entity_id"].isna().to_numpy() & ~non_entity
    out = out.loc[out["entity_id"].notna(), list(EVENT_COLUMNS)]
    out = out.sort_values(
        ["entity_id", "channel", "actor_id", "known_at", "dedup_key"], kind="mergesort", na_position="last"
    ).reset_index(drop=True)
    out.attrs["dropped"] = {
        "echo": int(echo.sum()), "unresolved_entity": int(unresolved.sum()),
        "non_entity_channel": int(non_entity.sum()), "rows_in": n_in,
    }
    return out


def load_events(
    engine: Any,
    as_of: datetime,
    channels: Sequence[str],
    known_at_after: datetime | None = None,
    *,
    resolve_tickers: bool = True,
    contract: EventContract = PE_CONTRACT,
) -> pd.DataFrame:
    """The only database read: ``read_events(..., exclude_echoes=True)`` per channel.

    ``read_events`` filters on ``known_at <= as_of`` (never ``event_time``).
    Ticker-only rows are resolved through ``security_master.resolve_entity``
    as of the event's ``known_at`` New York date.

    Versioned store (people_events v2, PR #780): ``read_events`` returns the
    versions visible *at* ``as_of`` and hides one superseded or retracted
    before it, so a grid of decisions earlier than ``as_of`` would lose that
    version for ``[known_at, superseded_at)``. Until the store can return
    superseded/retracted versions with their timestamps (which the features
    already honour through ``visible_until``), call this with ``as_of`` equal
    to the grid's last decision and treat earlier decisions as subject to that
    caveat; on the v1 store (no versioning) there is none. The frame says so:
    ``attrs["versioned_store_gap"]`` is True when the store is versioned but
    the rows carry no visibility timestamps, and :func:`build_receipt`
    records it.
    """
    if as_of.tzinfo is None:
        raise ValueError("as_of must be tz-aware")
    rows: list[PeopleEvent] = []
    for channel in channels:
        rows.extend(read_events(engine, as_of, channel=channel, known_at_after=known_at_after, exclude_echoes=True))
    resolver = None
    if resolve_tickers:
        from intelligence.security_master import resolve_entity

        cache: dict[tuple[str, date], str | None] = {}

        def resolver(ticker: str, day: date) -> str | None:
            key = (ticker, day)
            if key not in cache:
                cache[key] = resolve_entity(engine, "ticker", ticker, as_of=day)
            return cache[key]

    frame = events_frame(rows, ticker_resolver=resolver, contract=contract)
    if (frame["known_at"] > pd.Timestamp(as_of).tz_convert("UTC")).any():  # defence in depth over read_events
        raise AssertionError("load_events returned an event with known_at > as_of")
    frame.attrs["versioned_store_gap"] = _store_is_versioned(engine) and not frame["visible_until"].notna().any()
    frame.attrs["as_of"] = pd.Timestamp(as_of).tz_convert("UTC").isoformat()
    return frame


_VERSIONED_COLUMNS_SQL = text(
    "SELECT count(*) FROM information_schema.columns "
    "WHERE table_schema = current_schema() AND table_name = 'people_events' "
    "AND column_name IN ('superseded_at', 'retracted_at')"
)


def _store_is_versioned(engine: Any) -> bool:
    """True when people_events has v2 versioning columns (PR #780)."""
    with engine.connect() as conn:
        return int(conn.execute(_VERSIONED_COLUMNS_SQL).scalar() or 0) > 0


# --- coverage ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CoverageSpan:
    """``channel`` covered ``entity_id`` (None = every entity) over ``[start, stop]``."""

    channel: str
    start: pd.Timestamp
    stop: pd.Timestamp | None = None
    entity_id: str | None = None
    known_at_basis: str | None = None  # None = any basis

    def __post_init__(self) -> None:
        if self.channel not in CHANNELS:
            raise ValueError(f"unknown channel {self.channel!r}")
        for ts in (self.start, self.stop):
            if ts is not None and pd.Timestamp(ts).tzinfo is None:
                raise ValueError("coverage timestamps must be tz-aware")
        if self.known_at_basis is not None and self.known_at_basis not in KNOWN_AT_BASES:
            raise ValueError(f"unknown known_at_basis {self.known_at_basis!r}")


def _ns_f(stamps: Any) -> np.ndarray:
    """UTC nanoseconds as float64, exactly as ``panel_insider_density.density`` converts them."""
    index = pd.DatetimeIndex(stamps)
    if index.tz is None:
        raise ValueError("timestamps must be tz-aware")
    return index.tz_convert("UTC").as_unit("ns").asi8.astype(np.float64)


def _check_decisions(decisions: pd.DatetimeIndex) -> pd.DatetimeIndex:
    if not isinstance(decisions, pd.DatetimeIndex) or decisions.tz is None:
        raise ValueError("decisions must be a tz-aware DatetimeIndex")
    if not decisions.is_monotonic_increasing or decisions.has_duplicates:
        raise ValueError("decisions must be strictly increasing")
    return decisions.tz_convert("UTC").as_unit("ns")


def coverage_mask(
    spec: DensitySpec,
    entities: Sequence[str],
    decisions: pd.DatetimeIndex,
    coverage: Sequence[CoverageSpan],
) -> pd.DataFrame:
    """True where every channel of ``spec`` covered the entity over ``[t - W, t]``.

    A span counts for a spec when its basis is None or one the spec allows.
    """
    decisions = _check_decisions(decisions)
    t = decisions.asi8
    w = np.int64(spec.window_days) * np.int64(86_400_000_000_000)
    entities = list(entities)
    ok = np.ones((len(decisions), len(entities)), dtype=bool)
    for channel in spec.channels:
        covered = np.zeros_like(ok)
        for span in coverage:
            if span.channel != channel:
                continue
            if span.known_at_basis is not None and span.known_at_basis not in spec.allowed_known_at_bases:
                continue
            start = pd.Timestamp(span.start).tz_convert("UTC").as_unit("ns").value
            stop = np.iinfo(np.int64).max if span.stop is None else pd.Timestamp(span.stop).tz_convert("UTC").as_unit("ns").value
            rows = (start <= t - w) & (t <= stop)
            if span.entity_id is None:
                covered |= rows[:, None]
            elif span.entity_id in entities:
                covered[:, entities.index(span.entity_id)] |= rows
        ok &= covered
    return pd.DataFrame(ok, index=decisions, columns=entities)


def coverage_change_log(coverage: Sequence[CoverageSpan]) -> pd.DataFrame:
    """Every coverage start and stop, in time order (so a jump is never read as density)."""
    rows = []
    for span in coverage:
        for kind, at in (("start", span.start), ("stop", span.stop)):
            if at is None:
                continue
            rows.append({
                "at": pd.Timestamp(at).tz_convert("UTC"), "kind": kind, "channel": span.channel,
                "entity_id": span.entity_id or "*", "known_at_basis": span.known_at_basis or "*",
            })
    frame = pd.DataFrame(rows, columns=["at", "kind", "channel", "entity_id", "known_at_basis"])
    return frame.sort_values(["at", "channel", "entity_id", "known_at_basis", "kind"], kind="mergesort").reset_index(drop=True)


def _live_start(coverage: Sequence[CoverageSpan], channel: str) -> pd.Timestamp | None:
    starts = [pd.Timestamp(s.start).tz_convert("UTC") for s in coverage
              if s.channel == channel and s.known_at_basis == "first_seen"]
    return min(starts) if starts else None


# --- selection --------------------------------------------------------------------------


def _select(
    events: pd.DataFrame,
    spec: DensitySpec,
    entities: Sequence[str],
    decisions: pd.DatetimeIndex,
    coverage: Sequence[CoverageSpan] | None,
    *,
    need_sign: bool = False,
) -> pd.DataFrame:
    """Qualifying events of ``spec`` on ``entities`` known by the last decision.

    Adds ``actor_key`` and, with ``need_sign``, ``sign`` (+1 / -1) and
    ``undetermined`` (a Form 4 sell with no 10b5-1 determination). Planned
    (10b5-1) Form 4 sells are dropped; rules with ``exclude_planned`` also
    drop planned purchases (as VS1's event build does).
    """
    t_max = decisions[-1] if len(decisions) else pd.Timestamp.min.tz_localize("UTC")
    frame = events.loc[(events["known_at"] <= t_max) & events["entity_id"].isin(entities)]
    frame = frame.loc[frame["known_at_basis"].isin(spec.allowed_known_at_bases)]
    parts = []
    for rule in spec.rules:
        part = frame.loc[frame["channel"] == rule.channel]
        if rule.transaction_codes is not None:
            code = part["transaction_code"].fillna("").astype(str)
            singles = [c for c in rule.transaction_codes if len(c) == 1]
            wholes = [c for c in rule.transaction_codes if len(c) > 1]
            part = part.loc[code.str[:1].isin(singles) | code.isin(wholes)]
        if rule.directions is not None:
            part = part.loc[part["direction"].isin(rule.directions)]
        if rule.actor_field not in part.columns:
            raise KeyError(f"actor_field {rule.actor_field!r} is not an event column")
        if rule.actor_field == "actor_id":
            key = part["actor_id_basis"].fillna("?").astype(str) + ":" + part["actor_id"].astype(str)
        else:
            key = part[rule.actor_field].astype(str)
        parts.append((rule, part.assign(actor_key=key)))

    if coverage is not None:
        for rule, part in parts:
            seen = part.loc[part["known_at_basis"] == "first_seen", "known_at"]
            if seen.empty:
                continue
            live = _live_start(coverage, rule.channel)
            if live is None:
                raise ImpossibleEvent(f"{rule.channel}: first_seen events but no live start is registered")
            if (seen < live).any():
                raise ImpossibleEvent(f"{rule.channel}: a first_seen event is known before the live start {live}")

    signs = {**{d: 1.0 for d in POSITIVE_DIRECTIONS}, **{d: -1.0 for d in NEGATIVE_DIRECTIONS}}
    out = []
    for rule, part in parts:
        planned = part["plan_10b5_1"].fillna(False).astype(bool)
        if need_sign:
            sign = part["direction"].map(signs).fillna(0.0).astype(float)
            undetermined = pd.Series(False, index=part.index)
            if rule.channel == "form4":
                sells = sign < 0
                undetermined = sells & part["plan_10b5_1"].isna()
                keep = ~(sells & planned)
                part, sign, planned, undetermined = part.loc[keep], sign.loc[keep], planned.loc[keep], undetermined.loc[keep]
            part = part.assign(sign=sign, undetermined=undetermined.astype(bool))
            keep = (part["sign"] != 0).to_numpy()
            part, planned = part.loc[keep], planned.loc[keep]
        if rule.exclude_planned:
            part = part.loc[~planned.to_numpy()]
        out.append(part)
    cols = ["entity_id", "channel", "actor_key", "known_at", "visible_until"]
    cols += ["sign", "undetermined"] if need_sign else []
    if not out:
        return pd.DataFrame(columns=cols)
    return pd.concat([p[cols] for p in out], ignore_index=True)


# --- features ---------------------------------------------------------------------------


def _visible_pairs(
    known_f: np.ndarray, until_f: np.ndarray, t_f: np.ndarray, window_ns: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(event, decision, age) for every decision at which an event is visible and in the window.

    Visible at ``t``: ``known_at <= t < visible_until`` (superseded or
    retracted versions stop at that instant; ``+inf`` when never). In the
    window: ``0 <= t - known_at < W`` in float64 nanoseconds -- the same
    arithmetic as ``analysis.panel_insider_density.density``
    (``age = t - known``, ``age >= 0``, ``age < window_ns``).
    """
    n = len(known_f)
    if n == 0 or len(t_f) == 0:
        empty = np.zeros(0, dtype=np.int64)
        return empty, empty, np.zeros(0)
    lo = np.searchsorted(t_f, known_f, side="left")  # first t >= known_at
    hi = np.searchsorted(t_f, known_f + window_ns + 1e6, side="right")  # exact age check below
    hi = np.minimum(hi, np.searchsorted(t_f, until_f, side="left"))  # first t >= visible_until
    count = np.maximum(hi - lo, 0)
    event = np.repeat(np.arange(n), count)
    offset = np.arange(int(count.sum())) - np.repeat(np.cumsum(count) - count, count)
    decision = np.repeat(lo, count) + offset
    age = t_f[decision] - known_f[event]
    keep = (age >= 0) & (age < window_ns)
    return event[keep], decision[keep], age[keep]


def _until_f(sel: pd.DataFrame) -> np.ndarray:
    until = sel["visible_until"]
    out = np.full(len(sel), np.inf)
    has = until.notna().to_numpy()
    if has.any():
        out[has] = _ns_f(until[has])
    return out


#: Upper bound on (event, decision) pairs materialised at once; work is split
#: by entity so a cell's sum order never depends on the chunking.
MAX_PAIRS_PER_CHUNK = 4_000_000


def _entity_chunks(ent: np.ndarray, known_f: np.ndarray, t_f: np.ndarray, window_ns: float):
    """Row-index blocks of whole entities, each with at most about MAX_PAIRS_PER_CHUNK pairs."""
    order = np.argsort(ent, kind="stable")
    est = (np.searchsorted(t_f, known_f[order] + window_ns, side="right")
           - np.searchsorted(t_f, known_f[order], side="left")).clip(0)
    bounds = np.flatnonzero(np.r_[True, ent[order][1:] != ent[order][:-1], True])
    start, load = 0, 0
    for a, b in zip(bounds[:-1], bounds[1:], strict=True):
        cost = int(est[a:b].sum())
        if load and load + cost > MAX_PAIRS_PER_CHUNK:
            yield order[start:a]
            start, load = a, 0
        load += cost
    if start < len(order):
        yield order[start:]


def _density(sel: pd.DataFrame, spec: DensitySpec, entities: Sequence[str], decisions: pd.DatetimeIndex) -> np.ndarray:
    """``sum over actors (in key order) of max over visible in-window events of exp(-age / tau)``."""
    t_f = decisions.asi8.astype(np.float64)
    out = np.zeros((len(decisions), len(entities)))
    if sel.empty:
        return out
    window_ns, tau_ns = spec.window_days * DAY_NS, spec.tau_days * DAY_NS
    known_all, until_all = _ns_f(sel["known_at"]), _until_f(sel)
    ent_all = pd.Index(entities).get_indexer(sel["entity_id"])
    act_all = pd.factorize(sel["actor_key"].astype(str), sort=True)[0]  # codes in key order
    for rows in _entity_chunks(ent_all, known_all, t_f, window_ns):
        event, decision, age = _visible_pairs(known_all[rows], until_all[rows], t_f, window_ns)
        if len(event) == 0:
            continue
        weight = np.exp(-age / tau_ns)
        ent, act = ent_all[rows][event], act_all[rows][event]
        order = np.lexsort((decision, act, ent))
        ent, act, decision, weight = ent[order], act[order], decision[order], weight[order]
        starts = np.flatnonzero(
            np.r_[True, (ent[1:] != ent[:-1]) | (act[1:] != act[:-1]) | (decision[1:] != decision[:-1])]
        )
        best = np.maximum.reduceat(weight, starts)  # each actor once: its largest weight (latest event)
        # Cells are filled in (entity, actor-key) order and np.add.at is
        # unbuffered and sequential, so each cell is ((0 + w_a1) + w_a2) + ...
        # in actor-key order -- panel_insider_density's per_actor.sum(axis=0)
        # on a grid of two or more decisions -- and an actor with nothing
        # visible adds nothing, so rows known after t never change a value at
        # t, bit for bit. A cell belongs to one entity, so chunking by entity
        # cannot reorder it.
        np.add.at(out, (decision[starts], ent[starts]), best)
    return out


def _entities(entities: Sequence[str]) -> list[str]:
    out = list(entities)
    if len(set(out)) != len(out):
        raise ValueError("entities must be unique")
    return out


def _frame(values: np.ndarray, decisions: pd.DatetimeIndex, entities: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(values, index=decisions, columns=list(entities))


def _guard(values: np.ndarray, spec: DensitySpec, entities, decisions, coverage) -> np.ndarray:
    if coverage is None:
        return values
    mask = coverage_mask(spec, entities, decisions, coverage).to_numpy()
    return np.where(mask, values, np.nan)


def density_A(
    events: pd.DataFrame,
    spec: DensitySpec,
    entities: Sequence[str],
    decisions: pd.DatetimeIndex,
    *,
    coverage: Sequence[CoverageSpan] | None,
) -> pd.DataFrame:
    """``A(e, t)`` (decisions x entities). ``coverage=None`` returns unguarded raw values.

    An event counts at ``t`` iff ``t - W < known_at <= t`` (and it is not
    superseded or retracted by ``t``); each actor once, at the weight of its
    latest such event. ``coverage`` is keyword-only with no default, so a
    caller decides explicitly whether the coverage guard applies (NaN where
    it fails). Bit-identical to ``panel_insider_density.density`` on grids of
    two or more decisions (a one-decision grid differs from it by an ulp: its
    ``sum(axis=0)`` over an (n, 1) array is pairwise, this one sequential).
    """
    if spec.signed:
        raise ValueError(f"{spec.name} is a signed spec; use signed_S")
    decisions = _check_decisions(decisions)
    entities = _entities(entities)
    sel = _select(events, spec, entities, decisions, coverage)
    values = _density(sel, spec, entities, decisions)
    return _frame(_guard(values, spec, entities, decisions, coverage), decisions, entities)


def channel_count_C(
    events: pd.DataFrame,
    spec: DensitySpec,
    entities: Sequence[str],
    decisions: pd.DatetimeIndex,
    *,
    coverage: Sequence[CoverageSpan] | None,
) -> pd.DataFrame:
    """``C(e, t)``: distinct channels of ``spec`` with a visible qualifying event in ``(t - W, t]``."""
    decisions = _check_decisions(decisions)
    entities = _entities(entities)
    sel = _select(events, spec, entities, decisions, coverage)
    t_f = decisions.asi8.astype(np.float64)
    channels = sorted(spec.channels)
    present = np.zeros((len(decisions), len(entities), len(channels)), dtype=bool)
    if not sel.empty:
        window_ns = spec.window_days * DAY_NS
        known_all, until_all = _ns_f(sel["known_at"]), _until_f(sel)
        ent_all = pd.Index(entities).get_indexer(sel["entity_id"])
        chan_all = pd.Index(channels).get_indexer(sel["channel"])
        for rows in _entity_chunks(ent_all, known_all, t_f, window_ns):
            event, decision, _age = _visible_pairs(known_all[rows], until_all[rows], t_f, window_ns)
            present[decision, ent_all[rows][event], chan_all[rows][event]] = True
    out = present.sum(axis=2).astype(float)
    return _frame(_guard(out, spec, entities, decisions, coverage), decisions, entities)


def signed_S(
    events: pd.DataFrame,
    spec: DensitySpec,
    entities: Sequence[str],
    decisions: pd.DatetimeIndex,
    *,
    coverage: Sequence[CoverageSpan] | None,
    on_undetermined: str = "raise",
) -> pd.DataFrame:
    """``S(e, t) = A(positive events) - A(negative events)``; each actor once per sign.

    Planned (10b5-1) Form 4 sells are excluded. A Form 4 sell with **no**
    determination is never counted and never silently dropped: every cell
    ``(e, t)`` it would enter (visible and in the window) is undefined. With
    ``on_undetermined="raise"`` (default) :class:`UndefinedFeature` names the
    first such cell; with ``"nan"`` those cells are NaN and every other cell
    is computed. Cells a sell cannot reach (other entities, decisions before
    it is known or after it leaves the window) are unaffected, so appending
    rows known after ``t`` never changes the value at ``t``. Non-Form-4
    channels compute normally.
    """
    if on_undetermined not in ("raise", "nan"):
        raise ValueError("on_undetermined must be 'raise' or 'nan'")
    decisions = _check_decisions(decisions)
    entities = _entities(entities)
    sel = _select(events, spec, entities, decisions, coverage, need_sign=True)
    undetermined = sel.loc[sel["undetermined"]]
    counted = sel.loc[~sel["undetermined"]]
    pos = _density(counted.loc[counted["sign"] > 0], spec, entities, decisions)
    neg = _density(counted.loc[counted["sign"] < 0], spec, entities, decisions)
    values = pos - neg
    if not undetermined.empty:
        t_f = decisions.asi8.astype(np.float64)
        event, decision, _age = _visible_pairs(
            _ns_f(undetermined["known_at"]), _until_f(undetermined), t_f, spec.window_days * DAY_NS
        )
        if len(event):
            ent = pd.Index(entities).get_indexer(undetermined["entity_id"])[event]
            if on_undetermined == "raise":
                first = int(np.argmin(decision))
                raise UndefinedFeature(
                    f"{spec.name}: a Form 4 sell with no 10b5-1 determination enters "
                    f"{entities[ent[first]]} at {decisions[decision[first]]} ({len(set(zip(decision, ent, strict=True)))} cells)"
                )
            values[decision, ent] = np.nan
    return _frame(_guard(values, spec, entities, decisions, coverage), decisions, entities)


def _ny_dates(decisions: pd.DatetimeIndex) -> list[date]:
    return [ts.date() for ts in decisions.tz_convert(NEW_YORK)]


def d_self(a_weekly: pd.DataFrame, *, epsilon: float = D_SELF_EPSILON, weeks: int = D_SELF_WEEKS) -> pd.DataFrame:
    """``(A - median) / (MAD + eps)`` over the trailing ``weeks`` weekly values (decision <= t).

    The input must be on consecutive weekly decisions (7 New York calendar
    days apart). NaN where fewer than ``weeks`` finite observations exist in
    the trailing window (never imputed). MAD is the raw median absolute
    deviation (no 1.4826 scaling).
    """
    index = _check_decisions(pd.DatetimeIndex(a_weekly.index))
    days = _ny_dates(index)
    if any((b - a).days != 7 for a, b in zip(days[:-1], days[1:], strict=True)):
        raise ValueError("d_self needs consecutive weekly decisions (7 New York days apart)")
    values = a_weekly.to_numpy(dtype=float)
    out = np.full(values.shape, np.nan)
    if len(values) >= weeks:
        windows = np.lib.stride_tricks.sliding_window_view(values, weeks, axis=0)  # (n-w+1, k, w)
        finite = np.isfinite(windows).all(axis=2)
        with np.errstate(all="ignore"):
            med = np.median(windows, axis=2)
            mad = np.median(np.abs(windows - med[..., None]), axis=2)
            z = (values[weeks - 1:] - med) / (mad + epsilon)
        out[weeks - 1:] = np.where(finite, z, np.nan)
    return pd.DataFrame(out, index=a_weekly.index, columns=a_weekly.columns)


def _membership_matrix(membership: pd.DataFrame, entities: Sequence[str], days: Sequence[date]) -> np.ndarray:
    """Sector of each entity on each New York date (None when not a member)."""
    required = {"entity_id", "sector", "valid_from", "valid_to"}
    if not required <= set(membership.columns):
        raise ValueError(f"membership needs columns {sorted(required)}")
    col = {e: j for j, e in enumerate(entities)}
    day_arr = np.array([np.datetime64(d, "D") for d in days])
    out = np.full((len(days), len(entities)), None, dtype=object)
    for row in membership.itertuples(index=False):
        j = col.get(row.entity_id)
        if j is None:
            continue
        lo = np.datetime64(pd.Timestamp(row.valid_from).date(), "D")
        live = day_arr >= lo
        if row.valid_to is not None and not pd.isna(row.valid_to):
            live &= day_arr <= np.datetime64(pd.Timestamp(row.valid_to).date(), "D")
        clash = live & (out[:, j] != None) & (out[:, j] != row.sector)  # noqa: E711
        if clash.any():
            raise ValueError(f"{row.entity_id} has two sectors on {days[int(np.flatnonzero(clash)[0])]}")
        out[live, j] = row.sector
    return out


def d_peer(a: pd.DataFrame, membership: pd.DataFrame, *, min_peers: int = MIN_PEER_COUNT) -> pd.DataFrame:
    """Average-rank percentile (rank / n) of A among same-sector peers at each t.

    Membership is the frame valid on t's New York date (``valid_from <= d``
    and ``valid_to`` null or ``>= d``, as ``security_master`` reads it). NaN
    for non-members, NaN A, and sectors with fewer than ``min_peers`` scored
    entities.
    """
    index = _check_decisions(pd.DatetimeIndex(a.index))
    entities = list(a.columns)
    sectors = _membership_matrix(membership, entities, _ny_dates(index))
    values = a.to_numpy(dtype=float)
    out = np.full(values.shape, np.nan)
    for sector in sorted({s for s in sectors.ravel() if s is not None}):
        member = sectors == sector
        cols = np.flatnonzero(member.any(axis=0))
        scored = member[:, cols] & np.isfinite(values[:, cols])
        n = scored.sum(axis=1)
        # Unscored cells rank as +inf, above every scored value, so the
        # scored cells' average ranks are exactly their ranks among peers.
        ranks = rankdata(np.where(scored, values[:, cols], np.inf), method="average", axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            pct = ranks / n[:, None]
        keep = scored & (n[:, None] >= min_peers)
        out[:, cols] = np.where(keep, pct, out[:, cols])
    return pd.DataFrame(out, index=a.index, columns=entities)


def weekly_decisions(start: date, end: date) -> pd.DatetimeIndex:
    """Every Friday in ``[start, end]`` at 16:00 America/New_York, in UTC."""
    days = pd.date_range(start, end, freq="W-FRI")
    local = days + pd.Timedelta(hours=DECISION_HOUR_NY)
    return local.tz_localize(NEW_YORK).tz_convert("UTC").as_unit("ns")


def sector_weekly_aggregates(
    events: pd.DataFrame,
    specs: Sequence[DensitySpec],
    membership: pd.DataFrame,
    decisions: pd.DatetimeIndex,
    *,
    coverage: Sequence[CoverageSpan],
    on_undetermined: str = "raise",
) -> pd.DataFrame:
    """Per (Friday decision, sector, spec): sum A, #D_peer > 0.9, #covered, #members.

    ``sum_A`` is over covered, defined constituents and NaN when there is
    none. The coverage guard is mandatory here. ``on_undetermined`` is passed
    to :func:`signed_S` for signed specs; its NaN cells are counted in
    ``n_undefined``, separately from coverage.
    """
    decisions = _check_decisions(decisions)
    days = _ny_dates(decisions)
    if any(d.weekday() != 4 for d in days):
        raise ValueError("sector aggregates are Friday decisions")
    entities = sorted(set(membership["entity_id"]))
    sectors = _membership_matrix(membership, entities, days)
    rows = []
    for spec in sorted(specs, key=lambda s: s.name):
        if spec.signed:
            a = signed_S(events, spec, entities, decisions, coverage=coverage, on_undetermined=on_undetermined)
        else:
            a = density_A(events, spec, entities, decisions, coverage=coverage)
        peer = d_peer(a, membership).to_numpy()
        values = a.to_numpy()
        guard = coverage_mask(spec, entities, decisions, coverage).to_numpy()
        for i, t in enumerate(decisions):
            for sector in sorted({s for s in sectors[i] if s is not None}):
                members = sectors[i] == sector
                covered = members & guard[i]
                scored = covered & np.isfinite(values[i])
                rows.append({
                    "decision_at": t, "sector": str(sector), "spec": spec.name,
                    "sum_A": float(values[i, scored].sum()) if scored.any() else np.nan,
                    "n_dpeer_gt_0p9": int((members & (np.nan_to_num(peer[i], nan=-1.0) > D_PEER_HIGH)).sum()),
                    "n_covered": int(covered.sum()),
                    "n_undefined": int((covered & ~np.isfinite(values[i])).sum()),  # S with undetermined sells
                    "n_members": int(members.sum()),
                })
    frame = pd.DataFrame(rows, columns=list(AGGREGATE_COLUMNS))
    return frame.sort_values(["decision_at", "sector", "spec"], kind="mergesort").reset_index(drop=True)


# --- hashing, artifact, receipt ---------------------------------------------------------


def _canonical(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {str(k): _canonical(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}
    if isinstance(obj, (list, tuple)):
        return [_canonical(v) for v in obj]
    if isinstance(obj, float) and not np.isfinite(obj):
        return repr(obj)
    return obj


def _canonical_sha256(obj: Any) -> str:
    payload = json.dumps(_canonical(obj), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def event_set_sha256(events: pd.DataFrame) -> str:
    """Order-, timezone- and platform-independent hash of a normalised event frame."""
    cols = ["channel", "dedup_key", "known_at", "known_at_basis", "actor_id", "actor_id_basis",
            "entity_id", "direction", "transaction_code", "plan_10b5_1", "event_time", "visible_until"]
    recs = []
    for row in events[cols].itertuples(index=False):
        rec = []
        for v in row:
            if isinstance(v, pd.Timestamp):
                rec.append(int(v.tz_convert("UTC").as_unit("ns").value))
            elif v is None or v is pd.NA or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
                rec.append(None)
            elif isinstance(v, (bool, np.bool_)):
                rec.append(bool(v))
            else:
                rec.append(str(v))
        recs.append(rec)
    recs.sort(key=lambda r: json.dumps(r))
    return _canonical_sha256(recs)


def membership_sha256(membership: pd.DataFrame) -> str:
    recs = sorted(
        ([str(r.entity_id), str(r.sector), str(pd.Timestamp(r.valid_from).date()),
          None if r.valid_to is None or pd.isna(r.valid_to) else str(pd.Timestamp(r.valid_to).date())]
         for r in membership.itertuples(index=False)),
        key=json.dumps,  # a total order even when None sits next to a string
    )
    return _canonical_sha256(recs)


def lf_sha256(path: Path | str) -> str:
    """sha256 of a text file with CRLF normalised to LF (a CRLF checkout hashes the same)."""
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def code_sha256() -> str:
    """LF-normalised sha256 of this module's source."""
    return lf_sha256(__file__)


def write_frozen_artifact(frame: pd.DataFrame, path: Path | str, *, receipt: Mapping[str, Any] | None = None) -> str:
    """Write ``frame`` as deterministic parquet and return its sha256.

    Column order is the frame's; rows are sorted by every column in that
    order; datetimes become ``timestamp[ns, UTC]``, floats ``float64``,
    integers ``int64``, everything else ``string``. No pandas/arrow schema
    metadata is written, so bytes depend only on the values and the pyarrow
    version. A receipt JSON (with the artifact sha256 added) is written next
    to it as ``<path>.receipt.json``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(path)
    data = frame.sort_values(list(frame.columns), kind="mergesort", na_position="last").reset_index(drop=True)
    arrays, fields = [], []
    for name in data.columns:
        s = data[name]
        if isinstance(s.dtype, pd.DatetimeTZDtype):
            typ = pa.timestamp("ns", tz="UTC")
            arr = pa.array(s.dt.tz_convert("UTC").dt.as_unit("ns").astype("int64").to_numpy(), type=pa.int64()).cast(typ)
        elif pd.api.types.is_bool_dtype(s.dtype):
            typ, arr = pa.bool_(), pa.array(s.to_numpy(dtype=bool))
        elif pd.api.types.is_integer_dtype(s.dtype):
            typ, arr = pa.int64(), pa.array(s.to_numpy(dtype=np.int64))
        elif pd.api.types.is_float_dtype(s.dtype):
            typ, arr = pa.float64(), pa.array(s.to_numpy(dtype=np.float64), from_pandas=False)
        else:
            typ = pa.string()
            arr = pa.array([None if v is None or (isinstance(v, float) and np.isnan(v)) else str(v) for v in s], type=typ)
        arrays.append(arr)
        fields.append(pa.field(str(name), typ))
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd", compression_level=3, use_dictionary=False,
                   write_statistics=False, version="2.6", data_page_version="1.0", store_schema=False)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if receipt is not None:
        body = dict(_canonical(dict(receipt)))
        body["artifact_sha256"] = digest
        body["artifact_rows"] = int(len(table))
        text = json.dumps(body, sort_keys=True, indent=2, separators=(",", ": "), default=str) + "\n"
        Path(f"{path}.receipt.json").write_bytes(text.encode())
    return digest


def coverage_sha256(coverage: Sequence[CoverageSpan] | None) -> str | None:
    if coverage is None:
        return None
    recs = sorted(
        ([s.channel, pd.Timestamp(s.start).tz_convert("UTC").isoformat(),
          None if s.stop is None else pd.Timestamp(s.stop).tz_convert("UTC").isoformat(), s.entity_id, s.known_at_basis]
         for s in coverage),
        key=json.dumps,  # a total order even when None sits next to a string
    )
    return _canonical_sha256(recs)


def _versions() -> dict[str, str]:
    import platform

    import pyarrow
    import scipy

    return {"python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__,
            "pyarrow": pyarrow.__version__, "scipy": scipy.__version__}


def build_receipt(
    *,
    events: pd.DataFrame,
    specs: Sequence[DensitySpec],
    membership: pd.DataFrame,
    decisions: pd.DatetimeIndex,
    coverage: Sequence[CoverageSpan] | None,
    as_of: datetime,
    git_sha: str | None = None,
    contract: EventContract = PE_CONTRACT,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The receipt fields GD6/GD7 verify before reading an aggregate artifact."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be tz-aware")
    receipt = {
        "schema": "gd5-people-density-v1",
        "as_of": pd.Timestamp(as_of).tz_convert("UTC").isoformat(),
        "event_set_sha256": event_set_sha256(events),
        "event_rows": int(len(events)),
        "load_as_of": events.attrs.get("as_of"),
        # None = unknown (the frame did not come from load_events, or lost its attrs).
        "versioned_store_gap": events.attrs.get("versioned_store_gap"),
        "spec_sha256": {s.name: s.sha256 for s in sorted(specs, key=lambda s: s.name)},
        "membership_sha256": membership_sha256(membership),
        "coverage_sha256": coverage_sha256(coverage),
        "decisions": {
            "count": int(len(decisions)),
            "first": None if not len(decisions) else pd.Timestamp(decisions[0]).tz_convert("UTC").isoformat(),
            "last": None if not len(decisions) else pd.Timestamp(decisions[-1]).tz_convert("UTC").isoformat(),
            "sha256": _canonical_sha256([int(v) for v in _check_decisions(pd.DatetimeIndex(decisions)).asi8]),
        },
        "versions": _versions(),
        "code_sha256": code_sha256(),
        "git_sha": git_sha,
        "contract": asdict(contract),
        "constants": {"d_self_weeks": D_SELF_WEEKS, "d_self_epsilon": D_SELF_EPSILON,
                      "min_peer_count": MIN_PEER_COUNT, "d_peer_high": D_PEER_HIGH},
    }
    if extra:
        receipt["extra"] = dict(extra)
    return receipt
