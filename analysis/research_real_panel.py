"""Read-only real-panel adapter for ``analysis.offline_research_proof`` (S09).

What it does
------------
Reads a small declared universe of ``raw_series`` ids **only** through
``store.observations.read_window`` (``SUCCESS`` rows only, one row per
``obs_date`` = latest vintage, nothing observed after ``as_of``, nothing pulled
after ``as_of_ts``) and turns it into the feature frame and target levels the
research contract labels. It never writes, never builds SQL of its own, and
never reads ``discovered_hypotheses``, ``hypothesis_registry`` or any other
table.

Why a distinct origin
---------------------
``exploratory_replay`` is a self-declared label that accepts any latest-vintage
CSV. The ``pit_vintage_read`` origin is different: ``load_pit_panel`` is the
only way to build a :class:`PitPanel`; the panel carries a receipt (reader,
``as_of``/``as_of_ts``, declared specs and a sha256 over every observation it
read); the protocol must carry that receipt's hash; and the contract
(``discover``/``evaluate_holdout``) re-derives every row from the verified
panel via :func:`verify_pit_rows` and refuses rows that differ. A protocol that
only says ``origin="pit_vintage_read"`` is refused.

Availability (known-at) rules
-----------------------------
* A feature observation dated ``d`` becomes usable at the first session on or
  after ``d + lag_days`` (a conservative per-series publication lag), then is
  carried forward at most ``stale_sessions`` sessions before abstaining.
* A target level is the observation dated exactly on the session (no carry).
  Its forward label ends at session ``t + h`` and is known at the first session
  on or after ``label_end + lag_days``; a label not known inside its window is
  dropped (it cannot cross the holdout boundary by publication lag either).
* Sessions are business days (``pd.bdate_range``), not an exchange calendar:
  a holiday is a session with no observation (features carry, targets drop).

Honest limits
-------------
``raw_series`` keeps the latest vintage per date, and on griddb every row of
the S09 universe was pulled on or after 2026-03-24 (backfill). ``as_of_ts``
therefore makes the read reproducible and excludes later pulls, but it does
**not** give first-release values for 2004-2025: revisions are latest-vintage
hindsight. Series with known material revisions (e.g. NFCI, seasonally
adjusted claims) are refused (``revised=True``) until a vintage history exists.
yfinance-sourced ids (``YF:``/``YF_ADJ:``) are refused: historical ``YF:*:close``
rows carry more than one close per date (S07/#642 price-basis contamination).
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

from analysis.offline_research_proof import (
    LABELS,
    PIT_ORIGIN,
    build_family_rows,
    digest,
    stamp,
)
from store import observations

READER = "store.observations.read_window"
TRANSFORMS = ("diff", "pct")
FEATURE_SUFFIXES = ("chg5", "chg20", "z60")

# Universe exclusions (S09): internal telemetry/LLM counters, astro/celestial.
EXCLUDED_TOKENS = (
    "snap:",
    "llm",
    "ollama",
    "qwen",
    "telemetry",
    "pipeline",
    "hermes",
    "astro",
    "celestial",
    "lunar",
    "planet",
    "ephemeris",
    "jyotish",
    "vedic",
    "zodiac",
    "chinese_calendar",
)
UNVERIFIED_PRICE_PREFIXES = ("YF:", "YF_ADJ:")

_LOADER = object()  # capability: only load_pit_panel may construct a PitPanel


@dataclass(frozen=True)
class SeriesSpec:
    """A feature series: ``chg5``/``chg20`` as a difference or a percent change, plus ``z60``."""

    series_id: str
    transform: str = "diff"
    lag_days: int = 1  # calendar days from obs_date until published (conservative)
    stale_sessions: int = 5  # carry-forward limit before an explicit abstention
    revised: bool = False  # material revisions without a vintage history: refused


@dataclass(frozen=True)
class TargetSpec:
    """A target level labelled over ``horizon`` sessions as a ``change`` or ``return``."""

    series_id: str
    label: str = "change"
    lag_days: int = 1


def refusal(series_id: str) -> str | None:
    """Why ``series_id`` may not enter the S09 universe, or ``None``."""
    low = series_id.lower()
    if any(token in low for token in EXCLUDED_TOKENS):
        return "excluded: internal telemetry, LLM counter or astro/celestial"
    if series_id.startswith(UNVERIFIED_PRICE_PREFIXES):
        return "refused: yfinance price basis not verified single-valued per date"
    return None


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _observation_record(o: observations.Observation) -> list:
    return [o.obs_date.isoformat(), o.value, _utc(o.pull_timestamp).isoformat()]


class PitPanel:
    """Observations read through ``store.observations.read_window``, with a receipt.

    Built only by :func:`load_pit_panel`. Immutable by convention; ``verify``
    recomputes the receipt from the held observations, so a changed value, date
    or pull timestamp is detected.
    """

    def __init__(
        self,
        *,
        token: object,
        features: tuple[SeriesSpec, ...],
        targets: tuple[TargetSpec, ...],
        start: date,
        as_of: date,
        as_of_ts: datetime,
        data: dict[str, tuple[observations.Observation, ...]],
    ) -> None:
        if token is not _LOADER:
            raise TypeError("PitPanel is built only by load_pit_panel")
        self.features = features
        self.targets = targets
        self.start = start
        self.as_of = as_of
        self.as_of_ts = as_of_ts
        self._data = dict(data)
        self.receipt = self._receipt()
        self.receipt_sha = digest(self.receipt)

    # --- receipt ---------------------------------------------------------------

    def _check(self) -> None:
        for spec in (*self.features, *self.targets):
            if refusal(spec.series_id):
                raise ValueError(f"{spec.series_id}: {refusal(spec.series_id)}")
        for sid, obs in self._data.items():
            previous = None
            for o in obs:
                if not isinstance(o, observations.Observation):
                    raise ValueError(f"{sid}: not read through {READER}")  # noqa: TRY004
                ts = _utc(o.pull_timestamp)
                if (
                    o.series_id != sid
                    or not self.start <= o.obs_date <= self.as_of
                    or ts is None
                    or ts > self.as_of_ts
                    or (previous is not None and o.obs_date <= previous)
                    or not math.isfinite(o.value)
                ):
                    raise ValueError(f"{sid}: observation outside the vintage-safe read")
                previous = o.obs_date

    def _receipt(self) -> dict:
        self._check()
        return {
            "reader": READER,
            "origin": PIT_ORIGIN,
            "start": self.start.isoformat(),
            "as_of": self.as_of.isoformat(),
            "as_of_ts": self.as_of_ts.isoformat(),
            "features": [asdict(s) for s in self.features],
            "targets": [asdict(t) for t in self.targets],
            "series": {
                sid: {
                    "n": len(obs),
                    "first": obs[0].obs_date.isoformat() if obs else None,
                    "last": obs[-1].obs_date.isoformat() if obs else None,
                    "last_pull": max(
                        (_utc(o.pull_timestamp).isoformat() for o in obs), default=None
                    ),
                    "sha256": digest([_observation_record(o) for o in obs]),
                }
                for sid, obs in sorted(self._data.items())
            },
        }

    def verify(self) -> None:
        if digest(self._receipt()) != self.receipt_sha:
            raise ValueError("PIT panel changed after its vintage-safe read")

    # --- frames ----------------------------------------------------------------

    def series_observations(self, series_id: str) -> tuple[observations.Observation, ...]:
        return self._data[series_id]

    def session_index(self) -> pd.DatetimeIndex:
        return pd.bdate_range(self.start, self.as_of, tz="UTC")

    def feature_names(self) -> tuple[str, ...]:
        return tuple(
            f"{s.series_id}|{suffix}" for s in self.features for suffix in FEATURE_SUFFIXES
        )

    def family_names(self, horizons: tuple[int, ...]) -> tuple[str, ...]:
        return tuple(
            f"{t.series_id}|{t.label}|fwd{h}" for t in self.targets for h in horizons
        )

    def _available_level(self, spec: SeriesSpec, index: pd.DatetimeIndex) -> pd.Series:
        """Level as known at each session: published obs only, carried <= stale limit."""
        level = pd.Series(np.nan, index=index)
        obs = self._data[spec.series_id]
        if not obs:
            return level
        available = pd.DatetimeIndex(
            [pd.Timestamp(o.obs_date) + pd.Timedelta(days=spec.lag_days) for o in obs]
        ).tz_localize("UTC")
        position = index.searchsorted(available, side="left")
        values = np.array([o.value for o in obs], dtype=float)
        keep = position < len(index)
        # Several obs published by the same session: the newest obs_date wins.
        latest = pd.Series(values[keep], index=position[keep]).groupby(level=0).last()
        level.iloc[latest.index.to_numpy()] = latest.to_numpy()
        return level.ffill(limit=spec.stale_sessions)

    def feature_frame(self) -> pd.DataFrame:
        index = self.session_index()
        columns = {}
        for spec in self.features:
            x = self._available_level(spec, index)
            for n in (5, 20):
                columns[f"{spec.series_id}|chg{n}"] = (
                    x / x.shift(n) - 1 if spec.transform == "pct" else x - x.shift(n)
                )
            mean, std = x.rolling(60).mean(), x.rolling(60).std()
            columns[f"{spec.series_id}|z60"] = (x - mean) / std
        frame = pd.DataFrame(columns, index=index)[list(self.feature_names())]
        # inf (pct change from 0, zero rolling std) is not an observation: abstain.
        return frame.replace([np.inf, -np.inf], np.nan)

    def target_level(self, target: TargetSpec, index: pd.DatetimeIndex) -> pd.Series:
        obs = self._data[target.series_id]
        by_date = pd.Series(
            [o.value for o in obs],
            index=pd.DatetimeIndex([pd.Timestamp(o.obs_date) for o in obs]).tz_localize(
                "UTC"
            )
            if obs
            else pd.DatetimeIndex([], tz="UTC"),
            dtype=float,
        )
        return by_date.reindex(index)

    def family_rows(self, protocol, window: str) -> dict[str, list[dict]]:
        """Rows for every declared family, target known-at shifted by its publication lag."""
        index = self.session_index()
        features = self.feature_frame()
        targets = {t.series_id: t for t in self.targets}
        bound = stamp(protocol.split if window == "discovery" else protocol.end)
        out = {}
        for family in protocol.families:
            sid, label, fwd = family.rsplit("|", 2)
            target = targets.get(sid)
            if target is None or label != target.label or not fwd.startswith("fwd"):
                raise ValueError(f"family {family} is not a declared panel target")
            rows = build_family_rows(
                protocol,
                features,
                self.target_level(target, index),
                int(fwd[3:]),
                window,
                label,
            )
            kept = []
            for row in rows:
                end = pd.Timestamp(row["label_end"])
                known_pos = index.searchsorted(
                    end + pd.Timedelta(days=target.lag_days), side="left"
                )
                if known_pos >= len(index):
                    continue  # not published inside the read
                known = index[known_pos]
                if known.to_pydatetime() >= bound:
                    continue  # published only after the window closes: purged
                kept.append({**row, "target_known_at": known.isoformat()})
            out[family] = kept
        return out


def load_pit_panel(
    conn,
    features: tuple[SeriesSpec, ...],
    targets: tuple[TargetSpec, ...],
    *,
    start: date,
    as_of: date,
    as_of_ts: datetime,
) -> PitPanel:
    """Read every declared series once through ``store.observations.read_window``.

    ``conn`` is a SQLAlchemy connection; the caller owns timeouts/read-only
    session settings. One bounded query per series (``series_id`` + date
    window), nothing else.
    """
    if as_of_ts.tzinfo is None:
        raise ValueError("as_of_ts must carry a timezone")
    if not start < as_of:
        raise ValueError("start must precede as_of")
    ids = [s.series_id for s in features]
    if not features or not targets or len(set(ids)) != len(ids):
        raise ValueError("unique non-empty feature and target universe required")
    if len({t.series_id for t in targets}) != len(targets):
        raise ValueError("duplicate target")
    for spec in features:
        if spec.transform not in TRANSFORMS or spec.lag_days < 0 or spec.stale_sessions < 0:
            raise ValueError(f"{spec.series_id}: invalid feature spec")
        if spec.revised:
            raise ValueError(
                f"{spec.series_id}: revised series need a vintage history (not available)"
            )
    for target in targets:
        if target.label not in LABELS or target.lag_days < 0:
            raise ValueError(f"{target.series_id}: invalid target spec")
    for sid in ids + [t.series_id for t in targets]:
        reason = refusal(sid)
        if reason:
            raise ValueError(f"{sid}: {reason}")
    data = {}
    for sid in dict.fromkeys(ids + [t.series_id for t in targets]):
        data[sid] = tuple(
            observations.read_window(
                conn, sid, start=start, as_of=as_of, as_of_ts=as_of_ts
            )
        )
    return PitPanel(
        token=_LOADER,
        features=tuple(features),
        targets=tuple(targets),
        start=start,
        as_of=as_of,
        as_of_ts=as_of_ts,
        data=data,
    )


def verify_pit_rows(panel, protocol, families: dict, window: str) -> None:
    """Refuse PIT rows unless they are exactly what the verified panel derives."""
    if not isinstance(panel, PitPanel):  # a refusal, like every other contract check
        raise ValueError(  # noqa: TRY004
            "pit_vintage_read requires a panel loaded through the vintage-safe read path"
        )
    panel.verify()
    if protocol.pit_receipt != panel.receipt_sha:
        raise ValueError("protocol receipt does not match the vintage-safe panel")
    if tuple(protocol.features) != panel.feature_names():
        raise ValueError("protocol features differ from the panel's declared universe")
    as_of_end = datetime.combine(panel.as_of + timedelta(days=1), datetime.min.time())
    if stamp(protocol.end) > as_of_end.replace(tzinfo=timezone.utc):
        raise ValueError("evaluation window extends past the panel's as_of")
    if digest(families) != digest(panel.family_rows(protocol, window)):
        raise ValueError("PIT rows differ from rows re-derived from the vintage-safe panel")
