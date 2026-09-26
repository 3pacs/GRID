"""Read-only latest-vintage panel adapter for ``analysis.offline_research_proof`` (S09/S09b).

What it does
------------
Reads a small declared universe of ``raw_series`` ids **only** through
``store.observations.read_window`` (``SUCCESS`` rows only, one row per
``obs_date`` = latest vintage, nothing observed after ``as_of``, nothing pulled
after ``as_of_ts``) and turns it into the feature frame, per-value known-at
stamps and target levels the research contract labels. It never writes, never
builds SQL of its own, and never reads ``discovered_hypotheses``,
``hypothesis_registry`` or any other table.

Why a distinct origin, and why it is not called point-in-time
-------------------------------------------------------------
``exploratory_replay`` is a self-declared label that accepts any latest-vintage
CSV. The ``latest_vintage_read`` origin is different: ``load_latest_vintage_panel``
is the only way to build a :class:`LatestVintagePanel`; the panel carries a
receipt (reader, ``as_of``/``as_of_ts``, declared specs, publication schedules,
proxy groups and a sha256 over every observation it read); the protocol must
carry that receipt's hash; and the contract (``discover``/``evaluate_holdout``)
re-derives every row from the verified panel via
:func:`verify_latest_vintage_rows` and refuses rows that differ.

It was called ``pit_vintage_read`` until S09b. That overclaimed: ``raw_series``
keeps the latest vintage per date, and on griddb every row of the S09 universe
was pulled on or after 2026-03-24 (backfill). ``as_of_ts`` makes the read
reproducible and excludes later pulls, but values for 2004-2025 are
latest-vintage hindsight, not first releases.

Availability (known-at) rules (S09b)
------------------------------------
* Every series declares a publication ``source`` (:data:`PUBLICATIONS`): an
  observation dated ``d`` is known at ``d + lag`` (business days on the US
  federal holiday calendar, or calendar days) at the source's declared UTC
  time of day, conservative where the time is not verified.
* Decisions are taken at 00:00Z of each session, so a value is usable at the
  first session at or after its known-at stamp, then carried forward at most
  ``stale_sessions`` sessions before abstaining. Each feature value carries
  that stamp as its ``known_at``; the contract refuses one later than its
  decision.
* A target level is the observation dated exactly on the session (no carry).
  Its forward label ends at session ``t + h`` and is known at the declared
  publication stamp of the observation dated ``t + h``; a label not known
  inside its window is dropped (it cannot cross the holdout boundary by
  publication lag either).
* Sessions are business days (``pd.bdate_range``), not an exchange calendar:
  a holiday is a session with no observation (features carry, targets drop).

Refusals (enforced here, not caller-declared)
---------------------------------------------
* ``snap:*``, LLM/telemetry counters and astro/celestial ids.
* yfinance ids (``YF:``/``YF_ADJ:``): historical ``YF:*:close`` rows carry more
  than one close per date (S07/#642 price-basis contamination).
* Series with material revisions (:data:`REVISED_SERIES`,
  :data:`REVISED_PREFIXES`): a latest-vintage read of them is hindsight.
* A target without a declared proxy group (:data:`PROXY_GROUPS`).

Proxy groups (S09b)
-------------------
Each target declares the series that are its own value or a near-copy of it.
Every (family, feature) trial whose feature series is in the family target's
group is a ``self_lag`` trial: measured for the record, never selectable,
never a candidate. The pairs are derived here from code, and the contract
refuses a protocol whose ``self_lag`` differs.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

from analysis.offline_research_proof import (
    LABELS,
    LATEST_VINTAGE_ORIGIN,
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

# --- revised-series denylist (S09b) ----------------------------------------------
# A latest-vintage read of these is hindsight: their history is re-estimated or
# revised after first release. Maintained by hand from publisher revision
# policies (sources below); ALFRED (alfred.stlouisfed.org) vintage counts are
# the check before any id is removed. Absence from this list is not proof a
# series is never revised -- H.15, ICE BofA and CBOE closes are treated as
# unrevised by their publishers' practice.
REVISED_SERIES = frozenset(
    {
        # Chicago Fed: CFNAI re-estimated monthly
        "CFNAI",
        "CFNAIMA3",
        # Kansas City Fed financial stress index: re-estimated monthly
        "KCFSI",
        # Dallas/NY Fed Weekly Economic Index: revised weekly
        "WEI",
        # DOL claims: advance -> revised next week, annual seasonal-factor revision
        "ICSA",
        "ICNSA",
        "CCSA",
        "CCNSA",
        "IC4WSA",
        "IURSA",
        # BLS CES/CPS: monthly revisions, annual benchmark and seasonal factors
        "PAYEMS",
        "UNRATE",
        "CIVPART",
        "CES0500000003",
        "AHETPI",
        "JTSJOL",
        # BLS CPI (seasonal factors revised) and BEA PCE prices / NIPA
        "CPIAUCSL",
        "CPILFESL",
        "PCEPI",
        "PCEPILFE",
        "PCE",
        "DSPIC96",
        "PSAVERT",
        "GDP",
        "GDPC1",
        # Census / Fed G.17 activity: revised with later months and benchmarks
        "INDPRO",
        "TCU",
        "RSAFS",
        "RSXFS",
        "RRSFS",
        "HOUST",
        "PERMIT",
        "DGORDER",
        "NEWORDER",
        # Fed H.6 money stock, G.19 consumer credit, H.8 bank credit: revised
        "M1SL",
        "M2SL",
        "WM2NS",
        "TOTALSL",
        "BUSLOANS",
        "TOTBKCR",
        # University of Michigan sentiment: preliminary -> final
        "UMCSENT",
        # Fed broad dollar index: history revised when trade weights are updated
        "DTWEXBGS",
        "DTWEXAFEGS",
        "DTWEXEMEGS",
    }
)
# Whole families whose entire history is re-estimated each release.
REVISED_PREFIXES = (
    "NFCI",  # Chicago Fed NFCI and sub-indices: whole history re-estimated weekly
    "ANFCI",  # adjusted NFCI
    "STLFSI",  # St. Louis Fed financial stress indices: re-estimated weekly
)
REVISED_SOURCES = (
    "Chicago Fed NFCI/CFNAI methodology notes (history re-estimated each release)",
    "St. Louis Fed STLFSI and Kansas City Fed KCFSI notes (re-estimated)",
    "DOL weekly claims release (advance vs revised; annual seasonal factors)",
    "BLS CES/CPS/CPI/JOLTS revision and benchmark policies",
    "BEA NIPA revision schedule; Census M3/retail/housing revisions",
    "Federal Reserve G.17, H.6, H.8, G.19 and H.10 broad-index weight updates",
    "University of Michigan preliminary vs final sentiment",
    "ALFRED vintage history is the check before removing any id",
)

# --- publication schedules (S09b) ------------------------------------------------

_HOLIDAYS = (
    USFederalHolidayCalendar()
    .holidays("1990-01-01", "2040-12-31")
    .to_numpy()
    .astype("datetime64[D]")
)


@dataclass(frozen=True)
class Publication:
    """An observation dated ``d`` is known at ``d + lag`` (``unit``) at ``time_utc``."""

    lag: int
    unit: str  # "business" (US federal holiday calendar) or "calendar"
    time_utc: str  # "HH:MM", conservative where not verified
    basis: str


PUBLICATIONS: dict[str, Publication] = {
    "FRB_H15": Publication(
        1,
        "business",
        "21:17",
        "H.15 (DGS*, DFII*, DFF): next business day ~20:17Z in EDT (reviewer-verified); "
        "+1 h so EST is covered",
    ),
    "FRED_H15_SPREAD": Publication(
        1,
        "business",
        "23:59",
        "FRED-computed spreads/breakevens from H.15 legs (T10Y2Y, T10Y3M, T10YIE, "
        "T5YIE): next business day, posted after H.15; time not verified -> end of day",
    ),
    "ICE_BOFA": Publication(
        1,
        "business",
        "23:59",
        "ICE BofA OAS on FRED: next business day; time not verified -> end of day",
    ),
    "CBOE_VIX": Publication(
        1,
        "business",
        "23:59",
        "VIXCLS on FRED: next business day; time not verified -> end of day",
    ),
    "FRB_H10": Publication(
        8,
        "calendar",
        "21:15",
        "H.10 posts a week of daily rates on Monday ~16:15 ET; 8 calendar days covers "
        "the oldest day of that week plus a Monday holiday (reviewer-verified lag 8)",
    ),
    "FRB_H41": Publication(
        2,
        "calendar",
        "21:30",
        "H.4.1: Wednesday level released Thursday 16:30 ET; lag 2 covers a "
        "holiday-shifted Friday release (reviewer-verified lag 2)",
    ),
    "NYFED_RRP": Publication(
        1,
        "business",
        "23:59",
        "RRPONTSYD on FRED: next business day; time not verified -> end of day",
    ),
    "FREDDIE_PMMS": Publication(
        1,
        "calendar",
        "17:00",
        "Freddie Mac PMMS: Thursday-dated, released Thursday 12:00 ET; stamped the "
        "next day at 17:00Z (reviewer-verified lag 1)",
    ),
    "AAII": Publication(
        1,
        "calendar",
        "23:59",
        "AAII sentiment: Thursday-dated survey pulled Friday (reviewer-verified lag 1); "
        "time not verified -> end of day",
    ),
}


def publication_times(dates, publication: Publication) -> pd.DatetimeIndex:
    """When observations dated ``dates`` became known (UTC), per ``publication``."""
    days = np.asarray([np.datetime64(d, "D") for d in dates], dtype="datetime64[D]")
    if publication.unit == "business":
        # Saturday + 1 business day = Monday; a holiday-dated obs rolls back first.
        days = np.busday_offset(days, publication.lag, roll="backward", holidays=_HOLIDAYS)
    elif publication.unit == "calendar":
        days = days + np.timedelta64(publication.lag, "D")
    else:
        raise ValueError(f"unknown publication unit {publication.unit}")
    hours, minutes = (int(part) for part in publication.time_utc.split(":"))
    return pd.DatetimeIndex(days).tz_localize("UTC") + pd.Timedelta(
        hours=hours, minutes=minutes
    )


# --- proxy groups (S09b) ---------------------------------------------------------
# Rule (checked by required_proxies against the declared universe at load):
#   (a) the target's own series and its legs (SPREAD_LEGS; a level is its own leg);
#   (b) every series sharing a leg with the target;
#   (c) for a spread target, the other leg of every spread that shares a leg
#       with it (T10YIE + DFII10 rebuild the 10-year leg of T10Y2Y);
#   (d) for each Treasury leg, the adjacent quoted tenors on TREASURY_TENORS
#       and, where an adjacent tenor is absent from the universe, the nearest
#       tenor present on that side;
#   (e) by declaration: rating sub-indices and yield/total-return variants of the
#       same credit index, and other indices of the same implied-vol family.
# Keyed by target series id, not by series equality: a target without a
# declared group is refused, and so is a group missing a required member.
SPREAD_LEGS: dict[str, tuple[str, str]] = {
    "T10Y2Y": ("DGS10", "DGS2"),
    "T10Y3M": ("DGS10", "DGS3MO"),
    "T10Y1Y": ("DGS10", "DGS1"),
    "T10YIE": ("DGS10", "DFII10"),  # 10-year breakeven = nominal - real
    "T5YIE": ("DGS5", "DFII5"),
}
TREASURY_TENORS = (
    "DGS1MO",
    "DGS3MO",
    "DGS6MO",
    "DGS1",
    "DGS2",
    "DGS3",
    "DGS5",
    "DGS7",
    "DGS10",
    "DGS20",
    "DGS30",
)
PROXY_GROUPS: dict[str, frozenset[str]] = {
    "VIXCLS": frozenset({"VIXCLS", "VXVCLS", "VXOCLS", "VIX3M", "VIX9D"}),
    "DGS2": frozenset(
        {
            "DGS2",
            # adjacent tenors; DGS5 is the nearest longer tenor in the universe
            "DGS1",
            "DGS3",
            "DGS5",
            # spreads with a 2-year leg
            "T10Y2Y",
        }
    ),
    "T10Y2Y": frozenset(
        {
            "T10Y2Y",
            # legs
            "DGS10",
            "DGS2",
            # adjacent tenors of the legs, and the nearest ones in the universe
            "DGS7",
            "DGS20",
            "DGS1",
            "DGS3",
            "DGS5",
            "DGS30",
            # spreads sharing the 10-year leg
            "T10Y3M",
            "T10Y1Y",
            "T10YIE",
            # their other legs
            "DGS3MO",
            "DFII10",
        }
    ),
    "BAMLH0A0HYM2": frozenset(
        {
            "BAMLH0A0HYM2",
            # rating sub-indices of the US HY master
            "BAMLH0A1HYBB",
            "BAMLH0A2HYB",
            "BAMLH0A3HYC",
            # yield / total-return versions of the same indices
            "BAMLH0A0HYM2EY",
            "BAMLH0A1HYBBEY",
            "BAMLH0A2HYBEY",
            "BAMLH0A3HYCEY",
            "BAMLHYH0A0HYM2TRIV",
        }
    ),
}

_LOADER = object()  # capability: only load_latest_vintage_panel may build a panel


@dataclass(frozen=True)
class SeriesSpec:
    """A feature series: ``chg5``/``chg20`` as a difference or a percent change, plus ``z60``."""

    series_id: str
    transform: str = "diff"
    source: str = "FRB_H15"  # key into PUBLICATIONS
    stale_sessions: int = 5  # carry-forward limit before an explicit abstention


@dataclass(frozen=True)
class TargetSpec:
    """A target level labelled over ``horizon`` sessions as a ``change`` or ``return``."""

    series_id: str
    label: str = "change"
    source: str = "FRB_H15"


def revised(series_id: str) -> bool:
    return series_id in REVISED_SERIES or series_id.startswith(REVISED_PREFIXES)


def refusal(series_id: str) -> str | None:
    """Why ``series_id`` may not enter the S09 universe, or ``None``."""
    low = series_id.lower()
    if any(token in low for token in EXCLUDED_TOKENS):
        return "excluded: internal telemetry, LLM counter or astro/celestial"
    if series_id.startswith(UNVERIFIED_PRICE_PREFIXES):
        return "refused: yfinance price basis not verified single-valued per date"
    if revised(series_id):
        return "refused: materially revised series need a vintage history (not available)"
    return None


def proxy_group(target_id: str, universe=None) -> frozenset[str]:
    """The declared group; with ``universe``, also refuse one missing a rule member."""
    group = PROXY_GROUPS.get(target_id)
    if group is None or target_id not in group:
        raise ValueError(f"{target_id}: refused: no declared proxy group for this target")
    if universe is not None:
        missing = required_proxies(target_id, universe) - group
        if missing:
            raise ValueError(
                f"{target_id}: refused: proxy group lacks rule members {sorted(missing)}"
            )
    return group


def legs(series_id: str) -> tuple[str, ...]:
    return SPREAD_LEGS.get(series_id, (series_id,))


def _neighbour_tenors(leg: str, universe: frozenset[str]) -> set[str]:
    """Adjacent quoted tenors of ``leg`` and, per side, the nearest in the universe."""
    if leg not in TREASURY_TENORS:
        return set()
    i = TREASURY_TENORS.index(leg)
    out = set()
    for side in (TREASURY_TENORS[:i][::-1], TREASURY_TENORS[i + 1 :]):
        if side:
            out.add(side[0])
            out.update(next(([t] for t in side if t in universe), []))
    return out


def required_proxies(target_id: str, universe) -> frozenset[str]:
    """Members rules (a)-(d) require in ``target_id``'s group, given ``universe``."""
    universe = frozenset(universe)
    own = set(legs(target_id))
    required = {target_id, *own}
    for sid in universe | set(SPREAD_LEGS):
        if own & set(legs(sid)):
            required.add(sid)  # (b) shares a leg
            if target_id in SPREAD_LEGS:
                required.update(legs(sid))  # (c) the other leg of that spread
    for leg in own:
        required |= _neighbour_tenors(leg, universe)  # (d)
    return frozenset(required)


def self_lag_pairs(
    families: tuple[str, ...], feature_names: tuple[str, ...]
) -> tuple[tuple[str, str], ...]:
    """(family, feature) trials whose feature series proxies the family's target."""
    pairs = []
    for family in families:
        group = proxy_group(family.rsplit("|", 2)[0])
        pairs.extend(
            (family, name) for name in feature_names if name.rsplit("|", 1)[0] in group
        )
    return tuple(pairs)


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _observation_record(o: observations.Observation) -> list:
    return [o.obs_date.isoformat(), o.value, _utc(o.pull_timestamp).isoformat()]


class LatestVintagePanel:
    """Observations read through ``store.observations.read_window``, with a receipt.

    Built only by :func:`load_latest_vintage_panel`. Immutable by convention;
    ``verify`` recomputes the receipt from the held observations, so a changed
    value, date or pull timestamp is detected.
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
            raise TypeError("LatestVintagePanel is built only by load_latest_vintage_panel")
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
            if spec.source not in PUBLICATIONS:
                raise ValueError(f"{spec.series_id}: undeclared publication source")
        universe = {s.series_id for s in (*self.features, *self.targets)}
        for target in self.targets:
            proxy_group(target.series_id, universe)
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
                    raise ValueError(f"{sid}: observation outside the latest-vintage read")
                previous = o.obs_date
        # Proxy groups, schedules and the denylist are code, frozen in the receipt.

    def _receipt(self) -> dict:
        self._check()
        sources = sorted({s.source for s in (*self.features, *self.targets)})
        return {
            "reader": READER,
            "origin": LATEST_VINTAGE_ORIGIN,
            "vintage": "latest vintage per obs_date (hindsight), not first release",
            "start": self.start.isoformat(),
            "as_of": self.as_of.isoformat(),
            "as_of_ts": self.as_of_ts.isoformat(),
            "features": [asdict(s) for s in self.features],
            "targets": [asdict(t) for t in self.targets],
            "publications": {k: asdict(PUBLICATIONS[k]) for k in sources},
            "proxy_groups": {
                t.series_id: sorted(proxy_group(t.series_id)) for t in self.targets
            },
            "revised_denylist_sha256": digest(
                [sorted(REVISED_SERIES), list(REVISED_PREFIXES)]
            ),
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
            raise ValueError("latest-vintage panel changed after its read")

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

    def self_lag(self, families: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
        """The proxy trials for ``families`` over this panel's features."""
        return self_lag_pairs(tuple(families), self.feature_names())

    def _available(
        self, spec: SeriesSpec, index: pd.DatetimeIndex
    ) -> tuple[pd.Series, pd.Series]:
        """Level as known at each session and when it became known.

        Only published observations: an observation is usable from the first
        session at or after its publication stamp, carried <= ``stale_sessions``.
        """
        level = pd.Series(np.nan, index=index)
        known = pd.Series(pd.NaT, index=index, dtype="datetime64[ns, UTC]")
        obs = self._data[spec.series_id]
        if not obs:
            return level, known
        published = publication_times(
            [o.obs_date for o in obs], PUBLICATIONS[spec.source]
        )
        position = index.searchsorted(published, side="left")
        values = np.array([o.value for o in obs], dtype=float)
        keep = position < len(index)
        # Several obs published by the same session: the newest obs_date wins.
        latest = (
            pd.DataFrame(
                {"value": values[keep], "known": published[keep]},
                index=position[keep],
            )
            .groupby(level=0)
            .last()
        )
        where = latest.index.to_numpy()
        level.iloc[where] = latest["value"].to_numpy()
        known.iloc[where] = latest["known"].to_numpy()
        level = level.ffill(limit=spec.stale_sessions)
        known = known.ffill(limit=spec.stale_sessions)
        if not known.dropna().is_monotonic_increasing:
            raise ValueError(f"{spec.series_id}: publication stamps are not monotone")
        return level, known

    def _available_level(self, spec: SeriesSpec, index: pd.DatetimeIndex) -> pd.Series:
        return self._available(spec, index)[0]

    def _frames(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        index = self.session_index()
        columns, stamps = {}, {}
        for spec in self.features:
            x, known = self._available(spec, index)
            for n in (5, 20):
                name = f"{spec.series_id}|chg{n}"
                columns[name] = x / x.shift(n) - 1 if spec.transform == "pct" else x - x.shift(n)
                stamps[name] = known  # monotone: the window's latest stamp is today's
            mean, std = x.rolling(60).mean(), x.rolling(60).std()
            columns[f"{spec.series_id}|z60"] = (x - mean) / std
            stamps[f"{spec.series_id}|z60"] = known
        names = list(self.feature_names())
        frame = pd.DataFrame(columns, index=index)[names]
        # inf (pct change from 0, zero rolling std) is not an observation: abstain.
        frame = frame.replace([np.inf, -np.inf], np.nan)
        return frame, pd.DataFrame(stamps, index=index)[names]

    def feature_frame(self) -> pd.DataFrame:
        return self._frames()[0]

    def feature_known_at(self) -> pd.DataFrame:
        """When each feature value became known (NaT where the value abstains)."""
        frame, known = self._frames()
        return known.where(frame.notna())

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
        """Rows for every declared family, stamped with declared publication times."""
        index = self.session_index()
        features, known_at = self._frames()
        known_at = known_at.where(features.notna())
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
                known_at=known_at,
            )
            ends = [pd.Timestamp(row["label_end"]).date() for row in rows]
            published = publication_times(ends, PUBLICATIONS[target.source])
            kept = []
            for row, known in zip(rows, published):
                if known.to_pydatetime() >= bound:
                    continue  # published only after the window closes: purged
                kept.append({**row, "target_known_at": known.isoformat()})
            out[family] = kept
        return out


def load_latest_vintage_panel(
    conn,
    features: tuple[SeriesSpec, ...],
    targets: tuple[TargetSpec, ...],
    *,
    start: date,
    as_of: date,
    as_of_ts: datetime,
) -> LatestVintagePanel:
    """Read every declared series once through ``store.observations.read_window``.

    ``conn`` is a SQLAlchemy connection; the caller owns timeouts/read-only
    session settings. One bounded query per series (``series_id`` + date
    window), nothing else. Refusals (denylists, sources, proxy groups) are
    checked before any read.
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
        if spec.transform not in TRANSFORMS or spec.stale_sessions < 0:
            raise ValueError(f"{spec.series_id}: invalid feature spec")
    for target in targets:
        if target.label not in LABELS:
            raise ValueError(f"{target.series_id}: invalid target spec")
    for sid in ids + [t.series_id for t in targets]:
        reason = refusal(sid)
        if reason:
            raise ValueError(f"{sid}: {reason}")
    for spec in (*features, *targets):
        if spec.source not in PUBLICATIONS:
            raise ValueError(f"{spec.series_id}: undeclared publication source")
    for target in targets:
        proxy_group(target.series_id, ids + [t.series_id for t in targets])
    data = {}
    for sid in dict.fromkeys(ids + [t.series_id for t in targets]):
        data[sid] = tuple(
            observations.read_window(
                conn, sid, start=start, as_of=as_of, as_of_ts=as_of_ts
            )
        )
    return LatestVintagePanel(
        token=_LOADER,
        features=tuple(features),
        targets=tuple(targets),
        start=start,
        as_of=as_of,
        as_of_ts=as_of_ts,
        data=data,
    )


def verify_latest_vintage_rows(panel, protocol, families: dict, window: str) -> None:
    """Refuse rows unless they are exactly what the verified panel derives."""
    if not isinstance(panel, LatestVintagePanel):  # a refusal, like every other check
        raise ValueError(  # noqa: TRY004
            "latest_vintage_read requires a panel loaded through store.observations.read_window"
        )
    panel.verify()
    if protocol.read_receipt != panel.receipt_sha:
        raise ValueError("protocol receipt does not match the latest-vintage panel")
    if tuple(protocol.features) != panel.feature_names():
        raise ValueError("protocol features differ from the panel's declared universe")
    if tuple(protocol.self_lag) != panel.self_lag(protocol.families):
        raise ValueError("protocol self_lag differs from the declared proxy groups")
    as_of_end = datetime.combine(panel.as_of + timedelta(days=1), datetime.min.time())
    if stamp(protocol.end) > as_of_end.replace(tzinfo=timezone.utc):
        raise ValueError("evaluation window extends past the panel's as_of")
    if digest(families) != digest(panel.family_rows(protocol, window)):
        raise ValueError("rows differ from rows re-derived from the latest-vintage panel")


# --- relabelling of pre-S09b frozen candidates -----------------------------------


def relabel_frozen_candidates(candidates: list[dict]) -> dict:
    """Mark pre-S09b frozen candidates as SELF_LAG proxy or cross-series.

    Pure: the original receipts are never altered. A SELF_LAG candidate is a
    target predicting itself or a declared near-copy and is never a candidate;
    a cross-series one remains only under the proxy rule and is marked
    ``RESCAN_REQUIRED`` -- its run predates the S09b publication-time known_at
    and data-driven block fixes, so it may not be forward-logged as is.
    """
    relabelled = []
    for candidate in candidates:
        spec = candidate["specification"]
        target = spec["family"].rsplit("|", 2)[0]
        feature_series = spec["feature"].rsplit("|", 1)[0]
        group = proxy_group(target)
        is_self_lag = feature_series in group
        relabelled.append(
            {
                "original_sha256": candidate["sha256"],
                "family": spec["family"],
                "feature": spec["feature"],
                "direction": spec["direction"],
                "target_series": target,
                "feature_series": feature_series,
                "proxy_label": "SELF_LAG" if is_self_lag else "CROSS_SERIES",
                "proxy_group": sorted(group),
                "remains_candidate_under_proxy_rule": not is_self_lag,
                "original_state": candidate["state"],
                "state": "SELF_LAG_NEVER_A_CANDIDATE" if is_self_lag else "RESCAN_REQUIRED",
                "promotion_allowed": False,
                "origin_relabel": {
                    "recorded": spec.get("origin"),
                    "correct": LATEST_VINTAGE_ORIGIN,
                },
            }
        )
    return {
        "relabelled": relabelled,
        "counts": {
            "total": len(relabelled),
            "self_lag": sum(r["proxy_label"] == "SELF_LAG" for r in relabelled),
            "cross_series": sum(r["proxy_label"] == "CROSS_SERIES" for r in relabelled),
        },
        "proxy_groups": {k: sorted(v) for k, v in sorted(PROXY_GROUPS.items())},
        "notes": [
            "Original receipts are unchanged; this file is a relabelled copy.",
            (
                "The data is latest-vintage hindsight (origin latest_vintage_read), "
                "not point-in-time."
            ),
            (
                "The run predates S09b: its feature known_at was 00:00Z of the session "
                "(before H.15 publication) and its null used block 1. A cross-series "
                "candidate is RESCAN_REQUIRED: it needs a new scan under S09b before "
                "any forward logging."
            ),
            "promotion_allowed stays false for every entry.",
        ],
    }
