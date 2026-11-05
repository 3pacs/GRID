"""Generic sector-relative price panel for panel mode (GD6a).

Mirrors ``analysis.panel_insider_density.load_price_panel`` / ``relative_labels``
without editing them, for any sector, under panel-mode custody:

* closes come only from ``store.observations.read_window`` with an explicit
  ``source="TIINGO"`` (source_catalog 524, ``YF:{ticker}:adj_close``) and the
  frozen ``as_of_ts`` of the key, only for tickers in a frozen admitted-price
  manifest whose digest ``inputs_frozen`` pins;
* no read without a :class:`analysis.panel_mode.PanelDiscoveryKey` /
  :class:`~analysis.panel_mode.PanelHoldoutKey`, and the
  :class:`~analysis.panel_mode.OutcomeWindowGuard` checks the read window first;
* split first: a discovery read never asks for a close on or after the split
  (``as_of <= split - 1 day``), and every close outside the label window is
  blanked before a label is computed;
* XLRE / XLC pre-inception rule (``docs/paper_log/vs1-sectors-v4-preregistration.md``
  section 3): before the benchmark's first admitted close the sessions are a
  declared calendar ticker's, and the benchmark return is the equal-weighted
  mean close-to-close return of the sector's admitted issuers over the same
  sessions. The rank IC is unchanged by the benchmark; only magnitudes differ.

Nothing here writes a DB or is a trading signal.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_mode as pm
from analysis.offline_research_proof import digest
from store import observations

PRICE_SOURCE = "TIINGO"  # source_catalog id 524
PRICE_SOURCE_ID = 524
SERIES_TEMPLATE = "YF:{ticker}:adj_close"
MOMENTUM_SESSIONS = v1.MOMENTUM_SESSIONS
_LOADER = object()


@dataclass(frozen=True)
class PanelPriceManifest:
    """The frozen admitted-price contract of one sector (its digest is pinned in ``inputs_frozen``).

    ``calendar`` is the ticker whose admitted closes define the sessions; it is
    the benchmark unless the benchmark starts late (XLRE, XLC), in which case
    ``benchmark_first_close`` is that first close and, before it, the
    benchmark return is the equal-weighted issuer mean.
    """

    sector: str
    source: str
    series_template: str
    basis: str
    benchmark: str
    calendar: str
    admitted: tuple[str, ...]
    probe_report_sha256: str
    benchmark_first_close: str | None = None

    def validate(self) -> None:
        if self.source != PRICE_SOURCE:
            raise ValueError(f"panel mode reads only the {PRICE_SOURCE} source, not {self.source!r}")
        if self.series_template != SERIES_TEMPLATE:
            raise ValueError(f"panel mode reads only {SERIES_TEMPLATE}")
        if not self.basis:
            raise ValueError("declare the price basis")
        if tuple(sorted(set(self.admitted))) != tuple(self.admitted):
            raise ValueError("admitted tickers are sorted and distinct")
        if self.benchmark not in self.admitted or self.calendar not in self.admitted:
            raise ValueError("benchmark and calendar tickers must be admitted")
        if self.calendar != self.benchmark and self.benchmark_first_close is None:
            raise ValueError("a non-benchmark calendar is only for a benchmark that starts late")
        if self.benchmark_first_close is not None:
            date.fromisoformat(self.benchmark_first_close)
        if not v1._is_hex64(self.probe_report_sha256):
            raise ValueError("probe_report_sha256 must be a sha256 hex digest")

    def digest(self) -> str:
        return digest({**asdict(self), "admitted": list(self.admitted)})


class PanelPrices:
    """Closes read through ``read_window`` for one window, with a receipt (built only by the loader)."""

    def __init__(self, token: object, *, manifest: PanelPriceManifest, start: date, as_of: date,
                 as_of_ts, window: str, data: dict) -> None:
        if token is not _LOADER:
            raise TypeError("PanelPrices is built only by load_panel_prices")
        self.manifest, self.start, self.as_of, self.as_of_ts, self.window = manifest, start, as_of, as_of_ts, window
        self._data = dict(data)
        self.receipt = self._receipt()
        self.receipt_sha = digest(self.receipt)

    def _receipt(self) -> dict:
        return {
            "reader": "store.observations.read_window",
            "manifest": {**asdict(self.manifest), "admitted": list(self.manifest.admitted)},
            "window": self.window, "start": self.start.isoformat(), "as_of": self.as_of.isoformat(),
            "as_of_ts": self.as_of_ts.isoformat(),
            "series": {
                t: {"n": len(obs), "first": obs[0].obs_date.isoformat() if obs else None,
                    "last": obs[-1].obs_date.isoformat() if obs else None,
                    "sha256": digest([[o.obs_date.isoformat(), o.value] for o in obs])}
                for t, obs in sorted(self._data.items())
            },
        }

    def verify(self) -> None:
        if digest(self._receipt()) != self.receipt_sha:
            raise ValueError("price panel changed after its read")

    def closes(self) -> pd.DataFrame:
        """Session x ticker closes; sessions are the calendar ticker's dates."""
        cal = self._data[self.manifest.calendar]
        index = pd.DatetimeIndex([pd.Timestamp(o.obs_date) for o in cal])
        frame = pd.DataFrame(index=index)
        for ticker, obs in self._data.items():
            series = pd.Series([o.value for o in obs], index=pd.DatetimeIndex([pd.Timestamp(o.obs_date) for o in obs]),
                               dtype=float)
            frame[ticker] = series.reindex(index)
        return frame


def load_panel_prices(
    conn,
    manifest: PanelPriceManifest,
    *,
    start: date,
    as_of: date,
    key: pm.PanelDiscoveryKey | pm.PanelHoldoutKey,
    guard: pm.OutcomeWindowGuard,
) -> PanelPrices:
    """Read the sector's whole admitted universe under a key, after the outcome-window guard.

    Discovery stops before the split. The universe is the manifest's (admitted
    minus benchmark and calendar), so a run cannot measure a hand-picked subset.
    """
    manifest.validate()
    pm.require_guard(guard)
    if isinstance(key, pm.PanelDiscoveryKey):
        window = "discovery"
    elif isinstance(key, pm.PanelHoldoutKey):
        window = "holdout"
    else:
        raise PermissionError("no price is read without a PanelDiscoveryKey or PanelHoldoutKey")
    _, run, _ = pm.registered(key.registry)
    lo, hi = run.window_bounds(window)
    if as_of >= hi.date():
        raise PermissionError(f"{window} reads stop before {hi.date()} (split first)")
    if start > as_of:
        raise ValueError("start must not be after as_of")
    if manifest.sector not in run.sectors or run.benchmark(manifest.sector) != manifest.benchmark:
        raise PermissionError("the manifest's sector / benchmark are not the run's")
    pinned = dict(key.inputs.get("price_manifest_sha256s") or [])
    if manifest.digest() != pinned.get(manifest.sector):
        raise PermissionError("price manifest differs from the one inputs_frozen pinned for this sector")
    wanted = sorted(set(manifest.admitted))
    guard.check(manifest.sector, wanted, manifest.benchmark, (start, as_of + timedelta(days=1)))
    data = {}
    for ticker in wanted:
        data[ticker] = tuple(observations.read_window(
            conn, manifest.series_template.format(ticker=ticker), source=manifest.source,
            start=start, as_of=as_of, as_of_ts=key.as_of_ts,
        ))
    if not data[manifest.calendar]:
        raise ValueError("no calendar closes in the read window")
    prices = PanelPrices(_LOADER, manifest=manifest, start=start, as_of=as_of, as_of_ts=key.as_of_ts,
                         window=window, data=data)
    pm.record_prices_read(key, prices.receipt_sha, manifest.digest())
    return prices


def relative_labels(
    closes: pd.DataFrame,
    benchmark: str,
    tickers: list[str],
    horizon: int,
    lo: pd.Timestamp,
    hi: pd.Timestamp,
    *,
    benchmark_first_close: date | None = None,
) -> tuple[list[int], np.ndarray, np.ndarray]:
    """``panel_insider_density.relative_labels`` for explicit window bounds ``[lo, hi)``.

    Every close outside the window is blanked before labelling. Before
    ``benchmark_first_close`` the benchmark's return is the equal-weighted mean
    of the issuers' returns over the same sessions (NaN when none has one).
    """
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    dates = closes.index
    day = pd.DatetimeIndex(dates).tz_localize("UTC")
    inside = np.asarray((day >= lo.normalize()) & (day < hi.normalize()))
    raw = closes[tickers + [benchmark]].to_numpy(dtype=float)
    visible = raw.copy()
    visible[~inside, :] = np.nan
    early = (np.asarray(pd.DatetimeIndex(dates).date < benchmark_first_close)
             if benchmark_first_close is not None else np.zeros(len(dates), dtype=bool))
    positions = [i for i in np.flatnonzero(inside)[::horizon] if i + horizon < len(dates) and inside[i + horizon]]

    def bench_return(a: np.ndarray, b: np.ndarray, issuers: np.ndarray, at: int) -> float:
        if early[at]:
            finite = issuers[np.isfinite(issuers)]
            return float(finite.mean()) if len(finite) else np.nan
        return b[-1] / a[-1] - 1.0

    labels, momentum = [], []
    for i in positions:
        start, end = visible[i], visible[i + horizon]
        with np.errstate(divide="ignore", invalid="ignore"):
            issuer = end[:-1] / start[:-1] - 1.0
            issuer[~np.isfinite(issuer)] = np.nan
            rel = issuer - bench_return(start, end, issuer, i)
        rel[~np.isfinite(rel)] = np.nan
        labels.append(rel)
        if i >= MOMENTUM_SESSIONS:
            past, now = raw[i - MOMENTUM_SESSIONS], raw[i]
            with np.errstate(divide="ignore", invalid="ignore"):
                issuer_m = now[:-1] / past[:-1] - 1.0
                issuer_m[~np.isfinite(issuer_m)] = np.nan
                mom = issuer_m - bench_return(past, now, issuer_m, i - MOMENTUM_SESSIONS)
            mom[~np.isfinite(mom)] = np.nan
        else:
            mom = np.full(len(tickers), np.nan)
        momentum.append(mom)
    shape = (len(positions), len(tickers))
    return positions, np.array(labels).reshape(shape), np.array(momentum).reshape(shape)


def build_label_panel(
    prices: PanelPrices,
    run: pm.PanelRunSpec,
    construct: pm.ConstructSpec,
    features: pd.DataFrame,
    horizon: int,
) -> pm.TrialPanel:
    """One (sector, horizon) trial panel from verified prices and a frozen feature frame.

    ``features`` is decision-instant (UTC) x ticker, as read from a frozen
    artifact; a decision with no feature row abstains. The feature also
    abstains where the issuer has no close at the decision. The entities are
    the manifest's whole universe (:func:`analysis.panel_mode.universe`).
    """
    prices.verify()
    closes = prices.closes()
    manifest = prices.manifest
    tickers = pm.universe(manifest)
    lo, hi = run.window_bounds(prices.window)
    first = date.fromisoformat(manifest.benchmark_first_close) if manifest.benchmark_first_close else None
    positions, labels, momentum = relative_labels(closes, manifest.benchmark, tickers, horizon, lo, hi,
                                                  benchmark_first_close=first)
    decided = v1.decision_instants([closes.index[i].date() for i in positions])
    ends = v1.decision_instants([closes.index[i + horizon].date() for i in positions])
    frame = features.reindex(index=decided, columns=tickers)
    feature = frame.to_numpy(dtype=float)
    listed = (np.isfinite(closes[tickers].to_numpy(dtype=float)[positions]) if positions
              else np.zeros((0, len(tickers)), bool))
    feature = np.where(listed, feature, np.nan)
    trial = f"{construct.name}|fwd{horizon}"
    return pm.TrialPanel(trial=trial, window=prices.window, horizon=horizon,
                         decision_at=[d.isoformat() for d in decided], label_end=[d.isoformat() for d in ends],
                         entities=tickers, feature=feature, label=labels, momentum=momentum)


__all__ = ["PanelPriceManifest", "PanelPrices", "load_panel_prices", "relative_labels", "build_label_panel",
           "PRICE_SOURCE", "SERIES_TEMPLATE"]
