"""Explicit price-series contract for the versioned signal evaluator (W3d).

This module answers one question, honestly: *for a given instrument, which
``feature_registry``/``resolved_series`` rows are its daily close prices,
and where did that mapping come from?* ``evaluation/signal_outcomes.py``'s
previous default accessor (``make_default_price_accessor``, before this
change) answered that question by guessing feature names
(``instrument``, ``f"{instrument}_CLOSE"``, ``f"{instrument}_close"``) and
taking whichever one the database happened to have — never distinguishing
"resolved by an explicit rule" from "got lucky with a string match", and
silently returning the first row on a ``LIMIT 1`` if more than one guess
matched. This module replaces that with:

  * :class:`PriceSeriesContract` — a resolver from instrument -> the exact
    ``feature_registry``/``source_catalog`` row that backs its close price,
    using the SAME candidate-name rule the GRID app itself uses to look up
    a ticker's close (``api/routers/watchlist_helpers.py::_resolve_feature_names``,
    lines 393-426), confirmed against the database with the SAME join
    condition the app's own ticker-quote endpoint uses
    (``api/routers/watchlist_overview.py::get_ticker_quote``, the
    ``fr.name = ANY(:names)`` condition at lines 418-429) — never a fresh
    guess of our own. Unlike that endpoint (which takes ``LIMIT 1`` with no
    tie-break, so it is not even deterministic which row wins when more
    than one candidate name is registered), this resolver treats more than
    one match as :class:`AmbiguousInstrumentError` and refuses to pick.

  * :class:`PITPriceAccessor` — the PIT-safe accessor callable that
    ``evaluation/signal_outcomes.py`` actually calls. It resolves the
    instrument once via the contract, then reads the price through
    ``store/pit.py::PITStore.get_pit`` (``LATEST_AS_OF`` policy, so it is
    capped at ``as_of`` exactly the way every other PIT-correct read in
    GRID is), and returns a :class:`PricePoint` carrying full provenance
    (``obs_date``, ``value``, ``release_date``, ``vintage_date``,
    ``source_ref``) plus an explicit sanity flag rather than silently
    dropping an out-of-bounds price.

Explicit failure modes (never a silent guess):
  * Instrument not mapped to any registered feature -> ``UnsupportedInstrumentError``.
  * More than one registered feature matches the candidate names ->
    ``AmbiguousInstrumentError`` (a subclass of ``UnsupportedInstrumentError``).
  * ``yfinance`` is not itself registered in ``source_catalog`` (so the
    daily-bar convention this contract assumes cannot be confirmed) ->
    ``UnsupportedInstrumentError``.
  * A resolved price outside ``sanity_bounds`` -> returned (not raised,
    not dropped) with ``sanity_ok=False`` and ``sanity_reason`` set, so
    ``evaluate_signal`` can mark the signal ``INELIGIBLE(price_sanity)``
    from real data rather than a crash or a silently-accepted bad print.

See ``docs/reference/PRICE_SERIES_CONTRACT.md`` for the full contract
write-up, including the known gap this module does NOT solve (no
``unit``/currency column exists anywhere in ``feature_registry`` or
``source_catalog`` today; ``unit`` is a documented, hard-coded assumption,
not something read from the database).

This module is import-light and DB-optional by design (mirrors
``evaluation/signal_outcomes.py``'s own style) — nothing here talks to a
database at import time, and nothing here is wired into Hermes, the
scheduler, or any production read/write path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


class UnsupportedInstrumentError(Exception):
    """Raised when an instrument cannot be resolved to a price series.

    Covers both "no feature_registry row matched any candidate name" and
    "the source this contract assumes (yfinance) is not registered in
    source_catalog". ``reason`` is a short, human-readable explanation —
    never a guess dressed up as an answer.
    """

    def __init__(self, instrument: str, reason: str) -> None:
        self.instrument = instrument
        self.reason = reason
        super().__init__(f"{instrument}: {reason}")


class AmbiguousInstrumentError(UnsupportedInstrumentError):
    """Raised when more than one feature_registry row matches.

    ``candidates`` lists every (feature_id, feature_name) pair that
    matched, so the caller can see exactly what was ambiguous. This is
    deliberately NOT resolved by picking the first row (that is the bug
    this module exists to remove — see module docstring) — an operator
    must add or fix an explicit mapping instead.
    """

    def __init__(self, instrument: str, candidates: list[tuple[int, str]]) -> None:
        self.candidates = candidates
        names = ", ".join(f"{name!r} (id={fid})" for fid, name in candidates)
        super().__init__(
            instrument,
            f"{len(candidates)} feature_registry rows matched candidate names, "
            f"never picking one silently: {names}",
        )


# ---------------------------------------------------------------------------
# The bar convention this contract resolves
# ---------------------------------------------------------------------------

# The only source this contract currently understands. Daily OHLCV bars
# land in raw_series under series_id f"YF:{ticker}:{field}" (see
# ingestion/yfinance_pull.py:103 SOURCE_NAME and :199-203 the series_id
# convention), get merged into resolved_series by feature_id
# (normalization/resolver.py), and this contract is scoped to the "close"
# field of that convention specifically — see PRICE_SERIES_CONTRACT.md.
BAR_SOURCE_NAME = "yfinance"
BAR_KIND = "close"

# No currency/unit column exists in feature_registry or source_catalog
# today (confirmed against schema.sql). This is a documented assumption,
# not a database read — see docs/reference/PRICE_SERIES_CONTRACT.md.
DEFAULT_UNIT = "price"

# Same default sanity window evaluate_signal() itself uses
# (evaluation/signal_outcomes.py::evaluate_signal's `sanity_bounds` default).
DEFAULT_SANITY_BOUNDS: tuple[float, float] = (0.0, 1_000_000.0)

REASON_PRICE_OUT_OF_BOUNDS = "price_outside_sanity_bounds"


# ---------------------------------------------------------------------------
# Resolved descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeriesDescriptor:
    """How one instrument maps onto a price series. The contract's answer."""

    instrument: str
    feature_id: int
    feature_name: str
    series_id: str  # raw ingestion series_id, e.g. "YF:AAPL:close"
    source_catalog_id: int
    source_catalog_name: str
    bar_kind: str
    unit: str


# ---------------------------------------------------------------------------
# Price point (PIT-safe, full provenance)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PricePoint:
    """A single PIT-safe priced bar, with full provenance.

    ``price``/``bar_date``/``basis`` are read-only aliases for
    ``value``/``obs_date``/``basis`` so this is a drop-in
    ``evaluation.signal_outcomes.PricePoint`` for anything that duck-types
    on those three attributes (i.e. ``evaluate_signal`` itself) — no
    adapter object required.
    """

    obs_date: date
    value: float
    release_date: date
    vintage_date: date
    source_ref: str
    basis: str = BAR_KIND
    sanity_ok: bool = True
    sanity_reason: Optional[str] = None

    @property
    def price(self) -> float:
        return self.value

    @property
    def bar_date(self) -> date:
        return self.obs_date


# ---------------------------------------------------------------------------
# Candidate-name rule — imported lazily so this module stays DB-optional
# and import-light (mirrors evaluation/signal_outcomes.py's own local
# `from sqlalchemy import text` style).
# ---------------------------------------------------------------------------


def _default_candidate_names(instrument: str) -> list[str]:
    """The app's own ticker-close candidate-name rule.

    Delegates to ``api.routers.watchlist_helpers._resolve_feature_names``
    (lines 393-426) — the exact function
    ``api/routers/watchlist_overview.py::get_ticker_quote`` (lines 398,
    418-429) uses to build the ``:names`` list for its
    ``fr.name = ANY(:names)`` lookup. Reused verbatim, not reimplemented,
    so this contract can never drift from the app's own rule.
    """
    from api.routers.watchlist_helpers import _resolve_feature_names

    return _resolve_feature_names(instrument)


CandidateNameFn = Callable[[str], list[str]]


# ---------------------------------------------------------------------------
# PriceSeriesContract
# ---------------------------------------------------------------------------


class PriceSeriesContract:
    """Resolves an instrument id to its price series, explicitly.

    Parameters:
        engine: SQLAlchemy engine (or anything exposing ``.connect()``
            returning a context-managed connection with ``.execute()`` —
            a fake engine is fine for tests, see tests/test_evaluation_prices.py).
        candidate_name_fn: overrides the candidate-name rule. Defaults to
            :func:`_default_candidate_names` (the app's own rule). Tests
            inject a fixed list here instead of depending on
            ``_resolve_feature_names``'s own guessing internals.
        source_name: the source_catalog name this contract assumes bars
            come from. Defaults to :data:`BAR_SOURCE_NAME` ("yfinance").
    """

    def __init__(
        self,
        engine,
        *,
        candidate_name_fn: Optional[CandidateNameFn] = None,
        source_name: str = BAR_SOURCE_NAME,
        unit: str = DEFAULT_UNIT,
    ) -> None:
        self._engine = engine
        self._candidate_name_fn = candidate_name_fn or _default_candidate_names
        self._source_name = source_name
        self._unit = unit
        self._cache: dict[str, SeriesDescriptor] = {}

    def resolve(self, instrument: str) -> SeriesDescriptor:
        """Return the :class:`SeriesDescriptor` for ``instrument``.

        Raises:
            UnsupportedInstrumentError: no feature_registry row matched any
                candidate name, or the assumed source is not registered in
                source_catalog.
            AmbiguousInstrumentError: more than one feature_registry row
                matched the candidate names.
        """
        cached = self._cache.get(instrument)
        if cached is not None:
            return cached

        from sqlalchemy import text  # local import: keep this module DB-optional

        candidate_names = self._candidate_name_fn(instrument)
        if not candidate_names:
            raise UnsupportedInstrumentError(
                instrument, "candidate-name rule produced no names to look up"
            )

        with self._engine.connect() as conn:
            # SAME join condition as api/routers/watchlist_overview.py:422
            # (`fr.name = ANY(:names)`) — confirmed against the database,
            # never assumed to exist just because it is in the candidate list.
            feature_rows = conn.execute(
                text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)"),
                {"names": list(candidate_names)},
            ).fetchall()

        if not feature_rows:
            raise UnsupportedInstrumentError(
                instrument,
                f"no feature_registry row matches any candidate name: {candidate_names}",
            )

        distinct_ids = {row[0] for row in feature_rows}
        if len(distinct_ids) > 1:
            raise AmbiguousInstrumentError(
                instrument, [(row[0], row[1]) for row in feature_rows]
            )

        feature_id, feature_name = feature_rows[0][0], feature_rows[0][1]

        with self._engine.connect() as conn:
            # SAME case-insensitive source_catalog lookup rule as
            # ingestion/base.py:321 (`_resolve_source_id`), for the source
            # this contract's bar convention assumes (ingestion/yfinance_pull.py:103).
            source_rows = conn.execute(
                text("SELECT id, name FROM source_catalog WHERE LOWER(name) = LOWER(:name)"),
                {"name": self._source_name},
            ).fetchall()

        if not source_rows:
            raise UnsupportedInstrumentError(
                instrument,
                f"source_catalog has no {self._source_name!r} row — this "
                "contract's daily-bar convention cannot be confirmed",
            )

        source_id, source_name = source_rows[0][0], source_rows[0][1]

        # Ground truth for the series_id naming convention:
        # ingestion/yfinance_pull.py:203 (`series_id = f"YF:{yf_ticker}:{field_key}"`)
        # with field_key="close" (ingestion/yfinance_pull.py:66-73 _FIELD_MAP).
        series_id = f"YF:{instrument}:{BAR_KIND}"

        descriptor = SeriesDescriptor(
            instrument=instrument,
            feature_id=feature_id,
            feature_name=feature_name,
            series_id=series_id,
            source_catalog_id=source_id,
            source_catalog_name=source_name,
            bar_kind=BAR_KIND,
            unit=self._unit,
        )
        self._cache[instrument] = descriptor
        return descriptor


# ---------------------------------------------------------------------------
# PITPriceAccessor
# ---------------------------------------------------------------------------


def _coerce_date(value) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    if hasattr(value, "date"):
        return value.date()
    raise TypeError(f"Cannot coerce {value!r} to a date")


class PITPriceAccessor:
    """The PIT-safe accessor callable used by ``evaluation/signal_outcomes.py``.

    ``accessor(instrument, as_of)`` resolves the instrument via
    ``contract`` once (cached), then reads the price through
    ``store/pit.py::PITStore.get_pit`` with ``vintage_policy="LATEST_AS_OF"``
    — i.e. the same never-look-past-``as_of`` guarantee every other PIT
    read in GRID relies on — and returns a :class:`PricePoint`, or ``None``
    if no bar exists yet at or before ``as_of``.

    Raises ``evaluation.signal_outcomes.UnsupportedInstrumentError`` (not
    this module's own error type) when the contract cannot resolve the
    instrument, so it can be handed directly to
    ``evaluation.signal_outcomes.evaluate_signal`` as its ``price_accessor``
    with no adapter needed.
    """

    def __init__(
        self,
        engine,
        contract: PriceSeriesContract,
        *,
        vintage_policy: str = "LATEST_AS_OF",
        sanity_bounds: tuple[float, float] = DEFAULT_SANITY_BOUNDS,
    ) -> None:
        self._engine = engine
        self._contract = contract
        self._vintage_policy = vintage_policy
        self._sanity_bounds = sanity_bounds

    def __call__(self, instrument: str, as_of: date) -> Optional[PricePoint]:
        try:
            descriptor = self._contract.resolve(instrument)
        except UnsupportedInstrumentError as exc:
            # Local import: evaluation/signal_outcomes.py is the module
            # that defines the exception type evaluate_signal() actually
            # catches; re-raise as that type so this accessor is usable
            # directly as evaluate_signal()'s price_accessor.
            from evaluation.signal_outcomes import (
                UnsupportedInstrumentError as _SignalOutcomesUnsupported,
            )

            raise _SignalOutcomesUnsupported(str(exc)) from exc

        from store.pit import PITStore  # local import: keep this module DB-optional

        pit_store = PITStore(self._engine)
        df = pit_store.get_pit(
            [descriptor.feature_id], as_of_date=as_of, vintage_policy=self._vintage_policy
        )
        if df.empty:
            return None

        df = df.sort_values("obs_date")
        last = df.iloc[-1]

        obs_date = _coerce_date(last["obs_date"])
        release_date = _coerce_date(last["release_date"])
        vintage_date = _coerce_date(last["vintage_date"])
        value = float(last["value"])

        lo, hi = self._sanity_bounds
        sanity_ok = lo <= value <= hi
        sanity_reason = None if sanity_ok else REASON_PRICE_OUT_OF_BOUNDS

        return PricePoint(
            obs_date=obs_date,
            value=value,
            release_date=release_date,
            vintage_date=vintage_date,
            source_ref=f"{descriptor.source_catalog_name}:{descriptor.series_id}",
            basis=descriptor.bar_kind,
            sanity_ok=sanity_ok,
            sanity_reason=sanity_reason,
        )
