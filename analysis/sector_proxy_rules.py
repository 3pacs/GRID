"""Declared PROXY / SELF_LAG rules for sector-relative targets (GD7a).

A sector or issuer target must never "discover" its own price. This module is
the declared rule set (plan 2026-09-27 section 2.4) that turns every feature
which is the target's own price, a near-copy of it, or a composite of the same
inputs into a forced ``SELF_LAG`` trial for that target: measured for the
record, never selectable, never allocatable. It mirrors
``analysis.research_real_panel.proxy_group`` / ``required_proxies`` /
``self_lag_pairs`` for the rates/vol/credit universe, which it does not edit:
the S09/S10 path and its hypothesis-forward-v1 prereg stay untouched, and panel
mode (GD6) and Route A (GD7b) consume this module directly.

Pure functions and data only: no DB, no prices, no labels, no outcomes.

Targets (one declared grammar; anything else is refused)
--------------------------------------------------------
* ``REL:{ETF}-SPY``     a sector benchmark ETF minus SPY (Route A, time series).
* ``REL:{TICKER}-{ETF}`` an issuer minus its sector benchmark; the issuer's
  sector comes from the caller's frozen ``members`` map and must match the ETF.
* ``SECTOR:{sector}``   the panel-mode cross-section (issuer minus its sector
  ETF, GD6 family ``SECTOR:{sector}|rel_ret|fwd{h}``).

Families are ``TARGET|label|fwdH`` as everywhere in the S11 ledger.

Feature series grammar (version ``gd7a-v1``)
-------------------------------------------
A feature is ``{series}|{suffix}`` (the S11 convention; the suffix is a
transform or a window such as ``chg20`` or ``W30``). The series is
``NS``, ``NS:SUBJECT`` or ``NS:SUBJECT:FIELD``:

* price namespaces :data:`PRICE_NAMESPACES` (``PX``, ``RET``, ``MOM``,
  ``TIINGO``, ``TWELVEDATA``, ``REL``): past closes, returns or momentum of the
  subject. A subject is a ticker or a ``{A}-{B}`` pair whose right leg is a
  benchmark or SPY. A namespace with **no subject** is the entity's own value in
  panel mode (each issuer's own past return).
* ``ETF_FLOWS:{ETF}``: the ``etf_flows`` table, a dollar-volume proxy (price x
  volume), not creation/redemption data.
* ``OPTIONS:{TICKER}:{field}``: options features; ``max_pain`` and
  ``spot_price`` embed spot.
* ``FUNDAMENTAL_DIVERGENCE:{TICKER}:price_score`` and
  ``TICKER_METRICS_DAILY:{TICKER}:market_cap_usd``: price-derived columns.
* ``SECTOR_HEALTH_SNAPSHOTS[:...]``: a composite of the same inputs.
* people-density features: GD5 spec names (``A_insider_buy``,
  ``S_insider``, ``A_insider_buy@edgar``, ``D_peer_congress``, ``A_multi`` ...)
  as the panel-mode construct, or ``SECTOR_DENSITY:{sector}:{spec}:W{w}`` for
  Route A's weekly sector aggregates. These are the features under test and are
  never proxies.

Rules (:data:`PROXY_TABLES`; each carries the reason the ledger records)
-----------------------------------------------------------------------
For every declared target, a feature is a PROXY (forced ``SELF_LAG``) when it
is: the ETF's or a constituent's own past return/momentum; SPY's past return;
``etf_flows`` of any ETF and ``sector_health_snapshots`` (both also R6:
they may appear only as PROXY members, never as features under test); an
options spot field, ``fundamental_divergence.price_score`` or
``ticker_metrics_daily.market_cap_usd`` of a target leg or constituent.
A pair subject (``REL:XLE-SPY``) is a proxy when either leg is: a series that
shares a leg with the target is a near-copy (``research_real_panel`` rule (b)),
so every ``*-SPY`` relative return is a proxy of every sector-relative target.
A ticker the caller's ``members`` map does not place in a sector is treated as
a possible constituent of every sector (fail-closed).

``contains_price`` (plan section 2.3, GD9 PX flywheels)
------------------------------------------------------
Any other price-derived construct (another sector's momentum, a flywheel with a
PX stage, ...) is ``contains_price``. It is not forced to SELF_LAG, but it may
be selected only through :func:`momentum_gate`: the same run must have tested
the target's momentum family, and the construct must beat it (rule
:data:`MOMENTUM_GATE_RULE`). Otherwise it is refused.

A channel family is one vote
----------------------------
:func:`vote_class` maps every variant of one people channel (QuiverQuant and
EDGAR sources, ``A`` and ``S`` and the ``D_self``/``D_peer`` normalizations) to
one feature class ``people_density_{channel}``, so the S11 allocator, whose arms
are ``{feature_class}::{family}``, sees one arm per channel and family and
cannot farm one channel as several arms.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date

from analysis.ledger_steered_exploration import SELF_LAG_CLASS, Catalog

RULES_VERSION = "gd7a-v1"
MARKET = "SPY"
TECHNOLOGY = "Technology"

#: The 11 equity sectors and their benchmark ETF (VS1 v1 ``EQUITY_SECTORS``,
#: ``analysis/sector_map_data.yaml``; a test pins all three against each other).
SECTOR_BENCHMARKS: dict[str, str] = {
    "Technology": "XLK",
    "Energy": "XLE",
    "Financials": "XLF",
    "Healthcare": "XLV",
    "Industrials": "XLI",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Communication Services": "XLC",
    "Materials": "XLB",
}
NON_TECH_SECTORS: tuple[str, ...] = tuple(s for s in SECTOR_BENCHMARKS if s != TECHNOLOGY)
ETF_SECTOR: dict[str, str] = {etf: sector for sector, etf in SECTOR_BENCHMARKS.items()}
#: First admitted close of the late ETFs (sectors-v2..v4 ``LATE_ETF_START``).
LATE_ETF_START: dict[str, str] = {"XLRE": "2015-10", "XLC": "2018-06"}
PRE_INCEPTION_RULE = (
    "vs1-sectors-v4 section 3: before XLRE's (2015-10) or XLC's (2018-06) first admitted "
    "close, the sector uses XLK's session calendar and its benchmark is the "
    "equal-weighted mean close-to-close return of the sector's admitted issuers over "
    "the same sessions; the constituents' own returns then ARE the benchmark, so they "
    "stay PROXY members throughout"
)

PRICE_NAMESPACES: tuple[str, ...] = ("PX", "RET", "MOM", "TIINGO", "TWELVEDATA", "REL")
MOMENTUM_NAMESPACES: tuple[str, ...] = ("MOM", "RET")
SPOT_OPTION_FIELDS: tuple[str, ...] = ("max_pain", "spot_price")
#: R6 never-a-channel tables (GD-INDEX): refused as features outright.
R6_REFUSED_NAMESPACES: tuple[str, ...] = (
    "signal_data",
    "insider_trades",
    "congressional_trades",
    "wealth_flows",
    "dollar_flows",
    "actor_connections",
    "lever_pullers",
    "influence_loops",
    "sector_density",
)
R6_REFUSED_TOKENS: tuple[str, ...] = ("is_cluster_buy",)

# --- people channels (one vote each) -------------------------------------------------

#: Channel family -> the tokens that name it in a GD5 spec / construct name.
CHANNEL_TOKENS: dict[str, tuple[str, ...]] = {
    "insider": ("insider", "form4"),
    "congress": ("congress",),
    "inst": ("inst", "thirteen", "13f"),
    "contract": ("contract", "gov"),
    "lobby": ("lobby", "lobbying"),
    "news": ("news",),
}
MULTI_TOKEN = "multi"
MEASURES: tuple[str, ...] = ("A", "C", "S", "D")
#: Source variants of one channel; a construct may name one after ``@``.
SOURCE_VARIANTS: tuple[str, ...] = ("quiverquant", "quiver", "qq", "edgar", "sec")


@dataclass(frozen=True)
class ProxyRule:
    """One declared PROXY rule; ``scope`` is ``own``, ``market`` or ``any``."""

    rule_id: str
    namespaces: tuple[str, ...]
    fields: tuple[str, ...] | None
    scope: str
    reason: str


PROXY_TABLES: tuple[ProxyRule, ...] = (
    ProxyRule(
        "own_price",
        PRICE_NAMESPACES,
        None,
        "own",
        "the target ETF's or a constituent's own past return/momentum: the target "
        "predicting itself (unmapped tickers count as possible constituents)",
    ),
    ProxyRule(
        "market_price",
        PRICE_NAMESPACES,
        None,
        "market",
        "SPY past returns: the market leg of every sector-relative target",
    ),
    ProxyRule(
        "etf_flows",
        ("ETF_FLOWS",),
        None,
        "any",
        "etf_flows is a dollar-volume (price x volume) proxy, not creation/redemption "
        "data; R6: only ever a PROXY member, never a feature under test",
    ),
    ProxyRule(
        "options_spot",
        ("OPTIONS",),
        SPOT_OPTION_FIELDS,
        "own+market",
        "options features that embed spot (max_pain, spot_price)",
    ),
    ProxyRule(
        "fundamental_divergence_price_score",
        ("FUNDAMENTAL_DIVERGENCE",),
        ("price_score",),
        "own+market",
        "fundamental_divergence.price_score is price-derived",
    ),
    ProxyRule(
        "ticker_metrics_market_cap",
        ("TICKER_METRICS_DAILY",),
        ("market_cap_usd",),
        "own+market",
        "ticker_metrics_daily.market_cap_usd is price x shares",
    ),
    ProxyRule(
        "sector_health_snapshots",
        ("SECTOR_HEALTH_SNAPSHOTS",),
        None,
        "any",
        "sector_health_snapshots is a composite of the same price inputs; R6: only "
        "ever a PROXY member, never a feature under test",
    ),
)
#: Namespaces whose features carry price even when they are not a PROXY member of a
#: given target (another sector's momentum, an options greek, ...).
CONTAINS_PRICE_NAMESPACES: tuple[str, ...] = (
    *PRICE_NAMESPACES,
    "ETF_FLOWS",
    "OPTIONS",
    "SECTOR_HEALTH_SNAPSHOTS",
)
CONTAINS_PRICE_FIELDS: dict[str, tuple[str, ...]] = {
    "FUNDAMENTAL_DIVERGENCE": ("price_score",),
    "TICKER_METRICS_DAILY": ("market_cap_usd",),
}
MOMENTUM_GATE_RULE = (
    "a contains_price feature of family F may be selected in a run only if the same run "
    "tested F's momentum family (MOM/RET features of F's own legs or constituents, "
    "measured as SELF_LAG) with at least one finite statistic, and the feature beats "
    "every such momentum trial: p <= the smallest momentum p AND |statistic| > the "
    "largest finite |momentum statistic|; otherwise it is refused"
)

# --- parsing -------------------------------------------------------------------------

_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,14}$")
_NAMESPACE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Target:
    """A parsed sector-relative target."""

    target_id: str
    kind: str  # "etf_spy" | "issuer_etf" | "sector_panel"
    sector: str
    etf: str
    issuer: str | None = None


def _tickers(subject: str | None) -> tuple[str, ...]:
    """Tickers of a feature subject; a ``{A}-{B}`` pair needs a benchmark/SPY right leg."""
    if subject is None:
        return ()
    if "-" in subject:
        left, right = subject.rsplit("-", 1)
        if right in ETF_SECTOR or right == MARKET:
            return (left, right)
    return (subject,)


def parse_target(target_id: str, members: Mapping[str, str] | None = None) -> Target:
    """Parse a declared target; refuse one with no declared proxy group."""
    if target_id.startswith("SECTOR:"):
        sector = target_id[len("SECTOR:") :]
        if sector not in SECTOR_BENCHMARKS:
            raise ValueError(f"{target_id}: refused: no declared proxy group for this target")
        return Target(target_id, "sector_panel", sector, SECTOR_BENCHMARKS[sector])
    if target_id.startswith("REL:"):
        body = target_id[len("REL:") :]
        if "-" not in body:
            raise ValueError(f"{target_id}: refused: no declared proxy group for this target")
        left, right = body.rsplit("-", 1)
        if right == MARKET and left in ETF_SECTOR:
            return Target(target_id, "etf_spy", ETF_SECTOR[left], left)
        if right in ETF_SECTOR and _TICKER.match(left) and left not in ETF_SECTOR and left != MARKET:
            sector = ETF_SECTOR[right]
            mapped = (members or {}).get(left)
            if mapped is None:
                raise ValueError(
                    f"{target_id}: refused: issuer {left} has no sector in the declared "
                    "members map, so its proxy group cannot be derived"
                )
            if mapped != sector:
                raise ValueError(
                    f"{target_id}: refused: issuer {left} is mapped to {mapped!r}, "
                    f"not {sector!r} ({right})"
                )
            return Target(target_id, "issuer_etf", sector, right, issuer=left)
    raise ValueError(f"{target_id}: refused: no declared proxy group for this target")


def split_series(series: str) -> tuple[str, str | None, str | None]:
    """``NS[:SUBJECT[:FIELD]]`` -> (namespace, subject, field)."""
    if not series:
        raise ValueError("empty feature series")
    if series.startswith("REL:"):
        return "REL", series[len("REL:") :] or None, None
    parts = series.split(":")
    if len(parts) > 3 or not _NAMESPACE.match(parts[0]) or any(p == "" for p in parts):
        raise ValueError(f"{series!r}: refused: not a gd7a-v1 feature series")
    namespace = parts[0]
    subject = parts[1] if len(parts) > 1 else None
    field = parts[2] if len(parts) > 2 else None
    return namespace, subject, field


def series_of(feature: str) -> str:
    """The series part of an S11 feature name (``{series}|{suffix}``)."""
    return feature.rsplit("|", 1)[0] if "|" in feature else feature


def r6_refusal(feature: str) -> str | None:
    """Why ``feature`` is an R6 never-a-channel input, or ``None``."""
    series = series_of(feature)
    namespace = series.split(":", 1)[0]
    if namespace in R6_REFUSED_NAMESPACES or any(t in feature.lower() for t in R6_REFUSED_TOKENS):
        return f"{feature}: refused: R6 never-a-channel input"
    return None


# --- the own set of a target ----------------------------------------------------------


def _own(target: Target, tickers: tuple[str, ...], members: Mapping[str, str]) -> bool:
    """Is any ticker the target's own leg or (possibly) one of its constituents?"""
    if not tickers:  # no subject: the entity's own value (panel mode); conservative otherwise
        return True
    for ticker in tickers:
        if ticker == MARKET:
            continue
        if ticker == target.etf or ticker == target.issuer:
            return True
        if ticker in ETF_SECTOR:  # another sector's benchmark
            continue
        sector = members.get(ticker)
        if sector is None or sector == target.sector:
            return True  # a constituent, or unmapped: fail closed
    return False


def _market(tickers: tuple[str, ...]) -> bool:
    return MARKET in tickers


def proxy_rule(
    target_id: str, feature: str, members: Mapping[str, str] | None = None
) -> ProxyRule | None:
    """The first :data:`PROXY_TABLES` rule that makes ``feature`` a proxy of the target."""
    target = parse_target(target_id, members)
    members = members or {}
    if people_channel(feature) is not None:
        return None  # people densities are the features under test, never proxies
    namespace, subject, field = split_series(series_of(feature))
    tickers = _tickers(subject) if namespace != "SECTOR_HEALTH_SNAPSHOTS" else ()
    for rule in PROXY_TABLES:
        if namespace not in rule.namespaces:
            continue
        if rule.fields is not None and field not in rule.fields:
            continue
        if rule.scope == "any":
            return rule
        if rule.scope == "own" and _own(target, tickers, members):
            return rule
        if rule.scope == "market" and _market(tickers):
            return rule
        if rule.scope == "own+market" and (_own(target, tickers, members) or _market(tickers)):
            return rule
    return None


def required_rel_proxies(
    target_id: str, universe: Iterable[str], members: Mapping[str, str] | None = None
) -> frozenset[str]:
    """Series of ``universe`` the rules require in ``target_id``'s proxy group."""
    parse_target(target_id, members)  # refuses an undeclared target
    return frozenset(
        series_of(s) for s in universe if proxy_rule(target_id, s, members) is not None
    )


def rel_proxy_group(
    target_id: str,
    universe: Iterable[str] = (),
    *,
    members: Mapping[str, str] | None = None,
    declared: Iterable[str] | None = None,
) -> frozenset[str]:
    """The target's proxy group over ``universe``.

    Without ``declared`` it is the rule-derived group. With ``declared`` (e.g.
    a group written into a prereg) it is that group, refused when it lacks a
    rule member, mirroring ``research_real_panel.proxy_group``.
    """
    universe = tuple(universe)
    required = required_rel_proxies(target_id, universe, members)
    if declared is None:
        return required
    group = frozenset(declared)
    missing = required - group
    if missing:
        raise ValueError(
            f"{target_id}: refused: proxy group lacks rule members {sorted(missing)}"
        )
    return group


def family_target(family: str) -> str:
    parts = family.rsplit("|", 2)
    if len(parts) != 3 or not parts[2].startswith("fwd") or not parts[2][3:].isdigit():
        raise ValueError(f"unknown family shape {family!r} (TARGET|label|fwdH)")
    return parts[0]


def rel_self_lag_pairs(
    families: Iterable[str],
    features: Iterable[str],
    *,
    members: Mapping[str, str] | None = None,
) -> tuple[tuple[str, str], ...]:
    """(family, feature) trials whose feature is a PROXY of the family's target."""
    features = tuple(features)
    pairs = []
    for family in families:
        target = family_target(family)
        parse_target(target, members)
        pairs.extend(
            (family, f) for f in features if proxy_rule(target, f, members) is not None
        )
    return tuple(pairs)


def self_lag_reasons(
    families: Iterable[str],
    features: Iterable[str],
    *,
    members: Mapping[str, str] | None = None,
) -> dict[tuple[str, str], dict]:
    """The rule id and reason the ledger records for each forced SELF_LAG pair."""
    features = tuple(features)
    out = {}
    for family in families:
        target = family_target(family)
        for f in features:
            rule = proxy_rule(target, f, members)
            if rule is not None:
                out[(family, f)] = {"rule_id": rule.rule_id, "reason": rule.reason}
    return out


# --- contains_price and the momentum gate --------------------------------------------


def contains_price(feature: str, flagged: Iterable[str] = ()) -> bool:
    """Whether a feature carries price: a price/proxy namespace or a flagged construct.

    ``flagged`` names constructs whose GD6 ``ConstructSpec.contains_price`` is
    true (GD9 PX flywheels); a construct is matched by its series name.
    """
    series = series_of(feature)
    if series in set(flagged):
        return True
    if people_channel(feature) is not None:
        return False
    namespace, _subject, field = split_series(series)
    if namespace in CONTAINS_PRICE_NAMESPACES:
        return True
    return field in CONTAINS_PRICE_FIELDS.get(namespace, ())


def momentum_features(
    family: str, features: Iterable[str], members: Mapping[str, str] | None = None
) -> tuple[str, ...]:
    """The family's momentum family: MOM/RET features of its own legs or constituents."""
    target = parse_target(family_target(family), members)
    members = members or {}
    out = []
    for f in features:
        if people_channel(f) is not None:
            continue
        namespace, subject, _field = split_series(series_of(f))
        if namespace in MOMENTUM_NAMESPACES and _own(target, _tickers(subject), members):
            out.append(f)
    return tuple(out)


def _finite(value) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def momentum_gate(
    family: str,
    stats: Mapping[str, Mapping[str, float]],
    *,
    flagged: Iterable[str] = (),
    members: Mapping[str, str] | None = None,
) -> dict[str, str | None]:
    """Admissibility of every selectable ``contains_price`` trial of one run's family.

    ``stats`` maps each feature the run tested for ``family`` to its discovery
    ``p`` and signed ``statistic``. Returns ``{feature: None}`` for an admissible
    trial and ``{feature: reason}`` for a refused one, for every feature that is
    ``contains_price`` and not already a forced SELF_LAG (those are never
    selectable). Rule: :data:`MOMENTUM_GATE_RULE`.
    """
    flagged = frozenset(flagged)
    target = family_target(family)
    momentum = momentum_features(family, stats, members)
    finite = [m for m in momentum if _finite(stats[m].get("statistic")) and _finite(stats[m].get("p"))]
    out: dict[str, str | None] = {}
    for feature, s in sorted(stats.items()):
        if proxy_rule(target, feature, members) is not None or not contains_price(feature, flagged):
            continue
        if not momentum:
            out[feature] = "refused: contains_price and the run did not test the momentum family"
            continue
        if not finite:
            out[feature] = "refused: contains_price and the momentum family was untestable"
            continue
        if not (_finite(s.get("p")) and _finite(s.get("statistic"))):
            out[feature] = "refused: contains_price trial has no finite statistic"
            continue
        best_p = min(float(stats[m]["p"]) for m in finite)
        best_stat = max(abs(float(stats[m]["statistic"])) for m in finite)
        if float(s["p"]) <= best_p and abs(float(s["statistic"])) > best_stat:
            out[feature] = None
        else:
            out[feature] = (
                f"refused: contains_price does not beat momentum (p={float(s['p']):.6g} vs "
                f"{best_p:.6g}, |stat|={abs(float(s['statistic'])):.6g} vs {best_stat:.6g})"
            )
    return out


def gate_selections(
    family: str,
    selected: Iterable[str],
    stats: Mapping[str, Mapping[str, float]],
    *,
    flagged: Iterable[str] = (),
    members: Mapping[str, str] | None = None,
) -> tuple[tuple[str, ...], dict[str, str]]:
    """Split one family's discovery selections into (kept, {refused: reason}).

    A forced SELF_LAG feature is refused outright; a ``contains_price`` feature
    is kept only if :func:`momentum_gate` admits it.
    """
    target = family_target(family)
    gate = momentum_gate(family, stats, flagged=flagged, members=members)
    kept, refused = [], {}
    for feature in selected:
        rule = proxy_rule(target, feature, members)
        if rule is not None:
            refused[feature] = f"refused: SELF_LAG ({rule.rule_id})"
        elif contains_price(feature, flagged):
            reason = gate.get(feature, "refused: contains_price trial missing from the run stats")
            if reason is None:
                kept.append(feature)
            else:
                refused[feature] = reason
        else:
            kept.append(feature)
    return tuple(kept), refused


# --- one vote per channel -------------------------------------------------------------


def _construct(series: str) -> str:
    """The GD5 spec / construct name inside a feature series."""
    if series.startswith("SECTOR_DENSITY:"):
        parts = series.split(":")
        if len(parts) != 4 or parts[1] not in SECTOR_BENCHMARKS or not re.match(r"^W\d+$", parts[3]):
            raise ValueError(
                f"{series!r}: refused: Route A aggregates are SECTOR_DENSITY:{{sector}}:{{spec}}:W{{w}}"
            )
        return parts[2]
    return series


def people_channel(feature: str) -> str | None:
    """The people channel family a density feature measures, ``multi``, or ``None``."""
    series = series_of(feature)
    construct = _construct(series).split("@", 1)
    name, source = construct[0], construct[1] if len(construct) > 1 else None
    tokens = name.lower().split("_")
    if not tokens or tokens[0].upper() not in MEASURES:
        return None
    if source is not None and source.lower() not in SOURCE_VARIANTS:
        raise ValueError(f"{feature!r}: refused: unknown source variant {source!r}")
    if tokens[0] == "c" or MULTI_TOKEN in tokens:
        return MULTI_TOKEN
    hits = {
        channel
        for channel, words in CHANNEL_TOKENS.items()
        if any(w in tokens for w in words)
    }
    if len(hits) > 1:
        raise ValueError(f"{feature!r}: refused: names several channels {sorted(hits)}; use multi")
    return hits.pop() if hits else None


def vote_class(feature: str) -> str:
    """The feature class (one vote) of a feature; refuses one it cannot classify."""
    refusal = r6_refusal(feature)
    if refusal:
        raise ValueError(refusal)
    channel = people_channel(feature)
    if channel is not None:
        return f"people_density_{channel}"
    namespace = split_series(series_of(feature))[0]
    if namespace in PRICE_NAMESPACES:
        return "price"
    fixed = {
        "ETF_FLOWS": "etf_flows",
        "OPTIONS": "options",
        "SECTOR_HEALTH_SNAPSHOTS": "sector_health",
        "FUNDAMENTAL_DIVERGENCE": "fundamental_divergence",
        "TICKER_METRICS_DAILY": "ticker_metrics",
    }
    if namespace in fixed:
        return fixed[namespace]
    raise ValueError(f"{feature!r}: refused: no declared feature class")


def rel_catalog(
    families: Iterable[str],
    features: Iterable[str],
    *,
    members: Mapping[str, str] | None = None,
    classes: Mapping[str, str] | None = None,
) -> Catalog:
    """An S11 :class:`Catalog` over sector-relative families with GD7a classes and SELF_LAG.

    ``classes`` names the class of a feature :func:`vote_class` cannot classify
    (e.g. a GD9 flywheel construct). It may not re-class a people channel: that
    would let one channel farm several arms.
    """
    families = tuple(families)
    features = tuple(features)
    classes = dict(classes or {})
    mapping = []
    for f in features:
        refusal = r6_refusal(f)
        if refusal:
            raise ValueError(refusal)
        try:
            cls = vote_class(f)
        except ValueError:
            if f not in classes:
                raise
            cls = classes[f]
        else:
            if f in classes and classes[f] != cls and people_channel(f) is not None:
                raise ValueError(
                    f"{f!r}: refused: a people channel is one vote ({cls}), not {classes[f]!r}"
                )
            cls = classes.get(f, cls)
        if cls == SELF_LAG_CLASS:
            raise ValueError("feature classes may not be SELF_LAG")
        mapping.append((f, cls))
    catalog = Catalog(
        families=families,
        features=features,
        classes=tuple(mapping),
        self_lag=rel_self_lag_pairs(families, features, members=members),
    )
    catalog.validate()
    return catalog


# --- benchmarks and the declared manifest ---------------------------------------------


def pre_inception(etf: str, session: date) -> bool:
    """True before a late ETF's first admitted close (the equal-weight benchmark applies)."""
    start = LATE_ETF_START.get(etf)
    if start is None:
        return False
    year, month = (int(x) for x in start.split("-"))
    return (session.year, session.month) < (year, month)


def rules_manifest() -> dict:
    """Every declared rule as data; its sha256 is what a prereg or ledger cites."""
    return {
        "version": RULES_VERSION,
        "market": MARKET,
        "sector_benchmarks": dict(SECTOR_BENCHMARKS),
        "late_etf_start": dict(LATE_ETF_START),
        "pre_inception_rule": PRE_INCEPTION_RULE,
        "price_namespaces": list(PRICE_NAMESPACES),
        "momentum_namespaces": list(MOMENTUM_NAMESPACES),
        "proxy_tables": [asdict(rule) for rule in PROXY_TABLES],
        "contains_price_namespaces": list(CONTAINS_PRICE_NAMESPACES),
        "contains_price_fields": {k: list(v) for k, v in CONTAINS_PRICE_FIELDS.items()},
        "momentum_gate_rule": MOMENTUM_GATE_RULE,
        "channel_tokens": {k: list(v) for k, v in CHANNEL_TOKENS.items()},
        "source_variants": list(SOURCE_VARIANTS),
        "r6_refused_namespaces": list(R6_REFUSED_NAMESPACES),
        "r6_refused_tokens": list(R6_REFUSED_TOKENS),
    }


def rules_sha256() -> str:
    data = json.dumps(rules_manifest(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()
