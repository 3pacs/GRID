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
``NS``, ``NS:SUBJECT`` or ``NS:SUBJECT:FIELD``. Matching is case-normalized
(namespaces and subjects upper-case, fields lower-case), so no spelling of a
declared table or ticker escapes the rules.

* price namespaces :data:`PRICE_NAMESPACES` (``PX``, ``RET``, ``MOM``,
  ``TIINGO``, ``TWELVEDATA``, ``REL``): past closes, returns or momentum of the
  subject. A subject is a ticker or a ``{A}-{B}`` pair whose right leg is a
  benchmark or SPY. A namespace with **no subject** is the entity's own value in
  panel mode (each issuer's own past return).
* ``ETF_FLOWS:{ETF}``: the ``etf_flows`` table, a dollar-volume proxy (price x
  volume), not creation/redemption data.
* ``OPTIONS:{TICKER}:{field}``: options features; spot levels (``max_pain``,
  ``spot_price``, ``gamma_flip``, ``call_wall``, ``put_wall``) embed spot.
* ``FUNDAMENTAL_DIVERGENCE:{TICKER}[:{field}]`` and
  ``TICKER_METRICS_DAILY:{TICKER}[:{field}]``: every column of these tables is
  treated as price-derived (``price_score``, ``divergence``, ``close_price``,
  ``market_cap_usd`` ...).
* ``SECTOR_HEALTH_SNAPSHOTS[:...]``: a composite of the same inputs.
* people-density features: GD5 spec names (``A_insider_buy``,
  ``S_insider``, ``A_insider_buy@edgar``, ``D_peer_congress``, ``A_multi`` ...)
  as the panel-mode construct, or exactly ``SECTOR_DENSITY:{sector}:{spec}:W{w}``
  for Route A's weekly sector aggregates. These are the features under test
  and are never proxies.
* refused outright: R6 never-a-channel tables (any case; the only
  ``sector_density`` spelling allowed is GD5's exact aggregate form above) and
  yfinance ids (``YF``/``YF_ADJ``, unverified price basis).

Rules (:data:`PROXY_TABLES`; each carries the reason the ledger records)
-----------------------------------------------------------------------
For every declared target, a feature is a PROXY (forced ``SELF_LAG``) when it
is: the ETF's or a constituent's own past return/momentum; SPY's past return;
``etf_flows`` of any ETF and ``sector_health_snapshots`` (both also R6:
they may appear only as PROXY members, never as features under test); an
options spot level or any ``fundamental_divergence`` / ``ticker_metrics_daily``
column of a target leg, a constituent or SPY.
A pair subject (``REL:XLE-SPY``) is a proxy when either leg is: a series that
shares a leg with the target is a near-copy (``research_real_panel`` rule (b)),
so every ``*-SPY`` relative return is a proxy of every sector-relative target.
A ticker the caller's ``members`` map does not place in a sector is treated as
a possible constituent of every sector (fail-closed).

``contains_price`` (plan section 2.3, GD9 PX flywheels)
------------------------------------------------------
Fail-closed: every feature that is not a people-density channel carries price
unless its construct is explicitly declared ``nonprice``. Such a feature is not
forced to SELF_LAG, but it may be selected only through :func:`momentum_gate`:
the same run must have tested the target's momentum family, the feature must
beat it, and its incremental (momentum-residualized) p must be below 0.05
(rule :data:`MOMENTUM_GATE_RULE`). Otherwise it is refused.

A channel family is one vote
----------------------------
:func:`vote_class` maps every variant of one people channel (QuiverQuant and
EDGAR sources, ``A`` and ``S`` and the ``D_self``/``D_peer`` normalizations) to
one feature class ``people_density_{channel}``, so the S11 allocator, whose arms
are ``{feature_class}::{family}``, sees one arm per channel and family and
cannot farm one channel as several arms. A declared class cannot be overridden.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date

import numpy as np

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
#: Option fields that are spot levels (embed spot); fields are matched lower-case.
SPOT_OPTION_FIELDS: tuple[str, ...] = ("max_pain", "spot_price", "gamma_flip", "call_wall", "put_wall")
#: Tables whose every column is price-derived or price-relative (close_price,
#: market_cap_usd, price_score, divergence = fundamental_score - price_score, ...).
PRICE_TABLE_NAMESPACES: tuple[str, ...] = ("TICKER_METRICS_DAILY", "FUNDAMENTAL_DIVERGENCE")
#: yfinance ids: price basis not verified single-valued per date (S07/#642); refused.
UNVERIFIED_PRICE_NAMESPACES: tuple[str, ...] = ("YF", "YF_ADJ")
#: R6 never-a-channel tables (GD-INDEX): refused as features outright (any case).
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
MOMENTUM_GATE_ALPHA = 0.05

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
    """One declared PROXY rule; ``scope`` is ``own``, ``market``, ``own+market`` or ``any``."""

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
        "options features that are spot levels (max_pain, spot_price, gamma_flip, "
        "call_wall, put_wall)",
    ),
    ProxyRule(
        "fundamental_divergence",
        ("FUNDAMENTAL_DIVERGENCE",),
        None,
        "own+market",
        "fundamental_divergence is price-relative in every column (price_score; "
        "divergence = fundamental_score - price_score; its classification)",
    ),
    ProxyRule(
        "ticker_metrics_daily",
        ("TICKER_METRICS_DAILY",),
        None,
        "own+market",
        "ticker_metrics_daily carries close_price and market_cap_usd (price x shares); "
        "every column is treated as price-derived",
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
#: Every namespace this module knows; a construct named by a ``classes`` override
#: may not use one (nor a subject), so an override cannot smuggle a table in.
KNOWN_NAMESPACES: tuple[str, ...] = (
    *PRICE_NAMESPACES,
    "ETF_FLOWS",
    "OPTIONS",
    "SECTOR_HEALTH_SNAPSHOTS",
    *PRICE_TABLE_NAMESPACES,
    *UNVERIFIED_PRICE_NAMESPACES,
)
MOMENTUM_GATE_RULE = (
    "contains_price is fail-closed: every feature that is not a people-density channel "
    "(and not declared nonprice by its construct) carries price. Such a feature of "
    "family F may be selected in a run only if (a) every stat comes from that one run "
    "(run_id), (b) the run tested F's momentum family (every own-leg/constituent/SPY "
    "price-namespace feature, measured as SELF_LAG) with at least one finite statistic, "
    "(c) it beats every momentum trial: p <= the smallest momentum p AND |statistic| > "
    "the largest finite |momentum statistic|, and (d) its incremental p (the block-"
    "permutation p of its correlation with the label after both are residualized on the "
    "momentum family, incremental_pvalue) is below MOMENTUM_GATE_ALPHA = 0.05; "
    "otherwise it is refused"
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
    """``NS[:SUBJECT[:FIELD]]`` -> (NAMESPACE, SUBJECT, field), case-normalized."""
    if not series:
        raise ValueError("empty feature series")
    if series[:4].upper() == "REL:":
        return "REL", series[4:].upper() or None, None
    parts = series.split(":")
    if len(parts) > 3 or not _NAMESPACE.match(parts[0]) or any(p == "" for p in parts):
        raise ValueError(f"{series!r}: refused: not a gd7a-v1 feature series")
    namespace = parts[0].upper()
    subject = parts[1].upper() if len(parts) > 1 else None
    field = parts[2].lower() if len(parts) > 2 else None
    return namespace, subject, field


def series_of(feature: str) -> str:
    """The series part of an S11 feature name (``{series}|{suffix}``)."""
    return feature.rsplit("|", 1)[0] if "|" in feature else feature


def _gd5_aggregate(series: str) -> bool:
    """Exactly GD5's ``SECTOR_DENSITY:{sector}:{spec}:W{w}`` (upper-case, four parts)."""
    parts = series.split(":")
    return (
        len(parts) == 4
        and parts[0] == "SECTOR_DENSITY"
        and parts[1] in SECTOR_BENCHMARKS
        and bool(parts[2])
        and re.match(r"^W\d+$", parts[3]) is not None
    )


def r6_refusal(feature: str) -> str | None:
    """Why ``feature`` is refused outright (R6 never-a-channel, unverified price), or ``None``.

    Matched case-insensitively. The only ``sector_density`` spelling allowed is
    GD5's exact Route A aggregate ``SECTOR_DENSITY:{sector}:{spec}:W{w}``.
    """
    series = series_of(feature)
    namespace = series.split(":", 1)[0]
    if any(t in feature.lower() for t in R6_REFUSED_TOKENS) or (
        namespace.lower() in R6_REFUSED_NAMESPACES and not _gd5_aggregate(series)
    ):
        return f"{feature}: refused: R6 never-a-channel input"
    if namespace.upper() in UNVERIFIED_PRICE_NAMESPACES:
        return f"{feature}: refused: yfinance price basis not verified single-valued per date"
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
    refusal = r6_refusal(feature)
    if refusal:
        raise ValueError(refusal)
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


def contains_price(feature: str, nonprice: Iterable[str] = ()) -> bool:
    """Fail-closed: every non-people feature carries price unless declared ``nonprice``.

    ``nonprice`` names constructs whose GD6 ``ConstructSpec.contains_price`` is
    explicitly false (matched by series name); the declaration is the caller's
    and is recorded in its prereg. People-density channels never carry price.
    """
    if people_channel(feature) is not None:
        return False
    return series_of(feature) not in set(nonprice)


def momentum_features(
    family: str, features: Iterable[str], members: Mapping[str, str] | None = None
) -> tuple[str, ...]:
    """The family's momentum family: every price-namespace feature that is SELF_LAG for it.

    That is the past price/return/momentum of the target's own legs, its
    constituents (or unmapped tickers) and SPY, in any price namespace
    (``MOM``, ``RET``, ``PX``, ``TIINGO``, ``TWELVEDATA``, ``REL`` pairs).
    """
    target = family_target(family)
    out = []
    for f in features:
        rule = proxy_rule(target, f, members)
        if rule is not None and rule.rule_id in ("own_price", "market_price"):
            out.append(f)
    return tuple(out)


def _finite(value) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def incremental_pvalue(
    feature,
    momentum,
    label,
    *,
    block: int,
    perms: int,
    seed: int,
) -> tuple[float, float]:
    """Partial correlation of a feature with the label given the momentum family, and its p.

    Both the feature and the label are residualized (OLS with an intercept) on
    the momentum columns; the p is ``offline_research_proof``'s two-sided
    block-permutation p of the residuals' correlation. The run's harness calls
    this on the discovery rows and passes the p to :func:`momentum_gate` as
    ``incremental_p``.
    """
    from analysis.offline_research_proof import block_permutation_pvalue

    x = np.asarray(feature, dtype=float)
    y = np.asarray(label, dtype=float)
    m = np.asarray(momentum, dtype=float).reshape(len(x), -1)
    design = np.column_stack([np.ones(len(x)), m])
    rx = x - design @ np.linalg.lstsq(design, x, rcond=None)[0]
    ry = y - design @ np.linalg.lstsq(design, y, rcond=None)[0]
    if float(rx @ rx) <= 1e-12 * max(1.0, float(x @ x)):
        return 0.0, 1.0  # the feature is (a linear copy of) momentum
    return block_permutation_pvalue(rx, ry, block, perms, seed)


def momentum_gate(
    family: str,
    stats: Mapping[str, Mapping[str, float]],
    *,
    run_id: str,
    nonprice: Iterable[str] = (),
    members: Mapping[str, str] | None = None,
) -> dict[str, str | None]:
    """Admissibility of every selectable ``contains_price`` trial of one run's family.

    ``stats`` maps each feature the run tested for ``family`` to its ``run_id``,
    discovery ``p`` and signed ``statistic``; a ``contains_price`` trial also
    carries ``incremental_p`` (:func:`incremental_pvalue`). Returns
    ``{feature: None}`` for an admissible trial and ``{feature: reason}`` for a
    refused one, for every ``contains_price`` feature that is not already a
    forced SELF_LAG (those are never selectable). Rule: :data:`MOMENTUM_GATE_RULE`.
    """
    nonprice = frozenset(nonprice)
    target = family_target(family)
    foreign = sorted(f for f, s in stats.items() if not run_id or s.get("run_id") != run_id)
    momentum = momentum_features(family, stats, members)
    finite = [m for m in momentum if _finite(stats[m].get("statistic")) and _finite(stats[m].get("p"))]
    out: dict[str, str | None] = {}
    for feature, s in sorted(stats.items()):
        if proxy_rule(target, feature, members) is not None or not contains_price(feature, nonprice):
            continue
        if foreign:
            out[feature] = f"refused: stats are not all from run {run_id!r}: {foreign}"
            continue
        if not momentum:
            out[feature] = "refused: contains_price and the run did not test the momentum family"
            continue
        if not finite:
            out[feature] = "refused: contains_price and the momentum family was untestable"
            continue
        if not (_finite(s.get("p")) and _finite(s.get("statistic")) and _finite(s.get("incremental_p"))):
            out[feature] = "refused: contains_price trial lacks a finite p, statistic or incremental_p"
            continue
        best_p = min(float(stats[m]["p"]) for m in finite)
        best_stat = max(abs(float(stats[m]["statistic"])) for m in finite)
        beats = float(s["p"]) <= best_p and abs(float(s["statistic"])) > best_stat
        incremental = float(s["incremental_p"]) < MOMENTUM_GATE_ALPHA
        if beats and incremental:
            out[feature] = None
        else:
            out[feature] = (
                f"refused: contains_price does not beat momentum (p={float(s['p']):.6g} vs "
                f"{best_p:.6g}, |stat|={abs(float(s['statistic'])):.6g} vs {best_stat:.6g}, "
                f"incremental_p={float(s['incremental_p']):.6g} vs {MOMENTUM_GATE_ALPHA})"
            )
    return out


def gate_selections(
    family: str,
    selected: Iterable[str],
    stats: Mapping[str, Mapping[str, float]],
    *,
    run_id: str,
    nonprice: Iterable[str] = (),
    members: Mapping[str, str] | None = None,
) -> tuple[tuple[str, ...], dict[str, str]]:
    """Split one family's discovery selections into (kept, {refused: reason}).

    A forced SELF_LAG feature is refused outright; a ``contains_price`` feature
    is kept only if :func:`momentum_gate` admits it.
    """
    target = family_target(family)
    gate = momentum_gate(family, stats, run_id=run_id, nonprice=nonprice, members=members)
    kept, refused = [], {}
    for feature in selected:
        rule = proxy_rule(target, feature, members)
        if rule is not None:
            refused[feature] = f"refused: SELF_LAG ({rule.rule_id})"
        elif contains_price(feature, nonprice):
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
    if series.upper().startswith("SECTOR_DENSITY:"):
        if not _gd5_aggregate(series):
            raise ValueError(
                f"{series!r}: refused: Route A aggregates are SECTOR_DENSITY:{{sector}}:{{spec}}:W{{w}}"
            )
        return series.split(":")[2]
    return series


def people_channel(feature: str) -> str | None:
    """The people channel family a density feature measures, ``multi``, or ``None``."""
    series = series_of(feature)
    construct = _construct(series).split("@", 1)
    name, source = construct[0], construct[1] if len(construct) > 1 else None
    if ":" in name:
        return None  # a table series, never a people construct
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


class Unclassified(ValueError):
    """A well-formed feature with no declared class (a construct needing a ``classes`` entry)."""


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
    raise Unclassified(f"{feature!r}: refused: no declared feature class")


def rel_catalog(
    families: Iterable[str],
    features: Iterable[str],
    *,
    members: Mapping[str, str] | None = None,
    classes: Mapping[str, str] | None = None,
) -> Catalog:
    """An S11 :class:`Catalog` over sector-relative families with GD7a classes and SELF_LAG.

    ``classes`` names the class of a construct :func:`vote_class` cannot
    classify (e.g. a GD9 flywheel). It is refused for any feature
    :func:`vote_class` can classify (no re-classing, so one channel or one
    price table cannot farm several arms), for a refused input, for a
    construct with a subject or a known namespace, and for a
    ``people_density_*`` class on a non-people feature. Such constructs are
    ``contains_price`` (fail-closed) unless declared nonprice.
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
        except Unclassified:
            if f not in classes:
                raise
            series = series_of(f)
            cls = classes[f]
            if ":" in series or series.upper() in KNOWN_NAMESPACES:
                raise ValueError(
                    f"{f!r}: refused: a class override names a construct, not a table or a ticker"
                ) from None
            if not cls or cls.startswith("people_density_"):
                raise ValueError(
                    f"{f!r}: refused: {cls!r} is reserved for people-density channels"
                ) from None
        else:
            if f in classes and classes[f] != cls:
                raise ValueError(
                    f"{f!r}: refused: its class is declared ({cls}); one vote, not {classes[f]!r}"
                )
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
        "price_table_namespaces": list(PRICE_TABLE_NAMESPACES),
        "unverified_price_namespaces": list(UNVERIFIED_PRICE_NAMESPACES),
        "spot_option_fields": list(SPOT_OPTION_FIELDS),
        "proxy_tables": [asdict(rule) for rule in PROXY_TABLES],
        "matching": "namespaces and subjects upper-cased, fields lower-cased",
        "contains_price": "fail-closed: every non-people feature unless declared nonprice",
        "momentum_gate_rule": MOMENTUM_GATE_RULE,
        "momentum_gate_alpha": MOMENTUM_GATE_ALPHA,
        "channel_tokens": {k: list(v) for k, v in CHANNEL_TOKENS.items()},
        "source_variants": list(SOURCE_VARIANTS),
        "r6_refused_namespaces": list(R6_REFUSED_NAMESPACES),
        "r6_refused_tokens": list(R6_REFUSED_TOKENS),
    }


def rules_sha256() -> str:
    data = json.dumps(rules_manifest(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(data).hexdigest()
