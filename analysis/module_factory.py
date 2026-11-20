"""Module factory run command (3pacs/GRID#1155, factory 1: the thin run).

One fluid ranking run of SPY intraday modules over the staged dev data, recorded
in a factory ledger (``analysis.ledger_steered_exploration.Ledger.create_factory``).

A run file (JSON) declares everything: decision times, horizons, features,
z thresholds, sides, the per-share cost, alpha, permutations and seed. A module
(a trial) is ``feature z >= +k`` or ``feature z <= -k`` at one decision time,
taken long or short, entered at the open one minute after T (a declared bar-price
proxy, not an executable-quote guarantee), judged through T+horizon minus the cost
(``analysis.factory_stats`` statistic ``net_when_fires``). The run's bar is the
Westfall-Young step-down max-T over every tested module (selection ``max_t``).

Order of a run (the ledger sees the run before the data does):

1. parse and check the run file (dev window only; any holdout path refused);
2. declare the run on the ledger: run file + hash, the staged data manifest's
   hash, and the module count the run file implies;
3. verify the staged files against the manifest (a mismatch abandons the run);
4. build the point-in-time rows, discover, write the frozen manifest and the
   report (``write_once``: never overwritten);
5. record the result (trials, max-T bar, report and frozen hashes).

Features (factory 1, SPY only; edge9 dial definitions, computed at the bar that
closes at the decision time T, i.e. the bar stamped T-1):

* ``F1``: SPY return since the prior session's close;
* ``F10``: position of SPY in today's range so far (0 low, 1 high);
* ``F11``: position of SPY in the prior session's range;
* ``F1_shift30``: change of ``F1`` over the last 30 minutes.

Factory 2 (3pacs/GRID#1156) adds the rest of edge9's price/momentum class
(:data:`PRICE_CLASS_SPECS`, edge9 ``dials_edge9.build_moments`` definitions
with amendments 1-3), over SPY, QQQ, RSP, HYG, VIXY and TLT. The target is
still SPY's move; the other five symbols enter only as features:

* ``F2``: VIXY return since its prior close minus beta x SPY's (beta: OLS of
  daily close-to-close returns over the previous ``beta_history`` sessions);
* ``F3``: RSP return minus SPY return since the prior close;
* ``F4``: HYG return minus beta x SPY's, as ``F2``;
* ``F5``: TLT return since the prior close;
* ``F6``: QQQ minus SPY return over the last 30 minutes;
* ``S6``: month-end pressure: in a month's last 3 sessions, SPY minus TLT
  month-to-date return to the prior close, else 0;
* ``<dial>_shift30`` for F2..F6, F10 and F11: the dial at the bar closing at
  T minus the dial 30 minutes earlier (as ``F1_shift30``).

Every symbol's price is the close of its last bar stamped at or before T-1.
A non-SPY price is missing when that bar is more than ``stale_minutes`` old
(edge9 amendment 2), and a prior close is missing unless the prior session's
last bar starts at or after 15:50. A feature built on a missing price is
None (it never fires); nothing is filled. The flow dials (F7-F9, tape) and
the FRED dials (S1-S5, macro) are later factory classes.

Factory 3 (3pacs/GRID#1157) adds two classes, SPY only (the live recorder
holds SPY alone):

* tape (:data:`TAPE_SPECS`), from 1-minute o/h/l/c/v bars: relative volume over
  the last 30 minutes and since the open (``VR30``, ``VRDAY``), relative 30-minute
  range (``RR30``), effort without result (``EWR30`` = VR30 / RR30, edge8's W1 idea
  as a dial), the gap to the session VWAP (``VWAP``), and edge8's W1L / W1H
  markers (``catalog_edge8.detect``'s W rule on bars) fired in the last 30
  minutes. The staged research bars hold no bid/ask and no trade counts, and
  there are no aggressor tags, so no spread feature and no buy/sell split exist;
  edge9's flow dials F7-F9 (aggressor flow) are not rebuilt. Baselines are
  medians over the previous ``tape_history`` sessions.
* calendar (:data:`CALENDAR_SPECS`): weekday flags, monthly options expiration,
  and FOMC / CPI / NFP days and the FOMC eve from the pinned event manifest
  (:class:`analysis.factory_session_calendar.FactoryEvents`): an event counts
  only when its date was public before the decision (``known_at`` < T). Time of
  day is the decision time itself, so it is the family dimension, not a feature.

Flags (:data:`FLAGS`: the W1 markers and every calendar feature) take the rule
``on`` (the module fires when the flag is 1) and no z. ``class_shares`` in a run
file states each class's share of the run's modules and is checked against the
modules the run file implies; the report adds a class table and every bucket's
hit rate against chance. ``skip_windows`` leaves out a decision window, such as
15:00+60m, which is the same trade as 15:00+close.

Factory 4 (3pacs/GRID#1158) adds the options-positioning class (:data:`OPTIONS_SPECS`), built by
:mod:`analysis.factory_options` through the live GEX Levels page's own book function (gex
``levels.horizon_book`` / ``levels.position_levels``, vendored byte for byte and hash-pinned):

* OI levels by the page's level rule (``nearest-position/1``) on the 0DTE book: the distance to the
  nearest OI ceiling and floor (``OIR1``, ``OIS1``) and spot's place between them (``OIMID``);
* walls and the modeled dealer gamma on the same book, gamma rebuilt from the staged gexhist 5-minute
  quotes: call, put and hedge walls (``CWALL``, ``PWALL``, ``HWALL``), the gamma flip (``FLIP``) and
  the net share of gross modeled gamma (``NGEX``); ``measurement-unqualified`` (#1132, #1108 open);
* OI levels over every expiry from a frozen copy of the OCC open-interest archive (``OIR1A``,
  ``OIS1A``; the archive's trailing year only), pinned by ``oi_archive_sha256`` in the run file.

Quotes count from one minute after their mark and only up to T-1 minute; open interest only after its
publication (see :mod:`analysis.factory_options`). Features that need live-only inputs (today's
contract volume, recorded positions) are :data:`FORWARD_ONLY` and refused in a ranking run.

Factory 5 (3pacs/GRID#1159) adds the macro class (:data:`MACRO_SPECS`), built by :mod:`analysis.factory_macro`
from one frozen snapshot of GRID's FRED stores, pinned by ``macro_sha256`` in the run file (``--macro FILE``):

* edge9's slow dials S1-S5: SPY trend and drawdown from its own settled closes, the VIX curve (VIXCLS / VXVCLS),
  the 20-observation change of the high-yield OAS, and VIX minus SPY's 20-day realized vol;
* FRED dailies as levels and changes: credit (HY OAS), VIX, the 2-year yield, the 10y-2y curve, the 10-year real
  yield and breakeven, SOFR minus effective fed funds, the Fed's reverse repo, the yen, and the daily
  uncertainty indexes (EPU, EMU) read through their ALFRED vintages.

A macro value counts at T only when its publication rule (GRID's ``research_real_panel.PUBLICATIONS``) puts it
strictly before T; a revised series counts only through its vintages, each from the day after its vintage date.
Revised series without vintages (payrolls, CPI, GDP and the like) are not used.

Each is turned into a robust z against the same feature at the same decision
time over the previous ``z_history`` eligible sessions (median / 1.4826 MAD,
capped at +/-4), so a threshold never uses data after its decision.

The report adds the co-firing matrix of the run's decision signals
(``cofiring.json``, hashed into the report) and clusters built from fire
overlap only, never from returns (:func:`analysis.factory_stats.cofiring`),
with one representative per cluster: one idea, at most one live slot.

Variables are locked to declared pieces: every feature has a spec
(:data:`FEATURE_SPECS`: definition, unit, source, live source, point-in-time
rule) and is scale-free, so SPY's price level never enters a feature. The
target unit is declared too (``target_unit``):

* ``vol`` (default): the SPY change divided by the median absolute change of
  the same (decision time, horizon) window over the previous ``vol_history``
  eligible sessions (a point-in-time risk unit; it removes both the price
  level and the volatility regime);
* ``bps``: the change in basis points of the entry price;
* ``dollars``: the change per share.

The per-share cost is converted into the same unit on every row, and every
module's mean net is also reported in dollars per share (what a live slot
earns). The report carries each feature's spec, its coverage and its median
by year (a drift check). Eligibility requires the complete opening-to-decision
bar prefix, never the eventual full-day bar count. ``full_bars`` is a retrospective
coverage diagnostic only. Entry/exit availability controls labels, not signal history.
Ranking only: nothing here claims a discovery or promotes anything; the
evidence is forward live recording in gex-signals ``panel-factory``.

  python -m analysis.module_factory run --run-file RUN.json --ledger L --anchor A --out DIR [--genesis ID]
      [--oi-archive OI.db] [--macro MACRO.db]
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import sqlite3
import statistics
import sys
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from analysis import factory_macro as fm
from analysis import factory_options as fo
from analysis import ledger_steered_exploration as lse
from analysis.factory_session_calendar import FactoryCalendar, FactoryEvents
from analysis.factory_stats import (
    COFIRING_MEASURES,
    FACTORY_ORIGIN,
    MAX_T_SELECTION,
    NET_WHEN_FIRES,
    FactoryProtocol,
    cofiring,
    discover,
)
from analysis.offline_research_proof import digest, write_once

ORIGIN = FACTORY_ORIGIN
DEV_FIRST = "2022-11-14"  # edge9's holdout ends before this; nothing earlier is read
NY = ZoneInfo("America/New_York")
FEATURE_SPECS = {
    "F1": {
        "definition": "SPY close at the bar closing at T / prior session's last close - 1",
        "unit": "return (scale-free)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bar stamped T-1 (closes at T); prior session complete",
    },
    "F10": {
        "definition": "(close - day low so far) / (day high so far - day low so far), at the bar closing at T",
        "unit": "position in range, 0..1 (scale-free)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bars stamped up to T-1",
    },
    "F11": {
        "definition": "(close - prior session low) / (prior session high - prior session low), at the bar closing at T",
        "unit": "position in range (scale-free; outside 0..1 beyond the prior range)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bar stamped T-1; prior session complete",
    },
    "F1_shift30": {
        "definition": "F1 at the bar closing at T minus F1 at the bar 30 minutes earlier",
        "unit": "return (scale-free)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bars stamped T-31 and T-1",
    },
}
# FEATURE_SPECS / FEATURES / raw_features stay the SPY-only factory 1 set: the
# live parity check (#1160) compares exactly these against the live recorder.
FEATURES = tuple(FEATURE_SPECS)
SYMBOLS = ("SPY", "QQQ", "RSP", "HYG", "VIXY", "TLT")
_DATA = "staged edge9/bars.db, {} 1-minute bars (Databento XNAS)"
_SPY_LIVE = "gex-signals bars_1m_live, SPY (Schwab)"
_NOT_LIVE = "none yet: {} not in the live RTD subscription (design gap 8); research-only until recorded live"
_PIT_RET = ("bars stamped at or before T-1; a non-SPY price older than stale_minutes is missing; "
            "prior closes from a last bar at or after 15:50")


def _shift_spec(dial: str, spec: dict) -> dict:
    return {
        "definition": f"{dial} at the bar closing at T minus {dial} at the bar 30 minutes earlier",
        "unit": spec["unit"],
        "source": spec["source"],
        "live_source": spec["live_source"],
        "pit": spec["pit"] + "; the earlier value from bars stamped at or before T-31",
        "symbols": spec["symbols"],
    }


_LEVELS = {
    "F2": {
        "definition": "VIXY return since its prior close - beta x SPY return since its prior close, at the bar "
                      "closing at T; beta = OLS slope of VIXY on SPY daily close-to-close returns over the previous "
                      "beta_history sessions with both closes",
        "unit": "return (scale-free)",
        "source": _DATA.format("SPY, VIXY"),
        "live_source": _NOT_LIVE.format("VIXY"),
        "pit": _PIT_RET + "; beta from sessions before today",
        "symbols": ("SPY", "VIXY", "HYG"),  # the beta's sessions need all three closes (edge9 amendment 2)
    },
    "F3": {
        "definition": "RSP return - SPY return, each since its prior close, at the bar closing at T",
        "unit": "return (scale-free)",
        "source": _DATA.format("SPY, RSP"),
        "live_source": _NOT_LIVE.format("RSP"),
        "pit": _PIT_RET,
        "symbols": ("SPY", "RSP"),
    },
    "F4": {
        "definition": "HYG return since its prior close - beta x SPY return since its prior close, at the bar "
                      "closing at T; beta as F2, HYG on SPY",
        "unit": "return (scale-free)",
        "source": _DATA.format("SPY, HYG"),
        "live_source": _NOT_LIVE.format("HYG"),
        "pit": _PIT_RET + "; beta from sessions before today",
        "symbols": ("SPY", "VIXY", "HYG"),
    },
    "F5": {
        "definition": "TLT close at the bar closing at T / TLT prior close - 1",
        "unit": "return (scale-free)",
        "source": _DATA.format("TLT"),
        "live_source": _NOT_LIVE.format("TLT"),
        "pit": _PIT_RET,
        "symbols": ("TLT",),
    },
    "F6": {
        "definition": "QQQ return - SPY return over the 30 minutes to the bar closing at T",
        "unit": "return (scale-free)",
        "source": _DATA.format("SPY, QQQ"),
        "live_source": _NOT_LIVE.format("QQQ"),
        "pit": "bars stamped T-31 and T-1; a QQQ price older than stale_minutes is missing",
        "symbols": ("SPY", "QQQ"),
    },
}
PRICE_CLASS_SPECS = {
    **_LEVELS,
    "S6": {
        "definition": "in the last 3 sessions of a month: SPY month-to-date return - TLT month-to-date return, both "
                      "from the previous month's last close to the prior close; 0 on other sessions",
        "unit": "return (scale-free)",
        "source": _DATA.format("SPY, TLT"),
        "live_source": _NOT_LIVE.format("TLT"),
        "pit": "closes of the prior session and earlier; the month's last sessions from the pinned exchange calendar",
        "symbols": ("SPY", "TLT"),
    },
    **{f"{d}_shift30": _shift_spec(d, spec) for d, spec in _LEVELS.items()},
    **{
        f"{d}_shift30": _shift_spec(d, {**FEATURE_SPECS[d], "symbols": ("SPY",)})
        for d in ("F10", "F11")
    },
}
SPECS = {**{f: {**s, "symbols": ("SPY",)} for f, s in FEATURE_SPECS.items()}, **PRICE_CLASS_SPECS}
ALL_FEATURES = tuple(SPECS)  # the price/momentum class (factory 1 + 2)

# Factory 3 (3pacs/GRID#1157): the tape and calendar classes. SPY only: the live recorder holds SPY alone.
_TAPE_DATA = "staged edge9/bars.db, SPY 1-minute bars (Databento XNAS: o/h/l/c and Nasdaq-only volume)"
_TAPE_V_LIVE = ("research-only: live SPY interval volume is withheld as unreconciled (gex-watch volume_quality: "
                "VWAP, relative volume and volume spikes withheld); bars_1m_live v is a Schwab cumulative-volume "
                "difference (consolidated, null on some minutes) while research v is Nasdaq-only, about 5x lower; "
                "a ratio to each source's own history may agree, unproven (#1160)")
_TAPE_HL_LIVE = ("gex-signals bars_1m_live, SPY h/l (Schwab); parity unchecked: live h/l come from about 9 sampled "
                 "quotes a minute, so live ranges can read narrower than the trade tape (#1160)")
_TAPE_PIT = "bars stamped at or before T-1; the baseline from earlier sessions only (tape_history)"
TAPE_SPECS = {
    "VR30": {
        "definition": "SPY volume over the 30 bars stamped T-30..T-1 / median of the same over the previous "
                      "tape_history sessions",
        "unit": "ratio to the session-time baseline (scale-free)",
        "live_source": _TAPE_V_LIVE,
    },
    "VRDAY": {
        "definition": "SPY volume from 09:30 through the bar stamped T-1 / median of the same over the previous "
                      "tape_history sessions",
        "unit": "ratio to the session-time baseline (scale-free)",
        "live_source": _TAPE_V_LIVE,
    },
    "RR30": {
        "definition": "SPY high - low over the 30 bars stamped T-30..T-1 / median of the same over the previous "
                      "tape_history sessions",
        "unit": "ratio to the session-time baseline (scale-free)",
        "live_source": _TAPE_HL_LIVE,
    },
    "EWR30": {
        "definition": "effort without result (edge8 W1, continuous): VR30 / RR30",
        "unit": "ratio (scale-free)",
        "live_source": _TAPE_V_LIVE,
    },
    "VWAP": {
        "definition": "SPY close at the bar stamped T-1 / session VWAP so far - 1; VWAP = sum((h+l+c)/3 x v) / "
                      "sum(v) over bars 09:30..T-1",
        "unit": "return (scale-free)",
        "live_source": _TAPE_V_LIVE,
    },
    "W1L": {
        "definition": "1 when edge8's W1L marker fired on a bar stamped T-30..T-1, else 0: the bar's volume >= 3x "
                      "the median volume of that minute over the previous tape_history sessions, its range <= the "
                      "median bar range of the session so far, and its close at the 30-bar closing low (edge8 "
                      "catalog_edge8.detect, 09:45 onward)",
        "unit": "flag (fires on 1)",
        "live_source": _TAPE_V_LIVE,
        "kind": "flag",
    },
    "W1H": {
        "definition": "as W1L, with the close at the 30-bar closing high (edge8 W1H)",
        "unit": "flag (fires on 1)",
        "live_source": _TAPE_V_LIVE,
        "kind": "flag",
    },
}
for _spec in TAPE_SPECS.values():
    _spec.update(source=_TAPE_DATA, pit=_TAPE_PIT, symbols=("SPY",))
    _spec.setdefault("kind", "level")
_CAL_LIVE = ("date and pinned manifests only, computable live once the XNYS and event manifests are extended "
             "past 2026-11-10 with known_at (not done)")
_EVENT_DEF = "1 on a session holding a scheduled {} whose date was public before T (known_at < T), else 0"
CALENDAR_SPECS = {
    **{
        day: {"definition": f"1 on {name}s, else 0", "source": "the session date"}
        for day, name in (("MON", "Monday"), ("TUE", "Tuesday"), ("WED", "Wednesday"), ("THU", "Thursday"),
                          ("FRI", "Friday"))
    },
    "OPEX": {"definition": "1 on the monthly listed-options expiration session (the third Friday; the session "
                           "before it when that Friday is an exchange holiday), else 0",
             "source": "pinned XNYS sessions (analysis/factory_session_calendar.py)"},
    "FOMC": {"definition": _EVENT_DEF.format("FOMC statement (14:00 ET)"),
             "source": "pinned event manifest (factory-events/1)"},
    "FOMC_EVE": {"definition": "1 on the session before a scheduled FOMC statement whose date was public before T, "
                               "else 0", "source": "pinned event manifest and XNYS sessions"},
    "CPI": {"definition": _EVENT_DEF.format("CPI release (08:30 ET)"),
            "source": "pinned event manifest (factory-events/1)"},
    "NFP": {"definition": _EVENT_DEF.format("Employment Situation release (08:30 ET)"),
            "source": "pinned event manifest (factory-events/1)"},
}
for _spec in CALENDAR_SPECS.values():
    _spec.update(unit="flag (fires on 1)", live_source=_CAL_LIVE, kind="flag", symbols=(),
                 pit="the date, exchange sessions from the pinned calendar, and events only with known_at < T")
WEEKDAYS = ("MON", "TUE", "WED", "THU", "FRI")

# Factory 4 (3pacs/GRID#1158): the options-positioning class, through gex levels.horizon_book (analysis.factory_options).
_UNQUALIFIED = ("measurement-unqualified: the wall measurement contract (#1132) and canonical snapshots (#1108) are "
                "open; a diagnostic feature, not a qualified wall")
_OI_OBSERVED = "observed OCC open interest (unsigned); no dealer sign, no gamma model; not a wall measurement"
_DTE0_SRC = ("staged edge2/gexhist.db (Databento OPRA.PILLAR: OCC open interest stat_type 9 and 5-minute cbbo-1m "
             "marks) for SPY options expiring that session, $1 strikes within 8% of its open; SPY bars from "
             "edge9/bars.db")
_LIVE_LEVELS = ("gex-watch level book {}, the Levels page's {} horizon (same function, vendored); live OI is OCC "
                "(occ-oi/1, since 2026-11-17) with RTD streamed OI winning; parity unchecked: research ends "
                "2026-11-09 and the page's books are not recorded per decision time (#1160)")
_LIVE_GAMMA = ("gex-watch level book {} (dte0); live gamma is the provider's where present, else BS from IV, "
               "research is always BS from the 5-minute mark's IV; parity unchecked (#1160); " + _UNQUALIFIED)
_PIT_DTE0 = ("spot: bar stamped T-1; quotes: the latest 5-minute mark m <= T-2 minutes (m known at m+1), IV at the "
             "spot of the bar stamped m; OI: the session's OCC release, only after 07:30 ET that day")
OPTIONS_SPECS = {
    "OIR1": {"definition": "(R1 - spot) / spot; R1 = the nearest strike above spot holding >= 0.5 x the largest call "
                           "OI within 5% of spot (gex levels.position_levels, nearest-position/1), 0DTE book",
             "unit": "distance, share of spot (scale-free)", "basis": "oi-unsigned", "measurement": _OI_OBSERVED,
             "live_source": _LIVE_LEVELS.format("position_levels", "0DTE")},
    "OIS1": {"definition": "(spot - S1) / spot; S1 = the nearest strike below spot holding >= 0.5 x the largest put "
                           "OI within 5% of spot (nearest-position/1), 0DTE book",
             "unit": "distance, share of spot (scale-free)", "basis": "oi-unsigned", "measurement": _OI_OBSERVED,
             "live_source": _LIVE_LEVELS.format("position_levels", "0DTE")},
    "OIMID": {"definition": "(spot - S1) / (R1 - S1): spot's place between the 0DTE OI floor (0) and ceiling (1)",
              "unit": "position between levels, 0..1 (scale-free)", "basis": "oi-unsigned",
              "measurement": _OI_OBSERVED, "live_source": _LIVE_LEVELS.format("position_levels", "0DTE")},
    "CWALL": {"definition": "(call wall - spot) / spot; call wall = the strike with the largest modeled call gamma "
                            "x OI (horizon_book call_wall), 0DTE book",
              "unit": "distance, share of spot (scale-free)", "basis": "gamma-unsigned", "measurement": _UNQUALIFIED,
              "live_source": _LIVE_GAMMA.format("call_wall")},
    "PWALL": {"definition": "(spot - put wall) / spot; put wall = the strike with the largest modeled put gamma x OI "
                            "(horizon_book put_wall), 0DTE book",
              "unit": "distance, share of spot (scale-free)", "basis": "gamma-unsigned", "measurement": _UNQUALIFIED,
              "live_source": _LIVE_GAMMA.format("put_wall")},
    "HWALL": {"definition": "(hedge wall - spot) / spot; hedge wall = the strike with the largest |call| + |put| "
                            "modeled gamma x OI (horizon_book hedge_wall), 0DTE book",
              "unit": "signed distance, share of spot (scale-free)", "basis": "gamma-unsigned",
              "measurement": _UNQUALIFIED, "live_source": _LIVE_GAMMA.format("hedge_wall")},
    "FLIP": {"definition": "(spot - gamma flip) / spot; flip = the zero crossing of the assumed-sign net gamma profile "
                           "nearest spot (horizon_book gamma_flip), 0DTE book; None without a crossing",
             "unit": "signed distance, share of spot (scale-free)", "basis": "gamma-signed-assumed",
             "measurement": _UNQUALIFIED, "live_source": _LIVE_GAMMA.format("gamma_flip")},
    "NGEX": {"definition": "sum of per-strike net modeled GEX / sum of per-strike |call| + |put| GEX (dealers assumed "
                           "long calls, short puts), 0DTE book",
             "unit": "share of gross modeled gamma, -1..1 (scale-free)", "basis": "gamma-signed-assumed",
             "measurement": _UNQUALIFIED, "live_source": _LIVE_GAMMA.format("net_gex_at_spot_m")},
    "OIR1A": {"definition": "as OIR1, from the OI of every unexpired expiry (the page's ALL horizon)",
              "unit": "distance, share of spot (scale-free)", "basis": "oi-unsigned", "measurement": _OI_OBSERVED,
              "live_source": _LIVE_LEVELS.format("position_levels", "ALL")},
    "OIS1A": {"definition": "as OIS1, from the OI of every unexpired expiry (the page's ALL horizon)",
              "unit": "distance, share of spot (scale-free)", "basis": "oi-unsigned", "measurement": _OI_OBSERVED,
              "live_source": _LIVE_LEVELS.format("position_levels", "ALL")},
}
for _f, _spec in OPTIONS_SPECS.items():
    _archive = _f in fo.ARCHIVE_FEATURES
    _spec.update(kind="level", symbols=("SPY",), function="gex levels.horizon_book", formula=fo.FORMULA["id"],
                 source=("frozen copy of the OCC open-interest archive (gex-oi-capture oi.db, Databento "
                         "OPRA.PILLAR statistics), SPY, every expiry; pinned by oi_archive_sha256") if _archive
                 else _DTE0_SRC,
                 pit=("spot: bar stamped T-1; OI: the session's OCC release, only after 07:30 ET that day or its "
                      "archive published_at when later") if _archive else _PIT_DTE0)
# Live-only inputs: no history exists, so these cannot be ranked; they may only record forward (factory 7).
FORWARD_ONLY = {
    "FLOWC1": "today's contract volume at strike (levels flow_today, flow-today/1): live brokerage volume only, "
              "recorded from 2026-11-12; gexhist and the OCC archive hold no option volume",
    "FLOWP1": "as FLOWC1, puts",
    "POSREC": "the page's recorded position levels (gex-signals positions, from 2026-11-12): a live record, "
              "no history; the factory rebuilds positions from OCC OI instead",
}

# Factory 5 (3pacs/GRID#1159): the macro class, from a frozen snapshot of GRID's FRED stores (analysis.factory_macro).
_FRED_LIVE = ("GRID raw_series FRED pull (ingestion/fred.py): the latest observation whose publication rule puts it "
              "before T; parity unchecked (#1160)")
_SPY_DAILY_LIVE = "SPY daily closes from gex-signals bars_1m_live; parity unchecked (#1160)"
_MACRO_DEFS = {
    "S1": ("SPY trend (edge9 S1): last settled close before the session / mean of the last 200 settled closes - 1",
           "return (scale-free)", "trend", "spy-bars", _SPY_DAILY_LIVE),
    "S2": ("SPY drawdown (edge9 S2): last settled close before the session / max of the last 252 settled closes - 1",
           "return, <= 0 (scale-free)", "trend", "spy-bars", _SPY_DAILY_LIVE),
    "S3": ("VIX curve (edge9 S3): VIXCLS / VXVCLS on the newest date both are known", "ratio (scale-free)", "vol",
           "unrevised-latest", "none yet: VXVCLS is not in GRID's FRED pull list (staged file only); research-only"),
    "S4": (("credit (edge9 S4): high-yield OAS (BAMLH0A0HYM2), newest known observation minus the one 20 "
            "observations earlier"), "percentage points", "credit", "unrevised-latest", _FRED_LIVE),
    "S5": (("vol premium (edge9 S5): VIXCLS on its newest known date o minus 100 x sqrt(252) x the stdev of SPY's "
            "20 daily log returns ending at o's close"), "vol points", "vol", "unrevised-latest",
           _FRED_LIVE + "; SPY closes from bars_1m_live"),
    "HYOAS": ("high-yield OAS (BAMLH0A0HYM2), newest known observation", "percentage points", "credit",
              "unrevised-latest", _FRED_LIVE),
    "HYOAS_D1": ("high-yield OAS, newest known observation minus the one before", "percentage points", "credit",
                 "unrevised-latest", _FRED_LIVE),
    "VIX": ("VIXCLS, newest known observation", "index points", "vol", "unrevised-latest", _FRED_LIVE),
    "VIX_D1": ("VIXCLS, newest known observation minus the one before", "index points", "vol", "unrevised-latest",
               _FRED_LIVE),
    "Y2_D1": ("2-year Treasury yield (DGS2), newest known observation minus the one before", "percentage points",
              "rates", "unrevised-latest", _FRED_LIVE),
    "Y2_D20": ("2-year Treasury yield, newest known observation minus the one 20 observations earlier",
               "percentage points", "rates", "unrevised-latest", _FRED_LIVE),
    "CURVE": ("10-year minus 2-year Treasury (T10Y2Y), newest known observation", "percentage points", "rates",
              "unrevised-latest", _FRED_LIVE),
    "REAL10_D20": ("10-year TIPS real yield (DFII10), 20-observation change", "percentage points", "rates",
                   "unrevised-latest", _FRED_LIVE),
    "BE10_D20": ("10-year breakeven inflation (T10YIE), 20-observation change", "percentage points", "rates",
                 "unrevised-latest", _FRED_LIVE),
    "SOFRFF": ("SOFR minus the effective fed funds rate (DFF) on SOFR's newest known date (both known)",
               "percentage points", "funding", "unrevised-latest", _FRED_LIVE),
    "RRP_D20": ("Fed overnight reverse repo (RRPONTSYD), 20-observation change", "$bn", "funding",
                "unrevised-latest", _FRED_LIVE),
    "JPY_D20": ("yen per dollar (DEXJPUS, H.10), 20-observation return", "return (scale-free)", "fx",
                "unrevised-latest", _FRED_LIVE),
    "EPU5": (("daily Economic Policy Uncertainty (USEPUINDXD): mean of the newest 5 known observations, each as "
              "its newest vintage known before T"), "index points", "uncertainty", "alfred-vintage",
             "GRID fred_vintages refresh (ingestion/fred_vintages.py), the same vintage rule; parity unchecked (#1160)"),
    "EMU5": ("daily Equity Market Uncertainty (WLEMUINDXD): as EPU5", "index points", "uncertainty",
             "alfred-vintage",
             "GRID fred_vintages refresh (ingestion/fred_vintages.py), the same vintage rule; parity unchecked (#1160)"),
}
MACRO_SPECS = {}
for _f, (_def, _unit, _group, _basis, _live) in _MACRO_DEFS.items():
    _kind, _inputs = fm.FEATURES[_f]
    MACRO_SPECS[_f] = {
        "definition": _def, "unit": _unit, "group": _group, "basis": _basis, "live_source": _live,
        "series": list(_inputs), "kind": "level", "symbols": ("SPY",) if _f in fm.SPY_FEATURES else (),
        "source": "frozen factory-macro/1 snapshot pinned by macro_sha256: " + "; ".join(
            f"{sid} from {fm.SERIES[sid]['origin']}" for sid in _inputs) + (
            "; SPY settled closes from staged edge9/bars.db" if _f in fm.SPY_FEATURES else ""),
        "pit": "; ".join([fm.rule_text(sid) for sid in _inputs] + (
            ["SPY closes of sessions before the decision's session"] if _f in fm.SPY_FEATURES else [])
            + [f"stale beyond macro_stale_days; revision: {', '.join(sorted({fm.SERIES[sid]['revision'] for sid in _inputs})) or 'none (SPY closes)'}"]),
    }
FEATURE_CLASS = {**dict.fromkeys(SPECS, "price"), **dict.fromkeys(TAPE_SPECS, "tape"),
                 **dict.fromkeys(CALENDAR_SPECS, "calendar"), **dict.fromkeys(OPTIONS_SPECS, "options"),
                 **dict.fromkeys(MACRO_SPECS, "macro")}
REGISTRY = {**{f: {**s, "class": "price", "kind": "level"} for f, s in SPECS.items()},
            **{f: {**s, "class": "tape"} for f, s in TAPE_SPECS.items()},
            **{f: {**s, "class": "calendar"} for f, s in CALENDAR_SPECS.items()},
            **{f: {**s, "class": "options"} for f, s in OPTIONS_SPECS.items()},
            **{f: {**s, "class": "macro"} for f, s in MACRO_SPECS.items()}}
KNOWN_FEATURES = tuple(REGISTRY)
FLAGS = frozenset(f for f, s in REGISTRY.items() if s["kind"] == "flag")
CLASSES = ("price", "tape", "calendar", "options", "macro")
W1_VOLUME_MULTIPLE, W1_BARS, W1_FROM = 3.0, 30, "09:45"  # edge8 catalog_edge8.detect: W1 rule, 30-bar extremes
TAPE_QUANTITIES = ("vol30", "cumvol", "range30")
EVENT_FEATURES = frozenset(("FOMC", "FOMC_EVE", "CPI", "NFP"))
LAST_BAR_FROM = "15:50"  # a prior close needs a last bar at or after this (edge9 amendment 2)
TARGET_UNITS = ("vol", "bps", "dollars")
SIDES = {"long": 1.0, "short": -1.0}
Z_CAP = 4.0
DATA_FILE = "edge9/bars.db"


class RunFileError(ValueError):
    pass


@dataclass(frozen=True)
class RunFile:
    run_id: str
    first_session: str
    last_session: str
    decision_times: tuple[str, ...]
    horizons: tuple[str, ...]  # "<minutes>m" or "close"
    features: tuple[str, ...]
    z_thresholds: tuple[float, ...]
    sides: tuple[str, ...] = ("long", "short")
    cost_per_share: float = 0.04
    alpha: float = 0.05
    perms: int = 10000
    seed: int = 20261113
    min_n: int = 30
    z_history: int = 60
    full_bars: int = 370
    symbol: str = "SPY"
    target_unit: str = "vol"
    vol_history: int = 20
    stale_minutes: int = 10  # a non-SPY price older than this is missing (edge9 amendment 2)
    beta_history: int = 60  # sessions in the F2 / F4 daily betas
    cluster_by: str = "jaccard"  # co-firing similarity: "jaccard" or "phi"
    cluster_threshold: float = 0.5  # signals linked at or above this share a cluster
    tape_history: int = 20  # sessions in the tape baselines (edge8 RV_LOOKBACK)
    skip_windows: tuple[str, ...] = ()  # "<time>+<horizon>" windows left out (e.g. 15:00+60m, the same trade as close)
    class_shares: dict | None = None  # {class: share of the run's modules}; checked against the modules implied
    oi_archive_sha256: str | None = None  # the frozen OCC archive copy the OI-archive features read (factory 4)
    macro_sha256: str | None = None  # the frozen factory-macro/1 snapshot the macro features read (factory 5)
    macro_stale_days: int = 7  # a macro value whose newest observation became known longer ago than this is None

    @classmethod
    def from_dict(cls, value: dict) -> RunFile:
        known = set(cls.__dataclass_fields__)
        unknown = set(value) - known
        if unknown:
            raise RunFileError(f"unknown run file keys: {sorted(unknown)}")
        tuples = ("decision_times", "horizons", "features", "z_thresholds", "sides", "skip_windows")
        run = cls(**{k: tuple(v) if k in tuples else v for k, v in value.items()})
        run.validate()
        return run

    def validate(self) -> None:
        if not self.run_id or any(c.isspace() for c in self.run_id):
            raise RunFileError("run_id must be a non-empty token")
        try:
            first, last = date.fromisoformat(self.first_session), date.fromisoformat(self.last_session)
        except ValueError as e:
            raise RunFileError(f"bad session dates: {e}") from None
        if self.first_session < DEV_FIRST:
            raise RunFileError(f"first_session {self.first_session} is before dev ({DEV_FIRST}): holdout refused")
        if last < first:
            raise RunFileError("last_session is before first_session")
        if self.symbol != "SPY":
            raise RunFileError("the factory's target is SPY only; other symbols enter as features")
        for t in self.decision_times:
            hm = _minutes(t)
            if not 10 * 60 <= hm <= 15 * 60:
                raise RunFileError(f"decision time {t} outside 10:00..15:00 ET")
        for h in self.horizons:
            if h != "close" and not (h.endswith("m") and h[:-1].isdigit() and 2 <= int(h[:-1]) <= 390):
                raise RunFileError(f"horizon {h!r} is '<minutes>m' (at least 2m) or 'close'")
        if set(self.features) & set(FORWARD_ONLY):
            f = sorted(set(self.features) & set(FORWARD_ONLY))[0]
            raise RunFileError(f"{f} is forward-only (no history to rank): {FORWARD_ONLY[f]}")
        if set(self.features) - set(KNOWN_FEATURES):
            raise RunFileError(f"unknown features {sorted(set(self.features) - set(KNOWN_FEATURES))}")
        if set(self.sides) - set(SIDES) or not self.sides:
            raise RunFileError("sides are 'long' and/or 'short'")
        if not self.z_thresholds or any(not (0 < k <= Z_CAP) for k in self.z_thresholds):
            raise RunFileError(f"z thresholds lie in (0, {Z_CAP}]")
        for name, values in (("decision_times", self.decision_times), ("horizons", self.horizons),
                             ("features", self.features), ("z_thresholds", self.z_thresholds),
                             ("sides", self.sides)):
            if len(set(values)) != len(values) or not values:
                raise RunFileError(f"{name} must be non-empty and unique")
        if not (math.isfinite(self.cost_per_share) and self.cost_per_share >= 0):
            raise RunFileError("cost_per_share must be >= 0")
        if not 0 < self.alpha <= 0.10 or self.perms < 99 or self.min_n < 30 or self.z_history < 20:
            raise RunFileError("alpha in (0, 0.10], perms >= 99, min_n >= 30, z_history >= 20")
        if self.target_unit not in TARGET_UNITS or self.vol_history < 10:
            raise RunFileError(f"target_unit is one of {TARGET_UNITS}; vol_history >= 10")
        if not 0 <= self.stale_minutes <= 60 or self.beta_history < 20:
            raise RunFileError("stale_minutes in [0, 60]; beta_history >= 20")
        if self.cluster_by not in COFIRING_MEASURES or not 0 < self.cluster_threshold <= 1:
            raise RunFileError(f"cluster_by is one of {COFIRING_MEASURES}; cluster_threshold in (0, 1]")
        for t in self.decision_times:
            for h in self.horizons:
                if h != "close" and _minutes(t) + int(h[:-1]) > 16 * 60:
                    raise RunFileError(f"{t} + {h} runs past the close")
        windows = {f"{t}+{h}" for t in self.decision_times for h in self.horizons}
        if set(self.skip_windows) - windows or len(set(self.skip_windows)) != len(self.skip_windows):
            raise RunFileError(f"skip_windows must be unique windows of this run: {sorted(windows)}")
        if not self.families():
            raise RunFileError("skip_windows leaves no window")
        if self.tape_history < 10:
            raise RunFileError("tape_history >= 10")
        if self.class_shares is not None:
            self._check_class_shares()
        archive = bool(set(self.features) & set(fo.ARCHIVE_FEATURES))
        sha = self.oi_archive_sha256
        if archive != (sha is not None) or (sha is not None and not (
                isinstance(sha, str) and len(sha) == 64 and all(c in "0123456789abcdef" for c in sha))):
            raise RunFileError("oi_archive_sha256 (64 hex) is required with, and only with, the OI-archive features "
                               f"{fo.ARCHIVE_FEATURES}")
        macro = any(FEATURE_CLASS[f] == "macro" for f in self.features)
        sha = self.macro_sha256
        if macro != (sha is not None) or (sha is not None and not (
                isinstance(sha, str) and len(sha) == 64 and all(c in "0123456789abcdef" for c in sha))):
            raise RunFileError("macro_sha256 (64 hex) is required with, and only with, macro features")
        if not isinstance(self.macro_stale_days, int) or not 1 <= self.macro_stale_days <= 30:
            raise RunFileError("macro_stale_days in [1, 30]")

    def class_counts(self) -> dict[str, int]:
        """Modules per feature class (every family holds the same modules)."""
        out: dict[str, int] = {}
        for name in self.modules():
            cls = FEATURE_CLASS[name.split("|")[0]]
            out[cls] = out.get(cls, 0) + len(self.families())
        return out

    def _check_class_shares(self) -> None:
        shares = self.class_shares
        counts = self.class_counts()
        total = sum(counts.values())
        if not isinstance(shares, dict) or set(shares) != set(counts):
            raise RunFileError(f"class_shares names exactly the run's classes: {sorted(counts)}")
        if any(not isinstance(v, (int, float)) or not 0 < v <= 1 for v in shares.values()) or \
                abs(sum(shares.values()) - 1) > 1e-6:
            raise RunFileError("class_shares are in (0, 1] and sum to 1")
        actual = {c: n / total for c, n in counts.items()}
        off = {c: round(actual[c], 4) for c in counts if abs(shares[c] - actual[c]) > 0.005}
        if off:
            raise RunFileError(f"class_shares do not match the modules the run file implies: actual {off}")

    def families(self) -> tuple[str, ...]:
        skip = set(self.skip_windows)
        return tuple(f"SPY|change|{t}+{h}" for t in self.decision_times for h in self.horizons
                     if f"{t}+{h}" not in skip)

    def modules(self) -> tuple[str, ...]:
        """Signal names, the same in every family: feature | z rule | side; a flag feature's rule is ``on``
        (it fires when the flag is 1)."""
        out = []
        for f in self.features:
            if f in FLAGS:
                out += [f"{f}|on|{side}" for side in self.sides]
            else:
                out += [f"{f}|z{op}{k:g}|{side}" for k in self.z_thresholds for op in (">=", "<=")
                        for side in self.sides]
        return tuple(out)

    def trial_count(self) -> int:
        return len(self.families()) * len(self.modules())

    def symbols(self) -> tuple[str, ...]:
        """The symbols whose bars the run's features read, SPY first."""
        need = {s for f in self.features for s in REGISTRY[f]["symbols"]}
        return tuple(s for s in SYMBOLS if s == "SPY" or s in need)


def _minutes(hm: str) -> int:
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def _hm(minutes: int) -> str:
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}"


def robust_z(x, hist):
    """edge9 robust z: median / 1.4826 MAD (then pstdev, then 0), capped; None without a full history."""
    if x is None or len(hist) < hist.maxlen:
        return None
    med = statistics.median(hist)
    s = 1.4826 * statistics.median(abs(h - med) for h in hist)
    if s == 0:
        s = statistics.pstdev(hist)
    if s == 0:
        return 0.0
    return max(-Z_CAP, min(Z_CAP, (x - med) / s))


class Session:
    """One SPY session's minute bars {hm: (o, h, low, c, v)} with close-at-or-before lookups."""

    def __init__(self, bars: dict):
        self.hms = sorted(bars)
        self.bars = bars
        self._hi, self._lo = [], []
        hi = lo = None
        for hm in self.hms:
            b = bars[hm]
            hi = b[1] if hi is None else max(hi, b[1])
            lo = b[2] if lo is None else min(lo, b[2])
            self._hi.append(hi)
            self._lo.append(lo)

    def _at(self, hm):
        return bisect.bisect_right(self.hms, hm) - 1

    def close(self, hm, max_age=None):
        """Close of the last bar stamped at or before ``hm``; None when there is none, or when it is
        more than ``max_age`` minutes older than ``hm``."""
        i = self._at(hm)
        if i < 0 or (max_age is not None and _minutes(hm) - _minutes(self.hms[i]) > max_age):
            return None
        return self.bars[self.hms[i]][3]

    def hilo(self, hm):
        i = self._at(hm)
        return (None, None) if i < 0 else (self._hi[i], self._lo[i])

    @property
    def last_close(self):
        return self.bars[self.hms[-1]][3]


def raw_features(day: Session, prev: Session, bar: str) -> dict:
    """F1, F10, F11, F1_shift30 at the bar stamped ``bar`` (edge9 definitions, SPY only). Pure."""
    out = dict.fromkeys(FEATURES)
    c, pc = day.close(bar), prev.last_close
    if not c or not pc:
        return out
    out["F1"] = c / pc - 1
    c30 = day.close(_hm(_minutes(bar) - 30))
    if c30:
        out["F1_shift30"] = (c - pc) / pc - (c30 - pc) / pc
    hi, lo = day.hilo(bar)
    if hi is not None and hi > lo:
        out["F10"] = (c - lo) / (hi - lo)
    phi, plo = prev.hilo("23:59")
    if phi is not None and phi > plo:
        out["F11"] = (c - plo) / (phi - plo)
    return out


def settled_close(session: Session | None):
    """A session's close for the next day's returns: its last bar's close when that bar starts at or after
    15:50 (edge9 amendment 2), else None."""
    if session is None:
        return None
    i = session._at("15:59")
    return session.bars[session.hms[i]][3] if i >= 0 and session.hms[i] >= LAST_BAR_FROM else None


def ols_slope(xs, ys):
    """OLS slope of ys on xs; None below two points or with no variance in xs (edge9). Pure."""
    n = len(xs)
    if n < 2:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    vx = sum((x - mx) ** 2 for x in xs)
    if vx == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / vx


def class_features(px: dict, prior_close: dict, betas: dict, bar: str, stale: int) -> dict:
    """F2..F6 at the bar stamped ``bar`` (edge9 ``build_moments`` definitions). Pure.

    ``px``: {symbol: today's Session or None}; ``prior_close``: {symbol: settled prior close or None};
    ``betas``: {"VIXY": slope, "HYG": slope} from earlier sessions (missing until there are enough).
    Only bars stamped at or before ``bar`` are read, for every symbol; a non-SPY price older than
    ``stale`` minutes is missing, and a dial built on a missing price is None.
    """
    def price(sym, hm):
        session = px.get(sym)
        return None if session is None else session.close(hm, None if sym == "SPY" else stale)

    def ret(sym):
        c, pc = price(sym, bar), prior_close.get(sym)
        return c / pc - 1 if c and pc else None

    r = {sym: ret(sym) for sym in SYMBOLS}
    out = dict.fromkeys(_LEVELS)
    spy = r["SPY"]
    if spy is not None:
        if r["VIXY"] is not None and betas.get("VIXY") is not None:
            out["F2"] = r["VIXY"] - betas["VIXY"] * spy
        if r["RSP"] is not None:
            out["F3"] = r["RSP"] - spy
        if r["HYG"] is not None and betas.get("HYG") is not None:
            out["F4"] = r["HYG"] - betas["HYG"] * spy
    out["F5"] = r["TLT"]
    earlier = _hm(_minutes(bar) - 30)
    q, q0, c, c0 = price("QQQ", bar), price("QQQ", earlier), price("SPY", bar), price("SPY", earlier)
    if q and q0 and c and c0:
        out["F6"] = (q / q0 - 1) - (c / c0 - 1)
    return out


def month_end(day: str, sessions: list[str], closes: dict) -> float | None:
    """S6 for ``day``: in a month's last 3 sessions, SPY minus TLT month-to-date return to the prior close,
    else 0; None when a needed close is missing. ``sessions``: the exchange session list (pinned calendar);
    ``closes``: {symbol: {day: settled close}} of earlier sessions only. Pure."""
    i = bisect.bisect_left(sessions, day)
    if i >= len(sessions) or sessions[i] != day:
        return None
    month = [d for d in sessions if d[:7] == day[:7]]
    if day not in month[-3:]:
        return 0.0
    before = [d for d in sessions[:i] if d[:7] < day[:7]]
    if i == 0 or not before:
        return None
    prior, base = sessions[i - 1], before[-1]
    try:
        spy = closes["SPY"][prior] / closes["SPY"][base] - 1
        tlt = closes["TLT"][prior] / closes["TLT"][base] - 1
    except (KeyError, TypeError, ZeroDivisionError):
        return None
    return spy - tlt


def tape_window(day: Session, bar: str) -> dict:
    """The tape quantities at the bar stamped ``bar``, from bars stamped 09:30..``bar`` only. Pure.

    ``vol30`` / ``range30``: volume and high - low of the 30 bars stamped ``bar``-29..``bar`` (None unless
    all 30 are there); ``cumvol``: volume since 09:30; ``vwap_gap``: close / session VWAP - 1 (typical
    price (h+l+c)/3). A bar without a volume makes every volume quantity None: missing is never zero.
    """
    hms = day.hms[:day._at(bar) + 1]
    start = _hm(_minutes(bar) - (W1_BARS - 1))
    last = [h for h in hms if h >= start]
    out = dict.fromkeys(("vol30", "cumvol", "range30", "vwap_gap"))
    if not hms or hms[-1] != bar:
        return out
    if len(last) == W1_BARS:
        out["range30"] = max(day.bars[h][1] for h in last) - min(day.bars[h][2] for h in last)
    vols = [day.bars[h][4] for h in hms]
    if any(v is None or v < 0 for v in vols):
        return out
    if len(last) == W1_BARS:
        out["vol30"] = float(sum(vols[-W1_BARS:]))
    out["cumvol"] = float(sum(vols))
    if out["cumvol"] > 0:
        vwap = sum((day.bars[h][1] + day.bars[h][2] + day.bars[h][3]) / 3 * day.bars[h][4] for h in hms) / out["cumvol"]
        out["vwap_gap"] = day.bars[bar][3] / vwap - 1
    return out


def w1_events(day: Session, minute_base: dict | None, until: str) -> list[tuple[str, str]]:
    """edge8 W1 markers ``[(hm, "W1L"|"W1H")]`` on bars stamped up to ``until`` (catalog_edge8.detect's W rule,
    bars only: no flow, IV or gamma). ``minute_base``: {hm: median volume of that minute over earlier sessions}.
    A marker at a bar reads only that bar and earlier ones. Pure."""
    if not minute_base:
        return []
    hms = day.hms[:day._at(until) + 1]
    ranges, out = [], []
    for i, hm in enumerate(hms):
        _o, h, low, c, v = day.bars[hm]
        ranges.append(h - low)
        if i < W1_BARS - 1 or hm < W1_FROM:
            continue
        base = minute_base.get(hm)
        if not base or v is None or v / base < W1_VOLUME_MULTIPLE or (h - low) > statistics.median(ranges):
            continue
        closes = [day.bars[x][3] for x in hms[i - (W1_BARS - 1):i + 1]]
        if c <= min(closes):
            out.append((hm, "W1L"))
        if c >= max(closes):
            out.append((hm, "W1H"))
    return out


def tape_features(window: dict, base: dict, markers: list | None, bar: str) -> dict:
    """VR30, VRDAY, RR30, EWR30, VWAP, W1L, W1H at the bar stamped ``bar``. ``window``: :func:`tape_window`;
    ``base``: {"vol30", "cumvol", "range30"} medians of earlier sessions (None until there are enough);
    ``markers``: :func:`w1_events` of the session (None without a volume base). Pure."""
    def ratio(key):
        x, b = window[key], base.get(key)
        return x / b if x is not None and b else None

    out = {"VR30": ratio("vol30"), "VRDAY": ratio("cumvol"), "RR30": ratio("range30"), "VWAP": window["vwap_gap"]}
    out["EWR30"] = out["VR30"] / out["RR30"] if out["VR30"] is not None and out["RR30"] else None
    start = _hm(_minutes(bar) - (W1_BARS - 1))
    for name in ("W1L", "W1H"):
        out[name] = None if markers is None else float(any(m == name and start <= hm <= bar for hm, m in markers))
    return out


def opex_days(sessions: list[str]) -> dict[str, str | None]:
    """{month: its listed-options expiration session} over the months ``sessions`` covers in full: the third
    Friday, or the last session before it when that Friday is not a session. None for a month whose third
    Friday lies outside the covered dates. Pure."""
    have = set(sessions)
    out = {}
    first, last = sessions[0], sessions[-1]
    for month in sorted({d[:7] for d in sessions}):
        y, m = int(month[:4]), int(month[5:])
        fridays = [d for d in (date(y, m, k) for k in range(1, 22)) if d.weekday() == 4]
        third = fridays[2].isoformat()
        if not first <= third <= last:
            out[month] = None
            continue
        before = [d for d in sessions if d[:7] == month and d <= third]
        out[month] = third if third in have else (before[-1] if before else None)
    return out


def calendar_features(day: str, decided: datetime, nxt: str | None, opex: dict, events) -> dict:
    """The calendar class for a decision at ``decided`` on session ``day``. ``nxt``: the next session (None
    beyond the calendar); ``opex``: :func:`opex_days`; ``events``: :class:`FactoryEvents` or None. An event
    counts only with known_at < ``decided``; outside the event manifest's coverage it is None, never 0. Pure."""
    wd = date.fromisoformat(day).weekday()
    out = {name: float(wd == i) for i, name in enumerate(WEEKDAYS)}
    month = opex.get(day[:7])
    out["OPEX"] = None if month is None else float(month == day)
    known = events.known(day, decided) if events is not None else None
    for name in ("FOMC", "CPI", "NFP"):
        out[name] = None if known is None else float(name.lower() in known)
    ahead = events.known(nxt, decided) if events is not None and nxt is not None else None
    out["FOMC_EVE"] = None if ahead is None else float("fomc" in ahead)
    return out


def load_bars(db_path: Path, run: RunFile, symbols: tuple[str, ...] | None = None) -> dict[str, dict[str, Session]]:
    """{symbol: {day: Session}} for the run's window, read-only; refuses a symbol holding any day before dev."""
    symbols = symbols or run.symbols()
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    out: dict[str, dict[str, Session]] = {}
    try:
        for sym in symbols:
            lo = con.execute("select min(day) from bars where sym = ?", (sym,)).fetchone()[0]
            if lo is None or lo < DEV_FIRST:
                raise RunFileError(f"{db_path} holds {sym} sessions before dev ({lo}): holdout refused")
            rows = con.execute(
                "select day, hm, o, h, l, c, v from bars where sym = ? and day between ? and ? order by day, hm",
                (sym, run.first_session, run.last_session),
            ).fetchall()
            by_day: dict[str, dict] = {}
            for day, hm, o, h, low, c, v in rows:
                by_day.setdefault(day, {})[hm] = (o, h, low, c, v)
            out[sym] = {d: Session(b) for d, b in by_day.items()}
    finally:
        con.close()
    return out


def load_sessions(db_path: Path, run: RunFile) -> dict[str, Session]:
    """{day: Session} of the target (SPY) for the run's window, read-only; refuses any day before dev."""
    return load_bars(db_path, run, (run.symbol,))[run.symbol]


def features_at(session: Session, prior: Session, px: dict, prior_close: dict, betas: dict, s6, bar: str,
                wanted: tuple[str, ...], stale: int) -> dict:
    """Every wanted feature at the bar stamped ``bar``: the SPY-only set through :func:`raw_features`, the
    price/momentum class through :func:`class_features`, and their 30-minute shifts. Pure; reads no bar
    stamped after ``bar`` for any symbol."""
    out = raw_features(session, prior, bar)
    earlier = _hm(_minutes(bar) - 30)
    if any(f in wanted for f in ("F10_shift30", "F11_shift30")):
        before = raw_features(session, prior, earlier)
        for d in ("F10", "F11"):
            a, b = out[d], before[d]
            out[f"{d}_shift30"] = a - b if a is not None and b is not None else None
    if any(f in _LEVELS or f[:-8] in _LEVELS for f in wanted):
        now, then = class_features(px, prior_close, betas, bar, stale), class_features(px, prior_close, betas, earlier, stale)
        for d in _LEVELS:
            out[d] = now[d]
            out[f"{d}_shift30"] = now[d] - then[d] if now[d] is not None and then[d] is not None else None
    out["S6"] = s6
    return {f: out.get(f) for f in wanted}


def build_rows(sessions: dict[str, Session], run: RunFile, calendar: FactoryCalendar | None = None,
               cross: dict[str, dict[str, Session]] | None = None,
               events: FactoryEvents | None = None,
               options: fo.OptionsInputs | None = None,
               macro: fm.MacroInputs | None = None) -> tuple[dict[str, list[dict]], dict]:
    """Point-in-time rows per family, and a coverage summary.

    The z history of a (feature, decision time) holds only earlier eligible
    sessions' raw values, appended after the session's own z is taken.
    ``cross``: {symbol: {day: Session}} for the run's non-SPY symbols (from
    :func:`load_bars`). The daily betas use only sessions before the current
    one, and every symbol's price comes from a bar stamped at or before T-1.
    The tape baselines (``tape_history``) hold earlier sessions only, as the z
    history does; ``events`` (:class:`FactoryEvents`) answers which scheduled
    events were known before each decision. Flags (:data:`FLAGS`) fire on 1
    and take no z. ``options`` (:class:`analysis.factory_options.OptionsInputs`)
    supplies the options class: quotes up to T-1 minute and OI only after its
    publication, one session at a time. ``macro`` (:class:`analysis.factory_macro.MacroInputs`) supplies the
    macro class: each value only once its publication rule (or vintage) puts it before the decision.
    """
    days = sorted(sessions)
    cross = cross or {}
    missing = [s for s in run.symbols() if s != run.symbol and s not in cross]
    if missing:
        raise RunFileError(f"the run's features need bars for {missing}")
    bars_of = {run.symbol: sessions, **cross}
    settled = {sym: {d: settled_close(by_day[d]) for d in by_day} for sym, by_day in bars_of.items()}
    session_list = [s.key.split(":")[1] for s in calendar.sessions] if calendar is not None else days
    pairs = deque(maxlen=run.beta_history)  # (SPY, VIXY, HYG) daily returns of earlier sessions
    pending_pair = None
    need_betas = any(f in run.features for f in ("F2", "F4", "F2_shift30", "F4_shift30"))
    need_s6 = "S6" in run.features
    full_coverage = [d for d in days if len(sessions[d].hms) >= run.full_bars]
    eligible = set()
    history = {(f, t): deque(maxlen=run.z_history) for f in run.features for t in run.decision_times}
    moves = {fam: deque(maxlen=run.vol_history) for fam in run.families()}  # |dollar change|, earlier sessions
    price_wanted = tuple(f for f in run.features if FEATURE_CLASS[f] == "price")
    need_tape = any(FEATURE_CLASS[f] == "tape" for f in run.features)
    need_w1 = any(f in ("W1L", "W1H") for f in run.features)
    need_cal = any(FEATURE_CLASS[f] == "calendar" for f in run.features)
    if events is None and set(run.features) & EVENT_FEATURES:
        raise RunFileError("the run's event features need the pinned event manifest")
    options_wanted = tuple(f for f in run.features if FEATURE_CLASS[f] == "options")
    if options_wanted and (options is None or (options.gexhist is None and set(options_wanted) - set(fo.ARCHIVE_FEATURES))
                           or (options.archive is None and set(options_wanted) & set(fo.ARCHIVE_FEATURES))):
        raise RunFileError("the run's options features need the staged gexhist store (and the OCC archive copy)")
    macro_wanted = tuple(f for f in run.features if FEATURE_CLASS[f] == "macro")
    if macro_wanted and macro is None:
        raise RunFileError("the run's macro features need the frozen macro snapshot")
    tape_base = {(q, t): deque(maxlen=run.tape_history) for q in TAPE_QUANTITIES for t in run.decision_times}
    minute_hist = deque(maxlen=run.tape_history)  # {hm: volume} of earlier sessions (edge8 vol_hist)
    pending_minutes = None
    last_bar = _hm(max(_minutes(t) for t in run.decision_times) - 1)
    opex = opex_days(session_list) if need_cal else {}
    following = dict(zip(session_list, list(session_list[1:]) + [None]))
    skip = set(run.skip_windows)
    modules = run.modules()
    parsed = []
    for name in modules:
        feature, rule, side = name.split("|")
        parsed.append((name, feature, rule, side, None if rule == "on" else float(rule[3:]), rule[1:3] == ">="))
    families = {f: [] for f in run.families()}
    skipped_no_label = skipped_no_scale = 0
    diag = {(f, t): {"rows": 0, "z": 0, "raw_by_year": {}, "z_by_year": {}} for f in run.features for t in run.decision_times}
    prev_day = None
    for day in days:
        session = sessions[day]
        prior_day, prev_day = prev_day, day  # fixture-only fallback; run() always supplies pinned calendar
        if pending_pair is not None:  # the previous session's daily returns join the betas only now
            pairs.append(pending_pair)
            pending_pair = None
        if pending_minutes is not None:  # likewise the previous session's minute volumes join the tape base
            minute_hist.append(pending_minutes)
            pending_minutes = None
        schedule = None
        if calendar is not None:
            schedule = calendar.for_day(day)
            if schedule is None:
                continue
            prior_day = calendar.previous_day(day)
        if need_w1:
            pending_minutes = {hm: b[4] for hm, b in session.bars.items()}
        if need_betas and prior_day is not None:
            today = [settled[s].get(day) for s in ("SPY", "VIXY", "HYG")]
            before = [settled[s].get(prior_day) for s in ("SPY", "VIXY", "HYG")]
            if all(today) and all(before):
                pending_pair = tuple(a / b - 1 for a, b in zip(today, before))
        if calendar is not None:
            prior_schedule = calendar.for_day(prior_day)
            # Conservative protocol: shortened priors remain withheld even if
            # a vendor incorrectly supplies fabricated after-close minute bars.
            if (prior_schedule.closes - prior_schedule.opens).total_seconds() != 390 * 60:
                skipped_no_label += len(run.families())
                continue
        prior = sessions.get(prior_day) if prior_day is not None else None
        if prior is None:
            continue
        # Without an authenticated historical session calendar, a short prior
        # may be a half-day or a broken capture. Never substitute an older day.
        expected_prior = {_hm(i) for i in range(9 * 60 + 30, 16 * 60)}
        if set(prior.hms) != expected_prior:
            skipped_no_label += len(run.families())
            continue
        px = {sym: by_day.get(day) for sym, by_day in bars_of.items()}
        prior_close = {sym: (prior.last_close if sym == run.symbol else settled[sym].get(prior_day))
                       for sym in bars_of}
        betas = {}
        if need_betas and len(pairs) == pairs.maxlen:
            spy = [q[0] for q in pairs]
            betas = {"VIXY": ols_slope(spy, [q[1] for q in pairs]), "HYG": ols_slope(spy, [q[2] for q in pairs])}
        s6 = month_end(day, session_list, settled) if need_s6 else None
        markers = None
        if need_w1 and len(minute_hist) == minute_hist.maxlen:
            minute_base = {}
            for hm in session.hms:
                seen = [h[hm] for h in minute_hist if h.get(hm) is not None]
                if hm <= last_bar and len(seen) >= minute_hist.maxlen // 2:  # edge8: >= 10 of 20
                    minute_base[hm] = statistics.median(seen)
            markers = w1_events(session, minute_base, last_bar)
        for t in run.decision_times:
            if schedule is not None and _minutes(t) >= _minutes(schedule.closes.astimezone(NY).strftime('%H:%M')):
                continue
            bar = _hm(_minutes(t) - 1)
            # Only bars whose intervals have closed by T may qualify signals.
            # A missing later entry, exit, or post-exit bar cannot change z history.
            required = {_hm(i) for i in range(9 * 60 + 30, _minutes(t))}
            if not required.issubset(session.bars):
                skipped_no_label += len(run.horizons)
                continue
            eligible.add(day)
            decided = datetime.fromisoformat(f"{day}T{t}").replace(tzinfo=NY)
            raw = features_at(session, prior, px, prior_close, betas, s6, bar, price_wanted, run.stale_minutes)
            if need_tape:
                window = tape_window(session, bar)
                base = {}
                for q in TAPE_QUANTITIES:
                    past_q = tape_base[(q, t)]
                    base[q] = statistics.median(past_q) if len(past_q) == past_q.maxlen else None
                    if window[q] is not None:
                        past_q.append(window[q])  # after use: a session never sets its own baseline
                raw.update(tape_features(window, base, markers, bar))
            if need_cal:
                raw.update(calendar_features(day, decided, following.get(day), opex, events))
            if options_wanted:
                raw.update(options.features_at(day, session, t, decided, options_wanted))
            if macro_wanted:
                raw.update(macro.features_at(day, decided, macro_wanted, prior_day=prior_day))
            raw = {f: raw.get(f) for f in run.features}
            z = {f: None if f in FLAGS else robust_z(raw[f], history[(f, t)]) for f in run.features}
            for f in run.features:
                d = diag[(f, t)]
                d["rows"] += 1
                if raw[f] is not None:
                    if f not in FLAGS:
                        history[(f, t)].append(raw[f])
                    d["raw_by_year"].setdefault(day[:4], []).append(raw[f])
                if z[f] is not None or (f in FLAGS and raw[f] is not None):
                    d["z"] += 1
                if z[f] is not None:
                    d["z_by_year"].setdefault(day[:4], []).append(z[f])
            entry_bar = _hm(_minutes(t) + 1)
            entry = session.bars[entry_bar][0] if entry_bar in session.bars else None
            signals = {}
            for name, feature, rule, side, k, ge in parsed:
                if k is None:
                    fires = raw[feature] == 1.0
                else:
                    zf = z[feature]
                    fires = zf is not None and (zf >= k if ge else zf <= -k)
                signals[name] = {"value": SIDES[side] if fires else None, "known_at": decided.isoformat()}
            for h in run.horizons:
                if f"{t}+{h}" in skip:
                    continue
                if h == "close":
                    exit_bar, ends = "15:59", decided.replace(hour=16, minute=0)
                else:
                    exit_bar = _hm(_minutes(bar) + int(h[:-1]))
                    ends = decided + timedelta(minutes=int(h[:-1]))
                if schedule is not None and ends > schedule.closes:
                    skipped_no_label += 1
                    continue
                exit_px = session.bars[exit_bar][3] if exit_bar in session.bars else None
                if not entry or not exit_px:
                    skipped_no_label += 1
                    continue
                family = f"SPY|change|{t}+{h}"
                change = float(exit_px - entry)
                past = moves[family]
                if run.target_unit == "vol":
                    scale = statistics.median(past) if len(past) == past.maxlen else None
                elif run.target_unit == "bps":
                    scale = entry / 1e4
                else:
                    scale = 1.0
                past.append(abs(change))  # after use: a session never scales itself
                if not scale:
                    skipped_no_scale += 1
                    continue
                families[family].append({
                    "origin": ORIGIN,
                    "decision_at": decided.isoformat(),
                    "entry_at": (decided + timedelta(minutes=1)).isoformat(),
                    "entry_price_basis": "next_minute_open_proxy",
                    "label_end": ends.isoformat(),
                    "target_known_at": ends.isoformat(),
                    "horizon": f"{t}+{h}",
                    "label": "change",
                    "target": change / scale,
                    "cost": run.cost_per_share / scale,
                    "target_dollars": change,
                    "features": signals,
                })
    coverage = {
        "sessions_read": len(days),
        "symbols": {
            sym: {"sessions": len(by_day), "first": min(by_day, default=None), "last": max(by_day, default=None),
                  "median_bars": statistics.median(len(v.hms) for v in by_day.values()) if by_day else None}
            for sym, by_day in bars_of.items()
        },
        "stale_minutes": run.stale_minutes,
        "tape_history": run.tape_history,
        "skip_windows": list(run.skip_windows),
        "event_calendar": events.version if events is not None else None,
        **({"options": options.coverage()} if options_wanted else {}),
        **({"macro": macro.coverage()} if macro_wanted else {}),
        "sessions_eligible": len(eligible),
        "sessions_below_full_bars_diagnostic": len(days) - len(full_coverage),
        "full_bars_role": "retrospective coverage only; never eligibility",
        "signal_eligibility": "complete 09:30 through T-1 prefix; prior session complete; shortened prior withheld",
        "calendar_qualification": calendar.version if calendar else "UNQUALIFIED: prior observed day is not verified against exchange calendar",
        "first_eligible": min(eligible) if eligible else None,
        "last_eligible": max(eligible) if eligible else None,
        "rows_per_family": {f: len(r) for f, r in families.items()},
        "labels_missing": skipped_no_label,
        "labels_unscaled": skipped_no_scale,
        "target_unit": run.target_unit,
        "entry_rule": "open at T+1 minute; quote fills/slippage not established",
        "features": {
            f"{f}@{t}": {
                "coverage_z": round(d["z"] / d["rows"], 4) if d["rows"] else None,
                "median_raw_by_year": {y: round(statistics.median(v), 6) for y, v in sorted(d["raw_by_year"].items())},
                "median_z_by_year": {y: round(statistics.median(v), 3) for y, v in sorted(d["z_by_year"].items())},
                "z_drift": round(max(m := [statistics.median(v) for v in d["z_by_year"].values()]) - min(m), 3)
                if d["z_by_year"] else None,
            }
            for (f, t), d in diag.items()
        },
    }
    return families, coverage


def protocol_for(run: RunFile) -> FactoryProtocol:
    first = datetime.fromisoformat(f"{run.first_session}T00:00").replace(tzinfo=NY)
    split = datetime.fromisoformat(f"{run.last_session}T00:00").replace(tzinfo=NY) + timedelta(days=1)
    return FactoryProtocol(
        run_id=run.run_id,
        features=run.modules(),
        families=run.families(),
        start=first.isoformat(),
        split=split.isoformat(),
        end=(split + timedelta(days=1)).isoformat(),
        min_n=run.min_n,
        origin=ORIGIN,
        sampling="horizon_spaced",
        perms=run.perms,
        seed=run.seed,
        statistic=NET_WHEN_FIRES,
        selection=MAX_T_SELECTION,
        selection_alpha=run.alpha,
        cost=0.0,  # every row carries its own cost in the run's target unit
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_data(data_dir: Path) -> tuple[dict, str]:
    """(manifest, manifest sha256) of a staged dev data folder; refuses a holdout path."""
    if "holdout" in str(data_dir).lower():
        raise RunFileError(f"holdout path refused: {data_dir}")
    manifest_path = data_dir / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("dev_first", "") < DEV_FIRST:
        raise RunFileError("the staged manifest starts before dev: holdout refused")
    return manifest, sha256_file(manifest_path)


def verify_data(data_dir: Path, manifest: dict, needed=(DATA_FILE,)) -> None:
    files = {f["path"]: f for f in manifest["files"]}
    for name in needed:
        entry = files.get(name)
        if entry is None:
            raise RunFileError(f"{name} is not in the staged manifest")
        if sha256_file(data_dir / name) != entry["sha256"]:
            raise RunFileError(f"{name} does not match its manifest sha256")


def dollars_net(families: dict, trials: list[dict], cost: float) -> dict:
    """trial_id -> mean net in dollars per share over its fires (side * change - cost)."""
    out = {}
    for t in trials:
        fired = [(row["features"][t["feature"]]["value"], row["target_dollars"])
                 for row in families[t["family"]] if row["features"][t["feature"]]["value"] is not None]
        out[t["trial_id"]] = (sum(s * d for s, d in fired) / len(fired) - cost) if fired else None
    return out


def signal_name(family: str, feature: str) -> str:
    """The decision signal a module trades: ``decision time|feature|z rule``. Its long and short sides and
    its horizons share it, because they fire on the same decisions."""
    t = family.split("|")[2].split("+")[0]
    f, rule, _side = feature.split("|")
    return f"{t}|{f}|{rule}"


RANKED = ("trial_id", "family", "feature", "n", "r", "net_dollars", "t", "p", "adjusted_p", "selected")
MARGINAL = 0.05  # the bucket hit rate: share of tested modules with marginal p <= this (chance: about this)


def buckets(run: RunFile, trials: list[dict]) -> dict:
    """Every bucket of the run's tested modules: count, marginal p <= 0.05 against chance, best t and adjusted p.

    Buckets: class, decision time (the time-of-day dimension), horizon, feature, z rule and side, and for a run
    with options features their measurement basis (oi-unsigned, gamma-unsigned, gamma-signed-assumed), and for a
    run with macro features their basis (spy-bars, unrevised-latest, alfred-vintage) and group. Reported
    for every bucket, never only the good ones; trials are correlated, so chance is a rough guide, not a test."""
    keys = {
        "class": lambda t: FEATURE_CLASS[t["feature"].split("|")[0]],
        "decision_time": lambda t: t["family"].split("|")[2].split("+")[0],
        "horizon": lambda t: t["family"].split("+")[1],
        "feature": lambda t: t["feature"].split("|")[0],
        "rule": lambda t: t["feature"].split("|")[1],
        "side": lambda t: t["feature"].split("|")[2],
    }
    if any(FEATURE_CLASS[f] in ("options", "macro") for f in run.features):
        keys["basis"] = lambda t: REGISTRY[t["feature"].split("|")[0]].get("basis", FEATURE_CLASS[t["feature"].split("|")[0]])
    if any(FEATURE_CLASS[f] == "macro" for f in run.features):
        keys["group"] = lambda t: REGISTRY[t["feature"].split("|")[0]].get("group", FEATURE_CLASS[t["feature"].split("|")[0]])
    out = {}
    for by, key in keys.items():
        groups: dict[str, list] = {}
        for t in trials:
            groups.setdefault(key(t), []).append(t)
        rows = {}
        for name, members in groups.items():
            tested = [t for t in members if t["status"] == "tested"]
            hits = sum(t["p"] <= MARGINAL for t in tested)
            rows[name] = {
                "declared": len(members),
                "tested": len(tested),
                "p_le_05": hits,
                "chance": round(MARGINAL * len(tested), 1),
                "hit_rate": round(hits / len(tested), 4) if tested else None,
                "positive_t": sum(t["t"] > 0 for t in tested),
                "best_t": round(max(t["t"] for t in tested), 3) if tested else None,
                "best_adjusted_p": round(min(t["adjusted_p"] for t in tested), 4) if tested else None,
                "selected": sum(bool(t["selected"]) for t in tested),
            }
        out[by] = dict(sorted(rows.items()))
    return out


def class_table(run: RunFile, trials: list[dict]) -> dict:
    """Per feature class: features, modules, the run file's declared share and the actual share of modules."""
    counts = run.class_counts()
    total = sum(counts.values())
    declared = run.class_shares or {}
    out = {}
    for cls in CLASSES:
        if cls not in counts:
            continue
        members = [t for t in trials if FEATURE_CLASS[t["feature"].split("|")[0]] == cls]
        tested = [t for t in members if t["status"] == "tested"]
        out[cls] = {
            "features": [f for f in run.features if FEATURE_CLASS[f] == cls],
            "modules": counts[cls],
            "share": round(counts[cls] / total, 4),
            "share_declared": declared.get(cls),
            "tested": len(tested),
            "share_of_tested": round(len(tested) / max(1, sum(t["status"] == "tested" for t in trials)), 4),
            "selected": sum(bool(t["selected"]) for t in tested),
        }
    return out


def report(run: RunFile, frozen: dict, coverage: dict, families: dict, top: int = 25, cofire=None) -> dict:
    """The run report. ``cofire``: :func:`analysis.factory_stats.cofiring`'s result (computed here if not
    given); its matrix is written beside the report as ``cofiring.json`` and only its hash is held here."""
    payload = frozen["payload"]
    trials = payload["ledger"]
    dollars = dollars_net(families, trials, run.cost_per_share)
    for t in trials:
        t["net_dollars"] = dollars[t["trial_id"]]
    matrix, clusters, membership = cofire or cofiring(
        families, trials, signal_name, by=run.cluster_by, threshold=run.cluster_threshold)
    for t in trials:
        t["cluster"], t["representative"] = membership[t["trial_id"]]
        t["class"] = FEATURE_CLASS[t["feature"].split("|")[0]]
    by_id = {t["trial_id"]: t for t in trials}

    def rep_row(trial_id):
        if trial_id is None:
            return None
        t = by_id[trial_id]
        return {**{k: t[k] for k in RANKED}, "class": t["class"],
                "live_source": REGISTRY[t["feature"].split("|")[0]]["live_source"]}

    cluster_rows = [{**c, "representative": rep_row(c["representative"])} for c in clusters]
    tested = sorted((t for t in trials if t["status"] == "tested"), key=lambda t: -t["t"])
    return {
        "run_id": run.run_id,
        "state": "INTRADAY_DEV_RANKING_ONLY",
        "promotion_allowed": False,
        "frozen_sha256": frozen["sha256"],
        "method": payload["method"],
        "target_unit": run.target_unit,
        "feature_specs": {f: REGISTRY[f] for f in run.features},
        "classes": class_table(run, trials),
        "coverage": coverage,
        "trial_count": payload["trial_count"],
        "tested": len(tested),
        "untestable": payload["untestable_count"],
        "max_t": payload["max_t"],
        "selected": [t["trial_id"] for t in tested if t["selected"]],
        "ranking": [
            {**{k: t[k] for k in RANKED}, "class": t["class"], "cluster": t["cluster"],
             "representative": t["representative"]}
            for t in tested[:top]
        ],
        "buckets": buckets(run, trials),
        "cofiring": {
            "file": "cofiring.json",
            "sha256": digest(matrix),
            "by": matrix["by"],
            "threshold": matrix["threshold"],
            "basis": matrix["basis"],
            "linkage": "single (connected components): signals at or above the threshold share a cluster, "
                       "so no two representatives co-fire at or above it",
            "signals": len(matrix["signals"]),
            "sessions": matrix["sessions"],
            "clusters": len(clusters),
            "clusters_with_tested_trials": sum(c["representative"] is not None for c in clusters),
            "representatives_selected": sum(bool(c["representative"] and c["representative"]["selected"])
                                            for c in cluster_rows),
        },
        "clusters": cluster_rows,
        "note": "dev ranking only; a selected module is a candidate for a panel-factory live slot, not a finding; "
                "one cluster takes at most one slot, through its representative",
    }


def report_markdown(rep: dict) -> str:
    mt = rep["max_t"]
    lines = [
        f"# Factory run {rep['run_id']}",
        "",
        f"State: {rep['state']} (ranking only; forward live recording is the test).",
        "",
        f"- Modules declared {rep['trial_count']}, tested {rep['tested']}, untestable {rep['untestable']}.",
        (
            f"- Sessions eligible {rep['coverage']['sessions_eligible']} "
            f"({rep['coverage']['first_eligible']}..{rep['coverage']['last_eligible']}), "
            f"below full-day coverage threshold (diagnostic only) {rep['coverage']['sessions_below_full_bars_diagnostic']}."
        ),
        (
            f"- Bar: best-of-shuffled critical t {mt['critical_t']:.3f} at the run alpha "
            f"(block {mt['block']}, {mt['sessions']} sessions)."
            if mt.get("critical_t") is not None
            else "- Bar: none (nothing tested)."
        ),
        f"- Selected {len(rep['selected'])} (adjusted p <= alpha and mean net > 0).",
        f"- Calendar: {rep['coverage']['calendar_qualification']}.",
        "",
        f"Target unit: {rep['target_unit']}; mean net is in that unit, and also in dollars per share.",
        "",
        f"| # | family | module | fires | mean net ({rep['target_unit']}) | $/share | t | p | adjusted p | selected | cluster |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(rep["ranking"], 1):
        lines.append(
            f"| {i} | {r['family']} | {r['feature']} | {r['n']} | {r['r']:+.4f} | {r['net_dollars']:+.4f} | "
            f"{r['t']:.2f} | {r['p']:.4f} | {r['adjusted_p']:.4f} | {'yes' if r['selected'] else ''} | "
            f"{r['cluster']}{' (rep)' if r['representative'] else ''} |"
        )
    cf = rep["cofiring"]
    lines += [
        "",
        (f"Clusters by fire overlap ({cf['by']} >= {cf['threshold']:g}, {cf['linkage'].split(':')[0]}; returns never "
         f"enter): {cf['clusters']} clusters over {cf['signals']} decision signals and {cf['sessions']} sessions; "
         f"{cf['clusters_with_tested_trials']} hold a tested trial; {cf['representatives_selected']} representatives "
         f"cleared the bar. Matrix: `{cf['file']}` (sha256 {cf['sha256'][:12]})."),
        "",
        "| cluster | signals | weakest pair | representative | fires | t | adjusted p | selected | live source |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for c in rep["clusters"][:25]:
        r = c["representative"]
        if r is None:
            continue
        shown = ", ".join(c["signals"][:4]) + (f" (+{len(c['signals']) - 4})" if len(c["signals"]) > 4 else "")
        lines.append(
            f"| {c['cluster']} | {shown} | {c['min_pair_similarity']} | {r['family']} {r['feature']} | {r['n']} | "
            f"{r['t']:.2f} | {r['adjusted_p']:.4f} | {'yes' if r['selected'] else ''} | {r['live_source'].split(':')[0]} |"
        )
    lines += ["", "| class | features | modules | share | declared share | tested | selected |",
              "|---|---|---|---|---|---|---|"]
    for cls, c in rep["classes"].items():
        lines.append(f"| {cls} | {len(c['features'])} | {c['modules']} | {c['share']:.3f} | "
                     f"{'' if c['share_declared'] is None else c['share_declared']} | {c['tested']} | {c['selected']} |")
    lines += ["", f"Buckets: tested modules with marginal p <= {MARGINAL} against chance (trials are correlated).", ""]
    for by, rows in rep["buckets"].items():
        lines += [f"| {by} | tested | p <= {MARGINAL} | chance | best t | best adjusted p |", "|---|---|---|---|---|---|"]
        for name, b in rows.items():
            lines.append(f"| {name} | {b['tested']} | {b['p_le_05']} | {b['chance']} | {b['best_t']} | "
                         f"{b['best_adjusted_p']} |")
        lines.append("")
    opt = rep["coverage"].get("options")
    if opt:
        pin = opt["levels_function"]
        lines += ["", (f"Options class: {pin['function']} from {pin['repo']} {pin['commit'][:8]} (levels.py sha256 "
                       f"{pin['files']['levels.py']['sha256'][:12]}), level rule {opt['level_rule']['rule']}; gamma "
                       f"{opt['formula']['id']} ({opt['formula']['unit']}). Walls and signed gamma are "
                       "measurement-unqualified diagnostics (#1132, #1108 open)."),
                  "", "| options input | count |", "|---|---|"]
        lines += [f"| {k} | {v} |" for k, v in opt["counts"].items()]
    mac = rep["coverage"].get("macro")
    if mac:
        snap = mac["snapshot"]
        lines += ["", (f"Macro class: snapshot {snap['schema']} (content sha256 {snap['content_sha256'][:12]}, "
                       f"{snap['window']['first']}..{snap['window']['last']}); a value counts only when its "
                       f"publication rule puts it before T, a revised series only through its vintages; stale "
                       f"after {mac['stale_days']} days."),
                  "", "| series | observations | first | last | revision | rule |", "|---|---|---|---|---|---|"]
        for sid, m in mac["series"].items():
            lines.append(f"| {sid} | {m['observations']} | {m['first']} | {m['last']} | {m['revision']} | "
                         f"{m['rule'].split(';')[0]} |")
        lines += ["", "| macro feature | decisions | value | not known | stale | history | missing | first value |",
                  "|---|---|---|---|---|---|---|---|"]
        for f, c in mac["features"].items():
            lines.append(f"| {f} | {c['decisions']} | {c['value']} | {c['not_known']} | {c['stale']} | "
                         f"{c['history']} | {c['missing']} | {c['first_value']} |")
    lines += ["", "| feature @ time | coverage | z drift (max - min yearly median z) |", "|---|---|---|"]
    for name, f in rep["coverage"]["features"].items():
        lines.append(f"| {name} | {f['coverage_z']} | {f['z_drift']} |")
    lines += ["", f"Method: {rep['method']}", ""]
    return "\n".join(lines)


def open_ledger(ledger_path: Path, anchor_path: Path, genesis: str | None) -> lse.Ledger:
    if genesis:
        return lse.Ledger.create_factory(ledger_path, ledger_id=genesis, anchor=anchor_path)
    ledger = lse.Ledger(ledger_path, anchor=anchor_path)
    if not ledger.is_factory:
        raise RunFileError("that ledger is not a factory ledger")
    return ledger


def run(run_file: Path, data_dir: Path, ledger: lse.Ledger, out: Path, oi_archive: Path | None = None,
        macro_snapshot: Path | None = None) -> dict:
    """One factory run, declared on ``ledger`` before any data is read. Returns the report.

    ``oi_archive``: the frozen OCC archive copy, for runs with OI-archive features; its sha256 must equal the run
    file's ``oi_archive_sha256`` (checked after the declaration and before it is read). ``macro_snapshot``: the
    frozen factory-macro/1 snapshot, for runs with macro features, checked the same way against ``macro_sha256``."""
    body = json.loads(Path(run_file).read_text())
    spec = RunFile.from_dict(body)
    calendar = FactoryCalendar()
    calendar.validate_window(spec.first_session, spec.last_session)
    events = FactoryEvents() if set(spec.features) & EVENT_FEATURES else None  # pinned; checked before declaring
    options_wanted = {f for f in spec.features if FEATURE_CLASS[f] == "options"}
    levels = fo.load_levels() if options_wanted else None  # vendored and hash-pinned; checked before declaring
    if bool(spec.oi_archive_sha256) != (oi_archive is not None):
        raise RunFileError("an OCC archive copy is passed with, and only with, a run file's oi_archive_sha256")
    if bool(spec.macro_sha256) != (macro_snapshot is not None):
        raise RunFileError("a macro snapshot is passed with, and only with, a run file's macro_sha256")
    manifest, manifest_sha = check_data(Path(data_dir))
    out = Path(out)
    out.mkdir(parents=True, exist_ok=False)
    lse.record_factory_declared(
        ledger, run_id=spec.run_id, run_file=body, data_manifest_sha256=manifest_sha,
        trial_count=spec.trial_count(),
    )
    try:
        need_gexhist = bool(options_wanted - set(fo.ARCHIVE_FEATURES))
        verify_data(Path(data_dir), manifest, needed=(DATA_FILE, fo.DATA_FILE) if need_gexhist else (DATA_FILE,))
        options = None
        if options_wanted:
            if oi_archive is not None and sha256_file(Path(oi_archive)) != spec.oi_archive_sha256:
                raise RunFileError("the OCC archive copy does not match the run file's oi_archive_sha256")
            options = fo.OptionsInputs(
                fo.GexHist(Path(data_dir) / fo.DATA_FILE, spec.decision_times) if need_gexhist else None,
                fo.OIArchive(Path(oi_archive)) if oi_archive is not None else None, levels=levels)
        bars = load_bars(Path(data_dir) / DATA_FILE, spec)
        sessions = bars.pop(spec.symbol)
        macro = None
        if spec.macro_sha256:
            if sha256_file(Path(macro_snapshot)) != spec.macro_sha256:
                raise RunFileError("the macro snapshot does not match the run file's macro_sha256")
            macro = fm.MacroInputs(fm.MacroSnapshot(Path(macro_snapshot)),
                                   {d: settled_close(s) for d, s in sessions.items()},
                                   stale_days=spec.macro_stale_days)
        families, coverage = build_rows(sessions, spec, calendar=calendar, cross=bars, events=events,
                                        options=options, macro=macro)
        frozen = discover(protocol_for(spec), families)
        write_once(out / "discovery-frozen.json", frozen)
        cofire = cofiring(families, frozen["payload"]["ledger"], signal_name,
                          by=spec.cluster_by, threshold=spec.cluster_threshold)
        write_once(out / "cofiring.json", cofire[0])
        rep = report(spec, frozen, coverage, families, cofire=cofire)
        write_once(out / "report.json", rep)
        with (out / "report.md").open("x", encoding="utf-8") as stream:
            stream.write(report_markdown(rep))
    except Exception as e:  # the declared run is closed, never left open
        lse.record_factory_abandoned(ledger, reason=f"{type(e).__name__}: {e}")
        raise
    trials = [
        {k: t.get(k) for k in ("trial_id", "family", "feature", "status", "n", "r", "net_dollars", "t", "p",
                               "adjusted_p", "selected", "cluster", "representative", "class")}
        for t in frozen["payload"]["ledger"]
    ]
    lse.record_factory_result(
        ledger, trials=trials, max_t=frozen["payload"]["max_t"],
        report_sha256=digest(rep), frozen_sha256=frozen["sha256"],
    )
    return rep


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--run-file", required=True, type=Path)
    r.add_argument("--data-dir", required=True, type=Path)
    r.add_argument("--ledger", required=True, type=Path)
    r.add_argument("--anchor", required=True, type=Path)
    r.add_argument("--out", required=True, type=Path)
    r.add_argument("--genesis", metavar="LEDGER_ID", help="create the factory ledger first (once)")
    r.add_argument("--oi-archive", type=Path, help="frozen OCC archive copy (runs with OI-archive features)")
    r.add_argument("--macro", type=Path, help="frozen factory-macro/1 snapshot (runs with macro features)")
    s = sub.add_parser("summary")
    s.add_argument("--ledger", required=True, type=Path)
    s.add_argument("--anchor", required=True, type=Path)
    v = sub.add_parser("verdicts", help="append the final live verdicts in a gex-signals panel-factory export (#1162)")
    v.add_argument("--file", required=True, type=Path)
    v.add_argument("--ledger", required=True, type=Path)
    v.add_argument("--anchor", required=True, type=Path)
    g = sub.add_parser("prescreen-register", help="append a pre-screen registration to the ledger; reads no older data")
    g.add_argument("--registration", required=True, type=Path)
    g.add_argument("--ledger", required=True, type=Path)
    g.add_argument("--anchor", required=True, type=Path)
    p = sub.add_parser("prescreen", help="the one older-data look of a registration already on the ledger")
    p.add_argument("--registration", required=True, type=Path)
    p.add_argument("--data", required=True, type=Path, help="the older-data bars file named by the registration's sha256")
    p.add_argument("--ledger", required=True, type=Path)
    p.add_argument("--anchor", required=True, type=Path)
    p.add_argument("--witness-records", required=True, type=int,
                   help="record count of a head of this ledger, at or after the registration, that was recorded off-host")
    p.add_argument("--witness-head", required=True, help="that head's sha256")
    p.add_argument("--witness-where", required=True, help="where the off-host copy is (for example the Desk receipt hub post)")
    w = sub.add_parser("verdicts-witness", help="mark live verdicts as witnessed by a Desk receipt that read the same stored look")
    w.add_argument("--payload", required=True, type=Path, help="the receipt's panel_trials payload file")
    w.add_argument("--receipt-session", required=True)
    w.add_argument("--receipt-head", required=True, help="the receipt record's head sha256, as posted off-host")
    w.add_argument("--ledger", required=True, type=Path)
    w.add_argument("--anchor", required=True, type=Path)
    a = ap.parse_args(argv)
    if a.cmd == "verdicts-witness":
        try:
            ledger = lse.Ledger(a.ledger, anchor=a.anchor)
            appended = lse.witness_factory_verdicts(ledger, a.payload.read_bytes(), receipt_session=a.receipt_session,
                                                    receipt_head_sha256=a.receipt_head)
        except (ValueError, OSError) as e:
            print(json.dumps({"ok": False, "refused": str(e)}))
            return 2
        live = lse.summarize(ledger)["live"]
        print(json.dumps({"ok": True, "witnessed_now": appended, "witnessed": live["witnessed"], "unwitnessed": live["unwitnessed"],
                          "head_sha256": ledger.head}))
        return 0
    if a.cmd in ("prescreen", "prescreen-register"):
        from analysis import factory_prescreen  # imported here: that module imports this one

        try:
            ledger = lse.Ledger(a.ledger, anchor=a.anchor)
            if a.cmd == "prescreen-register":
                done = factory_prescreen.register(a.registration, ledger)
                print(json.dumps({"ok": True, "registered_sha256": done["sha256"], "seq": done["record"]["seq"],
                                  "head_sha256": ledger.head, "records": len(ledger.records),
                                  "rule": done["record"]["rule"], "store": done["record"]["store"]["id"],
                                  "next": "the look needs a head at or after this record that was recorded off-host"}))
                return 0
            witness = {"ledger_records": a.witness_records, "ledger_head_sha256": a.witness_head, "where": a.witness_where}
            done = factory_prescreen.prescreen(a.registration, a.data, ledger, witness)["record"]
        except (ValueError, OSError) as e:
            print(json.dumps({"ok": False, "refused": str(e)}))
            return 2
        left = lse.summarize(ledger)["live"]["prescreen"]["stores"][done["store"]["id"]]["left"]
        print(json.dumps({"ok": True, "registrations_left": left, "head_sha256": ledger.head,
                          **{k: done[k] for k in ("verdict", "n", "mean_net", "lower_bound", "baseline", "excess_mean",
                                                  "excess_lower_bound", "trimmed_mean_net", "top_share", "by_year",
                                                  "rule", "evidence")}}))
        return 0
    if a.cmd == "summary":
        print(json.dumps(lse.summarize(lse.Ledger(a.ledger, anchor=a.anchor)), indent=1))
        return 0
    if a.cmd == "verdicts":
        try:
            ledger = lse.Ledger(a.ledger, anchor=a.anchor)
            appended = lse.ingest_factory_verdicts(ledger, a.file)
        except (ValueError, OSError) as e:
            print(json.dumps({"ok": False, "refused": str(e)}))
            return 2
        print(json.dumps({"ok": True, "appended": appended, "head_sha256": ledger.head, "live": lse.summarize(ledger)["live"]}))
        return 0
    try:
        ledger = open_ledger(a.ledger, a.anchor, a.genesis)
        rep = run(a.run_file, a.data_dir, ledger, a.out, oi_archive=a.oi_archive, macro_snapshot=a.macro)
    except (RunFileError, fo.OptionsDataError, fm.MacroDataError) as e:
        print(json.dumps({"ok": False, "refused": str(e)}))
        return 2
    print(json.dumps({"ok": True, "run_id": rep["run_id"], "trials": rep["trial_count"], "tested": rep["tested"],
                      "selected": len(rep["selected"]), "critical_t": rep["max_t"]["critical_t"], "out": str(a.out)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
