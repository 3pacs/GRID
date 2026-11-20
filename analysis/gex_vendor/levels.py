"""Level book: granular floors and ceilings from dealer gamma positioning plus intraday structure.

Inputs (all already present in gex-watch STATE; nothing new is polled):
  * chain rows   — Cboe delayed SPY chain mapped by cboe_chain.py (all expiries; OI, IV, gamma, volume per contract)
  * rtd rows     — live RTD rows for the subscribed strikes (broker feed rows: iv, open_interest, provider_gamma)
  * spot         — live RTD price when the leg is live, else the public price
  * bars         — today's 1-minute bars (t, c, h, l, v) and prior-session reference points from quote()

Method (documented, deterministic, no fitting):
  * Dealer gamma exposure per contract: gamma × OI × 100 × S² × 0.01 (USD per 1% spot move), calls +, puts −
    (standard dealer-long-calls / short-puts convention; an assumption, not observed inventory). Provider gamma is used
    where present; otherwise Black-Scholes gamma from the contract's IV. RTD rows override Cboe rows for the same
    contract (live IV/OI/gamma), and are tagged `rtd`.
  * Contract volume (2026-10-26 follow-up): an RTD row's own `volume` (thinkorswim cumulative day volume, live) replaces
    the Cboe value for that contract; any contract without one keeps the Cboe delayed volume. Each contract carries
    `volume_src` ('rtd' | 'cboe_delayed' | None when neither has a number); per strike, `volume_rtd` and `volume_cboe` are
    the two sums, `volume` their total and `volume_src` says 'rtd', 'cboe_delayed' or 'mixed' — never mixed silently, and an
    absent volume stays None (never 0).
  * Horizons: 0DTE (today's expiry), WEEK (expiries within 7 calendar days), MONTH (through the next monthly third-Friday
    expiry), ALL. Each horizon gets its own book, so a 0DTE wall and a monthly wall are never confused.
  * Per horizon: per-strike call/put/net GEX; CEILINGS = strikes above spot ranked by call GEX (R1..R3); FLOORS = strikes
    below spot ranked by |put GEX| (S1..S3); call wall / put wall = R1 / S1 over the whole horizon; HEDGE WALL = strike with
    the largest |call|+|put| gamma; FLIP = zero crossing of the net gamma profile nearest spot, computed from BS gamma over a
    spot grid, plus the same crossing with IV +20% / +40% (the flip band); MAX PAIN (nearest expiry of the horizon);
    AIR POCKETS = gaps > $2 between significant strikes (|net| ≥ 10% of the horizon max).
  * Structure (from bars): VWAP (cumulative typical × volume), opening range 15/30 min high/low, session high/low,
    prior-session high/low/close, pre-market high/low. CONFLUENCE marks a gamma level within $0.75 of any structure level.
  * STRENGTH 0–100 per level: 60 × (|GEX| / horizon max |GEX|) + 10 per other horizon where the same strike is a wall
    (max 20) + 10 per confluence (max 20). A documented heuristic for ranking, not a probability.
All times are UTC epoch seconds; the page formats. Pure functions; no I/O; never raises into the server loop except via
the caller's try/except. Nothing here predicts direction: it describes where hedging pressure is concentrated.
"""
import math
import volume_quality
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')
RATE, DIVY = 0.04, 0.012
MULT = 100.0
T_FLOOR = 5.0 / (60 * 24 * 365)   # 5 minutes in years: at-the-money 0DTE gamma would otherwise blow up into the bell (2026-11-05)


def _num(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def bs_gamma(S, K, T, iv):
    if S <= 0 or K <= 0 or T <= 0 or not iv or iv <= 0:
        return 0.0
    d1 = (math.log(S / K) + (RATE - DIVY + 0.5 * iv * iv) * T) / (iv * math.sqrt(T))
    return math.exp(-DIVY * T) * math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi) / (S * iv * math.sqrt(T))


def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_delta(S, K, T, iv, side):
    """Black-Scholes delta with the book's RATE/DIVY. T <= 0 is settlement: 1/0 (call), -1/0 (put), 0.5 / -0.5 at the strike."""
    if T <= 0 or not iv or iv <= 0:
        if S > K:
            return 1.0 if side == 'call' else 0.0
        if S < K:
            return 0.0 if side == 'call' else -1.0
        return 0.5 if side == 'call' else -0.5
    d1 = (math.log(S / K) + (RATE - DIVY + 0.5 * iv * iv) * T) / (iv * math.sqrt(T))
    c = math.exp(-DIVY * T) * _ncdf(d1)
    return c if side == 'call' else c - math.exp(-DIVY * T)


def bs_vanna_charm(S, K, T, iv, side):
    """(vanna, charm) for one option. vanna = d(delta)/d(iv) per 1.00 of iv (same for calls and puts); charm = d(delta)/dt
    per year of calendar time passing (spot and iv fixed), i.e. minus d(delta)/dT. Zero when inputs are unusable."""
    if S <= 0 or K <= 0 or T <= 0 or not iv or iv <= 0:
        return 0.0, 0.0
    sq = math.sqrt(T)
    d1 = (math.log(S / K) + (RATE - DIVY + 0.5 * iv * iv) * T) / (iv * sq)
    d2 = d1 - iv * sq
    eq = math.exp(-DIVY * T)
    pdf = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    vanna = -eq * pdf * d2 / iv
    common = eq * pdf * (2 * (RATE - DIVY) * T - d2 * iv * sq) / (2 * T * iv * sq)
    charm_call = DIVY * eq * _ncdf(d1) - common
    return vanna, (charm_call if side == 'call' else charm_call - DIVY * eq)


# Second-order hedging flows (Anik 2026-11-13, "build charm and vanna exposure on the gex page"). Same assumed dealer
# position as GEX (long calls, short puts: +1 per call contract, -1 per put contract) and the same 100-share multiplier.
# Each number is the stock dealers would BUY (+) or SELL (-) to stay hedged, in $M at today's spot:
#   charm_flow_m_per_h      one hour of calendar time passes, spot and IV unchanged
#   vanna_flow_m_per_volpt  every option's IV falls 1 vol point (a rise is the same number with the opposite sign)
#   charm_to_close_m        (0DTE only) spot holds here to the 16:00 settlement: today's options go to delta 1/0
# Black-Scholes from each contract's own IV (provider gamma has no vanna/charm partner); contracts without IV are left
# out and counted. Positioning arithmetic under an assumed inventory, not observed dealer flow and not a forecast.
SECOND_ORDER_RULE = 'charm-vanna/1'
HOUR_Y = 1.0 / (365.0 * 24.0)


def second_order_flows(S, K, T, iv, side, oi):
    """(charm $M/hour, vanna $M per -1 vol pt) dealer hedge flow for one contract line; buy = +."""
    vanna, charm = bs_vanna_charm(S, K, max(T, T_FLOOR), iv, side)
    pos = 1.0 if side == 'call' else -1.0
    shares = oi * MULT
    return -pos * charm * HOUR_Y * shares * S / 1e6, pos * vanna * 0.01 * shares * S / 1e6


def to_close_flow(S, K, T, iv, side, oi):
    """$M dealers buy (+) / sell (-) between now and settlement if spot stays at S (0DTE contracts)."""
    pos = 1.0 if side == 'call' else -1.0
    d_now = bs_delta(S, K, max(T, T_FLOOR), iv, side)
    return -pos * (bs_delta(S, K, 0.0, iv, side) - d_now) * oi * MULT * S / 1e6


def expiry_utc(expiry):
    """16:00 ET settlement of an ISO expiry date, as a UTC datetime."""
    d = date.fromisoformat(expiry)
    return datetime(d.year, d.month, d.day, 16, 0, tzinfo=ET).astimezone(timezone.utc)


def third_friday(year, month):
    d = date(year, month, 15)
    while d.weekday() != 4:
        d += timedelta(days=1)
    return d


def horizons_for(today):
    """today: ET date. Returns ordered list of (name, predicate(expiry_date))."""
    week_end = today + timedelta(days=7)
    tf = third_friday(today.year, today.month)
    if tf < today:
        ny, nm = (today.year + 1, 1) if today.month == 12 else (today.year, today.month + 1)
        tf = third_friday(ny, nm)
    return [('dte0', lambda e: e == today), ('week', lambda e: today <= e <= week_end),
            ('month', lambda e: today <= e <= tf), ('all', lambda e: e >= today)], tf


TIER_RULE = 'nearest-significant/1'
TIER_SIG_FRAC = 0.25
# Position levels (Anik 2026-11-11): what the page shows as ceilings/floors. Gamma $ per strike peaks at the money for
# short-dated options whatever is held there (2026-11-11 0DTE: the strike $0.02 under spot held 8% of the largest put OI
# within reach but 43% of the largest put gamma, so it was "S1"; the biggest put position sat $7 lower). Positions are
# open interest: R1/S1 = the NEAREST strike above (below) spot holding >= POSITION_OI_FRAC of the largest call (put) OI
# within POSITION_WINDOW_PCT of spot, R2/S2 the next, R3/S3 the next. The gamma tiers above (TIER_RULE) are unchanged
# and stay in `ceilings`/`floors` for the registered consumers (gex-signals pin-range/2 and level_series, gex-health
# level alerts, Records), which read R1/S1 by that rule.
POSITION_RULE = 'nearest-position/1'
POSITION_OI_FRAC = 0.5
POSITION_WINDOW_PCT = 5.0


def position_levels(above, below, spot):
    """Ceilings/floors by open interest (see POSITION_RULE). New dicts: the gamma tier rows are not touched."""
    win = spot * POSITION_WINDOW_PCT / 100.0
    pa = [v for v in above if v['strike'] - spot <= win]; pb = [v for v in below if spot - v['strike'] <= win]
    big_c = max((v['call_oi'] for v in pa), default=0.0); big_p = max((v['put_oi'] for v in pb), default=0.0)
    def row(v, side, i, big):
        oi = v['call_oi'] if side == 'call' else v['put_oi']
        return {'tier': ('R' if side == 'call' else 'S') + str(i + 1), 'strike': v['strike'], 'dist': v['dist'],
                'dist_pct': v['dist_pct'], 'oi': int(round(oi)), 'oi_share': round(oi / big, 3), 'oi_chg': v.get(side + '_oi_chg'),
                'gex_m': v['call_gex_m'] if side == 'call' else v['put_gex_m'], 'live': v['live']}
    ceil = sorted([v for v in pa if big_c > 0 and v['call_oi'] >= POSITION_OI_FRAC * big_c], key=lambda v: v['strike'])[:3]
    floor = sorted([v for v in pb if big_p > 0 and v['put_oi'] >= POSITION_OI_FRAC * big_p], key=lambda v: -v['strike'])[:3]
    return {'rule': POSITION_RULE, 'oi_frac': POSITION_OI_FRAC, 'window_pct': POSITION_WINDOW_PCT,
            'max_call_oi': int(round(big_c)), 'max_put_oi': int(round(big_p)),
            'ceilings': [row(v, 'call', i, big_c) for i, v in enumerate(ceil)],
            'floors': [row(v, 'put', i, big_p) for i, v in enumerate(floor)]}


# Today's flow (Anik 2026-11-11, "show today's new positions"): open interest is settled overnight, so the position levels
# miss whatever is opened today, which on 0DTE is most of the activity. flow_today lists, per side, the FLOW_TOP strikes
# within POSITION_WINDOW_PCT of spot with the most contracts traded today and that strike's open interest. Traded volume is
# observed but counts opening AND closing trades: a strike trading many times its open interest is where today's activity
# is, not proof of new positions (tomorrow's open interest says how many stayed open).
FLOW_RULE = 'flow-today/1'
FLOW_TOP = 3


def flow_today(flow, per, spot):
    """{'rule', 'window_pct', 'calls': [...], 'puts': [...]} from today's volume per strike and side. Pure."""
    win = spot * POSITION_WINDOW_PCT / 100.0
    near = {k: v for k, v in flow.items() if abs(k - spot) <= win}
    def side(name):
        out = []
        for k, v in sorted(near.items(), key=lambda kv: (-kv[1][name], abs(kv[0] - spot)))[:FLOW_TOP]:
            if v[name] <= 0:
                continue
            oi = (per.get(k) or {}).get(name + '_oi') or 0.0
            out.append({'strike': k, 'dist': round(k - spot, 2), 'volume': int(round(v[name])), 'oi': int(round(oi)),
                        'volume_to_oi': round(v[name] / oi, 2) if oi > 0 else None,
                        'volume_src': 'mixed' if len(v['src']) > 1 else next(iter(v['src']))})
        return out
    return {'rule': FLOW_RULE, 'window_pct': POSITION_WINDOW_PCT, 'calls': side('call'), 'puts': side('put'),
            'basis': 'contracts traded today (live brokerage volume where streamed, else Cboe delayed); includes closing trades'}


def pain_expiry(name, exps, today, tf):
    """The one expiry whose max pain a horizon shows (max pain is a per-settlement number). dte0: today's expiry; week:
    this week's Friday (the first Friday expiry in the horizon, else its last expiry); month: the monthly OPEX (third
    Friday `tf`) when listed, else the horizon's last expiry; all: the monthly OPEX too (the settlement most open interest
    is anchored to). 2026-11-04: every horizon used its nearest expiry, so WEEK / MONTH / ALL all repeated today's 0DTE
    max pain (762 on all four tabs)."""
    if not exps:
        return None
    if name == 'dte0':
        return exps[0]
    if name == 'week':
        fri = [e for e in exps if date.fromisoformat(e).weekday() == 4]
        return fri[0] if fri else exps[-1]
    if tf.isoformat() in exps:
        return tf.isoformat()
    return exps[-1] if name == 'month' else exps[0]


# Live spike guard (2026-11-11 19:25Z: one live quote made the 753 call 65x its own size for a minute, so for that minute it was
# the "biggest" strike and could have been the magnet). A live contract's gamma or open interest above SPIKE_X x its last accepted
# value is held back (the last accepted value is used) until the jumped value has persisted SPIKE_CONFIRM_S; a value back in range
# clears it. Only upward jumps: a real move still arrives, one minute late. Stateful; server.py keeps one per process.
SPIKE_RULE = 'live-spike-guard/1'
SPIKE_X = 5.0
SPIKE_CONFIRM_S = 60.0
SPIKE_MAX_GAP_S = 180.0
SPIKE_FIELDS = ('provider_gamma', 'open_interest')


class SpikeGuard:
    def __init__(self, x=SPIKE_X, confirm_s=SPIKE_CONFIRM_S, max_gap_s=SPIKE_MAX_GAP_S):
        self.x, self.confirm_s, self.max_gap_s = x, confirm_s, max_gap_s
        self.last, self.pending = {}, {}

    def filter(self, rows, now_s):
        """Copies of rows with held-back fields replaced; returns (rows, held) where held lists (expiry, strike, side, field)."""
        out, held = [], []
        for r in rows or []:
            k = (r.get('expiry'), _num(r.get('strike')), r.get('opt_type'))
            r2 = dict(r)
            for f in SPIKE_FIELDS:
                v = _num(r.get(f))
                if v is None:
                    continue
                last = self.last.get((k, f))
                if last and last[0] > 0 and v > self.x * last[0] and now_s - last[1] <= self.max_gap_s:
                    first = self.pending.setdefault((k, f), now_s)
                    if now_s - first < self.confirm_s:
                        r2[f] = last[0]
                        held.append(k + (f,))
                        continue
                self.pending.pop((k, f), None)
                self.last[(k, f)] = (v, now_s)
            out.append(r2)
        return out, held


def merge_rows(chain_rows, rtd_rows, occ_index=None):
    """Contracts keyed by (expiry, strike, side); RTD rows (live) override Cboe rows (delayed).

    occ_index (OCC_OI_RULE, 2026-11-17): {(expiry, strike, side): (oi, oi_prev)} for TODAY's OCC release, or None. A Cboe
    row with an OCC entry takes the OCC open interest (dated, the official number; Cboe's is undated) and keeps the Cboe value
    as oi_cboe; every row carries oi_src ('occ' | 'cboe_delayed' | 'rtd') and oi_prev (the previous trading day's OCC value,
    None without OCC). An RTD open interest still wins: it is the same OCC number streamed live."""
    out = {}
    occ_index = occ_index or {}
    for c in chain_rows or []:
        k = (c.get('expiry'), _num(c.get('strike')), 'call' if c.get('side') == 'C' else 'put')
        if None in k:
            continue
        out[k] = {'expiry': k[0], 'strike': k[1], 'side': k[2], 'iv': _num(c.get('iv')), 'oi': _num(c.get('open_interest')) or 0.0,
                  'gamma': _num(c.get('gamma')), 'volume': _num(c.get('volume')), 'src': 'cboe_delayed',
                  'oi_src': 'cboe_delayed', 'oi_prev': None}
        out[k]['volume_src'] = 'cboe_delayed' if out[k]['volume'] is not None else None
        occ = occ_index.get(k)
        if occ is not None:
            out[k]['oi_cboe'] = out[k]['oi']
            out[k]['oi'], out[k]['oi_prev'], out[k]['oi_src'] = float(occ[0]), occ[1], 'occ'
    for r in rtd_rows or []:
        k = (r.get('expiry'), _num(r.get('strike')), r.get('opt_type'))
        if None in k or k[2] not in ('call', 'put'):
            continue
        prev = out.get(k, {})
        vol = _num(r.get('volume'))
        roi = _num(r.get('open_interest'))
        occ = occ_index.get(k)
        out[k] = {'expiry': k[0], 'strike': k[1], 'side': k[2], 'iv': _num(r.get('implied_vol')) or prev.get('iv'),
                  'oi': roi if roi is not None else prev.get('oi', float(occ[0]) if occ is not None else 0.0),
                  'gamma': _num(r.get('provider_gamma')), 'src': 'rtd',
                  'volume': vol if vol is not None else prev.get('volume'),
                  'volume_src': 'rtd' if vol is not None else prev.get('volume_src'),
                  'oi_src': 'rtd' if roi is not None else prev.get('oi_src', 'occ' if occ is not None else None),
                  'oi_prev': prev.get('oi_prev', occ[1] if occ is not None else None)}
    return list(out.values())


def structure_from_bars(bars, prev_close, pm_high, pm_low, prior_high, prior_low, now):
    """Intraday structure from today's regular-session 1-minute bars [{t,c,h,l,v}]."""
    reg = [b for b in (bars or []) if _num(b.get('c')) is not None and in_regular_session(b.get('t'))]
    def hl(bs):
        hs = [b['h'] for b in bs if _num(b.get('h')) is not None]; ls = [b['l'] for b in bs if _num(b.get('l')) is not None]
        return (max(hs) if hs else None, min(ls) if ls else None)
    open_t = reg[0]['t'] if reg else None
    or15 = hl([b for b in reg if open_t is not None and b['t'] < open_t + 15 * 60])
    or30 = hl([b for b in reg if open_t is not None and b['t'] < open_t + 30 * 60])
    sess = hl(reg)
    # Incident containment: prices remain usable; imputed interval volumes do not.
    return {'vwap': None, 'vwap_basis': volume_quality.REASON,
            'vwap_status': 'unavailable', 'volume_quality': volume_quality.quality(),
            'or15': {'high': or15[0], 'low': or15[1]}, 'or30': {'high': or30[0], 'low': or30[1]},
            'session': {'high': sess[0], 'low': sess[1], 'bars': len(reg)},
            'prior': {'high': prior_high, 'low': prior_low, 'close': prev_close},
            'premarket': {'high': pm_high, 'low': pm_low}}


def in_regular_session(t):
    try:
        d = datetime.fromtimestamp(float(t), ET)
    except (TypeError, ValueError, OverflowError, OSError):
        return False
    return (9, 30) <= (d.hour, d.minute) < (16, 0)


PHANTOM_1M_V = 5_000_000   # indicator_tracker.fetch_bars' guard, so page volume matches signals.db bars_1m


def clean_bar_volume(bars):
    """Yahoo's 1-min SPY series carries phantom volume: single mid-session minutes of 15-25M shares against a ~100k norm
    (2026-10-26: 11 such minutes made the day 161.7M raw vs 22.2M clean and moved VWAP 0.70 above the clean value).
    Rule: a regular-session bar other than 09:30 / 15:59 ET (New York time, so DST-correct) above PHANTOM_1M_V gets the upper
    median of the previous 30 ORDINARY minutes of the input; the raw value stays on the bar as v_raw. Prices are untouched.
    Ordinary = regular session, not the 09:30 / 15:59 auction minutes, volume present and not above PHANTOM_1M_V: the
    opening auction and earlier phantom minutes never set a replacement (2026-10-27 counterexample: a 09:31 5.2M minute
    took the 9.5M 09:30 auction volume). A missing volume is not an observation and is left out of the window (the raw
    rule counted it as 0). With no ordinary prior minute yet the replacement is 0 (the minute adds nothing).
    indicator_tracker.fetch_bars (bars_1m) still uses the older raw-window rule, so the two can differ whenever the window
    holds an auction minute, an earlier phantom minute or a missing volume. Returns new bar dicts in the input order."""
    out, ordinary = [], []
    for b in bars or []:
        b = dict(b)
        if in_regular_session(b.get('t')):
            v = _num(b.get('v'))
            d = datetime.fromtimestamp(float(b['t']), ET)
            auction = (d.hour, d.minute) in ((9, 30), (15, 59))
            if v is not None and v > PHANTOM_1M_V and not auction:
                prior = sorted(ordinary[-30:])
                b['v_raw'], b['v'] = b.get('v'), prior[len(prior) // 2] if prior else 0
            elif v is not None and not auction and v <= PHANTOM_1M_V:
                ordinary.append(v)
        out.append(b)
    return out


def structure_points(st):
    pts = []
    def add(name, v):
        if _num(v) is not None:
            pts.append((name, float(v)))
    add('VWAP', st['vwap']); add('OR15 high', st['or15']['high']); add('OR15 low', st['or15']['low'])
    add('OR30 high', st['or30']['high']); add('OR30 low', st['or30']['low'])
    add('session high', st['session']['high']); add('session low', st['session']['low'])
    add('prior high', st['prior']['high']); add('prior low', st['prior']['low']); add('prior close', st['prior']['close'])
    add('premarket high', st['premarket']['high']); add('premarket low', st['premarket']['low'])
    return pts


def horizon_book(rows, spot, now, name, max_pain_expiry):
    """Per-strike book and summary levels for one horizon's rows."""
    per = {}
    T_by = {}
    by_exp = {}   # net GEX by expiry, for the nearest-expiry share of the total (2026-11-04)
    flow = {}     # today's traded volume per strike and side, every unexpired contract (also those with no open interest yet)
    so = {'charm': 0.0, 'vanna': 0.0, 'to_close': 0.0, 'used': 0, 'no_iv': 0}   # SECOND_ORDER_RULE totals
    for r in rows:
        T = T_by.get(r['expiry'])
        if T is None:
            T = max((expiry_utc(r['expiry']) - now).total_seconds(), 0.0) / (365.0 * 86400.0); T_by[r['expiry']] = T
        if T > 0 and r.get('volume') is not None and r.get('volume_src') in ('rtd', 'cboe_delayed'):
            f = flow.setdefault(r['strike'], {'call': 0.0, 'put': 0.0, 'src': set()})
            f[r['side']] += r['volume']; f['src'].add(r['volume_src'])
        if T <= 0 or r['oi'] <= 0:
            continue
        g = r['gamma'] if r['gamma'] is not None else bs_gamma(spot, r['strike'], max(T, T_FLOOR), r['iv'])
        gex = g * r['oi'] * MULT * spot * spot * 0.01 / 1e6  # $M per 1% move
        by_exp[r['expiry']] = by_exp.get(r['expiry'], 0.0) + (gex if r['side'] == 'call' else -gex)
        k = per.setdefault(r['strike'], {'strike': r['strike'], 'call_gex_m': 0.0, 'put_gex_m': 0.0, 'call_oi': 0.0, 'put_oi': 0.0,
                                         '_call_prev': 0.0, '_put_prev': 0.0, '_call_noprev': 0, '_put_noprev': 0, '_call_hasprev': 0, '_put_hasprev': 0,
                                         'charm_m_h': 0.0, 'vanna_m_volpt': 0.0,
                                         'volume': None, 'volume_rtd': None, 'volume_cboe': None, 'volume_src': None, 'live': False})
        if r['iv'] and r['iv'] > 0:
            cm, vm = second_order_flows(spot, r['strike'], T, r['iv'], r['side'], r['oi'])
            k['charm_m_h'] += cm; k['vanna_m_volpt'] += vm
            so['charm'] += cm; so['vanna'] += vm; so['used'] += 1
            if name == 'dte0':
                so['to_close'] += to_close_flow(spot, r['strike'], T, r['iv'], r['side'], r['oi'])
        else:
            so['no_iv'] += 1
        if r['side'] == 'call':
            k['call_gex_m'] += gex; k['call_oi'] += r['oi']
        else:
            k['put_gex_m'] -= gex; k['put_oi'] += r['oi']
        if r.get('oi_prev') is None:
            k['_' + r['side'] + '_noprev'] += 1
        else:
            k['_' + r['side'] + '_prev'] += r['oi_prev']; k['_' + r['side'] + '_hasprev'] += 1
        vsrc = r.get('volume_src')
        if r.get('volume') is not None and vsrc in ('rtd', 'cboe_delayed'):
            part = 'volume_rtd' if vsrc == 'rtd' else 'volume_cboe'
            k[part] = (k[part] or 0.0) + r['volume']; k['volume'] = (k['volume'] or 0.0) + r['volume']
            k['volume_src'] = vsrc if k['volume_src'] in (None, vsrc) else 'mixed'
        if r['src'] == 'rtd':
            k['live'] = True
    table = []
    for v in sorted(per.values(), key=lambda x: x['strike']):
        v['net_gex_m'] = v['call_gex_m'] + v['put_gex_m']; v['abs_gex_m'] = abs(v['call_gex_m']) + abs(v['put_gex_m'])
        v['dist'] = round(v['strike'] - spot, 2); v['dist_pct'] = round((v['strike'] - spot) / spot * 100, 3)
        for key in ('call_gex_m', 'put_gex_m', 'net_gex_m', 'abs_gex_m'):
            v[key] = round(v[key], 2)
        v['charm_m_h'] = round(v['charm_m_h'], 3); v['vanna_m_volpt'] = round(v['vanna_m_volpt'], 3)
        # OCC_OI_RULE: overnight change, only when that side of the strike has contracts and every one has a previous OCC value
        for sd in ('call', 'put'):
            prev, noprev, hasprev = v.pop('_' + sd + '_prev'), v.pop('_' + sd + '_noprev'), v.pop('_' + sd + '_hasprev')
            v[sd + '_oi_chg'] = int(round(v[sd + '_oi'] - prev)) if hasprev and not noprev else None
        table.append(v)
    if not table:
        return None
    above = [v for v in table if v['strike'] > spot]; below = [v for v in table if v['strike'] < spot]
    # TIER_RULE (Anik 2026-11-04, "by distance"): R1/S1 = the NEAREST significant ceiling/floor, R2/S2 the next, R3/S3 the
    # next; significant = call (put) GEX >= TIER_SIG_FRAC of the largest above (below) spot. Size lives in `strength` and
    # in the wall names. Before: the three LARGEST, so S1 could sit $12 away while "S3" was right under price and the
    # labels hopped between strikes as gamma reshuffled.
    big_c = max((v['call_gex_m'] for v in above), default=0.0); big_p = max((-v['put_gex_m'] for v in below), default=0.0)
    ceilings = sorted([v for v in above if big_c > 0 and v['call_gex_m'] >= TIER_SIG_FRAC * big_c], key=lambda v: v['strike'])[:3]
    floors = sorted([v for v in below if big_p > 0 and -v['put_gex_m'] >= TIER_SIG_FRAC * big_p], key=lambda v: -v['strike'])[:3]
    call_wall = max(table, key=lambda v: v['call_gex_m'])['strike']
    put_wall = min(table, key=lambda v: v['put_gex_m'])['strike']
    hedge_wall = max(table, key=lambda v: v['abs_gex_m'])['strike']
    maxabs = max(v['abs_gex_m'] for v in table) or 1.0
    # net gamma profile over a spot grid for the flip (+ IV shock band) — vectorised (numpy) over contracts × grid
    lo, hi = spot * 0.95, spot * 1.05
    grid = [round(lo + i * 0.5, 2) for i in range(int((hi - lo) / 0.5) + 1)]
    live = [r for r in rows if T_by.get(r['expiry'], 0.0) > 0 and r['oi'] > 0 and r['iv'] and r['iv'] > 0]
    import numpy as np
    K = np.array([r['strike'] for r in live]); T = np.maximum(np.array([T_by[r['expiry']] for r in live]), T_FLOOR); IV = np.array([r['iv'] for r in live])
    OI = np.array([r['oi'] for r in live]); SG = np.array([1.0 if r['side'] == 'call' else -1.0 for r in live]); S = np.array(grid)[:, None]
    def flips(shock):
        if len(live) == 0:
            return []
        iv = IV * (1 + shock); sq = np.sqrt(T)
        d1 = (np.log(S / K) + (RATE - DIVY + 0.5 * iv * iv) * T) / (iv * sq)
        gam = np.exp(-DIVY * T) * np.exp(-0.5 * d1 * d1) / np.sqrt(2 * np.pi) / (S * iv * sq)
        prof = (gam * OI * MULT * S * S * 0.01 * SG).sum(axis=1)
        roots = []
        for i in range(len(grid) - 1):
            if prof[i] * prof[i + 1] < 0:
                roots.append(round(float(grid[i] - prof[i] * (grid[i + 1] - grid[i]) / (prof[i + 1] - prof[i])), 2))
        return roots
    roots0 = flips(0.0)
    flip = min(roots0, key=lambda x: abs(x - spot)) if roots0 else None
    band = [min(r, key=lambda x: abs(x - spot)) if r else None for r in (flips(0.2), flips(0.4))]
    # max pain on the horizon's nearest expiry
    near = [r for r in rows if r['expiry'] == max_pain_expiry]
    pain = None
    if near:
        ks = sorted({r['strike'] for r in near})
        cost = lambda K: sum(r['oi'] * max(0.0, K - r['strike']) for r in near if r['side'] == 'call') + sum(r['oi'] * max(0.0, r['strike'] - K) for r in near if r['side'] == 'put')
        pain = min(ks, key=cost)
    # air pockets between significant strikes
    sig = [v['strike'] for v in table if v['abs_gex_m'] >= 0.1 * maxabs]
    pockets = [{'from': a, 'to': b, 'width': round(b - a, 2)} for a, b in zip(sig, sig[1:]) if b - a > 2.0]
    # A nearest zero crossing does not identify the sign on either side.
    # Use the same unrounded assumed-position exposure as this book's totals.
    net_at_spot = sum(by_exp.values())
    regime = ('modeled positive gamma' if net_at_spot > 0 else
              'modeled negative gamma' if net_at_spot < 0 else 'modeled neutral gamma')
    return {'name': name, 'expiries': sorted({r['expiry'] for r in rows}), 'contracts': len(rows), 'live_contracts': sum(1 for r in rows if r['src'] == 'rtd'),
            'volume_sources': {'rtd': sum(1 for r in rows if r.get('volume_src') == 'rtd'), 'cboe_delayed': sum(1 for r in rows if r.get('volume_src') == 'cboe_delayed'),
                               'none': sum(1 for r in rows if r.get('volume_src') is None)},
            'call_wall': call_wall, 'put_wall': put_wall, 'hedge_wall': hedge_wall, 'gamma_flip': flip, 'flip_band_iv_plus_20_40': band,
            'max_pain': pain, 'max_pain_expiry': max_pain_expiry, 'regime': regime,
            'net_gex_sign': 'positive' if net_at_spot > 0 else 'negative' if net_at_spot < 0 else 'neutral',
            'ceilings': ceilings, 'floors': floors, 'position_levels': position_levels(above, below, spot),
            'flow_today': flow_today(flow, per, spot),
            'air_pockets': pockets[:8], 'max_abs_gex_m': round(maxabs, 2),
            'net_gex_at_spot_m': round(net_at_spot, 1),
            # totals add up over every expiry in the horizon, so wider tabs read larger; the nearest expiry's part says how
            # much of it is today's (or the next) settlement
            'net_gex_nearest_expiry_m': round(by_exp.get(min(by_exp), 0.0), 1) if by_exp else None,
            'nearest_expiry': min(by_exp) if by_exp else None,
            'second_order': {'rule': SECOND_ORDER_RULE, 'charm_flow_m_per_h': round(so['charm'], 2),
                             'vanna_flow_m_per_volpt': round(so['vanna'], 2),
                             'charm_to_close_m': round(so['to_close'], 1) if name == 'dte0' and so['used'] else None,
                             'contracts_used': so['used'], 'contracts_without_iv': so['no_iv'],
                             'basis': 'dealer stock to buy (+) or sell (-) in $M at spot; assumed long calls / short puts; '
                                      'Black-Scholes from each contract\'s IV; spot held fixed'},
            'per_strike': table}


OCC_OI_RULE = 'occ-oi/1'


def occ_block(occ, today):
    """(index to apply or None, the book's open_interest block). Applied only when the OCC file is TODAY's release (ET)."""
    if not occ or occ.get('status') != 'ok':
        return None, {'rule': OCC_OI_RULE, 'status': 'unavailable', 'reason': (occ or {}).get('reason') or 'no OCC open interest',
                      'basis': 'Cboe delayed / brokerage open interest (undated)'}
    base = {'rule': OCC_OI_RULE, 'day': occ.get('day'), 'prev_day': occ.get('prev_day'), 'published_at': occ.get('published_at'),
            'exported_at': occ.get('exported_at'), 'source': occ.get('source'), 'occ_contracts': len(occ.get('index') or {})}
    if occ.get('day') != today.isoformat():
        return None, dict(base, status='not_today', reason='newest OCC release is %s, not today (%s)' % (occ.get('day'), today.isoformat()),
                          basis='Cboe delayed / brokerage open interest (undated)')
    return occ['index'], dict(base, status='applied', basis='OCC open interest published %s (prior close); RTD streamed where live' % occ.get('published_at'))


def build_level_book(chain_rows, rtd_rows, spot, bars, *, prev_close=None, pm_high=None, pm_low=None, prior_high=None, prior_low=None, now=None, quote=None, chain_meta=None, degraded=None, occ=None):
    """degraded (2026-11-05): reasons the caller knows the inputs are not a current picture (stale Cboe chain, no live
    brokerage leg in regular hours). A non-empty list makes status 'degraded' (the book is still built and served so the
    page can show it as LAST KNOWN), and gex-signals refuses to sample a non-'ok' book into level_snaps."""
    now = now or datetime.now(timezone.utc)
    degraded = list(degraded or [])
    spot = _num(spot)
    if not spot or spot <= 0:
        return {'status': 'unavailable', 'reason': 'no spot', 'as_of': now.isoformat()}
    # settled expiries (16:00 ET passed) leave every horizon: they add no GEX (T=0) but were still counted in the
    # expiry list and chosen as the max-pain expiry after the close (2026-11-03: max_pain_expiry = today at 21:06Z)
    today = now.astimezone(ET).date()
    occ_index, oi_info = occ_block(occ, today)
    merged = merge_rows(chain_rows, rtd_rows, occ_index)
    if occ_index is not None:
        oi_info['matched'] = sum(1 for r in merged if r.get('oi_cboe') is not None)
        oi_info['cboe_differs'] = sum(1 for r in merged if r.get('oi_cboe') is not None and r['oi_cboe'] != r['oi'])
        oi_info['chain_without_occ'] = sum(1 for r in merged if r['src'] == 'cboe_delayed' and r.get('oi_cboe') is None)
    rows = [r for r in merged if expiry_utc(r['expiry']) > now]
    hz, tf = horizons_for(today)
    st = structure_from_bars(bars, prev_close, pm_high, pm_low, prior_high, prior_low, now)
    spts = structure_points(st)
    books = {}
    for name, pred in hz:
        sel = [r for r in rows if pred(date.fromisoformat(r['expiry']))]
        exps = sorted({r['expiry'] for r in sel})
        books[name] = horizon_book(sel, spot, now, name, pain_expiry(name, exps, today, tf)) if sel else None
    # confluence + cross-horizon agreement + strength
    walls_by_strike = {}
    for name, b in books.items():
        if b:
            for s in (b['call_wall'], b['put_wall']):
                walls_by_strike.setdefault(s, set()).add(name)
    for name, b in books.items():
        if not b:
            continue
        for tier in ('ceilings', 'floors'):
            for i, lv in enumerate(b[tier]):
                lv['tier'] = ('R' if tier == 'ceilings' else 'S') + str(i + 1)
                lv['confluence'] = [n for n, v in spts if abs(v - lv['strike']) <= 0.75]
                others = len([h for h in walls_by_strike.get(lv['strike'], set()) if h != name])
                mag = (lv['call_gex_m'] if tier == 'ceilings' else -lv['put_gex_m']) / (b['max_abs_gex_m'] or 1.0)
                lv['strength'] = int(round(min(100, max(0, 60 * mag + min(20, 10 * others) + min(20, 10 * len(lv['confluence']))))))
                lv['horizons_as_wall'] = sorted(walls_by_strike.get(lv['strike'], set()))
        for tier in ('ceilings', 'floors'):
            for lv in (b.get('position_levels') or {}).get(tier, []):
                lv['confluence'] = [n for n, v in spts if abs(v - lv['strike']) <= 0.75]
                lv['horizons_as_wall'] = sorted(walls_by_strike.get(lv['strike'], set()))
    primary = books.get('dte0') or books.get('week') or books.get('month') or books.get('all')
    read = None
    if primary and primary['gamma_flip'] is not None:
        d = spot - primary['gamma_flip']
        read = {'horizon': primary['name'], 'spot_vs_flip': 'above' if d >= 0 else 'below', 'flip_distance': round(d, 2), 'flip_distance_pct': round(d / spot * 100, 3),
                'nearest_ceiling': primary['ceilings'][0] if primary['ceilings'] else None, 'nearest_floor': primary['floors'][0] if primary['floors'] else None,
                'regime': primary['regime']}
    return {'status': 'degraded' if degraded else 'ok', 'degraded': degraded, 'reason': ', '.join(degraded) if degraded else None, 'as_of': now.isoformat(), 'spot': spot, 'quote': quote, 'chain': chain_meta, 'tier_rule': TIER_RULE,
            'today_et': today.isoformat(), 'monthly_expiry': tf.isoformat(), 'structure': st, 'read': read, 'open_interest': oi_info,
            'horizons': {k: ({kk: vv for kk, vv in v.items() if kk != 'per_strike'} if v else None) for k, v in books.items()},
            'per_strike': {k: (v['per_strike'] if v else []) for k, v in books.items()},
            'method': 'dealer gamma $M per 1% move = gamma × OI × 100 × S² × 0.01, calls +, puts −; provider gamma where present else BS from IV; RTD rows override Cboe rows; contract volume from RTD where the row has it else Cboe delayed (per-strike volume_src rtd/cboe_delayed/mixed); horizons by expiry; flip = zero crossing of BS net-gamma profile nearest spot (band at IV +20%/+40%); max pain on nearest expiry; strength = 60×magnitude + 10/other-horizon wall (≤20) + 10/confluence (≤20); open interest (occ-oi/1) = today\'s OCC release where present, else Cboe delayed, RTD streamed wins; per strike/side call_oi_chg/put_oi_chg = OCC today − previous trading day (None without OCC; the 2-expiry panel stays /api/oi-changes); charm/vanna (charm-vanna/1) = dealer stock to buy or sell per hour of decay, per 1 vol point lower IV, and (0DTE) to the 16:00 settlement at today\'s spot, same assumed position. Positioning, not prediction.'}
