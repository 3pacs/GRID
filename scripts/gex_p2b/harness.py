"""Offline P2-B matched real-input packet: one Cboe delayed SPY chain fed to
GRID, Gamma Watch and the independent P2-A reference under identical inputs.

Run via ``python -m scripts.gex_p2b.harness``. Only explicitly named local files
are read; nothing is fetched. The output directory is create-only and is
created after every artifact has been serialized, so a run never leaves inputs
without results. No network, database, broker, SSH, timer, collector import or
runtime configuration. Binding protocol: docs/GAMMA-WATCH-P2-RECONCILIATION.md.

Evidence boundary: this is class 1 exact-input arithmetic. Cboe chain data
feeding both local engines and an independent recompute does NOT validate a
vendor model, dealer positioning or any directional edge. Class 2 (matched
vendor reproduction) is reported NOT_SUPPORTED unless a free source supplies
the vendor's snapshot, universe, OI vintage, timestamp, units and methodology.
No canonical-engine choice is made or implied here (GEX-P2C, owner decision).
"""

from __future__ import annotations

import argparse
import ast
import builtins
import copy
from functools import lru_cache
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, localcontext
import json
import math
from pathlib import Path
import re
import sys
import uuid
from zoneinfo import ZoneInfo

import numpy as np

from scripts.gex_p2a import harness as p2a
from scripts.gex_p2a.reference import gamma as decimal_gamma
from scripts.gex_p2a.reference import reference_bound

ROOT = Path(__file__).resolve().parents[2]
PROTOCOL = ROOT / "docs/GAMMA-WATCH-P2-RECONCILIATION.md"

PACKET_SCHEMA = "gex-p2b-cboe-spy-v1"
RECEIPT_SCHEMA = "gex-p2b-receipt-v1"
RESULT_SCHEMA = "gex-p2b-result-v1"
MANIFEST_SCHEMA = "gex-p2b-manifest-v1"

STATUSES = (
    "PASS_NUMERICAL",
    "FAIL_NUMERICAL",
    "NOT_COMPARABLE",
    "NOT_SUPPORTED",
    "INPUT_REJECTED",
    "INDETERMINATE",
)

ET = ZoneInfo("America/New_York")
OSI_SPY = re.compile(r"^SPY(\d{6})([CP])(\d{8})$")
CALENDAR_VERSION = (
    "us-equity-options-expiry-1600-America/New_York-v1;"
    "early-close-candidates-flagged-not-modeled"
)
YEAR_SECONDS = 365 * 86400

# Common class 1 parameters: protocol step 2 (r=.04, q=0, explicit fractional T).
BASELINE = {"r": 0.04, "q": 0.0}
GRID_NATIVE = {"r": 0.05, "q": 0.0}
WATCH_NATIVE = {"r": 0.04, "q": 0.012}
# Gamma Watch's native curve grid (broker.py:curves), reused as the common grid.
WATCH_GRID = (735.0, 790.0, 0.25)
WATCH_SPOT_GATE = (745.0, 780.0)

# Protocol tolerances, frozen in docs/GAMMA-WATCH-P2-RECONCILIATION.md before
# any real comparison result was viewed. Do not tune them to results.
TOLERANCES = {
    "contract_gamma": "abs(error) <= 1e-12 + 1e-8 * abs(reference)",
    "aggregate_curve": "abs(error) <= $0.01 + 1e-8 * gross_absolute_GEX",
    "root_bracket_width": 0.001,
    "root_match_separation": 0.01,
    "reference_float_certification_rel": 1e-10,
}
CONTRACT_ABS, CONTRACT_REL = 1e-12, 1e-8
AGG_ABS, AGG_REL = 0.01, 1e-8
ROOT_BRACKET, ROOT_MATCH = 0.001, 0.01
CERTIFY_REL = 1e-10

# Pinned AST fingerprints of the collector code this harness executes. Drift
# fails closed as NOT_SUPPORTED for review; it never silently adapts.
WATCH_AGGREGATION_FINGERPRINT = (
    "efb3117a6e75bbba9c9b8cc2ef4b6359ce1e7d6275ae21c3780c8d3b3442e1d4"
)
WATCH_NATIVE_FINGERPRINT = (
    "ce6ec5c04da726e22f71040e59209e5d996b1abb827828d64aa5c66513a62dcf"
)
COLLECTOR_DRIFT = (StopIteration, SyntaxError, OSError, ValueError)

digest = p2a.digest
canonical = p2a.canonical


class InputRejected(ValueError):
    """Packet-level rejection; reported as INPUT_REJECTED with a reason."""


# ── strict parsing ────────────────────────────────────────────────────────


def strict_json(raw: bytes):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise InputRejected("duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=unique)
    except InputRejected:
        raise
    except (ValueError, TypeError) as exc:
        raise InputRejected(f"malformed JSON: {exc}") from exc


def number(value) -> float | None:
    """A finite JSON number, else None (bool/string/null/NaN never coerce)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def aware(text, field: str) -> datetime:
    try:
        return p2a.instant(text)
    except ValueError as exc:
        raise InputRejected(f"{field}: {exc}") from exc


@lru_cache(maxsize=None)
def expiry_instant(day: date) -> datetime:
    return datetime.combine(day, time(16, 0), ET).astimezone(timezone.utc)


@lru_cache(maxsize=None)
def early_close_candidate(day: date) -> bool:
    """NYSE 13:00 ET early-close candidates. Flagged, not modeled."""
    if day.month == 11 and day.weekday() == 4:
        thursdays = [d for d in range(1, 31) if date(day.year, 11, d).weekday() == 3]
        return day.day == thursdays[3] + 1
    if day.month == 12 and day.day == 24:
        return day.weekday() <= 3
    if day.month == 7 and day.day == 3:
        return day.weekday() <= 3
    return False


@lru_cache(maxsize=None)
def _osi_date(text: str) -> date:
    return date(2000 + int(text[:2]), int(text[2:4]), int(text[4:6]))


# ── packet normalization ─────────────────────────────────────────────────


def normalize(raw: bytes, receipt_raw: bytes) -> dict:
    """Normalize once; every adapter receives these exact rows and parameters."""
    receipt = strict_json(receipt_raw)
    if not isinstance(receipt, dict) or receipt.get("schema") != RECEIPT_SCHEMA:
        raise InputRejected("unsupported receipt schema")
    if receipt.get("raw_sha256") != digest(raw):
        raise InputRejected("receipt does not bind these raw bytes")
    clock = receipt.get("clock")
    if clock not in ("untrusted_workstation", "grid_svr_pull_receipt"):
        raise InputRejected("receipt clock basis must be named explicitly")
    started = aware(receipt.get("request_started_at"), "request_started_at")
    received = aware(receipt.get("receipt_completed_at"), "receipt_completed_at")
    if started > received:
        raise InputRejected("receipt completed before it started")

    doc = strict_json(raw)
    if not isinstance(doc, dict) or not isinstance(doc.get("data"), dict):
        raise InputRejected("not a Cboe delayed-quotes document")
    data = doc["data"]
    if data.get("symbol") != "SPY" or doc.get("symbol") not in (None, "SPY"):
        raise InputRejected("initial P2-B scope is standard SPY contracts only")
    spot = number(data.get("current_price"))
    change = number(data.get("price_change"))
    if spot is None or spot <= 0 or change is None:
        raise InputRejected("underlying current_price/price_change missing")
    trade_time = data.get("last_trade_time")
    if not isinstance(trade_time, str):
        raise InputRejected("underlying last_trade_time missing")
    try:
        local = datetime.fromisoformat(trade_time)
    except ValueError as exc:
        raise InputRejected("underlying last_trade_time malformed") from exc
    if local.tzinfo is not None:
        raise InputRejected("unexpected offset on Cboe last_trade_time")
    # Assumption, recorded in the packet: Cboe's naive underlying trade time is
    # America/New_York. Provider timezone is not independently verified.
    valuation = local.replace(tzinfo=ET).astimezone(timezone.utc)
    if received < valuation:
        raise InputRejected("receipt precedes valuation instant")
    session = valuation.astimezone(ET).date()
    options = data.get("options")
    if not isinstance(options, list) or not options:
        raise InputRejected("empty option chain")

    rows, exclusions, seen = [], [], set()
    for item in options:
        symbol = item.get("option") if isinstance(item, dict) else None
        oi_raw = number(item.get("open_interest")) if isinstance(item, dict) else None
        match = OSI_SPY.fullmatch(symbol) if isinstance(symbol, str) else None
        if match is None:
            exclusions.append(
                {"contract": symbol, "reason": "NONSTANDARD_SYMBOL", "oi": oi_raw}
            )
            continue
        expiry_day = _osi_date(match[1])
        side = "call" if match[2] == "C" else "put"
        strike = int(match[3]) / 1000
        identity = ["SPY", expiry_instant(expiry_day).isoformat(), strike, side, 100.0]
        key = tuple(identity)
        if key in seen:
            raise InputRejected(f"duplicate contract identity {symbol}")
        seen.add(key)
        reason = None
        iv = number(item.get("iv"))
        if oi_raw is None or oi_raw < 0 or oi_raw != int(oi_raw):
            reason = "OI_INVALID"
        elif expiry_instant(expiry_day) <= valuation:
            reason = "EXPIRED"
        elif oi_raw == 0:
            reason = "ZERO_OI_NO_EXPOSURE"
        elif iv is None or iv <= 0:
            reason = "IV_MISSING_OR_NONPOSITIVE"
        elif iv > 5:
            reason = "IV_OUT_OF_RANGE_PERCENT_SUSPECT"
        if reason:
            # Invalid OI is unknown mass, never its raw (or zero) value.
            mass = None if reason == "OI_INVALID" else oi_raw
            exclusions.append({"contract": symbol, "reason": reason, "oi": mass})
            continue
        seconds = (expiry_instant(expiry_day) - valuation).total_seconds()
        rows.append(
            {
                "contract": symbol,
                "identity": identity,
                "expiry_date": expiry_day.isoformat(),
                "strike": strike,
                "side": side,
                "sign": 1 if side == "call" else -1,
                "iv": iv,
                "iv_origin": "direct",
                "iv_provider": "cboe_delayed",
                "oi": oi_raw,
                "oi_as_of": None,
                "multiplier": 100.0,
                "multiplier_basis": "osi_standard_root_inferred",
                "deliverable": "standard_shares_inferred",
                "T": seconds / YEAR_SECONDS,
                "calendar_dte": (expiry_day - session).days,
                "early_close_candidate": early_close_candidate(expiry_day),
                "provider": {
                    k: number(item.get(k))
                    for k in ("gamma", "delta", "bid", "ask", "theo", "volume")
                },
            }
        )
    rows.sort(key=lambda r: r["identity"])
    exclusions.sort(key=lambda e: (e["reason"], str(e["contract"])))
    if not rows:
        raise InputRejected("no eligible contracts after exclusions")
    normalized = {
        "schema": PACKET_SCHEMA,
        "source": "cboe_delayed_quotes_options_json",
        "source_url": receipt.get("url"),
        "price_basis": "unadjusted",
        "calendar_version": CALENDAR_VERSION,
        "valuation_at": valuation.isoformat(),
        "valuation_basis": "cboe_underlying_last_trade_time_assumed_America/New_York",
        "session_date": session.isoformat(),
        "available_at": received.isoformat(),
        "availability_basis": clock,
        "spot": spot,
        "spot_basis": "cboe_current_price_delayed",
        "prior_close_derived": spot - change,
        "prior_close_field": number(data.get("prev_day_close")),
        "provider_timestamp_raw": doc.get("timestamp"),
        "r": BASELINE["r"],
        "q": BASELINE["q"],
        "rows": rows,
        "exclusions": exclusions,
    }
    normalized["packet_id"] = str(uuid.UUID(hex=digest(raw)[:32]))
    return normalized


def packet_hash(p: dict) -> str:
    return digest(canonical(p))


# ── independent reference (imports neither engine) ───────────────────────


def reference_gamma_vec(s, k, t, r, q, iv):
    """Float64 independent recompute; certified against the Decimal reference."""
    d = (np.log(s / k) + (r - q + iv * iv / 2) * t) / (iv * np.sqrt(t))
    return np.exp(-q * t - d * d / 2) / (s * iv * np.sqrt(2 * math.pi * t))


def arrays(rows, *, T=None, iv=None):
    return {
        "K": np.array([r["strike"] for r in rows]),
        "T": np.array([r["T"] for r in rows]) if T is None else np.asarray(T),
        "iv": np.array([r["iv"] for r in rows]) if iv is None else np.asarray(iv),
        "oi": np.array([r["oi"] for r in rows]),
        "m": np.array([r["multiplier"] for r in rows]),
        "sign": np.array([float(r["sign"]) for r in rows]),
    }


def reference_terms(a, s, r, q):
    g = reference_gamma_vec(s, a["K"], a["T"], r, q, a["iv"])
    return a["sign"] * g * a["oi"] * a["m"] * s * s * 0.01


def reference_curve(a, spots, r, q):
    values, gross = [], []
    for s in spots:
        terms = reference_terms(a, float(s), r, q)
        values.append(math.fsum(terms.tolist()))
        gross.append(math.fsum(np.abs(terms).tolist()))
    return np.array(values), np.array(gross)


def agg_tolerance(gross: float) -> float:
    return AGG_ABS + AGG_REL * gross


# ── adapters over the actual engine code ─────────────────────────────────


def grid_engine(r: float):
    from physics.dealer_gamma import DealerGammaEngine

    return DealerGammaEngine(None, risk_free_rate=r)


def grid_chain(rows, dte):
    import pandas as pd

    return pd.DataFrame(
        {
            "strike": [r["strike"] for r in rows],
            "opt_type": [r["side"] for r in rows],
            "open_interest": [r["oi"] for r in rows],
            "implied_volatility": [r["iv"] for r in rows],
            "expiry": [date.fromisoformat(r["expiry_date"]) for r in rows],
            "dte": list(dte),
        }
    )


def grid_curve(rows, spots, r):
    """GRID engine-level aggregation (_gex_at_spots_vectorized), normalized.

    Adapter: dte column = T * 365 so the engine's T = dte / 365 reproduces the
    packet T (within one ulp; mismatches are counted). Native GRID units are
    gamma*OI*100*S; multiplying by S*0.01 at every point gives $/1% move.
    """
    engine = grid_engine(r)
    chain = grid_chain(rows, [row["T"] * 365.0 for row in rows])
    prepared = engine._prepare_chain_arrays(chain)
    spots = np.asarray(spots, dtype=np.float64)
    native = engine._gex_at_spots_vectorized(*prepared, spots)
    return native * spots * 0.01


def _collector_tree():
    path = ROOT / "collectors/gamma_watch/broker.py"
    return path, ast.parse(path.read_text(encoding="utf-8-sig"))


def _function(tree, name):
    return next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name
    )


def native_fingerprint(functions) -> str:
    """Bodies + argument names only: FunctionDef gained type_params in 3.12,
    so hashing the whole node would differ between Python 3.10/3.11/3.13."""
    signature = [[f.name, [a.arg for a in f.args.args]] for f in functions]
    body = [stmt for f in functions for stmt in f.body]
    return digest(
        canonical(
            [signature, p2a.ast_fingerprint(ast.Module(body=body, type_ignores=[]))]
        )
    )


class _Parameters(ast.NodeTransformer):
    """Disclosed test-only transformation (as P2-A): .04/.012 -> packet r/q."""

    def visit_Constant(self, node):
        if isinstance(node.value, float) and node.value in (0.04, 0.012):
            name = "r" if node.value == 0.04 else "q"
            return ast.copy_location(ast.Name(id=name, ctx=ast.Load()), node)
        return node


def watch_aggregation():
    """Extract curves()'s actual iv/d1/gamma/net statements (shock loop body).

    The statements are executed unrounded on packet arrays; the only
    transformation is r/q parameterization. Returns (callable, file sha256,
    fingerprint). Native rounding, gates and clock are exercised separately.
    """
    path, tree = _collector_tree()
    fn = _function(tree, "curves")
    loop = next(
        n
        for n in ast.walk(fn)
        if isinstance(n, ast.For)
        and isinstance(n.iter, ast.List)
        and [getattr(e, "value", None) for e in n.iter.elts] == [0, 0.2, 0.4]
    )
    selected = [
        n
        for n in loop.body
        if isinstance(n, ast.Assign)
        and isinstance(n.targets[0], ast.Name)
        and n.targets[0].id in ("iv", "d1", "gamma", "net")
    ]
    fingerprint = p2a.ast_fingerprint(ast.Module(body=selected, type_ignores=[]))
    module = ast.Module(
        body=[_Parameters().visit(copy.deepcopy(n)) for n in selected],
        type_ignores=[],
    )
    compiled = compile(ast.fix_missing_locations(module), str(path), "exec")

    def evaluate(a, spots, r, q):
        env = {
            "S": np.asarray(spots, dtype=np.float64)[:, None],
            "K": a["K"],
            "T": a["T"],
            "vol": a["iv"],
            "oi": a["oi"],
            "sign": a["sign"],
            "shock": 0,
            "r": r,
            "q": q,
            "np": np,
        }
        exec(compiled, {"__builtins__": {}}, env)
        return env["net"] * 1e9  # native billions -> $/1% move

    return evaluate, digest(path.read_bytes()), fingerprint


_NATIVE_BUILTINS = {
    name: getattr(builtins, name)
    for name in (
        "ValueError",
        "all",
        "float",
        "len",
        "range",
        "round",
        "set",
        "sorted",
        "sum",
    )
}


def watch_native(feed: dict, contracts: list, valuation: datetime):
    """Run the actual curves()/dt() source with only the wall clock frozen.

    Disclosed substitutions: datetime.now -> packet valuation instant;
    CONTRACTS -> packet universe; feed built from packet rows. Rounding, gates,
    IV pairing, 20:00Z expiry approximation and native r/q are unchanged.
    """
    path, tree = _collector_tree()
    selected = [_function(tree, "dt"), _function(tree, "curves")]
    fingerprint = native_fingerprint(selected)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return valuation.astimezone(tz) if tz else valuation.replace(tzinfo=None)

    namespace = {
        "__builtins__": _NATIVE_BUILTINS,
        "datetime": FrozenDatetime,
        "timezone": timezone,
        "np": np,
        "re": re,
        "CONTRACTS": contracts,
    }
    module = ast.Module(body=selected, type_ignores=[])
    exec(compile(module, str(path), "exec"), namespace)
    return namespace["curves"](feed), fingerprint


# ── comparisons ──────────────────────────────────────────────────────────


def contract_status(actual: float, ref: Decimal) -> tuple[str, Decimal]:
    error = abs(Decimal.from_float(float(actual)) - ref)
    tol = Decimal.from_float(CONTRACT_ABS) + Decimal.from_float(CONTRACT_REL) * abs(ref)
    return ("PASS_NUMERICAL" if error <= tol else "FAIL_NUMERICAL"), error


def per_contract(p, primitive, watch):
    """Protocol step 3 at the packet spot: contract gamma, signed contribution,
    expiry subtotal, strike subtotal and total, for GRID's shared primitive
    (per-strike path) and Gamma Watch's extracted kernel, vs the Decimal
    reference. The P2-A propagated error bound is reported alongside."""
    s, r, q = p["spot"], p["r"], p["q"]
    engines = {
        "grid_primitive": {"counts": {}, "bound_counts": {}, "max_rel_error": 0.0},
        "gamma_watch_kernel": {"counts": {}, "bound_counts": {}, "max_rel_error": 0.0},
    }
    contributions = {name: [] for name in engines}
    reference, certify = [], 0.0
    with localcontext() as ctx:
        ctx.prec = 80
        for row in p["rows"]:
            args = (s, row["strike"], row["T"], r, q, row["iv"])
            ref = decimal_gamma(*args, precision=80)
            converged = decimal_gamma(*args, precision=110)
            factor = Decimal.from_float(row["oi"] * row["multiplier"] * s * s * 0.01)
            factor *= row["sign"]
            reference.append(ref * factor)
            fl = float(reference_gamma_vec(s, row["strike"], row["T"], r, q, row["iv"]))
            if ref != 0:
                certify = max(certify, float(abs(Decimal.from_float(fl) - ref) / ref))
            outputs = {
                "grid_primitive": (
                    float(primitive.gamma(s, row["strike"], row["T"], r, row["iv"], q=q)),
                    lambda: reference_bound(*args),
                ),
            }
            if watch is not None:
                outputs["gamma_watch_kernel"] = (
                    float(watch(*args)),
                    lambda: watch(*args, bounded=True),
                )
            for name, (actual, ball) in outputs.items():
                if not math.isfinite(actual):
                    raise InputRejected("nonfinite engine gamma")
                status, error = contract_status(actual, ref)
                e = engines[name]
                e["counts"][status] = e["counts"].get(status, 0) + 1
                if ref != 0:
                    e["max_rel_error"] = max(e["max_rel_error"], float(error / abs(ref)))
                bound = ball().e + abs(ref - converged)
                bstatus = "PASS_NUMERICAL" if error <= bound else "FAIL_NUMERICAL"
                e["bound_counts"][bstatus] = e["bound_counts"].get(bstatus, 0) + 1
                term = actual * row["oi"] * row["multiplier"] * s * s * 0.01 * row["sign"]
                if not math.isfinite(term):
                    raise InputRejected("nonfinite exposure contribution")
                contributions[name].append(term)
    gross = float(sum(abs(x) for x in reference))
    tol = agg_tolerance(gross)
    if watch is None:
        engines["gamma_watch_kernel"] = {
            "status": "NOT_SUPPORTED",
            "reason": "collector d1/gamma fingerprint drift (P2-A pin); review adapter",
        }
    for name, e in engines.items():
        if name not in contributions or (watch is None and name == "gamma_watch_kernel"):
            continue
        terms = contributions[name]
        e["contract_status"] = (
            "FAIL_NUMERICAL" if e["counts"].get("FAIL_NUMERICAL") else "PASS_NUMERICAL"
        )
        e["subtotals"] = {}
        for label, key in (("expiry", "expiry_date"), ("strike", "strike")):
            groups_ref, groups = {}, {}
            for row, ref_term, term in zip(p["rows"], reference, terms):
                groups_ref[row[key]] = groups_ref.get(row[key], Decimal(0)) + ref_term
                groups.setdefault(row[key], []).append(term)
            worst = max(
                abs(math.fsum(groups[k]) - float(v)) for k, v in groups_ref.items()
            )
            e["subtotals"][label] = {
                "groups": len(groups_ref),
                "max_abs_error": worst,
                "tolerance": tol,
                "status": "PASS_NUMERICAL" if worst <= tol else "FAIL_NUMERICAL",
            }
        total, ref_total = math.fsum(terms), float(sum(reference, Decimal(0)))
        e["total"] = {
            "value": total,
            "reference": ref_total,
            "abs_error": abs(total - ref_total),
            "tolerance": tol,
            "status": "PASS_NUMERICAL"
            if abs(total - ref_total) <= tol
            else "FAIL_NUMERICAL",
            "sign": "INDETERMINATE"
            if abs(ref_total) <= tol
            else ("POSITIVE" if ref_total > 0 else "NEGATIVE"),
        }
        parts = [e["contract_status"], e["total"]["status"]]
        parts += [x["status"] for x in e["subtotals"].values()]
        e["status"] = worst_status(parts)
    return {
        "spot": s,
        "contracts": len(p["rows"]),
        "gross_reference": gross,
        "reference_float_max_rel_error": certify,
        "reference_float_certified": certify <= CERTIFY_REL,
        "engines": engines,
    }


def worst_status(statuses) -> str:
    order = (
        "INPUT_REJECTED",
        "FAIL_NUMERICAL",
        "INDETERMINATE",
        "NOT_SUPPORTED",
        "NOT_COMPARABLE",
        "PASS_NUMERICAL",
    )
    present = set(statuses)
    return next((s for s in order if s in present), "NOT_SUPPORTED")


def common_grid(spot: float):
    lo, hi, step = WATCH_GRID
    if not WATCH_SPOT_GATE[0] <= spot <= WATCH_SPOT_GATE[1]:
        lo, hi = round(spot * 0.95 / step) * step, round(spot * 1.05 / step) * step
    count = int(round((hi - lo) / step)) + 1
    return [lo + i * step for i in range(count)]


def brackets(values, spots, tol):
    """Strict sign-change brackets; |value|<=tol at an endpoint -> indeterminate."""
    found, uncertain = [], []
    for i in range(len(spots) - 1):
        a, b = values[i], values[i + 1]
        if a == 0 or b == 0:
            uncertain.append([spots[i], spots[i + 1]])
        elif (a > 0) != (b > 0):
            if tol is not None and (abs(a) <= tol[i] or abs(b) <= tol[i + 1]):
                uncertain.append([spots[i], spots[i + 1]])
            found.append((spots[i], spots[i + 1]))
    return found, uncertain


def refine(evaluate, lo, hi):
    f_lo = evaluate(lo)
    for _ in range(200):
        if hi - lo <= ROOT_BRACKET:
            break
        mid = (lo + hi) / 2
        f_mid = evaluate(mid)
        if f_mid == 0:
            return mid, mid
        if (f_mid > 0) == (f_lo > 0):
            lo, f_lo = mid, f_mid
        else:
            hi = mid
    return lo, hi


def root_set(evaluate, values, spots, tol=None):
    found, uncertain = brackets(values, spots, tol)
    roots = []
    for lo, hi in found:
        a, b = refine(evaluate, lo, hi)
        roots.append({"root": (a + b) / 2, "bracket": [a, b]})
    return roots, uncertain


def match_roots(engine, reference, uncertain):
    if uncertain:
        return "INDETERMINATE"
    if len(engine) != len(reference):
        return "FAIL_NUMERICAL"
    ok = all(
        abs(x["root"] - y["root"]) <= ROOT_MATCH for x, y in zip(engine, reference)
    )
    return "PASS_NUMERICAL" if ok else "FAIL_NUMERICAL"


def walls(per_strike: dict, tol: float) -> dict:
    """Protocol wall definitions; ties within tolerance are kept as sets."""

    def pick(values, best):
        if not values:
            return []
        target = best(values.values())
        return sorted(k for k, v in values.items() if abs(v - target) <= tol)

    calls = {k: v["call"] for k, v in per_strike.items() if v["call"] > 0}
    puts = {k: v["put"] for k, v in per_strike.items() if v["put"] < 0}
    nets = {k: abs(v["call"] + v["put"]) for k, v in per_strike.items()}
    return {
        "call_wall_max_positive_call": pick(calls, max),
        "put_wall_most_negative_put": pick(puts, min),
        "net_wall_max_abs_net": pick(nets, max),
    }


def reference_per_strike(a, rows, s, r, q):
    terms = reference_terms(a, s, r, q)
    out: dict[float, dict] = {}
    for row, term in zip(rows, terms.tolist()):
        bucket = out.setdefault(row["strike"], {"call": [], "put": []})
        bucket[row["side"]].append(term)
    return {
        k: {"call": math.fsum(v["call"]), "put": math.fsum(v["put"])}
        for k, v in out.items()
    }


def grid_per_strike(rows, s, r):
    engine = grid_engine(r)
    chain = grid_chain(rows, [row["T"] * 365.0 for row in rows])
    scale = s * 0.01
    return {
        x["strike"]: {"call": x["call_gex"] * scale, "put": x["put_gex"] * scale}
        for x in engine._compute_per_strike(chain, s)
    }


def engine_curves(p, a, spots, watch_eval):
    r, q = p["r"], p["q"]
    ref_values, ref_gross = reference_curve(a, spots, r, q)
    tol = [agg_tolerance(g) for g in ref_gross]
    out = {
        "grid": spots,
        "reference": {
            "values": ref_values.tolist(),
            "gross": ref_gross.tolist(),
            "implementation": "independent float64 + math.fsum, Decimal-certified",
        },
        "engines": {},
    }

    def ref_eval(x):
        return reference_curve(a, [x], r, q)[0][0]

    ref_roots, uncertain = root_set(ref_eval, ref_values, spots, tol)
    out["reference"]["roots"] = ref_roots
    out["reference"]["indeterminate_brackets"] = uncertain
    out["reference"]["sign_indeterminate_points"] = [
        x for x, v, t in zip(spots, ref_values, tol) if abs(v) <= t
    ]
    evaluators = {
        "grid_engine_vectorized": (
            (lambda xs: grid_curve(p["rows"], xs, r)) if q == 0 else None,
            "physics/dealer_gamma.py DealerGammaEngine._gex_at_spots_vectorized",
        ),
        "gamma_watch_aggregation": (
            (lambda xs: watch_eval(a, xs, r, q)) if watch_eval else None,
            "collectors/gamma_watch/broker.py curves() iv/d1/gamma/net statements",
        ),
    }
    for name, (fn, source) in evaluators.items():
        if fn is None:
            out["engines"][name] = {
                "source": source,
                "status": "NOT_SUPPORTED",
                "reason": "engine cannot represent packet parameters (q != 0)"
                if name.startswith("grid")
                else "collector source fingerprint drift; review adapter",
            }
            continue
        values = np.asarray(fn(spots), dtype=np.float64)
        if not np.all(np.isfinite(values)):
            raise InputRejected(f"nonfinite {name} curve")
        errors = np.abs(values - ref_values)
        point_ok = errors <= np.array(tol)
        roots, _ = root_set(lambda x: float(fn([x])[0]), values.tolist(), spots)
        root_status = match_roots(roots, ref_roots, uncertain)
        curve_status = "PASS_NUMERICAL" if bool(point_ok.all()) else "FAIL_NUMERICAL"
        out["engines"][name] = {
            "source": source,
            "values": values.tolist(),
            "max_abs_error": float(errors.max()),
            "max_error_over_gross": float(np.max(errors / np.maximum(ref_gross, 1e-300))),
            "points_failed": int((~point_ok).sum()),
            "curve_status": curve_status,
            "roots": roots,
            "root_status": root_status,
            "status": worst_status([curve_status, root_status]),
        }
    return out, ref_eval


def wall_comparison(p, a):
    s, r, q = p["spot"], p["r"], p["q"]
    ref = reference_per_strike(a, p["rows"], s, r, q)
    gross = math.fsum(abs(v["call"]) + abs(v["put"]) for v in ref.values())
    tol = agg_tolerance(gross)
    ref_walls = walls(ref, tol)
    out = {"spot": s, "tolerance": tol, "reference": ref_walls, "engines": {}}
    if q != 0:
        out["engines"]["grid_per_strike"] = {
            "status": "NOT_SUPPORTED",
            "reason": "GRID per-strike path has no dividend yield",
        }
    else:
        grid = grid_per_strike(p["rows"], s, r)
        same_keys = set(grid) == set(ref)
        worst = max(
            max(abs(grid[k]["call"] - v["call"]), abs(grid[k]["put"] - v["put"]))
            for k, v in ref.items()
            if k in grid
        )
        grid_walls = walls(grid, tol)
        strike_status = (
            "PASS_NUMERICAL" if same_keys and worst <= tol else "FAIL_NUMERICAL"
        )
        wall_status = "PASS_NUMERICAL" if grid_walls == ref_walls else "FAIL_NUMERICAL"
        out["engines"]["grid_per_strike"] = {
            "source": "physics/dealer_gamma.py DealerGammaEngine._compute_per_strike",
            "walls": grid_walls,
            "strike_max_abs_error": worst,
            "strike_status": strike_status,
            "wall_status": wall_status,
            "status": worst_status([strike_status, wall_status]),
        }
    out["engines"]["gamma_watch"] = {
        "status": "NOT_SUPPORTED",
        "reason": "broker.py:curves computes no strike walls; its walls are ZeroGEX vendor output (class 3)",
    }
    return out


# ── native behavior (protocol step 1) ────────────────────────────────────


def grid_native(p):
    """Actual DealerGammaEngine.compute_gex_profile with only the two DB loaders
    substituted by packet adapters (chain + prior-close receipt). Native r=.05,
    q=0, integer calendar DTE, dte>0/OI>0/IV>0 filters, prior close spot."""
    session = date.fromisoformat(p["session_date"])
    valuation = datetime.fromisoformat(p["valuation_at"])
    engine = grid_engine(GRID_NATIVE["r"])
    chain = grid_chain(p["rows"], [row["calendar_dte"] for row in p["rows"]])
    chain = chain[(chain["dte"] > 0) & (chain["open_interest"] > 0)].copy()
    chain.attrs.update(
        snap_date=session,
        batch_id=p["packet_id"],
        capture_ordinal=1,
        capture_started_at=valuation,
        capture_completed_at=valuation,
        provider_regular_market_at_min=valuation,
        provider_regular_market_at_max=valuation,
        created_at_min=valuation,
        created_at_max=valuation,
    )
    prior = session - timedelta(days=1)
    while prior.weekday() >= 5:
        prior -= timedelta(days=1)
    receipt = {
        "price": p["prior_close_derived"],
        "obs_date": prior,
        "available_at": valuation,
        "receipt_created_at": valuation,
        "release_date": session,
        "vintage_date": session,
        "receipt_id": "adapter:cboe_current_minus_change",
    }
    engine._load_chain = lambda ticker, snap_date, capture_batch_id=None: chain
    engine._get_spot_receipt = lambda ticker, completed: receipt
    out = engine.compute_gex_profile("SPY", session)
    out["adapter"] = (
        "DB loaders replaced by packet chain/receipt; receipt is provider-derived "
        "(current_price - price_change), not GRID's spy_close_receipt"
    )
    return out


def watch_native_run(p):
    valuation = datetime.fromisoformat(p["valuation_at"])
    rows = [
        {
            "expiry": r["expiry_date"],
            "strike": r["strike"],
            "opt_type": r["side"],
            "implied_vol": r["iv"],
            "open_interest": r["oi"],
            "iv_origin": "direct",
            "provider_gamma": None,
        }
        for r in p["rows"]
    ]
    feed = {
        "rows": rows,
        "price": p["spot"],
        "coverage": 1.0,
        "near_spot_coverage": 1.0,
        "collected_at": p["valuation_at"],
        "expiries": sorted({r["expiry"] for r in rows}),
    }
    try:
        path, tree = _collector_tree()
        fingerprint = native_fingerprint(
            [_function(tree, "dt"), _function(tree, "curves")]
        )
    except (StopIteration, SyntaxError, OSError):
        fingerprint = None
    if fingerprint == WATCH_NATIVE_FINGERPRINT:
        try:
            out, fingerprint = watch_native(feed, rows, valuation)
        except ValueError as exc:
            # The collector's own gates (coverage, spot window) withheld it.
            return {"status": "NOT_SUPPORTED", "reason": f"native gate: {exc}"}
    if fingerprint != WATCH_NATIVE_FINGERPRINT:
        return {
            "status": "NOT_SUPPORTED",
            "reason": "collector curves()/dt() fingerprint drift; review adapter",
            "fingerprint": fingerprint,
        }
    out["adapter"] = (
        "clock frozen at packet valuation; CONTRACTS/feed built from packet rows; "
        "RTD coverage gates set to 1.0 because every packet row has direct IV"
    )
    return out


# ── one-factor attribution (protocol step 4) ─────────────────────────────


def paired_otm_iv(rows, spot):
    by_key = {(r["expiry_date"], r["strike"], r["side"]): r["iv"] for r in rows}
    out = []
    for r in rows:
        side = "put" if r["strike"] < spot else "call"
        out.append(by_key.get((r["expiry_date"], r["strike"], side), r["iv"]))
    return out


def attribution(p, spots):
    rows, spot = p["rows"], p["spot"]
    valuation = datetime.fromisoformat(p["valuation_at"])
    t_integer = [r["calendar_dte"] / 365.0 for r in rows]
    t_fixed20 = [
        (
            datetime.combine(date.fromisoformat(r["expiry_date"]), time(20, 0), timezone.utc)
            - valuation
        ).total_seconds()
        / YEAR_SECONDS
        for r in rows
    ]
    window = [WATCH_GRID[0] <= r["strike"] <= WATCH_GRID[1] for r in rows]

    def run(*, r=BASELINE["r"], q=BASELINE["q"], T=None, iv=None, keep=None, at=spot):
        use = [i for i, row in enumerate(rows) if (keep is None or keep[i])]
        pick = lambda xs: None if xs is None else [xs[i] for i in use]  # noqa: E731
        T_use = pick(T)
        if T_use is not None and any(t <= 0 for t in T_use):
            use = [i for i, t in zip(use, T_use) if t > 0]
            T_use = [x for x in T_use if x > 0]
        a = arrays([rows[i] for i in use], T=T_use, iv=pick(iv))
        at_value = math.fsum(reference_terms(a, at, r, q).tolist())
        values, gross = reference_curve(a, spots, r, q)
        roots, _ = root_set(
            lambda x: reference_curve(a, [x], r, q)[0][0], values.tolist(), spots
        )
        return {
            "G_at_eval_spot": at_value,
            "eval_spot": at,
            "contracts": len(use),
            "roots": [round(x["root"], 3) for x in roots],
        }

    base = run()
    factors = {
        "r_grid_native_0.05": run(r=GRID_NATIVE["r"]),
        "q_watch_native_0.012": run(q=WATCH_NATIVE["q"]),
        "T_grid_integer_calendar_dte": run(T=t_integer),
        "T_watch_fixed_20Z_expiry": run(T=t_fixed20),
        "spot_prior_close_derived": run(at=p["prior_close_derived"]),
        "iv_watch_paired_otm": run(iv=paired_otm_iv(rows, spot)),
        "universe_watch_strike_window_735_790": run(keep=window),
    }
    zero_dte = sum(1 for r in rows if r["calendar_dte"] == 0)
    combined = {
        "grid_native_combined": run(
            r=GRID_NATIVE["r"], T=t_integer, at=p["prior_close_derived"]
        ),
        "watch_native_combined": run(
            r=WATCH_NATIVE["r"],
            q=WATCH_NATIVE["q"],
            T=t_fixed20,
            iv=paired_otm_iv(rows, spot),
            keep=window,
        ),
    }
    for item in list(factors.values()) + list(combined.values()):
        item["delta_G"] = item["G_at_eval_spot"] - base["G_at_eval_spot"]
    members = {
        "grid_native_combined": (
            "r_grid_native_0.05",
            "T_grid_integer_calendar_dte",
            "spot_prior_close_derived",
        ),
        "watch_native_combined": (
            "q_watch_native_0.012",
            "T_watch_fixed_20Z_expiry",
            "iv_watch_paired_otm",
            "universe_watch_strike_window_735_790",
        ),
    }
    for name, parts in members.items():
        combined[name]["interaction_residual"] = combined[name]["delta_G"] - sum(
            factors[x]["delta_G"] for x in parts
        )
    return {
        "evaluator": "certified independent reference (float64 + fsum)",
        "baseline": base,
        "factors": factors,
        "combined": combined,
        "zero_dte": {
            "rows": zero_dte,
            "status": "NOT_SUPPORTED" if zero_dte else "NOT_COMPARABLE",
            "note": "GRID native loader excludes calendar DTE 0; none present at this valuation"
            if not zero_dte
            else "GRID native equivalence NOT_SUPPORTED for 0DTE rows",
        },
        "oi_vintage": {
            "status": "NOT_SUPPORTED",
            "note": "single OI vintage; Cboe supplies no OI as-of date",
        },
        "note": "Interacting factors are not additive; residuals are reported, not apportioned.",
    }


# ── vendor comparability (classes 2 and 3) ───────────────────────────────


def cboe_greek_diagnostic(p):
    s, r, q = p["spot"], p["r"], p["q"]
    rel = []
    for row in p["rows"]:
        g = row["provider"]["gamma"]
        if g is None or g <= 0:
            continue
        ref = float(reference_gamma_vec(s, row["strike"], row["T"], r, q, row["iv"]))
        if ref > 0:
            rel.append(abs(g - ref) / ref)
    rel.sort()
    pct = (lambda f: rel[min(len(rel) - 1, int(f * len(rel)))]) if rel else None
    return {
        "class": 3,
        "status": "NOT_COMPARABLE",
        "contracts_with_provider_gamma": len(rel),
        "rel_diff_median": pct(0.5) if pct else None,
        "rel_diff_p95": pct(0.95) if pct else None,
        "rel_diff_max": rel[-1] if rel else None,
        "reason": "Cboe gamma methodology, rates, dividends, time convention and rounding are undisclosed",
    }


def vendor_table(p):
    return {
        "class_2_eligible_packets": 0,
        "class_2_status": "NOT_SUPPORTED",
        "class_2_reason": (
            "No free source supplies a vendor's underlying snapshot, contract "
            "universe, OI vintage, timestamp, units and methodology. Not bought, "
            "inferred, time-shifted or backfilled."
        ),
        "rows": [
            {
                "vendor": "ZeroGEX delayed SPY levels",
                "class": 3,
                "status": "NOT_COMPARABLE",
                "reason": "headline levels only; no universe/OI vintage/methodology; not fetched in this run",
            },
            {
                "vendor": "Cboe per-contract greeks (same packet)",
                **cboe_greek_diagnostic(p),
            },
            {
                "vendor": "thinkorswim RTD GAMMA",
                "class": 3,
                "status": "NOT_COMPARABLE",
                "reason": "rounded provider field; not in this packet",
            },
            {
                "vendor": "paid GEX vendors",
                "class": 2,
                "status": "NOT_SUPPORTED",
                "reason": "paid APIs are off; any purchase is a separate owner ask",
            },
        ],
    }


# ── orchestration and receipts ───────────────────────────────────────────


def code_hashes():
    files = {
        "p2b_harness": Path(__file__),
        "p2a_harness": ROOT / "scripts/gex_p2a/harness.py",
        "p2a_reference": ROOT / "scripts/gex_p2a/reference.py",
        "grid_dealer_gamma": ROOT / "physics/dealer_gamma.py",
        "grid_black_scholes": ROOT / "physics/greeks/black_scholes.py",
        "gamma_watch_broker": ROOT / "collectors/gamma_watch/broker.py",
        "protocol": PROTOCOL,
    }
    return {k: digest(v.read_bytes()) for k, v in files.items()}


def environment():
    return {
        "python": sys.version,
        "numpy": np.__version__,
        "platform": sys.platform,
        "float_mantissa_bits": sys.float_info.mant_dig,
    }


def reconcile(raw: bytes, receipt_raw: bytes) -> tuple[dict, dict]:
    """Pure: bytes in, (complete result, normalized packet) out."""
    p = normalize(raw, receipt_raw)
    phash = packet_hash(p)
    primitive, _ = p2a.load_primitive()
    try:
        watch, _ = p2a.watch_kernel()
    except COLLECTOR_DRIFT:
        watch = None
    a = arrays(p["rows"])
    spots = common_grid(p["spot"])
    try:
        agg_eval, _, agg_fingerprint = watch_aggregation()
    except COLLECTOR_DRIFT:
        agg_eval, agg_fingerprint = None, None
    if agg_fingerprint != WATCH_AGGREGATION_FINGERPRINT:
        agg_eval = None
    contract = per_contract(p, primitive, watch)
    curves, _ = engine_curves(p, a, spots, agg_eval)
    if not contract["reference_float_certified"]:
        for e in curves["engines"].values():
            e["status"] = "INDETERMINATE"
            e["reason"] = "float reference not certified against Decimal reference"
    wall = wall_comparison(p, a)
    class1 = [e["status"] for e in contract["engines"].values()]
    class1 += [e["status"] for e in curves["engines"].values()]
    class1 += [
        e["status"] for e in wall["engines"].values() if e["status"] != "NOT_SUPPORTED"
    ]
    timing = (
        "INDETERMINATE"
        if p["availability_basis"] == "untrusted_workstation"
        else "NOT_SUPPORTED"
    )
    return {
        "schema": RESULT_SCHEMA,
        "packet_id": p["packet_id"],
        "packet_sha256": phash,
        "raw_sha256": digest(raw),
        "receipt_sha256": digest(receipt_raw),
        "class_1_status": worst_status(class1),
        "timing_validation": {
            "status": timing,
            "basis": p["availability_basis"],
            "note": "pull receipt proves when bytes were held, not exchange freshness or OI age",
        },
        "parameters": {"r": p["r"], "q": p["q"], "spot": p["spot"], "grid": spots},
        "tolerances": TOLERANCES,
        "per_contract": contract,
        "curves": curves,
        "walls": wall,
        "native": {"grid": grid_native(p), "gamma_watch": watch_native_run(p)},
        "attribution": attribution(p, spots),
        "vendor": vendor_table(p),
        "exclusion_summary": exclusion_summary(p),
        "code_hashes": code_hashes(),
        "environment": environment(),
        "scope": (
            "class 1 exact-input arithmetic on one real Cboe delayed SPY packet; "
            "not vendor validation, dealer positioning or a trading edge; "
            "no canonical-engine choice"
        ),
    }, p


def exclusion_summary(p):
    out = {}
    for e in p["exclusions"]:
        bucket = out.setdefault(e["reason"], {"rows": 0, "oi": 0.0, "oi_unknown": 0})
        bucket["rows"] += 1
        if e["oi"] is None:
            bucket["oi_unknown"] += 1
        else:
            bucket["oi"] += e["oi"]
    flagged = [r["contract"] for r in p["rows"] if r["early_close_candidate"]]
    return {
        "by_reason": out,
        "included_rows": len(p["rows"]),
        "included_oi": math.fsum(r["oi"] for r in p["rows"]),
        "early_close_candidate_rows": len(flagged),
        "early_close_candidate_oi": math.fsum(
            r["oi"] for r in p["rows"] if r["early_close_candidate"]
        ),
    }


def report(result: dict, packet: dict | None) -> str:
    lines = [
        "# GEX P2-B matched real-input packet report",
        "",
        f"Status (class 1 exact-input arithmetic): **{result['class_1_status']}**",
        "",
        "Evidence boundary: identical-input arithmetic only. Not vendor validation, "
        "observed dealer positioning, or a trading edge. No canonical-engine choice.",
        "",
    ]
    if result["class_1_status"] == "INPUT_REJECTED":
        lines += [f"Rejected: {result.get('reason')}", ""]
    if packet:
        lines += [
            "## Packet",
            "",
            f"- packet_id `{result['packet_id']}`; packet sha256 `{result['packet_sha256']}`",
            f"- raw sha256 `{result['raw_sha256']}`; receipt sha256 `{result['receipt_sha256']}`",
            f"- valuation_at {packet['valuation_at']} ({packet['valuation_basis']})",
            f"- available_at {packet['available_at']} ({packet['availability_basis']}); "
            f"timing validation {result['timing_validation']['status']}",
            f"- spot {packet['spot']} (Cboe current_price); prior close derived "
            f"{packet['prior_close_derived']}; prev_day_close field {packet['prior_close_field']}",
            f"- calendar {packet['calendar_version']}",
            f"- common parameters r={packet['r']}, q={packet['q']}, exact fractional T (ACT/365 seconds)",
            "",
        ]
        ex = result["exclusion_summary"]
        lines += ["## Exclusion ledger (no post-hoc exclusion of failures)", ""]
        lines += [
            f"- {k}: {v['rows']} rows, OI {v['oi']:.0f} (+{v['oi_unknown']} unknown)"
            for k, v in sorted(ex["by_reason"].items())
        ]
        lines += [
            f"- included: {ex['included_rows']} rows, OI {ex['included_oi']:.0f}; "
            f"early-close candidates flagged (not modeled): {ex['early_close_candidate_rows']} "
            f"rows, OI {ex['early_close_candidate_oi']:.0f}",
            "",
        ]
        pc = result["per_contract"]
        lines += [
            "## Class 1: per contract and subtotals at the packet spot",
            "",
            f"Reference: 80/110-digit Decimal (P2-A). Float reference max relative "
            f"error {pc['reference_float_max_rel_error']:.2e} (certified "
            f"{pc['reference_float_certified']}).",
            "",
            "| engine | contracts | contract gamma | protocol tol | P2-A bound | expiry subtotal | strike subtotal | total | sign |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for name, e in pc["engines"].items():
            lines.append(
                f"| {name} | {pc['contracts']} | max rel err {e['max_rel_error']:.2e} | "
                f"{e['contract_status']} {e['counts']} | {e['bound_counts']} | "
                f"{e['subtotals']['expiry']['status']} | {e['subtotals']['strike']['status']} | "
                f"{e['total']['status']} (err ${e['total']['abs_error']:.4f}) | {e['total']['sign']} |"
            )
        cv = result["curves"]
        lines += [
            "",
            f"## Class 1: engine-level curve on the common grid ({len(cv['grid'])} points, "
            f"{cv['grid'][0]}..{cv['grid'][-1]})",
            "",
            f"Reference roots: {[round(x['root'], 3) for x in cv['reference']['roots']]}; "
            f"sign-indeterminate points: {len(cv['reference']['sign_indeterminate_points'])}",
            "",
            "| engine | curve | max abs err | max err/gross | roots | root set |",
            "|---|---|---|---|---|---|",
        ]
        for name, e in cv["engines"].items():
            if "values" not in e:
                lines.append(f"| {name} | {e['status']} | | | | {e.get('reason')} |")
                continue
            lines.append(
                f"| {name} | {e['curve_status']} | ${e['max_abs_error']:.4f} | "
                f"{e['max_error_over_gross']:.2e} | "
                f"{[round(x['root'], 3) for x in e['roots']]} | {e['root_status']} |"
            )
        w = result["walls"]
        lines += ["", "## Class 1: walls at the packet spot", ""]
        lines.append(f"- reference: {w['reference']}")
        for name, e in w["engines"].items():
            lines.append(f"- {name}: {e['status']} {e.get('walls', e.get('reason'))}")
        nat = result["native"]
        g = nat["grid"]
        lines += [
            "",
            "## Native behavior (own parameters; NOT_COMPARABLE across engines)",
            "",
            f"- GRID compute_gex_profile (r=.05, q=0, integer DTE, prior close {g.get('spot')}): "
            f"gex_aggregate {g.get('gex_aggregate')}, gamma_flip {g.get('gamma_flip')} "
            f"({g.get('gamma_flip_crossings')} crossings), call/put/gamma wall "
            f"{g.get('call_wall')}/{g.get('put_wall')}/{g.get('gamma_wall')}, regime {g.get('regime')}",
        ]
        gw = nat["gamma_watch"]
        if "variants" in gw:
            for v in gw["variants"]:
                lines.append(
                    f"- Gamma Watch curves ({v['name']}, IV shock {v['iv_shock']}): roots {v['roots']}, used {v['used']}"
                )
        else:
            lines.append(f"- Gamma Watch curves: {gw['status']} ({gw['reason']})")
        at = result["attribution"]
        lines += [
            "",
            "## One-factor attribution from the common baseline (reference evaluator)",
            "",
            f"Baseline G(spot) ${at['baseline']['G_at_eval_spot']:,.0f}; roots {at['baseline']['roots']}",
            "",
            "| factor | delta G | roots |",
            "|---|---|---|",
        ]
        for k, v in list(at["factors"].items()) + list(at["combined"].items()):
            extra = (
                f" (interaction residual ${v['interaction_residual']:,.0f})"
                if "interaction_residual" in v
                else ""
            )
            lines.append(f"| {k} | ${v['delta_G']:,.0f}{extra} | {v['roots']} |")
        lines += [
            f"| zero_dte | {at['zero_dte']['status']} | {at['zero_dte']['note']} |",
            f"| oi_vintage | {at['oi_vintage']['status']} | {at['oi_vintage']['note']} |",
            "",
            at["note"],
        ]
        vt = result["vendor"]
        lines += [
            "",
            "## Vendor comparability (classes 2 and 3 never pooled with class 1)",
            "",
            f"Class 2 eligible packets: {vt['class_2_eligible_packets']} -> "
            f"{vt['class_2_status']}. {vt['class_2_reason']}",
            "",
        ]
        for row in vt["rows"]:
            lines.append(
                f"- {row['vendor']}: class {row['class']} {row['status']} ({row['reason']})"
            )
            if row.get("contracts_with_provider_gamma"):
                lines.append(
                    f"  - diagnostic only: {row['contracts_with_provider_gamma']} contracts, "
                    f"rel diff median {row['rel_diff_median']:.2e}, p95 {row['rel_diff_p95']:.2e}"
                )
    lines += ["", "No engine edit, consumer rewire, activation or canonical choice follows from this report.", ""]
    return "\n".join(lines)


def build(raw: bytes, receipt_raw: bytes) -> dict[str, bytes]:
    """Every artifact, fully serialized, before anything touches the disk."""
    packet = None
    try:
        result, packet = reconcile(raw, receipt_raw)
    except (
        InputRejected,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        IndexError,
        ArithmeticError,
    ) as exc:
        result = {
            "schema": RESULT_SCHEMA,
            "class_1_status": "INPUT_REJECTED",
            "reason": f"{type(exc).__name__}: {exc}",
            "raw_sha256": digest(raw),
            "receipt_sha256": digest(receipt_raw),
            "vendor": {"class_2_eligible_packets": 0, "class_2_status": "NOT_SUPPORTED"},
        }
    files = {
        "input/cboe_raw.json": raw,
        "input/receipt.json": receipt_raw,
        "results.json": canonical(result) + b"\n",
        "packet.json": canonical(packet) + b"\n" if packet else b"null\n",
        "exclusions.json": canonical(packet["exclusions"] if packet else []) + b"\n",
        "REPORT.md": report(result, packet).encode("utf-8"),
    }
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "protocol": "docs/GAMMA-WATCH-P2-RECONCILIATION.md",
        "status_vocabulary": list(STATUSES),
        "class_1": {
            "eligible_packets": 1 if packet else 0,
            "status": result["class_1_status"],
        },
        "class_2": {
            "eligible_packets": 0,
            "status": "NOT_SUPPORTED",
            "reason": "no free matched vendor input exists",
        },
        "class_3": {"status": "NOT_COMPARABLE", "pooled_with_class_1": False},
        "packet_id": result.get("packet_id"),
        "packet_sha256": result.get("packet_sha256"),
        "files": {name: digest(data) for name, data in sorted(files.items())},
        "tolerances": TOLERANCES,
        "code_hashes": code_hashes(),
        "environment": environment(),
        "frozen_v1_paper_log": "not read, imported or written",
    }
    files["manifest.json"] = canonical(manifest) + b"\n"
    return files


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cboe", required=True, type=Path, help="raw Cboe JSON bytes")
    parser.add_argument("--receipt", required=True, type=Path, help="pull receipt JSON")
    parser.add_argument(
        "--output", required=True, type=Path, help="new directory; refuses overwrite"
    )
    args = parser.parse_args(argv)
    if args.output.exists():
        raise FileExistsError(str(args.output))
    files = build(args.cboe.read_bytes(), args.receipt.read_bytes())
    args.output.mkdir(parents=True, exist_ok=False)
    for name, data in files.items():
        target = args.output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "xb") as handle:
            handle.write(data)
    status = json.loads(files["results.json"])["class_1_status"]
    print(status)
    return 0 if status == "PASS_NUMERICAL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
