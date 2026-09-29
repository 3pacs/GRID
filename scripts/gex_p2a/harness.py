"""Offline P2-A packets/reference comparison. Run via python -m scripts.gex_p2a.harness.

Only explicitly named local files are read; output directories are create-only.
No collector import, database, broker, network, timer or runtime configuration.
"""

import argparse
import ast
import copy
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sys
from uuid import UUID

import numpy as np

from .reference import Ball, BoundMath, U, dec, gamma, reference_bound

ROOT = Path(__file__).resolve().parents[2]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(data):
    return json.dumps(
        data, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def instant(text):
    d = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if d.tzinfo is None:
        raise ValueError("naive timestamp")
    return d.astimezone(timezone.utc)


def finite(value, positive=False, nonnegative=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("numeric field must be a JSON number, not bool/string")
    value = float(value)
    if (
        not math.isfinite(value)
        or (positive and value <= 0)
        or (nonnegative and value < 0)
    ):
        raise ValueError("invalid numeric input")
    return value


def packet(raw):
    """Normalize once; every adapter receives these exact rows/parameters."""

    def unique(pairs):
        result = {}
        for k, v in pairs:
            if k in result:
                raise ValueError("duplicate JSON key")
            result[k] = v
        return result

    p = json.loads(raw, object_pairs_hook=unique)
    if p["schema"] != "gex-p2a-v1" or p["basis"] != "unadjusted":
        raise ValueError("unsupported packet schema/price basis")
    if p["availability_basis"] != "grid_svr_pull_receipt":
        raise ValueError("ANIK clocks are not trusted availability")
    packet_id = str(UUID(p["packet_id"]))
    receipt, valuation = instant(p["available_at"]), instant(p["valuation_at"])
    if receipt > valuation:
        raise ValueError("input unavailable at valuation")
    if not p.get("source") or not p.get("calendar_version"):
        raise ValueError("source and expiry calendar provenance required")
    r, q = finite(p["r"]), finite(p["q"])
    spots = [finite(x, positive=True) for x in p["spots"]]
    if not spots or len(spots) != len(set(spots)):
        raise ValueError("empty/duplicate spot grid")
    rows, excluded, seen = [], [], set()
    for row in p["rows"]:
        expiry = instant(row["expiry"])
        k, iv = finite(row["strike"], positive=True), finite(row["iv"], positive=True)
        oi, mult = (
            finite(row["oi"], nonnegative=True),
            finite(row["multiplier"], positive=True),
        )
        if (
            oi != int(oi)
            or row["side"] not in ("call", "put")
            or row["underlying"] != "SPY"
        ):
            raise ValueError("unsupported contract identity/OI")
        if row.get("deliverable") != "standard_shares" or "oi_as_of" not in row:
            raise ValueError("explicit deliverable and OI date/unknown required")
        if (
            row["oi_as_of"] is not None
            and date.fromisoformat(row["oi_as_of"]) > receipt.date()
        ):
            raise ValueError("future OI vintage")
        if row.get("iv_origin") != "direct":
            raise ValueError("P2-A direct-IV baseline only")
        key = (row["underlying"], expiry.isoformat(), k, row["side"], mult)
        if key in seen:
            raise ValueError("duplicate contract identity")
        seen.add(key)
        if expiry <= valuation:
            excluded.append({"identity": key, "reason": "EXPIRED"})
            continue
        t = (expiry - valuation).total_seconds() / (365 * 86400)
        rows.append(
            {
                "identity": key,
                "strike": k,
                "iv": iv,
                "oi": oi,
                "oi_as_of": row["oi_as_of"],
                "iv_origin": row["iv_origin"],
                "multiplier": mult,
                "T": t,
                "sign": 1 if row["side"] == "call" else -1,
            }
        )
    rows.sort(key=lambda x: x["identity"])
    normalized = {
        "packet_id": packet_id,
        "r": r,
        "q": q,
        "spots": spots,
        "rows": rows,
        "valuation_at": valuation.isoformat(),
        "available_at": receipt.isoformat(),
        "source": p["source"],
        "calendar_version": p["calendar_version"],
        "exclusions": excluded,
    }
    return p, normalized, digest(canonical(normalized))


def load_primitive():
    path = ROOT / "physics/greeks/black_scholes.py"
    spec = importlib.util.spec_from_file_location("gex_offline_primitive", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, digest(path.read_bytes())


def watch_kernel():
    """Extract actual arithmetic AST, not a hand-transcribed comparison clone.

    Only d1/gamma assignments from curves are evaluated. No module execution.
    Explicit test-only transformation: hard-coded r/q become packet parameters.
    """
    path = ROOT / "collectors/gamma_watch/broker.py"
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "curves"
    )
    selected = [
        n
        for n in ast.walk(fn)
        if isinstance(n, ast.Assign)
        and len(n.targets) == 1
        and isinstance(n.targets[0], ast.Name)
        and n.targets[0].id in ("d1", "gamma")
    ]
    fingerprint = digest(
        ast.dump(
            ast.Module(body=selected, type_ignores=[]), include_attributes=False
        ).encode()
    )
    if (
        fingerprint
        != "0e21de0981b0c85222af54430ca8fc436877b24cbc16338e0bba861850b8b0d9"
    ):
        raise ValueError("collector kernel structure changed; review adapter")
    assignments = {n.targets[0].id: n for n in selected}

    class Parameters(ast.NodeTransformer):
        def visit_Constant(self, node):
            if isinstance(node.value, float) and node.value in (0.04, 0.012):
                return ast.copy_location(
                    ast.Name(id="r" if node.value == 0.04 else "q", ctx=ast.Load()),
                    node,
                )
            return node

    module = ast.Module(
        body=[
            Parameters().visit(copy.deepcopy(assignments[n])) for n in ("d1", "gamma")
        ],
        type_ignores=[],
    )
    compiled = compile(ast.fix_missing_locations(module), str(path), "exec")

    def calculate(s, k, t, r, q, iv, bounded=False):
        values = (s, k, t, r, q, iv)
        env = dict(
            zip(
                ("S", "K", "T", "r", "q", "iv"),
                map(Ball, values) if bounded else values,
            )
        )
        env["np"] = BoundMath if bounded else np
        exec(compiled, {"__builtins__": {}}, env)
        return env["gamma"]

    return calculate, digest(path.read_bytes())


def assess(actual, ref, bound):
    error = abs(dec(actual) - ref)
    return {
        "actual": float(actual),
        "reference": str(ref),
        "absolute_error": str(error),
        "error_bound": str(bound),
        "status": "PASS_NUMERICAL" if error <= bound else "FAIL_NUMERICAL",
    }


def reconcile(raw):
    source, p, packet_hash = packet(raw)
    primitive, primitive_hash = load_primitive()
    watch, watch_hash = watch_kernel()
    output = {
        "schema": "gex-p2a-result-v1",
        "packet_sha256": packet_hash,
        "raw_sha256": digest(raw),
        "normalized": p,
        "source_metadata": source,
        "engines": [],
        "code_hashes": {
            "grid_primitive": primitive_hash,
            "gamma_watch": watch_hash,
            "harness": digest(Path(__file__).read_bytes()),
            "reference": digest((Path(__file__).parent / "reference.py").read_bytes()),
        },
        "environment": {
            "python": sys.version,
            "numpy": np.__version__,
            "platform": sys.platform,
            "float_mantissa_bits": sys.float_info.mant_dig,
        },
        "scope": "synthetic/offline arithmetic only; no native loader, walls, roots or inventory validation",
        "vendor_status": "NOT_COMPARABLE",
        "bound_assumption": "binary64 round-to-nearest; exp/log/sqrt <=2 ulp; gradual underflow; no overflow",
        "watch_transformation": "AST d1/gamma only; .04/.012 replaced by packet r/q; no IV recovery/shock",
        "grid_production_status": "NOT_SUPPORTED: loader/prior-close path not exercised",
    }
    if not p["rows"]:
        output["status"] = "INPUT_REJECTED"
        return output
    with localcontext() as ctx:
        ctx.prec = 80
        for name in ("grid_primitive", "gamma_watch_kernel"):
            engine = {"name": name, "packet_sha256": packet_hash, "points": []}
            for s in p["spots"]:
                terms, refs, errors, checks = [], [], [], []
                unsupported = (
                    any(row["T"] < primitive.T_MIN for row in p["rows"])
                    if name == "grid_primitive"
                    else False
                )
                if unsupported:
                    engine["points"].append(
                        {
                            "spot": s,
                            "status": "NOT_SUPPORTED",
                            "reason": "GRID T floor differs from exact T",
                        }
                    )
                    continue
                for row in p["rows"]:
                    args = (s, row["strike"], row["T"], p["r"], p["q"], row["iv"])
                    ref = gamma(*args, precision=80)
                    converged = gamma(*args, precision=110)
                    if name == "grid_primitive":
                        actual = float(
                            primitive.gamma(
                                s, row["strike"], row["T"], p["r"], row["iv"], q=p["q"]
                            )
                        )
                        ball = reference_bound(*args)
                    else:
                        actual = float(watch(*args))
                        ball = watch(*args, bounded=True)
                    if not math.isfinite(actual):
                        raise ValueError("nonfinite engine output")
                    # Do not absorb a wrong formula into a larger tolerance.
                    bound = ball.e + abs(ref - converged)
                    checks.append(
                        {"identity": row["identity"], **assess(actual, ref, bound)}
                    )
                    # Same explicit native unit normalization, separately bounded.
                    exact_factor = (
                        dec(row["oi"])
                        * dec(row["multiplier"])
                        * dec(s)
                        * dec(s)
                        * dec(0.01)
                        * dec(row["sign"])
                    )
                    contribution = (
                        actual
                        * row["oi"]
                        * row["multiplier"]
                        * s
                        * s
                        * 0.01
                        * row["sign"]
                    )
                    term_ball = (
                        Ball(ref, bound)
                        * row["oi"]
                        * row["multiplier"]
                        * s
                        * s
                        * 0.01
                        * row["sign"]
                    )
                    refs.append(ref * exact_factor)
                    terms.append(contribution)
                    errors.append(term_ball.e + abs(term_ball.v - refs[-1]))
                reference_sum = sum(refs, Decimal(0))
                # Sequential summation: gamma_(n-1) * sum absolute perturbed terms.
                n = max(0, len(terms) - 1)
                reduction = (
                    (n * U)
                    / (1 - n * U)
                    * sum((abs(x) + e for x, e in zip(refs, errors)), Decimal(0))
                )
                bound = sum(errors, Decimal(0)) + reduction
                result = assess(sum(terms), reference_sum, bound)
                result.update(
                    spot=s,
                    contracts=checks,
                    gross_gex=str(sum(map(abs, refs))),
                    reduction_bound=str(reduction),
                    sign="INDETERMINATE"
                    if abs(reference_sum) <= bound
                    else ("POSITIVE" if reference_sum > 0 else "NEGATIVE"),
                )
                if any(c["status"] != "PASS_NUMERICAL" for c in checks):
                    result["status"] = "FAIL_NUMERICAL"
                engine["points"].append(result)
            output["engines"].append(engine)
    statuses = {p["status"] for e in output["engines"] for p in e["points"]}
    output["status"] = (
        "FAIL_NUMERICAL"
        if "FAIL_NUMERICAL" in statuses
        else ("NOT_SUPPORTED" if "NOT_SUPPORTED" in statuses else "PASS_NUMERICAL")
    )
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path)
    parser.add_argument(
        "--output", required=True, type=Path, help="new directory; refuses overwrite"
    )
    args = parser.parse_args(argv)
    raw = args.packet.read_bytes()
    try:
        result = reconcile(raw)
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        result = {
            "status": "INPUT_REJECTED",
            "reason": str(exc),
            "raw_sha256": digest(raw),
        }
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "input.json").write_bytes(raw)
    (args.output / "result.json").write_bytes(canonical(result) + b"\n")
    print(result["status"])
    return 0 if result["status"] == "PASS_NUMERICAL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
