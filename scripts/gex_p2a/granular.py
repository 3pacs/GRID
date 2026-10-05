"""Offline granular GEX strike/expiry breakdown with hypothetical inventory scenarios.

Bounded offline extension of P2-A harness. No external network, live clocks, database,
or model fitting. Reuses canonical P2-A reconcile arithmetic verification.

Run via:
  python -m scripts.gex_p2a.granular <packet.json> --output <new-output-dir>
"""

import argparse
from datetime import date
from decimal import Decimal, localcontext
import json
import math
from pathlib import Path

from .harness import canonical, digest, finite, instant, packet, reconcile
from .reference import Ball


def _parse_unique_json(raw: bytes) -> dict:
    def _unique_pairs(pairs):
        res = {}
        for k, v in pairs:
            if k in res:
                raise ValueError(f"duplicate JSON key: {k}")
            res[k] = v
        return res

    return json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_pairs)


def _aggregate(contracts: list) -> dict:
    return {
        "call_oi_gross": math.fsum(
            c["oi_gross_usd_per_1pct"] for c in contracts if c["side"] == "call"
        ),
        "put_oi_gross": math.fsum(
            c["oi_gross_usd_per_1pct"] for c in contracts if c["side"] == "put"
        ),
        "oi_gross": math.fsum(c["oi_gross_usd_per_1pct"] for c in contracts),
        "call_inventory_gross": math.fsum(
            c["inventory_gross_usd_per_1pct"] for c in contracts if c["side"] == "call"
        ),
        "put_inventory_gross": math.fsum(
            c["inventory_gross_usd_per_1pct"] for c in contracts if c["side"] == "put"
        ),
        "inventory_gross": math.fsum(
            c["inventory_gross_usd_per_1pct"] for c in contracts
        ),
        "call_signed": math.fsum(
            c["signed_usd_per_1pct"] for c in contracts if c["side"] == "call"
        ),
        "put_signed": math.fsum(
            c["signed_usd_per_1pct"] for c in contracts if c["side"] == "put"
        ),
        "signed_net": math.fsum(c["signed_usd_per_1pct"] for c in contracts),
    }


def _format_diagnostic(b: Ball) -> dict:
    if b.lo > 0:
        sign = "POSITIVE"
    elif b.hi < 0:
        sign = "NEGATIVE"
    else:
        sign = "INDETERMINATE"
    return {
        "absolute_error_bound": str(b.e),
        "lower": str(b.lo),
        "upper": str(b.hi),
        "sign": sign,
    }


def _recenter_ball(b: Ball, actual_float: float) -> Ball:
    actual_f = float(actual_float)
    if not math.isfinite(actual_f):
        raise ArithmeticError(f"non-finite actual float: {actual_f}")
    actual_d = Decimal.from_float(actual_f)
    discrepancy = abs(b.v - actual_d)
    scale = max(abs(b.lo), abs(b.hi), abs(actual_d))
    from decimal import getcontext

    guard = Decimal(10) ** (-getcontext().prec + 3) * max(Decimal(1), scale)
    new_e = b.e + discrepancy + guard
    if not new_e.is_finite():
        raise ArithmeticError("non-finite error bound in recenter")
    return Ball(actual_d, new_e)


def _sum_balls(balls: list) -> Ball:
    if not balls:
        return Ball(Decimal(0), Decimal(0))
    res = balls[0]
    for b in balls[1:]:
        res = res + b
    return res


def _aggregate_bounds(grp_contracts: list, grp_balls: list) -> dict:
    agg_floats = _aggregate(grp_contracts)

    call_oi_balls = [b["oi_gross"] for b in grp_balls if b["side"] == "call"]
    call_oi_ball = _recenter_ball(
        _sum_balls(call_oi_balls), agg_floats["call_oi_gross"]
    )

    put_oi_balls = [b["oi_gross"] for b in grp_balls if b["side"] == "put"]
    put_oi_ball = _recenter_ball(_sum_balls(put_oi_balls), agg_floats["put_oi_gross"])

    all_oi_balls = [b["oi_gross"] for b in grp_balls]
    oi_ball = _recenter_ball(_sum_balls(all_oi_balls), agg_floats["oi_gross"])

    call_inv_balls = [b["inventory_gross"] for b in grp_balls if b["side"] == "call"]
    call_inv_ball = _recenter_ball(
        _sum_balls(call_inv_balls), agg_floats["call_inventory_gross"]
    )

    put_inv_balls = [b["inventory_gross"] for b in grp_balls if b["side"] == "put"]
    put_inv_ball = _recenter_ball(
        _sum_balls(put_inv_balls), agg_floats["put_inventory_gross"]
    )

    all_inv_balls = [b["inventory_gross"] for b in grp_balls]
    inv_ball = _recenter_ball(
        _sum_balls(all_inv_balls), agg_floats["inventory_gross"]
    )

    call_signed_balls = [b["signed_usd"] for b in grp_balls if b["side"] == "call"]
    call_signed_ball = _recenter_ball(
        _sum_balls(call_signed_balls), agg_floats["call_signed"]
    )

    put_signed_balls = [b["signed_usd"] for b in grp_balls if b["side"] == "put"]
    put_signed_ball = _recenter_ball(
        _sum_balls(put_signed_balls), agg_floats["put_signed"]
    )

    all_signed_balls = [b["signed_usd"] for b in grp_balls]
    signed_net_ball = _recenter_ball(
        _sum_balls(all_signed_balls), agg_floats["signed_net"]
    )

    return {
        "call_oi_gross": _format_diagnostic(call_oi_ball),
        "put_oi_gross": _format_diagnostic(put_oi_ball),
        "oi_gross": _format_diagnostic(oi_ball),
        "call_inventory_gross": _format_diagnostic(call_inv_ball),
        "put_inventory_gross": _format_diagnostic(put_inv_ball),
        "inventory_gross": _format_diagnostic(inv_ball),
        "call_signed": _format_diagnostic(call_signed_ball),
        "put_signed": _format_diagnostic(put_signed_ball),
        "signed_net": _format_diagnostic(signed_net_ball),
    }


def build(raw: bytes) -> dict:
    raw_hash = digest(raw)
    try:
        p = _parse_unique_json(raw)
        if not isinstance(p, dict):
            raise ValueError("root must be a JSON object")
    except Exception as exc:
        return {
            "schema": "gex-granular-v1",
            "status": "INPUT_REJECTED",
            "reason": f"malformed JSON: {exc}",
            "raw_sha256": raw_hash,
        }

    try:
        # Validate the complete canonical packet before any field-clock exclusion.
        # This also rejects duplicate identities and bad OI vintages in expired rows.
        _, normalized, _ = packet(raw)
        for label in ("source", "calendar_version"):
            if not isinstance(p[label], str) or not p[label].strip():
                raise ValueError(f"{label} must be a nonempty string")
        valuation = instant(normalized["valuation_at"])
        receipt = instant(normalized["available_at"])
        r_val, q_val, spots = normalized["r"], normalized["q"], normalized["spots"]

        granular = p.get("granular")
        if not isinstance(granular, dict):
            raise ValueError("missing granular object")
        if granular.get("version") != "gex-granular-input-v1":
            raise ValueError("unsupported granular version")

        spot_clocks = granular.get("spot_clocks")
        if not isinstance(spot_clocks, dict):
            raise ValueError("missing spot_clocks object")
        if (
            "received_at" not in spot_clocks
            or "source_at" not in spot_clocks
            or "source" not in spot_clocks
        ):
            raise ValueError(
                "spot clock received_at, source_at, and source keys required"
            )
        sc_source = spot_clocks.get("source")
        if not isinstance(sc_source, str) or not sc_source.strip():
            raise ValueError("spot clock source label must be non-empty string")
        sc_rec = spot_clocks.get("received_at")
        if sc_rec is None:
            raise ValueError("unknown spot receipt; cannot price exposure")
        if not isinstance(sc_rec, str):
            raise ValueError("spot received_at must be string")
        sc_rec_dt = instant(sc_rec)
        if sc_rec_dt > receipt or sc_rec_dt > valuation:
            raise ValueError("future spot receipt timestamp")
        sc_src = spot_clocks.get("source_at")
        if sc_src is not None:
            if not isinstance(sc_src, str):
                raise ValueError("spot source_at must be string or null")
            sc_src_dt = instant(sc_src)
            if sc_src_dt > sc_rec_dt or sc_src_dt > receipt or sc_src_dt > valuation:
                raise ValueError("future spot source timestamp")

        expected_ids = granular.get("expected_contract_ids")
        if not isinstance(expected_ids, list) or not expected_ids:
            raise ValueError("missing expected_contract_ids list")
        expected_set = set()
        for eid in expected_ids:
            if not isinstance(eid, str) or not eid:
                raise ValueError("expected_contract_id must be non-empty string")
            if eid in expected_set:
                raise ValueError(f"duplicate expected_contract_id: {eid}")
            expected_set.add(eid)

        user_scenarios = granular.get("scenarios", [])
        if not isinstance(user_scenarios, list):
            raise ValueError("scenarios must be a list")
        if len(user_scenarios) > 8:
            raise ValueError("exceeded maximum of 8 scenarios")

        seen_scenario_names = {"oi_sign_baseline"}
        validated_scenarios = []
        for sc in user_scenarios:
            if not isinstance(sc, dict):
                raise ValueError("scenario must be a dict")
            s_name = sc.get("name")
            if not isinstance(s_name, str) or not s_name:
                raise ValueError("invalid scenario name")
            if s_name in seen_scenario_names:
                raise ValueError(f"duplicate or reserved scenario name: {s_name}")
            seen_scenario_names.add(s_name)
            if sc.get("kind") != "hypothetical_signed_inventory":
                raise ValueError("scenario kind must be hypothetical_signed_inventory")
            fractions = sc.get("fractions")
            if not isinstance(fractions, dict):
                raise ValueError("scenario fractions must be a dict")
            if set(fractions.keys()) != expected_set:
                raise ValueError(
                    f"scenario '{s_name}' fractions must exactly map expected_contract_ids"
                )
            parsed_fractions = {}
            for cid in expected_ids:
                val = finite(fractions[cid])
                if val < -1.0 or val > 1.0:
                    raise ValueError(
                        f"fraction {val} for {cid} outside range [-1.0, 1.0]"
                    )
                parsed_fractions[cid] = val
            validated_scenarios.append(
                {
                    "name": s_name,
                    "kind": "hypothetical_signed_inventory",
                    "fractions": parsed_fractions,
                }
            )

        raw_rows = p.get("rows")
        if not isinstance(raw_rows, list):
            raise ValueError("rows must be a list")

        seen_row_ids = set()
        validated_rows = []

        for row in raw_rows:
            if not isinstance(row, dict):
                raise ValueError("row must be a dict")
            cid = row.get("contract_id")
            if not isinstance(cid, str) or not cid:
                raise ValueError("row contract_id must be non-empty string")
            if cid in seen_row_ids:
                raise ValueError(f"duplicate contract_id in rows: {cid}")
            seen_row_ids.add(cid)
            if cid not in expected_set:
                raise ValueError(
                    f"observed contract_id {cid} outside expected universe"
                )

            exp = instant(row["expiry"])

            clocks = row.get("clocks")
            if not isinstance(clocks, dict):
                raise ValueError(f"missing clocks dict for contract {cid}")

            row_clocks_parsed = {}
            for field_name in ("quote", "greek", "oi"):
                clk = clocks.get(field_name)
                if (
                    not isinstance(clk, dict)
                    or "received_at" not in clk
                    or "source_at" not in clk
                    or "source" not in clk
                ):
                    raise ValueError(
                        f"malformed clock structure for {field_name} in {cid}"
                    )

                c_src = clk["source"]
                if not isinstance(c_src, str) or not c_src.strip():
                    raise ValueError(
                        f"malformed or missing clock source for {field_name} in {cid}"
                    )

                r_at = clk["received_at"]
                r_dt = None
                if r_at is not None:
                    if not isinstance(r_at, str):
                        raise ValueError(
                            f"received_at must be string or null for {field_name} in {cid}"
                        )
                    r_dt = instant(r_at)

                s_at = clk["source_at"]
                s_dt = None
                if s_at is not None:
                    if not isinstance(s_at, str):
                        raise ValueError(
                            f"source_at must be string or null for {field_name} in {cid}"
                        )
                    s_dt = instant(s_at)

                row_clocks_parsed[field_name] = (r_dt, s_dt)

            oi_as_of = row.get("oi_as_of")
            if oi_as_of is not None:
                if not isinstance(oi_as_of, str):
                    raise ValueError(
                        f"oi_as_of must be ISO date string or null in row {cid}"
                    )
                oi_date = date.fromisoformat(oi_as_of)
                if oi_date > receipt.date():
                    raise ValueError(f"future OI vintage in row {cid}")
                oi_r_dt = row_clocks_parsed["oi"][0]
                if oi_r_dt is not None and oi_date > oi_r_dt.date():
                    raise ValueError(f"future OI vintage in row {cid}")

            validated_rows.append(
                {
                    "row": row,
                    "cid": cid,
                    "exp": exp,
                    "clocks_parsed": row_clocks_parsed,
                }
            )

        admitted_rows = []
        exclusions = []

        for v in validated_rows:
            row = v["row"]
            cid = v["cid"]
            exp = v["exp"]
            clocks_parsed = v["clocks_parsed"]

            if exp <= valuation:
                exclusions.append({"contract_id": cid, "reason": "EXPIRED"})
                continue

            row_excluded = False
            for field_name in ("quote", "greek", "oi"):
                r_dt, s_dt = clocks_parsed[field_name]
                if r_dt is None:
                    exclusions.append(
                        {
                            "contract_id": cid,
                            "reason": f"MISSING_{field_name.upper()}_RECEIPT",
                        }
                    )
                    row_excluded = True
                    break
                if r_dt > receipt or r_dt > valuation:
                    exclusions.append(
                        {
                            "contract_id": cid,
                            "reason": f"FUTURE_{field_name.upper()}_RECEIPT",
                        }
                    )
                    row_excluded = True
                    break
                if s_dt is not None:
                    if s_dt > r_dt or s_dt > receipt or s_dt > valuation:
                        exclusions.append(
                            {
                                "contract_id": cid,
                                "reason": f"FUTURE_{field_name.upper()}_SOURCE",
                            }
                        )
                        row_excluded = True
                        break

            if row_excluded:
                continue

            admitted_rows.append(row)

        for cid in expected_ids:
            if cid not in seen_row_ids:
                exclusions.append({"contract_id": cid, "reason": "MISSING_CONTRACT"})

        exclusions.sort(key=lambda x: x["contract_id"])

        all_scenarios = [
            {
                "name": "oi_sign_baseline",
                "kind": "assumed_oi_sign_baseline",
                "fractions": {
                    r["contract_id"]: (1.0 if r["side"] == "call" else -1.0)
                    for r in admitted_rows
                },
            },
            *validated_scenarios,
        ]

        canonical_params = {
            "packet_id": p["packet_id"],
            "source": p["source"],
            "basis": p["basis"],
            "calendar_version": p["calendar_version"],
            "availability_basis": p["availability_basis"],
            "available_at": p["available_at"],
            "valuation_at": p["valuation_at"],
            "r": r_val,
            "q": q_val,
            "spots": spots,
            "spot_clocks": spot_clocks,
            "expected_contract_ids": sorted(expected_ids),
            "all_rows": sorted(
                [
                    {
                        "contract_id": r["contract_id"],
                        "underlying": r["underlying"],
                        "expiry": instant(r["expiry"]).isoformat(),
                        "strike": float(r["strike"]),
                        "side": r["side"],
                        "iv": float(r["iv"]),
                        "iv_origin": r["iv_origin"],
                        "oi": int(r["oi"]),
                        "oi_as_of": r.get("oi_as_of"),
                        "multiplier": float(r["multiplier"]),
                        "deliverable": r["deliverable"],
                        "clocks": r["clocks"],
                        **{k: r[k] for k in ("bid", "ask", "provider_gamma") if k in r},
                    }
                    for r in raw_rows
                ],
                key=lambda x: x["contract_id"],
            ),
            "exclusions": exclusions,
            "scenarios": sorted(
                [
                    {
                        "name": sc["name"],
                        "kind": sc["kind"],
                        "fractions": {
                            cid: sc["fractions"][cid] for cid in sorted(sc["fractions"])
                        },
                    }
                    for sc in all_scenarios
                ],
                key=lambda x: x["name"],
            ),
            "units": "USD_per_1pct_underlying_move",
            "scope": "declared_fixture_universe_only",
        }
        canonical_params_hash = digest(canonical(canonical_params))

        is_synthetic = "synthetic" in str(p.get("source", "")).lower()
        disclaimers = []
        if is_synthetic:
            disclaimers.append(
                "Synthetic fixture; not market data; no ground truth observed."
            )
        disclaimers.extend(
            [
                "Inventory fractions are hypothetical assumptions, not observed dealer positions.",
                "Net signed exposure is an arithmetic sensitivity envelope, not a directional signal or confidence interval.",
                "European Black-Scholes approximation for SPY American options; dividend/early-exercise effects unmodeled.",
                "0DTE defined strictly by calendar UTC date matching valuation UTC date; no exchange-local trading session claim.",
                "Conditional arithmetic error bounds assume binary64 round-to-nearest, transcendental error <=2 ulp, gradual underflow, no overflow, and fixed hypothetical inventory fractions; unmodeled American/dividend and market effects are excluded.",
            ]
        )

        if not admitted_rows:
            return {
                "schema": "gex-granular-v1",
                "status": "INSUFFICIENT_DATA",
                "numerical_status": "INSUFFICIENT_DATA",
                "coverage_status": "INSUFFICIENT_DATA",
                "inventory_status": "HYPOTHETICAL_UNOBSERVED",
                "units": "USD_per_1pct_underlying_move",
                "scope": "declared_fixture_universe_only",
                "arithmetic_scope": "conditional_numerical_bounds_under_fixed_hypothetical_fractions",
                "bound_assumption": "binary64 round-to-nearest; exp/log/sqrt <=2 ulp; gradual underflow; no overflow; fixed hypothetical inputs; European model only",
                "full_market_coverage": None,
                "source_authentication": "SYNTHETIC"
                if is_synthetic
                else "NOT_VERIFIED",
                "raw_sha256": raw_hash,
                "code_hashes": {"granular": digest(Path(__file__).read_bytes())},
                "canonical_parameters_sha256": canonical_params_hash,
                "input_metadata": {
                    "packet_id": p["packet_id"],
                    "source": p["source"],
                    "basis": p["basis"],
                    "calendar_version": p["calendar_version"],
                    "availability_basis": p["availability_basis"],
                    "available_at": p["available_at"],
                    "valuation_at": p["valuation_at"],
                    "r": r_val,
                    "q": q_val,
                    "spots": spots,
                    "spot_clocks": spot_clocks,
                    "rows": raw_rows,
                },
                "coverage": {
                    "scope": "declared_fixture_universe_only",
                    "full_market_coverage": None,
                    "expected_count": len(expected_ids),
                    "observed_count": len(raw_rows),
                    "admitted_count": 0,
                    "excluded_count": len(exclusions),
                    "missing_count": len(
                        [e for e in exclusions if e["reason"] == "MISSING_CONTRACT"]
                    ),
                    "exclusions": exclusions,
                },
                "disclaimers": disclaimers,
                "scenarios": [],
                "p2a_reconcile": None,
            }

        filtered_packet = {
            "schema": p["schema"],
            "packet_id": p["packet_id"],
            "source": p["source"],
            "basis": p["basis"],
            "calendar_version": p["calendar_version"],
            "availability_basis": p["availability_basis"],
            "available_at": p["available_at"],
            "valuation_at": p["valuation_at"],
            "r": p["r"],
            "q": p["q"],
            "spots": p["spots"],
            "rows": [
                {
                    "underlying": r["underlying"],
                    "expiry": r["expiry"],
                    "strike": r["strike"],
                    "side": r["side"],
                    "iv": r["iv"],
                    "iv_origin": r["iv_origin"],
                    "oi": r["oi"],
                    "oi_as_of": r.get("oi_as_of"),
                    "multiplier": r["multiplier"],
                    "deliverable": r["deliverable"],
                }
                for r in admitted_rows
            ],
        }
        filtered_raw = canonical(filtered_packet)
        p2a_result = reconcile(filtered_raw)

        grid_engine = next(
            (e for e in p2a_result["engines"] if e["name"] == "grid_primitive"), None
        )
        if not grid_engine:
            raise ValueError("reconcile grid_primitive engine output missing")

        engine_point_map = {pt["spot"]: pt for pt in grid_engine["points"]}
        spot_gamma_maps = {}
        for s in spots:
            pt = engine_point_map.get(s)
            if pt and pt.get("status") == "PASS_NUMERICAL":
                pt_contracts = pt.get("contracts")
                if not isinstance(pt_contracts, list):
                    raise ValueError(f"malformed contracts in reconcile point for spot {s}")
                c_map = {}
                for c in pt_contracts:
                    if not isinstance(c, dict):
                        raise ValueError(f"malformed contract check in reconcile for spot {s}")
                    if c.get("status") != "PASS_NUMERICAL":
                        raise ValueError(f"non-pass contract check in PASS point: {c.get('identity')}")
                    eb_raw = c.get("error_bound")
                    if eb_raw is None or isinstance(eb_raw, bool):
                        raise ValueError(f"missing error_bound for contract {c.get('identity')}")
                    try:
                        eb_dec = Decimal(str(eb_raw))
                    except Exception as exc:
                        raise ValueError(f"invalid error_bound decimal '{eb_raw}': {exc}")
                    if not eb_dec.is_finite() or eb_dec < 0:
                        raise ValueError(f"non-finite or negative error_bound: {eb_dec}")
                    actual_g = c.get("actual")
                    if actual_g is None or isinstance(actual_g, bool) or not isinstance(actual_g, (int, float)):
                        raise ValueError(f"missing or non-numeric actual gamma: {actual_g}")
                    actual_g = float(actual_g)
                    if not math.isfinite(actual_g) or actual_g < 0:
                        raise ValueError(f"non-finite or negative actual gamma: {actual_g}")
                    c_map[tuple(c["identity"])] = (actual_g, eb_dec)
                spot_gamma_maps[s] = c_map
            else:
                spot_gamma_maps[s] = None

        sorted_admitted_rows = sorted(
            admitted_rows,
            key=lambda r: (
                instant(r["expiry"]).isoformat(),
                float(r["strike"]),
                r["side"],
                float(r["multiplier"]),
                r["contract_id"],
            ),
        )

        with localcontext() as ctx:
            ctx.prec = 80
            scenario_outputs = []
            for sc in all_scenarios:
                sc_spots = []
                for s in spots:
                    pt = engine_point_map.get(s)
                    gamma_map = spot_gamma_maps.get(s)

                    if not pt or pt.get("status") != "PASS_NUMERICAL" or gamma_map is None:
                        sc_spots.append(
                            {
                                "spot": s,
                                "status": pt.get("status", "NOT_SUPPORTED")
                                if pt
                                else "NOT_SUPPORTED",
                                "reason": pt.get("reason")
                                if pt
                                else "engine evaluation unavailable",
                                "aggregates": None,
                                "arithmetic_bounds": None,
                                "contracts": None,
                            }
                        )
                        continue

                    contracts = []
                    contract_balls = []
                    expiry_groups = {}
                    expiry_group_balls = {}
                    strike_groups = {}
                    strike_group_balls = {}

                    s_ball = Ball(s)
                    s2_ball = s_ball * s_ball

                    for r in sorted_admitted_rows:
                        cid = r["contract_id"]
                        exp_iso = instant(r["expiry"]).isoformat()
                        k = float(r["strike"])
                        mult = float(r["multiplier"])
                        oi = float(r["oi"])
                        row_key = (r["underlying"], exp_iso, k, r["side"], mult)
                        if row_key not in gamma_map:
                            raise ValueError(f"contract {row_key} missing from gamma map")
                        g, eb_dec = gamma_map[row_key]
                        frac = sc["fractions"][cid]
                        oi_gross = g * oi * mult * (s**2) * 0.01
                        inv_gross = oi_gross * abs(frac)
                        signed_usd = oi_gross * frac

                        if (
                            not math.isfinite(oi_gross)
                            or not math.isfinite(inv_gross)
                            or not math.isfinite(signed_usd)
                        ):
                            raise ArithmeticError(f"non-finite exposure for {cid}")

                        gamma_ball = Ball(g, eb_dec)
                        term_ball = gamma_ball * oi * mult * s2_ball * 0.01
                        oi_gross_ball = _recenter_ball(term_ball, oi_gross)

                        inv_raw = oi_gross_ball * abs(frac)
                        inv_gross_ball = _recenter_ball(inv_raw, inv_gross)

                        signed_raw = oi_gross_ball * frac
                        signed_usd_ball = _recenter_ball(signed_raw, signed_usd)

                        c_bounds_dict = {
                            "oi_gross_usd_per_1pct": _format_diagnostic(oi_gross_ball),
                            "inventory_gross_usd_per_1pct": _format_diagnostic(inv_gross_ball),
                            "signed_usd_per_1pct": _format_diagnostic(signed_usd_ball),
                        }

                        c_dict = {
                            "contract_id": cid,
                            "expiry": exp_iso,
                            "strike": k,
                            "side": r["side"],
                            "iv": float(r["iv"]),
                            "iv_origin": r.get("iv_origin", "direct"),
                            "oi_as_of": r.get("oi_as_of"),
                            "gamma": g,
                            "oi": oi,
                            "multiplier": mult,
                            "assumed_dealer_fraction": frac,
                            "signed_position_contracts": oi * frac,
                            "oi_gross_usd_per_1pct": oi_gross,
                            "inventory_gross_usd_per_1pct": inv_gross,
                            "signed_usd_per_1pct": signed_usd,
                            "arithmetic_bounds": c_bounds_dict,
                            "clocks": r["clocks"],
                            "unknown": {
                                "oi_as_of": r.get("oi_as_of") is None,
                                "quote_source_at": r["clocks"]["quote"].get("source_at")
                                is None,
                                "greek_source_at": r["clocks"]["greek"].get("source_at")
                                is None,
                                "oi_source_at": r["clocks"]["oi"].get("source_at") is None,
                            },
                        }
                        if "bid" in r:
                            c_dict["bid"] = r["bid"]
                        if "ask" in r:
                            c_dict["ask"] = r["ask"]
                        if "provider_gamma" in r:
                            c_dict["provider_gamma"] = r["provider_gamma"]
                            c_dict["provider_gamma_provenance"] = "NOT_FRESH"

                        c_ball_item = {
                            "oi_gross": oi_gross_ball,
                            "inventory_gross": inv_gross_ball,
                            "signed_usd": signed_usd_ball,
                            "side": r["side"],
                        }

                        contracts.append(c_dict)
                        contract_balls.append(c_ball_item)
                        expiry_groups.setdefault(exp_iso, []).append(c_dict)
                        expiry_group_balls.setdefault(exp_iso, []).append(c_ball_item)
                        strike_groups.setdefault((exp_iso, k), []).append(c_dict)
                        strike_group_balls.setdefault((exp_iso, k), []).append(c_ball_item)

                    by_expiry = []
                    by_expiry_bounds = []
                    for exp in sorted(expiry_groups.keys()):
                        grp = expiry_groups[exp]
                        grp_b = expiry_group_balls[exp]
                        by_expiry.append(
                            {
                                "expiry": exp,
                                "is_0dte": instant(exp).date() == valuation.date(),
                                **_aggregate(grp),
                            }
                        )
                        by_expiry_bounds.append(
                            {
                                "expiry": exp,
                                **_aggregate_bounds(grp, grp_b),
                            }
                        )

                    by_strike = []
                    by_strike_bounds = []
                    for exp, k in sorted(strike_groups.keys()):
                        grp = strike_groups[(exp, k)]
                        grp_b = strike_group_balls[(exp, k)]
                        by_strike.append(
                            {
                                "expiry": exp,
                                "strike": k,
                                **_aggregate(grp),
                            }
                        )
                        by_strike_bounds.append(
                            {
                                "expiry": exp,
                                "strike": k,
                                **_aggregate_bounds(grp, grp_b),
                            }
                        )

                    bounds_total = _aggregate_bounds(contracts, contract_balls)

                    sc_spots.append(
                        {
                            "spot": s,
                            "status": "PASS_NUMERICAL",
                            "aggregates": {
                                "total": _aggregate(contracts),
                                "by_expiry": by_expiry,
                                "by_strike_within_expiry": by_strike,
                            },
                            "arithmetic_bounds": {
                                "total": bounds_total,
                                "by_expiry": by_expiry_bounds,
                                "by_strike_within_expiry": by_strike_bounds,
                            },
                            "contracts": contracts,
                        }
                    )

                scenario_outputs.append(
                    {
                        "name": sc["name"],
                        "kind": sc["kind"],
                        "spots": sc_spots,
                    }
                )

        code_hashes = dict(p2a_result.get("code_hashes", {}))
        code_hashes["granular"] = digest(Path(__file__).read_bytes())

        result = {
            "schema": "gex-granular-v1",
            "status": p2a_result["status"],
            "numerical_status": p2a_result["status"],
            "coverage_status": "COMPLETE_DECLARED_COVERAGE"
            if not exclusions
            else "PARTIAL_DECLARED_COVERAGE",
            "inventory_status": "HYPOTHETICAL_UNOBSERVED",
            "units": "USD_per_1pct_underlying_move",
            "scope": "declared_fixture_universe_only",
            "arithmetic_scope": "conditional_numerical_bounds_under_fixed_hypothetical_fractions",
            "bound_assumption": "binary64 round-to-nearest; exp/log/sqrt <=2 ulp; gradual underflow; no overflow; fixed hypothetical inputs; European model only",
            "full_market_coverage": None,
            "source_authentication": "SYNTHETIC" if is_synthetic else "NOT_VERIFIED",
            "raw_sha256": raw_hash,
            "canonical_parameters_sha256": canonical_params_hash,
            "code_hashes": code_hashes,
            "input_metadata": {
                "packet_id": p["packet_id"],
                "source": p["source"],
                "basis": p["basis"],
                "calendar_version": p["calendar_version"],
                "availability_basis": p["availability_basis"],
                "available_at": p["available_at"],
                "valuation_at": p["valuation_at"],
                "r": r_val,
                "q": q_val,
                "spots": spots,
                "spot_clocks": spot_clocks,
                "rows": raw_rows,
            },
            "coverage": {
                "scope": "declared_fixture_universe_only",
                "full_market_coverage": None,
                "expected_count": len(expected_ids),
                "observed_count": len(raw_rows),
                "admitted_count": len(admitted_rows),
                "excluded_count": len(exclusions),
                "missing_count": len(
                    [e for e in exclusions if e["reason"] == "MISSING_CONTRACT"]
                ),
                "exclusions": exclusions,
            },
            "disclaimers": disclaimers,
            "scenarios": scenario_outputs,
            "p2a_reconcile": p2a_result,
        }

        canonical(result)
        return result

    except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError) as exc:
        return {
            "schema": "gex-granular-v1",
            "status": "INPUT_REJECTED",
            "reason": str(exc),
            "raw_sha256": raw_hash,
        }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path, help="Path to input packet JSON file")
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="New target output directory; refuses overwrite",
    )
    args = parser.parse_args(argv)

    raw = args.packet.read_bytes()
    try:
        result = build(raw)
        result_bytes = canonical(result) + b"\n"
    except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError) as exc:
        result = {
            "schema": "gex-granular-v1",
            "status": "INPUT_REJECTED",
            "reason": str(exc),
            "raw_sha256": digest(raw),
        }
        result_bytes = canonical(result) + b"\n"

    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "input.json").write_bytes(raw)
    (args.output / "result.json").write_bytes(result_bytes)
    print(result.get("status", "PASS"))
    return 0 if result.get("status") == "PASS_NUMERICAL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
