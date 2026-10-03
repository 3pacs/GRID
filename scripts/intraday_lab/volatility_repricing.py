"""Offline matched-contract repricing diagnostics, never dealer inventory.

Input rows use the intraday_lab trusted-receipt contract, instrument OPTION,
with values containing contract_id, underlying, expiry_at (UTC epoch), right,
strike, multiplier, bid/ask, bid_size/ask_size, spot, iv (decimal), rate, yield.
Quotes and spot/IV must be a synchronized snapshot, not last-sale prices.
The European continuous-yield proxy is approximate for American ETF options.
No historical observation is admitted by manufacturing its available_at.
"""
from __future__ import annotations

import math

YEAR_SECONDS = 365.25 * 86400


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def proxy_price(spot, strike, years, iv, rate, dividend_yield, right):
    """European sensitivity proxy; caller supplies synchronized measured inputs."""
    if min(spot, strike, years, iv) <= 0 or right not in ("C", "P"):
        raise ValueError("positive spot/strike/time/iv and C/P required")
    root = iv * math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate - dividend_yield + iv * iv / 2) * years) / root
    d2 = d1 - root
    norm = lambda x: (1 + math.erf(x / math.sqrt(2))) / 2
    a, b = spot * math.exp(-dividend_yield * years), strike * math.exp(-rate * years)
    return a * norm(d1) - b * norm(d2) if right == "C" else b * norm(-d2) - a * norm(-d1)


def compute_repricing(packet, *, max_age=10, max_window=300):
    """Return per-contract measurements and unavailable reasons, no directional vote.

    Changes are model-decomposed in the fixed order spot -> time -> IV -> carry.
    Order matters for nonlinear sensitivities. Quote half-spreads are retained
    as a conservative uncertainty envelope, not executable midpoint profits.
    """
    t = packet.get("decision_at")
    if not finite(t) or not packet.get("session"):
        raise ValueError("trusted decision_at and exchange session required")
    groups = {}
    for row in packet.get("observations", []):
        if row.get("instrument") == "OPTION":
            key = (row.get("values") or {}).get("contract_id")
            groups.setdefault(key if isinstance(key, str) and key else "missing_identity", []).append(row)
    output = {}
    identity_fields = ("contract_id", "underlying", "expiry_at", "right", "strike", "multiplier")
    numeric_fields = ("expiry_at", "strike", "multiplier", "bid", "ask", "bid_size", "ask_size", "spot", "iv", "rate", "yield")
    for key, records in groups.items():
        def reject(reason):
            output[key] = {"status": "unavailable", "value": None, "reason": reason}
        if key == "missing_identity":
            reject("missing contract identity")
            continue
        records.sort(key=lambda r: r.get("event_at") if finite(r.get("event_at")) else -math.inf)
        good = True
        for r in records:
            v, event, receipt = r.get("values") or {}, r.get("event_at"), r.get("available_at")
            if not (r.get("source_id") and r.get("clock") == "trusted_receipt" and r.get("status") == "available"
                    and finite(event) and finite(receipt) and t - max_window <= event <= receipt <= t
                    and all(finite(v.get(f)) for f in numeric_fields)
                    and v.get("right") in ("C", "P") and isinstance(v.get("underlying"), str) and v["underlying"]
                    and v.get("synchronized") is True and v.get("iv_basis") == "provider"
                    and 0 <= v["bid"] < v["ask"] and min(v["bid_size"], v["ask_size"], v["spot"], v["strike"], v["multiplier"], v["iv"]) > 0
                    and v["expiry_at"] > event):
                good = False
        if not good:
            reject("invalid, unavailable, future or unsynchronized snapshot")
            continue
        if (len(records) < 2 or t - records[-1]["event_at"] > max_age
                or len({r["event_at"] for r in records}) != len(records)
                or len({r["source_id"] for r in records}) != 1
                or len({tuple(r["values"][f] for f in identity_fields) for r in records}) != 1):
            reject("insufficient, stale, duplicate or mismatched contract/source")
            continue
        first, last = records[0], records[-1]
        a, b = first["values"], last["values"]
        ta, tb = (a["expiry_at"] - first["event_at"]) / YEAR_SECONDS, (b["expiry_at"] - last["event_at"]) / YEAR_SECONDS
        try:
            def price(s, time, vol, rate, carry):
                return proxy_price(s, a["strike"], time, vol, rate, carry, a["right"])
            p0 = price(a["spot"], ta, a["iv"], a["rate"], a["yield"])
            ps = price(b["spot"], ta, a["iv"], a["rate"], a["yield"])
            pt = price(b["spot"], tb, a["iv"], a["rate"], a["yield"])
            pv = price(b["spot"], tb, b["iv"], a["rate"], a["yield"])
            p1 = price(b["spot"], tb, b["iv"], b["rate"], b["yield"])
            mid0, mid1 = (a["bid"] + a["ask"]) / 2, (b["bid"] + b["ask"]) / 2
            values = {"iv_change_vol_points": 100 * (b["iv"] - a["iv"]),
                      "midpoint_change": mid1 - mid0, "spot_component": ps - p0,
                      "time_component": pt - ps, "iv_component": pv - pt,
                      "carry_component": p1 - pv, "model_residual": mid1 - mid0 - (p1 - p0),
                      "quote_uncertainty": (a["ask"] - a["bid"] + b["ask"] - b["bid"]) / 2,
                      "log_moneyness_change": math.log(b["spot"] / a["spot"]),
                      "years_remaining": tb}
            if not all(finite(x) for x in values.values()):
                raise ValueError("nonfinite decomposition")
        except (ValueError, OverflowError, ZeroDivisionError):
            reject("model domain or overflow")
            continue
        output[key] = {"status": "available", "values": values, "source_id": first["source_id"],
                       "first_event_at": first["event_at"], "last_event_at": last["event_at"],
                       "first_available_at": first["available_at"], "latest_available_at": max(r["available_at"] for r in records),
                       "identity": {f: a[f] for f in identity_fields}, "window_seconds": last["event_at"] - first["event_at"]}
    return {"decision_at": t, "session": packet["session"], "provenance": packet.get("provenance", "unknown"),
            "contracts": output, "status": "available" if any(v["status"] == "available" for v in output.values()) else "unavailable",
            "interpretation": "European proxy decomposition; provider IV; no validated lead, dealer inventory or executable midpoint return"}
