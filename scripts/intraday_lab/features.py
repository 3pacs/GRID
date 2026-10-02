"""Point-in-time feature primitives. Missing inputs never become neutral votes.

Packets contain decision_at (trusted server receipt), session, provenance, and
observations: [{source_id, instrument, event_at, available_at, status, values}].
All clocks are epoch UTC seconds. available_at must be first trusted receipt;
historical bar timestamps and an ANIK callback clock are not substitutes.
"""
from __future__ import annotations

import math
from statistics import mean, pstdev

FEATURES = (
    "price_momentum", "es_depth", "es_ofi", "es_aggression", "es_absorption",
    "breadth", "breadth_divergence", "es_lead", "nq_lead", "constituent_lead",
    "auction", "range_compression", "failed_breaks", "gamma_range",
)


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def rows(packet, instrument, fields, lookback=1200, max_age=10):
    """Reject future/untrusted/unavailable records; don't forward-fill across gaps."""
    t = packet["decision_at"]
    result = []
    for r in packet.get("observations", []):
        event, receipt = r.get("event_at"), r.get("available_at")
        v = r.get("values", {})
        if (r.get("instrument") == instrument and r.get("status") == "available"
                and r.get("source_id") and r.get("clock") == "trusted_receipt"
                and number(event) and number(receipt) and event <= receipt <= t
                and t - lookback <= event and all(number(v.get(k)) for k in fields)):
            result.append(r)
    result.sort(key=lambda r: (r["event_at"], r["available_at"]))
    if not result or t - result[-1]["event_at"] > max_age:
        return []
    # Mixed feeds or duplicate exchange events need an upstream reconciliation.
    if len({r["source_id"] for r in result}) != 1:
        return []
    if len({r["event_at"] for r in result}) != len(result):
        return []
    return result


def path(packet, instrument="SPY", seconds=300):
    data = rows(packet, instrument, ("price",), seconds + 10)
    if (len(data) < 2 or data[0]["event_at"] > packet["decision_at"] - seconds + 10
            or any(b["event_at"] - a["event_at"] > 10 for a, b in zip(data, data[1:]))
            or any(r["values"]["price"] <= 0 for r in data)):
        return []
    return data


def move(data):
    return 10000 * (data[-1]["values"]["price"] / data[0]["values"]["price"] - 1)


def compute(packet):
    """Fixed research features. Each returns value/status/input lineage, not a trade."""
    t = packet.get("decision_at")
    if not number(t) or not packet.get("session"):
        raise ValueError("trusted decision_at and exchange session required")
    out = {name: {"status": "unavailable", "value": None, "reason": "missing, stale or incomplete inputs"}
           for name in FEATURES}

    def put(name, value, inputs):
        if not number(value):
            return
        out[name] = {"status": "available", "value": value,
                     "input_sources": sorted({r["source_id"] for r in inputs}),
                     "latest_available_at": max(r["available_at"] for r in inputs)}

    spy = path(packet)
    if spy:
        put("price_momentum", move(spy), spy)
    book = rows(packet, "ES_BOOK", ("bid", "ask", "bid_size", "ask_size"), 60)
    book = [r for r in book if 0 < r["values"]["bid"] < r["values"]["ask"]
            and r["values"]["bid_size"] > 0 and r["values"]["ask_size"] > 0]
    if book:
        v = book[-1]["values"]
        put("es_depth", (v["bid_size"] - v["ask_size"]) / (v["bid_size"] + v["ask_size"]), book[-1:])
    if (len(book) > 1 and book[0]["event_at"] <= t - 50
            and all(b["event_at"] - a["event_at"] <= 10 for a, b in zip(book, book[1:]))):
        ofi = 0
        for prev, current in zip(book, book[1:]):
            a, b = prev["values"], current["values"]
            ofi += (b["bid_size"] * (b["bid"] >= a["bid"])
                    - a["bid_size"] * (b["bid"] <= a["bid"])
                    - b["ask_size"] * (b["ask"] <= a["ask"])
                    + a["ask_size"] * (b["ask"] >= a["ask"]))
        depth = mean(r["values"]["bid_size"] + r["values"]["ask_size"] for r in book)
        put("es_ofi", ofi / depth, book)
    flow = rows(packet, "ES_FLOW", ("buy_volume", "sell_volume", "window_seconds"), 10)
    if flow:
        r = flow[-1]
        v = r["values"]
        total = v["buy_volume"] + v["sell_volume"]
        if (v["buy_volume"] >= 0 and v["sell_volume"] >= 0 and total > 0
                and v["window_seconds"] == 60 and v.get("classification") == "verified_aggressor"):
            delta = (v["buy_volume"] - v["sell_volume"]) / total
            put("es_aggression", delta, [r])
            es = path(packet, "ES", 60)
            if es and abs(delta) >= .3 and abs(move(es)) <= 1:
                put("es_absorption", -delta, [r] + es)
    breadth = rows(packet, "BREADTH", ("tick", "up_volume", "down_volume"), 300)
    breadth = [r for r in breadth if r["values"]["up_volume"] >= 0
               and r["values"]["down_volume"] >= 0]
    if breadth:
        v = breadth[-1]["values"]
        total = v["up_volume"] + v["down_volume"]
        if total > 0:
            strength = .5 * max(-1, min(1, v["tick"] / 1000)) + .5 * (v["up_volume"] - v["down_volume"]) / total
            put("breadth", strength, breadth[-1:])
            if spy and strength * move(spy) < 0:
                put("breadth_divergence", strength, breadth[-1:] + spy)
    for instrument, name in (("ES", "es_lead"), ("NQ", "nq_lead"), ("SPX_CONSTITUENTS", "constituent_lead")):
        leader = path(packet, instrument, 60)
        follower = path(packet, "SPY", 60)
        if leader and follower:
            # A discrepancy is only a candidate lead; paired downstream outcomes decide.
            put(name, move(leader) - move(follower), leader + follower)
    auction = rows(packet, "AUCTION", ("buy_notional", "sell_notional", "seconds_to_close"), 10)
    if auction:
        r, v = auction[-1], auction[-1]["values"]
        total = v["buy_notional"] + v["sell_notional"]
        if (0 <= v["seconds_to_close"] <= 600 and min(v["buy_notional"], v["sell_notional"]) >= 0
                and total > 0 and v.get("universe") == "SPX_CONSTITUENTS"):
            put("auction", (v["buy_notional"] - v["sell_notional"]) / total, [r])
    long_path = path(packet, "SPY", 1200)
    if long_path:
        prices = [r["values"]["price"] for r in long_path]
        returns = [math.log(b / a) for a, b in zip(prices, prices[1:])]
        # Same sampling cadence for both volatility windows.
        steps = [b["event_at"] - a["event_at"] for a, b in zip(long_path, long_path[1:])]
        if max(steps) == min(steps) and min(steps) > 0:
            recent = returns[-int(300 / steps[0]):]
            if len(recent) > 1 and pstdev(returns) > 0:
                compression = 1 - pstdev(recent) / pstdev(returns)
                put("range_compression", compression, long_path)
                gex = rows(packet, "GEX", ("net_gex", "spot", "call_wall", "put_wall"), 1800, 1800)
                if gex:
                    g = gex[-1]["values"]
                    proximity = min(abs(prices[-1] - g["call_wall"]), abs(prices[-1] - g["put_wall"])) / prices[-1]
                    if g["net_gex"] > 0 and proximity <= .002:
                        put("gamma_range", compression, long_path + gex[-1:])
        # Boundaries fixed using the earlier ten minutes, count excursions in the later ten.
        early = [r["values"]["price"] for r in long_path if r["event_at"] <= t - 600]
        later = [r["values"]["price"] for r in long_path if r["event_at"] > t - 600]
        if early and later and max(early) > min(early):
            lo, hi = min(early), max(early)
            outside, failures = 0, 0
            for price in later:
                side = 1 if price > hi else -1 if price < lo else 0
                if outside and side == 0:
                    failures += 1
                outside = side
            put("failed_breaks", float(failures), long_path)
    return {"decision_at": t, "session": packet["session"], "provenance": packet.get("provenance", "unknown"),
            "features": out, "interpretation": "research candidates, no validated lead or dealer inventory"}
