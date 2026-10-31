"""Deterministic tests for ingestion/schwab_chain_seam.py (pure; synthetic chain bodies).

Negative controls: invalid bodies, identity mismatches (OCC symbol, putCall, strike),
missing provider quote time, non-finite / sentinel / fractional / oversized values,
absent contracts, malformed buckets, and the GEX identity gate (mirrored from
collectors/gamma_watch/broker.py) rejecting the seam's own envelope.
"""

from __future__ import annotations

import copy
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from ingestion.schwab_chain_seam import (
    CONTRACT_FIELDS,
    MAX_CONTRACTS,
    SCHEMA,
    UNDERLYING_FIELDS,
    Contract,
    InvalidChainError,
    occ_to_tos,
    parse_contracts,
    schwab_chain_to_rtd_records,
    tos_symbol,
)
from ingestion.schwab_quote_path import PrivateQuote

RECEIVED = datetime(2026, 10, 31, 14, 30, 0, tzinfo=timezone.utc)
QUOTE_DT = RECEIVED - timedelta(seconds=2)
QT_MS = int(QUOTE_DT.timestamp() * 1000)
EXPIRY = "2026-11-20"
YMD = "261120"

UNIVERSE: list[dict[str, Any]] = [
    {"symbol": ".SPY261120C735", "expiry": EXPIRY, "opt_type": "call", "strike": 735.0},
    {"symbol": ".SPY261120P735", "expiry": EXPIRY, "opt_type": "put", "strike": 735.0},
    {"symbol": ".SPY261120C735.5", "expiry": EXPIRY, "opt_type": "call", "strike": 735.5},
    {"symbol": ".SPY261120P735.5", "expiry": EXPIRY, "opt_type": "put", "strike": 735.5},
]


def occ(strike: float, cp: str, root: str = "SPY", ymd: str = YMD) -> str:
    return f"{root:<6}{ymd}{cp}{round(strike * 1000):08d}"


def entry(cp: str, strike: float, **over: Any) -> dict[str, Any]:
    base = {
        "putCall": cp,
        "symbol": occ(strike, cp[0]),
        "strikePrice": strike,
        "expirationDate": f"{EXPIRY}T21:00:00.000+00:00",
        "bid": 1.2,
        "ask": 1.3,
        "last": 1.25,
        "bidSize": 10,
        "askSize": 12,
        "totalVolume": 55,
        "openInterest": 1000,
        "volatility": 18.5,
        "delta": 0.5,
        "gamma": 0.012,
        "quoteTimeInLong": QT_MS,
        "tradeTimeInLong": QT_MS,
    }
    base.update(over)
    return base


def body(**over: Any) -> dict[str, Any]:
    b: dict[str, Any] = {
        "symbol": "SPY",
        "status": "SUCCESS",
        "underlyingPrice": 731.37,
        "underlying": {"symbol": "SPY", "last": 731.37, "bid": 731.35, "ask": 731.39, "quoteTime": QT_MS},
        "callExpDateMap": {f"{EXPIRY}:20": {"735.0": [entry("CALL", 735.0)], "735.5": [entry("CALL", 735.5)]}},
        "putExpDateMap": {f"{EXPIRY}:20": {"735.0": [entry("PUT", 735.0)], "735.5": [entry("PUT", 735.5)]}},
    }
    b.update(over)
    return b


def convert(b: dict[str, Any] | None = None, **kw: Any) -> dict[str, Any]:
    return schwab_chain_to_rtd_records(body() if b is None else b, UNIVERSE, received_at=RECEIVED, **kw)


def by_field(env: dict[str, Any], symbol: str) -> dict[str, dict[str, Any]]:
    return {r["field"]: r for r in env["records"] if r["symbol"] == symbol}


# ── symbol conversion ────────────────────────────────────────────────────────

class TestSymbols:
    @pytest.mark.parametrize(
        ("occ_text", "tos"),
        [
            ("SPY   260928C00735000", ".SPY260928C735"),
            ("SPY   260928P00735500", ".SPY260928P735.5"),
            ("spy   260928c00735000", ".SPY260928C735"),
            ("QQQ   261120C00500250", ".QQQ261120C500.25"),
        ],
    )
    def test_occ_to_tos(self, occ_text, tos):
        assert occ_to_tos(occ_text) == tos

    @pytest.mark.parametrize("bad", ["SPY260928C735", ".SPY260928C735", "SPY   260928X00735000", "", None, 735, b"SPY"])
    def test_occ_to_tos_rejects_malformed(self, bad):
        assert occ_to_tos(bad) is None

    def test_tos_symbol_trims_zeros(self):
        assert tos_symbol("SPY", "260928", "C", 735.0) == ".SPY260928C735"
        assert tos_symbol("SPY", "260928", "C", 735.5) == ".SPY260928C735.5"
        assert tos_symbol("SPY", "260928", "C", 735.25) == ".SPY260928C735.25"


class TestUniverse:
    def test_parse_contracts(self):
        out = parse_contracts(UNIVERSE)
        assert out[0] == Contract(".SPY261120C735", EXPIRY, "call", 735.0)
        assert len(out) == 4

    @pytest.mark.parametrize(
        "bad",
        [
            "not a list",
            {"symbol": ".SPY261120C735"},
            [1],
            [{"expiry": EXPIRY, "opt_type": "call", "strike": 735.0}],
            [{"symbol": "SPY261120C735", "expiry": EXPIRY, "opt_type": "call", "strike": 735.0}],
            [{"symbol": ".SPY261120C735", "expiry": "11/20/2026", "opt_type": "call", "strike": 735.0}],
            [{"symbol": ".SPY261120C735", "expiry": EXPIRY, "opt_type": "CALL", "strike": 735.0}],
            [{"symbol": ".SPY261120C735", "expiry": EXPIRY, "opt_type": "call", "strike": 0}],
            [{"symbol": ".SPY261120C735", "expiry": EXPIRY, "opt_type": "call", "strike": "x"}],
            [{"symbol": ".SPY261120C735", "expiry": EXPIRY, "opt_type": "put", "strike": 735.0}],
            [{"symbol": ".SPY261120C735", "expiry": EXPIRY, "opt_type": "call", "strike": 736.0}],
            [{"symbol": ".SPY261120C735", "expiry": "2026-11-21", "opt_type": "call", "strike": 735.0}],
            UNIVERSE + [UNIVERSE[0]],
        ],
    )
    def test_parse_contracts_rejects(self, bad):
        with pytest.raises(InvalidChainError):
            parse_contracts(bad)

    def test_universe_cap(self):
        big = [
            {"symbol": tos_symbol("SPY", YMD, "C", 100.0 + i), "expiry": EXPIRY, "opt_type": "call", "strike": 100.0 + i}
            for i in range(MAX_CONTRACTS + 1)
        ]
        with pytest.raises(InvalidChainError, match="more than"):
            parse_contracts(big)


# ── happy path ───────────────────────────────────────────────────────────────

class TestConversion:
    def test_envelope_identity_and_counts(self):
        env = convert()
        assert env["schema"] == SCHEMA == "spy-chain-schwab-v1"
        assert env["host"] == "GRID"
        assert env["source"] == "schwab-marketdata-v1"
        assert env["audience"] == "private" and env["redistribution"] == "prohibited"
        assert env["persist_allowed"] is False
        assert env["exchange_timestamp_available"] is True
        assert env["collected_at"] == RECEIVED.isoformat()
        assert env["heartbeat"] == 1 and env["update_count"] == 4
        assert env["universe_size"] == 4 and env["seen_contracts"] == 4
        assert env["rejected"] == []
        assert env["topic_count"] == len(env["records"]) == 4 * len(CONTRACT_FIELDS) + len(UNDERLYING_FIELDS)

    def test_gex_identity_gate_rejects_the_seam_envelope(self):
        """Mirror of broker.get_snapshot's gate: the seam never passes it by accident."""
        env = convert()
        accepted = env.get("schema") == "spy-rtd-v1" and env.get("host", "").upper() == "ANIK"
        assert accepted is False

    def test_contract_records_match_rtd_shape_and_units(self):
        env = convert()
        f = by_field(env, ".SPY261120C735")
        assert set(f) == set(CONTRACT_FIELDS)
        for rec in f.values():
            assert set(rec) == {"symbol", "field", "value", "received_at", "callback_seen"}
            assert rec["callback_seen"] is True
            assert rec["received_at"] == QUOTE_DT.isoformat()  # provider exchange time, not receipt
        assert f["BID"]["value"] == 1.2 and f["ASK"]["value"] == 1.3
        assert f["IMPL_VOL"]["value"] == pytest.approx(0.185)  # percent -> fraction
        assert f["OPEN_INT"]["value"] == 1000 and isinstance(f["OPEN_INT"]["value"], int)
        assert f["VOLUME"]["value"] == 55 and f["GAMMA"]["value"] == 0.012 and f["DELTA"]["value"] == 0.5

    def test_half_strike_contract_is_matched(self):
        f = by_field(convert(), ".SPY261120P735.5")
        assert f["BID"]["callback_seen"] is True

    def test_underlying_from_body(self):
        f = by_field(convert(), "SPY")
        assert set(f) == set(UNDERLYING_FIELDS)
        assert (f["LAST"]["value"], f["BID"]["value"], f["ASK"]["value"]) == (731.37, 731.35, 731.39)
        assert f["LAST"]["received_at"] == QUOTE_DT.isoformat()

    def test_underlying_from_measured_private_quote_takes_precedence(self):
        q = _quote("fresh", live=True)
        f = by_field(convert(underlying=q), "SPY")
        assert f["LAST"]["value"] == 700.0 and f["LAST"]["received_at"] == q.quote_time.isoformat()

    @pytest.mark.parametrize("status", ["missing", "invalid", "unavailable"])
    def test_unmeasured_private_quote_yields_unseen_underlying(self, status):
        f = by_field(convert(underlying=_quote(status)), "SPY")
        assert all(r["callback_seen"] is False and r["value"] is None for r in f.values())

    def test_underlying_param_must_be_private_quote(self):
        with pytest.raises(InvalidChainError, match="PrivateQuote"):
            convert(underlying={"last": 1})  # type: ignore[arg-type]

    def test_underlying_absent_or_unparseable_is_unseen(self):
        for b in (body(underlying=None), body(underlying={"last": 1, "bid": 1, "ask": 1}), body(underlying="x")):
            f = by_field(convert(b), "SPY")
            assert all(r["callback_seen"] is False for r in f.values())

    def test_deterministic_and_strict_json(self):
        a, b = convert(), convert()
        assert a == b
        json.dumps(a, allow_nan=False)
        order = [r["symbol"] for r in a["records"] if r["symbol"] != "SPY"]
        assert order == [c["symbol"] for c in UNIVERSE for _ in CONTRACT_FIELDS]

    def test_host_override(self):
        assert convert(host=" grid-svr ")["host"] == "grid-svr"


# ── per-contract negative controls ───────────────────────────────────────────

class TestContractRejection:
    def test_absent_contract_is_unseen_not_rejected(self):
        b = body()
        del b["putExpDateMap"][f"{EXPIRY}:20"]["735.5"]
        env = convert(b)
        f = by_field(env, ".SPY261120P735.5")
        assert all(r["callback_seen"] is False and r["value"] is None and r["received_at"] is None for r in f.values())
        assert env["seen_contracts"] == 3 and env["rejected"] == []

    def test_symbol_identity_mismatch_is_rejected(self):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, symbol=occ(736.0, "C"))]
        env = convert(b)
        assert env["rejected"] == [{"symbol": ".SPY261120C735", "reason": "provider symbol does not match universe contract"}]
        assert all(not r["callback_seen"] for r in by_field(env, ".SPY261120C735").values())
        assert env["seen_contracts"] == 3

    @pytest.mark.parametrize("sym", [None, "", "SPY", 735, "SPY   261120X00735000"])
    def test_malformed_symbol_is_rejected(self, sym):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, symbol=sym)]
        assert convert(b)["rejected"][0]["reason"] == "provider symbol missing or not OCC form"

    def test_put_call_mismatch_is_rejected(self):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, putCall="PUT")]
        assert convert(b)["rejected"][0]["reason"] == "putCall does not match universe contract"

    @pytest.mark.parametrize("strike", [736.0, None, "x", math.nan])
    def test_strike_mismatch_is_rejected(self, strike):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, strikePrice=strike)]
        assert convert(b)["rejected"][0]["reason"] == "strikePrice does not match universe contract"

    @pytest.mark.parametrize("qt", [None, "soon", -1, 4_102_444_800_001, math.inf, True, 1e300])
    def test_missing_quote_time_is_rejected(self, qt):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, quoteTimeInLong=qt)]
        env = convert(b)
        assert env["rejected"][0]["reason"] == "no parseable quoteTimeInLong"
        assert all(not r["callback_seen"] for r in by_field(env, ".SPY261120C735").values())

    @pytest.mark.parametrize(
        ("field", "value", "rtd"),
        [
            ("bid", "abc", "BID"), ("bid", True, "BID"), ("bid", math.nan, "BID"), ("bid", -0.01, "BID"),
            ("ask", math.inf, "ASK"), ("ask", None, "ASK"),
            ("volatility", -999.0, "IMPL_VOL"), ("volatility", -999.99, "IMPL_VOL"), ("volatility", 0, "IMPL_VOL"),
            ("volatility", math.nan, "IMPL_VOL"), ("volatility", "18.5%", "IMPL_VOL"),
            ("openInterest", 12.5, "OPEN_INT"), ("openInterest", -1, "OPEN_INT"), ("openInterest", 2**64, "OPEN_INT"),
            ("openInterest", "1e3", "OPEN_INT"), ("openInterest", True, "OPEN_INT"),
            ("totalVolume", 1.5, "VOLUME"), ("gamma", "g", "GAMMA"), ("delta", math.inf, "DELTA"),
        ],
    )
    def test_invalid_value_only_drops_that_field(self, field, value, rtd):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, **{field: value})]
        env = convert(b)
        f = by_field(env, ".SPY261120C735")
        assert f[rtd]["callback_seen"] is False and f[rtd]["value"] is None
        others = [k for k in CONTRACT_FIELDS if k != rtd]
        assert all(f[k]["callback_seen"] is True for k in others)
        assert env["rejected"] == [] and env["seen_contracts"] == 4

    def test_integer_strings_keep_precision(self):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, openInterest="9007199254740993")]
        v = by_field(convert(b), ".SPY261120C735")["OPEN_INT"]["value"]
        assert v == 9007199254740993 and isinstance(v, int)

    def test_zero_values_are_kept_as_zero_not_dropped(self):
        b = body()
        b["callExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("CALL", 735.0, bid=0.0, openInterest=0, totalVolume=0)]
        f = by_field(convert(b), ".SPY261120C735")
        assert f["BID"]["value"] == 0.0 and f["OPEN_INT"]["value"] == 0 and f["VOLUME"]["value"] == 0

    def test_nothing_seen_gives_zero_heartbeat(self):
        env = convert(body(callExpDateMap={}, putExpDateMap={}))
        assert env["heartbeat"] == 0 and env["update_count"] == 0 and env["seen_contracts"] == 0


# ── body-level negative controls ─────────────────────────────────────────────

class TestInvalidBodies:
    @pytest.mark.parametrize(
        "bad",
        [None, "x", [], 1, body(status="FAILED"), body(status=""), {"status": "SUCCESS"},
         body(callExpDateMap=[]), body(putExpDateMap="x")],
    )
    def test_structurally_invalid_bodies_raise(self, bad):
        with pytest.raises(InvalidChainError):
            schwab_chain_to_rtd_records(bad, UNIVERSE, received_at=RECEIVED)

    def test_status_absent_is_accepted(self):
        b = body()
        del b["status"]
        assert convert(b)["seen_contracts"] == 4

    def test_malformed_buckets_are_recorded_not_fatal(self):
        b = body()
        b["callExpDateMap"]["bad-key"] = {"735.0": [entry("CALL", 735.0)]}
        b["callExpDateMap"][f"{EXPIRY}:20"]["abc"] = [entry("CALL", 735.0)]
        b["callExpDateMap"][f"{EXPIRY}:20"]["736.0"] = "not a list"
        b["putExpDateMap"][f"{EXPIRY}:20"]["735.0"] = [entry("PUT", 735.0), entry("PUT", 735.0)]
        b["putExpDateMap"][f"{EXPIRY}:20"]["735.5"] = [1]
        env = convert(b)
        reasons = sorted(r["reason"] for r in env["rejected"])
        assert reasons == sorted([
            "malformed expiry bucket", "malformed strike bucket", "malformed strike bucket",
            "duplicate contract entry", "contract entry is not a mapping",
        ])
        assert env["seen_contracts"] == 3  # P735.5 had no usable entry

    def test_received_at_must_be_aware(self):
        with pytest.raises(InvalidChainError, match="timezone-aware"):
            schwab_chain_to_rtd_records(body(), UNIVERSE, received_at=RECEIVED.replace(tzinfo=None))

    @pytest.mark.parametrize("host", ["", "  ", None, 5])
    def test_host_must_be_non_empty(self, host):
        with pytest.raises(InvalidChainError, match="host"):
            convert(host=host)  # type: ignore[arg-type]

    def test_body_contract_cap(self):
        b = body()
        bucket = {str(100.0 + i): [entry("CALL", 100.0 + i)] for i in range(MAX_CONTRACTS + 1)}
        b["callExpDateMap"] = {f"{EXPIRY}:20": bucket}
        with pytest.raises(InvalidChainError, match="more than"):
            convert(b)

    def test_input_is_not_mutated(self):
        b = body()
        before = copy.deepcopy(b)
        convert(b)
        assert b == before


# ── source guard ─────────────────────────────────────────────────────────────

def test_module_does_not_touch_gex_tree_or_io():
    src = Path("ingestion/schwab_chain_seam.py").read_text()
    code = src.split('"""', 2)[2]
    for token in ("collectors", "gamma_watch", "broker", "requests", "urllib", "socket", "open(",
                  "Path(", "subprocess", "sqlalchemy", "datetime.now", "SchwabMarketClient", "load_credentials"):
        assert token not in code, token


def _quote(status: str, *, live: bool = False) -> PrivateQuote:
    measured = status in ("fresh", "stale", "delayed")
    return PrivateQuote(
        symbol="SPY", status=status, reason=None if status == "fresh" else status, live=live and status == "fresh",
        last=700.0 if measured else None, bid=699.9 if measured else None, ask=700.1 if measured else None,
        bid_size=1 if measured else None, ask_size=1 if measured else None, total_volume=1 if measured else None,
        quote_time=QUOTE_DT if measured else None, trade_time=None, received_at=RECEIVED,
        age_seconds=2.0 if measured else None, realtime=True if measured else None,
        asset_main_type="EQUITY" if measured else None, max_quote_age_s=20.0, provider_symbol="SPY",
    )  # type: ignore[arg-type]
