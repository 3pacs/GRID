"""One-shot public snapshot custody. No timer, service, account or paid API.

python -m scripts.intraday_lab.capture --output /scratch/intraday-capture
Only grid-svr's pull receipt is an eligible RTD available_at; ANIK clocks are
unmonitored. Historical bars are retained raw, never backdated into decisions.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import socket
import time
from urllib.request import urlopen


URL = "https://gex.stepdad.finance/api/state"


def epoch(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, AttributeError):
        return None


def normalize(state, captured_at, host):
    """Public snapshot inventory, not admission to a research forward registry."""
    obs = []
    clock = "trusted_receipt" if host.split(".")[0].lower() == "grid-svr" else "unmonitored_local_receipt"
    q, g = state.get("quote") or {}, state.get("gex") or {}
    if q:
        # Brokerage exchange-event time is not established by an RTD callback.
        rtd = bool(q.get("is_rtd"))
        obs.append({"instrument": "SPY", "source_id": "gamma_watch_rtd_price" if rtd else "yahoo_intraday_unadjusted",
                    "event_at": None if rtd else epoch(q.get("as_of")), "available_at": captured_at,
                    "clock": clock, "status": "unavailable" if rtd or state.get("quote_error") else "available",
                    "values": {"price": q.get("price")},
                    "reason": "RTD exchange event time unknown" if rtd else "Public latency not guaranteed"})
    if g:
        obs.append({"instrument": "GEX", "source_id": "zerogex_vendor_delayed_gex", "event_at": epoch(g.get("as_of")),
                    "available_at": captured_at, "clock": clock,
                    "status": "unavailable" if state.get("gex_error") else "available",
                    "values": {"net_gex": g.get("net_gex_at_spot"), "spot": g.get("spot"),
                               "call_wall": g.get("call_wall"), "put_wall": g.get("put_wall")}})
    return {"decision_at": captured_at, "session": datetime.fromtimestamp(captured_at, timezone.utc).date().isoformat(),
            "provenance": "capture_inventory_not_admitted", "observations": obs,
            "broker_active": bool(state.get("broker_active")), "broker_error": state.get("broker_error"),
            "missing_inputs": ["ES depth", "verified ES executions", "SPYU executable quotes", "auction imbalances"],
            "note": "No historical bar known-at inference; no frozen/RTD model conflation; not a forward prediction."}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--input", type=Path, help="Inventory an existing public snapshot; never infer its original receipt")
    args = p.parse_args()
    if args.input:
        body = args.input.read_bytes()
    else:
        with urlopen(URL, timeout=20) as response:  # public market data only
            body = response.read(5_000_001)
    if len(body) > 5_000_000:
        raise ValueError("snapshot size exceeds limit")
    captured_at = time.time()
    digest = hashlib.sha256(body).hexdigest()
    state = json.loads(body)
    root = args.output / (str(time.time_ns()) + "-" + digest[:12])
    root.mkdir(parents=True, exist_ok=False)
    (root / "raw.json").write_bytes(body)
    packet = normalize(state, captured_at, socket.gethostname())
    receipt = {"url": URL, "raw_sha256": digest, "captured_at": captured_at,
               "host": socket.gethostname(), "packet": packet,
               "capture_basis": "local_file_inventory" if args.input else "public_http_receipt"}
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2, allow_nan=False) + "\n", encoding="utf-8", newline="\n")
    (root / "packets.jsonl").write_text(json.dumps(packet, allow_nan=False) + "\n", encoding="utf-8", newline="\n")
    print(root)


if __name__ == "__main__":
    main()
