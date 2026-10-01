"""Shared builders for the E2 scoreboard tests (not a test module)."""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

from evals.e2 import scoring
from evals.e2.adapters.e2_stream import E2StreamAdapter
from evals.e2.resolve import StaticPriceSource

STREAM = "synthetic_v1"
PREREG = "a" * 64
MANIFEST_INFO = {"manifest_sha256": "f" * 64, "version": "e2-v1", "files": 0}
CODE_SHA = "1" * 40


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def rules(version: str = "e2-v1") -> dict:
    r = copy.deepcopy(scoring.load_json("rules.json"))
    r["version"] = version
    r["registered_at"] = "2026-09-01T00:00:00+00:00"
    r["streams"][STREAM] = {"adapter": "e2_stream", "prereg_sha256": PREREG, "disclosure": "open", "sector": None}
    return r


def cost_model() -> dict:
    return scoring.load_json("cost_model.json")


def write_chain(path: Path, records: list[dict]) -> None:
    """Write records with the S10/E2 chain convention (canonical lines, prev_sha256)."""
    previous = None
    with open(path, "wb") as stream:
        for record in records:
            line = json.dumps({**record, "prev_sha256": previous}, sort_keys=True, separators=(",", ":"),
                              allow_nan=False).encode("utf-8")
            stream.write(line + b"\n")
            previous = hashlib.sha256(line).hexdigest()


ISSUED = "2026-10-01T14:00:00+00:00"   # 10:00 ET, before the 16:00 ET entry close
ENTRY, EXIT = "2026-10-01", "2026-10-02"
CLASS = "us_equity_large_cap"           # 2.5 + 0.5 + 5.0 = 8 bp per side, 16 bp round trip

CLOSES = {"AAA": (100.0, 110.0), "BBB": (100.0, 95.0), "CCC": (50.0, 51.0), "DDD": (20.0, 19.0),
          "EEE": (10.0, 10.5)}


def stream_records() -> list[dict]:
    header = {"kind": "header", "format": "e2-stream-v1", "stream": STREAM, "prereg_sha256": PREREG,
              "run_at": "2026-10-01T13:00:00+00:00"}

    def pred(pid, rule_id, call, ticker, family):
        return {"kind": "prediction", "run_at": ISSUED, "prediction_id": f"{STREAM}:{pid}", "family": family,
                "sector": "synthetic", "rule_id": rule_id, "target": {"instrument": ticker, "instrument_class": CLASS},
                "horizon": {"label": "1d", "entry_date": ENTRY, "exit_date": EXIT}, "call": call}

    out = [header,
           pred("d1", "e2.direction.v1", {"kind": "direction", "side": 1}, "AAA", "dir"),
           pred("d2", "e2.direction.v1", {"kind": "direction", "side": 1}, "BBB", "dir"),
           pred("d3", "e2.direction.v1", {"kind": "direction", "side": -1}, "CCC", "dir"),
           pred("p1", "e2.probability.v1", {"kind": "probability", "event": "up", "p": 0.8}, "AAA", "prob"),
           pred("p2", "e2.probability.v1", {"kind": "probability", "event": "up", "p": 0.3}, "BBB", "prob")]
    for ticker, score in (("AAA", 5.0), ("BBB", 1.0), ("CCC", 3.0), ("DDD", 2.0), ("EEE", 4.0)):
        out.append(pred(f"r_{ticker}", "e2.rank_ic.v1", {"kind": "rank_score", "score": score}, ticker, "rank"))
    return out


def prices(available_hour: int = 21) -> StaticPriceSource:
    """Entry closes observable 10-01 at HH:00Z, exit closes 10-02 at HH:00Z (the closes are 20:00Z, EDT)."""
    rows = []
    for ticker, (entry, exit_) in CLOSES.items():
        rows.append((ticker, date(2026, 10, 1), entry, utc(2026, 10, 1, available_hour), f"v{ticker}e"))
        rows.append((ticker, date(2026, 10, 2), exit_, utc(2026, 10, 2, available_hour), f"v{ticker}x"))
    return StaticPriceSource(rows)


def stream_adapter(log_path: Path, r: dict, price_source) -> E2StreamAdapter:
    return E2StreamAdapter(log_path, STREAM, r["streams"][STREAM], r, price_source)
