"""Pure parsers and point-in-time selection. Never calls a feed or writes a DB."""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from ingestion.realtime.research_intervals import MinuteBar, _utc


@dataclass(frozen=True)
class ParsedMinutes:
    bars: tuple[MinuteBar, ...]
    excluded_forming: int
    received_at: datetime
    payload_sha256: str


def schwab_minutes(
    payload: dict,
    *,
    requested_symbol: str,
    frequency_type: str,
    frequency: int,
    received_at: datetime,
) -> ParsedMinutes:
    """Parse a retained successful /pricehistory response with request metadata.

    Receipt is the actual response arrival, including for a historical backfill.
    Never substitute candle event time. Does not claim historical PIT coverage.
    """
    received = _utc(received_at)
    if frequency_type != "minute" or frequency != 1:
        raise ValueError("native one-minute request metadata required")
    if not requested_symbol or payload.get("symbol") != requested_symbol:
        raise ValueError("provider/request symbol mismatch")
    candles = payload.get("candles")
    if not isinstance(candles, list) or len(candles) > 100000:
        raise ValueError("invalid or oversized candles")
    digest = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()
    output = []
    excluded = 0
    seen = set()
    for row in candles:
        raw_ts = row.get("datetime")
        if isinstance(raw_ts, bool) or not isinstance(raw_ts, int):
            raise ValueError("integer millisecond event timestamp required")
        ts = datetime.fromtimestamp(raw_ts / 1000, timezone.utc)
        if ts.second or ts.microsecond or ts > received or ts in seen:
            raise ValueError("unaligned, future or duplicate candle")
        seen.add(ts)
        if ts + timedelta(minutes=1) > received:
            excluded += 1
            continue
        vals = [row.get(k) for k in ("open", "high", "low", "close", "volume")]
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in vals):
            raise ValueError("numeric OHLCV required")
        o, h, low, c, v = vals
        # rollup performs equivalent validation; reject corrupt data at intake too.
        import math

        if (
            not all(math.isfinite(x) for x in vals)
            or min(o, h, low, c) <= 0
            or v < 0
            or low > min(o, c)
            or h < max(o, c)
            or low > h
        ):
            raise ValueError("invalid OHLCV")
        output.append(
            MinuteBar(
                requested_symbol,
                "schwab_pricehistory",
                ts,
                received,
                o,
                h,
                low,
                c,
                v,
                f"{digest}:{raw_ts}",
                price_basis="provider_unspecified",
            )
        )
    return ParsedMinutes(
        tuple(sorted(output, key=lambda b: b.ts)), excluded, received, digest
    )


def select_as_of(
    rows,
    *,
    symbol: str,
    source: str,
    price_basis: str,
    as_of: datetime,
    opens: datetime,
    closes: datetime,
) -> tuple[MinuteBar, ...]:
    """Latest received revision per minute; ambiguous same-time corrections refuse."""
    cutoff, start, end = _utc(as_of), _utc(opens), _utc(closes)
    candidates = {}
    for b in rows:
        if (b.symbol, b.source, b.price_basis) != (symbol, source, price_basis):
            continue
        ts, available = _utc(b.ts), _utc(b.available_at)
        if not (start <= ts < end) or available > cutoff:
            continue
        candidates.setdefault(ts, []).append(b)
    selected = []
    for ts in sorted(candidates):
        group = candidates[ts]
        latest = max(_utc(b.available_at) for b in group)
        current = [b for b in group if _utc(b.available_at) == latest]
        if any(b != current[0] for b in current[1:]):
            raise ValueError("ambiguous same-time revision")
        selected.append(current[0])
    return tuple(selected)


def retained_jsonl_loader(path, *, max_bytes=8_000_000):
    """Bind one operator-selected immutable research export, not a request path.

    Each line: {schema: grid.native-minute.v1, ...MinuteBar fields...}.
    This only reads; ingestion/publication of that file is a separate step.
    Missing receipt/revision/quality fields fail closed. No event-time backfill
    of receipt metadata and no credentials or network fallback.
    """
    from pathlib import Path

    target = Path(path)

    def load(symbol, source, session, as_of):
        import stat

        with target.open("rb") as stream:
            if not stat.S_ISREG(__import__("os").fstat(stream.fileno()).st_mode):
                raise ValueError("regular retained export required")
            raw = stream.read(max_bytes + 1)
        if len(raw) > max_bytes:
            raise ValueError("retained export exceeds read bound")
        output = []
        for line in raw.decode("utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if row.pop("schema", None) != "grid.native-minute.v1":
                raise ValueError("retained schema missing")
            required = {
                "symbol",
                "source",
                "ts",
                "available_at",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "revision",
                "price_basis",
                "quality",
            }
            if set(row) != required:
                raise ValueError("retained provenance fields required")
            for key in ("ts", "available_at"):
                row[key] = _utc(datetime.fromisoformat(row[key]))
            bar = MinuteBar(**row)
            if (
                (bar.symbol, bar.source) == (symbol, source)
                and _utc(session.opens) <= bar.ts < _utc(session.closes)
                and bar.available_at <= _utc(as_of)
            ):
                output.append(bar)
        return tuple(output)

    return load
