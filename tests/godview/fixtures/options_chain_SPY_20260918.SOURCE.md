# Fixture provenance — `options_chain_SPY_20260918.json`

**What:** a real, live options-chain snapshot for SPY (SPDR S&P 500 ETF
Trust), calls + puts, single expiry `2026-10-16` (a standard monthly
expiry, ~28 days out from capture — chosen to avoid same-day/0-DTE
contracts where `t_years` could be degenerate).

**Where:** fetched via `yfinance` 1.7.0 (`yfinance.Ticker("SPY").option_chain("2026-10-16")`
and `yfinance.Ticker("SPY").fast_info`), which scrapes Yahoo! Finance's
public, credential-free options-chain endpoint. No API key, login, or
paid data plan was used. `yfinance` is already a pinned dependency of
this repo (`pip show yfinance` → 1.7.0, confirmed installed in this
worktree's environment before the fetch).

**When:** captured `2026-09-18T` (exact wall-clock capture timestamp is
recorded per-run inside the JSON itself, field `capture_timestamp_utc`)
— see that field in the fixture for the precise UTC instant. `snap_date`
recorded in the fixture is the UTC calendar date of capture, `2026-09-18`.

**How (exact fetch script, run once, output committed verbatim):**

```python
import json
import datetime as dt
import yfinance as yf

SYMBOL = "SPY"
EXPIRY = "2026-10-16"

capture_ts = dt.datetime.now(dt.timezone.utc).isoformat()

t = yf.Ticker(SYMBOL)
fast_info = dict(t.fast_info)
spot = fast_info.get("lastPrice")

chain = t.option_chain(EXPIRY)
calls = chain.calls
puts = chain.puts

def rows_from(df, opt_type):
    out = []
    for _, r in df.iterrows():
        out.append({
            "opt_type": opt_type,
            "strike": float(r["strike"]),
            "open_interest": None if r["openInterest"] is None or (isinstance(r["openInterest"], float) and r["openInterest"] != r["openInterest"]) else float(r["openInterest"]),
            "implied_vol": None if r["impliedVolatility"] is None or (isinstance(r["impliedVolatility"], float) and r["impliedVolatility"] != r["impliedVolatility"]) else float(r["impliedVolatility"]),
            "last_trade_date": str(r["lastTradeDate"]),
            "volume": None if r["volume"] is None or (isinstance(r["volume"], float) and r["volume"] != r["volume"]) else float(r["volume"]),
        })
    return out

rows = rows_from(calls, "call") + rows_from(puts, "put")

fixture = {
    "symbol": SYMBOL,
    "expiry": EXPIRY,
    "capture_timestamp_utc": capture_ts,
    "spot_price": spot,
    "snap_date": dt.datetime.now(dt.timezone.utc).date().isoformat(),
    "rows": rows,
}
```

The fetch succeeded on the first attempt — no fallback to a public CSV
was needed.

**Fields kept** (per row, both calls and puts): `opt_type`, `strike`,
`open_interest`, `implied_vol` (Yahoo's `impliedVolatility`, already
computed by Yahoo — this test never solves for IV itself, matching the
engine's own "IV is read directly, never solved for" rule),
`last_trade_date`, `volume` (kept for context, not used by the
validation test). Underlying/spot price as reported by
`yfinance.Ticker("SPY").fast_info["lastPrice"]` at capture time is stored
once at the fixture top level (`spot_price`), not per-row (yfinance's
option-chain response does not repeat it per contract).

**Counts:** 402 total rows (205 calls + 197 puts) for the single expiry
`2026-10-16`; all 402 rows carry a positive `implied_vol` (Yahoo backfills
a model IV even for zero-open-interest strikes) and a positive
time-to-expiry as of `snap_date`, so all 402 are "usable" under this
engine's own skip rule (missing/non-positive IV or `expiry <= snap_date`).
380 of the 402 rows carry `open_interest > 0`; the remaining 22 have
`open_interest = 0` (far OTM strikes) and contribute exactly zero dollar
gamma but are still counted as "used" contracts by the engine, since its
`contracts_used` counter is defined by usable-IV-and-T, not by OI — the
independent test in `test_dealer_gex_validation.py` mirrors this rule
exactly so the two counts can be compared on equal terms.

**Known limitation, disclosed rather than hidden:** Yahoo's
`impliedVolatility` is itself a *model output* (Yahoo's own IV solve from
last/mid price), not a raw market-quoted number — this is standard for
any public, credential-free chain source and is exactly the same
`implied_vol` semantics the production `dealer_gex_pillar.py` engine
already assumes for its own `options_snapshots` input (its docstring:
"implied_vol is read DIRECTLY... this engine never solves for or assumes
one"). This fixture does not change or launder that assumption; it
supplies a real, sourced set of `(strike, opt_type, open_interest,
implied_vol, expiry)` tuples and a real spot price to drive both the
engine and an independently-written expected-value calculation, so the
two can be checked for mechanical agreement — see the "what this proves"
paragraph in `test_dealer_gex_validation.py` and in
`docs/reference/GODVIEW_PILLAR_CONTRACT.md` section 16.
