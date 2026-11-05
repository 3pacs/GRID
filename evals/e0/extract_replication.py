"""Build E0's cached replication panel from a read-only pre-2011 price extract.

Input: a CSV ``ticker,date,adj_close`` exported read-only from ``raw_series``
(source TIINGO, id 524, series ``YF:{ticker}:adj_close``, ``pull_status =
'SUCCESS'``, the ticker's latest pull batch only, ``obs_date`` 1993-06-01 ..
2010-12-31) for today's non-Technology sector-map companies. The database
query ran with ``default_transaction_read_only=on`` and a statement timeout,
outside the 03:25-10:30Z backup window.

This tool refuses input that reaches 2011-01-01 or names a ticker in the VS1
Technology deny-list, samples every 5th session of the union trading calendar
and writes ``data/replication_pre2011_nontech_grid5.npz`` plus provenance. The
output is pinned in ``MANIFEST.sha256``; rebuilding it is a version change.

Usage: ``python -m evals.e0.extract_replication --csv FILE --out evals/e0/data``
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from evals.e0.replication import check_guards

PACKAGE = Path(__file__).resolve().parent
NAME = "replication_pre2011_nontech_grid5"
CUTOFF = date(2011, 1, 1)
GRID = 5


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--query-note", default="")
    args = parser.parse_args(argv)

    deny = frozenset(json.loads((PACKAGE / "data" / "vs1_technology_denylist.json").read_text(encoding="utf-8"))["tickers"])
    frame = pd.read_csv(args.csv, header=None, names=["ticker", "date", "value"], dtype={"ticker": str})
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame = frame.dropna()
    frame = frame[frame["value"] > 0]
    tickers = sorted(frame["ticker"].unique())
    calendar = sorted(frame["date"].unique())
    check_guards(calendar, tickers, deny, CUTOFF)
    wide = frame.pivot_table(index="date", columns="ticker", values="value", aggfunc="last").reindex(calendar)
    grid_dates = calendar[::GRID]
    closes = wide.loc[grid_dates, tickers].to_numpy(dtype=np.float32)
    args.out.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(args.out / f"{NAME}.npz", dates=np.array([d.isoformat() for d in grid_dates]),
                        tickers=np.array(tickers), closes=closes)
    digest = hashlib.sha256(Path(args.csv).read_bytes()).hexdigest()
    provenance = {
        "panel": NAME,
        "source": "raw_series TIINGO (source_id 524) YF:{ticker}:adj_close, SUCCESS rows of the latest pull batch",
        "access": "read-only transaction, statement_timeout 120s, outside 03:25-10:30Z",
        "obs_window": ["1993-06-01", "2010-12-31"],
        "universe": "today's non-Technology sector-map company tickers minus the VS1 Technology deny-list",
        "grid": f"every {GRID}th session of the union trading calendar",
        "tickers": len(tickers),
        "grid_dates": len(grid_dates),
        "first_date": grid_dates[0].isoformat(),
        "last_date": grid_dates[-1].isoformat(),
        "csv_sha256": digest,
        "query_note": args.query_note,
    }
    (args.out / f"{NAME}.provenance.json").write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n",
                                                      encoding="utf-8", newline="\n")
    print(json.dumps(provenance, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
