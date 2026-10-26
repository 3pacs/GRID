"""Build ``derived/submissions.parquet`` from the SEC Form 3/4/5 quarterly zips (VS1 input).

Why: the VS1 pre-registration (§2.1 "Section 16 activity", §2.2 rule 3) counts
*every* accession of an issuer -- any form type, any code, amended or not.
``derived/nonderiv_transactions.parquet`` only has accessions carrying a
non-derivative transaction line, so it misses holdings-only Form 3s,
derivative-only Form 4s and holdings-only Form 5s. This file is the SUBMISSION
table itself, one row per accession x reporting owner (an accession without a
REPORTINGOWNER row is kept once, owner CIK empty).

Source: the SEC DERA "Insider Transactions Data Sets" quarterly zips already on
grid-svr (``/data/sec/form345/raw/<YYYY>q<N>_form345.zip``, the same zips the
non-derivative file was derived from). Members read: ``SUBMISSION.tsv`` and
``REPORTINGOWNER.tsv``, straight out of each zip (no bulk unzip).

Columns: ``accession_number``, ``quarter``, ``document_type``, ``amended``
(``/A`` form type or an original-submission date), ``filing_date`` (ISO),
``filing_date_raw`` (the data set's DD-MON-YYYY), ``period_of_report``,
``issuer_cik``, ``issuer_ticker`` (ISSUERTRADINGSYMBOL as filed), ``owner_cik``.

Run (on grid-svr, files only -- no database connection, no DB writes)::

    python -m scripts.build_form345_submissions \\
        --raw-dir /data/sec/form345/raw \\
        --out /data/sec/form345/derived/submissions.parquet

The output path must not exist (write-once). A receipt
``<out>.receipt.json`` records every zip's sha256, per-quarter row counts and
the output's sha256; the VS1 harness records the output's sha256 in its own
receipts and in the registry's ``inputs_frozen`` record.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SUBMISSION_COLS = (
    "ACCESSION_NUMBER", "FILING_DATE", "PERIOD_OF_REPORT", "DATE_OF_ORIG_SUB",
    "DOCUMENT_TYPE", "ISSUERCIK", "ISSUERTRADINGSYMBOL",
)
OWNER_COLS = ("ACCESSION_NUMBER", "RPTOWNERCIK")
OUT_COLUMNS = [
    "accession_number", "quarter", "document_type", "amended", "filing_date", "filing_date_raw",
    "period_of_report", "issuer_cik", "issuer_ticker", "owner_cik",
]


def _read_member(zf: zipfile.ZipFile, name: str, usecols: tuple[str, ...]) -> pd.DataFrame:
    with zf.open(name) as fh:
        # every column as text: CIKs, dates and form types stay exactly as filed
        return pd.read_csv(
            fh, sep="\t", dtype=str, usecols=lambda c: c in usecols,
            keep_default_na=False, na_values=[""], quoting=3,
        )


def _iso(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, format="%d-%b-%Y", errors="coerce").dt.strftime("%Y-%m-%d")


def process_quarter(zip_path: str | Path, quarter: str) -> pd.DataFrame:
    """One quarter's SUBMISSION x REPORTINGOWNER rows (every accession kept)."""
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        for required in ("SUBMISSION.tsv", "REPORTINGOWNER.tsv"):
            if required not in names:
                raise FileNotFoundError(f"{quarter}: {required} missing from {zip_path}")
        sub = _read_member(zf, "SUBMISSION.tsv", SUBMISSION_COLS)
        own = _read_member(zf, "REPORTINGOWNER.tsv", OWNER_COLS)
    for column in SUBMISSION_COLS:
        if column not in sub.columns:
            sub[column] = pd.NA
    if sub["ACCESSION_NUMBER"].duplicated().any():
        raise ValueError(f"{quarter}: SUBMISSION.tsv repeats an accession number")
    owners = own.dropna(subset=["ACCESSION_NUMBER"]).drop_duplicates()
    merged = sub.merge(owners, on="ACCESSION_NUMBER", how="left", validate="one_to_many")
    out = pd.DataFrame({
        "accession_number": merged["ACCESSION_NUMBER"].str.strip(),
        "quarter": quarter,
        "document_type": merged["DOCUMENT_TYPE"].str.strip(),
        "amended": (
            merged["DOCUMENT_TYPE"].fillna("").str.contains("/A", regex=False)
            | merged["DATE_OF_ORIG_SUB"].fillna("").str.len().gt(0)
        ),
        "filing_date": _iso(merged["FILING_DATE"]),
        "filing_date_raw": merged["FILING_DATE"],
        "period_of_report": _iso(merged["PERIOD_OF_REPORT"]),
        "issuer_cik": merged["ISSUERCIK"].str.strip(),
        "issuer_ticker": merged["ISSUERTRADINGSYMBOL"],
        "owner_cik": merged["RPTOWNERCIK"].str.strip(),
    })
    return out[OUT_COLUMNS]


def _sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build(raw_dir: str | Path, out: str | Path) -> dict:
    """Write the parquet and its receipt; both must be new. Returns the receipt."""
    out = Path(out)
    receipt_path = Path(f"{out}.receipt.json")
    if out.exists() or receipt_path.exists():
        raise FileExistsError(f"{out} (or its receipt) exists: derived files are write-once")
    zips = sorted(glob.glob(os.path.join(str(raw_dir), "*_form345.zip")))
    if not zips:
        raise FileNotFoundError(f"no *_form345.zip in {raw_dir}")
    frames, quarters = [], []
    for path in zips:
        quarter = os.path.basename(path).replace("_form345.zip", "")
        frame = process_quarter(path, quarter)
        frames.append(frame)
        quarters.append({
            "quarter": quarter,
            "zip": os.path.basename(path),
            "zip_sha256": _sha256(path),
            "rows": int(len(frame)),
            "accessions": int(frame["accession_number"].nunique()),
        })
        print(f"  {quarter}: {len(frame)} rows", file=sys.stderr)
    full = pd.concat(frames, ignore_index=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    full.to_parquet(out, index=False)
    receipt = {
        "builder": "scripts/build_form345_submissions.py",
        "built_at": datetime.now(timezone.utc).isoformat(),
        "output": out.name,
        "output_sha256": _sha256(out),
        "rows": int(len(full)),
        "accessions": int(full["accession_number"].nunique()),
        "quarters": quarters,
    }
    with receipt_path.open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True)
    return receipt


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw-dir", required=True, help="directory of <YYYY>q<N>_form345.zip files")
    parser.add_argument("--out", required=True, help="output .parquet path (must not exist)")
    args = parser.parse_args(argv)
    receipt = build(args.raw_dir, args.out)
    print(json.dumps({k: receipt[k] for k in ("output", "output_sha256", "rows", "accessions")}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
