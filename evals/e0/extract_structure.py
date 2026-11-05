"""Extract the VS1 v7 Technology panel STRUCTURE (features only) for E0.

Offline, one-time tool. It rebuilds the four declared VS1 v7 feature panels
(``A90``/``A30`` x ``fwd5``/``fwd20``) exactly as the v7 Stage-0 design
simulation did (``v6.load_inputs`` + ``v6.feature_panel`` on proxy sessions for
the 202 price-admitted v6 issuers, discovery start 2011-10-01, end 2020-01-01),
and writes them with anonymised entity ids to ``evals/e0/data``.

What it reads: the copied SEC Form 3/4/5 non-derivative transactions and
SUBMISSION parquet files, SEC ``company_tickers.json``, the issuer SIC map and
the sanitised v6 price-probe METADATA (for the admitted ticker list only).
What it never reads: any price, return, IC, discovery or holdout artefact. The
output carries no ticker or CIK, only the feature geometry (dates, abstentions,
event-driven density values).

Usage (from the repo root)::

    python -m evals.e0.extract_structure --form4 F --submissions S \
        --issuer-map company_tickers.json --sic-map issuer_sic_map.jsonl \
        --probe-metadata probe_report.v6.metadata.json --out evals/e0/data

The committed file is pinned in ``evals/e0/MANIFEST.sha256``; re-extraction is
a benchmark version change.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import numpy as np

STRUCTURE_NAME = "vs1_v7_technology_structure"
DISCOVERY_START = date(2011, 10, 1)
DISCOVERY_END_EXCLUSIVE = date(2020, 1, 1)
TRIALS = ("A90|fwd5", "A30|fwd5", "A90|fwd20", "A30|fwd20")
EXPECTED_ADMITTED_ISSUERS = 202


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("form4", "submissions", "issuer-map", "sic-map", "probe-metadata", "out"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args(argv)

    from analysis import panel_insider_density as v1
    from analysis import panel_insider_density_v6 as v6

    probe = json.loads(args.probe_metadata.read_text(encoding="utf-8"))
    admitted = set(probe["admitted"])
    sector_map = v1.load_sector_map()
    issuer_map = v6.load_issuer_map(args.issuer_map)
    sic_map = v6.load_sic_map(args.sic_map)
    universe, _ = v6.v2_universe(sector_map, issuer_map, sic_map)
    selected = universe[universe["ticker"].isin(admitted)].reset_index(drop=True)
    if len(selected) != EXPECTED_ADMITTED_ISSUERS:
        raise SystemExit(f"expected {EXPECTED_ADMITTED_ISSUERS} admitted issuers, got {len(selected)}")
    events, admission = v6.load_inputs(args.form4, args.submissions, universe)
    ciks = selected["cik"].astype(int).tolist()

    sessions = v1.proxy_sessions(DISCOVERY_START, DISCOVERY_END_EXCLUSIVE)
    arrays: dict[str, np.ndarray] = {
        "sessions": np.array([d.isoformat() for d in sessions]),
    }
    summary = {}
    for trial in TRIALS:
        name, horizon = trial.split("|fwd")
        h = int(horizon)
        positions = np.arange(0, len(sessions), h)
        decided = v1.decision_instants([sessions[i] for i in positions])
        panel = v6.feature_panel(events, admission, ciks, decided, name).to_numpy(dtype=float)
        key = trial.replace("|", "_")
        arrays[f"{key}__feature"] = panel.astype(np.float64)
        arrays[f"{key}__positions"] = positions.astype(np.int64)
        finite = np.isfinite(panel)
        summary[trial] = {
            "decisions": int(panel.shape[0]),
            "entities": int(panel.shape[1]),
            "finite_share": float(finite.mean()),
            "nonzero_share_of_finite": float((panel[finite] > 0).mean()),
        }

    args.out.mkdir(parents=True, exist_ok=True)
    npz = args.out / f"{STRUCTURE_NAME}.npz"
    # Rows are ordered by admitted-issuer position; entity identities are dropped.
    np.savez_compressed(npz, **arrays)
    provenance = {
        "structure": STRUCTURE_NAME,
        "contents": "feature geometry only: proxy sessions, decision positions, "
                    "decisions x anonymised-entity insider-buy density (NaN = not admitted)",
        "never_read": ["prices", "returns", "rank IC", "discovery or holdout artefacts"],
        "discovery_window": [DISCOVERY_START.isoformat(), DISCOVERY_END_EXCLUSIVE.isoformat()],
        "trials": list(TRIALS),
        "issuers": EXPECTED_ADMITTED_ISSUERS,
        "proxy_sessions": len(sessions),
        "summary": summary,
        "input_sha256": {
            "form4": _sha256(args.form4),
            "submissions": _sha256(args.submissions),
            "issuer_map": _sha256(args.issuer_map),
            "sic_map": _sha256(args.sic_map),
            "probe_metadata": _sha256(args.probe_metadata),
        },
        "rebuilt_with": "analysis.panel_insider_density_v6.load_inputs + feature_panel (v7 Stage-0 path)",
    }
    (args.out / f"{STRUCTURE_NAME}.provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
