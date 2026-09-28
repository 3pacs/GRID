"""VS1 v3: SIC-expanded Technology insider buy density vs XLK, primary trial A90|fwd5 (panel harness CLI).

Pre-registration: ``docs/paper_log/vs1-insider-density-v3-preregistration.md``
(body sha256 pinned in ``analysis.panel_insider_density_v3.PREREG_BODY_SHA256``).
Stages, inputs and one-shot rules are those of ``scripts/run_vs1_v2_insider_density.py``
(the same code, driven by the v3 harness): its own registry
(``granular_panel_prereg_v3.jsonl``) and its own pinned off-host witness
(``05-GRID/Paper-Log/vs1/granular_panel_prereg_v3.anchors.jsonl`` on vault ``main``).

    python -m scripts.run_vs1_v3_insider_density hash-prereg
    python -m scripts.run_vs1_v3_insider_density register --log-dir DIR
    python -m scripts.run_vs1_v3_insider_density power --form4 ... --submissions ... \
        --issuer-map company_tickers.json --sic-map issuer_sic_map.jsonl --out NEW_DIR
    python -m scripts.run_vs1_v3_insider_density freeze-inputs ... --price-manifest ... --probe-report ... \
        --power NEW_DIR/power.json --as-of-ts ISO --code-sha SHA
    (then open-discovery / discover / open-holdout / holdout / export-anchors as in the v2 CLI)

No DB writes; prices only through ``store.observations.read_window`` behind a v3 key.
"""

from __future__ import annotations

import sys

from analysis import panel_insider_density_v3 as v3
from scripts.run_vs1_v2_insider_density import main as _main


def main(argv: list[str] | None = None) -> None:
    _main(argv, h=v3)


if __name__ == "__main__":
    main(sys.argv[1:])
