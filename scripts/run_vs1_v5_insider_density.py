"""VS1 v5: v4 + source filtering, pull-batch splice check and ticker-reuse price bound (panel harness CLI).

Pre-registration: ``docs/paper_log/vs1-insider-density-v5-preregistration.md``
(body sha256 pinned in ``analysis.panel_insider_density_v5.PREREG_BODY_SHA256``).
Stages, inputs and one-shot rules are those of ``scripts/run_vs1_v2_insider_density.py``
(the same code, driven by the v5 harness): its own registry
(``granular_panel_prereg_v5.jsonl``) and its own pinned off-host witness
(``05-GRID/Paper-Log/vs1/granular_panel_prereg_v5.anchors.jsonl`` on vault ``main``).

    python -m scripts.run_vs1_v5_insider_density hash-prereg
    python -m scripts.run_vs1_v5_insider_density register --log-dir DIR
    # post-admission Stage-0 (binding, v5 §10): on the manifest's price-admitted issuers
    python -m scripts.run_vs1_v5_insider_density power --form4 ... --submissions ... \
        --issuer-map company_tickers.json --sic-map issuer_sic_map.jsonl \
        --price-manifest M --probe-report P --crosscheck-report C --tiingo-meta-report T --out NEW_DIR
    python -m scripts.run_vs1_v5_insider_density freeze-inputs ... --price-manifest M --probe-report P \
        --crosscheck-report C --tiingo-meta-report T --power NEW_DIR/power.json --as-of-ts <probe snapshot> --code-sha SHA
    # open-holdout additionally needs --holdout-probe-report (the 2020+ basis report, v5 §8)
    (then open-discovery / discover / open-holdout / holdout / export-anchors as in the v2 CLI)

No DB writes; prices only through ``store.observations.read_window`` behind a v5 key.
"""

from __future__ import annotations

import sys

from analysis import panel_insider_density_v5 as v5
from scripts.run_vs1_v2_insider_density import main as _main


def main(argv: list[str] | None = None) -> None:
    _main(argv, h=v5)


if __name__ == "__main__":
    main(sys.argv[1:])
