"""VS1 v4: v3 + TIINGO/TwelveData price admission and a post-admission power gate (panel harness CLI).

Pre-registration: ``docs/paper_log/vs1-insider-density-v4-preregistration.md``
(body sha256 pinned in ``analysis.panel_insider_density_v4.PREREG_BODY_SHA256``).
Stages, inputs and one-shot rules are those of ``scripts/run_vs1_v2_insider_density.py``
(the same code, driven by the v4 harness): its own registry
(``granular_panel_prereg_v4.jsonl``) and its own pinned off-host witness
(``05-GRID/Paper-Log/vs1/granular_panel_prereg_v4.anchors.jsonl`` on vault ``main``).

    python -m scripts.run_vs1_v4_insider_density hash-prereg
    python -m scripts.run_vs1_v4_insider_density register --log-dir DIR
    # post-admission Stage-0 (binding, v4 §10): on the manifest's price-admitted issuers
    python -m scripts.run_vs1_v4_insider_density power --form4 ... --submissions ... \
        --issuer-map company_tickers.json --sic-map issuer_sic_map.jsonl \
        --price-manifest M --probe-report P --crosscheck-report C --out NEW_DIR
    python -m scripts.run_vs1_v4_insider_density freeze-inputs ... --price-manifest M --probe-report P \
        --crosscheck-report C --power NEW_DIR/power.json --as-of-ts <probe snapshot> --code-sha SHA
    # open-holdout additionally needs --holdout-probe-report (the 2020+ basis report, v4 §8)
    (then open-discovery / discover / open-holdout / holdout / export-anchors as in the v2 CLI)

No DB writes; prices only through ``store.observations.read_window`` behind a v4 key.
"""

from __future__ import annotations

import sys

from analysis import panel_insider_density_v4 as v4
from scripts.run_vs1_v2_insider_density import main as _main


def main(argv: list[str] | None = None) -> None:
    _main(argv, h=v4)


if __name__ == "__main__":
    main(sys.argv[1:])
