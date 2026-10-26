"""S11 synthetic dry runs: python -m scripts.demo_ledger_steered_exploration NEW_OUTPUT_DIRECTORY

Writes two new ledgers (files only, no DB): ``planted/`` (consecutive runs with
one planted-signal family) and ``noise/`` (runs of pure noise). Prints each
run's allocation per family and the cumulative discoveries. SYNTHETIC ONLY.
"""

import argparse
from pathlib import Path

from analysis.ledger_steered_exploration import run_synthetic_dry_runs


def show(title: str, summary: dict) -> None:
    steps = summary["steps"]
    print(f"== {title} ==")
    keys = sorted(steps[0]["counts"])
    print("family".ljust(28) + "".join(f"{'run' + str(i + 1):>6}" for i in range(len(steps))))
    for key in keys:
        print(key.ljust(28) + "".join(f"{s['counts'][key]:>6}" for s in steps))
    print(
        "trials".ljust(28) + "".join(f"{sum(s['counts'].values()):>6}" for s in steps)
    )
    ledger = summary["ledger"]
    print(
        f"alpha spent {ledger['alpha_spent']:.4f} of q={ledger['q']}; "
        f"discoveries {ledger['discoveries']}; holdout survivors "
        f"{ledger['holdout_survivors']}; ledger head {ledger['head_sha256'][:12]}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("--runs", type=int, default=2)
    parser.add_argument("--noise-runs", type=int, default=20)
    parser.add_argument("--perms-cap", type=int, default=2999)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    show(
        "planted signal: alpha::T1",
        run_synthetic_dry_runs(output / "planted", runs=args.runs, perms_cap=args.perms_cap),
    )
    show(
        "pure noise",
        run_synthetic_dry_runs(
            output / "noise", runs=args.noise_runs, planted=None, perms_cap=args.perms_cap
        ),
    )
    print("SYNTHETIC ONLY: files + ledger; no weights, promotion or registry writes")
