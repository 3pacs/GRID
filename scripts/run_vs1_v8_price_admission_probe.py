"""VS1 v8 admission probe: exact 2007-11-02 window, v6 rules, witnessed v8 registration.

Add --log-dir and --vault-repo to fetch-twelvedata, fetch-tiingo-meta and
probe. This script does not run automatically or open a research outcome.
"""

from __future__ import annotations

import sys
from pathlib import Path

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v8 as v8
from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4
from scripts import run_price_admission_probe as prior
from scripts.run_vs1_v7_insider_density import _drop_pairs, _option
from scripts.run_vs1_v8_insider_density import PROBE_END, PROBE_START, _registered


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("v8 admission command required")
    command = args[0]
    if command in {"fetch-twelvedata", "fetch-tiingo-meta", "probe"}:
        v8.check_prereg()
        _registered(Path(_option(args, "--log-dir")), Path(_option(args, "--vault-repo")))
        args = _drop_pairs(args, "--log-dir", "--vault-repo")
    if command == "probe":
        for name, expected in (("--start", PROBE_START.isoformat()), ("--end", PROBE_END.isoformat())):
            if name in args and _option(args, name) != expected:
                raise PermissionError(f"v8 {name} must equal {expected}")
            if name not in args:
                args.extend((name, expected))
        if "--submissions" not in args or "--sic-map" not in args or "--unpinned-sic-map" in args:
            raise PermissionError("v8 probe needs the pinned SEC names and C1 submissions")
    with v1.discovery_window(v8.DISCOVERY_START), \
            fetch.discovery_vendor_window(PROBE_START.isoformat()), \
            gd4.preregistered_window(PROBE_START.isoformat(), v8.VERSION, v8.PREREG_BODY_SHA256):
        prior.main(args)


if __name__ == "__main__":
    main()
