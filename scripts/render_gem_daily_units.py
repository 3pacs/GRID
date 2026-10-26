#!/usr/bin/env python3
"""Render the GEM daily systemd drop-ins for one immutable pin SHA.

``python scripts/render_gem_daily_units.py <40-hex-sha> <out_dir>`` writes:

* ``grid-options-puller.service.d/50-grid652-immutable-pin.conf``
* ``grid-options-puller.service.d/99-grid652-quarantine.conf``
* ``grid-options-puller.timer.d/50-gem-daily.conf``

It only renders files; installing them under /etc/systemd/system and
enabling the timer is the (separately authorized) activation step.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_TEMPLATES = Path(__file__).resolve().parents[1] / "deploy" / "systemd" / "grid-options-puller.gem-daily"
_SHA = re.compile(r"[0-9a-f]{40}")


def render(pin_sha: str, out_dir: Path) -> list[Path]:
    if not _SHA.fullmatch(pin_sha):
        raise ValueError("pin must be a full 40-hex lowercase commit SHA")
    service_d = out_dir / "grid-options-puller.service.d"
    timer_d = out_dir / "grid-options-puller.timer.d"
    service_d.mkdir(parents=True, exist_ok=True)
    timer_d.mkdir(parents=True, exist_ok=True)
    pin = (_TEMPLATES / "50-grid652-immutable-pin.conf.template").read_text(encoding="utf-8")
    rendered = pin.replace("@PIN_SHA@", pin_sha)
    if "@" in rendered:
        raise ValueError("unrendered placeholder left in pin drop-in")
    outputs = {
        service_d / "50-grid652-immutable-pin.conf": rendered,
        service_d / "99-grid652-quarantine.conf":
            (_TEMPLATES / "99-grid652-quarantine.conf").read_text(encoding="utf-8"),
        timer_d / "50-gem-daily.conf":
            (_TEMPLATES / "timer-50-gem-daily.conf").read_text(encoding="utf-8"),
    }
    for path, content in outputs.items():
        path.write_text(content, encoding="utf-8", newline="\n")
    return list(outputs)


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__)
        return 2
    for path in render(argv[1], Path(argv[2])):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
