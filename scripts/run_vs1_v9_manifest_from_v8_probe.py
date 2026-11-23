"""Build the deterministic VS1 v9 admitted-price manifest from the bound v8 probe directory.

Reads ONLY the pinned v8 probe report triple (``probe_report.json``, ``crosscheck_report.json``,
``tiingo_meta_report.json``) and the pinned XNYS calendar manifest. Performs NO database read,
NO network access, and NO rerun of the admission probe (owner ruling R9).

    python -m scripts.run_vs1_v9_manifest_from_v8_probe --probe-dir PROBE_DIR --out OUT.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v8 as v8
from analysis import panel_insider_density_v9 as v9


def _check_sha256s_file(probe_dir: Path, expected: dict[str, str]) -> None:
    sha_file = probe_dir / "sha256s.txt"
    if not sha_file.exists():
        return
    parsed: dict[str, str] = {}
    for line in sha_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 2:
            raise PermissionError("probe sha256s.txt has malformed line")
        parsed[parts[1]] = parts[0]
    for name, digest_hex in expected.items():
        if parsed.get(name) != digest_hex:
            raise PermissionError(f"probe sha256s.txt disagrees with pinned hash for {name}")


def build_v9_manifest_from_v8_probe(probe_dir: Path, *, repo_root: Path = v2.REPO) -> dict[str, Any]:
    """Validate the pinned v8 probe triple and derive the deterministic 195-issuer v9 manifest."""
    v9.check_probe_pins()
    probe_dir = Path(probe_dir)
    probe_path = probe_dir / "probe_report.json"
    cross_path = probe_dir / "crosscheck_report.json"
    meta_path = probe_dir / "tiingo_meta_report.json"
    for p in (probe_path, cross_path, meta_path):
        if not p.is_file():
            raise PermissionError(f"missing required v8 probe report: {p.name}")

    probe_sha = v1.data_sha256(probe_path)
    cross_sha = v1.data_sha256(cross_path)
    meta_sha = v1.data_sha256(meta_path)
    if (probe_sha, cross_sha, meta_sha) != (
        v9.PROBE_REPORT_SHA256,
        v9.CROSSCHECK_REPORT_SHA256,
        v9.TIINGO_META_REPORT_SHA256,
    ):
        raise PermissionError("probe directory does not match the pinned v8 probe report triple")
    _check_sha256s_file(
        probe_dir,
        {
            "probe_report.json": probe_sha,
            "crosscheck_report.json": cross_sha,
            "tiingo_meta_report.json": meta_sha,
        },
    )

    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    cross = json.loads(cross_path.read_text(encoding="utf-8"))
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    expected_prereg = {"study": v8.VERSION, "body_sha256": v8.PREREG_BODY_SHA256}
    expected_window = {"start": v9.PROBE_START, "end": v9.PROBE_END}
    for name, report in (("probe", probe), ("crosscheck", cross), ("tiingo_meta", meta)):
        prereg = report.get("prereg") or {}
        if any(prereg.get(k) != val for k, val in expected_prereg.items()):
            raise PermissionError(f"{name} report is not bound to vs1-v8")
        if report.get("window") != "discovery":
            raise PermissionError(f"{name} report is not a discovery probe")
        rw = report.get("read_window") or {}
        if rw.get("start") != expected_window["start"] or rw.get("end") != expected_window["end"]:
            raise PermissionError(f"{name} report window differs from {v9.PROBE_START}..{v9.PROBE_END}")
        if (report.get("source") or {}).get("name") != v9.PRICE_SOURCE:
            raise PermissionError(f"{name} report source is not {v9.PRICE_SOURCE}")
        if report.get("snapshot_as_of_ts") != v9.PROBE_SNAPSHOT_AS_OF_TS:
            raise PermissionError(f"{name} report snapshot differs from pinned {v9.PROBE_SNAPSHOT_AS_OF_TS}")
        if report.get("code_sha") != v9.PROBE_CODE_SHA:
            raise PermissionError(f"{name} report code_sha differs from pinned probe code_sha")

    if probe.get("crosscheck_report_sha256") != cross_sha or probe.get("tiingo_meta_report_sha256") != meta_sha:
        raise PermissionError("probe_report.json internal hashes disagree with sibling report files")
    if probe.get("benchmark") != v9.BENCHMARK or probe.get("benchmark_admitted") is not False:
        raise PermissionError("bound v8 probe must report benchmark XLK with benchmark_admitted=false")
    if (
        probe.get("benchmark_calendar_sessions") != v9.PROBE_CALENDAR_SESSIONS
        or probe.get("benchmark_calendar") != [v9.PROBE_START, v9.PROBE_END]
    ):
        raise PermissionError("bound v8 probe calendar summary differs from 3,061 sessions on 2007-11-02..2019-12-31")

    # Verify the pinned XNYS calendar manifest reproduces the probe's session count and bounds.
    xnys_sessions = v9.load_xnys_calendar(repo_root)
    probe_lo, probe_hi = date.fromisoformat(v9.PROBE_START), date.fromisoformat(v9.PROBE_END)
    probe_span = [d for d in xnys_sessions if probe_lo <= d <= probe_hi]
    if (
        len(probe_span) != v9.PROBE_CALENDAR_SESSIONS
        or probe_span[0] != probe_lo
        or probe_span[-1] != probe_hi
    ):
        raise PermissionError("pinned XNYS calendar does not match the v8 probe session span")

    tickers_map = probe.get("tickers") or {}
    admitted = sorted(
        t for t, rec in tickers_map.items()
        if rec.get("admitted") is True and not rec.get("benchmark") and t != v9.BENCHMARK
    )
    probe_admitted_issuers = sorted(t for t in (probe.get("admitted") or ()) if t != v9.BENCHMARK)
    if admitted != probe_admitted_issuers or len(admitted) != v9.PROBE_ADMITTED_ISSUERS:
        raise PermissionError(
            f"bound v8 probe admitted issuer list mismatch: got {len(admitted)}, expected {v9.PROBE_ADMITTED_ISSUERS}"
        )

    listed_raw = probe.get("listed_from") or {}
    listed_from: dict[str, str] = {}
    selected_rows_sha: dict[str, str] = {}
    for t in admitted:
        rec = tickers_map[t]
        start_date = listed_raw.get(t)
        if not start_date or start_date != rec.get("listed_from"):
            raise PermissionError(f"{t}: missing or inconsistent listed_from in probe report")
        listed_from[t] = str(start_date)
        rows_sha = (rec.get("rows_examined") or {}).get("selected_adj_close_sha256")
        if not v1._is_hex64(rows_sha):
            raise PermissionError(f"{t}: missing valid selected_adj_close_sha256 in probe report")
        selected_rows_sha[t] = rows_sha

    manifest = v9.PriceManifest(
        source=probe["source"]["name"],
        series_template=probe["series_template"],
        basis=probe["basis"],
        benchmark=v9.BENCHMARK,
        benchmark_admitted=False,
        admitted=tuple(admitted),
        probe_report_sha256=probe_sha,
        crosscheck_report_sha256=cross_sha,
        tiingo_meta_report_sha256=meta_sha,
        listed_from=tuple(sorted(listed_from.items())),
        calendar_manifest=v9.XNYS_MANIFEST_PATH.as_posix(),
        calendar_manifest_sha256=v9.XNYS_MANIFEST_SHA256,
        probe_snapshot_as_of_ts=v9.PROBE_SNAPSHOT_AS_OF_TS,
        selected_adj_close_sha256=tuple(sorted(selected_rows_sha.items())),
    )
    manifest.validate()
    return manifest.to_dict()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--probe-dir", required=True, help="bound v8 probe directory (no DB read)")
    parser.add_argument("--out", required=True, help="write-once destination JSON path")
    args = parser.parse_args(argv)

    doc = build_v9_manifest_from_v8_probe(Path(args.probe_dir))
    raw = (json.dumps(doc, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "xb") as stream:
        stream.write(raw)
    loaded = v9.PriceManifest.from_file(out)
    print(json.dumps({
        "out": str(out),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "manifest_digest": loaded.digest(),
        "admitted_issuers": len(loaded.admitted),
        "benchmark_admitted": loaded.benchmark_admitted,
        "probe_snapshot_as_of_ts": loaded.probe_snapshot_as_of_ts,
        "calendar_manifest_sha256": loaded.calendar_manifest_sha256,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
