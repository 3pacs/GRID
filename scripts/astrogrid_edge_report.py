#!/usr/bin/env python3
"""Reproduce the frozen Astrogrid historical evaluation; never writes the DB."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def download(cache: Path) -> list[dict]:
    cache.mkdir(parents=True, exist_ok=True)
    months = ["2025-11", "2025-12"] + [f"2026-{m:02d}" for m in range(1, 11)]

    def fetch(pair: tuple[str, str]) -> dict:
        symbol, month = pair
        name = f"{symbol}USDT-5m-{month}.zip"
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}USDT/5m/{name}"
        path = cache / name
        with urlopen(url + ".CHECKSUM", timeout=30) as response:
            checksum = response.read().decode().split()[0]
        if not path.exists():
            with urlopen(url, timeout=60) as response:
                path.write_bytes(response.read())
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != checksum:
            raise ValueError(f"Checksum mismatch: {name}")
        return {"symbol": symbol, "month": month, "url": url, "sha256": digest,
                "bytes": path.stat().st_size, "checksum_verified": True}

    with ThreadPoolExecutor(max_workers=3) as pool:
        manifest = list(pool.map(fetch, [(s, m) for s in ("BTC", "ETH", "SOL") for m in months]))
    (cache / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--download-only", action="store_true")
    args = parser.parse_args()
    manifest = download(args.cache)
    if args.download_only:
        print(json.dumps({"archives": len(manifest), "checksums_verified": True}))
        return
    from evaluation.astrogrid_edge import historical_report

    report = historical_report(args.cache, manifest)
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["protocol_sha256"] = hashlib.sha256(
        (Path(__file__).resolve().parents[1] / report["protocol_path"]).read_bytes()).hexdigest()
    if args.output is None:
        parser.error("--output required unless --download-only")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"verdict": report["verdict"], "test": report["test"]["sample"]}))


if __name__ == "__main__":
    main()
