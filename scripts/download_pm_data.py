#!/usr/bin/env python3
"""
Download and extract the Jon Becker prediction market dataset.

36GB compressed archive containing 7.68M markets and 72.1M trades
from Polymarket and Kalshi in Parquet format.

Usage:
    python scripts/download_pm_data.py
    python scripts/download_pm_data.py --data-dir /custom/path
"""

from __future__ import annotations

import argparse
import subprocess
import tarfile
from pathlib import Path

URL = "https://s3.jbecker.dev/data.tar.zst"
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "prediction_markets"


def download_and_extract(data_dir: Path) -> None:
    sentinel = data_dir / ".download_complete"
    if sentinel.exists():
        print(f"Data already downloaded at {data_dir}")
        return

    data_dir.mkdir(parents=True, exist_ok=True)
    archive_path = data_dir / "data.tar.zst"

    # Download
    print(f"Downloading prediction market dataset from {URL}")
    print(f"Destination: {data_dir}")
    print("This is ~36GB, please be patient...")
    print()

    try:
        subprocess.run(
            ["curl", "-L", "--progress-bar", "-o", str(archive_path), URL],
            check=True,
        )
    except subprocess.CalledProcessError:
        print("curl failed, trying wget...")
        subprocess.run(
            ["wget", "--progress=bar:force", "-O", str(archive_path), URL],
            check=True,
        )

    print(f"\nDownloaded: {archive_path.stat().st_size / (1024**3):.1f} GB")

    # Extract using Python zstandard
    print("Extracting archive (this may take a while)...")
    try:
        import zstandard as zstd

        with open(archive_path, "rb") as compressed:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(compressed) as reader:
                with tarfile.open(fileobj=reader, mode="r|") as tar:
                    tar.extractall(path=str(data_dir))
    except ImportError:
        print("zstandard not installed, trying CLI zstd...")
        zstd_proc = subprocess.Popen(
            ["zstd", "-d", str(archive_path), "--stdout"],
            stdout=subprocess.PIPE,
        )
        try:
            tar_result = subprocess.run(
                ["tar", "-xf", "-", "-C", str(data_dir)],
                stdin=zstd_proc.stdout,
                check=False,
            )
        finally:
            if zstd_proc.stdout is not None:
                zstd_proc.stdout.close()
            zstd_rc = zstd_proc.wait()
        if zstd_rc != 0:
            raise subprocess.CalledProcessError(zstd_rc, ["zstd", "-d", str(archive_path), "--stdout"])
        if tar_result.returncode != 0:
            raise subprocess.CalledProcessError(tar_result.returncode, tar_result.args)

    # Move nested data/ up if present
    nested = data_dir / "data"
    if nested.is_dir():
        for item in nested.iterdir():
            target = data_dir / item.name
            if not target.exists():
                item.rename(target)
        nested.rmdir()

    # Cleanup
    if archive_path.exists():
        archive_path.unlink()

    sentinel.touch()
    print(f"\nData directory ready: {data_dir}")

    # Print structure
    for platform in ["kalshi", "polymarket"]:
        pdir = data_dir / platform
        if pdir.exists():
            for subdir in sorted(pdir.iterdir()):
                if subdir.is_dir():
                    files = list(subdir.glob("*.parquet"))
                    total_size = sum(f.stat().st_size for f in files)
                    print(f"  {platform}/{subdir.name}/: {len(files)} files, {total_size / (1024**2):.0f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download prediction market dataset")
    parser.add_argument(
        "--data-dir", type=Path, default=DEFAULT_DATA_DIR,
        help=f"Output directory (default: {DEFAULT_DATA_DIR})",
    )
    args = parser.parse_args()
    download_and_extract(args.data_dir)
