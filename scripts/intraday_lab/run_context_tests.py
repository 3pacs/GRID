"""Run offline context controls concurrently; passing does not establish an edge."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
LANES = {
    "time_of_day": "tests/test_intraday_time_of_day.py",
    "sector": "tests/test_intraday_sector.py",
    "volatility": "tests/test_volatility_repricing.py",
}


def utc():
    return datetime.now(timezone.utc).isoformat()


def run_lane(name, path, output, timeout=180):
    started = utc()
    command = [
        sys.executable,
        "-B",
        "-m",
        "pytest",
        path,
        "-q",
        "--noconftest",
        "-p",
        "no:cacheprovider",
    ]
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    test_hash = None
    try:
        test_hash = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        result = subprocess.run(
            command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout
        )
        code, log = result.returncode, result.stdout + result.stderr
        status = "PASS" if code == 0 else "FAIL"
    except (OSError, subprocess.TimeoutExpired) as exc:
        code, log, status = None, str(exc), "ERROR"
    log_bytes = log.encode("utf-8")
    (output / f"{name}.log").write_bytes(log_bytes)
    return {
        "lane": name,
        "test_path": path,
        "started_at": started,
        "finished_at": utc(),
        "status": status,
        "returncode": code,
        "test_sha256": test_hash,
        "log_sha256": hashlib.sha256(log_bytes).hexdigest(),
    }


def run_all(output, timeout=180):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    with ThreadPoolExecutor(max_workers=len(LANES)) as executor:
        futures = [
            executor.submit(run_lane, name, path, output, timeout)
            for name, path in LANES.items()
        ]
        receipts = [future.result() for future in futures]
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    receipt = {
        "status": "PASS" if all(r["status"] == "PASS" for r in receipts) else "FAIL",
        "scope": "OFFLINE_SYNTHETIC_CONTROLS_NO_VALIDATED_EDGE",
        "git_head": head.stdout.strip() if head.returncode == 0 else None,
        "python": sys.version,
        "lanes": receipts,
    }
    receipt["source_sha256"] = {
        str(path.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for folder in (ROOT / "scripts/intraday_lab",)
        for path in folder.glob("*.py")
    }
    sector = ROOT / "scripts/intraday_sector.py"
    if sector.exists():
        receipt["source_sha256"]["scripts/intraday_sector.py"] = hashlib.sha256(
            sector.read_bytes()
        ).hexdigest()
    (output / "receipt.json").write_text(
        json.dumps(receipt, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    receipt = run_all(args.output)
    print(json.dumps(receipt, indent=2))
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
