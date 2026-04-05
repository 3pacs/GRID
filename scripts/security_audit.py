#!/usr/bin/env python3
"""
GRID Security Audit — scan Python dependencies for known CVEs.

Sources:
  - PyPI Advisory Database (OSV format, pip-audit default)
  - GitHub Advisory Database (via pip-audit)

Run daily. Alerts on CRITICAL/HIGH CVEs. Logs all findings.

Usage:
    python scripts/security_audit.py              # scan and report
    python scripts/security_audit.py --fix        # auto-fix safe upgrades
    python scripts/security_audit.py --json       # JSON output
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

LOG_DIR = Path("/data/grid/logs")
LOG_FILE = LOG_DIR / "security_audit.log"

# CVEs we've reviewed and accepted the risk on (with reason)
SUPPRESSED = {
    # "PYSEC-2021-421": "babel 2.8 — only used for locale formatting, not user-facing",
}


def run_audit(fix: bool = False) -> dict:
    """Run pip-audit and return structured results."""
    cmd = ["pip-audit", "--format", "json", "--progress-spinner", "off"]
    if fix:
        cmd.append("--fix")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"error": result.stderr, "dependencies": []}

    return data


def analyze(data: dict) -> dict:
    """Analyze audit results, classify severity."""
    deps = data.get("dependencies", [])
    vulnerable = [d for d in deps if d.get("vulns")]

    critical = []
    high = []
    medium = []
    suppressed = []

    for dep in vulnerable:
        name = dep["name"]
        version = dep["version"]
        for v in dep["vulns"]:
            vuln_id = v["id"]
            fix_versions = v.get("fix_versions", [])

            if vuln_id in SUPPRESSED:
                suppressed.append({"id": vuln_id, "pkg": name, "reason": SUPPRESSED[vuln_id]})
                continue

            entry = {
                "id": vuln_id,
                "pkg": f"{name}=={version}",
                "fix": fix_versions,
            }

            # Classify by package criticality to GRID
            if name in ("requests", "pyjwt", "pyopenssl", "cryptography", "curl-cffi"):
                critical.append(entry)
            elif name in ("twisted", "setuptools", "pip", "idna"):
                high.append(entry)
            else:
                medium.append(entry)

    return {
        "scanned": len(deps),
        "vulnerable_packages": len(vulnerable),
        "critical": critical,
        "high": high,
        "medium": medium,
        "suppressed": suppressed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    fix = "--fix" in sys.argv
    as_json = "--json" in sys.argv

    data = run_audit(fix=fix)
    report = analyze(data)

    if as_json:
        print(json.dumps(report, indent=2))
        return

    ts = report["timestamp"]
    print(f"\n{'='*60}")
    print(f"GRID Security Audit — {ts}")
    print(f"{'='*60}")
    print(f"Packages scanned: {report['scanned']}")
    print(f"Vulnerable: {report['vulnerable_packages']}")
    print()

    if report["critical"]:
        print(f"CRITICAL ({len(report['critical'])}):")
        for v in report["critical"]:
            print(f"  {v['id']}  {v['pkg']}  fix: {v['fix']}")

    if report["high"]:
        print(f"\nHIGH ({len(report['high'])}):")
        for v in report["high"]:
            print(f"  {v['id']}  {v['pkg']}  fix: {v['fix']}")

    if report["medium"]:
        print(f"\nMEDIUM ({len(report['medium'])}):")
        for v in report["medium"]:
            print(f"  {v['id']}  {v['pkg']}  fix: {v['fix']}")

    if report["suppressed"]:
        print(f"\nSUPPRESSED ({len(report['suppressed'])}):")
        for s in report["suppressed"]:
            print(f"  {s['id']}  {s['pkg']}  reason: {s['reason']}")

    total = len(report["critical"]) + len(report["high"]) + len(report["medium"])
    print(f"\nTotal actionable: {total}")

    if report["critical"]:
        print("\n*** ACTION REQUIRED: Critical vulnerabilities found ***")

    # Log to file
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(report) + "\n")


if __name__ == "__main__":
    main()
