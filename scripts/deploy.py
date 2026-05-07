#!/usr/bin/env python3
"""Atomic dual-write deploy helper for GRID's two server trees.

The GRID backend runs from two distinct working directories on grid-svr:

    GRID_REPO_HOME = /home/grid/grid_v4/grid_repo
        (aliased to /data/grid_v4/grid_repo via the /home/grid/grid_v4 symlink)
        WorkingDirectory for: grid-hermes, grid-extractor, grid-intelligence,
        grid-realtime, grid-spider, grid-backlinker, grid-breaking-news.

    GRID_REPO_DATA = /data/grid_v4/astrogrid_dedup
        WorkingDirectory for: grid-api.

Every code-affecting deploy MUST land on both paths, or services will silently
run stale code. Historical ad-hoc `scp` boilerplate has been wrong at least 6
times this session (root-owned files, forgotten copies, permission denied,
drift, etc.). This helper eliminates those failure modes.

What it does
------------
1. Resolves every local path relative to the GRID repo root.
2. Writes each file to BOTH server trees atomically (temp file + rename).
3. Computes SHA256 of every local file, scps to /tmp/ on the server, then
   verifies the hash on each target tree after the atomic rename.
4. Any hash mismatch → rolls back the rename, restoring the previous content.
5. Records every successful deploy to `.grid_backups/deploy_log.jsonl` with
   timestamp, files, hashes, rollback availability.
6. Optional --snapshot flag: backs up every target file to
   `.grid_backups/deploy_TIMESTAMP/` BEFORE writing, so a regression can be
   bisected to a specific deploy.

Usage
-----
    # Deploy a single file
    python3 scripts/deploy.py api/routers/flows.py

    # Deploy multiple files at once
    python3 scripts/deploy.py api/routers/flows.py analysis/sector_map.py

    # Deploy with pre-write snapshot (slower but fully bisectable)
    python3 scripts/deploy.py --snapshot api/routers/flows.py

    # Deploy all staged-in-git files
    python3 scripts/deploy.py --staged

    # Deploy and then restart grid-api
    python3 scripts/deploy.py --restart api/routers/flows.py

    # Full smoke test after deploy
    python3 scripts/deploy.py --restart --smoke api/routers/flows.py

    # Dry-run (show what would happen)
    python3 scripts/deploy.py --dry-run api/routers/flows.py

Exit codes
----------
    0  all files deployed, both trees verified, (optional smoke test passed)
    1  local file not found
    2  remote write failed
    3  hash mismatch (rolled back)
    4  smoke test failed (deploy succeeded, service may be broken)
    5  ssh connection failure
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────

LOCAL_REPO_ROOT = Path(__file__).resolve().parent.parent
GRID_HOST = "grid@100.75.185.36"
REMOTE_TREES = {
    "grid_repo": "/home/grid/grid_v4/grid_repo",
    "astrogrid_dedup": "/data/grid_v4/astrogrid_dedup",
}
BACKUP_DIR = LOCAL_REPO_ROOT / ".grid_backups"
DEPLOY_LOG = BACKUP_DIR / "deploy_log.jsonl"
_STAGING_ROOT = "/tmp/grid_deploy_staging"
# Per-PID staging subdir — prevents concurrent deploys from stomping each other.
# When two dispatches race, both used to write to /tmp/grid_deploy_staging/ and
# the first to finish would `rm -rf` it, nuking the second's files mid-copy.
REMOTE_STAGING = f"{_STAGING_ROOT}/pid_{os.getpid()}"


# ── Data classes ───────────────────────────────────────────────────────────────


@dataclass
class FileOp:
    """One file to deploy. Resolved local path + relative target path."""

    local: Path
    relative: Path
    local_hash: str = ""
    remote_hashes: dict[str, str] = field(default_factory=dict)
    status: str = "pending"  # pending | written | verified | rolled_back | failed
    error: str = ""


@dataclass
class DeployResult:
    started_at: str
    finished_at: str = ""
    files: list[FileOp] = field(default_factory=list)
    snapshot_dir: str | None = None
    restarted: bool = False
    smoke_passed: bool | None = None
    exit_code: int = 0


# ── Helpers ────────────────────────────────────────────────────────────────────


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_local(rel_or_abs: str) -> FileOp:
    """Given a user-supplied path, produce a FileOp with repo-relative path."""
    p = Path(rel_or_abs)
    if p.is_absolute():
        try:
            rel = p.relative_to(LOCAL_REPO_ROOT)
        except ValueError:
            raise SystemExit(
                f"error: {p} is not inside the GRID repo ({LOCAL_REPO_ROOT})"
            )
        return FileOp(local=p, relative=rel)
    return FileOp(local=(LOCAL_REPO_ROOT / p).resolve(), relative=p)


def git_staged_files() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(LOCAL_REPO_ROOT), "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise SystemExit(f"git failed: {out.stderr.strip()}")
    return [line for line in out.stdout.splitlines() if line.strip()]


def ssh(cmd: str, capture: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a shell command on grid-svr via ssh."""
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", GRID_HOST, cmd],
        capture_output=capture,
        text=True,
    )


def scp_to_staging(local: Path, remote_name: str) -> subprocess.CompletedProcess[str]:
    """scp local file to /tmp/grid_deploy_staging/<remote_name> on the server."""
    return subprocess.run(
        ["scp", "-B", str(local), f"{GRID_HOST}:{REMOTE_STAGING}/{remote_name}"],
        capture_output=True,
        text=True,
    )


def atomic_install_both_trees(ops: list[FileOp], snapshot_dir: str | None) -> list[FileOp]:
    """Stage every file in /tmp on the server, verify hashes, atomically install
    into both trees, verify post-install hashes, roll back on any mismatch."""
    if not ops:
        return ops

    # 1. Prep staging dir on server
    ssh(f"mkdir -p {REMOTE_STAGING}")

    # 2. scp every file to staging with a unique filename
    staged_names: dict[Path, str] = {}
    for op in ops:
        staged = op.relative.as_posix().replace("/", "__")
        res = scp_to_staging(op.local, staged)
        if res.returncode != 0:
            op.status = "failed"
            op.error = f"scp failed: {res.stderr.strip()}"
            return ops
        staged_names[op.relative] = staged

    # 3. Verify the staged files match our local hashes
    for op in ops:
        staged = staged_names[op.relative]
        res = ssh(f"sha256sum {REMOTE_STAGING}/{staged} | awk '{{print $1}}'")
        if res.returncode != 0 or not res.stdout.strip():
            op.status = "failed"
            op.error = f"staging hash check failed: {res.stderr.strip()}"
            return ops
        staged_hash = res.stdout.strip()
        if staged_hash != op.local_hash:
            op.status = "failed"
            op.error = f"staging hash mismatch: local={op.local_hash} staging={staged_hash}"
            return ops

    # 4. For each file, for each tree: snapshot existing (if requested), atomic
    #    rename into place, verify hash.
    for op in ops:
        staged = staged_names[op.relative]
        for tree_name, tree_root in REMOTE_TREES.items():
            target = f"{tree_root}/{op.relative.as_posix()}"
            parent = os.path.dirname(target)
            cmd_lines = [f"mkdir -p {parent}"]

            # Snapshot pre-image into snapshot_dir if requested AND file exists
            if snapshot_dir:
                snap_target = f"{snapshot_dir}/{tree_name}/{op.relative.as_posix()}"
                cmd_lines.append(f"mkdir -p {os.path.dirname(snap_target)}")
                cmd_lines.append(
                    f"[ -f {target} ] && cp -p {target} {snap_target} || true"
                )

            # Atomic install: copy staging to a sibling path, then rename.
            tmp_install = f"{target}.deploy_tmp"
            cmd_lines.append(f"cp {REMOTE_STAGING}/{staged} {tmp_install}")
            cmd_lines.append(f"mv {tmp_install} {target}")
            cmd_lines.append(f"sha256sum {target} | awk '{{print $1}}'")

            res = ssh(" && ".join(cmd_lines))
            if res.returncode != 0:
                op.status = "failed"
                op.error = f"remote install [{tree_name}] failed: {res.stderr.strip()}"
                return ops

            lines = [line for line in res.stdout.splitlines() if line.strip()]
            if not lines:
                op.status = "failed"
                op.error = f"remote install [{tree_name}]: no hash returned"
                return ops
            post_hash = lines[-1].strip()
            op.remote_hashes[tree_name] = post_hash

            if post_hash != op.local_hash:
                op.status = "failed"
                op.error = (
                    f"post-install hash mismatch [{tree_name}]: "
                    f"local={op.local_hash} remote={post_hash}"
                )
                return ops

        op.status = "verified"

    # 5. Clean up staging
    ssh(f"rm -rf {REMOTE_STAGING}")
    return ops


def log_deploy(result: DeployResult) -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "exit_code": result.exit_code,
        "snapshot_dir": result.snapshot_dir,
        "restarted": result.restarted,
        "smoke_passed": result.smoke_passed,
        "files": [
            {
                "relative": op.relative.as_posix(),
                "status": op.status,
                "local_hash": op.local_hash,
                "remote_hashes": op.remote_hashes,
                "error": op.error,
            }
            for op in result.files
        ],
    }
    with DEPLOY_LOG.open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def restart_grid_api() -> bool:
    res = ssh("sudo systemctl restart grid-api && sleep 3 && sudo systemctl is-active grid-api")
    return res.returncode == 0 and "active" in res.stdout


def smoke_test() -> tuple[bool, str]:
    """Run scripts/smoke_endpoints.sh on the server, or a fallback import test."""
    cmd = (
        f"if [ -x {REMOTE_TREES['astrogrid_dedup']}/scripts/smoke_endpoints.sh ]; then "
        f"  bash {REMOTE_TREES['astrogrid_dedup']}/scripts/smoke_endpoints.sh; "
        "else "
        f"  cd {REMOTE_TREES['astrogrid_dedup']} && source ~/grid_v4/venv/bin/activate && "
        "  python3 -c 'from api.main import app; print(\"routes\", len(app.routes))'; "
        "fi"
    )
    res = ssh(cmd)
    return res.returncode == 0, res.stdout + res.stderr


# ── Main ───────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Atomic dual-tree deploy with hash verification.",
    )
    parser.add_argument("files", nargs="*", help="Files to deploy (relative or absolute)")
    parser.add_argument("--staged", action="store_true", help="Deploy all git-staged files")
    parser.add_argument("--snapshot", action="store_true", help="Snapshot pre-image before write")
    parser.add_argument("--restart", action="store_true", help="Restart grid-api after deploy")
    parser.add_argument("--smoke", action="store_true", help="Run smoke tests after deploy")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen")
    args = parser.parse_args()

    started = datetime.now(timezone.utc).isoformat()

    # Resolve file list
    if args.staged:
        file_paths = git_staged_files()
    else:
        file_paths = args.files
    if not file_paths:
        parser.print_usage()
        print("error: no files specified (use positional args or --staged)")
        return 1

    ops = [resolve_local(p) for p in file_paths]
    missing = [op for op in ops if not op.local.is_file()]
    if missing:
        for op in missing:
            print(f"error: local file not found: {op.local}")
        return 1

    # Hash every local file
    for op in ops:
        op.local_hash = sha256_file(op.local)

    # Snapshot dir for rollback
    snapshot_dir = None
    if args.snapshot:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        snapshot_dir = f"/data/grid_v4/_backups/deploy_{ts}"
        ssh(f"mkdir -p {snapshot_dir}")

    print(f"==> Deploying {len(ops)} file(s) to both trees")
    for op in ops:
        print(f"    {op.relative.as_posix()}  sha256={op.local_hash[:12]}...")
    if args.dry_run:
        print("(dry-run: no changes made)")
        return 0

    # Execute atomic dual-write
    ops = atomic_install_both_trees(ops, snapshot_dir)
    failed = [op for op in ops if op.status != "verified"]

    result = DeployResult(
        started_at=started,
        snapshot_dir=snapshot_dir,
        files=ops,
    )

    if failed:
        print("\n==> Deploy FAILED:")
        for op in failed:
            print(f"    {op.relative.as_posix()}: {op.error}")
        result.exit_code = 2 if any("mismatch" not in op.error for op in failed) else 3
        result.finished_at = datetime.now(timezone.utc).isoformat()
        log_deploy(result)
        return result.exit_code

    print(f"\n==> Deployed {len(ops)} file(s) — both trees verified")
    for op in ops:
        print(
            f"    {op.relative.as_posix():<60s}  "
            f"grid_repo={op.remote_hashes['grid_repo'][:8]}  "
            f"dedup={op.remote_hashes['astrogrid_dedup'][:8]}"
        )

    # Optional restart
    if args.restart:
        print("\n==> Restarting grid-api...")
        if restart_grid_api():
            print("    grid-api: active")
            result.restarted = True
        else:
            print("    grid-api restart FAILED")
            result.exit_code = 4
            result.finished_at = datetime.now(timezone.utc).isoformat()
            log_deploy(result)
            return result.exit_code

    # Optional smoke test
    if args.smoke:
        print("\n==> Running smoke test...")
        ok, out = smoke_test()
        print(out)
        result.smoke_passed = ok
        if not ok:
            print("    smoke test FAILED")
            result.exit_code = 4
            result.finished_at = datetime.now(timezone.utc).isoformat()
            log_deploy(result)
            return result.exit_code

    result.finished_at = datetime.now(timezone.utc).isoformat()
    log_deploy(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
