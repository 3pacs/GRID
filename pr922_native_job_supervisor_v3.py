"""ENABLED-BYTES REVIEW PROPOSAL. DO NOT RUN OR PUBLISH without admission.

Before admission this exact file and workflow, process/socket attestation and
failure paths require independent review and explicit execution authorization.
"""
import asyncio
import hashlib
import json
import os
import pathlib
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

NATIVE_EXECUTION_ENABLED = True  # REVIEW BYTES ONLY: not execution authority
PINNED_COMMIT = "94c481f84c47a5179df26395f690f1d2e38e041b"
PINNED_TREE = "a7175ca730e7b2ffd82b4da5fc013ee5b7f9cd48"
MANIFEST = "pr922-native-v3-manifest.json"
SOURCE_PATHS = {"pr922_native_job_supervisor_v3.py", "pr922_native_job_contract_v3.py",
                ".github/workflows/python-floor-qualification.yml"}
PG_BIN = pathlib.Path("/usr/lib/postgresql/16/bin")
PG_NAMES = ("initdb", "pg_ctl", "postgres", "psql")


def parse_pg_hashes(text):
    """Accept exactly one preflight digest for each prescribed absolute path."""
    parsed = {}
    for line in text.splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (/usr/lib/postgresql/16/bin/(?:initdb|pg_ctl|postgres|psql))", line)
        if not match:
            raise RuntimeError("invalid preflight hash record")
        digest, path = match.groups()
        name = pathlib.PurePosixPath(path).name
        if name in parsed:
            raise RuntimeError("duplicate preflight binary")
        parsed[name] = digest
    if set(parsed) != set(PG_NAMES):
        raise RuntimeError("incomplete preflight binary record")
    return parsed


def verify_pg_tool(name, hashes):
    if name not in PG_NAMES or name not in hashes:
        raise RuntimeError("unapproved PostgreSQL executable")
    path = PG_BIN / name
    if not path.is_file() or not os.access(path, os.X_OK):
        raise RuntimeError("PostgreSQL binary unavailable")
    if hashlib.sha256(path.read_bytes()).hexdigest() != hashes[name]:
        raise RuntimeError("PostgreSQL binary changed since preflight")
    return str(path)


def pg_call(name, args, hashes, *, data=None, timeout=15):
    # The selected tool and server bytes are rechecked before each command.
    # These checks are not atomic against same-user filesystem replacement.
    executable = verify_pg_tool(name, hashes)
    if name in ("initdb", "pg_ctl"):
        verify_pg_tool("postgres", hashes)
    call([executable, *args], data=data, timeout=timeout)


def verify_checkout():
    """Check a controller-pinned commit and manifest, not authorization itself.

    The controller must independently approve the exact commit and manifest
    hash. An input/environment variable is not a trust anchor by itself.
    """
    expected = os.environ.get("PR922_APPROVED_COMMIT", "")
    if not re.fullmatch(r"[0-9a-f]{40}", expected):
        raise RuntimeError("exact reviewed commit missing")
    root = pathlib.Path(os.environ["GITHUB_WORKSPACE"]).resolve(strict=True)
    if pathlib.Path.cwd().resolve(strict=True) != root or pathlib.Path(__file__).resolve(strict=True) != root / "pr922_native_job_supervisor_v3.py":
        raise RuntimeError("unexpected checkout path")
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=root, text=True, stderr=subprocess.DEVNULL, timeout=5).strip()
    if git("rev-parse", "HEAD") != expected or git("rev-parse", PINNED_COMMIT + "^{tree}") != PINNED_TREE:
        raise RuntimeError("checkout or candidate mismatch")
    changed = set(git("diff", "--name-only", "--no-renames", PINNED_COMMIT, "HEAD").splitlines())
    if changed != SOURCE_PATHS | {MANIFEST}:
        raise RuntimeError("unexpected change paths")
    mpath = root / MANIFEST
    if mpath.is_symlink() or mpath.resolve(strict=True) != mpath:
        raise RuntimeError("unexpected manifest path")
    manifest = json.loads(mpath.read_text(encoding="utf-8"))
    if set(manifest) != SOURCE_PATHS:
        raise RuntimeError("unexpected manifest members")
    for name, expected_hash in manifest.items():
        path = root / name
        if path.is_symlink() or path.resolve(strict=True) != path or not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
            raise RuntimeError("unexpected source path")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
            raise RuntimeError("source hash mismatch")
    if git("status", "--porcelain", "--untracked-files=all", "--ignored=matching"):
        raise RuntimeError("checkout contains modified, untracked or ignored files")


def process_start(pid):
    stat = pathlib.Path("/proc", str(pid), "stat").read_text()
    return stat[stat.rfind(")") + 2:].split()[19]


def listener_inode(pid, port):
    # Postmaster owns the listening descriptor; require IPv4 loopback and
    # ensure this process owns exactly one matching listening socket inode.
    owned = set()
    for fd in pathlib.Path("/proc", str(pid), "fd").iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        if target.startswith("socket:["):
            owned.add(target[8:-1])
    found = []
    for line in pathlib.Path("/proc/net/tcp").read_text().splitlines()[1:]:
        f = line.split()
        address, hexport = f[1].split(":")
        if f[3] == "0A" and int(hexport, 16) == port and f[9] in owned:
            if address != "0100007F":
                raise RuntimeError("nonloopback listener")
            found.append(f[9])
    if len(found) != 1:
        raise RuntimeError("listener ownership unknown")
    return found[0]


def postgres_processes_remain(executable):
    """Conservative scan: a matching same-user PostgreSQL process is UNKNOWN."""
    for proc in pathlib.Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            if proc.stat().st_uid == os.getuid() and str((proc / "exe").resolve(strict=True)) == executable:
                return True
        except FileNotFoundError:  # process exited during scan
            continue
    return False


class Context(SimpleNamespace):
    def attest(self):
        verify_work(self.work, self.work_identity, self.root_identity)
        if (os.getuid() == 0 or process_start(self.pid) != self.process_start or
                os.stat("/proc/" + str(self.pid)).st_uid != os.getuid() or
                str(pathlib.Path("/proc", str(self.pid), "exe").resolve(strict=True)) != self.postgres_exe or
                listener_inode(self.pid, self.port) != self.inode):
            raise RuntimeError("owned endpoint identity unknown")
        data = pathlib.Path(self.data_dir)
        if data.is_symlink() or str(data.resolve(strict=True)) != self.data_dir:
            raise RuntimeError("data directory identity changed")
        if data.stat().st_ino != self.data_inode:
            raise RuntimeError("data directory replaced")


def identity(stat):
    return (stat.st_dev, stat.st_ino, stat.st_uid)


def verify_work(path, expected, root_expected):
    work = pathlib.Path(path)
    root = work.parent
    if (work.is_symlink() or root.is_symlink() or
            str(work.resolve(strict=True)) != str(work) or
            tuple(identity(work.stat())) != tuple(expected) or
            tuple(identity(root.stat())) != tuple(root_expected)):
        raise RuntimeError("work directory identity unknown")


def remove_verified_work(path, expected, root_expected):
    """Remove only witnessed job-owned directory; ambiguity raises UNKNOWN."""
    if not getattr(shutil.rmtree, "avoids_symlink_attacks", False):
        raise RuntimeError("safe removal unavailable")
    work = pathlib.Path(path)
    verify_work(work, expected, root_expected)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    rootfd = os.open(str(work.parent), flags)
    try:
        if identity(os.fstat(rootfd)) != tuple(root_expected):
            raise RuntimeError("temp root replaced")
        childfd = os.open(work.name, flags, dir_fd=rootfd)
        try:
            if identity(os.fstat(childfd)) != tuple(expected):
                raise RuntimeError("job directory replaced")
            if identity(os.stat(work.name, dir_fd=rootfd, follow_symlinks=False)) != tuple(expected):
                raise RuntimeError("job directory changed")
            shutil.rmtree(work.name, dir_fd=rootfd)
        finally:
            os.close(childfd)
    finally:
        os.close(rootfd)


def cleanup_choice(has_work_identity, setup_started, launch_attempted, has_context):
    if not has_work_identity:
        return "UNKNOWN"
    if has_context:
        return "STOP_AND_REMOVE"
    if launch_attempted:
        return "RECOVER_WITNESS"
    if not setup_started:
        return "REMOVE_EMPTY"
    # A failed initdb can have spawned children. Do not delete blindly.
    return "UNKNOWN"


def call(argv, *, data=None, timeout=15):
    # No inherited PG* overrides, shell interpolation, secret arguments, or
    # raw output/error printing. Input may contain the generated test password.
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    result = subprocess.run(argv, input=data, text=True, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, env=env, timeout=timeout)
    if result.returncode:
        raise RuntimeError("owned setup command failed")


def worker():
    # Anonymous stdin only, no secrets in argv or receipt. Parent bounds this
    # whole worker process separately from the reserved cleanup period.
    raw = json.load(sys.stdin)
    ctx = Context(**raw)
    ctx.started = None
    ctx.contract_pass = False
    ctx.client_cleanup = None
    ctx.observed_stale_listening = None
    try:
        from pr922_native_job_contract_v3 import run_contract
        asyncio.run(run_contract(ctx))
    except BaseException:
        pass
    result = {"contract": "PASS" if ctx.contract_pass else "FAIL",
              "client_cleanup": ctx.client_cleanup if ctx.client_cleanup is not None else [["cleanup", "UNKNOWN"]],
              "stale_listening_observed": ctx.observed_stale_listening}
    print(json.dumps(result), flush=True)


def main():
    if not NATIVE_EXECUTION_ENABLED:
        raise SystemExit("native execution disabled pending independent review and authorization")
    verify_checkout()
    if sys.argv[1:] == ["--worker"]:
        worker()
        return
    if sys.argv[1:]:
        raise SystemExit("arguments refused")
    if sys.platform != "linux" or os.getuid() == 0:
        raise SystemExit("Linux non-root disposable job required")
    # Presence of these flags is not attestation. Controller must separately
    # approve this exact single-use hosted runner and workflow at action time.
    if os.environ.get("GITHUB_ACTIONS") != "true" or not os.environ.get("RUNNER_TEMP"):
        raise SystemExit("hosted runner admission missing")
    preflight_hashes = parse_pg_hashes((pathlib.Path(os.environ["RUNNER_TEMP"]) / "grid922-pg16-sha256.txt").read_text(encoding="utf-8"))
    for name in PG_NAMES:
        verify_pg_tool(name, preflight_hashes)
    postgres_exe = (PG_BIN / "postgres").resolve(strict=True)
    temp_root = pathlib.Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    root_identity = identity(temp_root.stat())
    work = None
    work_identity = None
    setup_started = False
    launch_attempted = False
    started_at = time.monotonic()
    setup_elapsed_seconds = None
    state = "UNKNOWN"
    postmaster_stopped = "UNKNOWN"
    postgres_executable_scan = "NOT_RUN"
    descendant_cleanup = "UNKNOWN"
    directory_cleanup = "RETAINED_UNKNOWN"
    worker_result = {"contract": "UNRUN", "client_cleanup": []}
    ctx = None
    try:
        # The finalizer covers allocation, initdb and launch from inception.
        # If allocation returns but its identity cannot be captured, do not
        # guess which path is safe to delete.
        work = pathlib.Path(tempfile.mkdtemp(prefix="grid922-", dir=str(temp_root)))
        work_identity = identity(work.stat())
        verify_work(work, work_identity, root_identity)
        os.chmod(work, 0o700)
        data = work / "data"
        sock = work / "socket"; sock.mkdir(mode=0o700)
        token = secrets.token_hex(8)
        role, admin, database = "grid922_" + token, "boot922_" + token, "grid922_" + token
        password = secrets.token_hex(32)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0)); port = probe.getsockname()[1]
        # Port reservation closes before postgres binds: exclusive fresh VM
        # and post-start attestation are essential; ambiguity is UNKNOWN.
        setup_started = True
        pg_call("initdb", ["-D", str(data), "--username=" + admin,
                "--auth-local=trust", "--auth-host=scram-sha-256", "--no-locale"], preflight_hashes, timeout=45)
        data_inode = data.stat().st_ino
        opts = "-h 127.0.0.1 -p " + str(port) + " -k " + shlex.quote(str(sock))
        launch_attempted = True
        pg_call("pg_ctl", ["-D", str(data), "-l", str(work / "server.log"),
                "-p", str(PG_BIN / "postgres"), "-o", opts, "-w", "-t", "10", "start"], preflight_hashes, timeout=15)
        pid = int((data / "postmaster.pid").read_text().splitlines()[0])
        ctx = Context(pid=pid, process_start=process_start(pid),
                      inode=listener_inode(pid, port), postgres_exe=str(postgres_exe),
                      data_dir=str(data.resolve(strict=True)),
                      data_inode=data_inode, work=str(work), work_identity=work_identity,
                      root_identity=root_identity, port=port, role=role, password=password,
                      database=database, admin=admin, socket_dir=str(sock), token=token,
                      app="grid922_" + token)
        ctx.attest()
        # Values are generated hex identifiers and password, never user input.
        sql = ("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "';\n" +
               "CREATE DATABASE " + database + " OWNER " + role + ";\n")
        pg_call("psql", ["-X", "-v", "ON_ERROR_STOP=1", "-h", str(sock), "-p", str(port),
                "-U", admin, "-d", "postgres"], preflight_hashes, data=sql, timeout=10)
        ctx.attest()
        # This check runs only after setup returns; it is not an independent
        # 90-second watchdog and cannot interrupt hung in-process setup.
        setup_elapsed_seconds = time.monotonic() - started_at
        if setup_elapsed_seconds > 90:
            raise RuntimeError("setup deadline exceeded; reserve cleanup")
        # The worker may be forcibly stopped at its deadline. Until a complete
        # sanitized receipt is returned, client teardown is UNKNOWN.
        worker_result = {"contract": "UNKNOWN", "client_cleanup": [["worker", "UNKNOWN"]]}
        result = subprocess.run([sys.executable, __file__, "--worker"],
                                input=json.dumps(vars(ctx)), text=True, capture_output=True,
                                timeout=45)
        if result.returncode == 0:
            worker_result = json.loads(result.stdout)
        # Do not print raw worker output, stderr, postgres logs, or secrets.
    except BaseException:
        pass
    finally:
        # If pg_ctl timed out after spawning the server, recover a witness only
        # from the original, still-verified work directory and data inode. Any
        # missing/ambiguous evidence means no signal and UNKNOWN.
        decision = cleanup_choice(work_identity is not None, setup_started, launch_attempted, ctx is not None)
        if decision == "RECOVER_WITNESS":
            try:
                verify_work(work, work_identity, root_identity)
                if data.stat().st_ino != data_inode:
                    raise RuntimeError("data changed during startup")
                pid = int((data / "postmaster.pid").read_text().splitlines()[0])
                ctx = Context(pid=pid, process_start=process_start(pid), inode=listener_inode(pid, port),
                              postgres_exe=str(postgres_exe), data_dir=str(data.resolve(strict=True)),
                              data_inode=data_inode, work=str(work), work_identity=work_identity,
                              root_identity=root_identity, port=port)
                ctx.attest()
            except BaseException:
                ctx = None
        decision = cleanup_choice(work_identity is not None, setup_started, launch_attempted, ctx is not None)
        if decision == "STOP_AND_REMOVE":
            try:
                ctx.attest()
                pg_call("pg_ctl", ["-D", str(data), "-w", "-t", "10", "-m", "fast", "stop"], preflight_hashes, timeout=15)
                # Existence of the same PID, including a reused PID, means
                # ownership/exit is unresolved and must not be treated as safe.
                if pathlib.Path("/proc", str(ctx.pid)).exists():
                    raise RuntimeError("process exit unknown")
                postmaster_stopped = "PASS"
                postgres_executable_scan = "UNKNOWN"
                if postgres_processes_remain(ctx.postgres_exe):
                    postgres_executable_scan = "MATCH_FOUND"
                else:
                    postgres_executable_scan = "NO_MATCH_OBSERVED"
                # Scanning one executable/UID is not descendant accounting.
                # Do not delete the directory or report complete teardown.
                descendant_cleanup = "UNKNOWN"
                directory_cleanup = "RETAINED_UNKNOWN"
                state = "UNKNOWN"
            except BaseException:
                state = "UNKNOWN"
        elif decision == "REMOVE_EMPTY":
            try:
                remove_verified_work(work, work_identity, root_identity)
                postmaster_stopped = "NOT_STARTED"
                descendant_cleanup = "NOT_STARTED"
                directory_cleanup = "PASS"
                state = "PASS"
            except BaseException:
                state = "UNKNOWN"
        # A failed start may have created a server even without a witness;
        # leave all state intact and explicitly record UNKNOWN.
        print(json.dumps({"candidate": PINNED_COMMIT, "tree": PINNED_TREE,
                          "contract": worker_result.get("contract", "UNRUN"),
                          "client_cleanup": worker_result.get("client_cleanup", []),
                          "stale_listening_observed": worker_result.get("stale_listening_observed"),
                          "setup_limit_mode": "POST_SETUP_CHECK",
                          "setup_elapsed_seconds": setup_elapsed_seconds,
                          "postmaster_stopped": postmaster_stopped,
                          "postgres_executable_scan": postgres_executable_scan,
                          "descendant_cleanup": descendant_cleanup,
                          "directory_cleanup": directory_cleanup,
                          "cluster_cleanup": state}), flush=True)
    if state != "PASS" or worker_result.get("contract") != "PASS" or not worker_result.get("client_cleanup") or any(
        item[1] != "PASS" for item in worker_result.get("client_cleanup", [])):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
