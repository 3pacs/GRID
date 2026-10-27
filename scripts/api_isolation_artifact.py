"""Executable byte-level producer and receiver; no files, network or subprocesses.

The supplied runtime is a complete independently built archive, never a shared
venv copy. These transforms do not build/install packages or attest native ABI.
Transport and physical inode/ownership verification remain separate interfaces.
"""
import hashlib
import io
import json
import tarfile

from api_bundle_contract import (git_blob, json_hash, relative, require,
                                 verify_projection, verify_bundle)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encode(files):
    """Canonical USTAR: regular members only, reproducible headers, no links."""
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, (mode, data) in sorted(files.items()):
            relative(name)
            require(mode in ("100644", "100755") and isinstance(data, bytes), "unsafe payload")
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), int(mode[-3:], 8)
            info.uid = info.gid = info.mtime = 0
            archive.addfile(info, io.BytesIO(data))
    return output.getvalue()


def decode(raw, expected_sha256, max_bytes, max_members):
    """Authenticate before parsing; reject aliases, specials and ambiguous names.

No extract/extractall call exists. Canonical re-encoding rejects hidden trailing
payloads, PAX/GNU indirection, duplicate headers and noncanonical metadata.
"""
    require(isinstance(raw, bytes) and len(raw) <= max_bytes, "archive size limit")
    require(sha(raw) == expected_sha256, "archive digest")
    files, size = {}, 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for member in archive:
            require(len(files) < max_members, "archive member limit")
            name = member.name
            relative(name)
            require(name not in files and member.type == tarfile.REGTYPE and not member.linkname,
                    "duplicate/link/special archive member")
            require(member.mode in (0o644, 0o755), "archive mode")
            require(not member.pax_headers and member.size >= 0, "archive extensions")
            size += member.size
            require(size <= max_bytes, "expanded archive size limit")
            parts = name.split("/")
            require(all("/".join(parts[:i]) not in files for i in range(1, len(parts))), "file ancestor collision")
            require(not any(p.startswith(name + "/") for p in files), "directory/file collision")
            content = archive.extractfile(member).read()
            require(len(content) == member.size, "truncated archive member")
            files[name] = ("100" + oct(member.mode)[2:], content)
    require(encode(files) == raw, "noncanonical archive framing")
    return files


def produce(identity, provenance, projection, blobs, runtime_archive,
            runtime_archive_hash, expected_runtime, generated=None):
    """Assemble real bytes from authenticated Git objects and prebuilt runtime.

expected_runtime is an externally pinned complete inventory, not derived proof.
generated is an already-built static frontend payload map, subject to receiver
rules. Missing canonical ordinary blobs fail even if a route may never use them.
"""
    projected = verify_projection(provenance, projection, identity["tree"])
    require(runtime_archive_hash == identity["runtime_archive_sha256"], "runtime input identity")
    runtime = decode(runtime_archive, runtime_archive_hash, 1024**3, 100000)
    require(set(runtime) == set(expected_runtime), "runtime closure inventory")
    files = {}
    for name, (mode, data) in runtime.items():
        require(name.startswith("runtime/"), "runtime namespace")
        require(expected_runtime[name] == {"mode": mode, "size": len(data), "sha256": sha(data)}, "runtime inventory mismatch")
        files[name] = (mode, data)
    for name, (mode, oid) in projected["files"].items():
        require(oid in blobs and git_blob(blobs[oid]) == oid, "missing/corrupt canonical blob")
        files["source/" + name] = (mode, blobs[oid])
    for name, payload in (generated or {}).items():
        require(name not in files, "generated source collision")
        files[name] = payload
    manifest = dict(identity, schema="grid.api-bundle.v2", python="3.10", platform="linux-x86_64",
                    provenance=provenance, projection=projection,
                    files={name: {"mode": mode, "size": len(data), "sha256": sha(data)}
                           for name, (mode, data) in files.items()})
    return manifest, encode(files)


def receive(manifest, raw, admission):
    """Validate exact receiving head and bytes before any sink is requested.

Synthetic identities here deliberately check CONTENT ONLY. They do not assert
physical inode independence. A later target descriptor inventory is mandatory.
"""
    require(json_hash(manifest) == admission["manifest_sha256"], "manifest substitution")
    expected = admission["expected"]
    require(manifest["commit"] == admission["receiving_commit"] == expected["commit"], "receiving head mismatch")
    files = decode(raw, admission["archive_sha256"], admission["max_bytes"], admission["max_members"])
    observations = {name: {"kind": "regular", "nlink": 1, "uid": expected["bundle_uid"],
                          "device": 0, "inode": i, "mode": mode, "content": data}
                    for i, (name, (mode, data)) in enumerate(files.items(), 1)}
    verify_bundle(manifest, observations, expected, set())
    return files


def transfer(manifest, raw, admission, sink):
    """Injected exclusive sink: stage privately, verify readback, publish once.

The sink must enforce no-follow/exclusive ownership and atomic no-replace publish.
Any failure leaves its isolated transaction for explicit recovery; no cleanup,
overwrite, retry, shell, network client or target path inference is provided.
"""
    files = receive(manifest, raw, admission)
    token = sink.begin_exclusive(admission["receiving_commit"], admission["manifest_sha256"])
    for name, (mode, data) in sorted(files.items()):
        sink.write_exclusive(token, name, mode, data)
    actual = sink.inventory(token)
    require(actual == manifest["files"], "transfer readback mismatch")
    sink.publish_no_replace(token, admission["manifest_sha256"])
    return {"files": len(files), "manifest_sha256": admission["manifest_sha256"],
            "physical_qualification": False}
