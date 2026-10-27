"""Source-only isolated builder recipe. Actual producer remains unqualified."""
import argparse
import json


def assemble(profile, commit, provenance, projection, blobs, runtime_archive,
             expected_runtime, native_receipt_sha256, generated=None):
    """Assemble a prebuilt qualified runtime plus exact Git projection bytes.

Does not execute the build recipe, download artifacts or attest its sandbox.
The separate receiving admission must independently pin the resulting inventory,
native receipt, interpreter, generated frontend and complete runtime closure.
"""
    from api_isolation_artifact import produce
    from api_bundle_contract import json_hash, require, sha
    build_plan(profile, commit)
    require(json_hash(projection) == profile["projection_sha256"], "profile projection mismatch")
    require(sha(native_receipt_sha256), "native receipt identity required")
    identity = {"commit": commit, "tree": profile["source_tree"],
                "lock_sha256": profile["requirements_lock_sha256"],
                "runtime_archive_sha256": profile["runtime_archive_sha256"],
                "native_receipt_sha256": native_receipt_sha256}
    return produce(identity, provenance, projection, blobs, runtime_archive,
                   profile["runtime_archive_sha256"], expected_runtime, generated)


def build_plan(profile, commit):
    # This file is self-contained so -I -S needs no checkout import path changes.
    def check(value, label):
        if not value:
            raise ValueError(label)
    def pin(value):
        return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
    check(len(commit) == 40 and all(c in "0123456789abcdef" for c in commit), "exact commit")
    check(profile["schema"] == "grid.api-builder.v2", "profile schema")
    check(len(profile["source_tree"]) == 40 and all(c in "0123456789abcdef" for c in profile["source_tree"]), "exact canonical source tree")
    check(profile["python"] == "3.10", "Python cutover forbidden")
    for field in ("builder_image_sha256", "runtime_archive_sha256", "interpreter_sha256", "requirements_lock_sha256", "node_archive_sha256", "package_lock_sha256", "projection_sha256"):
        check(pin(profile[field]), "missing artifact identity: " + field)
    check(profile["wheels"] and all(pin(v) for v in profile["wheels"].values()), "unpinned wheels")
    check(profile["network"] == "none" and profile["production_mounts"] == [] and profile["credential_mounts"] == [], "builder not isolated")
    root = "/work/api-build/" + commit
    return {"commit": commit, "root": root, "profile": profile,
            "commands": [
                {"argv": [root + "/runtime/bin/python3.10", "-I", "-m", "pip", "--isolated", "install", "--no-index", "--find-links", root + "/wheelhouse", "--require-hashes", "--no-deps", "--no-compile", "-r", root + "/requirements.lock"], "cwd": root},
                {"argv": [root + "/node/bin/npm", "ci", "--offline"], "cwd": root + "/source/pwa"},
                {"argv": [root + "/node/bin/npm", "run", "build"], "cwd": root + "/source/pwa"}],
            "preconditions": ["content-addressed input verification", "empty exclusive builder root", "credential-free sandbox", "verify canonical Git objects including all link blobs without following checkout links", "apply exact reviewed projection; materialize in-tree regular targets only; omit explicitly bound external links", "unpack regular independent runtime files only"],
            "outputs": ["complete projected-source/runtime bundle", "content manifest", "separate complete canonical Git provenance", "exact projection inventory and omission behavior binding", "package lock proof"],
            "target_installs": [], "implemented_executor": False,
            "qualification": "CPU/native/package import receipt required before target use; no blanket no-provider claim for API startup"}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--expected-commit", required=True)
    p.add_argument("--profile")
    p.add_argument("--execute", action="store_true")
    args = p.parse_args()
    if args.execute:
        p.exit(78, "NOT_ADOPTED: isolated producer/artifact inputs/native qualification absent; no build or install executed.\n")
    if not args.profile:
        p.error("--profile is required for source recipe output")
    with open(args.profile, encoding="utf-8") as stream:
        result = build_plan(json.load(stream), args.expected_commit)
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
