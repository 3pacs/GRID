"""Pure source/bundle verification and launcher plans; no native IO adapter."""
from __future__ import annotations
import hashlib
import json
from pathlib import PurePosixPath


class Refusal(ValueError):
    pass


def require(value, message):
    if not value:
        raise Refusal(message)


def sha(value, length=64):
    return isinstance(value, str) and len(value) == length and all(c in "0123456789abcdef" for c in value)


def relative(name):
    p = PurePosixPath(name)
    require(isinstance(name, str) and not p.is_absolute() and str(p) == name, "noncanonical member")
    require(name and ".." not in p.parts and "\\" not in name and ":" not in name and all(ord(c) >= 32 for c in name), "member escape")
    return p


def git_blob(data):
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def tree_hash(entries):
    root = {}
    for name, (mode, oid) in entries.items():
        parts = relative(name).parts
        node = root
        for part in parts[:-1]:
            node = node.setdefault(part, {})
            require(isinstance(node, dict), "file/directory collision")
        require(parts[-1] not in node, "duplicate source path")
        node[parts[-1]] = (mode, oid)
    def tree(node):
        records = []
        for name, value in node.items():
            if isinstance(value, dict):
                records.append((name + "/", "40000", name, tree(value)))
            else:
                records.append((name, value[0], name, value[1]))
        data = b"".join(mode.encode() + b" " + name.encode() + b"\0" + bytes.fromhex(oid) for _, mode, name, oid in sorted(records))
        return hashlib.sha1(b"tree " + str(len(data)).encode() + b"\0" + data).hexdigest()
    return tree(root)


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def verify_provenance(provenance, expected_tree):
    """Prove canonical Git metadata without making any filesystem symlinks.

    Regular-file content is checked when projected bytes are observed. All link
    blob bytes are retained here even for deliberately omitted runtime paths.
    """
    entries = provenance["entries"]
    require(entries and all(v[0] in ("100644", "100755", "120000") and sha(v[1], 40) for v in entries.values()), "unsupported canonical Git mode/blob")
    require(tree_hash(entries) == expected_tree, "canonical source tree mismatch")
    links = {name for name, value in entries.items() if value[0] == "120000"}
    require(set(provenance["link_targets"]) == links, "missing/extra canonical link bytes")
    for name in links:
        target = provenance["link_targets"][name]
        require(isinstance(target, str) and git_blob(target.encode("utf-8")) == entries[name][1], "canonical link blob mismatch")
    return entries


def internal_target(name, target):
    """Resolve Git link text lexically only; NEVER dereference a host path."""
    require(isinstance(target, str) and target and not target.startswith("/") and "\\" not in target and ":" not in target and all(ord(c) >= 32 for c in target), "external/invalid link target")
    parts = list(relative(name).parts[:-1])
    for part in target.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            require(parts, "link escapes canonical source")
            parts.pop()
        else:
            parts.append(part)
    require(parts, "link target is source root")
    return "/".join(parts)


def verify_projection(provenance, projection, expected_tree):
    canonical = verify_provenance(provenance, expected_tree)
    require(projection["schema"] == "grid.api-projection.v1" and projection["tree"] == expected_tree, "projection identity")
    require(set(projection["entries"]) == set(canonical), "projection must account for every canonical leaf")
    projected, omitted = {}, {}
    for name, (mode, oid) in canonical.items():
        decision = projection["entries"][name]
        action = decision["action"]
        if action == "copy":
            require(mode in ("100644", "100755"), "canonical link cannot be copied as ordinary source")
            projected[name] = (mode, oid)
        elif action == "materialize":
            require(mode == "120000", "materialization is only for canonical links")
            target = internal_target(name, provenance["link_targets"][name])
            require(decision["target"] == target, "materialization target substitution")
            require(target in canonical and canonical[target][0] in ("100644", "100755"), "materialization target is missing/link/nonregular")
            projected[name] = tuple(canonical[target])
        elif action == "omit":
            require(mode == "120000" and decision["reason"] and decision["behavior"] == "unavailable-until-projection-qualification", "unbound omission behavior")
            omitted[name] = decision
        else:
            raise Refusal("unsupported projection action")
    return {"files": projected, "omitted": omitted,
            "native_or_behavior_qualified": False}


def verify_bundle(manifest, observed, expected, protected_inodes):
    """observed content/identity MUST come from a no-follow native reader.

    This pure verifier cannot prove the provenance/freshness of observations.
    The target adapter, receipt verifier and descriptor-based reader are absent.
    """
    require(manifest["schema"] == "grid.api-bundle.v2", "bundle schema")
    for key in ("commit", "tree", "lock_sha256", "runtime_archive_sha256", "native_receipt_sha256"):
        require(manifest[key] == expected[key], "identity mismatch: " + key)
        require(sha(manifest[key], 40 if key in ("commit", "tree") else 64), "invalid pin")
    require(manifest["python"] == "3.10", "runtime cutover forbidden")
    require(manifest["platform"] == "linux-x86_64", "target platform")
    require(json_hash(manifest["provenance"]) == expected["provenance_sha256"], "unbound canonical provenance")
    require(json_hash(manifest["projection"]) == expected["projection_sha256"], "unbound runtime projection")
    projection = verify_projection(manifest["provenance"], manifest["projection"], expected["tree"])
    files = manifest["files"]
    require(files and set(files) == set(observed), "partial or extra bundle member")
    inventory = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    require(inventory == expected["inventory_sha256"], "unqualified file inventory")
    require(expected["required_runtime_files"] and all(p.startswith("runtime/") and p in files for p in expected["required_runtime_files"]), "incomplete qualified runtime")
    seen_source, seen_inodes = set(), set()
    for name, entry in files.items():
        p = relative(name)
        require(p.parts[0] in ("source", "runtime"), "unowned member root")
        if p.parts[0] == "runtime":
            require(p.suffix not in (".pth", ".egg-link") and p.name not in ("pyvenv.cfg", "sitecustomize.py", "usercustomize.py"), "import redirection member")
        obs = observed[name]
        require(obs["kind"] == "regular" and obs["nlink"] == 1, "symlink/hardlink/special member")
        require(obs["uid"] == expected["bundle_uid"], "member owner")
        identity = (obs["device"], obs["inode"])
        require(identity not in protected_inodes and identity not in seen_inodes, "shared inode")
        seen_inodes.add(identity)
        data = obs["content"]
        require(isinstance(data, bytes) and len(data) == entry["size"], "partial file")
        require(hashlib.sha256(data).hexdigest() == entry["sha256"], "content drift")
        require(entry["mode"] in ("100644", "100755") and obs["mode"] == entry["mode"], "unsafe mode")
        if p.parts[0] == "source":
            source_name = PurePosixPath(*p.parts[1:]).as_posix()
            if source_name in projection["files"]:
                require((entry["mode"], git_blob(data)) == projection["files"][source_name], "projected source bytes/mode mismatch")
                seen_source.add(source_name)
            else:
                require(source_name not in projection["omitted"], "omitted link reintroduced")
                require(len(p.parts) > 2 and p.parts[1] == "pwa_dist" and p.suffix in (".html", ".js", ".css", ".map", ".json", ".svg", ".png", ".ico", ".jpg", ".webp", ".woff", ".woff2", ".ttf", ".txt"), "untracked executable/source shadow")
    require(seen_source == set(projection["files"]), "partial runtime source projection")
    interpreter = "runtime/bin/python3.10"
    require(interpreter in files and files[interpreter]["mode"] == "100755", "missing independent interpreter")
    require(files[interpreter]["sha256"] == expected["interpreter_sha256"], "interpreter identity")
    return {"commit": expected["commit"], "tree": expected["tree"], "files": len(files)}


def launch_plan(resolved_target, commit, environment, import_origins):
    """Resolve current once in the future native adapter, then use this plan.

    import_origins is a separately admitted import qualification observation,
    never generated here by importing application/packages. Dynamic imports not
    represented by that observation remain a qualification limitation.
    """
    require(sha(commit, 40), "commit")
    target = "/data/grid_v4/api-isolated/releases/" + commit
    require(resolved_target == target, "wrong/legacy release alias")
    for key in environment:
        require(not key.startswith(("PYTHON", "PIP_", "LD_")) and key not in ("VIRTUAL_ENV", "PATH"), "inherited import/loader path")
    source = target + "/source"
    runtime = target + "/runtime"
    paths = [source, runtime + "/lib/python3.10", runtime + "/lib/python3.10/lib-dynload", runtime + "/lib/python3.10/site-packages"]
    require(import_origins, "missing import qualification")
    for origin in import_origins.values():
        if origin in ("built-in", "frozen"):
            continue
        require(any(origin.startswith(p + "/") for p in paths), "absolute import escaped isolated release")
        require(".." not in PurePosixPath(origin).parts and "\\" not in origin, "noncanonical import origin")
    return {"cwd": source, "interpreter": runtime + "/bin/python3.10", "python_flags": ["-I", "-S", "-B"],
            "sys_path_exact": paths, "environment_keys": sorted(environment),
            "module": "uvicorn", "argv": ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"],
            "source_alias_follow_count": 1, "inherited_api_effects": "unchanged DB/model-registry/contracts/retry/serving behavior"}


def rollback_plan(binding, receipt):
    require(receipt["expected_current"] == binding["candidate"], "rollback current target mismatch")
    require(receipt["old_target"] == binding["old"], "rollback old target mismatch")
    require(all(sha(receipt[k]) for k in ("old_bundle_manifest", "new_bundle_manifest", "compatibility_receipt", "journal_sha256", "owner_receipt")), "unbound rollback")
    require(receipt["plan_sha256"] == binding["plan_sha256"], "rollback plan mismatch")
    return {"expected": binding["candidate"], "restore": binding["old"], "service": "grid-api.service",
            "retain": [binding["old"], binding["candidate"]], "undo_database": False,
            "automatic_retry": False, "cleanup": "none", "needs_native_adapter": True}
