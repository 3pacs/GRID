"""Proposed stable launcher entry point. Adoption/native adapter are absent.

The native successor must open+verify one immutable release, apply launch_plan's
exact sys.path/environment through an independently qualified bootstrap, and exec
its independently owned Python3.10. It must not resolve current a second time.
This source does NOT execute API startup or change any service.
"""
import argparse


def launch(kernel, commit, environment, import_origins, state_binding=None):
    """Executable launcher composition over a descriptor-pinning kernel.

pin_current resolves the isolated alias exactly once and pins the release,
source cwd and interpreter descriptors. verify_release must authenticate both
manifest/native receipt and immutable mount/owner topology. exec_pinned must use
those descriptors without reopening current or inheriting unbound descriptors.
No default kernel is supplied and main still refuses execution unconditionally.
"""
    import json
    from api_bundle_contract import launch_plan, require
    from api_isolation_bootstrap import state_contract
    target_expected = "/data/grid_v4/api-isolated/releases/" + commit
    bound = state_contract(state_binding, target_expected)
    token, target = kernel.pin_current()
    spec = launch_plan(target, commit, environment, import_origins)
    spec["release_root"] = target_expected
    kernel.verify_release(token, commit, spec)
    # -I -S prevents site/user-site/.pth/environment path setup. The explicitly
    # pinned standalone runtime must supply encodings before this code starts.
    # Require a specifically admitted state-binding bootstrap. There is no raw
    # api.main:app fallback: product proposals preserve legacy behavior if absent.
    program = kernel.state_bound_program(token, spec, bound)
    require(isinstance(program, str) and program, "missing bound bootstrap")
    argv = [spec["interpreter"], *spec["python_flags"], "-c", program]
    require(all(isinstance(k, str) and isinstance(v, str) and "\0" not in k + v
                for k, v in environment.items()), "invalid exec environment")
    kernel.exec_pinned(token, argv, dict(environment), spec["cwd"])
    raise RuntimeError("exec_pinned returned without replacing process")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", required=True)
    parser.parse_args()
    parser.exit(78, "NOT_ADOPTED: verified descriptor-based launcher/bootstrap and runtime are not installed or qualified.\n")


if __name__ == "__main__":
    main()
