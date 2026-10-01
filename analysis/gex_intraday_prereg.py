"""GEX P3 intraday hypothesis family v1: pre-registration registry (design only).

The body of ``docs/paper_log/gex-intraday-v1-preregistration.md`` (LF bytes
strictly between the VS1 body markers) is pinned in :data:`PREREG_BODY_SHA256`.
Its machine-readable hypothesis block is validated here: five sub-families with
separate error budgets, the price-only baseline never pooled with gamma-model
hypotheses, every hypothesis bound to an executable price contract and, where it
uses gamma, to the pinned engine.

The registry reuses the S10/VS1 chain (``analysis.research_forward_log.ForwardLog``):
canonical JSON lines, ``prev_sha256``, a chained anchor file, and an off-host
witness file appended from a vault worktree. This module only defines and checks
the records. Writing the registry on grid-svr, the vault witness commit, the
Stage-0 power run and any logger activation are owner gates; nothing here reads
an outcome, a price, the database or the frozen GEX-levels v1 log.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

from analysis.research_forward_log import ForwardLog, _lines, canonical

REPO = Path(__file__).resolve().parents[1]
VERSION = "gex_intraday_v1"
PREREG_PATH = Path("docs/paper_log/gex-intraday-v1-preregistration.md")
PREREG_BODY_SHA256 = "3ccc664a306747d70a1d4c832de5db65418cd28fe8245f2d4a90caedee264cbf"
BODY_START = "<!-- PREREG-BODY-START -->"
BODY_END = "<!-- PREREG-BODY-END -->"

REGISTRY_LOG = "gex_intraday_v1_prereg.jsonl"
REGISTRY_ANCHORS = "gex_intraday_v1_prereg.anchors.jsonl"
REGISTRY_LOCK = ".gex_intraday_v1_prereg.lock"
WITNESS_PATH = Path("05-GRID/Paper-Log/gex_intraday_v1/gex_intraday_v1_prereg.anchors.jsonl")

SUB_FAMILIES = ("PO", "DW", "MG", "SC", "RB")
PRICE_CONTRACTS = ("PC-OC", "PC-CC")
INPUT_STATUSES = ("READY", "BLOCKED_INPUT", "BLOCKED_PRICE_CONTRACT")
KINDS = ("trade", "forecast")
ENGINE_FILES = ("physics/dealer_gamma.py", "physics/greeks/black_scholes.py")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
ISO_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
REQUIRED = (
    "id",
    "sub_family",
    "kind",
    "uses_gamma",
    "price_contract",
    "decision",
    "e2_rule",
    "input",
    "rule",
    "statistic",
    "direction",
    "planted_effect",
    "n_discovery",
    "n_holdout",
    "n_holdout_ladder",
    "input_status",
)


class PreregError(ValueError):
    """The pre-registration or its family block violates a registered rule."""


def prereg_body(text: str) -> str:
    text = text.replace("\r\n", "\n")
    if text.count(BODY_START) != 1 or text.count(BODY_END) != 1:
        raise PreregError("pre-registration needs exactly one body start and end marker")
    start, end = text.index(BODY_START) + len(BODY_START), text.index(BODY_END)
    if end <= start:
        raise PreregError("pre-registration body markers are out of order")
    return text[start:end]


def body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def read_body(repo_root: Path = REPO) -> str:
    return prereg_body((Path(repo_root) / PREREG_PATH).read_text(encoding="utf-8"))


def family_block(body: str) -> dict:
    blocks = re.findall(r"```json\n(.*?)\n```", body, flags=re.S)
    if len(blocks) != 1:
        raise PreregError("the body must carry exactly one json family block")

    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise PreregError(f"duplicate key {key!r} in family block")
            out[key] = value
        return out

    return json.loads(blocks[0], object_pairs_hook=unique)


def validate_family(family: dict) -> dict:
    """Every registered structural rule of section 1/3/4/7, checked mechanically."""
    if family.get("family") != VERSION:
        raise PreregError("family name mismatch")
    if tuple(family.get("sub_families", ())) != SUB_FAMILIES:
        raise PreregError("sub-families must be exactly PO, DW, MG, SC, RB")
    hold = family.get("holdout", {})
    if hold.get("family_alpha") != 0.05 or not hold.get("same_sign"):
        raise PreregError("holdout family alpha must be 0.05 with same sign")
    if hold.get("correction") != "frozen_selection_bonferroni_within_sub_family":
        raise PreregError("holdout correction must be within sub-family, never pooled")
    pin = family.get("engine_pin", {})
    if set(pin) != set(ENGINE_FILES) | {"reference_commit"} or not all(
        HEX64.fullmatch(str(pin[f])) for f in ENGINE_FILES
    ):
        raise PreregError("engine pin must name both engine files by sha256")
    hypotheses = family.get("hypotheses", [])
    ids = [h.get("id") for h in hypotheses]
    if len(ids) != len(set(ids)) or len(ids) != 10:
        raise PreregError("exactly ten uniquely named hypotheses")
    by_family: dict[str, list[str]] = {k: [] for k in SUB_FAMILIES}
    for h in hypotheses:
        missing = [k for k in REQUIRED if k not in h]
        if missing:
            raise PreregError(f"{h.get('id')}: missing {missing}")
        if h["sub_family"] not in SUB_FAMILIES or not h["id"].startswith(h["sub_family"]):
            raise PreregError(f"{h['id']}: sub-family mismatch")
        # The price-only baseline is never a gamma-model test, and vice versa.
        if (h["sub_family"] == "PO") == bool(h["uses_gamma"]) and h["sub_family"] != "RB":
            raise PreregError(f"{h['id']}: price-only and gamma hypotheses must not mix")
        if h["kind"] not in KINDS or h["price_contract"] not in PRICE_CONTRACTS:
            raise PreregError(f"{h['id']}: unknown kind or price contract")
        if h["input_status"] not in INPUT_STATUSES:
            raise PreregError(f"{h['id']}: unknown input status")
        if h["price_contract"] == "PC-OC" and h["input_status"] == "READY":
            raise PreregError(f"{h['id']}: no opening-auction source is admitted yet")
        if h["direction"] not in ("positive", "negative"):
            raise PreregError(f"{h['id']}: direction must be one-sided")
        ladder = h["n_holdout_ladder"]
        if (
            not ladder
            or ladder[0] != h["n_holdout"]
            or ladder != sorted(set(ladder))
            or h["n_discovery"] <= 0
        ):
            raise PreregError(f"{h['id']}: holdout ladder must start at n_holdout and grow")
        # Written before outcomes: no hypothesis may reference a calendar date.
        text = json.dumps({k: v for k, v in h.items() if k != "id"})
        if ISO_DATE.search(text):
            raise PreregError(f"{h['id']}: hypotheses may not reference specific dates")
        by_family[h["sub_family"]].append(h["id"])
    if any(not v for v in by_family.values()):
        raise PreregError("every sub-family needs at least one hypothesis")
    return by_family


def check_prereg(repo_root: Path = REPO, pinned: str | None = None) -> dict:
    body = read_body(repo_root)
    actual = body_sha256(body)
    expected = PREREG_BODY_SHA256 if pinned is None else pinned
    if actual != expected:
        raise PreregError(f"pre-registration body sha256 {actual} != pinned {expected}")
    family = family_block(body)
    return {
        "body_sha256": actual,
        "family_sha256": hashlib.sha256(canonical(family)).hexdigest(),
        "sub_families": validate_family(family),
        "family": family,
    }


def engine_matches_pin(family: dict, repo_root: Path = REPO) -> dict[str, bool]:
    """Whether the checkout's engine files still equal the pinned engine.

    A mismatch is not an error here (the engine may legitimately move on); it
    means the forward logger must run the immutable pinned archive instead.
    """
    pin = family["engine_pin"]
    return {
        f: hashlib.sha256((Path(repo_root) / f).read_bytes()).hexdigest() == pin[f]
        for f in ENGINE_FILES
    }


def registry(log_dir: Path, prereg_sha256: str = PREREG_BODY_SHA256) -> ForwardLog:
    return ForwardLog(
        log_dir,
        log_filename=REGISTRY_LOG,
        anchor_filename=REGISTRY_ANCHORS,
        lock_filename=REGISTRY_LOCK,
        prereg_sha256=prereg_sha256,
    )


def registration_records(now: datetime, code_sha: str, checked: dict) -> list[dict]:
    if now.tzinfo is None:
        raise PreregError("now must carry a timezone")
    if not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise PreregError("code_sha must be a full 40-hex git commit")
    family = checked["family"]
    return [
        {
            "kind": "header",
            "version": VERSION,
            "run_at": now.isoformat(),
            "code_sha": code_sha,
            "prereg_path": PREREG_PATH.as_posix(),
            "prereg_sha256": checked["body_sha256"],
        },
        {
            "kind": "preregistration",
            "run_at": now.isoformat(),
            "code_sha": code_sha,
            "family_sha256": checked["family_sha256"],
            "trials": [h["id"] for h in family["hypotheses"]],
            "sub_families": checked["sub_families"],
            "input_status": {h["id"]: h["input_status"] for h in family["hypotheses"]},
            "engine_pin": family["engine_pin"],
            "promotion_allowed": False,
        },
    ]


def register(
    log_dir: Path,
    now: datetime,
    code_sha: str,
    *,
    repo_root: Path = REPO,
    pinned: str | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Write header + preregistration into an EMPTY registry directory (owner gate)."""
    checked = check_prereg(repo_root, pinned)
    records = registration_records(now, code_sha, checked)
    log = registry(log_dir, checked["body_sha256"])
    if dry_run:
        return records
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        if log.read_all():
            raise PermissionError("already registered here; a second registration is a fork")
        return log.append_locked(records)


def export_anchors(log_dir: Path, vault_worktree: Path, prereg_sha256: str = PREREG_BODY_SHA256) -> list[str]:
    """Append missing anchor lines to the witness file in a vault WORKTREE only.

    Refuses when the witness file is not a prefix of this registry's anchors.
    The operator reviews, commits and pushes (owner gate); nothing is pushed here.
    """
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        local = list(_lines(log.anchor_path))
    path = Path(vault_worktree) / WITNESS_PATH
    existing = [line.rstrip(b"\r") for line in _lines(path)] if path.exists() else []
    if existing != local[: len(existing)]:
        raise PermissionError("the witness file anchors another registry chain; not appending")
    new = local[len(existing) :]
    if new:
        path.parent.mkdir(parents=True, exist_ok=True)
        tail = path.read_bytes() if path.exists() else b""
        with open(path, "ab") as stream:
            if tail and not tail.endswith(b"\n"):
                stream.write(b"\n")
            for line in new:
                stream.write(line + b"\n")
    return [line.decode("utf-8") for line in new]


def verify(log_dir: Path, external_anchors: Path | None = None, prereg_sha256: str = PREREG_BODY_SHA256) -> dict[str, Any]:
    result = registry(log_dir, prereg_sha256).verify_chain(external_anchors)
    if result["ok"]:
        records = registry(log_dir, prereg_sha256).read_all()
        kinds = [r.get("kind") for r in records[:2]]
        if kinds != ["header", "preregistration"]:
            return {**result, "ok": False, "detail": "registry does not start with header + preregistration"}
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="verify the body hash and the family rules")
    reg = sub.add_parser("register", help="OWNER GATE: write header + preregistration")
    reg.add_argument("--log-dir", type=Path, required=True)
    reg.add_argument("--code-sha", required=True)
    reg.add_argument("--now", required=True, help="ISO-8601 with offset")
    reg.add_argument("--dry-run", action="store_true")
    exp = sub.add_parser("export-anchors", help="append anchor lines into a vault worktree")
    exp.add_argument("--log-dir", type=Path, required=True)
    exp.add_argument("--vault-worktree", type=Path, required=True)
    ver = sub.add_parser("verify")
    ver.add_argument("--log-dir", type=Path, required=True)
    ver.add_argument("--anchor", type=Path)
    args = parser.parse_args(argv)
    if args.command == "check":
        checked = check_prereg()
        out = {k: v for k, v in checked.items() if k != "family"}
        out["engine_matches_pin"] = engine_matches_pin(checked["family"])
    elif args.command == "register":
        out = register(
            args.log_dir, datetime.fromisoformat(args.now), args.code_sha, dry_run=args.dry_run
        )
    elif args.command == "export-anchors":
        out = export_anchors(args.log_dir, args.vault_worktree)
    else:
        out = verify(args.log_dir, args.anchor)
    sys.stdout.write(json.dumps(out, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
