"""GEX P3 intraday hypothesis family v1: pre-registration registry (design only).

The body of ``docs/paper_log/gex-intraday-v1-preregistration.md`` (LF bytes
strictly between the VS1 body markers) is pinned in :data:`PREREG_BODY_SHA256`.
Its machine-readable hypothesis block is validated here against every
structural rule the body states: five sub-families with fixed Bonferroni k, the
price-only baseline never mixed with gamma-model hypotheses, paired incremental
statistics for the gamma/breadth trade claims, each hypothesis bound to an
executable price contract and the matching E2 rule, a complete Stage-0 power
specification, the cost model, and an engine pinned by LF content hash.

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
PREREG_BODY_SHA256 = "865c02f8213030758aae5c5f14fbe798e3d7aaf1dd643eb299e6b3903101b075"
BODY_START = "<!-- PREREG-BODY-START -->"
BODY_END = "<!-- PREREG-BODY-END -->"

REGISTRY_LOG = "gex_intraday_v1_prereg.jsonl"
REGISTRY_ANCHORS = "gex_intraday_v1_prereg.anchors.jsonl"
REGISTRY_LOCK = ".gex_intraday_v1_prereg.lock"
WITNESS_PATH = Path("05-GRID/Paper-Log/gex_intraday_v1/gex_intraday_v1_prereg.anchors.jsonl")
DECISION_WITNESS_PATH = Path(
    "05-GRID/Paper-Log/gex_intraday_v1/gex_intraday_v1_decisions.anchors.jsonl"
)
#: Pinned after the real (owner-gated) registration, as VS1 does with its
#: REGISTERED_RECORD_SHA256. While None, no registration has been witnessed.
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None

SUB_FAMILIES = ("PO", "DW", "MG", "SC", "RB")
FIXED_K = {"PO": 2, "DW": 2, "MG": 3, "SC": 2, "RB": 1}
PRICE_CONTRACTS = ("PC-OC", "PC-CC")
INPUT_STATUSES = ("READY", "BLOCKED_INPUT", "BLOCKED_PRICE_CONTRACT")
E2_RULE = {
    ("PC-OC", "trade"): "e2.auction_oc.v1",
    ("PC-OC", "forecast"): "e2.abs_move.v1",
    ("PC-OC", "paired_trade"): "e2.paired_oc.v1",
    ("PC-CC", "trade"): "e2.direction.v1",
}
PLANTED_MODELS = {
    "trade": {"trade": ("net_bps", "noise_bps")},
    "paired_trade": {"paired_trade": ("net_bps", "noise_bps")},
    "forecast": {
        "forecast_continuous": ("slope_per_sd", "noise_log_sd", "control_corr"),
        "forecast_binary": ("diff", "base_rate", "noise_log_sd", "control_corr"),
    },
}
#: The engine and its spot path (the logger imports them only from a git
#: archive of ``reference_commit`` whose tree must equal ``reference_tree``).
ENGINE_FILES = (
    "physics/dealer_gamma.py",
    "physics/greeks/black_scholes.py",
    "store/astrogrid.py",
    "store/availability.py",
    "ingestion/market_calendar.py",
    "price_close_contract.py",
)
HEX64 = re.compile(r"^[0-9a-f]{64}$")
HEX40 = re.compile(r"^[0-9a-f]{40}$")
# Written before outcomes: hypotheses may not reference a calendar date, a year
# or a named month (a pattern that could postdate registration).
DATE_LIKE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}\b|\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b|\b(?:19|20)\d{2}\b"
    r"|\b(?:January|February|March|April|May|June|July|August|September|October|November"
    r"|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\b"
)
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


def lf_sha256(data: bytes) -> str:
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


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


def _int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_family(family: dict) -> dict:
    """Every structural rule the body registers, checked mechanically."""
    if family.get("family") != VERSION:
        raise PreregError("family name mismatch")
    if tuple(family.get("sub_families", ())) != SUB_FAMILIES:
        raise PreregError("sub-families must be exactly PO, DW, MG, SC, RB")
    stage0 = family.get("stage0", {})
    if not (
        _int(stage0.get("seed"))
        and _int(stage0.get("simulations"))
        and stage0.get("simulations", 0) >= 1000
        and stage0.get("min_joint_power") == 0.5
        and _number(stage0.get("control_loading"))
    ):
        raise PreregError("stage0 must fix seed, simulations, joint power 0.5 and control loading")
    if family.get("discovery") != {"select_one_sided_p": 0.10}:
        raise PreregError("discovery selection must be one-sided p <= 0.10")
    hold = family.get("holdout", {})
    if (
        hold.get("family_alpha") != 0.05
        or hold.get("same_sign") is not True
        or hold.get("correction") != "fixed_k_bonferroni_within_sub_family"
        or hold.get("k") != FIXED_K
    ):
        raise PreregError("holdout must be fixed-k Bonferroni within sub-family, never pooled")
    cost = family.get("cost", {})
    if cost.get("model") != "e2-costs-v1" or cost.get("bps_per_side") != 3.0:
        raise PreregError("cost must be e2-costs-v1 at 3 bp per side")
    pin = family.get("engine_pin", {})
    if (
        set(pin) != set(ENGINE_FILES) | {"hash_basis", "reference_commit", "reference_tree"}
        or not all(HEX64.fullmatch(str(pin[f])) for f in ENGINE_FILES)
        or pin.get("hash_basis") != "sha256 of LF git content"
        or not HEX40.fullmatch(str(pin.get("reference_commit")))
        or not HEX40.fullmatch(str(pin.get("reference_tree")))
    ):
        raise PreregError(
            "engine pin must name the engine and spot-path files by LF sha256, a full commit and its tree"
        )
    hypotheses = family.get("hypotheses", [])
    ids = [h.get("id") for h in hypotheses]
    if len(ids) != len(set(ids)) or len(ids) != sum(FIXED_K.values()):
        raise PreregError("exactly ten uniquely named hypotheses")
    by_family: dict[str, list[str]] = {k: [] for k in SUB_FAMILIES}
    for h in hypotheses:
        missing = [k for k in REQUIRED if k not in h]
        if missing:
            raise PreregError(f"{h.get('id')}: missing {missing}")
        hid = h["id"]
        if h["sub_family"] not in SUB_FAMILIES or not hid.startswith(h["sub_family"]):
            raise PreregError(f"{hid}: sub-family mismatch")
        # The price-only baseline and rebalancing never use gamma; DW/MG/SC always do.
        if bool(h["uses_gamma"]) != (h["sub_family"] in ("DW", "MG", "SC")):
            raise PreregError(f"{hid}: price-only and gamma hypotheses must not mix")
        if (h["price_contract"], h["kind"]) not in E2_RULE:
            raise PreregError(f"{hid}: unknown kind or price contract")
        if h["e2_rule"] != E2_RULE[(h["price_contract"], h["kind"])]:
            raise PreregError(f"{hid}: e2 rule does not match the price contract and kind")
        if h["input_status"] not in INPUT_STATUSES:
            raise PreregError(f"{hid}: unknown input status")
        if h["price_contract"] == "PC-OC" and h["input_status"] == "READY":
            raise PreregError(f"{hid}: no opening-auction source is admitted yet")
        if h["direction"] not in ("positive", "negative"):
            raise PreregError(f"{hid}: direction must be one-sided")
        if h["kind"] == "paired_trade" and "d = net(" not in h["statistic"]:
            raise PreregError(f"{hid}: a paired hypothesis must test the paired difference")
        planted = h["planted_effect"]
        models = PLANTED_MODELS[h["kind"]]
        fields = models.get(planted.get("model"))
        if fields is None or set(planted) != {"model", *fields} or not all(
            _number(planted[f]) for f in fields
        ):
            raise PreregError(f"{hid}: planted effect does not match its Stage-0 model")
        if "base_rate" in planted and not 0 < planted["base_rate"] < 1:
            raise PreregError(f"{hid}: base rate must lie in (0, 1)")
        effect = next(planted[k] for k in ("net_bps", "slope_per_sd", "diff") if k in planted)
        if effect == 0 or (effect > 0) != (h["direction"] == "positive"):
            raise PreregError(f"{hid}: planted effect sign must agree with the registered direction")
        ladder = h["n_holdout_ladder"]
        if (
            not _int(h["n_discovery"])
            or not _int(h["n_holdout"])
            or not isinstance(ladder, list)
            or not all(_int(x) for x in ladder)
            or not ladder
            or ladder[0] != h["n_holdout"]
            or ladder != sorted(set(ladder))
            or h["n_discovery"] <= 0
        ):
            raise PreregError(f"{hid}: integer windows; ladder must start at n_holdout and grow")
        # Only the prose fields: integer windows such as 2000 are not years.
        text = " ".join(v for k, v in h.items() if isinstance(v, str) and k != "id")
        if DATE_LIKE.search(text):
            raise PreregError(f"{hid}: hypotheses may not reference specific dates")
        by_family[h["sub_family"]].append(hid)
    if {k: len(v) for k, v in by_family.items()} != FIXED_K:
        raise PreregError("sub-family sizes must equal the registered fixed k")
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
    """Whether the checkout's engine files (LF-normalized) equal the pinned engine.

    A mismatch is not an error here (the engine may legitimately move on); it
    means the forward logger must run the immutable pinned archive instead.
    """
    pin = family["engine_pin"]
    return {
        f: lf_sha256((Path(repo_root) / f).read_bytes()) == pin[f] for f in ENGINE_FILES
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
    if not HEX40.fullmatch(code_sha):
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
            "promotion_allowed": False,
        },
        {
            "kind": "preregistration",
            "run_at": now.isoformat(),
            "code_sha": code_sha,
            "family_sha256": checked["family_sha256"],
            "trials": [h["id"] for h in family["hypotheses"]],
            "sub_families": checked["sub_families"],
            "fixed_k": family["holdout"]["k"],
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


def export_anchors(
    log_dir: Path, vault_worktree: Path, prereg_sha256: str = PREREG_BODY_SHA256
) -> list[str]:
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


def verify(
    log_dir: Path,
    external_anchors: Path | None = None,
    prereg_sha256: str = PREREG_BODY_SHA256,
    registered: tuple[str, str] | None = None,
) -> dict[str, Any]:
    """Chain + anchors; the first two records must be header + preregistration
    with promotion_allowed false, and, once the real registration is pinned,
    exactly the pinned record hashes (anything else is a fork)."""
    log = registry(log_dir, prereg_sha256)
    result = log.verify_chain(external_anchors)
    if not result["ok"]:
        return result
    records = log.read_all()
    head = records[:2]
    if [r.get("kind") for r in head] != ["header", "preregistration"] or any(
        r.get("promotion_allowed") is not False for r in head
    ):
        return {**result, "ok": False, "detail": "registry does not start with the pinned registration"}
    pinned = REGISTERED_RECORD_SHA256 if registered is None else registered
    if pinned is not None:
        hashes = tuple(hashlib.sha256(canonical(r)).hexdigest() for r in head)
        if hashes != tuple(pinned):
            return {**result, "ok": False, "detail": "registration differs from the pinned one (fork)"}
    return result


def parse_now(text: str) -> datetime:
    """ISO-8601 with an offset; a trailing 'Z' is accepted on Python 3.10."""
    value = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    if value.tzinfo is None:
        raise PreregError("--now must carry an offset")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="verify the body hash and the family rules")
    reg = sub.add_parser("register", help="OWNER GATE: write header + preregistration")
    reg.add_argument("--log-dir", type=Path, required=True)
    reg.add_argument("--code-sha", required=True)
    reg.add_argument("--now", required=True, help="ISO-8601 with offset or Z")
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
        out = register(args.log_dir, parse_now(args.now), args.code_sha, dry_run=args.dry_run)
    elif args.command == "export-anchors":
        out = export_anchors(args.log_dir, args.vault_worktree)
    else:
        out = verify(args.log_dir, args.anchor)
    sys.stdout.write(json.dumps(out, indent=1, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
