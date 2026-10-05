"""E3B gates adapter: verifies E1 CI receipt and E0 statistical non-degradation.

In-process judge-only trust boundary: all inputs, baselines, and receipts are
judge-owned. Proposers cannot supply cards to bypass fresh benchmark execution.
"""

from __future__ import annotations

import hashlib, importlib, json, math
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from evals.e0 import machinery

MANDATORY_CI = ("Backend Tests", "Frontend Build", "Lint")
RATE_KEYS = ("power_holm_run_alpha", "power_bh", "power_raw_threshold")
SCENARIO_FLAGS = ("fdr_bh_controlled", "fwer_holm_controlled", "null_p_uniform_ks")
SECTION_PATHS = (
    ("fdr", "global_null", "fwer_holm_run_alpha", "rate"),
    ("fdr", "all_simulations", "fdr_bh", "rate"),
    ("null_calibration", "pooled", "ks_stat"),
    ("null_calibration", "pooled", "critical_5pct"),
    ("null_calibration", "pooled", "ks_p"),
)


def ci_gate(
    commit_sha: str,
    receipt: Mapping[str, Any],
    required_checks: Sequence[str] = MANDATORY_CI,
) -> dict[str, Any]:
    """Validate judge-owned CI receipt tied to candidate commit SHA."""
    if not isinstance(receipt, dict):
        return {
            "passed": False,
            "reason": "Receipt must be a mapping",
            "receipt_hash": "",
        }
    r_hash = hashlib.sha256(
        json.dumps(receipt, sort_keys=True).encode("utf-8")
    ).hexdigest()
    if (
        not isinstance(commit_sha, str)
        or len(commit_sha) != 40
        or not all(c in "0123456789abcdefABCDEF" for c in commit_sha)
    ):
        return {
            "passed": False,
            "reason": "Candidate commit SHA must be 40-hex string",
            "receipt_hash": r_hash,
        }
    if receipt.get("commit_sha") != commit_sha:
        return {
            "passed": False,
            "reason": f"Receipt SHA != candidate {commit_sha}",
            "receipt_hash": r_hash,
        }
    if not required_checks or not set(MANDATORY_CI).issubset(set(required_checks)):
        return {
            "passed": False,
            "reason": "required_checks must contain all mandatory CI checks",
            "receipt_hash": r_hash,
        }
    checks = receipt.get("checks")
    if not isinstance(checks, dict):
        return {
            "passed": False,
            "reason": "Receipt checks must be a mapping",
            "receipt_hash": r_hash,
        }
    for name in required_checks:
        chk = checks.get(name)
        if (
            not isinstance(chk, dict)
            or chk.get("status") != "completed"
            or chk.get("conclusion") != "success"
        ):
            return {
                "passed": False,
                "reason": f"Check {name} not completed successfully: {chk}",
                "receipt_hash": r_hash,
            }
    return {
        "passed": True,
        "reason": "All required CI checks completed successfully",
        "receipt_hash": r_hash,
    }


def _get_num(d: Mapping[str, Any], path: Sequence[str]) -> float | None:
    cur: Any = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    v = cur.get("rate") if isinstance(cur, dict) else cur
    return (
        float(v)
        if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
        else None
    )


def _check_power_curve(c_pc: Any, b_pc: Any, ctx: str) -> str | None:
    if not isinstance(c_pc, list) or not isinstance(b_pc, list) or not c_pc or not b_pc:
        return f"Missing or empty power_curve list in {ctx}"
    b_map = {
        p.get("target_ic"): p for p in b_pc if isinstance(p, dict) and "target_ic" in p
    }
    c_map = {
        p.get("target_ic"): p for p in c_pc if isinstance(p, dict) and "target_ic" in p
    }
    if len(b_map) != len(b_pc) or len(c_map) != len(c_pc) or set(b_map) != set(c_map):
        return f"power_curve target_ic points mismatch in {ctx}"
    for ic, b_pt in b_map.items():
        c_pt = c_map[ic]
        for rk in RATE_KEYS:
            b_n, c_n = _get_num(b_pt, [rk]), _get_num(c_pt, [rk])
            if b_n is None or c_n is None:
                return f"Non-finite rate at {ic}:{rk} in {ctx}"
            if c_n < b_n:
                return f"Degradation at {ic}:{rk} in {ctx}: candidate {c_n} < baseline {b_n}"
    return None


def exact_block_control(
    repo_root: Path | str = Path("."),
) -> dict[str, Any]:
    """Execute judge-owned exact-block exchangeability serial null control.

    Supplementary diagnostic gate for E0 non-degradation under overlapping targets.
    Streams per-world simulations without retaining full panels in memory.
    """
    import hashlib
    import json
    import math
    from pathlib import Path
    import numpy as np
    from analysis import panel_insider_density as panel
    from evals.e0 import benchmark as e0_benchmark
    from evals.e0 import machinery as e0_machinery
    from evals.e0 import runners as e0_runners
    from evals.e0 import scorer as e0_scorer

    repo = Path(repo_root).resolve()
    cfg_file = repo / "evals" / "e3" / "config.json"
    if not cfg_file.exists():
        return {
            "passed": False,
            "reason": f"Config file not found: {cfg_file}",
            "receipt": {},
        }

    try:
        cfg = json.loads(cfg_file.read_text(encoding="utf-8"))
    except Exception as e:
        return {
            "passed": False,
            "reason": f"Failed to parse e3 config.json: {e}",
            "receipt": {},
        }

    if not isinstance(cfg, dict) or "serial_null_control" not in cfg:
        return {
            "passed": False,
            "reason": "Missing serial_null_control specification in e3 config",
            "receipt": {},
        }

    spec = cfg["serial_null_control"]
    if not isinstance(spec, dict):
        return {
            "passed": False,
            "reason": "serial_null_control specification must be a dictionary",
            "receipt": {},
        }

    # Validate static approved definition: exact-block-v1
    if spec.get("definition") != "exact-block-v1":
        return {
            "passed": False,
            "reason": f"Unsupported definition: {spec.get('definition')!r}",
            "receipt": {},
        }
    if spec.get("dgp") != "exact_block_exchangeability_repeats":
        return {
            "passed": False,
            "reason": f"Unsupported dgp: {spec.get('dgp')!r}",
            "receipt": {},
        }

    n_blocks = spec.get("n_blocks")
    h = spec.get("h")
    n_rows = spec.get("n_rows")
    entities_count = spec.get("entities")
    sims = spec.get("sims")
    seed = spec.get("seed")
    perms = spec.get("perms")
    trials = spec.get("trials")

    if (
        type(n_blocks) is not int
        or n_blocks != 60
        or type(h) is not int
        or h != 4
        or type(n_rows) is not int
        or n_rows != 240
        or type(entities_count) is not int
        or entities_count != 40
        or type(sims) is not int
        or sims != 300
        or type(seed) is not int
        or seed != 7
        or type(perms) is not int
        or perms != 999
        or type(trials) is not list
        or trials != ["A90|fwd5", "A90|fwd20", "A30|fwd5", "A30|fwd20"]
    ):
        return {
            "passed": False,
            "reason": "serial_null_control parameters do not match static approved definition",
            "receipt": {},
        }

    # Unchanged E0 selection configuration (.1, k1, .1, +1)
    try:
        e0_cfg = e0_benchmark.load_config()
        sel = e0_cfg["selection"]
        ledger_q = float(sel["ledger_q"])
        run_k = int(sel["run_k"])
        bh_q = float(sel["bh_q"])
        direction = int(sel["direction"])
    except Exception as e:
        return {
            "passed": False,
            "reason": f"Failed to load E0 selection configuration: {e}",
            "receipt": {},
        }

    if (ledger_q, run_k, bh_q, direction) != (0.1, 1, 0.1, 1):
        return {
            "passed": False,
            "reason": f"E0 selection parameters modified: {(ledger_q, run_k, bh_q, direction)}",
            "receipt": {},
        }

    selection = e0_runners.Selection(
        ledger_q=ledger_q, run_k=run_k, bh_q=bh_q, direction=direction
    )
    alpha = panel.run_alpha(run_k, ledger_q)
    threshold = alpha / len(trials)

    # Validate exact selection specification against loaded E0 config and computed thresholds
    expected_selection = {
        "ledger_q": ledger_q,
        "run_k": run_k,
        "bh_q": bh_q,
        "direction": direction,
        "alpha": alpha,
        "threshold": threshold,
    }
    if spec.get("selection") != expected_selection:
        return {
            "passed": False,
            "reason": "serial_null_control selection spec does not match E0 configuration",
            "receipt": {},
        }

    rng = np.random.default_rng(seed)
    decision_at = [f"d_{i:03d}" for i in range(n_rows)]
    label_end = [f"l_{i:03d}" for i in range(n_rows)]
    entities = [f"E_{j:02d}" for j in range(entities_count)]

    # Stream simulation rows per world to prevent storing full panels in memory
    clean_sim_rows = []
    for s_idx in range(sims):
        trials_rec = {}
        for trial_name in trials:
            horizon = int(trial_name.split("fwd")[-1]) if "fwd" in trial_name else h
            feat_60 = rng.standard_normal((n_blocks, entities_count))
            label_60 = rng.standard_normal((n_blocks, entities_count))
            feat_240 = np.repeat(feat_60, h, axis=0)
            label_240 = np.repeat(label_60, h, axis=0)
            tp = panel.TrialPanel(
                trial=trial_name,
                window="discovery",
                horizon=horizon,
                decision_at=decision_at,
                label_end=label_end,
                entities=entities,
                feature=feat_240,
                label=label_240,
            )
            rec = panel.measure_trial(tp, block=h, perms=perms, sensitivity=False)
            if not isinstance(rec, dict):
                return {
                    "passed": False,
                    "reason": f"measure_trial returned non-mapping for {trial_name} in world {s_idx}",
                    "receipt": {},
                }
            if rec.get("status") != "tested":
                return {
                    "passed": False,
                    "reason": f"Trial {trial_name} in world {s_idx} status != 'tested': {rec.get('status')}",
                    "receipt": {},
                }
            n_val = rec.get("n")
            if type(n_val) is not int or n_val != n_rows:
                return {
                    "passed": False,
                    "reason": f"Trial {trial_name} in world {s_idx} n != {n_rows}: {n_val}",
                    "receipt": {},
                }
            p_val = rec.get("p")
            if (
                type(p_val) is bool
                or not isinstance(p_val, (int, float))
                or not math.isfinite(p_val)
                or not (0.0 <= p_val <= 1.0)
            ):
                return {
                    "passed": False,
                    "reason": f"Trial {trial_name} in world {s_idx} invalid p: {p_val}",
                    "receipt": {},
                }
            mean_ic = rec.get("mean_ic")
            if (
                type(mean_ic) is bool
                or not isinstance(mean_ic, (int, float))
                or not math.isfinite(mean_ic)
            ):
                return {
                    "passed": False,
                    "reason": f"Trial {trial_name} in world {s_idx} invalid mean_ic: {mean_ic}",
                    "receipt": {},
                }
            trials_rec[trial_name] = rec

        decisions = e0_runners.family_decision(trials_rec, selection)
        sim_clean = {
            "sim": s_idx,
            "trials": {t: {**rec, **decisions[t]} for t, rec in trials_rec.items()},
        }
        clean_sim_rows.append(sim_clean)

    if len(clean_sim_rows) != sims:
        return {
            "passed": False,
            "reason": f"Simulations count {len(clean_sim_rows)} != {sims}",
            "receipt": {},
        }

    clean_scored = e0_scorer.score_scenario(
        {"0": clean_sim_rows},
        primary=trials[0],
        planted=[],
        threshold=threshold,
        scales={},
        direction=direction,
    )

    clean_fdr_pass = bool(
        e0_benchmark._within(clean_scored["fdr"]["global_null"]["fdr_bh"], bh_q)
    )
    clean_fwer_pass = bool(
        e0_benchmark._within(
            clean_scored["fdr"]["global_null"]["fwer_holm_run_alpha"], alpha
        )
    )

    clean_ks_stat = float(clean_scored["null_calibration"]["pooled"]["ks_stat"])
    clean_ks_crit = float(clean_scored["null_calibration"]["pooled"]["critical_5pct"])
    clean_ks_pass = bool(clean_ks_stat <= clean_ks_crit)

    passed = bool(clean_fdr_pass and clean_fwer_pass and clean_ks_pass)
    reasons = []
    if not clean_fdr_pass:
        reasons.append("FDR threshold exceeded")
    if not clean_fwer_pass:
        reasons.append("FWER threshold exceeded")
    if not clean_ks_pass:
        reasons.append("Strict KS uniform calibration failed")
    reason = (
        "Exact block exchangeability control passed (FDR, FWER, and KS calibrated)"
        if passed
        else f"Exact block control failed: {'; '.join(reasons)}"
    )

    spec_sha = hashlib.sha256(
        json.dumps(spec, sort_keys=True).encode("utf-8")
    ).hexdigest()
    cur_mach = e0_machinery.machinery_fingerprint(repo)
    mach_sha = hashlib.sha256(
        json.dumps(cur_mach, sort_keys=True).encode("utf-8")
    ).hexdigest()

    receipt = {
        "dgp": spec.get("dgp", "exact_block_exchangeability_repeats"),
        "definition": spec.get("definition", "exact-block-v1"),
        "spec_sha256": spec_sha,
        "machinery_sha256": mach_sha,
        "machinery_fingerprint": cur_mach,
        "params": {
            "n_blocks": n_blocks,
            "h": h,
            "n_rows": n_rows,
            "entities": entities_count,
            "sims": sims,
            "seed": seed,
            "perms": perms,
            "trials": list(trials),
            "selection": {
                "ledger_q": ledger_q,
                "run_k": run_k,
                "bh_q": bh_q,
                "direction": direction,
                "alpha": alpha,
                "threshold": threshold,
            },
        },
        "source_pins": spec.get("provenance", {}).get(
            "source_pins", spec.get("source_pins", {})
        ),
        "raw_scores": {
            "fdr": clean_scored.get("fdr", {}).get("global_null", {}).get("fdr_bh"),
            "fwer": clean_scored.get("fdr", {})
            .get("global_null", {})
            .get("fwer_holm_run_alpha"),
            "null_calibration": {
                "ks_stat": clean_ks_stat,
                "critical_5pct": clean_ks_crit,
                "ks_p": clean_scored.get("null_calibration", {})
                .get("pooled", {})
                .get("ks_p"),
            },
        },
        "flags": {
            "fdr_bh_controlled": clean_fdr_pass,
            "fwer_holm_controlled": clean_fwer_pass,
            "null_p_uniform_ks": clean_ks_pass,
        },
        "passed": passed,
    }

    return {
        "passed": passed,
        "reason": reason,
        "receipt": receipt,
    }


def e0_gate(
    candidate_kind: str,
    *,
    baseline_hashes: Mapping[str, str],
    baseline_cards: Mapping[str, Mapping[str, Any]],
    baseline_digests: Mapping[str, str],
    repo_root: Path | str = Path("."),
    runner: Callable[[Mapping[str, Any], str], Mapping[str, Any]] | None = None,
    profile: str = "ci",
) -> dict[str, Any]:
    """Evaluate E0 non-degradation against released versions or fingerprint."""
    if candidate_kind not in (
        "feature",
        "parameter",
        "machinery-change",
        "generator-change",
    ):
        return {
            "passed": False,
            "reason": f"Invalid candidate_kind: {candidate_kind!r}",
            "details": {},
        }
    if not all(
        isinstance(x, dict) for x in (baseline_hashes, baseline_cards, baseline_digests)
    ):
        return {
            "passed": False,
            "reason": "Invalid baseline argument types",
            "details": {},
        }
    repo = Path(repo_root).resolve()
    try:
        cur_mach = machinery.machinery_fingerprint(repo)
    except Exception as e:
        return {
            "passed": False,
            "reason": f"Failed machinery fingerprint: {e}",
            "details": {},
        }

    mach_keys = set(getattr(machinery, "MACHINERY_FILES", cur_mach.keys()))
    if (
        not baseline_hashes
        or set(baseline_hashes.keys()) != mach_keys
        or not all(
            isinstance(v, str)
            and len(v) == 64
            and all(c in "0123456789abcdef" for c in v)
            for v in baseline_hashes.values()
        )
    ):
        return {
            "passed": False,
            "reason": "Missing or invalid baseline_hashes",
            "details": {},
        }

    if candidate_kind in ("feature", "parameter"):
        if cur_mach == baseline_hashes:
            return {
                "passed": True,
                "reason": "Trivial pass: machinery fingerprint matches baseline",
                "details": {
                    "trivial_pass": True,
                    "machinery_sha256": cur_mach,
                    "cards": {},
                },
            }

    rel_file = repo / "evals" / "RELEASED.json"
    if not rel_file.exists():
        return {
            "passed": False,
            "reason": f"RELEASED.json not found: {rel_file}",
            "details": {},
        }
    try:
        registry = json.loads(rel_file.read_text(encoding="utf-8"))
        raw = registry.get("entries") if isinstance(registry, dict) else None
        entries = (
            [
                e
                for e in raw
                if isinstance(e, dict)
                and e.get("kind") == "suite"
                and e.get("suite") == "e0"
            ]
            if isinstance(raw, list)
            else []
        )
    except Exception as e:
        return {
            "passed": False,
            "reason": f"Failed to parse RELEASED.json: {e}",
            "details": {},
        }
    if not entries:
        return {
            "passed": False,
            "reason": "No released E0 suite entries found in registry",
            "details": {},
        }

    fresh_cards: dict[str, Any] = {}
    for entry in entries:
        ver, reg_sha, pkg_str = (
            entry.get("version"),
            entry.get("manifest_sha256"),
            entry.get("path"),
        )
        if (
            not ver
            or not reg_sha
            or not pkg_str
            or ver not in baseline_cards
            or ver not in baseline_digests
        ):
            return {
                "passed": False,
                "reason": f"Missing entry metadata or baseline for {ver}",
                "details": {},
            }
        base_card = baseline_cards[ver]
        if (
            not isinstance(base_card, dict)
            or hashlib.sha256(
                json.dumps(base_card, sort_keys=True).encode("utf-8")
            ).hexdigest()
            != baseline_digests[ver]
        ):
            return {
                "passed": False,
                "reason": f"Baseline card digest mismatch for {ver}",
                "details": {},
            }

        pkg_path = pkg_str.replace("/", ".").strip(".")
        try:
            verified = importlib.import_module(f"{pkg_path}.manifest").verify()
        except Exception as e:
            return {
                "passed": False,
                "reason": f"Manifest verification failed for {ver}: {e}",
                "details": {},
            }
        suite_sha = (
            verified
            if isinstance(verified, str)
            else (
                verified.get("manifest_sha256") or verified.get("sha256")
                if isinstance(verified, dict)
                else ""
            )
        )
        if suite_sha != reg_sha:
            return {
                "passed": False,
                "reason": f"Manifest sha mismatch for {ver}",
                "details": {},
            }

        if (
            base_card.get("version") != ver
            or (
                base_card.get("manifest") != verified
                and base_card.get("manifest") != reg_sha
            )
            or base_card.get("machinery_sha256") != baseline_hashes
        ):
            return {
                "passed": False,
                "reason": f"Baseline card integrity mismatch for {ver}",
                "details": {},
            }

        try:
            card = (
                runner(entry, profile)
                if runner
                else importlib.import_module(f"{pkg_path}.benchmark").run(
                    profile=profile, replicate=False
                )
            )
        except Exception as e:
            return {
                "passed": False,
                "reason": f"Runner failed for {ver}: {e}",
                "details": {},
            }

        if (
            not isinstance(card, dict)
            or card.get("version") != ver
            or (card.get("manifest") != verified and card.get("manifest") != reg_sha)
        ):
            return {
                "passed": False,
                "reason": f"Candidate card version/manifest mismatch for {ver}",
                "details": {},
            }
        if card.get("machinery_sha256") != cur_mach:
            return {
                "passed": False,
                "reason": f"Candidate card machinery fingerprint mismatch for {ver}",
                "details": {},
            }

        b_prof, c_prof = base_card.get("profile"), card.get("profile")
        b_pname = (
            b_prof.get("name")
            if isinstance(b_prof, dict)
            else getattr(b_prof, "name", None)
        )
        c_pname = (
            c_prof.get("name")
            if isinstance(c_prof, dict)
            else getattr(c_prof, "name", None)
        )
        b_pscens = (
            b_prof.get("scenarios")
            if isinstance(b_prof, dict)
            else getattr(b_prof, "scenarios", None)
        )
        c_pscens = (
            c_prof.get("scenarios")
            if isinstance(c_prof, dict)
            else getattr(c_prof, "scenarios", None)
        )
        if b_pname != profile or c_pname != profile or b_pname != c_pname:
            return {
                "passed": False,
                "reason": f"Profile mismatch for {ver}",
                "details": {},
            }
        if (
            b_pscens is not None
            and c_pscens is not None
            and set(b_pscens) != set(c_pscens)
        ):
            return {
                "passed": False,
                "reason": f"Profile scenario set mismatch for {ver}",
                "details": {},
            }

        c_scens, b_scens = card.get("scenarios"), base_card.get("scenarios")
        if (
            not isinstance(c_scens, dict)
            or not isinstance(b_scens, dict)
            or not c_scens
            or not b_scens
            or set(c_scens.keys()) != set(b_scens.keys())
        ):
            return {
                "passed": False,
                "reason": f"Scenarios missing, empty or mismatch for {ver}",
                "details": {},
            }
        prof_scens = c_pscens if c_pscens is not None else b_pscens
        if prof_scens is not None and (
            set(c_scens.keys()) != set(prof_scens)
            or set(b_scens.keys()) != set(prof_scens)
        ):
            return {
                "passed": False,
                "reason": f"Profile scenarios mismatch with card scenarios for {ver}",
                "details": {},
            }

        checks, b_checks = card.get("checks"), base_card.get("checks")
        if not isinstance(checks, dict) or not isinstance(b_checks, dict):
            return {
                "passed": False,
                "reason": f"Checks missing or invalid for {ver}",
                "details": {},
            }
        if set(checks.keys()) != set(c_scens.keys()) or set(b_checks.keys()) != set(
            b_scens.keys()
        ):
            return {
                "passed": False,
                "reason": f"Exact scenario check coverage mismatch for {ver}",
                "details": {},
            }

        for sc_name, b_sc in b_scens.items():
            c_sc = c_scens[sc_name]
            if not isinstance(c_sc, dict) or not isinstance(b_sc, dict):
                return {
                    "passed": False,
                    "reason": f"Scenario {sc_name} must be dict in {ver}",
                    "details": {},
                }

            b_chk, c_chk = b_checks.get(sc_name), checks.get(sc_name)
            if not isinstance(b_chk, dict) or not all(
                flg in b_chk for flg in SCENARIO_FLAGS
            ):
                return {
                    "passed": False,
                    "reason": f"Baseline checks missing scenario flags in {ver}:{sc_name}",
                    "details": {},
                }
            if not isinstance(c_chk, dict) or not all(
                c_chk.get(flg) is True for flg in SCENARIO_FLAGS
            ):
                return {
                    "passed": False,
                    "reason": f"Candidate checks missing True scenario flags in {ver}:{sc_name}",
                    "details": {},
                }

            for path in SECTION_PATHS:
                p_str = ".".join(path)
                if _get_num(b_sc, path) is None or _get_num(c_sc, path) is None:
                    return {
                        "passed": False,
                        "reason": f"Missing or non-finite {p_str} in {ver}:{sc_name}",
                        "details": {},
                    }

            err = _check_power_curve(
                c_sc.get("power_curve"), b_sc.get("power_curve"), f"{ver}:{sc_name}"
            )
            if err:
                return {"passed": False, "reason": err, "details": {}}

        fresh_cards[ver] = card
    try:
        ctrl = exact_block_control(repo_root=repo)
    except Exception as e:
        return {
            "passed": False,
            "reason": f"Exact block control exception: {e}",
            "details": {"cards": fresh_cards, "serial_null": {}},
        }

    ctrl_receipt = ctrl.get("receipt", {}) if isinstance(ctrl, dict) else {}
    if not isinstance(ctrl, dict) or ctrl.get("passed") is not True:
        fail_reason = (
            ctrl.get("reason", "Exact block control check failed")
            if isinstance(ctrl, dict)
            else "Invalid exact block control result"
        )
        return {
            "passed": False,
            "reason": f"E0 gate failed supplementary exact block control: {fail_reason}",
            "details": {"cards": fresh_cards, "serial_null": ctrl_receipt},
        }

    return {
        "passed": True,
        "reason": "All released E0 versions verified without degradation and exact block control passed",
        "details": {"cards": fresh_cards, "serial_null": ctrl_receipt},
    }


def evaluate_gates(
    commit_sha: str,
    receipt: Mapping[str, Any],
    candidate_kind: str,
    *,
    baseline_hashes: Mapping[str, str],
    baseline_cards: Mapping[str, Mapping[str, Any]],
    baseline_digests: Mapping[str, str],
    repo_root: Path | str = Path("."),
    runner: Callable[[Mapping[str, Any], str], Mapping[str, Any]] | None = None,
    profile: str = "ci",
    required_checks: Sequence[str] = MANDATORY_CI,
) -> dict[str, Any]:
    """Run E1 and E0 gates, returning passed conjunction and digest receipt."""
    ci_res = ci_gate(commit_sha, receipt, required_checks=required_checks)
    e0_res = e0_gate(
        candidate_kind,
        baseline_hashes=baseline_hashes,
        baseline_cards=baseline_cards,
        baseline_digests=baseline_digests,
        repo_root=repo_root,
        runner=runner,
        profile=profile,
    )
    out = {
        "passed": bool(ci_res.get("passed") and e0_res.get("passed")),
        "ci": ci_res,
        "e0": e0_res,
        "receipt_hash": ci_res.get("receipt_hash", ""),
    }
    out["digest"] = hashlib.sha256(
        json.dumps(out, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return out
