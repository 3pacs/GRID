"""Shared fixtures for the panel-mode tests (synthetic data and temp git vaults only).

Nothing here reads a price, a label or an IC of any real sector: every
feature and label is drawn from a seeded RNG.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_mode as pm
from analysis.research_forward_log import canonical

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
HEX = "ab" * 32


def git(cwd, *argv, check=True):
    result = subprocess.run(
        ["git", "-c", "user.name=gd6-test", "-c", "user.email=gd6-test@example.invalid",
         "-c", "commit.gpgsign=false", "-c", "core.autocrlf=false", "-c", "init.defaultBranch=main", *argv],
        cwd=cwd, capture_output=True, text=True, check=False,
    )
    if check:
        assert result.returncode == 0, result.stderr
    return result.stdout


class Vault:
    """A stand-in for the GitHub vault: a bare 'remote', a worktree on main, a fetch cache."""

    def __init__(self, root: Path) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.remote, self.worktree, self.cache = root / "remote.git", root / "worktree", root / "cache"
        git(root, "init", "-q", "--bare", str(self.remote))
        git(self.remote, "symbolic-ref", "HEAD", "refs/heads/main")
        git(root, "init", "-q", str(self.worktree))
        git(self.worktree, "checkout", "-q", "-b", "main")
        (self.worktree / "README.md").write_text("vault\n")
        git(self.worktree, "add", "README.md")
        git(self.worktree, "commit", "-q", "-m", "init")
        git(self.worktree, "remote", "add", "origin", str(self.remote))
        self.push()
        git(root, "init", "-q", str(self.cache))

    def commit(self, message="anchors", push=True):
        git(self.worktree, "add", "-A")
        git(self.worktree, "commit", "-q", "-m", message)
        if push:
            self.push()

    def push(self, ref="HEAD:refs/heads/main", force=False):
        git(self.worktree, "push", "-q", *(["--force"] if force else []), "origin", ref)

    def write(self, rel: str, data: bytes, message="file") -> None:
        path = self.worktree / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        self.commit(message)

    def publish(self, reg: pm.PanelRegistry) -> None:
        pm.export_anchors(reg, self.worktree)
        self.commit("granular anchors")

    def witness(self, registry_id: str) -> pm.GranularWitness:
        return pm.check_offhost(self.cache, registry_id, remote_url=str(self.remote))


def anchor_line(head: str, records: int, prev: bytes | None = None) -> bytes:
    import hashlib

    return canonical({"head_sha256": head, "prev_anchor_sha256": hashlib.sha256(prev).hexdigest() if prev else None,
                      "records": records, "run_at": "2026-09-30T00:00:00+00:00"})


def v8_terminal_witness(monkeypatch, root: Path) -> pm.V8TerminalWitness:
    """Lift R2 in a test: a temp vault whose v8 witness ends in a pinned (synthetic) terminal head."""
    vault = Vault(root)
    first = anchor_line("11" * 32, 2)
    last = anchor_line("22" * 32, 9, first)
    vault.write(pm.V8_WITNESS_PATH, first + b"\n" + last + b"\n", "v8 terminal")
    monkeypatch.setattr(pm, "V8_TERMINAL_PIN", {"head_sha256": "22" * 32, "records": 9, "status": "STOP"})
    return pm.issue_v8_terminal_witness(vault.cache, remote_url=str(vault.remote))


def construct(name="A90", window=90, horizons=(5, 20), direction=1, confirmatory=(20,),
              magnitude="positive_vs_zero", **kw) -> pm.ConstructSpec:
    return pm.ConstructSpec(name=name, feature_class=kw.pop("feature_class", "people_density_form4"),
                            scorer=kw.pop("scorer", f"gd5:{name}"), window_days=window, horizons=tuple(horizons),
                            direction=direction, confirmatory_horizons=tuple(confirmatory),
                            artifact_kinds=kw.pop("artifact_kinds", ("people_density",)), magnitude=magnitude, **kw)


def run_spec(sectors=("Technology",), benchmarks=None, *, discovery_start=v1.DISCOVERY_START, split=v1.SPLIT,
             end=v1.END, prereg=HEX, registry_id="gd6-test", run_id="gd6-test-run", perms=v1.PERMS,
             seed=v1.SEED, option="separate", k=1, **kw) -> pm.PanelRunSpec:
    benchmarks = benchmarks or tuple((s, v1.EQUITY_SECTORS.get(s, "SPY")) for s in sectors)
    opt = pm.LEDGER_OPTIONS[option]
    return pm.PanelRunSpec(run_id=run_id, registry_id=registry_id, ledger_option=option,
                           ledger_id=opt["ledger_id"], ledger_q=opt["q"], run_k=k, sectors=tuple(sectors),
                           benchmarks=tuple(benchmarks), discovery_start=discovery_start, split=split, end=end,
                           prereg_sha256=prereg, perms=perms, seed=seed, **kw)


def synthetic_panel(trial: str, window: str, start: str, n_dec: int, n_ent: int, seed: int, *,
                    plant: float = 0.0, entities=None) -> pm.TrialPanel:
    """A seeded insider-like panel (sparse non-negative feature, Gaussian label, optional planted IC)."""
    horizon = pm.trial_horizon(trial)
    rng = np.random.default_rng([seed, n_dec, n_ent, horizon])
    days = pd.bdate_range(start, periods=(n_dec + 1) * horizon + 1)
    decided = v1.decision_instants([days[i * horizon].date() for i in range(n_dec)])
    ends = v1.decision_instants([days[i * horizon + horizon].date() for i in range(n_dec)])
    feature = rng.poisson(0.4, size=(n_dec, n_ent)).astype(float)
    feature[rng.random((n_dec, n_ent)) < 0.05] = np.nan
    label = 0.02 * rng.standard_normal((n_dec, n_ent)) + plant * 0.02 * np.nan_to_num(feature)
    label[rng.random((n_dec, n_ent)) < 0.03] = np.nan
    momentum = 0.03 * rng.standard_normal((n_dec, n_ent))
    largest = np.where(np.nan_to_num(feature) > 0, rng.choice([2e5, 8e5], size=(n_dec, n_ent)), 0.0)
    return pm.TrialPanel(trial=trial, window=window, horizon=horizon,
                         decision_at=[d.isoformat() for d in decided], label_end=[d.isoformat() for d in ends],
                         entities=list(entities or [f"T{j:03d}" for j in range(n_ent)]), feature=feature,
                         label=label, momentum=momentum, largest=largest)


def dumps(value) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=True)


# --- an end-to-end custody fixture (synthetic prices served by a fake read_window) -------------

FLOW_SECTOR = "Energy"
FLOW_BENCH = "XLE"
SECTOR_TICKERS = {"Energy": [f"E{j:02d}" for j in range(25)], "Utilities": [f"U{j:02d}" for j in range(25)]}
FLOW_TICKERS = SECTOR_TICKERS[FLOW_SECTOR]
FLOW_WINDOWS = {"discovery_start": "2026-07-01T00:00:00+00:00", "split": "2028-07-01T00:00:00+00:00",
                "end": "2030-07-01T00:00:00+00:00"}


def write_prereg(repo_root: Path, body: str = "GD6 test pre-registration body\n") -> tuple[str, str]:
    rel = "docs/paper_log/gd6-test-preregistration.md"
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# test\n{v1.BODY_START}\n{body}{v1.BODY_END}\n", encoding="utf-8", newline="\n")
    return rel, v1.prereg_body_sha256(path)


def synthetic_closes(seed: int = 11) -> pd.DataFrame:
    days = pd.bdate_range("2026-06-01", "2030-06-28")
    rng = np.random.default_rng(seed)
    cols = [t for s in SECTOR_TICKERS for t in SECTOR_TICKERS[s]] + [v1.EQUITY_SECTORS[s] for s in SECTOR_TICKERS]
    return pd.DataFrame(np.exp(np.cumsum(0.01 * rng.standard_normal((len(days), len(cols))), axis=0)) * 40,
                        index=days, columns=cols)


class FakeReader:
    """Stands in for ``store.observations.read_window``; records every call (the split-first spy)."""

    def __init__(self, closes: pd.DataFrame) -> None:
        self.closes, self.calls = closes, []

    def __call__(self, conn, series_id, *, source=None, start=None, as_of=None, as_of_ts=None):
        from store.observations import Observation

        self.calls.append({"series_id": series_id, "source": source, "start": start, "as_of": as_of,
                           "as_of_ts": as_of_ts})
        ticker = series_id.split(":")[1]
        col = self.closes[ticker]
        sel = col[(col.index.date >= start) & (col.index.date <= as_of)].dropna()
        return [Observation(series_id=series_id, obs_date=d.date(), value=float(v), source=source)
                for d, v in sel.items()]


def flow_construct() -> pm.ConstructSpec:
    return construct("gd5_form4_buy_w60", 60, horizons=(5, 20), confirmatory=(5,), channels=("form4",),
                     event_filter="P")


def sector_manifest(sector: str):
    from analysis import panel_prices as pp

    bench = v1.EQUITY_SECTORS[sector]
    return pp.PanelPriceManifest(sector=sector, source=pp.PRICE_SOURCE, series_template=pp.SERIES_TEMPLATE,
                                 basis="split+dividend adjusted", benchmark=bench, calendar=bench,
                                 admitted=tuple(sorted(SECTOR_TICKERS[sector] + [bench])),
                                 probe_report_sha256="cd" * 32)


def flow_setup(tmp_path: Path, monkeypatch, *, registry_id="gd6-flow", supersedes=(), entity_split_salt=None,
               sectors=(FLOW_SECTOR,)):
    monkeypatch.setattr(pm, "OWNER_LEDGER_DECISION", "separate")
    repo_root = tmp_path / "repo"
    rel, prereg = write_prereg(repo_root)
    run = run_spec(sectors=tuple(sectors), registry_id=registry_id, prereg=prereg, perms=999,
                   supersedes=tuple(supersedes), entity_split_salt=entity_split_salt, **FLOW_WINDOWS)
    reg = pm.PanelRegistry(tmp_path / f"registry-{registry_id}", registry_id, prereg)
    manifests = {s: sector_manifest(s) for s in sectors}
    inputs = pm.PanelInputs(artifacts=(("ef" * 32, "fe" * 32),),
                            price_manifest_sha256s=tuple((s, m.digest()) for s, m in manifests.items()),
                            as_of_ts="2026-09-30T00:00:00+00:00")
    reader = FakeReader(synthetic_closes())
    monkeypatch.setattr("store.observations.read_window", reader)
    return {"repo_root": repo_root, "prereg_path": rel, "prereg": prereg, "run": run, "reg": reg,
            "construct": flow_construct(), "manifests": manifests, "manifest": manifests[sectors[0]],
            "inputs": inputs, "reader": reader, "vault": Vault(tmp_path / f"vault-{registry_id}")}


def flow_features(closes: pd.DataFrame, tickers, seed: int = 3) -> pd.DataFrame:
    """Decision-instant x ticker features (sparse counts), as a frozen artifact would carry them."""
    rng = np.random.default_rng(seed)
    instants = v1.decision_instants([d.date() for d in closes.index])
    return pd.DataFrame(rng.poisson(0.5, size=(len(instants), len(tickers))).astype(float),
                        index=instants, columns=list(tickers))
