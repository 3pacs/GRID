"""Tests for the VS1 holdout-period basis-report tool and holdout abstention wiring.

Synthetic fixtures only: in-memory SQLite, local bare git vault, fake vendor files,
pinned synthetic XNYS calendar manifest. Never reads /data, never hits the network.
"""

from __future__ import annotations

import dataclasses
import json
import sqlite3
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, create_engine

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4
from analysis.research_forward_log import ForwardLog, canonical

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

TIINGO, YFINANCE, TD_SPLITS, KAGGLE = 524, 2, 1035, 522
DISC_PULL = datetime(2026, 4, 7, 10, 0)
HOLDOUT_PULL = datetime(2026, 10, 6, 9, 0)
DISC_FROZEN_AT = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
FETCHED_AFTER_SEAL = datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc)
SNAPSHOT = datetime(2026, 10, 6, 18, 0, tzinfo=timezone.utc)
HO_LO = date(2020, 1, 1)
HO_HI = date(2026, 6, 30)
CODE_SHA = "c" * 40
N_HOLDOUT = 300  # > 250 cross-check min_pairs


# --- synthetic fixture builders --------------------------------------------------------------------


def _weekdays(start: date, count: int) -> list[date]:
    out, d = [], start
    while len(out) < count:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


BOUNDARY_ANCHOR = date(2019, 12, 31)
HO_SESSIONS = _weekdays(date(2020, 1, 2), N_HOLDOUT)  # 2020-01-01 is New Year's Day (holiday)


def _write_calendar_manifest(path: Path, sessions: list[date], *, start: date = date(2019, 12, 1),
                             end: date = HO_HI) -> Path:
    rows = []
    for d in sessions:
        rows.append({
            "key": f"XNYS:{d.isoformat()}:RTH",
            "opens": f"{d.isoformat()}T14:30:00+00:00",
            "closes": f"{d.isoformat()}T21:00:00+00:00",
        })
    doc = {
        "schema": "grid.xnys-session-manifest.v1",
        "exchange": "XNYS",
        "session_type": "RTH",
        "timezone": "America/New_York",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "calendar_version": (
            f"exchange_calendars:4.11.2:pandas:2.3.3:numpy:2.4.6:tzdata:2026.6:"
            f"XNYS:{start.isoformat()}:{end.isoformat()}"
        ),
        "sessions": rows,
    }
    raw = json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")
    path.write_bytes(raw)
    return path


def _series(days: list[date], base: float = 41.237, *, split_at: int | None = None,
            split: float = 2.0, dividends: tuple[int, ...] = ()) -> tuple[dict[date, float], dict[date, float]]:
    close = {}
    for i, d in enumerate(days):
        c = base * (1 + 0.013 * (((i * 7) % 11) - 5) / 5)
        if split_at is not None and i >= split_at:
            c /= split
        close[d] = round(c, 4)
    factor, f = {}, 1.0
    for i in range(len(days) - 1, -1, -1):
        factor[days[i]] = f
        if split_at is not None and i == split_at:
            f /= split
        if i in dividends:
            f *= 1 - 0.37 / close[days[i - 1]]
    adj = {d: close[d] * factor[d] for d in days}
    return close, adj


def _td_payload(closes: dict[date, float], symbol: str) -> bytes:
    values = [{"datetime": d.isoformat(), "close": f"{v:.5f}"} for d, v in sorted(closes.items(), reverse=True)]
    return json.dumps({"meta": {"symbol": symbol}, "values": values, "status": "ok"}).encode("utf-8")


def _write_td_dir(td_dir: Path, series: dict[str, tuple[dict[date, float], dict[date, float]]], *,
                  fetched_at: datetime = FETCHED_AFTER_SEAL, bad_tickers: tuple[str, ...] = (),
                  missing_tickers: tuple[str, ...] = (), extra_dates: dict[str, dict[date, float]] | None = None,
                  include_boundary: dict[str, tuple[float, float]] | None = None) -> Path:
    td_dir.mkdir(parents=True, exist_ok=True)
    log = fetch.FetchLog(td_dir / "fetch_log.jsonl")
    extra_dates = extra_dates or {}
    include_boundary = include_boundary or {}
    for ticker, (close, adj) in sorted(series.items()):
        sym = fetch.td_symbol(ticker)
        for adjust in fetch.TD_ADJUST_MODES:
            key = f"{ticker}|{adjust}"
            if ticker in missing_tickers:
                body = json.dumps({"code": 400, "message": "not found", "status": "error"}).encode()
                name = f"{ticker}/{adjust}.json.gz"
                log.append({
                    "key": key, "ticker": ticker, "adjust": adjust, "outcome": "unavailable",
                    "fetched_at": fetched_at.isoformat(), "td_code": 400,
                    "file": name, "body_sha256": fetch.sha256_bytes(body),
                    "file_sha256": fetch.write_gz(td_dir / name, body),
                })
                continue
            vals = dict(adj if adjust == "all" else close)
            if ticker in include_boundary:
                b_close, b_adj = include_boundary[ticker]
                vals[BOUNDARY_ANCHOR] = b_adj if adjust == "all" else b_close
            if ticker in extra_dates:
                vals.update(extra_dates[ticker])
            if ticker in bad_tickers:
                for i, d in enumerate(sorted(vals)):
                    if i % 3 == 0:
                        vals[d] *= 1.03
            body = _td_payload(vals, sym)
            name = f"{ticker}/{adjust}.json.gz"
            dates_sorted = sorted(d.isoformat() for d in vals)
            log.append({
                "key": key, "ticker": ticker, "adjust": adjust, "outcome": "ok",
                "fetched_at": fetched_at.isoformat(), "values": len(vals),
                "first": dates_sorted[0] if dates_sorted else None,
                "last": dates_sorted[-1] if dates_sorted else None,
                "file": name, "body_sha256": fetch.sha256_bytes(body),
                "file_sha256": fetch.write_gz(td_dir / name, body),
            })
    return td_dir


def _write_tiingo_meta_dir(meta_dir: Path, tickers: list[str], *,
                           start_dates: dict[str, str] | None = None,
                           names: dict[str, str] | None = None,
                           missing: tuple[str, ...] = (),
                           fetched_at: datetime = FETCHED_AFTER_SEAL) -> Path:
    meta_dir.mkdir(parents=True, exist_ok=True)
    log = fetch.FetchLog(meta_dir / "fetch_log.jsonl")
    start_dates = start_dates or {}
    names = names or {}
    for t in sorted(set(tickers)):
        if t in missing:
            body = b'{"detail":"Not found."}'
            name = f"{t}.json.gz"
            log.append({
                "key": t, "ticker": t, "outcome": "unavailable", "fetched_at": fetched_at.isoformat(),
                "file": name, "body_sha256": fetch.sha256_bytes(body),
                "file_sha256": fetch.write_gz(meta_dir / name, body),
            })
            continue
        doc = {
            "ticker": t, "name": names.get(t, f"{t} Systems Inc"), "exchangeCode": "NYSE",
            "startDate": start_dates.get(t, "2010-01-04"), "endDate": "2026-06-30",
        }
        body = json.dumps(doc).encode("utf-8")
        name = f"{t}.json.gz"
        log.append({
            "key": t, "ticker": t, "outcome": "ok", "fetched_at": fetched_at.isoformat(),
            "file": name, "body_sha256": fetch.sha256_bytes(body),
            "file_sha256": fetch.write_gz(meta_dir / name, body),
        })
    return meta_dir


def _write_manifest(path: Path, admitted: list[str], listed_from: dict[str, str], *,
                    benchmark: str = "XLK") -> tuple[Path, str]:
    doc = {
        "admitted": sorted(admitted),
        "basis": "split+dividend adjusted",
        "benchmark": benchmark,
        "crosscheck_report_sha256": "c" * 64,
        "listed_from": dict(sorted(listed_from.items())),
        "probe_report_sha256": "a" * 64,
        "series_template": "YF:{ticker}:adj_close",
        "source": "TIINGO",
        "tiingo_meta_report_sha256": "d" * 64,
    }
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    digest_val = v1.digest({
        **doc,
        "admitted": list(sorted(admitted)),
        "listed_from": [[t, d] for t, d in sorted(listed_from.items())],
    })
    return path, digest_val


class _LocalVault:
    """Minimal local bare-remote + cache git repo for off-host witness verification."""

    def __init__(self, root: Path) -> None:
        self.remote = root / "remote.git"
        self.work = root / "work"
        self.cache = root / "cache"
        self._git(None, "init", "--bare", "--initial-branch=main", str(self.remote))
        self._git(None, "init", "--initial-branch=main", str(self.work))
        self._git(self.work, "config", "user.email", "test@grid.local")
        self._git(self.work, "config", "user.name", "GRID Test")
        self._git(self.work, "remote", "add", "origin", str(self.remote))
        self._git(None, "init", "--bare", str(self.cache))

    @staticmethod
    def _git(cwd: Path | None, *args: str) -> str:
        cmd = ["git", *(["-C", str(cwd)] if cwd else []), *args]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return res.stdout

    def publish_anchors(self, anchor_file: Path, witness_relpath: str, msg: str = "witness") -> None:
        dst = self.work / witness_relpath
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(anchor_file.read_bytes())
        self._git(self.work, "add", witness_relpath)
        self._git(self.work, "commit", "-m", msg)
        self._git(self.work, "push", "origin", "main")


def _make_sealed_registry(
    tmp_path: Path,
    *,
    study: str = "vs1-v9",
    prereg_sha256: str | None = None,
    manifest_digest: str = "e" * 64,
    seal_discovery: bool = True,
    witness_discovery_frozen: bool = True,
    frozen_at: datetime = DISC_FROZEN_AT,
) -> tuple[Path, _LocalVault, str, str]:
    """Create a synthetic registry chain + off-host witness up to discovery_frozen (or earlier)."""
    if prereg_sha256 is None:
        from analysis import panel_insider_density_v9 as v9

        prereg_sha256 = v9.PREREG_BODY_SHA256 if study == "vs1-v9" else "9" * 64
    log_dir = tmp_path / "registry"
    num = study.split("-v")[1]
    stem = f"granular_panel_prereg_v{num}"
    log = ForwardLog(
        log_dir,
        log_filename=f"{stem}.jsonl",
        anchor_filename=f"{stem}.anchors.jsonl",
        lock_filename=f".{stem}.lock",
        prereg_sha256=prereg_sha256,
    )
    witness_path = v1.canonical_witness_path(study)
    vault = _LocalVault(tmp_path / "vault")

    t0 = frozen_at - timedelta(days=2)
    log.append([
        {"kind": "header", "version": study, "run_at": t0.isoformat(), "code_sha": CODE_SHA,
         "prereg_sha256": prereg_sha256, "promotion_allowed": False},
        {"kind": "preregistration", "version": study, "run_at": t0.isoformat(), "code_sha": CODE_SHA,
         "prereg_sha256": prereg_sha256, "promotion_allowed": False},
    ])
    vault.publish_anchors(log.anchor_path, witness_path, "register")

    t1 = frozen_at - timedelta(days=1)
    frozen_in = log.append([{
        "kind": "inputs_frozen", "run_at": t1.isoformat(), "prereg_sha256": prereg_sha256,
        "inputs": {
            "sector": "Technology", "price_manifest_sha256": manifest_digest,
            "probe_report_sha256": "a" * 64, "form4_sha256": "1" * 64, "submissions_sha256": "2" * 64,
            "issuer_map_sha256": "3" * 64, "power_sha256": "4" * 64, "sic_map_sha256": "5" * 64,
            "accept_underpowered": False, "as_of_ts": (t1 - timedelta(hours=1)).isoformat(),
        },
        "supersedes": None, "promotion_allowed": False,
    }])[0]
    inputs_sha = v1._record_sha256(frozen_in)

    log.append([{
        "kind": "discovery_opened", "run_at": (t1 + timedelta(hours=1)).isoformat(),
        "prereg_sha256": prereg_sha256, "inputs_frozen_sha256": inputs_sha,
        "promotion_allowed": False,
    }])
    vault.publish_anchors(log.anchor_path, witness_path, "discovery_opened")

    frozen_sha = ""
    if seal_discovery:
        rec = log.append([{
            "kind": "discovery_frozen", "run_at": frozen_at.isoformat(),
            "prereg_sha256": prereg_sha256, "inputs_frozen_sha256": inputs_sha,
            "discovery_sha256": "f" * 64, "calibration": "CONSISTENT", "selected": ["A90|fwd5"],
            "promotion_allowed": False,
        }])[0]
        frozen_sha = v1._record_sha256(rec)
        if witness_discovery_frozen:
            vault.publish_anchors(log.anchor_path, witness_path, "discovery_frozen")
    return log_dir, vault, prereg_sha256, frozen_sha


def _build_holdout_db(
    series: dict[str, tuple[dict[date, float], dict[date, float]]],
    *,
    boundary_rows: dict[str, tuple[float, float, datetime]] | None = None,
    pull_ts: datetime = HOLDOUT_PULL,
    extra_rows: list[dict] | None = None,
):
    engine = create_engine("sqlite://")
    md = MetaData()
    catalog = Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table("raw_series", md, Column("series_id", String), Column("source_id", Integer),
                Column("obs_date", Date), Column("pull_timestamp", DateTime), Column("value", Float),
                Column("raw_payload", Text), Column("pull_status", String))
    md.create_all(engine)
    rows: list[dict] = []
    for ticker, (close, adj) in series.items():
        for fld, vals in (("close", close), ("adj_close", adj)):
            for d, v in vals.items():
                rows.append({
                    "series_id": f"YF:{ticker}:{fld}", "source_id": TIINGO, "obs_date": d,
                    "pull_timestamp": pull_ts, "value": float(v), "raw_payload": "{}",
                    "pull_status": "SUCCESS",
                })
    for ticker, (c_val, a_val, ts) in (boundary_rows or {}).items():
        for fld, val in (("close", c_val), ("adj_close", a_val)):
            rows.append({
                "series_id": f"YF:{ticker}:{fld}", "source_id": TIINGO, "obs_date": BOUNDARY_ANCHOR,
                "pull_timestamp": ts, "value": float(val), "raw_payload": "{}",
                "pull_status": "SUCCESS",
            })
    rows.extend(extra_rows or [])
    with engine.begin() as conn:
        conn.execute(catalog.insert(), [
            {"id": TIINGO, "name": "TIINGO"},
            {"id": YFINANCE, "name": "yfinance"},
            {"id": TD_SPLITS, "name": "TWELVEDATA_SPLITS"},
            {"id": KAGGLE, "name": "KAGGLE_BULK"},
        ])
        if rows:
            conn.execute(raw.insert(), rows)
    return engine


# --- 1. Refusal tests (every refusal in the spec) --------------------------------------------------


def test_refuses_without_witnessed_discovery_frozen(tmp_path):
    from analysis import holdout_basis_probe as hb

    # (a) discovery_frozen absent from local chain
    log_dir, vault, prereg, _ = _make_sealed_registry(tmp_path / "a", seal_discovery=False)
    with pytest.raises(hb.HoldoutProbeRefused, match="discovery_frozen"):
        hb.verify_witnessed_discovery_frozen(
            log_dir, vault.cache, prereg_sha256=prereg, snapshot_as_of_ts=SNAPSHOT,
            allow_holdout=True, remote_url=str(vault.remote),
        )

    # (b) discovery_frozen present locally, NOT yet witnessed off-host
    log_dir2, vault2, prereg2, _ = _make_sealed_registry(
        tmp_path / "b", seal_discovery=True, witness_discovery_frozen=False,
    )
    with pytest.raises(hb.HoldoutProbeRefused, match="witness"):
        hb.verify_witnessed_discovery_frozen(
            log_dir2, vault2.cache, prereg_sha256=prereg2, snapshot_as_of_ts=SNAPSHOT,
            allow_holdout=True, remote_url=str(vault2.remote),
        )


def test_refuses_snapshot_not_later_than_discovery_frozen(tmp_path):
    from analysis import holdout_basis_probe as hb

    log_dir, vault, prereg, _ = _make_sealed_registry(tmp_path)
    # Equal to discovery_frozen.run_at
    with pytest.raises(hb.HoldoutProbeRefused, match="later than discovery_frozen"):
        hb.verify_witnessed_discovery_frozen(
            log_dir, vault.cache, prereg_sha256=prereg, snapshot_as_of_ts=DISC_FROZEN_AT,
            allow_holdout=True, remote_url=str(vault.remote),
        )
    # Earlier than discovery_frozen.run_at
    with pytest.raises(hb.HoldoutProbeRefused, match="later than discovery_frozen"):
        hb.verify_witnessed_discovery_frozen(
            log_dir, vault.cache, prereg_sha256=prereg,
            snapshot_as_of_ts=DISC_FROZEN_AT - timedelta(minutes=1),
            allow_holdout=True, remote_url=str(vault.remote),
        )
    # Naive datetime (no timezone)
    with pytest.raises(hb.HoldoutProbeRefused, match="timezone"):
        hb.verify_witnessed_discovery_frozen(
            log_dir, vault.cache, prereg_sha256=prereg,
            snapshot_as_of_ts=datetime(2026, 10, 7, 0, 0),
            allow_holdout=True, remote_url=str(vault.remote),
        )


def test_refuses_missing_allow_holdout(tmp_path):
    from analysis import holdout_basis_probe as hb
    from scripts import run_holdout_basis_probe as cli

    log_dir, vault, prereg, _ = _make_sealed_registry(tmp_path)
    with pytest.raises(hb.HoldoutProbeRefused, match="allow_holdout|allow-holdout"):
        hb.verify_witnessed_discovery_frozen(
            log_dir, vault.cache, prereg_sha256=prereg, snapshot_as_of_ts=SNAPSHOT,
            allow_holdout=False, remote_url=str(vault.remote),
        )
    # CLI without --allow-holdout
    cal = _write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS)
    manifest, _ = _write_manifest(tmp_path / "m.json", ["AAA"], {"AAA": "2010-01-04"})
    with pytest.raises(SystemExit) as exc:
        cli.main([
            "--manifest", str(manifest), "--log-dir", str(log_dir), "--vault-repo", str(vault.cache),
            "--twelvedata-dir", str(tmp_path / "td"), "--tiingo-meta-dir", str(tmp_path / "meta"),
            "--calendar-manifest", str(cal), "--submissions", str(tmp_path / "subs.csv"),
            "--issuers", str(tmp_path / "issuers.json"), "--sic-map", str(tmp_path / "sic.jsonl"),
            "--as-of-ts", SNAPSHOT.isoformat(),
            "--prereg-sha256", prereg, "--code-sha", CODE_SHA, "--out", str(tmp_path / "out"),
        ])
    assert "allow-holdout" in str(exc.value).lower() or "allow_holdout" in str(exc.value).lower()


def test_refuses_prereg_sha_mismatch(tmp_path):
    from analysis import holdout_basis_probe as hb

    log_dir, vault, _, _ = _make_sealed_registry(tmp_path, prereg_sha256="9" * 64)
    with pytest.raises(hb.HoldoutProbeRefused, match="prereg"):
        hb.verify_witnessed_discovery_frozen(
            log_dir, vault.cache, prereg_sha256="8" * 64, snapshot_as_of_ts=SNAPSHOT,
            allow_holdout=True, remote_url=str(vault.remote),
        )
    with pytest.raises(hb.HoldoutProbeRefused, match="prereg"):
        hb.verify_witnessed_discovery_frozen(
            log_dir, vault.cache, prereg_sha256="not-a-sha", snapshot_as_of_ts=SNAPSHOT,
            allow_holdout=True, remote_url=str(vault.remote),
        )


def test_refuses_existing_out_directory(tmp_path, monkeypatch):
    from analysis import holdout_basis_probe as hb
    from scripts import run_holdout_basis_probe as cli

    cal = _write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS)
    manifest, m_digest = _write_manifest(tmp_path / "m.json", ["AAA"], {"AAA": "2010-01-04"})
    log_dir, vault, prereg, _ = _make_sealed_registry(tmp_path, manifest_digest=m_digest)
    existing = tmp_path / "already_exists"
    existing.mkdir()

    monkeypatch.setattr(v1, "WITNESS_REMOTE_URL", str(vault.remote))
    with pytest.raises(SystemExit, match="exists|write-once"):
        cli.main([
            "--manifest", str(manifest), "--log-dir", str(log_dir), "--vault-repo", str(vault.cache),
            "--twelvedata-dir", str(tmp_path / "td"), "--tiingo-meta-dir", str(tmp_path / "meta"),
            "--calendar-manifest", str(cal), "--submissions", str(tmp_path / "subs.csv"),
            "--issuers", str(tmp_path / "issuers.json"), "--sic-map", str(tmp_path / "sic.jsonl"),
            "--as-of-ts", SNAPSHOT.isoformat(), "--allow-holdout",
            "--prereg-sha256", prereg, "--code-sha", CODE_SHA, "--out", str(existing),
        ])
    with pytest.raises(hb.HoldoutProbeRefused, match="exists|write-once"):
        hb.check_write_once_out(existing)


def test_refuses_form4_input_supplied(tmp_path):
    from analysis import holdout_basis_probe as hb
    from scripts import run_holdout_basis_probe as cli

    # (a) CLI flags rejected before anything else
    for flag in ("--form4", "--owners", "--transactions"):
        with pytest.raises(SystemExit, match="Form 4"):
            cli.main([flag, "nonderiv_transactions.parquet"])
        with pytest.raises(SystemExit, match="Form 4"):
            cli.main([f"{flag}=nonderiv_transactions.parquet"])

    # (b) Python API rejects Form 4 kwargs
    with pytest.raises(hb.HoldoutProbeRefused, match="Form 4"):
        hb.check_no_form4_input(form4=Path("nonderiv_transactions.parquet"))
    with pytest.raises(hb.HoldoutProbeRefused, match="Form 4"):
        hb.check_no_form4_input(events={"purchases": []})
    with pytest.raises(hb.HoldoutProbeRefused, match="Form 4"):
        hb.check_no_form4_input(owners=Path("reportingowner.parquet"))

    # (c) Form 4 transaction table masquerading as --submissions is rejected
    fake_form4 = tmp_path / "fake_subs.csv"
    fake_form4.write_text(
        "accession_number,filing_date,issuer_cik,document_type,issuer_ticker,trans_code,trans_shares\n"
        "0001-1,2020-02-03,100,4,AAA,P,5000\n",
        encoding="utf-8",
    )
    with pytest.raises(hb.HoldoutProbeRefused, match="Form 4"):
        hb.load_c1_interval(fake_form4, [{"ticker": "AAA", "cik": 100, "current_tickers": ["AAA"]}])


def test_refuses_read_outside_holdout_window(tmp_path):
    from analysis import holdout_basis_probe as hb
    from scripts import run_holdout_basis_probe as cli

    hb.check_holdout_window(HO_LO, HO_HI)
    for lo, hi in (
        (date(2019, 12, 31), HO_HI),     # reaches into discovery
        (date(2012, 1, 1), date(2019, 12, 31)),
        (HO_LO, date(2026, 7, 1)),       # reaches into reserved future
        (date(2025, 1, 2), date(2024, 1, 2)),
    ):
        with pytest.raises(hb.HoldoutProbeRefused):
            hb.check_holdout_window(lo, hi)

    # Boundary anchor helper only accepts immediately preceding session before 2020-01-01
    engine = _build_holdout_db({})
    with engine.connect() as conn:
        with pytest.raises(hb.HoldoutProbeRefused):
            hb.read_boundary_anchor(conn, "YF:AAA:close", date(2019, 11, 1), SNAPSHOT)
        with pytest.raises(hb.HoldoutProbeRefused):
            hb.read_boundary_anchor(conn, "YF:AAA:close", date(2020, 1, 2), SNAPSHOT)
        with pytest.raises(hb.HoldoutProbeRefused):
            hb.read_selected(conn, "YF:AAA:close", date(2019, 12, 31), HO_HI, SNAPSHOT)
        with pytest.raises(hb.HoldoutProbeRefused):
            hb.read_rows(conn, "YF:AAA:close", TIINGO, HO_LO, date(2026, 7, 1), SNAPSHOT)

    # CLI --start / --end outside holdout window refused
    cal = _write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS)
    manifest, m_digest = _write_manifest(tmp_path / "m.json", ["AAA"], {"AAA": "2010-01-04"})
    log_dir, vault, prereg, _ = _make_sealed_registry(tmp_path, manifest_digest=m_digest)
    for flag, val in (("--start", "2019-12-31"), ("--end", "2026-07-01")):
        with pytest.raises(SystemExit, match="holdout|window"):
            cli.main([
                "--manifest", str(manifest), "--log-dir", str(log_dir), "--vault-repo", str(vault.cache),
                "--twelvedata-dir", str(tmp_path / "td"), "--tiingo-meta-dir", str(tmp_path / "meta"),
                "--calendar-manifest", str(cal), "--submissions", str(tmp_path / "subs.csv"),
                "--issuers", str(tmp_path / "issuers.json"), "--sic-map", str(tmp_path / "sic.jsonl"),
                "--as-of-ts", SNAPSHOT.isoformat(), "--allow-holdout",
                "--prereg-sha256", prereg, "--code-sha", CODE_SHA, "--out", str(tmp_path / f"o_{val}"),
                flag, val,
            ])


# --- 2. Basis checks 1-4, cross-check, 2020-01-01 splice boundary, C1/startDate bounds ---------------


def test_splice_check_includes_the_2020_01_01_boundary(tmp_path):
    from analysis import holdout_basis_probe as hb

    c_aaa, a_aaa = _series(HO_SESSIONS, base=50.0)
    first_d = HO_SESSIONS[0]
    # AAA: pre-2020 backfill pull had factor 1.006x relative to 2020-01-02 holdout pull -> uncorroborated splice!
    # BBB: pre-2020 backfill pull had flat factor (1.0x) -> passes boundary!
    # CCC: pre-2020 backfill pull had real 1% ex-date step corroborated by TwelveData -> passes boundary!
    c_bbb, a_bbb = _series(HO_SESSIONS, base=60.0)
    c_ccc, a_ccc = _series(HO_SESSIONS, base=70.0)
    c_xlk, a_xlk = _series(HO_SESSIONS, base=90.0)

    series = {"AAA": (c_aaa, a_aaa), "BBB": (c_bbb, a_bbb), "CCC": (c_ccc, a_ccc), "XLK": (c_xlk, a_xlk)}
    boundary = {
        "AAA": (c_aaa[first_d], a_aaa[first_d] / 0.994, DISC_PULL),  # bad splice across 2020-01-01
        "BBB": (c_bbb[first_d], a_bbb[first_d], DISC_PULL),          # flat factor across 2020-01-01
        "CCC": (c_ccc[first_d], a_ccc[first_d] * 0.99, DISC_PULL),   # real ex-date on 2020-01-02
        "XLK": (c_xlk[first_d], a_xlk[first_d], DISC_PULL),
    }
    engine = _build_holdout_db(series, boundary_rows=boundary)
    td_dir = _write_td_dir(
        tmp_path / "td", series,
        include_boundary={"CCC": (c_ccc[first_d], a_ccc[first_d] * 0.99)},
    )
    meta_dir = _write_tiingo_meta_dir(tmp_path / "meta", ["AAA", "BBB", "CCC", "XLK"])
    cal_path = _write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS)
    cal = hb.load_calendar_manifest(cal_path)
    manifest_path, _ = _write_manifest(
        tmp_path / "m.json", ["AAA", "BBB", "CCC"],
        {"AAA": "2010-01-04", "BBB": "2010-01-04", "CCC": "2010-01-04"},
    )
    manifest = hb.load_price_manifest(manifest_path)
    vendors = hb.HoldoutVendorFiles(
        td_dir, meta_dir, lo=HO_LO, hi=HO_HI, boundary_anchor=cal.boundary_anchor,
        discovery_frozen_run_at=DISC_FROZEN_AT,
    )
    with engine.connect() as conn:
        probe = hb.run_holdout_probe(
            conn, manifest=manifest, calendar=cal, lo=HO_LO, hi=HO_HI,
            as_of_ts=SNAPSHOT, vendors=vendors,
        )
    recs = probe["records"]
    assert recs["AAA"]["abstains"] is True
    assert "splice_failed" in recs["AAA"]["reasons"]
    assert recs["AAA"]["splice"]["failed"] == [[BOUNDARY_ANCHOR.isoformat(), first_d.isoformat()]]
    assert recs["BBB"]["abstains"] is False and recs["BBB"]["splice"]["boundaries"] == 1
    assert recs["CCC"]["abstains"] is False and recs["CCC"]["splice"]["boundaries"] == 1
    assert probe["benchmark_record"]["admitted"] is True


def test_c1_and_start_date_bounds_and_vendor_guards(tmp_path):
    from analysis import holdout_basis_probe as hb

    base_s = {t: _series(HO_SESSIONS, base=30.0 + i * 5) for i, t in enumerate(["OK1", "SHIFT", "NOMETA", "OUTC1", "FUTURETD", "PRESEAL", "XLK"])}
    engine = _build_holdout_db(base_s)

    # FUTURETD has a reserved row on 2026-07-01 in TwelveData -> must fail closed!
    td_dir = _write_td_dir(
        tmp_path / "td", {k: v for k, v in base_s.items() if k != "PRESEAL"},
        extra_dates={"FUTURETD": {date(2026, 7, 1): 40.0}},
    )
    # Add PRESEAL fetched BEFORE discovery_frozen -> must fail closed!
    _write_td_dir(tmp_path / "td", {"PRESEAL": base_s["PRESEAL"]}, fetched_at=DISC_FROZEN_AT - timedelta(hours=1))

    # SHIFT has Tiingo startDate shifted later than manifest's listed_from
    meta_dir = _write_tiingo_meta_dir(
        tmp_path / "meta", list(base_s),
        start_dates={"SHIFT": "2022-05-01"},
        missing=("NOMETA",),
    )
    cal_path = _write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS)
    cal = hb.load_calendar_manifest(cal_path)
    issuers_list = ["OK1", "SHIFT", "NOMETA", "OUTC1", "FUTURETD", "PRESEAL"]
    manifest_path, _ = _write_manifest(
        tmp_path / "m.json", issuers_list, {t: "2010-01-04" for t in issuers_list},
    )
    manifest = hb.load_price_manifest(manifest_path)

    # C1 submissions: OUTC1 switched away to OTHER before 2020 and never returned
    subs_path = tmp_path / "subs.csv"
    subs_lines = ["accession_number,filing_date,issuer_cik,document_type,issuer_ticker"]
    issuers_meta = []
    for i, t in enumerate(issuers_list):
        cik = 1000 + i
        issuers_meta.append({"ticker": t, "cik": cik, "current_tickers": [t]})
        sym = "OTHER" if t == "OUTC1" else t
        subs_lines.append(f"{cik}-1,2019-06-01,{cik},4,{sym}")
    subs_path.write_text("\n".join(subs_lines) + "\n", encoding="utf-8")
    c1 = hb.load_c1_interval(subs_path, issuers_meta)

    vendors = hb.HoldoutVendorFiles(
        td_dir, meta_dir, lo=HO_LO, hi=HO_HI, boundary_anchor=cal.boundary_anchor,
        discovery_frozen_run_at=DISC_FROZEN_AT,
    )
    with engine.connect() as conn:
        probe = hb.run_holdout_probe(
            conn, manifest=manifest, calendar=cal, lo=HO_LO, hi=HO_HI,
            as_of_ts=SNAPSHOT, vendors=vendors, interval=c1,
        )
    recs = probe["records"]
    assert recs["OK1"]["abstains"] is False
    assert recs["SHIFT"]["abstains"] is True and "start_date_shifted" in recs["SHIFT"]["reasons"]
    assert recs["NOMETA"]["abstains"] is True and "no_meta" in recs["NOMETA"]["reasons"]
    assert recs["OUTC1"]["abstains"] is True and "outside_c1_interval" in recs["OUTC1"]["reasons"]
    assert recs["FUTURETD"]["abstains"] is True and "twelvedata_out_of_window_rows" in recs["FUTURETD"]["reasons"]
    assert recs["PRESEAL"]["abstains"] is True and "twelvedata_fetched_before_discovery_frozen" in recs["PRESEAL"]["reasons"]


# --- 3. End-to-end CLI: window=="holdout", tickers <= admitted, XLK magnitudes, outputs ------------


def test_end_to_end_cli_outputs_window_holdout_tickers_subset_and_xlk_magnitudes(tmp_path, monkeypatch):
    from analysis import holdout_basis_probe as hb
    from scripts import run_holdout_basis_probe as cli
    import scripts.run_real_panel_scan as rps

    # v9-style manifest: XLK is NOT in manifest.admitted; AAA passes, BBB fails basis 1 (multi-valued),
    # CCC fails basis 4 (quarantined), DDD fails TwelveData cross-check, XLK fails cross-check (magnitudes only).
    series = {
        "AAA": _series(HO_SESSIONS, base=41.237, split_at=120, dividends=(40, 200)),
        "BBB": _series(HO_SESSIONS, base=22.222),
        "CCC": _series(HO_SESSIONS, base=31.313),
        "DDD": _series(HO_SESSIONS, base=18.500),
        "XLK": _series(HO_SESSIONS, base=95.123),
    }
    extra_rows = [
        # BBB multi-valued TIINGO SUCCESS vintage -> basis 1 failure
        {"series_id": "YF:BBB:adj_close", "source_id": TIINGO, "obs_date": HO_SESSIONS[5],
         "pull_timestamp": HOLDOUT_PULL + timedelta(hours=1), "value": series["BBB"][1][HO_SESSIONS[5]] * 1.05,
         "raw_payload": "{}", "pull_status": "SUCCESS"},
        # CCC QUARANTINED TIINGO row -> basis 4 failure
        {"series_id": "YF:CCC:close", "source_id": TIINGO, "obs_date": HO_SESSIONS[10],
         "pull_timestamp": HOLDOUT_PULL, "value": 1.0, "raw_payload": "{}", "pull_status": "QUARANTINED"},
        # AAA other-source rows (KAGGLE_BULK / yfinance): counted, NOT disqualifying
        {"series_id": "YF:AAA:close", "source_id": KAGGLE, "obs_date": HO_SESSIONS[0],
         "pull_timestamp": HOLDOUT_PULL, "value": 999.0, "raw_payload": "{}", "pull_status": "SUCCESS"},
    ]
    engine = _build_holdout_db(series, extra_rows=extra_rows)
    td_dir = _write_td_dir(tmp_path / "td", series, bad_tickers=("DDD", "XLK"))
    meta_dir = _write_tiingo_meta_dir(tmp_path / "meta", list(series))
    cal_path = _write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS)
    admitted_in = ["AAA", "BBB", "CCC", "DDD"]  # XLK deliberately absent (v9 manifest)
    manifest_path, m_digest = _write_manifest(
        tmp_path / "price_manifest.json", admitted_in, {t: "2010-01-04" for t in admitted_in},
    )
    log_dir, vault, prereg, frozen_sha = _make_sealed_registry(tmp_path, manifest_digest=m_digest)

    # Synthetic C1 submissions, issuers JSON, and SIC map (required by v6 §2.3, §8 — HOLD-N2).
    issuers_path = tmp_path / "issuers.json"
    sic_map_path = tmp_path / "sic_map.jsonl"
    subs_path = tmp_path / "subs.csv"
    issuers_meta = [{"ticker": t, "cik": 1000 + i, "current_tickers": [t]} for i, t in enumerate(admitted_in)]
    issuers_path.write_text(json.dumps({"issuers": issuers_meta}), encoding="utf-8")
    sic_map_path.write_text(
        "".join(json.dumps({"cik": m["cik"], "sic": 7372, "name": f"{m['ticker']} Systems Inc"}) + "\n" for m in issuers_meta),
        encoding="utf-8",
    )
    subs_path.write_text(
        "accession_number,filing_date,issuer_cik,document_type,issuer_ticker\n"
        + "".join(f"{m['cik']}-1,2019-06-01,{m['cik']},4,{m['ticker']}\n" for m in issuers_meta),
        encoding="utf-8",
    )

    monkeypatch.setattr(v1, "WITNESS_REMOTE_URL", str(vault.remote))
    monkeypatch.setattr(rps, "read_only_engine", lambda *a, **k: engine)
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(cli, "resolve_code_sha", lambda repo_root=cli.REPO: CODE_SHA)

    out = tmp_path / "holdout_probe_out"
    cli.main([
        "--manifest", str(manifest_path), "--log-dir", str(log_dir), "--vault-repo", str(vault.cache),
        "--twelvedata-dir", str(td_dir), "--tiingo-meta-dir", str(meta_dir),
        "--calendar-manifest", str(cal_path), "--submissions", str(subs_path),
        "--issuers", str(issuers_path), "--sic-map", str(sic_map_path), "--unpinned-sic-map",
        "--as-of-ts", SNAPSHOT.isoformat(),
        "--allow-holdout", "--prereg-sha256", prereg, "--code-sha", CODE_SHA, "--out", str(out),
    ])

    assert (out / "holdout_basis_report.json").exists()
    assert (out / "crosscheck_report.json").exists()
    assert (out / "sha256s.txt").exists()
    assert not (out / "price_manifest.json").exists()  # holdout probe never rewrites the manifest

    report = json.loads((out / "holdout_basis_report.json").read_text(encoding="utf-8"))
    cross = json.loads((out / "crosscheck_report.json").read_text(encoding="utf-8"))

    assert report["window"] == "holdout"
    assert cross["window"] == "holdout"
    assert report["read_window"]["start"] == "2020-01-01" and report["read_window"]["end"] == "2026-06-30"
    assert report["prereg"]["body_sha256"] == prereg and report["prereg"]["study"] == "vs1-v9"
    assert report["snapshot_as_of_ts"] == SNAPSHOT.isoformat()
    assert report["discovery_frozen"]["sha256"] == frozen_sha
    assert report["discovery_frozen"]["run_at"] == DISC_FROZEN_AT.isoformat()
    assert report["manifest"]["sha256"] == gd4.file_sha256(manifest_path)
    assert report["manifest"]["digest"] == m_digest
    assert report["tolerances"]["crosscheck"]["min_share_within"] == 0.99
    assert report["tolerances"]["crosscheck"]["tolerance"] == 0.0010
    assert report["tolerances"]["crosscheck"]["min_pairs"] == 250
    assert report["tolerances"]["splice_tol"] == 1e-4

    # tickers <= admitted, admitted <= manifest.admitted, abstaining <= manifest.admitted
    assert set(report["tickers"]) <= set(admitted_in)
    assert set(report["admitted"]) <= set(admitted_in)
    assert set(report["abstaining"]) <= set(admitted_in)
    assert report["admitted"] == ["AAA"]
    assert report["abstaining"] == ["BBB", "CCC", "DDD"]
    assert report["tickers"]["AAA"]["abstains"] is False and report["tickers"]["AAA"]["reasons"] == []
    assert report["tickers"]["BBB"]["abstains"] is True and "multi_valued_dates" in report["tickers"]["BBB"]["reasons"]
    assert report["tickers"]["CCC"]["abstains"] is True and "quarantined_rows" in report["tickers"]["CCC"]["reasons"]
    assert report["tickers"]["DDD"]["abstains"] is True and "crosscheck_disagreement" in report["tickers"]["DDD"]["reasons"]
    assert report["tickers"]["AAA"]["source_filtering"]["other_source_rows"] == {"KAGGLE_BULK": {"SUCCESS": 1}}

    # XLK failed cross-check -> xlk_admitted_for_magnitudes is False, yet AAA still passed!
    assert report["xlk_admitted_for_magnitudes"] is False
    assert report["promotion_allowed"] is False

    # sha256s.txt matches exact file bytes
    lines = (out / "sha256s.txt").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    for line in lines:
        h, name = line.split()
        assert gd4.file_sha256(out / name) == h
    assert report["crosscheck_report_sha256"] == gd4.file_sha256(out / "crosscheck_report.json")

    # Requesting a ticker outside manifest.admitted refuses
    cal = hb.load_calendar_manifest(cal_path)
    manifest = hb.load_price_manifest(manifest_path)
    vendors = hb.HoldoutVendorFiles(td_dir, meta_dir, lo=HO_LO, hi=HO_HI, boundary_anchor=cal.boundary_anchor)
    with engine.connect() as conn:
        with pytest.raises(hb.HoldoutProbeRefused, match="admitted"):
            hb.run_holdout_probe(
                conn, manifest=manifest, calendar=cal, lo=HO_LO, hi=HO_HI,
                as_of_ts=SNAPSHOT, vendors=vendors, tickers=["AAA", "NOT_IN_MANIFEST"],
            )

    # Privacy check: no raw close or adj_close price leaks into either report
    aaa_prices = list(series["AAA"][0].values())[:20] + list(series["AAA"][1].values())[:20]
    for artifact in (report, cross):
        dumped = json.dumps(artifact)
        for p in aaa_prices:
            assert repr(p) not in dumped and f"{p:.4f}" not in dumped


def test_basis_checks_2_and_3_crosscheck_tolerances_and_source_refusal(tmp_path):
    from analysis import holdout_basis_probe as hb

    # Source refusal
    for bad_src in ("yfinance", "KAGGLE_BULK", "TWELVEDATA", ""):
        with pytest.raises(hb.HoldoutProbeRefused):
            hb.check_source(bad_src)

    # Basis check 2 (split_inconsistent), Basis check 3 (calendar_gaps),
    # Cross-check too_few_pairs (< 250) and adjustment_dominated (> 10%)
    c_ok, a_ok = _series(HO_SESSIONS, base=50.0, split_at=100, dividends=(30,))
    c_split, a_split = _series(HO_SESSIONS, base=40.0)
    a_split = {d: v * (1.45 if i >= 50 else 1.0) for i, (d, v) in enumerate(sorted(a_split.items()))}
    c_gap, a_gap = _series(HO_SESSIONS, base=60.0)
    del c_gap[HO_SESSIONS[77]]
    c_short, a_short = _series(HO_SESSIONS[-200:], base=35.0)
    c_adjdom, a_adjdom = _series(HO_SESSIONS, base=55.0, dividends=tuple(range(5, N_HOLDOUT, 8)))
    c_xlk, a_xlk = _series(HO_SESSIONS, base=90.0)

    series = {
        "OK": (c_ok, a_ok), "SPLIT": (c_split, a_split), "GAP": (c_gap, a_gap),
        "SHORT": (c_short, a_short), "ADJDOM": (c_adjdom, a_adjdom), "XLK": (c_xlk, a_xlk),
    }
    engine = _build_holdout_db(series)
    td_dir = _write_td_dir(tmp_path / "td", series)
    meta_dir = _write_tiingo_meta_dir(tmp_path / "meta", list(series))
    cal = hb.load_calendar_manifest(_write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS))
    # v6/v8-style manifest with XLK inside manifest.admitted
    admitted = ["ADJDOM", "GAP", "OK", "SHORT", "SPLIT", "XLK"]
    manifest_path, _ = _write_manifest(
        tmp_path / "m.json", admitted, {t: "2010-01-04" for t in admitted if t != "XLK"},
    )
    manifest = hb.load_price_manifest(manifest_path)
    vendors = hb.HoldoutVendorFiles(
        td_dir, meta_dir, lo=HO_LO, hi=HO_HI, boundary_anchor=cal.boundary_anchor,
        discovery_frozen_run_at=DISC_FROZEN_AT,
    )
    with engine.connect() as conn:
        probe = hb.run_holdout_probe(
            conn, manifest=manifest, calendar=cal, lo=HO_LO, hi=HO_HI, as_of_ts=SNAPSHOT, vendors=vendors,
        )
    recs = probe["records"]
    assert set(recs) <= set(manifest.admitted)
    assert recs["OK"]["abstains"] is False and recs["OK"]["reasons"] == []
    assert recs["XLK"]["abstains"] is False and probe["benchmark_record"]["admitted"] is True
    assert recs["SPLIT"]["abstains"] is True and "split_inconsistent" in recs["SPLIT"]["reasons"]
    assert recs["GAP"]["abstains"] is True and "calendar_gaps" in recs["GAP"]["reasons"]
    assert recs["SHORT"]["abstains"] is True and "crosscheck_too_few_pairs" in recs["SHORT"]["reasons"]
    assert recs["ADJDOM"]["abstains"] is True and "crosscheck_adjustment_dominated" in recs["ADJDOM"]["reasons"]


def test_build_trial_panels_rejects_abstain_tickers_on_discovery(tmp_path, monkeypatch):
    from tests.test_panel_insider_density_v6 import _panel_inputs, _vault
    from tests.test_panel_insider_density_v2 import _discovery_key

    monkeypatch.setattr(v6, "SUPERSEDED_BY", None)
    engine, events, admission, universe, manifest = _panel_inputs(tmp_path)
    vault = _vault(tmp_path / "vault")
    key, _ = _discovery_key(tmp_path / "reg", vault, manifest)
    with engine.connect() as conn:
        panel = v6.load_price_panel(
            conn, manifest, list(universe["ticker"]), start=date(2011, 11, 1),
            as_of=date(2019, 12, 31), window="discovery", key=key,
        )
    with pytest.raises(ValueError, match="holdout window"):
        v6.build_trial_panels(events, admission, universe, panel, "discovery", abstain_tickers=["T00"])


# --- 4. open-holdout records report sha & abstaining tickers get NaN labels ------------------------


def test_open_holdout_records_report_sha_and_abstaining_tickers_get_nan_labels(tmp_path, monkeypatch):
    from tests.test_panel_insider_density import NOW, _row, _submission
    from tests.test_panel_insider_density_v2 import _discovery_key, _observed, _universe_frame, _Vault
    from tests.test_panel_insider_density_v6 import _tiingo_db

    monkeypatch.setattr(v6, "SUPERSEDED_BY", None)

    rng = np.random.default_rng(17)
    tickers = [f"T{i:02d}" for i in range(24)]
    ciks = list(range(1000, 1024))
    dates = pd.bdate_range("2011-11-01", "2021-06-30")
    rets = rng.normal(0.0, 0.01, (len(dates), len(tickers)))
    rows = []
    for j, cik in enumerate(ciks):
        for i in range(60, len(dates) - 25, 45):
            rows.append(_row(
                accession_number=f"p-{cik}-{i}", issuer_cik=str(cik), owner_cik=str((i % 5) + 1),
                filing_date=str(dates[i + 1].date()), transaction_date=str(dates[i].date()),
            ))
            rets[i + 2 : i + 10, j] += 0.004
    prices_df = pd.DataFrame(100.0 * np.exp(np.cumsum(rets, axis=0)), index=dates, columns=tickers)
    prices_df["XLK"] = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.008, len(dates))))
    engine = _tiingo_db({c: prices_df[c] for c in prices_df.columns})

    subs = [
        {**_submission(accession_number=r["accession_number"], filing_date=r["filing_date"],
                       issuer_cik=r["issuer_cik"], owner_cik=r["owner_cik"]),
         "issuer_ticker": tickers[int(r["issuer_cik"]) - 1000]}
        for r in rows
    ]
    subs += [
        {**_submission(accession_number=f"h-{c}-{d.date()}", issuer_cik=str(c), filing_date=str(d.date()),
                       document_type="4", owner_cik="9"),
         "issuer_ticker": tickers[c - 1000]}
        for c in ciks for d in dates[::20]
    ]
    tx = pd.DataFrame(rows).astype("string")
    tx.columns = [c.upper() for c in tx.columns]
    submissions = pd.DataFrame(subs)
    events = v1.build_events(tx, submissions=submissions.rename(columns=str.upper))
    universe = _universe_frame([(t, c, [t]) for t, c in zip(tickers, ciks)])
    admission = v2.build_admission(submissions, universe)
    manifest = v6.PriceManifest(
        source="TIINGO", series_template="YF:{ticker}:adj_close", basis="split+dividend adjusted",
        benchmark="XLK", admitted=tuple(sorted(tickers + ["XLK"])),
        probe_report_sha256="a" * 64, crosscheck_report_sha256="c" * 64, tiingo_meta_report_sha256="d" * 64,
        listed_from=tuple((t, "2011-11-01") for t in tickers),
    )

    vault = _Vault(tmp_path / "vault", h=v6, seeds=("vs1-v1", "vs1-v2"))
    for m in (v3, v4, v5):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + b"\n")
    key, inputs = _discovery_key(tmp_path / "reg", vault, manifest)
    with engine.connect() as conn:
        disc_panel = v6.load_price_panel(
            conn, manifest, tickers, start=date(2011, 11, 1), as_of=date(2019, 12, 31),
            window="discovery", key=key,
        )
    disc_panels = v6.build_trial_panels(events, admission, universe, disc_panel, "discovery")
    frozen = v6.discover_panel(
        v6.run_spec("ho-abstain"), disc_panels, sensitivity=False,
        inputs={"inputs_frozen_sha256": key.inputs_frozen_sha256},
    )
    sealed = v6.seal_discovery(tmp_path / "reg", NOW, key, frozen)
    vault.publish(tmp_path / "reg")

    # Write a holdout basis report where T00 and T01 fail and abstain
    later = (datetime.fromisoformat(sealed["run_at"]) + timedelta(hours=2)).isoformat()
    abstain_set = ["T00", "T01"]
    ho_admitted = [t for t in sorted(tickers + ["XLK"]) if t not in abstain_set]
    report_doc = {
        "window": "holdout",
        "prereg": {"study": v6.VERSION, "body_sha256": v6.PREREG_BODY_SHA256},
        "snapshot_as_of_ts": later,
        "discovery_frozen": {"sha256": v1._record_sha256(sealed), "run_at": sealed["run_at"]},
        "manifest": {"digest": manifest.digest()},
        "admitted": ho_admitted,
        "abstaining": abstain_set,
        "xlk_admitted_for_magnitudes": True,
        "tickers": {
            t: {"ticker": t, "abstains": t in abstain_set, "admitted": t not in abstain_set,
                "reasons": ["splice_failed"] if t in abstain_set else []}
            for t in sorted(tickers + ["XLK"])
        },
    }
    report_path = tmp_path / "holdout_basis_report.json"
    report_path.write_text(json.dumps(report_doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_sha = v1.data_sha256(report_path)

    # open-holdout records the report's sha and abstaining list in holdout_opened
    v6.open_holdout(
        frozen, allow_holdout=True, prereg_sha256=v6.PREREG_BODY_SHA256,
        log_dir=tmp_path / "reg", now=NOW + timedelta(hours=3),
        observed=_observed(inputs), witness=vault.witness(),
        holdout_probe={"sha256": report_sha, "report": report_doc},
    )
    opened = [r for r in v6.registry(tmp_path / "reg").read_all() if r["kind"] == "holdout_opened"][0]
    assert opened["holdout_probe"]["sha256"] == report_sha
    assert opened["holdout_probe"]["abstaining"] == abstain_set

    vault.publish(tmp_path / "reg")
    hkey = v6.resume_holdout(
        frozen, allow_holdout=True, prereg_sha256=v6.PREREG_BODY_SHA256,
        log_dir=tmp_path / "reg", observed=_observed(inputs), witness=vault.witness(),
    )
    assert hkey.holdout_probe["sha256"] == report_sha
    assert v2.holdout_abstaining(hkey.holdout_probe, tickers) == abstain_set

    with engine.connect() as conn:
        ho_prices = v6.load_price_panel(
            conn, manifest, tickers, start=date(2019, 11, 1), as_of=date(2021, 6, 30),
            window="holdout", key=hkey,
        )

    # (a) build_trial_panels with abstain_tickers blanks abstaining tickers' holdout labels to NaN
    masked_panels = v6.build_trial_panels(
        events, admission, universe, ho_prices, "holdout", abstain_tickers=abstain_set,
    )
    p5 = masked_panels["A90|fwd5"]
    assert set(abstain_set) <= set(p5.entities)  # manifest/entities unchanged; listed, not dropped
    for t in abstain_set:
        j = p5.entities.index(t)
        assert np.isnan(p5.label[:, j]).all(), f"{t} must have all-NaN holdout labels"
        assert np.isnan(p5.feature[:, j]).all(), f"{t} must abstain in holdout"
    j_ok = p5.entities.index("T02")
    assert np.isfinite(p5.label[:, j_ok]).sum() > 0

    # (b) defense-in-depth: evaluate_panel_holdout enforces abstention from hkey.holdout_probe
    #     even if caller passed unmasked panels!
    unmasked_panels = v6.build_trial_panels(events, admission, universe, ho_prices, "holdout")
    assert np.isfinite(unmasked_panels["A90|fwd5"].label[:, unmasked_panels["A90|fwd5"].entities.index("T00")]).sum() > 0

    res_masked = v6.evaluate_panel_holdout(frozen, masked_panels, hkey, power={"gate_passed": True})
    res_defense = v6.evaluate_panel_holdout(frozen, unmasked_panels, hkey, power={"gate_passed": True})
    assert res_defense["holdout_probe_sha256"] == report_sha
    assert res_defense["holdout_abstaining_tickers"] == abstain_set
    assert res_defense["holdout_sha256"] == res_masked["holdout_sha256"]
    assert res_defense["holdout_checks"] == res_masked["holdout_checks"]


# --- 5. Regression test: discovery probe still refuses 2020-01-01+ reads ---------------------------


def test_discovery_probe_still_refuses_2020_and_later_reads(tmp_path):
    assert gd4.HOLDOUT_START == date(2020, 1, 1)
    for bad_end in (date(2020, 1, 1), date(2020, 1, 2), date(2026, 6, 30)):
        with pytest.raises(gd4.ProbeRefused, match="2020-01-01|holdout"):
            gd4.check_window(date(2011, 11, 2), bad_end)
    with pytest.raises(ValueError, match="holdout"):
        with gd4.preregistered_window("2020-01-01", "vs1-v6", "a" * 64):
            pass
    with pytest.raises(fetch.FetchStopped, match="holdout"):
        fetch.check_request_window("2011-11-02", "2020-01-02")

    engine = _build_holdout_db({})
    vendors = gd4.VendorFiles(tmp_path / "td", tmp_path / "meta")
    with engine.connect() as conn:
        with pytest.raises(gd4.ProbeRefused, match="holdout"):
            gd4.read_rows(conn, "YF:AAA:close", TIINGO, date(2019, 1, 2), date(2020, 1, 1), SNAPSHOT)
        with pytest.raises(gd4.ProbeRefused, match="holdout"):
            gd4.read_selected(conn, "YF:AAA:close", date(2019, 1, 2), date(2020, 1, 2), SNAPSHOT)
        with pytest.raises(gd4.ProbeRefused, match="holdout"):
            gd4.run_probe(
                conn, ["AAA"], benchmark="XLK", source="TIINGO",
                lo=date(2011, 11, 2), hi=date(2020, 1, 1), as_of_ts=SNAPSHOT, vendors=vendors,
            )


# --- 6. Follow-up guards: HOLD-N1, HOLD-N2, HOLD-N3, HOLD-N5 ---------------------------------------


def test_calendar_regex_and_schema_imported_from_market_calendar():
    from analysis import holdout_basis_probe as hb
    from ingestion import market_calendar as mc

    assert hb.CALENDAR_SCHEMA == mc.SCHEMA
    assert hb.CALENDAR_VERSION_RE is mc.VERSION
    assert hb.SESSION_KEY_RE is mc.KEY


def test_cli_refuses_missing_submissions_issuers_or_sic_map_and_empty_sec_names_fails_closed(tmp_path, monkeypatch):
    from analysis import holdout_basis_probe as hb
    from scripts import run_holdout_basis_probe as cli

    base = [
        "--manifest", str(tmp_path / "m.json"),
        "--log-dir", str(tmp_path / "reg"),
        "--vault-repo", str(tmp_path / "vault"),
        "--twelvedata-dir", str(tmp_path / "td"),
        "--tiingo-meta-dir", str(tmp_path / "meta"),
        "--calendar-manifest", str(tmp_path / "cal.json"),
        "--submissions", str(tmp_path / "subs.csv"),
        "--issuers", str(tmp_path / "issuers.json"),
        "--sic-map", str(tmp_path / "sic.jsonl"),
        "--as-of-ts", SNAPSHOT.isoformat(),
        "--allow-holdout",
        "--prereg-sha256", "9" * 64,
        "--out", str(tmp_path / "out"),
    ]
    for omit in ("--submissions", "--issuers", "--sic-map"):
        idx = base.index(omit)
        trimmed = base[:idx] + base[idx + 2 :]
        with pytest.raises(SystemExit) as exc:
            cli.main(trimmed)
        assert exc.value.code != 0

    # Passing sec_names={} to run_holdout_probe must fail closed as entity_mismatch (never silently skip!).
    series = {"AAA": _series(HO_SESSIONS, base=40.0), "XLK": _series(HO_SESSIONS, base=90.0)}
    engine = _build_holdout_db(series)
    td_dir = _write_td_dir(tmp_path / "td", series)
    meta_dir = _write_tiingo_meta_dir(tmp_path / "meta", list(series))
    cal = hb.load_calendar_manifest(_write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS))
    m_path, _ = _write_manifest(tmp_path / "m.json", ["AAA"], {"AAA": "2010-01-04"})
    manifest = hb.load_price_manifest(m_path)
    vendors = hb.HoldoutVendorFiles(td_dir, meta_dir, lo=HO_LO, hi=HO_HI, boundary_anchor=cal.boundary_anchor)
    with engine.connect() as conn:
        probe = hb.run_holdout_probe(
            conn, manifest=manifest, calendar=cal, lo=HO_LO, hi=HO_HI,
            as_of_ts=SNAPSHOT, vendors=vendors, sec_names={},
        )
    assert probe["records"]["AAA"]["abstains"] is True
    assert "entity_mismatch" in probe["records"]["AAA"]["reasons"]


def test_cli_computes_code_sha_from_clean_git_tree_and_asserts_flag(tmp_path, monkeypatch):
    from scripts import run_holdout_basis_probe as cli
    import scripts.run_real_panel_scan as rps

    # Create a real clean git repository in tmp_path.
    repo = tmp_path / "clean_repo"
    repo.mkdir()
    subprocess.run(["git", "init", "--initial-branch=main", str(repo)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@grid.local"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "GRID Test"], check=True, capture_output=True)
    (repo / "tracked.txt").write_text("v1\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "init"], check=True, capture_output=True)
    real_head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()

    series = {"AAA": _series(HO_SESSIONS, base=41.0), "XLK": _series(HO_SESSIONS, base=95.0)}
    engine = _build_holdout_db(series)
    td_dir = _write_td_dir(tmp_path / "td", series)
    meta_dir = _write_tiingo_meta_dir(tmp_path / "meta", list(series))
    cal_path = _write_calendar_manifest(tmp_path / "cal.json", [BOUNDARY_ANCHOR] + HO_SESSIONS)
    manifest_path, m_digest = _write_manifest(tmp_path / "m.json", ["AAA"], {"AAA": "2010-01-04"})
    log_dir, vault, prereg, _ = _make_sealed_registry(tmp_path, manifest_digest=m_digest)

    issuers_path = tmp_path / "issuers.json"
    sic_map_path = tmp_path / "sic_map.jsonl"
    subs_path = tmp_path / "subs.csv"
    issuers_path.write_text(json.dumps({"issuers": [{"ticker": "AAA", "cik": 1000, "current_tickers": ["AAA"]}]}), encoding="utf-8")
    sic_map_path.write_text(json.dumps({"cik": 1000, "sic": 7372, "name": "AAA Systems Inc"}) + "\n", encoding="utf-8")
    subs_path.write_text(
        "accession_number,filing_date,issuer_cik,document_type,issuer_ticker\n1000-1,2019-06-01,1000,4,AAA\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(cli, "REPO", repo)
    monkeypatch.setattr(v1, "WITNESS_REMOTE_URL", str(vault.remote))
    monkeypatch.setattr(rps, "read_only_engine", lambda *a, **k: engine)
    monkeypatch.setattr(engine, "dispose", lambda: None)

    base_args = [
        "--manifest", str(manifest_path), "--log-dir", str(log_dir), "--vault-repo", str(vault.cache),
        "--twelvedata-dir", str(td_dir), "--tiingo-meta-dir", str(meta_dir),
        "--calendar-manifest", str(cal_path), "--submissions", str(subs_path),
        "--issuers", str(issuers_path), "--sic-map", str(sic_map_path), "--unpinned-sic-map",
        "--as-of-ts", SNAPSHOT.isoformat(), "--allow-holdout", "--prereg-sha256", prereg,
    ]

    # (a) Omitting --code-sha computes HEAD automatically!
    out_auto = tmp_path / "out_auto"
    cli.main([*base_args, "--out", str(out_auto)])
    rep_auto = json.loads((out_auto / "holdout_basis_report.json").read_text(encoding="utf-8"))
    assert rep_auto["code_sha"] == real_head

    # (b) Supplying matching --code-sha succeeds!
    out_match = tmp_path / "out_match"
    cli.main([*base_args, "--code-sha", real_head, "--out", str(out_match)])
    rep_match = json.loads((out_match / "holdout_basis_report.json").read_text(encoding="utf-8"))
    assert rep_match["code_sha"] == real_head

    # (c) Supplying a free/mismatched --code-sha refuses!
    wrong_sha = ("0" if real_head[0] != "0" else "1") + real_head[1:]
    with pytest.raises(SystemExit, match="code.?sha|match"):
        cli.main([*base_args, "--code-sha", wrong_sha, "--out", str(tmp_path / "out_bad_sha")])
    assert not (tmp_path / "out_bad_sha").exists()

    # (d) Dirty tracked file refuses!
    (repo / "tracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="modified|dirty|code_sha"):
        cli.main([*base_args, "--out", str(tmp_path / "out_dirty")])
    assert not (tmp_path / "out_dirty").exists()
