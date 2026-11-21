"""schwab-usage/1: the shared Schwab call ledger (ingestion/schwab_usage.py) and its writer in SchwabMarketClient."""
from __future__ import annotations

import json
import multiprocessing
import os
import stat
from datetime import datetime, timezone
from pathlib import Path

import requests

from ingestion import schwab_usage as U
from ingestion.schwab_market import SchwabMarketClient, SchwabTransientError

from tests.test_schwab_market import FAKE_ACCESS, FAKE_KEY, FAKE_REFRESH, FAKE_SECRET, NOW, FakeResponse, FakeSession, write_token_file

T = datetime(2026, 11, 20, 15, 37, 7, tzinfo=timezone.utc).timestamp()


def lines(d: Path) -> list[list[str]]:
    out = []
    for p in sorted(d.glob("calls-*.tsv")):
        out += [ln.split("\t") for ln in p.read_text().splitlines()]
    return out


def test_record_format_permissions_and_no_query(tmp_path):
    d = tmp_path / "usage"
    assert U.record(d, "record_gamma_time", "get", "/chains?symbol=SPY&apikey=x", 429, now=T)
    (row,) = lines(d)
    assert row == [str(int(T * 1000)), "record_gamma_time", str(os.getpid()), "GET /chains", "429"]
    assert stat.S_IMODE(d.stat().st_mode) == 0o700
    assert stat.S_IMODE((d / "calls-20261120.tsv").stat().st_mode) == 0o600
    assert "SPY" not in (d / "calls-20261120.tsv").read_text() and "apikey" not in (d / "calls-20261120.tsv").read_text()


def test_client_names(monkeypatch):
    monkeypatch.setenv("GRID_SCHWAB_CLIENT", "")
    monkeypatch.setattr("sys.argv", ["/data/grid_v4/grid_release/scripts/record_gamma_time.py"])
    assert U.client_name() == "record_gamma_time"
    assert U.client_name("grid api; rm -rf") == "grid_api_rm_-rf"
    monkeypatch.setenv("GRID_SCHWAB_CLIENT", "hermes")
    assert U.client_name() == "hermes"
    assert len(U.client_name("x" * 200)) == 60


def test_never_raises(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("not a dir")
    assert U.record(blocker / "usage", "c", "GET", "/quotes", 200, now=T) is False


def test_prune_keeps_fourteen_days(tmp_path):
    d = tmp_path / "usage"; d.mkdir(mode=0o700)
    (d / "calls-20261101.tsv").write_text("x\n"); (d / "calls-20261110.tsv").write_text("x\n"); (d / "notes.txt").write_text("keep")
    U._STATE.pop(str(d), None)
    U.record(d, "c", "GET", "/quotes", 200, now=T)
    assert sorted(p.name for p in d.iterdir()) == ["calls-20261110.tsv", "calls-20261120.tsv", "notes.txt"]


def _burst(d, name, n):
    for i in range(n):
        U.record(d, name, "GET", "/quotes", 200, now=T + i / 1000)


def test_concurrent_writers_do_not_interleave(tmp_path):
    d = tmp_path / "usage"
    ps = [multiprocessing.Process(target=_burst, args=(d, "w%d" % k, 200)) for k in range(4)]
    [p.start() for p in ps]; [p.join() for p in ps]
    rows = lines(d)
    assert len(rows) == 800 and all(len(r) == 5 for r in rows)


def test_reader_per_minute_and_cli(tmp_path, capsys):
    d = tmp_path / "usage"
    for i in range(12):
        U.record(d, "gex-watch/quote", "GET", "/quotes", 200, now=T - 60 + i * 5)
    for i in range(3):
        U.record(d, "record_gamma_time", "GET", "/chains", 200, now=T - 6 + i)
    U.record(d, "gex-watch/quote", "GET", "/quotes", 429, now=T)
    table = U.per_minute(U.read(d, T - 120, T + 60))
    m = table["2026-11-20 15:37Z"]
    assert m["calls"]["record_gamma_time"] == 3 and m["refused"]["gex-watch/quote"] == 1
    assert U.main(["--around", "2026-11-20T15:37:07Z", "--minutes", "1", "--dir", str(d)]) == 0
    out = capsys.readouterr().out
    assert "15:37Z" in out and "429: gex-watch/quote 1" in out


def test_client_writes_every_request_and_refresh(tmp_path):
    token = tmp_path / "tokens.json"
    write_token_file(token, access_expires_at=NOW - 10, refresh_expires_at=NOW + 5 * 86400)
    s = FakeSession()
    s.post_queue.append(FakeResponse(200, {"access_token": FAKE_ACCESS, "expires_in": 1800, "refresh_token": FAKE_REFRESH}))
    s.get_queue += [requests.ConnectionError("boom"), FakeResponse(429, headers={"Retry-After": "1"}), FakeResponse(200, {"SPY": {}})]
    c = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=token, session=s, sleep=lambda _s: None,
                           now=lambda: NOW, max_retries=3, client="test-client")
    c._get("/quotes", {"symbols": "SPY"})
    rows = lines(tmp_path / "usage")
    assert [(r[1], r[3], r[4]) for r in rows] == [("test-client", "POST oauth/token", "200"), ("test-client", "GET /quotes", "net"),
                                                 ("test-client", "GET /quotes", "429"), ("test-client", "GET /quotes", "200")]
    text = "".join((tmp_path / "usage").joinpath(p.name).read_text() for p in (tmp_path / "usage").glob("*.tsv"))
    for secret in (FAKE_ACCESS, FAKE_REFRESH, FAKE_KEY, FAKE_SECRET):
        assert secret not in text


def test_exhausted_retries_are_all_on_the_ledger(tmp_path):
    token = tmp_path / "tokens.json"
    write_token_file(token, access_expires_at=NOW + 1500, refresh_expires_at=NOW + 5 * 86400)
    s = FakeSession(); s.get_queue += [FakeResponse(429)] * 3
    c = SchwabMarketClient(app_key=FAKE_KEY, app_secret=FAKE_SECRET, token_file=token, session=s, sleep=lambda _s: None,
                           now=lambda: NOW, max_retries=2, client="t")
    try:
        c._get("/quotes", {"symbols": "SPY"})
    except SchwabTransientError:
        pass
    assert [r[4] for r in lines(tmp_path / "usage")] == ["429", "429", "429"]
