"""Bridge provenance, failure and read-only pagination contracts; no live services."""
import json
import sqlite3
import time
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI, Response
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.routers import gamma_watch as gw

# A real gex-screen/2 frame (the v4 gex-watch candidate's PublicProjection over
# the live state at 2026-10-21T02:55Z); its ``served_at`` is epoch seconds.
V4_FRAME = Path(__file__).parent / "fixtures" / "gex_screen_v2" / "v4_candidate_projection_20261021T0255Z.json"


def client():
    app = FastAPI()
    app.include_router(gw.router)
    app.dependency_overrides[require_auth] = lambda: {"username": "test"}
    return TestClient(app)


def test_snapshot_preserves_source_semantics(monkeypatch):
    data = {"served_at": datetime.now(timezone.utc).isoformat(), "quote": {"price": 0, "as_of": "old"},
            "structural_live": {"rows": [{"value": None, "status": "unavailable"}]},
            "gex": {"assumptions": "dealer sign assumed"}, "gex_error": "stale"}
    monkeypatch.setattr(gw, "_read_upstream", lambda path: data)
    r = client().get('/api/v1/gamma-watch/state')
    assert r.status_code == 200
    assert r.headers['cache-control'] == 'no-store'
    assert r.json()['data'] == data


def test_stale_transport_fails_closed(monkeypatch):
    monkeypatch.setattr(gw, '_read_upstream', lambda p: {'served_at': (datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()})
    r = client().get('/api/v1/gamma-watch/state')
    assert r.status_code == 503
    assert r.json()['data'] is None


def test_upstream_error_does_not_leak_paths(monkeypatch):
    def fail(path):
        raise OSError('/private/account/path')
    monkeypatch.setattr(gw, '_read_upstream', fail)
    r = client().get('/api/v1/gamma-watch/state')
    assert r.status_code == 503
    assert '/private' not in r.text


def test_query_allowlist_and_listing_only(monkeypatch):
    calls = []
    monkeypatch.setattr(gw, '_read_upstream', lambda p: calls.append(p) or {})
    c = client()
    assert c.get('/api/v1/gamma-watch/contracts?symbol=SPY&contract=anything').status_code == 200
    assert calls == ['/api/contracts?symbol=SPY']
    assert c.get('/api/v1/gamma-watch/contracts?symbol=http://evil').status_code == 422
    assert c.get('/api/v1/gamma-watch/journal?since=nan').status_code == 422
    assert c.get('/api/v1/gamma-watch/archive?stream=sqlite_master').status_code == 422


def test_archive_readonly_cursor_and_receipt(monkeypatch, tmp_path):
    p = tmp_path/'journal.sqlite3'
    with sqlite3.connect(p) as db:
        db.execute('CREATE TABLE artifacts(kind TEXT,version TEXT,received REAL,body TEXT)')
        db.executemany('INSERT INTO artifacts VALUES(?,?,?,?)', [('structural_live', str(i), 100+i, json.dumps({'value': None, 'source_time': 'old'})) for i in range(3)])
    before = p.read_bytes()
    monkeypatch.setattr(gw, 'JOURNAL', p)
    c = client()
    first = c.get('/api/v1/gamma-watch/archive?limit=2').json()
    second = c.get('/api/v1/gamma-watch/archive?limit=2&after='+str(first['next_after'])).json()
    assert [r['received'] for r in first['records']+second['records']] == [100,101,102]
    assert first['records'][0]['body']['value'] is None
    assert p.read_bytes() == before


def test_missing_archive_is_not_created(monkeypatch, tmp_path):
    p = tmp_path/'absent.sqlite3'
    monkeypatch.setattr(gw, 'JOURNAL', p)
    assert client().get('/api/v1/gamma-watch/archive').status_code == 503
    assert not p.exists()


def test_redirect_refused():
    assert gw.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://elsewhere') is None


def test_archive_size_cap_keeps_cursor_progress(monkeypatch, tmp_path):
    p = tmp_path/'journal.sqlite3'
    with sqlite3.connect(p) as db:
        db.execute('CREATE TABLE samples(received REAL,source_time TEXT,price REAL,source TEXT)')
        db.executemany('INSERT INTO samples VALUES(?,?,?,?)', [(100,'old',1,'RTD')]*4)
    monkeypatch.setattr(gw, 'JOURNAL', p)
    monkeypatch.setattr(gw, 'MAX_BYTES', 150)
    r = gw.get_archive(Response(), stream='samples', after=0, limit=4)
    assert len(r['records']) == 1
    assert r['next_after'] == 1


def _v4_frame(served_at) -> dict:
    frame = json.loads(V4_FRAME.read_text(encoding="utf-8"))
    assert frame["schema"] == "gex-screen/2"
    assert isinstance(frame["served_at"], float)  # the shape that used to raise AttributeError
    return dict(frame, served_at=served_at)


def test_epoch_served_at_from_gex_screen_v2_frame_is_available(monkeypatch):
    frame = _v4_frame(time.time())
    monkeypatch.setattr(gw, "_read_upstream", lambda path: frame)
    r = client().get("/api/v1/gamma-watch/state")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "available"
    assert r.json()["data"]["served_at"] == pytest.approx(frame["served_at"])
    assert r.json()["data"]["delayed_gex"] == frame["delayed_gex"]


def test_integer_epoch_served_at_is_available(monkeypatch):
    monkeypatch.setattr(gw, "_read_upstream", lambda path: {"served_at": int(time.time())})
    assert client().get("/api/v1/gamma-watch/state").status_code == 200


def test_iso_served_at_with_z_suffix_still_available(monkeypatch):
    served = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    monkeypatch.setattr(gw, "_read_upstream", lambda path: {"served_at": served})
    r = client().get("/api/v1/gamma-watch/state")
    assert r.status_code == 200
    assert r.json()["data"]["served_at"] == served


@pytest.mark.parametrize("payload", [
    {},                                              # served_at missing
    {"served_at": None},
    {"served_at": "soon"},
    {"served_at": True},
    {"served_at": "2026-10-21T02:55:46"},           # naive: no offset
    {"served_at": 1e300},                            # fromtimestamp overflow
    {"served_at": [1792551346.0]},
])
def test_served_at_missing_or_invalid_is_503_not_exception(monkeypatch, payload):
    monkeypatch.setattr(gw, "_read_upstream", lambda path: payload)
    r = client().get("/api/v1/gamma-watch/state")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unavailable" and body["data"] is None
    assert body["reason"] == gw.UNAVAILABLE_REASON


def test_stale_epoch_served_at_fails_closed(monkeypatch):
    monkeypatch.setattr(gw, "_read_upstream", lambda path: _v4_frame(time.time() - 60))
    assert client().get("/api/v1/gamma-watch/state").status_code == 503
    monkeypatch.setattr(gw, "_read_upstream", lambda path: _v4_frame(time.time() + 30))
    assert client().get("/api/v1/gamma-watch/state").status_code == 503


def _http_error(code: int):
    def fail(path):
        raise urllib.error.HTTPError(gw.UPSTREAM + path, code, "nope", {}, None)
    return fail


def test_journal_route_404_reports_route_not_served(monkeypatch):
    # The v4 collector removes /api/journal and /api/contracts.
    monkeypatch.setattr(gw, "_read_upstream", _http_error(404))
    c = client()
    for route in ("/api/v1/gamma-watch/journal?since=0", "/api/v1/gamma-watch/contracts?symbol=SPY"):
        r = c.get(route)
        assert r.status_code == 503, route
        assert r.json()["reason"] == "route not served by collector"
        assert r.json()["data"] is None


def test_other_upstream_http_errors_keep_the_generic_reason(monkeypatch):
    monkeypatch.setattr(gw, "_read_upstream", _http_error(500))
    r = client().get("/api/v1/gamma-watch/state")
    assert r.status_code == 503
    assert r.json()["reason"] == gw.UNAVAILABLE_REASON


def test_served_at_parser_contract():
    assert gw._parse_served_at(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert gw._parse_served_at("2026-10-21T02:55:46Z") == datetime(2026, 10, 21, 2, 55, 46, tzinfo=timezone.utc)
    assert gw._parse_served_at("2026-10-21T02:55:46.017389+00:00").tzinfo is not None
    for bad in (None, True, float("nan"), float("inf"), "", "2026-10-21", object()):
        with pytest.raises((ValueError, TypeError)):
            gw._parse_served_at(bad)
