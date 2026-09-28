"""Bridge provenance, failure and read-only pagination contracts; no live services."""
import json
import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Response
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.routers import gamma_watch as gw


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
