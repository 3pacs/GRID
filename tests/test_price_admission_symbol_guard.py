"""Successful VS1 cached bodies belong to the requested ticker; no live I/O."""

from __future__ import annotations

import json
import socket

import pytest

from analysis import price_admission_fetch as fetch


@pytest.fixture(autouse=True)
def deny_live_io(monkeypatch):
    import psycopg2
    import requests
    from sqlalchemy.engine import Engine

    def denied(*_args, **_kwargs):
        raise AssertionError("fixture reader tests must not reach a database or provider")

    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(requests.sessions.Session, "request", denied)
    monkeypatch.setattr(psycopg2, "connect", denied)
    monkeypatch.setattr(Engine, "connect", denied)
    monkeypatch.setattr(fetch, "requests_get", denied)


def _body(symbol="AAA", *, meta="canonical", outcome="ok", extra_date="2019-12-31", overlap=10):
    if outcome != "ok":
        return json.dumps({"status": "error", "code": 404, "message": "no data"}).encode()
    doc = {"status": "ok", "values": [{"datetime": "2019-12-30", "close": str(overlap)},
                                       {"datetime": extra_date, "close": "11"}]}
    if meta != "missing":
        doc["meta"] = {"symbol": symbol} if meta == "canonical" else meta
    return json.dumps(doc).encode()


def _cache(tmp_path, *, ticker="AAA", part="main", adjust="all", body=None, outcome="ok"):
    """Hash-verified FetchLog entries; entry.ticker is deliberately not identity authority."""
    log = fetch.FetchLog(tmp_path / "fetch_log.jsonl")
    entries = {}

    def save(mode, kind, raw, result="ok"):
        suffix = "|supplement" if kind == "supplement" else ""
        key = f"{ticker}|{mode}{suffix}"
        name = f"{ticker}/{mode}{'.supplement' if suffix else ''}.json.gz"
        entry = {"key": key, "ticker": "UNTRUSTED-RECEIPT", "outcome": result, "file": name,
                 "file_sha256": fetch.write_gz(tmp_path / name, raw),
                 "params": fetch.td_params(ticker, mode)}
        if part == "required" and kind == "main":
            entry["params"]["end_date"] = "2019-12-31"
        log.append(entry)
        entries[key] = entry

    canonical = fetch.td_symbol(ticker)
    for mode in fetch.TD_ADJUST_MODES:
        main = _body(canonical)
        if part == "required":
            doc = json.loads(main)
            doc["values"] = doc["values"][:1]
            main = json.dumps(doc).encode()
        save(mode, "main", body if part == "main" and mode == adjust else main,
             outcome if part == "main" and mode == adjust else "ok")
        if part != "main":
            save(mode, "supplement", body if mode == adjust else _body(canonical),
                 outcome if mode == adjust else "ok")
    return entries


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
@pytest.mark.parametrize("part", ["main", "required", "optional"])
@pytest.mark.parametrize("meta", ["missing", None, [], "AAA", {}, {"symbol": 123},
                                 {"symbol": "BBB"}, {"symbol": "aaa"}])
def test_successful_wrong_or_missing_body_identity_stops_reader(tmp_path, adjust, part, meta):
    _cache(tmp_path, part=part, adjust=adjust, body=_body(meta=meta))
    with pytest.raises(fetch.FetchStopped, match="symbol does not match requested AAA"):
        fetch.load_td_closes(tmp_path, "AAA")


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
@pytest.mark.parametrize("part", ["main", "required", "optional"])
def test_canonical_share_class_identity_is_accepted_without_optional_merge(tmp_path, adjust, part):
    _cache(tmp_path, ticker="BRK-B", part=part, adjust=adjust, body=_body("BRK.B"))
    got = fetch.load_td_closes(tmp_path, "BRK-B")
    assert got["state"] == "ok"
    assert got[adjust] == {"2019-12-30": 10.0, "2019-12-31": 11.0}
    assert bool(got["supplemented"]) == (part == "required")


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
@pytest.mark.parametrize("part", ["main", "required", "optional"])
def test_raw_dash_is_not_an_alias_for_returned_canonical_dot(tmp_path, adjust, part):
    _cache(tmp_path, ticker="BRK-B", part=part, adjust=adjust, body=_body("BRK-B"))
    with pytest.raises(fetch.FetchStopped, match="requested BRK.B"):
        fetch.load_td_closes(tmp_path, "BRK-B")


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
@pytest.mark.parametrize("part", ["main", "required", "optional"])
@pytest.mark.parametrize("outcome", ["unavailable", "error"])
def test_recorded_error_bodies_keep_existing_exclusion_handling(tmp_path, adjust, part, outcome):
    _cache(tmp_path, part=part, adjust=adjust, body=_body(outcome=outcome), outcome=outcome)
    # error is not final in FetchLog; explicit done exercises the legacy caller contract as well.
    done = fetch.FetchLog(tmp_path / "fetch_log.jsonl").entries()
    got = fetch.load_td_closes(tmp_path, "AAA", {e["key"]: e for e in done})
    if part == "main":
        assert adjust not in got and got["state"] == "unavailable"
    else:
        assert got["state"] == "ok"
        assert got[adjust] == ({"2019-12-30": 10.0} if part == "required"
                               else {"2019-12-30": 10.0, "2019-12-31": 11.0})


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
def test_required_supplement_overlap_and_holdout_rules_remain(tmp_path, adjust):
    _cache(tmp_path, part="required", adjust=adjust, body=_body(overlap=999))
    assert fetch.load_td_closes(tmp_path, "AAA")["state"] == "supplement_mismatch"
    _cache(tmp_path, part="required", adjust=adjust, body=_body(extra_date="2020-01-02"))
    got = fetch.load_td_closes(tmp_path, "AAA")
    assert got["state"] == "holdout_rows" and adjust not in got


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
def test_optional_successful_rows_are_validated_but_still_not_merged(tmp_path, adjust):
    _cache(tmp_path, part="optional", adjust=adjust, body=_body(overlap=999, extra_date="2020-01-02"))
    got = fetch.load_td_closes(tmp_path, "AAA")
    assert got["state"] == "ok" and got["holdout_rows"] == 0
    assert got[adjust] == {"2019-12-30": 10.0, "2019-12-31": 11.0}
    assert got["supplemented"] == []


def test_closes_checks_actual_requested_ticker_instead_of_receipt_ticker(tmp_path):
    entries = _cache(tmp_path, body=_body("AAA"))
    entry = {**entries["AAA|all"], "ticker": "BBB"}
    assert fetch._closes(tmp_path, entry, "AAA") == ({"2019-12-30": 10.0, "2019-12-31": 11.0}, 0)
    with pytest.raises(fetch.FetchStopped, match="requested BBB"):
        fetch._closes(tmp_path, entry, "BBB")


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
@pytest.mark.parametrize("values", [[None], 123])
def test_optional_unused_rows_keep_existing_ignored_behavior(tmp_path, adjust, values):
    body = json.dumps({"status": "ok", "meta": {"symbol": "AAA"}, "values": values}).encode()
    _cache(tmp_path, part="optional", adjust=adjust, body=body)
    got = fetch.load_td_closes(tmp_path, "AAA")
    assert got["state"] == "ok" and got[adjust]["2019-12-31"] == 11


@pytest.mark.parametrize("adjust", fetch.TD_ADJUST_MODES)
@pytest.mark.parametrize("main_outcome", ["missing", "unavailable"])
def test_successful_optional_identity_is_checked_even_without_successful_main(tmp_path, adjust, main_outcome):
    entries = _cache(tmp_path, part="optional", adjust=adjust, body=_body("BBB"))
    if main_outcome == "missing":
        del entries[f"AAA|{adjust}"]
    else:
        entries[f"AAA|{adjust}"]["outcome"] = main_outcome
    with pytest.raises(fetch.FetchStopped, match="requested AAA"):
        fetch.load_td_closes(tmp_path, "AAA", entries)
