"""Research Desk findings: a catalogue loads only when it hashes to what it claims; reads, ranking and lines."""
import json

import pytest
from fastapi import HTTPException

from analysis import research_findings as RF


def record(**kw):
    r = {"id": "stress-reversal-look", "family": "reversal", "area": "cross_section",
         "question": "Does buying a sector's 5-day losers pay on VIX spike days?", "rule": "reversal_5d on spike-day nights",
         "kind": "confirmation_look", "verdict": "NOT_CONFIRMED", "status": "closed", "recorded_at": "2026-11-19",
         "window": {"from": "2023-01-03", "to": "2026-11-04"}, "sample": {"n": 58, "unit": "nights", "clusters": 15},
         "effect": {"value": 12.3, "unit": "bp a night net", "ci95": [-3.6, 28.2], "t": 1.51},
         "data_spent": ["2023-2026 spike days"],
         "source": {"path": "agentic-20261105/desk-state/STRESS-REVERSAL-RESULT-20261119.md", "quotes": ["..."], "sha256": "ab" * 32}}
    r.update(kw)
    return r


def catalogue(records, **kw):
    c = {"schema": RF.SCHEMA, "records": records, "count": len(records), "records_sha256": RF.records_sha256(records),
         "repo_commit": "99b49b1"}
    c.update(kw)
    return json.dumps(c).encode()


def test_parse_sound_catalogue():
    cat = RF.parse_catalogue(catalogue([record(), record(id="other", verdict="DESCRIPTION", kind="description")]))
    assert cat["count"] == 2


@pytest.mark.parametrize("raw, msg", [
    (b"not json", "not JSON"),
    (json.dumps({"schema": "x/1"}).encode(), "schema"),
    (catalogue([record()], records_sha256="0" * 64), "records_sha256"),
    (catalogue([record()], count=2), "count"),
    (catalogue([record(verdict="MAYBE")]), "verdict"),
    (catalogue([record(), record()]), "duplicate id"),
    (catalogue([record(recorded_at="19 Nov")]), "recorded_at"),
    (catalogue([record(question="")]), "lacks"),
])
def test_parse_refuses(raw, msg):
    with pytest.raises(RF.CatalogueError, match=msg):
        RF.parse_catalogue(raw)


def test_row_mapping():
    row = RF._row(record(), "f" * 64)
    assert (row["effect_value"], row["ci_low"], row["ci_high"], row["t_stat"], row["sample_n"]) == (12.3, -3.6, 28.2, 1.51, 58.0)
    assert row["window_from"] == "2023-01-03" and row["recorded_at"] == "2026-11-19"
    assert json.loads(row["data_spent"]) == ["2023-2026 spike days"]
    bare = RF._row(record(window=None, sample=None, effect={"value": True}), "f" * 64)
    assert bare["effect_value"] is None and bare["window_from"] is None and bare["ci_low"] is None


class FakeConn:
    def __init__(self, store):
        self.store = store

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if sql.startswith("SELECT 1 FROM research_findings_loads"):
            return FakeResult(self.store["loads"].get(params["s"]))
        if sql.startswith("INSERT INTO research_findings_loads"):
            self.store["loads"][params["s"]] = (1,)
        elif sql.startswith("INSERT INTO research_findings"):
            self.store["rows"] += list(params)
        return FakeResult(None)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeResult:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeEngine:
    def __init__(self):
        self.store = {"loads": {}, "rows": []}

    def begin(self):
        return FakeConn(self.store)


def test_load_once_per_version():
    eng, raw = FakeEngine(), catalogue([record(), record(id="b")])
    first = RF.load_catalogue(eng, raw, "desk-findings.json")
    assert first["status"] == "loaded" and first["count"] == 2 and len(eng.store["rows"]) == 2
    again = RF.load_catalogue(eng, raw, "desk-findings.json")
    assert again["status"] == "already loaded" and len(eng.store["rows"]) == 2


def test_rank_and_line():
    rows = [dict(RF._row(record(), "f"), effect_value=12.3, ci_low=-3.6, ci_high=28.2, t_stat=1.51, sample_n=58,
                 sample_unit="nights", question=record()["question"], rule="reversal_5d on spike-day nights"),
            dict(RF._row(record(id="dix", family="dix", question="Does a high DIX reading lead SPY?", rule="DIX >= 45%",
                                 recorded_at="2026-11-13"), "f"))]
    hits = RF.rank_for_topic(rows, "should I buy losers on a VIX spike day?")
    assert [h["finding_id"] for h in hits] == ["stress-reversal-look"]
    assert RF.rank_for_topic(rows, "what about the") == []
    text = RF.line(hits[0])
    assert text.startswith("[NOT_CONFIRMED] Does buying")
    assert "Effect +12.3 bp a night net (95% -3.6 to +28.2), t 1.51; n 58 nights; 2023-01-03..2026-11-04; status closed." in text


def test_context_is_empty_on_error(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("no table")
    monkeypatch.setattr(RF, "current_findings", boom)
    assert RF.context_for_prompt(object(), "spike day reversal") == ""


def test_router_validates_and_paginates(monkeypatch):
    from api.routers import research_findings as R
    monkeypatch.setattr(R, "get_db_engine", lambda: object())
    monkeypatch.setattr(R, "current_findings", lambda *a, **k: ([{"finding_id": "x"}], 3))
    out = R.list_findings(family=None, area=None, verdict=None, q=None, limit=1, offset=0)
    assert out == {"entries": [{"finding_id": "x"}], "total": 3, "limit": 1, "offset": 0, "has_more": True}
    with pytest.raises(HTTPException) as e:
        R.list_findings(family=None, area="moon", verdict=None, q=None, limit=1, offset=0)
    assert e.value.status_code == 422
