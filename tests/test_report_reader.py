"""report_reader: every report gets one read; structured and routine ones without a model, the rest by the local
model; failures are retried then counted as unreadable; nothing is dropped."""
import json

from intelligence import report_reader as RR

STRUCTURED = """# GEX deploy

## What changed
- **Deployed generation Y** to grid-svr.

## What was verified
- Health 200.

## What is blocked
- Merging GRID #1267 is waiting on Anik.

## What is left
- Check the 11-27 early close.
- nothing
"""


def row(**kw):
    r = {"id": "00000000-0000-0000-0000-000000000001", "date": "2026-11-21", "agent": "claude", "host": "h",
         "title": "t", "body_md": STRUCTURED, "attempts": 0}
    r.update(kw)
    return r


class Client:
    def __init__(self, reply):
        self.reply, self.calls = reply, 0

    def chat(self, messages, temperature, num_predict):
        self.calls += 1
        return self.reply


def test_sections_and_bullets():
    sec = RR.sections(STRUCTURED)
    assert set(sec) == {"changed", "verified", "blocked", "left"}
    assert RR.bullets(sec["left"]) == ["Check the 11-27 early close."]          # "nothing" dropped
    assert RR.bullets("One sentence. Another one.") == ["One sentence.", "Another one."]


def test_structured_read():
    r = RR.structured_read(row())
    assert r["how"] == "structured" and r["kind"] == "blocked"
    assert r["summary"] == "Deployed generation Y to grid-svr."                  # bold stripped
    assert r["needs_anik"] is True                                              # a blocked item waits on Anik
    assert r["open_items"] == ["Check the 11-27 early close.", "Merging GRID #1267 is waiting on Anik."]
    assert RR.structured_read(row(body_md="Just prose, no sections.")) is None


def test_routine():
    assert RR.is_routine("hermes-operator", "Hermes cycle — healthy", "## Hermes cycle — healthy\nall fine")
    assert not RR.is_routine("hermes-operator", "cycle", "## Events\npuller FAILED with timeout")
    assert not RR.is_routine("claude", "Hermes cycle — healthy", "")
    r = RR.read_one(row(agent="gpu-watch", title="gpu ok", body_md="temps normal"), None)
    assert r["how"] == "routine" and r["kind"] == "routine"


def test_llm_read_and_parse():
    good = json.dumps({"summary": "Fixed the puller.", "kind": "change", "needs_anik": True,
                       "open_items": ["merge #12 needs Anik", "none"], "results": ["t 1.5"]})
    c = Client("Here you go:\n" + good)
    r = RR.read_one(row(body_md="prose only, PR #1297 and commit a0f798a6"), c)
    assert c.calls == 1 and r["how"] == "llm" and r["needs_anik"] is True
    assert r["open_items"] == ["merge #12 needs Anik"]
    assert r["refs"] == {"prs": ["1297"], "commits": ["a0f798a6"]}
    assert RR.parse_llm("not json") is None
    assert RR.parse_llm(json.dumps({"summary": ""})) is None
    assert RR.parse_llm(json.dumps({"summary": "x", "kind": "weird"}))["kind"] == "other"
    assert RR.read_one(row(body_md="prose only"), Client(None)) is None          # the model gave nothing
    assert RR.read_one(row(body_md="prose only"), None) is None                 # no model: not read, retried later


class Result:
    def __init__(self, rows=None, scalar=None):
        self.rows, self._scalar = rows or [], scalar

    def fetchall(self):
        return self.rows


class Mapped:
    def __init__(self, d):
        self._mapping = d


class Conn:
    def __init__(self, eng):
        self.eng = eng

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if sql.startswith("SELECT r.id::text AS id"):
            return Result([Mapped(r) for r in self.eng.pending])
        if sql.startswith("INSERT INTO agent_report_reads"):
            self.eng.reads.append(params)
        elif sql.startswith("INSERT INTO agent_report_read_attempts"):
            self.eng.attempts.append(params)
        return Result()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Engine:
    def __init__(self, pending):
        self.pending, self.reads, self.attempts = pending, [], []

    def connect(self):
        return Conn(self)

    def begin(self):
        return Conn(self)


def test_batch_reads_retries_and_limits():
    prose = [row(id=f"id-{i}", body_md="prose only") for i in range(3)]
    eng = Engine([row(id="s"), row(id="h", agent="hermes-operator", title="Hermes cycle — healthy", body_md="ok"),
                  *prose, row(id="dead", body_md="prose only", attempts=2)])
    out = RR.read_batch(eng, client=Client(None), llm_limit=2)
    c = out["counts"]
    assert (c["structured"], c["routine"], c["deferred"]) == (1, 1, 2)          # two prose reports wait for the next run
    assert c["failed"] == 2 and len(eng.attempts) == 2                          # model gave nothing: attempts recorded
    hows = {p["report_id"]: p["how"] for p in eng.reads}
    assert hows["s"] == "structured" and hows["h"] == "routine"
    assert "dead" not in hows                                                    # deferred by the LLM limit this run
    eng2 = Engine([row(id="dead", body_md="prose only", attempts=2)])
    RR.read_batch(eng2, client=Client(None), llm_limit=5)
    assert eng2.reads[0]["how"] == "unreadable"                                  # third failure: counted, not dropped


def test_dry_run_writes_nothing():
    eng = Engine([row(id="s")])
    out = RR.read_batch(eng, client=None, dry_run=True)
    assert eng.reads == [] and out["reads"][0]["how"] == "structured"
