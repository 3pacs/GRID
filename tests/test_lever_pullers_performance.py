"""Performance guards for lever-puller dashboard helpers."""

from __future__ import annotations

from datetime import date


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        return _Result(self._rows)


class _Engine:
    def __init__(self, rows):
        self._rows = rows

    def connect(self):
        return _Conn(self._rows)


def _puller(source_id: str = "source-a"):
    from intelligence.lever_pullers import LeverPuller

    return LeverPuller(
        id=f"insider:{source_id}",
        name=source_id,
        category="insider",
        influence_rank=0.6,
        trust_score=0.8,
        position="Insider",
        motivation_model="routine",
    )


def test_active_events_can_reuse_identified_pullers(monkeypatch):
    from intelligence import lever_pullers as lp

    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    monkeypatch.setattr(lp, "identify_lever_pullers", lambda engine: (_ for _ in ()).throw(AssertionError("rescanned pullers")))
    monkeypatch.setattr(lp, "assess_motivation", lambda puller, action, engine: "routine")

    rows = [(
        "insider",
        "source-a",
        "NVDA",
        date.today(),
        "BUY",
        "{}",
        0.7,
    )]

    events = lp.get_active_lever_events(_Engine(rows), days=14, pullers=[_puller()])

    assert len(events) == 1
    assert events[0].puller.id == "insider:source-a"


def test_lever_convergence_can_reuse_identified_pullers(monkeypatch):
    from intelligence import lever_pullers as lp

    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    monkeypatch.setattr(lp, "identify_lever_pullers", lambda engine: (_ for _ in ()).throw(AssertionError("rescanned pullers")))

    pullers = [_puller("source-a"), _puller("source-b")]
    rows = [
        ("NVDA", "insider", "source-a", "BUY", date.today(), 0.7, "{}"),
        ("NVDA", "insider", "source-b", "BUY", date.today(), 0.8, "{}"),
    ]

    convergences = lp.find_lever_convergence(_Engine(rows), pullers=pullers)

    assert convergences[0]["ticker"] == "NVDA"
    assert len(convergences[0]["pullers"]) == 2
