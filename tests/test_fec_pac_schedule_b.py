"""FEC PAC disbursements (schedule_b) request shape (2026-11-07).

Every PAC fetch returned 422: schedule_b cannot sort on
contribution_receipt_date (schedule_a's field). It also ignored ``cycle``
(the filter is two_year_transaction_period) and paged with last_index alone.
"""

from __future__ import annotations

from ingestion.altdata.campaign_finance import CampaignFinancePuller


def _row(i: int) -> dict:
    return {"disbursement_amount": 1000 + i, "recipient_name": f"CANDIDATE {i}",
            "disbursement_date": "2026-10-01"}


def _puller(monkeypatch, pages: list[dict]) -> tuple[CampaignFinancePuller, list[dict]]:
    p = object.__new__(CampaignFinancePuller)  # no DB: request shape only
    calls: list[dict] = []

    def fake_get(endpoint, params=None):  # noqa: ANN001
        assert endpoint == "/schedules/schedule_b/"
        calls.append(dict(params))
        return pages[len(calls) - 1]

    monkeypatch.setattr(p, "_fec_get", fake_get, raising=False)
    monkeypatch.setattr(p, "_parse_contribution", lambda raw, cid: raw, raising=False)
    return p, calls


def test_request_uses_schedule_b_filters_and_sort(monkeypatch) -> None:
    p, calls = _puller(monkeypatch, [{"results": [_row(1)], "pagination": {"last_indexes": None}}])
    out = p._fetch_pac_contributions("C00520809", cycle=2026)
    assert len(out) == 1
    assert calls == [{"committee_id": "C00520809", "two_year_transaction_period": 2026,
                      "per_page": 100, "sort": "-disbursement_date"}]


def test_pagination_passes_every_cursor_key(monkeypatch) -> None:
    cursor = {"last_index": "4071420231753456662", "last_disbursement_date": "2026-09-30"}
    pages = [
        {"results": [_row(i) for i in range(100)], "pagination": {"last_indexes": cursor}},
        {"results": [_row(i) for i in range(7)], "pagination": {"last_indexes": None}},
    ]
    p, calls = _puller(monkeypatch, pages)
    out = p._fetch_pac_contributions("C00103697", cycle=2026)
    assert len(out) == 107
    assert len(calls) == 2
    assert {k: calls[1][k] for k in cursor} == cursor
    assert calls[1]["sort"] == "-disbursement_date" and "cycle" not in calls[1]


def test_short_page_or_missing_cursor_stops(monkeypatch) -> None:
    pages = [{"results": [_row(i) for i in range(100)], "pagination": {"last_indexes": {"last_index": None}}}]
    p, calls = _puller(monkeypatch, pages)
    assert len(p._fetch_pac_contributions("C00103697", cycle=2026)) == 100
    assert len(calls) == 1


# ── a refused (rate-limited) run is not "no new data" ────────────────────


def _rate_limited_puller(monkeypatch, *, stored_pac: int, api_key: str = "DEMO_KEY"):
    p = object.__new__(CampaignFinancePuller)
    p.api_key = api_key

    def pac():
        p._rate_limited_for = 57732  # what _fec_get records on a long 429
        return {"status": "SUCCESS", "total_fetched": stored_pac, "stored": stored_pac}

    monkeypatch.setattr(p, "pull_pac_contributions", pac, raising=False)
    monkeypatch.setattr(p, "pull_individual_contributions",
                        lambda: {"status": "SUCCESS", "total_fetched": 0, "stored": 0}, raising=False)
    return p


def test_fully_rate_limited_run_is_failed_with_the_reason(monkeypatch) -> None:
    out = _rate_limited_puller(monkeypatch, stored_pac=0).pull_all()
    assert out["status"] == "FAILED" and out["rows_inserted"] == 0
    assert "rate limited" in out["error"] and "FEC_API_KEY" in out["error"]


def test_partly_rate_limited_run_is_partial(monkeypatch) -> None:
    out = _rate_limited_puller(monkeypatch, stored_pac=12, api_key="real").pull_all()
    assert out["status"] == "PARTIAL" and out["rows_inserted"] == 12
    assert "rate limited" in out["note"] and "FEC_API_KEY" not in out["note"]


def test_unthrottled_run_keeps_its_status(monkeypatch) -> None:
    p = object.__new__(CampaignFinancePuller)
    p.api_key = "real"
    monkeypatch.setattr(p, "pull_pac_contributions", lambda: {"status": "SUCCESS", "stored": 3}, raising=False)
    monkeypatch.setattr(p, "pull_individual_contributions", lambda: {"status": "SUCCESS", "stored": 0}, raising=False)
    out = p.pull_all()
    assert out["status"] == "SUCCESS" and "error" not in out
