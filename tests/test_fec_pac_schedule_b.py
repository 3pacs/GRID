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
