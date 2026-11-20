"""Bounded and complete SPX contract-OI snapshots from the licensed client."""
import asyncio
from datetime import datetime, timedelta, timezone
import json

import pytest

from api import gamma_board_oi as oi

NOW = datetime(2026, 11, 10, 15, tzinfo=timezone.utc)
DAY = NOW.date()
NEXT = "https://api.polygon.io/v3/snapshot/options/I%3ASPX?cursor=page_two"


def row(strike, side="P", count=2):
    return {"details": {"ticker": f"O:SPXW261120{side}{strike * 1000:08d}",
                        "expiration_date": "2026-11-20", "strike_price": strike,
                        "contract_type": "call" if side == "C" else "put",
                        "shares_per_contract": 100}, "open_interest": count}


def page(rows, **fields):
    return {"status": "OK", "results": rows, **fields}


class Response:
    def __init__(self, doc, status=200):
        self.doc, self.status, self.content = doc, status, self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def iter_chunked(self, _size):
        blob = json.dumps(self.doc).encode()
        yield blob[:15]
        yield blob[15:]


@pytest.fixture
def provider(monkeypatch):
    replies, requests = [], []
    class Client:
        def __init__(self, **options):
            assert options["headers"]["Authorization"] == "Bearer sample"
            assert options["trust_env"] is False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def get(self, url, *, params, allow_redirects):
            assert allow_redirects is False and url == oi._URL
            requests.append(params)
            return replies.pop(0)
    monkeypatch.setattr(oi.aiohttp, "ClientSession", Client)
    monkeypatch.setattr(oi.settings, "POLYGON_API_KEY", "sample")
    monkeypatch.setattr(oi, "_cache", None)
    monkeypatch.setattr(oi, "_failed_until", 0)
    monkeypatch.setattr(oi, "_LIMIT", 2)
    return replies, requests


def read(low=7192, high=7874):
    return oi.read_spx_oi(day=DAY, low=low, high=high, now=NOW)


def test_paginated_exact_contracts_cached_without_aliasing_or_fabricating_missing_oi(provider):
    replies, requests = provider
    replies.extend([Response(page([row(7300, count=3429), row(7300, "C", 0)], next_url=NEXT)),
                    Response(page([row(7350, count=None)]))])
    values, stamp = read()
    assert values == {"SPXW261120P07300000":3429, "SPXW261120C07300000":0}
    assert stamp.tzinfo == timezone.utc
    assert requests == [{"expiration_date.gte": "2026-11-10", "expiration_date.lte": "2026-12-10",
                         "strike_price.gte":"7100","strike_price.lte":"7900","limit":"2"},
                        {"cursor":"page_two"}]
    values.clear()
    assert read(low=7185,high=7881)[0]["SPXW261120P07300000"] == 3429
    assert read()[1] == stamp and len(requests) == 2


@pytest.mark.parametrize("bad_next", [
    NEXT.replace("api.polygon.io", "example.org"), NEXT.replace("https:", "http:"),
    NEXT.replace("options", "elsewhere"), NEXT.replace("api.polygon.io","user@api.polygon.io"),
    NEXT + "&apiKey=private_sentinel", NEXT + "&cursor=third", "", NEXT.replace("page_two",""),
])
def test_rejects_untrusted_continuation_without_fetching_it(provider, bad_next):
    replies, requests = provider
    replies.append(Response(page([row(7300)], next_url=bad_next)))
    with pytest.raises(oi.OpenInterestUnavailable, match="SPX contract OI unavailable"):
        read()
    assert len(requests)==1 and oi._cache is None


@pytest.mark.parametrize("last", [
    Response({}, 302), Response({}, 429), Response(page([])),
    Response(page([row(7300)])), Response(page([row(7500)], next_url=NEXT)),
    Response(page([row(6990)])), Response(page([row(7350, count=-1)])),
    Response(page([row(7350, count=True)])), Response({"status":"ERROR","results":[row(7350)]}),
    Response(page([{**row(7360),"details":{**row(7360)["details"],"strike_price":7370}}])),
])
def test_partial_or_invalid_page_never_reused_and_cooldown_limits_retries(provider, last):
    replies, requests = provider
    replies.extend([Response(page([row(7300)], next_url=NEXT)), last])
    with pytest.raises(oi.OpenInterestUnavailable): read()
    with pytest.raises(oi.OpenInterestUnavailable, match="cooling down"): read()
    assert oi._cache is None and len(requests)==2


def test_full_terminal_page_without_continuation_and_page_budget_fail_closed(provider, monkeypatch):
    replies, requests = provider
    replies.append(Response(page([row(7300),row(7350)])))
    with pytest.raises(oi.OpenInterestUnavailable): read()
    monkeypatch.setattr(oi,"_failed_until",0)
    monkeypatch.setattr(oi,"_MAX_PAGES",1)
    replies.append(Response(page([row(7300)],next_url=NEXT)))
    with pytest.raises(oi.OpenInterestUnavailable): read()
    assert oi._cache is None and len(requests)==2


def test_time_limit_cancels_network_and_releases_the_builder(provider, monkeypatch):
    cancelled=[]
    async def stalled(*args):
        try: await asyncio.sleep(2)
        finally: cancelled.append(True)
    monkeypatch.setattr(oi,"_download",stalled)
    monkeypatch.setattr(oi,"_DEADLINE",.02)
    with pytest.raises(oi.OpenInterestUnavailable): read()
    assert cancelled == [True] and not oi._build_lock.locked() and oi._cache is None


def test_no_second_builder_no_historical_backfill_and_no_disabled_cache(provider, monkeypatch):
    replies, requests=provider
    with oi._build_lock, pytest.raises(oi.OpenInterestUnavailable,match="in progress"):
        read()
    with pytest.raises(oi.OpenInterestUnavailable,match="Invalid OI window"):
        oi.read_spx_oi(day=DAY-timedelta(days=1),now=NOW,low=7200,high=7800)
    replies.append(Response(page([row(7300)])))
    read()
    monkeypatch.setattr(oi.settings,"POLYGON_API_KEY","")
    with pytest.raises(oi.OpenInterestUnavailable,match="disabled"): read()
    assert len(requests)==1


def test_old_success_is_not_returned_when_its_scheduled_refresh_fails(provider, monkeypatch):
    replies, calls = provider
    clock = [10.]
    monkeypatch.setattr(oi, "monotonic", lambda: clock[0])
    replies.append(Response(page([row(7300)])))
    read()
    clock[0] += oi._TTL + 1
    replies.append(Response({}, 503))
    with pytest.raises(oi.OpenInterestUnavailable):
        read()
    with pytest.raises(oi.OpenInterestUnavailable, match="cooling down"):
        read()
    clock[0] += oi._COOLDOWN + 1
    replies.append(Response(page([row(7300, count=88)])))
    assert read()[0] == {"SPXW261120P07300000": 88}
    assert len(calls) == 3
