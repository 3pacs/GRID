from copy import deepcopy
from datetime import datetime, timezone

from scripts.windfall_watch import discover_reddit, scan

AS_OF = datetime(2026, 11, 23, 15, tzinfo=timezone.utc)
OBSERVED = "2026-11-23T14:50:00Z"


def snapshot():
    return {
        "leads": [{
            "ticker": "EXMP", "event_id": "results", "event": "Earnings",
            "observed_at": OBSERVED, "event_date": "2026-11-24",
            "evidence": [{
                "kind": "primary", "finding": "confirms_event",
                "url": "https://example.com/results", "observed_at": OBSERVED,
            }],
            "delivery_checks": [{
                "factor": "capacity", "status": "supporting", "side": "both",
                "url": "https://example.com/capacity", "observed_at": OBSERVED,
            }],
            "long": {"trigger": 50, "invalidation": 49, "target": 54,
                     "thesis": "upside", "antithesis": "weak peers", "observed_at": OBSERVED},
            "short": {"trigger": 47, "invalidation": 48, "target": 43,
                      "thesis": "downside", "antithesis": "reclaim", "observed_at": OBSERVED},
        }],
        "tape": [{
            "ticker": "EXMP", "observed_at": "2026-11-23T14:59:00Z",
            "price": 50.1, "relative_volume": 1.8, "spread_bps": 10,
            "sector_relative_return": 0.012, "average_daily_dollar_volume": 18_000_000,
            "regular_session": True, "halted": False, "short_available": True,
        }],
    }


def test_direction_changes_and_short_requires_availability():
    data = snapshot()
    assert scan(data, AS_OF, 10)[0]["status"] == "long"
    data["tape"][0].update(price=46.9, sector_relative_return=-0.012)
    assert scan(data, AS_OF, 10)[0]["status"] == "short"
    data["tape"][0].pop("short_available")
    row = scan(data, AS_OF, 10)[0]
    assert row["status"] == "no_trade"
    assert "short_availability_unverified" in row["setups"]["short"]["reasons"]


def test_social_cannot_confirm_and_future_evidence_cannot_leak():
    data = snapshot()
    primary = data["leads"][0]["evidence"][0]
    future = {**primary, "observed_at": "2026-11-23T15:01:00Z"}
    primary["url"] = "https://www.reddit.com/r/stocks/example"
    data["leads"][0]["evidence"] += [{**primary, "kind": "rumor"}, future]
    row = scan(data, AS_OF, 10)[0]
    assert row["event_evidence"] == "rumor"
    assert row["status"] == "no_trade"
    assert len(row["evidence"]) == 2


def test_no_chasing_or_stale_tape():
    data = snapshot()
    data["tape"][0].update(price=53, observed_at=OBSERVED)
    row = scan(data, AS_OF, 10)[0]
    assert row["status"] == "no_trade"
    reasons = row["setups"]["long"]["reasons"]
    assert "remaining_reward_below_2r" in reasons
    assert "stale_or_outside_regular_session" in reasons


def test_paid_research_and_side_specific_blockers():
    data = snapshot()
    lead = data["leads"][0]
    lead["delivery_checks"].append({
        "factor": "consensus", "status": "unknown", "decision_critical": True,
        "side": "long", "observed_at": OBSERVED,
    })
    lead["paid_research"] = {
        "observed_at": OBSERVED, "cost_usd": 25, "budget_usd": 30, "factor": "consensus",
        "source": "Snapshot vendor", "decision_change": "Drop if priced in",
        "free_sources_exhausted": True,
    }
    row = scan(data, AS_OF, 10)[0]
    assert row["status"] == "no_trade"
    assert row["paid_research"]["status"] == "review_purchase"
    lead["delivery_checks"].append({
        "factor": "funding", "status": "blocking", "side": "long",
        "url": "https://example.com/filing", "observed_at": OBSERVED,
    })
    row = scan(data, AS_OF, 10)[0]
    assert row["paid_research"]["status"] == "pass"
    assert row["negating_factors"][0]["factor"] == "funding"
    data["tape"][0].update(price=46.9, sector_relative_return=-0.012)
    assert scan(data, AS_OF, 10)[0]["status"] == "short"


def test_cancellation_and_contradiction():
    data = snapshot()
    lead = data["leads"][0]
    lead["evidence"].append({**lead["evidence"][0], "finding": "contradicts_event"})
    row = scan(data, AS_OF, 10)[0]
    assert row["event_evidence"] == "contradicted"
    assert row["status"] == "no_trade"
    cancelled = deepcopy(lead)
    cancelled.update(cancelled=True, observed_at="2026-11-23T14:58:00Z")
    assert scan({"leads": [cancelled, lead]}, AS_OF, 10) == []


def test_reddit_returns_only_recent_catalyst_cashtag_leads():
    class Client:
        def get_json(self, path, params):
            assert path == "/r/stocks/new"
            assert params == {"limit": 50}
            post = {"title": "$EXMP approval rumor", "permalink": "/r/stocks/comments/id/",
                    "created_utc": AS_OF.timestamp() - 3600}
            return {"data": {"children": [
                {"data": post},
                {"data": {**post, "title": "$EXMP to the moon"}},
                {"data": {**post, "created_utc": AS_OF.timestamp() - 4 * 86400}},
            ]}}

    leads = discover_reddit(Client(), "stocks", AS_OF)
    assert len(leads) == 1
    assert leads[0]["tickers"] == ["EXMP"]
    assert leads[0]["status"] == "unverified_lead"
