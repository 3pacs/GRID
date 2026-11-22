"""Rank U.S. equity catalyst setups from timestamped research and tape snapshots.

Run from the GRID root: python -m scripts.windfall_watch snapshot.json
See docs/reference/WINDFALL_WATCH.md for the input contract.
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


def timestamp(value):
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError("timestamps must include an offset")
    return result


def visible(item, as_of):
    return timestamp(item["observed_at"]) <= as_of


def source_host(url):
    parsed = urlparse(url)
    if parsed.scheme not in ("https", "http") or not parsed.hostname:
        raise ValueError("evidence requires an HTTP(S) source URL")
    return parsed.hostname


def assess_research(lead, as_of):
    evidence = [item for item in lead.get("evidence", []) if visible(item, as_of)]
    confirmed = False
    rumor = False
    contradicted = False
    for item in evidence:
        host = source_host(item["url"])
        social = host == "reddit.com" or host.endswith(".reddit.com") or host == "redd.it"
        if item["kind"] == "primary" and not social:
            confirmed |= item["finding"] == "confirms_event"
            contradicted |= item["finding"] == "contradicts_event"
        rumor |= social and item["kind"] == "rumor"

    checks = [check for check in lead.get("delivery_checks", []) if visible(check, as_of)]
    for check in checks:
        if check["status"] != "unknown":
            source_host(check["url"])
    negatives = [check for check in checks if check["status"] in ("negative", "blocking")]
    unknowns = [check for check in checks if check["status"] == "unknown"]
    proposal = lead.get("paid_research")
    review = None
    if proposal and visible(proposal, as_of):
        cost = proposal["cost_usd"]
        budget = proposal["budget_usd"]
        within_budget = isfinite(cost) and isfinite(budget) and 0 < cost <= budget
        resolves_blocker = any(
            check["factor"] == proposal["factor"] and check.get("decision_critical") is True
            for check in unknowns
        )
        eligible = (
            confirmed and not contradicted and within_budget and resolves_blocker
            and proposal.get("free_sources_exhausted") is True
            and bool(proposal.get("source")) and bool(proposal.get("decision_change"))
            and not any(check["status"] == "blocking" for check in negatives)
        )
        review = {**proposal, "status": "review_purchase" if eligible else "pass"}
    status = "confirmed" if confirmed else "rumor" if rumor else "unverified"
    if contradicted:
        status = "contradicted"
    return {
        "event_evidence": status,
        "reddit_rumor": rumor,
        "evidence": evidence,
        "delivery_checks": checks,
        "negating_factors": negatives,
        "unknown_factors": unknowns,
        "paid_research": review,
    }


def assess_side(side, plan, tape, as_of, confirmed, blockers):
    if not plan or not visible(plan, as_of):
        return None
    direction = 1 if side == "long" else -1
    trigger, stop, target = (plan[key] for key in ("trigger", "invalidation", "target"))
    if not all(isfinite(value) and value > 0 for value in (trigger, stop, target)):
        raise ValueError("setup levels must be positive finite prices")
    risk = direction * (trigger - stop)
    reward = direction * (target - trigger)
    if min(risk, reward) <= 0:
        raise ValueError(f"invalid {side} level ordering")

    result = {**plan, "planned_reward_risk": round(reward / risk, 2)}
    reasons = list(blockers)
    if not confirmed:
        reasons.append("catalyst_unconfirmed")
    if reward < 2 * risk:
        reasons.append("planned_reward_below_2r")
    if not tape or not visible(tape, as_of):
        reasons.append("tape_missing")
    else:
        sampled_at = timestamp(tape["observed_at"])
        age = as_of - sampled_at
        if sampled_at < timestamp(plan["observed_at"]):
            reasons.append("tape_predates_setup")
        price, volume, spread, relative_return, dollar_volume = (
            tape[key] for key in ("price", "relative_volume", "spread_bps",
                                  "sector_relative_return", "average_daily_dollar_volume")
        )
        if not all(isfinite(value) for value in (price, volume, spread, relative_return, dollar_volume)):
            raise ValueError("tape metrics must be finite")
        if price <= 0 or min(volume, spread, dollar_volume) < 0:
            raise ValueError("tape price must be positive and liquidity metrics nonnegative")
        if age > timedelta(minutes=5) or tape.get("regular_session") is not True:
            reasons.append("stale_or_outside_regular_session")
        if tape.get("halted") is not False:
            reasons.append("halt_or_unknown_halt_status")
        if spread > 30 or dollar_volume < 10_000_000:
            reasons.append("liquidity_below_floor")
        if side == "short" and tape.get("short_available") is not True:
            reasons.append("short_availability_unverified")
        if direction * (price - stop) <= 0:
            reasons.append("invalidation_level_breached")
        elif direction * (price - trigger) < 0:
            reasons.append("trigger_not_reached")
        else:
            risk_now = direction * (price - stop)
            reward_now = direction * (target - price)
            result["current_reward_risk"] = round(reward_now / risk_now, 2)
            if reward_now < 2 * risk_now:
                reasons.append("remaining_reward_below_2r")
        if volume < 1.5:
            reasons.append("relative_volume_below_1_5")
        if direction * relative_return <= 0:
            reasons.append("sector_relative_move_disagrees")
    if not plan.get("thesis") or not plan.get("antithesis"):
        reasons.append("thesis_or_antithesis_missing")
    result["status"] = "triggered" if not reasons else "watch"
    result["reasons"] = reasons
    return result


def scan(snapshot, as_of, horizon_days):
    today = market_date(as_of)
    last_day = today + timedelta(days=horizon_days)
    tape_by_ticker = {}
    for tape in snapshot.get("tape", []):
        if not visible(tape, as_of):
            continue
        ticker = tape["ticker"].upper()
        current = tape_by_ticker.get(ticker)
        if current is None or timestamp(tape["observed_at"]) > timestamp(current["observed_at"]):
            tape_by_ticker[ticker] = tape

    latest = {}
    for lead in snapshot["leads"]:
        if not visible(lead, as_of):
            continue
        key = (lead["ticker"].upper(), lead["event_id"])
        current = latest.get(key)
        if current is None or timestamp(lead["observed_at"]) > timestamp(current["observed_at"]):
            latest[key] = lead

    results = []
    for (ticker, event_id), lead in latest.items():
        event_day = date.fromisoformat(lead["event_date"])
        if not today <= event_day <= last_day or lead.get("cancelled") is True:
            continue
        research = assess_research(lead, as_of)
        sides = {}
        for side in ("long", "short"):
            checks = [
                check for check in research["delivery_checks"]
                if check.get("side", "long") in (side, "both")
            ]
            blockers = [
                check["factor"] for check in checks
                if check["status"] == "blocking"
                or (check["status"] == "unknown" and check.get("decision_critical") is True)
            ]
            if not any(check["status"] == "supporting" for check in checks):
                blockers.append("delivery_evidence_missing")
            setup = assess_side(side, lead.get(side), tape_by_ticker.get(ticker), as_of,
                                research["event_evidence"] == "confirmed", blockers)
            if setup is not None:
                sides[side] = setup
        if sum(setup["status"] == "triggered" for setup in sides.values()) > 1:
            for setup in sides.values():
                setup["status"] = "watch"
                setup["reasons"].append("conflicting_directional_triggers")
        results.append({
            "ticker": ticker, "event_id": event_id, "event": lead["event"],
            "event_date": event_day.isoformat(), **research, "setups": sides,
            "status": next((side for side, setup in sides.items()
                            if setup["status"] == "triggered"), "no_trade"),
        })

    def order(row):
        active = row["status"] != "no_trade"
        reward_risk = row["setups"].get(row["status"], {}).get("current_reward_risk", 0)
        return (not active, -reward_risk, row["event_evidence"] != "confirmed",
                row["event_date"], row["ticker"], row["event_id"])

    return sorted(results, key=order)


def market_date(as_of):
    return as_of.astimezone(ZoneInfo("America/New_York")).date()


def read_calendar(engine, as_of, horizon_days):
    from intelligence.catalyst_aggregator import (
        CATALYST_CLINICAL,
        CATALYST_EARNINGS,
        events_for_window,
    )

    start = market_date(as_of)
    events = events_for_window(
        engine, start=start, end=start + timedelta(days=horizon_days),
        event_types=[CATALYST_EARNINGS, CATALYST_CLINICAL],
    )
    return [
        {"ticker": event.ticker, "event": event.event_type,
         "event_date": event.event_date.isoformat(), "source": event.source,
         "notes": event.notes, "status": "verify_event"}
        for event in events if event.ticker
    ]


_SUBREDDIT = re.compile(r"[A-Za-z0-9_]{2,21}")
_CASHTAG = re.compile(r"(?<![\w$])\$([A-Z]{1,5}(?:[.-][A-Z])?)(?![\w])")
_CATALYST_TERMS = re.compile(
    r"\b(earnings|guidance|approval|decision|readout|contract|award|"
    r"buyout|merger|launch|dilution|offering|delay|cancellation)\b", re.IGNORECASE,
)


def discover_reddit(client, subreddit, observed_at):
    if not _SUBREDDIT.fullmatch(subreddit):
        raise ValueError(f"invalid subreddit: {subreddit}")
    listing = client.get_json(f"/r/{subreddit}/new", {"limit": 50})
    leads = []
    for child in listing["data"]["children"]:
        post = child["data"]
        permalink = post.get("permalink", "")
        if not permalink.startswith("/r/"):
            continue
        created_at = datetime.fromtimestamp(post["created_utc"], timezone.utc)
        if not observed_at - timedelta(days=3) <= created_at <= observed_at:
            continue
        title = post.get("title", "")
        content = f'{title} {post.get("selftext", "")}'
        tickers = sorted(set(_CASHTAG.findall(content)))
        terms = sorted({term.lower() for term in _CATALYST_TERMS.findall(content)})
        if tickers and terms:
            leads.append({
                "tickers": tickers, "terms": terms, "title": title,
                "url": "https://www.reddit.com" + permalink,
                "observed_at": observed_at.isoformat(), "status": "unverified_lead",
            })
    return leads


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path, nargs="?")
    parser.add_argument("--grid-calendar", action="store_true", help="Read GRID event leads")
    parser.add_argument("--reddit", action="append", metavar="SUBREDDIT",
                        help="Read recent posts via the configured Reddit Data API")
    parser.add_argument("--as-of", type=timestamp, help="Timezone-aware point-in-time cutoff")
    parser.add_argument("--days", type=int, default=10, choices=range(1, 31),
                        metavar="1-30", help="Forward calendar days (default: 10)")
    args = parser.parse_args()
    if not (args.snapshot or args.grid_calendar or args.reddit):
        parser.error("provide a snapshot, --grid-calendar, or --reddit")
    if args.as_of and (args.grid_calendar or args.reddit):
        parser.error("point-in-time replay requires a saved snapshot")
    as_of = args.as_of or datetime.now(timezone.utc)
    try:
        snapshot = json.loads(args.snapshot.read_text()) if args.snapshot else {"leads": []}
        output = {
            "as_of": as_of.isoformat(), "horizon_days": args.days,
            "candidates": scan(snapshot, as_of, args.days),
        }
        if args.grid_calendar:
            from db import get_engine

            output["calendar_leads"] = read_calendar(get_engine(), as_of, args.days)
        if args.reddit:
            from ingestion.altdata.reddit_oauth import RedditAppClient

            client = RedditAppClient()
            unique = {}
            for subreddit in dict.fromkeys(args.reddit):
                for lead in discover_reddit(client, subreddit, datetime.now(timezone.utc)):
                    unique[lead["url"]] = lead
            output["reddit_leads"] = list(unique.values())
        print(json.dumps(output, indent=2, allow_nan=False))
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as exc:
        parser.exit(2, f"windfall watch: {exc}\n")


if __name__ == "__main__":
    main()
