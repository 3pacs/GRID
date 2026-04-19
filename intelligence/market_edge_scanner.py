from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from statistics import median
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from analysis.market_universe import get_all_tickers, search_company
from analysis.sector_map import get_actor_influence, get_all_sectors

SOURCE_WINDOWS_DAYS: dict[str, int] = {
    "gov_contract": 540,
    "legislative": 540,
    "export_control": 540,
    "congressional": 120,
    "options_flow": 45,
}

SOURCE_LABELS: dict[str, str] = {
    "gov_contract": "Gov Contracts",
    "legislative": "Legislation",
    "export_control": "Export Controls",
    "congressional": "Congressional Trades",
    "options_flow": "Options Flow",
    "influence_loop": "Influence Loops",
}

SOURCE_PRIORITY = {
    "gov_contract": 0,
    "legislative": 1,
    "export_control": 2,
    "options_flow": 3,
    "influence_loop": 4,
    "congressional": 5,
}

EXCLUDED_TICKERS = {
    "QQQ",
    "IWM",
    "GLD",
    "SLV",
    "USO",
    "XLE",
    "XLK",
    "XLF",
    "XLV",
    "XHB",
    "ITA",
    "PHO",
    "DBA",
    "ICLN",
    "TAN",
    "XTN",
    "EEM",
    "FXI",
    "EWJ",
    "HYG",
    "SMH",
    "IGV",
    "IHI",
}

MANUAL_TICKER_META: dict[str, dict[str, str]] = {
    "HRB": {
        "company": "H&R Block",
        "sector": "Financials",
        "subsector": "Tax Preparation",
    }
}


@dataclass(frozen=True)
class PlaybookBlueprint:
    id: str
    title: str
    category: str
    sector_focus: str
    summary_stub: str
    thesis_stub: str
    mispricing_test: str
    proof_needed: str
    kill_switch: str
    route_hint: str
    target_pool: tuple[str, ...]
    primary_sources: tuple[str, ...]
    supporting_sources: tuple[str, ...]
    clue_prompts: tuple[str, ...]
    base_confidence: int
    base_edge: int
    setup_type: str
    horizon: str
    bias: str = "long"


@dataclass
class TickerSignalProfile:
    ticker: str
    company: str
    sector: str
    subsector: str
    source_counts: Counter[str] = field(default_factory=Counter)
    source_dates: dict[str, list[date]] = field(default_factory=dict)
    legislative_items: list[dict[str, Any]] = field(default_factory=list)
    gov_contract_items: list[dict[str, Any]] = field(default_factory=list)
    export_control_items: list[dict[str, Any]] = field(default_factory=list)
    congressional_items: list[dict[str, Any]] = field(default_factory=list)
    options_count: int = 0
    options_calls: int = 0
    options_puts: int = 0
    options_notional: float = 0.0
    influence_contracts_received: float = 0.0
    influence_suspicion: float = 0.0
    influence_updated_at: datetime | None = None
    last_signal_date: date | None = None


PLAYBOOKS: tuple[PlaybookBlueprint, ...] = (
    PlaybookBlueprint(
        id="defense-procurement-stack",
        title="Defense procurement is clustering in the contractor stack",
        category="Defense",
        sector_focus="Defense primes and mission IT",
        summary_stub="Awards, appropriation language, and contract-receipt loops are all pointing at the same prime contractors.",
        thesis_stub="Defense is being priced like background budget chatter even though the live award tape is concentrating in a short list of primes and mission IT operators.",
        mispricing_test="The market is still treating defense as a macro sector call when the actual spend is landing in a handful of named contractors.",
        proof_needed="More delivery orders, bridge awards, or procurement language hitting the same contractor stack over the next 30 to 60 days.",
        kill_switch="Kill it if the award tape goes cold, the policy language stops mapping to procurement, or the flow widens into sector baskets instead of named contractors.",
        route_hint="influence",
        target_pool=("GD", "LDOS", "BAH", "CACI", "SAIC", "LMT", "RTX", "NOC"),
        primary_sources=("gov_contract", "legislative", "influence_loop"),
        supporting_sources=("options_flow",),
        clue_prompts=(
            "Track follow-on task orders and bridge awards in the same agencies.",
            "Watch procurement language around Title 10, force readiness, and mission systems.",
            "Keep the basket tight to primes and mission IT names, not defense ETFs.",
        ),
        base_confidence=74,
        base_edge=18,
        setup_type="procurement-compound",
        horizon="1-3 months",
    ),
    PlaybookBlueprint(
        id="gov-it-modernization",
        title="Federal modernization spend is landing in real infrastructure names",
        category="Gov IT",
        sector_focus="Federal software, licensing, and systems modernization",
        summary_stub="Fresh federal awards are landing in the same enterprise infrastructure vendors instead of diffusing across broad tech.",
        thesis_stub="The market is lumping public-sector modernization into generic tech spend even though award flow is isolating the winning vendors.",
        mispricing_test="If the spend is real, federal licensing and systems awards should keep clustering in named vendors rather than broad software indices.",
        proof_needed="Additional modernization, lifecycle management, or license renewal awards for the same vendors this quarter.",
        kill_switch="Kill it if modernization awards stop printing or the wins rotate into an entirely different vendor set.",
        route_hint="timeline",
        target_pool=("DELL", "IBM"),
        primary_sources=("gov_contract", "influence_loop"),
        supporting_sources=("legislative",),
        clue_prompts=(
            "Monitor HHS, GSA, and agency bridge programs for repeat vendor wins.",
            "Separate software licensing and systems integration spend from generic AI chatter.",
            "Require named-award confirmation before promoting any adjacent vendor.",
        ),
        base_confidence=70,
        base_edge=16,
        setup_type="gov-it-refresh",
        horizon="1-3 months",
    ),
    PlaybookBlueprint(
        id="semis-export-regime",
        title="Export-control tightening is funneling into the compute chain",
        category="Semis",
        sector_focus="Advanced semis and export-gated compute",
        summary_stub="Export-control language and options activity are narrowing the semi trade down to actual compute names.",
        thesis_stub="The market keeps talking about semis as one blob, but the real leverage is in the names most exposed to advanced-compute licensing and spillover demand.",
        mispricing_test="If export policy is the real driver, the move should stay concentrated in advanced-compute names instead of bleeding evenly across the whole sector.",
        proof_needed="Additional rule tightening, licensing guidance, or follow-through call flow in the same compute cluster.",
        kill_switch="Kill it if the policy narrative fades, call flow reverses, or the trade turns back into a generic sector ETF move.",
        route_hint="options",
        target_pool=("NVDA", "TSM", "AMD"),
        primary_sources=("export_control",),
        supporting_sources=("options_flow",),
        clue_prompts=(
            "Watch BIS language, licensing exceptions, and China/Macau references.",
            "Demand ticker-level call skew before surfacing peers.",
            "Treat ETF options activity as noise unless the named compute chain confirms it.",
        ),
        base_confidence=71,
        base_edge=17,
        setup_type="policy-supply-gate",
        horizon="2-8 weeks",
    ),
    PlaybookBlueprint(
        id="homebuilder-policy-ladder",
        title="Housing policy is pointing at the builders, not the tape",
        category="Housing",
        sector_focus="Homebuilders and housing affordability",
        summary_stub="Fresh housing bills are clustering in the public builders instead of the usual macro housing proxies.",
        thesis_stub="Housing is being treated as a rates-only trade while the policy tape is concentrating on a specific builder group.",
        mispricing_test="If legislation matters here, the signal should keep landing in builders and suppliers rather than only in macro rates trades.",
        proof_needed="More committee movement, sponsorship, or companion bills tied to affordability and manufactured housing.",
        kill_switch="Kill it if housing legislation stalls or the signal stops selecting builder names.",
        route_hint="catalyst-timeline",
        target_pool=("DHI", "LEN", "TOL"),
        primary_sources=("legislative",),
        supporting_sources=(),
        clue_prompts=(
            "Track affordability, loan modernization, and manufactured housing bills.",
            "Promote only the builders repeatedly named by the policy map.",
            "Ignore broad housing baskets unless the builders confirm first.",
        ),
        base_confidence=64,
        base_edge=14,
        setup_type="policy-demand-release",
        horizon="1-3 months",
    ),
    PlaybookBlueprint(
        id="healthcare-policy-pressure",
        title="Healthcare policy pressure is concentrating in payors and pharma",
        category="Health",
        sector_focus="Managed care and big pharma",
        summary_stub="Healthcare legislation is repeatedly landing on the same payor and pharma names.",
        thesis_stub="The market treats healthcare policy as diffuse headline risk, but the bill flow keeps selecting the same small set of incumbents.",
        mispricing_test="If the policy pressure is real, the same payors and pharma names should keep taking the signal instead of the whole healthcare complex.",
        proof_needed="More bill progression or hearing activity tied to fraud, reimbursement, or patient-access language.",
        kill_switch="Kill it if the bills die quietly or the theme broadens into generic healthcare ETF noise.",
        route_hint="catalyst-timeline",
        target_pool=("UNH", "PFE", "JNJ"),
        primary_sources=("legislative",),
        supporting_sources=(),
        clue_prompts=(
            "Track fraud, reimbursement, and patient-access bill movement.",
            "Keep payors separated from drug manufacturers in the read-through.",
            "Demand repeated name-level hits before escalating confidence.",
        ),
        base_confidence=61,
        base_edge=12,
        setup_type="policy-pressure",
        horizon="1-3 months",
    ),
    PlaybookBlueprint(
        id="oil-policy-optionality",
        title="Energy policy is selecting integrated oil names",
        category="Energy",
        sector_focus="Integrated oil and upstream optionality",
        summary_stub="Energy legislation is printing into actual integrated oil names rather than broad commodity proxies.",
        thesis_stub="The market keeps flattening energy into crude beta while the policy tape is pointing at a short list of producers.",
        mispricing_test="If the legislative path matters, the move should stay concentrated in integrated producers and selected upstream optionality.",
        proof_needed="Additional land, security, or spill-recovery policy movement with the same oil names attached.",
        kill_switch="Kill it if policy momentum stalls or the signal dissolves back into crude proxies and sector ETFs.",
        route_hint="flows",
        target_pool=("XOM", "CVX", "OXY"),
        primary_sources=("legislative",),
        supporting_sources=(),
        clue_prompts=(
            "Track land-exchange, energy-security, and permitting language.",
            "Stay with named producers instead of crude proxies.",
            "Look for company-specific confirmation before broadening the basket.",
        ),
        base_confidence=60,
        base_edge=13,
        setup_type="policy-optionality",
        horizon="1-3 months",
    ),
    PlaybookBlueprint(
        id="nuclear-fuel-policy",
        title="Nuclear policy is surfacing the fuel suppliers",
        category="Nuclear",
        sector_focus="Nuclear fuel and uranium",
        summary_stub="Nuclear legislation is isolating the fuel suppliers instead of generic energy exposure.",
        thesis_stub="The market prices nuclear as an abstract long-cycle theme while the policy signal keeps narrowing down to fuel names.",
        mispricing_test="If this is a real longer-dated edge, policy movement should keep selecting uranium and fuel suppliers rather than the whole energy stack.",
        proof_needed="More bill movement tied to nuclear communities, domestic fuel security, or reactor supply-chain support.",
        kill_switch="Kill it if the bills stop moving or the signal widens back into generic energy baskets.",
        route_hint="signals",
        target_pool=("CCJ", "UEC"),
        primary_sources=("legislative",),
        supporting_sources=(),
        clue_prompts=(
            "Track domestic nuclear fuel, recovery, and reactor support language.",
            "Keep the focus on fuel suppliers before looking at second-order names.",
            "Require policy follow-through because this theme reprices slowly.",
        ),
        base_confidence=59,
        base_edge=15,
        setup_type="policy-scarcity",
        horizon="2-6 months",
    ),
    PlaybookBlueprint(
        id="tax-admin-software",
        title="Tax-policy churn is landing in the filing and compliance rails",
        category="Tax",
        sector_focus="Tax preparation and compliance rails",
        summary_stub="Tax-linked legislation is repeatedly pointing at the actual filing rails instead of the whole consumer-finance stack.",
        thesis_stub="Tax-policy noise usually gets ignored, but repeated bill flow into the same filing rails can still create a durable edge.",
        mispricing_test="If the policy path matters, tax-prep and compliance names should keep taking the signal before the market prices the knock-on revenue opportunity.",
        proof_needed="Fresh committee movement or new tax-administration language tied to the same preparation stack.",
        kill_switch="Kill it if the tax bills stall or the signal never graduates beyond one-off mentions.",
        route_hint="timeline",
        target_pool=("INTU", "HRB"),
        primary_sources=("legislative",),
        supporting_sources=(),
        clue_prompts=(
            "Track tax administration, taxpayer protection, and filing simplification bills.",
            "Stay with the filing rails, not generic fintech.",
            "Wait for repeated company-level matches before increasing size.",
        ),
        base_confidence=57,
        base_edge=12,
        setup_type="policy-plumbing",
        horizon="1-3 months",
    ),
    PlaybookBlueprint(
        id="banking-modernization",
        title="Banking modernization is showing up in real financial names",
        category="Banks",
        sector_focus="Money center banks and capital-markets franchises",
        summary_stub="Financial legislation and options activity are clustering in actual banking franchises instead of broad financial baskets.",
        thesis_stub="The market sees financial reform as a vague macro theme while the clue chain is already selecting the banks and brokers that benefit.",
        mispricing_test="If modernization is real, bank-specific flow should confirm the legislative signal instead of staying trapped in XLF-style basket action.",
        proof_needed="More legislative progress or repeat call-heavy options flow in the same bank cluster.",
        kill_switch="Kill it if call flow fades, the bills die, or the action falls back into broad financial ETFs.",
        route_hint="options",
        target_pool=("GS", "JPM", "BAC"),
        primary_sources=("legislative",),
        supporting_sources=("options_flow",),
        clue_prompts=(
            "Track merchant banking, capital formation, and supervisory reform language.",
            "Require named-bank flow confirmation before upgrading confidence.",
            "Ignore basket signals unless the large banks move first.",
        ),
        base_confidence=63,
        base_edge=13,
        setup_type="policy-plus-flow",
        horizon="2-8 weeks",
    ),
    PlaybookBlueprint(
        id="aviation-certification-cycle",
        title="Aviation rule changes are selecting OEM and airline names",
        category="Aviation",
        sector_focus="Aerospace OEM and airline certification cycle",
        summary_stub="Aviation legislation is pointing at the names closest to certification and fleet impact instead of broad transport baskets.",
        thesis_stub="The market treats aviation policy as political noise even when the certification path can create company-specific repricing.",
        mispricing_test="If the certification cycle matters, the move should stay concentrated in Boeing and the airlines most exposed to the rule path.",
        proof_needed="Additional FAA, certification, or competitiveness language with the same OEM and airline cluster attached.",
        kill_switch="Kill it if the legislation stalls or the read-through never tightens to named carriers and the OEM.",
        route_hint="catalyst-timeline",
        target_pool=("BA", "DAL", "UAL", "LUV"),
        primary_sources=("legislative",),
        supporting_sources=("gov_contract",),
        clue_prompts=(
            "Track FAA certification and aviation competitiveness bills.",
            "Keep Boeing separate from airline read-through when the signal splits.",
            "Avoid broad transport ETFs unless the carriers confirm first.",
        ),
        base_confidence=58,
        base_edge=11,
        setup_type="rule-cycle",
        horizon="1-3 months",
    ),
)


def _normalize_ticker(value: str | None) -> str | None:
    if not value:
        return None
    ticker = value.strip().upper().replace("-", ".")
    return ticker or None


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (float, int)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _shorten(text_value: str, limit: int = 72) -> str:
    compact = " ".join(text_value.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "…"


def _format_money(value: float) -> str:
    amount = abs(value)
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.0f}K"
    return f"${amount:.0f}"


def _format_date(value: date | datetime | None) -> str:
    if value is None:
        return "recently"
    if isinstance(value, datetime):
        value = value.date()
    return value.strftime("%b %-d")


def _dedupe(items: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        cleaned = item.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
        if len(result) >= limit:
            break
    return result


def _confidence_label(value: int) -> str:
    if value >= 78:
        return "high"
    if value >= 62:
        return "medium"
    return "low"


def _status_from_confidence(value: int) -> str:
    if value >= 78:
        return "active"
    if value >= 64:
        return "arming"
    if value >= 50:
        return "watch"
    return "background"


def _source_label(source_type: str) -> str:
    return SOURCE_LABELS.get(source_type, source_type.replace("_", " ").title())


def _ordered_source_types(source_types: set[str]) -> list[str]:
    return sorted(source_types, key=lambda item: SOURCE_PRIORITY.get(item, 99))


def _build_company_lookup() -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    market_universe_tickers = set(get_all_tickers())

    for ticker in TARGET_UNIVERSE:
        if ticker in EXCLUDED_TICKERS or ticker not in market_universe_tickers:
            continue
        matches = [item for item in search_company(ticker) if _normalize_ticker(item.get("ticker")) == ticker]
        if not matches:
            continue
        match = matches[0]
        lookup[ticker] = {
            "ticker": ticker,
            "company": match.get("name") or ticker,
            "sector": match.get("sector") or "Other",
            "subsector": match.get("industry") or "Other",
            "influence": 1.0,
        }

    for sector in get_all_sectors():
        for actor in get_actor_influence(sector):
            if actor.get("type") != "company":
                continue
            ticker = _normalize_ticker(actor.get("ticker"))
            if not ticker or ticker in EXCLUDED_TICKERS or ticker in lookup:
                continue
            candidate = {
                "ticker": ticker,
                "company": actor.get("name") or ticker,
                "sector": sector,
                "subsector": actor.get("subsector") or "Other",
                "influence": _to_float(actor.get("influence")),
            }
            current = lookup.get(ticker)
            if current is None or candidate["influence"] > current["influence"]:
                lookup[ticker] = candidate
    for ticker, meta in MANUAL_TICKER_META.items():
        lookup[ticker] = {
            "ticker": ticker,
            "company": meta["company"],
            "sector": meta["sector"],
            "subsector": meta["subsector"],
            "influence": 0.0,
        }
    return lookup


def _target_universe() -> list[str]:
    targets = {
        normalized
        for playbook in PLAYBOOKS
        for ticker in playbook.target_pool
        if (normalized := _normalize_ticker(ticker))
    }
    return sorted(targets)


TARGET_UNIVERSE = _target_universe()
COMPANY_LOOKUP = _build_company_lookup()


def _get_profile(ticker: str) -> TickerSignalProfile | None:
    meta = COMPANY_LOOKUP.get(ticker)
    if meta is None:
        return None
    return TickerSignalProfile(
        ticker=ticker,
        company=meta["company"],
        sector=meta["sector"],
        subsector=meta["subsector"],
    )


def _append_signal(profile: TickerSignalProfile, source_type: str, signal_date: date | None) -> None:
    profile.source_counts[source_type] += 1
    if signal_date and (profile.last_signal_date is None or signal_date > profile.last_signal_date):
        profile.last_signal_date = signal_date
        profile.source_dates.setdefault(source_type, []).append(signal_date)
    elif signal_date:
        profile.source_dates.setdefault(source_type, []).append(signal_date)


def _profile_strength(profile: TickerSignalProfile) -> float:
    contract_bonus = min(22.0, math.log10(profile.influence_contracts_received + 1.0) * 2.8)
    contract_tape = min(
        16.0,
        profile.source_counts["gov_contract"] * 2.2
        + math.log10(sum(item.get("amount", 0.0) for item in profile.gov_contract_items) + 1.0) * 1.8,
    )
    legislative_bonus = min(14.0, profile.source_counts["legislative"] * 1.4)
    export_bonus = min(
        18.0,
        profile.source_counts["export_control"] * 6.0
        + max((item.get("severity") or 0) for item in profile.export_control_items or [{"severity": 0}]),
    )
    option_skew = profile.options_calls - profile.options_puts
    options_bonus = 0.0
    if profile.options_count:
        options_bonus = min(
            14.0,
            profile.options_count * 0.18
            + math.log10(profile.options_notional + 1.0) * 1.3
            + max(option_skew, 0) * 0.12,
        )
    influence_bonus = min(8.0, profile.influence_suspicion * 30.0)
    return round(contract_bonus + contract_tape + legislative_bonus + export_bonus + options_bonus + influence_bonus, 2)


def _format_legislative_items(profile: TickerSignalProfile, limit: int = 2) -> list[str]:
    lines: list[str] = []
    for item in profile.legislative_items[:limit]:
        topics = item.get("topics") or []
        topic_text = f" [{', '.join(topics[:2])}]" if topics else ""
        status = item.get("status") or "active"
        lines.append(
            f"{profile.ticker} matched {_shorten(item.get('title') or 'legislative activity', 74)} "
            f"({status}{topic_text}) on {_format_date(item.get('date'))}"
        )
    return lines


def _format_gov_contract_items(profile: TickerSignalProfile, limit: int = 2) -> list[str]:
    lines: list[str] = []
    for item in profile.gov_contract_items[:limit]:
        amount = _format_money(_to_float(item.get("amount")))
        description = _shorten(item.get("description") or "federal award", 76)
        lines.append(f"{profile.ticker} won {amount} for {description} on {_format_date(item.get('date'))}")
    return lines


def _format_export_items(profile: TickerSignalProfile, limit: int = 1) -> list[str]:
    lines: list[str] = []
    for item in profile.export_control_items[:limit]:
        countries = item.get("countries") or []
        country_text = f" ({', '.join(countries[:2])})" if countries else ""
        severity = item.get("severity")
        sev_text = f", severity {severity}" if severity else ""
        lines.append(
            f"{profile.ticker} tied to {_shorten(item.get('title') or 'export-control action', 76)}"
            f"{country_text}{sev_text} on {_format_date(item.get('date'))}"
        )
    return lines


def _format_options_item(profile: TickerSignalProfile) -> list[str]:
    if not profile.options_count:
        return []
    skew = "calls" if profile.options_calls >= profile.options_puts else "puts"
    share = 0
    if profile.options_count:
        dominant = max(profile.options_calls, profile.options_puts)
        share = round((dominant / profile.options_count) * 100)
    return [
        f"{profile.ticker} printed {profile.options_count} unusual options signals with "
        f"{share}% {skew} and {_format_money(profile.options_notional)} notional"
    ]


def _format_influence_item(profile: TickerSignalProfile) -> list[str]:
    if profile.influence_contracts_received <= 0:
        return []
    return [
        f"{profile.ticker} already shows {_format_money(profile.influence_contracts_received)} in contract-receipt loop data"
    ]


def _supporting_source_types(profiles: list[TickerSignalProfile]) -> list[str]:
    source_types = {
        source_type
        for profile in profiles
        for source_type, count in profile.source_counts.items()
        if count
    }
    if any(profile.influence_contracts_received > 0 for profile in profiles):
        source_types.add("influence_loop")
    return [_source_label(source) for source in _ordered_source_types(source_types)]


def _distinct_dates_for_sources(
    profiles: list[TickerSignalProfile],
    source_types: tuple[str, ...] | list[str],
) -> list[date]:
    dates: set[date] = set()
    for profile in profiles:
        for source_type in source_types:
            for signal_date in profile.source_dates.get(source_type, []):
                dates.add(signal_date)
        if "influence_loop" in source_types and profile.influence_updated_at is not None:
            dates.add(profile.influence_updated_at.date())
    return sorted(dates)


def _last_seen_for_source(profiles: list[TickerSignalProfile], source_type: str) -> date | None:
    dates = _distinct_dates_for_sources(profiles, [source_type])
    return dates[-1] if dates else None


def _infer_cadence_days(
    profiles: list[TickerSignalProfile],
    source_types: tuple[str, ...] | list[str],
) -> int | None:
    dates = _distinct_dates_for_sources(profiles, source_types)
    if len(dates) < 2:
        return None

    intervals = [
        (dates[idx] - dates[idx - 1]).days
        for idx in range(1, len(dates))
        if (dates[idx] - dates[idx - 1]).days > 0
    ]
    if not intervals:
        return None

    return max(5, min(int(round(median(intervals[-5:]))), 90))


def _format_short_date(value: date | None) -> str:
    if value is None:
        return "n/a"
    return value.isoformat()


def _window_status(as_of: date, confirm_by: date | None, negate_by: date | None) -> str:
    if confirm_by is None or negate_by is None:
        return "unframed"
    if as_of <= confirm_by:
        return "fresh"
    if as_of <= negate_by:
        return "due"
    return "late"


def _row_status_for_source(
    as_of: date,
    count: int,
    last_seen: date | None,
    cadence_days: int | None,
) -> str:
    if count <= 0 or last_seen is None:
        return "missing"
    if cadence_days is None:
        return "confirmed"
    age_days = max((as_of - last_seen).days, 0)
    if age_days > cadence_days * 2:
        return "late"
    if age_days > cadence_days:
        return "due"
    return "confirmed"


def _collect_evidence(
    playbook: PlaybookBlueprint,
    profiles: list[TickerSignalProfile],
) -> list[str]:
    lines: list[str] = []
    ranked = sorted(profiles, key=_profile_strength, reverse=True)
    preferred_sources = playbook.primary_sources + playbook.supporting_sources + ("influence_loop",)

    for source_type in preferred_sources:
        for profile in ranked:
            if source_type == "gov_contract":
                lines.extend(_format_gov_contract_items(profile, limit=1))
            elif source_type == "legislative":
                lines.extend(_format_legislative_items(profile, limit=1))
            elif source_type == "export_control":
                lines.extend(_format_export_items(profile, limit=1))
            elif source_type == "options_flow":
                lines.extend(_format_options_item(profile))
            elif source_type == "influence_loop":
                lines.extend(_format_influence_item(profile))
            elif source_type == "congressional":
                if profile.congressional_items:
                    item = profile.congressional_items[0]
                    lines.append(
                        f"{profile.ticker} flagged in congressional trade data on {_format_date(item.get('date'))}"
                    )
        if len(_dedupe(lines, limit=6)) >= 6:
            break

    return _dedupe(lines, limit=6)


def _profile_names(profiles: list[TickerSignalProfile], limit: int = 3) -> str:
    ranked = sorted(profiles, key=_profile_strength, reverse=True)[:limit]
    return ", ".join(f"{profile.company} ({profile.ticker})" for profile in ranked)


def _top_targets(profiles: list[TickerSignalProfile], limit: int = 4) -> list[str]:
    ranked = sorted(profiles, key=_profile_strength, reverse=True)
    return [profile.ticker for profile in ranked[:limit]]


def _source_summary(playbook: PlaybookBlueprint, profiles: list[TickerSignalProfile]) -> str:
    live_sources = _supporting_source_types(profiles)
    if live_sources:
        if len(live_sources) == 1:
            return live_sources[0]
        if len(live_sources) == 2:
            return f"{live_sources[0]} and {live_sources[1]}"
        return f"{', '.join(live_sources[:-1])}, and {live_sources[-1]}"
    return "live public signals"


def _why_now(playbook: PlaybookBlueprint, profiles: list[TickerSignalProfile], targets: list[str]) -> str:
    latest_date = max((profile.last_signal_date for profile in profiles if profile.last_signal_date), default=None)
    source_summary = _source_summary(playbook, profiles).lower()
    target_summary = ", ".join(targets[:3])
    if latest_date:
        return (
            f"Fresh {source_summary} printed by {_format_date(latest_date)}, and the signal is still staying inside "
            f"{target_summary} instead of broad sector baskets."
        )
    return f"Fresh {source_summary} is still concentrated in {target_summary} instead of broad sector baskets."


def _entry_rule(playbook: PlaybookBlueprint, profiles: list[TickerSignalProfile], targets: list[str]) -> str:
    source_summary = _source_summary(playbook, profiles).lower()
    return (
        f"Stay interested only while new {source_summary} keeps landing in {', '.join(targets[:3])} "
        f"and the read-through stays company-specific."
    )


def _exit_rule(playbook: PlaybookBlueprint, targets: list[str]) -> str:
    return (
        f"Scale out after the first clean repricing in {', '.join(targets[:2])} or once the signal broadens into basket action."
    )


def _build_clues(playbook: PlaybookBlueprint, profiles: list[TickerSignalProfile]) -> list[str]:
    clues = list(playbook.clue_prompts)
    if any(profile.options_count for profile in profiles):
        clues.append("Look for repeat call-heavy flow in the same names before expanding the watchlist.")
    if any(profile.gov_contract_items for profile in profiles):
        clues.append("Track whether follow-on awards hit the same agencies and recipient names.")
    if any(profile.export_control_items for profile in profiles):
        clues.append("Watch for rule revisions, country list changes, or licensing commentary.")
    return _dedupe(clues, limit=4)


def _build_decision_window(
    playbook: PlaybookBlueprint,
    profiles: list[TickerSignalProfile],
    as_of: date,
) -> dict[str, Any]:
    latest_signal_date = max(
        (profile.last_signal_date for profile in profiles if profile.last_signal_date),
        default=None,
    )
    cadence_days = _infer_cadence_days(
        profiles,
        playbook.primary_sources + playbook.supporting_sources + ("influence_loop",),
    )
    confirm_by = (
        latest_signal_date + timedelta(days=cadence_days)
        if latest_signal_date is not None and cadence_days is not None
        else None
    )
    negate_by = (
        latest_signal_date + timedelta(days=cadence_days * 2)
        if latest_signal_date is not None and cadence_days is not None
        else None
    )
    status = _window_status(as_of, confirm_by, negate_by)

    if cadence_days is None:
        status_note = "Need another live print to frame the cadence."
    elif status == "fresh":
        status_note = f"Expect follow-through within about {cadence_days} days of the last live print."
    elif status == "due":
        status_note = "Confirmation should already be showing up. This edge is now in prove-it mode."
    else:
        status_note = "The confirmation window is stale. Treat silence as negation until a new print lands."

    return {
        "last_signal_date": _format_short_date(latest_signal_date),
        "confirm_by_date": _format_short_date(confirm_by) if confirm_by else None,
        "negate_by_date": _format_short_date(negate_by) if negate_by else None,
        "cadence_days": cadence_days,
        "status": status,
        "status_note": status_note,
    }


def _build_driver_stack(
    playbook: PlaybookBlueprint,
    profiles: list[TickerSignalProfile],
    target_names: str,
    targets: list[str],
    evidence: list[str],
) -> list[dict[str, str]]:
    lead_source = _source_summary(playbook, profiles)
    lead_trigger = evidence[0] if evidence else "Waiting on a fresh named trigger."
    repricing_path = _shorten(playbook.mispricing_test, 108)
    company_focus = target_names or ", ".join(targets[:3])
    return [
        {"label": "Lever", "value": lead_source},
        {"label": "Trigger", "value": _shorten(lead_trigger, 108)},
        {"label": "Names", "value": company_focus},
        {"label": "Repricing", "value": repricing_path},
    ]


def _build_confirmation_board(
    playbook: PlaybookBlueprint,
    profiles: list[TickerSignalProfile],
    targets: list[str],
    as_of: date,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for source_type in playbook.primary_sources + playbook.supporting_sources:
        last_seen = _last_seen_for_source(profiles, source_type)
        cadence_days = _infer_cadence_days(profiles, [source_type])
        if source_type == "influence_loop":
            count = sum(1 for profile in profiles if profile.influence_contracts_received > 0)
            last_seen = max(
                (profile.influence_updated_at.date() for profile in profiles if profile.influence_updated_at),
                default=None,
            )
        else:
            count = sum(profile.source_counts[source_type] for profile in profiles)
        detail = (
            f"{count} prints | last {_format_date(last_seen)}"
            if count and last_seen
            else "No live print in window"
        )
        if cadence_days:
            detail = f"{detail} | cadence ~{cadence_days}d"
        rows.append(
            {
                "label": _source_label(source_type),
                "status": _row_status_for_source(as_of, count, last_seen, cadence_days),
                "detail": detail,
            }
        )

    rows.append(
        {
            "label": "Breadth",
            "status": "confirmed" if len(targets) >= 3 else "narrow",
            "detail": f"{len(targets)} named targets carrying the setup",
        }
    )

    decision_window = _build_decision_window(playbook, profiles, as_of)
    rows.append(
        {
            "label": "Negation Risk",
            "status": "contained" if decision_window["status"] == "fresh" else decision_window["status"],
            "detail": decision_window["status_note"],
        }
    )

    return rows


def _build_stakes(
    profiles: list[TickerSignalProfile],
    targets: list[str],
    source_types: list[str],
) -> dict[str, Any]:
    contract_total = sum(
        _to_float(item.get("amount"))
        for profile in profiles
        for item in profile.gov_contract_items
    )
    influence_total = sum(profile.influence_contracts_received for profile in profiles)
    options_total = sum(profile.options_notional for profile in profiles)
    signal_count = sum(sum(profile.source_counts.values()) for profile in profiles)

    if contract_total > 0:
        capital_signal = f"{_format_money(contract_total)} in direct contract awards"
    elif influence_total > 0:
        capital_signal = f"{_format_money(influence_total)} in influence-loop receipts"
    elif options_total > 0:
        capital_signal = f"{_format_money(options_total)} in unusual options notional"
    else:
        capital_signal = f"{signal_count} live signal prints"

    return {
        "breadth_count": len(targets),
        "source_family_count": len(source_types),
        "signal_count": signal_count,
        "capital_signal": capital_signal,
    }


def _build_lagging_factors(
    playbook: PlaybookBlueprint,
    board_rows: list[dict[str, str]],
    targets: list[str],
    source_types: list[str],
) -> list[str]:
    issues: list[str] = []
    primary_labels = {_source_label(source) for source in playbook.primary_sources}

    if len(source_types) == 1:
        issues.append("Only one source family is carrying the setup.")

    for row in board_rows:
        label = row["label"]
        status = row["status"]
        if label == "Breadth" and status == "narrow":
            issues.append(f"Only {len(targets)} names are carrying the setup.")
        elif label == "Negation Risk" and status == "due":
            issues.append("Confirmation is due now.")
        elif label == "Negation Risk" and status == "late":
            issues.append("The confirmation window has gone stale.")
        elif label in primary_labels and status == "missing":
            issues.append(f"Still missing live {label.lower()} confirmation.")
        elif label in primary_labels and status == "due":
            issues.append(f"{label} needs a fresh print.")
        elif label in primary_labels and status == "late":
            issues.append(f"{label} has gone stale.")

    return _dedupe(issues, limit=3)


def _build_upgrade_trigger(
    playbook: PlaybookBlueprint,
    board_rows: list[dict[str, str]],
    targets: list[str],
    source_types: list[str],
    decision_window: dict[str, Any],
) -> str:
    primary_labels = {_source_label(source) for source in playbook.primary_sources}

    for row in board_rows:
        if row["label"] in primary_labels and row["status"] == "missing":
            return f"Need the first live {row['label'].lower()} print landing in {', '.join(targets[:2])}."
    for row in board_rows:
        if row["label"] in primary_labels and row["status"] == "due":
            return f"Need a fresh {row['label'].lower()} print before the current window expires."
    for row in board_rows:
        if row["label"] in primary_labels and row["status"] == "late":
            return f"Need a new {row['label'].lower()} print now or this setup downgrades."
    if len(source_types) == 1:
        return "Need a second live source family confirming the same names."
    if len(targets) < 3:
        return "Need one more named company confirming the same chain."
    if decision_window.get("status") == "fresh":
        return "Keep the same source families printing in the same names."
    return "Need a fresh confirming print to tighten the window."


def _build_quality_label(lagging_factors: list[str], confidence: int) -> str:
    if confidence >= 84 and len(lagging_factors) <= 1:
        return "tight"
    if confidence >= 72 and len(lagging_factors) <= 2:
        return "mixed"
    if len(lagging_factors) >= 2 or confidence < 62:
        return "lagging"
    return "mixed"


def _apply_quality_penalty(
    playbook: PlaybookBlueprint,
    board_rows: list[dict[str, str]],
    source_types: list[str],
    targets: list[str],
    decision_window: dict[str, Any],
) -> int:
    penalty = 0
    primary_labels = {_source_label(source) for source in playbook.primary_sources}

    if len(source_types) == 1:
        penalty += 3
    if len(targets) == 2:
        penalty += 2

    if decision_window["status"] == "due":
        penalty += 4
    elif decision_window["status"] == "late":
        penalty += 9

    for row in board_rows:
        if row["label"] not in primary_labels:
            continue
        if row["status"] == "missing":
            penalty += 4
        elif row["status"] == "due":
            penalty += 2
        elif row["status"] == "late":
            penalty += 4

    return penalty


def _base_opportunity(
    playbook: PlaybookBlueprint,
    profiles: list[TickerSignalProfile],
    as_of: date,
) -> dict[str, Any]:
    if not profiles:
        raise ValueError(f"{playbook.id} requires live profiles")

    targets = _top_targets(profiles)
    target_names = _profile_names(profiles)
    source_types = _supporting_source_types(profiles)

    confidence = playbook.base_confidence
    unique_sources = len(source_types)
    confidence += min(unique_sources * 3, 9)
    confidence += min(max(len(targets) - 1, 0) * 2, 6)
    if any(profile.options_calls > profile.options_puts for profile in profiles):
        confidence += 3
    if any(profile.influence_contracts_received >= 25_000_000 for profile in profiles):
        confidence += 4
    if any(
        sum(_to_float(item.get("amount")) for item in profile.gov_contract_items) >= 50_000_000
        for profile in profiles
    ):
        confidence += 4
    if any(profile.export_control_items for profile in profiles):
        confidence += 4
    latest_date = max((profile.last_signal_date for profile in profiles if profile.last_signal_date), default=None)
    if latest_date and latest_date >= (date.today() - timedelta(days=60)):
        confidence += 2

    evidence = _collect_evidence(playbook, profiles)
    decision_window = _build_decision_window(playbook, profiles, as_of)
    driver_stack = _build_driver_stack(playbook, profiles, target_names, targets, evidence)
    confirmation_board = _build_confirmation_board(playbook, profiles, targets, as_of)
    confidence -= _apply_quality_penalty(playbook, confirmation_board, source_types, targets, decision_window)
    confidence = max(35, min(int(round(confidence)), 94))
    expected_edge = max(10, playbook.base_edge + max(0, (confidence - 58) // 7))
    stakes = _build_stakes(profiles, targets, source_types)
    lagging_factors = _build_lagging_factors(playbook, confirmation_board, targets, source_types)
    upgrade_trigger = _build_upgrade_trigger(playbook, confirmation_board, targets, source_types, decision_window)
    quality_label = _build_quality_label(lagging_factors, confidence)

    return {
        "id": playbook.id,
        "title": playbook.title,
        "category": playbook.category,
        "setup_type": playbook.setup_type,
        "data_mode": "live",
        "bias": playbook.bias,
        "expected_edge_pct": int(expected_edge),
        "score": confidence,
        "confidence": confidence,
        "confidence_label": _confidence_label(confidence),
        "status": _status_from_confidence(confidence),
        "horizon": playbook.horizon,
        "sector_focus": playbook.sector_focus,
        "thesis": f"{playbook.thesis_stub} Focus names: {target_names}.",
        "summary": f"{playbook.summary_stub} Focus names: {target_names}.",
        "why_now": _why_now(playbook, profiles, targets),
        "mispricing_test": playbook.mispricing_test,
        "clues": _build_clues(playbook, profiles),
        "proof_needed": playbook.proof_needed,
        "entry_rule": _entry_rule(playbook, profiles, targets),
        "exit_rule": _exit_rule(playbook, targets),
        "kill_switch": playbook.kill_switch,
        "data_hooks": _build_clues(playbook, profiles),
        "targets": targets,
        "evidence": evidence,
        "source_tags": source_types or [_source_label(source) for source in playbook.primary_sources],
        "supporting_source_types": source_types,
        "decision_window": decision_window,
        "driver_stack": driver_stack,
        "confirmation_board": confirmation_board,
        "stakes": stakes,
        "lagging_factors": lagging_factors,
        "upgrade_trigger": upgrade_trigger,
        "quality_label": quality_label,
        "route_hint": playbook.route_hint,
    }


def _build_coverage_gaps(profiles: dict[str, TickerSignalProfile]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for playbook in PLAYBOOKS:
        matched_profiles = [profiles[ticker] for ticker in playbook.target_pool if ticker in profiles]
        if matched_profiles:
            continue
        gaps.append(
            {
                "id": playbook.id,
                "title": playbook.title,
                "sector_focus": playbook.sector_focus,
                "targets": list(playbook.target_pool[:4]),
                "missing_primary_sources": [_source_label(source) for source in playbook.primary_sources],
                "route_hint": playbook.route_hint,
                "reason": (
                    f"No qualifying live signals in the current window for {', '.join(playbook.target_pool[:4])}. "
                    f"Need fresh {', '.join(_source_label(source) for source in playbook.primary_sources)}."
                ),
            }
        )
    return gaps


def _load_live_profiles(engine: Engine, as_of: date) -> dict[str, TickerSignalProfile]:
    profiles: dict[str, TickerSignalProfile] = {}
    min_signal_date = as_of - timedelta(days=max(SOURCE_WINDOWS_DAYS.values()))
    influence_cutoff = datetime.combine(as_of - timedelta(days=365), datetime.min.time(), tzinfo=timezone.utc)

    with engine.connect() as conn:
        signal_rows = conn.execute(
            text(
                """
                SELECT ticker, source_type, signal_type, signal_date, trust_score, signal_value
                FROM signal_sources
                WHERE ticker = ANY(:tickers)
                  AND source_type = ANY(:source_types)
                  AND signal_date >= :min_signal_date
                ORDER BY signal_date DESC NULLS LAST, created_at DESC NULLS LAST
                """
            ),
            {
                "tickers": TARGET_UNIVERSE,
                "source_types": list(SOURCE_WINDOWS_DAYS.keys()),
                "min_signal_date": min_signal_date,
            },
        )

        for row in signal_rows:
            ticker = _normalize_ticker(row.ticker)
            source_type = row.source_type
            signal_date = row.signal_date
            if not ticker or ticker in EXCLUDED_TICKERS or source_type not in SOURCE_WINDOWS_DAYS:
                continue
            if signal_date is None or signal_date < (as_of - timedelta(days=SOURCE_WINDOWS_DAYS[source_type])):
                continue

            profile = profiles.get(ticker)
            if profile is None:
                profile = _get_profile(ticker)
                if profile is None:
                    continue
                profiles[ticker] = profile

            payload = _as_payload(row.signal_value)
            _append_signal(profile, source_type, signal_date)

            if source_type == "legislative":
                profile.legislative_items.append(
                    {
                        "date": signal_date,
                        "title": payload.get("title"),
                        "status": payload.get("status"),
                        "topics": payload.get("matched_topics") or [],
                    }
                )
            elif source_type == "gov_contract":
                profile.gov_contract_items.append(
                    {
                        "date": signal_date,
                        "amount": _to_float(payload.get("amount")),
                        "description": payload.get("description"),
                        "recipient_name": payload.get("recipient_name"),
                    }
                )
            elif source_type == "export_control":
                profile.export_control_items.append(
                    {
                        "date": signal_date,
                        "title": payload.get("title"),
                        "severity": payload.get("severity"),
                        "countries": payload.get("countries") or [],
                    }
                )
            elif source_type == "congressional":
                profile.congressional_items.append(
                    {
                        "date": signal_date,
                        "chamber": payload.get("chamber"),
                        "amount_midpoint": _to_float(payload.get("amount_midpoint")),
                    }
                )
            elif source_type == "options_flow":
                profile.options_count += 1
                direction = str(payload.get("direction") or "").upper()
                if direction == "CALL":
                    profile.options_calls += 1
                elif direction == "PUT":
                    profile.options_puts += 1
                profile.options_notional += _to_float(payload.get("notional"))

        influence_rows = conn.execute(
            text(
                """
                SELECT ticker, company, contracts_received, suspicion_score, computed_at
                FROM influence_loops
                WHERE ticker = ANY(:tickers)
                  AND computed_at >= :cutoff
                ORDER BY computed_at DESC NULLS LAST
                """
            ),
            {"tickers": TARGET_UNIVERSE, "cutoff": influence_cutoff},
        )

        for row in influence_rows:
            ticker = _normalize_ticker(row.ticker)
            if not ticker or ticker in EXCLUDED_TICKERS:
                continue
            profile = profiles.get(ticker)
            if profile is None:
                profile = _get_profile(ticker)
                if profile is None:
                    continue
                profiles[ticker] = profile
            profile.influence_contracts_received += _to_float(row.contracts_received)
            profile.influence_suspicion = max(profile.influence_suspicion, _to_float(row.suspicion_score))
            profile.influence_updated_at = row.computed_at
            if row.computed_at:
                computed_date = row.computed_at.date()
                if profile.last_signal_date is None or computed_date > profile.last_signal_date:
                    profile.last_signal_date = computed_date

    return profiles


def _build_live_opportunities(
    profiles: dict[str, TickerSignalProfile],
    as_of: date,
    limit: int,
) -> list[dict[str, Any]]:
    opportunities: list[dict[str, Any]] = []

    for playbook in PLAYBOOKS:
        selected_profiles = [profiles[ticker] for ticker in playbook.target_pool if ticker in profiles]
        if selected_profiles:
            opportunities.append(_base_opportunity(playbook, selected_profiles, as_of))

    opportunities.sort(key=lambda item: (item["score"], item["expected_edge_pct"]), reverse=True)
    return opportunities[:limit]


def _build_summary(opportunities: list[dict[str, Any]], coverage_gaps: list[dict[str, Any]]) -> dict[str, Any]:
    avg_edge = (
        sum(_to_float(item.get("expected_edge_pct")) for item in opportunities) / len(opportunities)
        if opportunities
        else 0.0
    )
    active_count = sum(1 for item in opportunities if item.get("status") == "active")
    arming_count = sum(1 for item in opportunities if item.get("status") == "arming")
    watch_count = sum(1 for item in opportunities if item.get("status") == "watch")
    background_count = sum(1 for item in opportunities if item.get("status") == "background")
    live_count = sum(1 for item in opportunities if item.get("data_mode") == "live")
    evidence_count = sum(len(item.get("evidence") or []) for item in opportunities)
    high_confidence_count = sum(1 for item in opportunities if _to_float(item.get("confidence")) >= 78)

    return {
        "count": len(opportunities),
        "active_count": active_count,
        "arming_count": arming_count,
        "watch_count": watch_count,
        "background_count": background_count,
        "live_count": live_count,
        "high_confidence_count": high_confidence_count,
        "avg_expected_edge_pct": round(avg_edge, 1),
        "evidence_count": evidence_count,
        "coverage_gap_count": len(coverage_gaps),
        "top_setup": opportunities[0]["id"] if opportunities else None,
        "public_data_only": True,
    }


def build_market_edge_snapshot(
    engine: Engine | None,
    as_of: date | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    as_of = as_of or datetime.now(timezone.utc).date()
    limit = max(1, min(limit, len(PLAYBOOKS)))

    if engine is None:
        profiles: dict[str, TickerSignalProfile] = {}
        opportunities: list[dict[str, Any]] = []
    else:
        profiles = _load_live_profiles(engine, as_of)
        opportunities = _build_live_opportunities(profiles, as_of, limit)
    coverage_gaps = _build_coverage_gaps(profiles)

    return {
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "public_data_only": True,
        "summary": _build_summary(opportunities, coverage_gaps),
        "opportunities": opportunities,
        "coverage_gaps": coverage_gaps,
    }


__all__ = ["build_market_edge_snapshot"]
