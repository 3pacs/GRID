"""Offline guards for a reviewer-authored public-critic check record.

No model, GRID, database, provider, scoring or publication imports/actions.
The caller must independently pin the context digest and classify claims.
This verifies structured assertions, not arbitrary prose, source authenticity,
causal identification, exhaustive claim coverage or citation entailment.
"""
import argparse
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value):
    require(isinstance(value, (str, int, float)) and not isinstance(value, bool),
            "number must be finite and not boolean")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid number") from exc
    require(result.is_finite(), "number must be finite")
    return result


def return_pct(context, prior_id, current_id, basis, horizon_id):
    require(basis in {"nav_price_level", "market_close_price"}, "unsupported basis")
    prior, current = (context["observations"][x] for x in (prior_id, current_id))
    horizon = context["horizons"][horizon_id]
    require(type(horizon["sessions"]) is int and horizon["sessions"] == 1,
            "this guard only admits a pinned single-session comparison")
    require(prior["basis"] == current["basis"] == basis, "mixed price/NAV basis")
    require(prior["unit"] == current["unit"], "mixed units")
    require(prior["instrument"] == current["instrument"], "instrument mismatch")
    require((prior["session"], current["session"]) ==
            (horizon["start"], horizon["end"]), "window mismatch")
    denominator = number(prior["value"])
    require(denominator > 0, "prior level must be positive")
    return (number(current["value"]) / denominator - 1) * 100


def verify(context, review, expected_context_sha256):
    require(digest(context) == expected_context_sha256, "context pin mismatch")
    require(context["scope"] == review["scope"] == "public_source_only", "scope mismatch")
    require(review["performance_status"] == "unscorable",
            "quality-only critic cannot admit performance scores")
    if not context["originals_available"] or not context["pit_outcomes_available"]:
        require(review["missing_originals_or_pit"] is True, "missing evidence hidden")
    for source in context["sources"].values():
        require(hashlib.sha256(source["text"].encode()).hexdigest() == source["sha256"],
                "source text pin mismatch")
    seen = set()
    require(bool(review["claims"]), "empty claim inventory")
    for claim in review["claims"]:
        require(claim["id"] not in seen, "duplicate claim")
        seen.add(claim["id"])
        quote = claim["exact_quote"]
        require(isinstance(quote, str) and bool(quote.strip()) and
                quote in context["passages"][claim["passage_id"]], "quote mismatch")
        require(claim["inference"] in {"observation", "causal_attribution"}, "inference missing")
        require(claim["disposition"] in {"supported", "hypothesis", "rejected"}, "bad disposition")
        if claim["inference"] == "causal_attribution":
            require(claim["disposition"] in {"hypothesis", "rejected"},
                    "causal attribution cannot be a supported fact in this bounded lane")
        require(bool(claim["citations"]), "citation missing")
        for citation in claim["citations"]:
            source = context["sources"][citation["source_id"]]
            require(citation["url"] == source["url"], "citation URL mismatch")
            excerpt = citation["exact_excerpt"]
            require(isinstance(excerpt, str) and bool(excerpt.strip()) and
                    excerpt in source["text"], "citation excerpt mismatch")
    for item in review["comparisons"]:
        actual = return_pct(context, item["prior"], item["current"], item["basis"], item["horizon"])
        places = item["decimal_places"]
        require(type(places) is int and 0 <= places <= 6, "invalid precision")
        quantum = Decimal(1).scaleb(-places)
        require(actual.quantize(quantum, rounding=ROUND_HALF_UP) == number(item["value_pct"]),
                "return arithmetic mismatch")
    for item in review["counts"]:
        members = item["members"]
        universe = context["count_universe"]
        require(len(universe) == len(set(universe)), "duplicate eligible member")
        require(len(members) == len(set(members)) and set(members) == set(universe),
                "count members must match the complete pinned universe")
        positives = 0
        for member in members:
            pair = context["count_pairs"][member]
            require(all(context["observations"][pair[key]]["instrument"] == member
                        for key in ("prior", "current")),
                    "count member/instrument mismatch or reused cross-member pair")
            positives += return_pct(context, pair["prior"], pair["current"],
                                    item["basis"], item["horizon"]) > 0
        require(type(item["numerator"]) is int and type(item["denominator"]) is int and
                (item["numerator"], item["denominator"]) == (positives, len(universe)),
                "count arithmetic mismatch")
    return {"deterministic_checks": "PASS", "performance_status": "unscorable",
            "semantic_review": "REQUIRED", "context_sha256": expected_context_sha256}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True, type=Path)
    parser.add_argument("--review", required=True, type=Path)
    parser.add_argument("--expected-context-sha256", required=True)
    args = parser.parse_args()
    try:
        result = verify(json.loads(args.context.read_text()), json.loads(args.review.read_text()),
                        args.expected_context_sha256)
    except (ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"deterministic_checks": "FAIL", "reason": str(exc)}))
        raise SystemExit(1)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
