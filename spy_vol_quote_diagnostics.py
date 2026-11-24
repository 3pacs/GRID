"""Offline D2-H1 quote-mark diagnostics; no collection, payoff, scoring or registration."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Context, Decimal, localcontext


@dataclass(frozen=True)
class Quote:
    """Canonical quote update. The caller must independently qualify both clocks."""

    contract: str
    generation: str
    observed_at: datetime
    available_at: datetime
    bid: Decimal | None
    ask: Decimal | None


def _aware(value: datetime) -> bool:
    return isinstance(value, datetime) and value.utcoffset() is not None


def _price_supported(price):
    if not isinstance(price, Decimal) or not price.is_finite():
        return False
    parts = price.as_tuple()
    return len(parts.digits) <= 18 and -12 <= parts.exponent <= 6 and price <= Decimal("1000000")


def _price_key(price):
    # Never compare malformed Decimal values (e.g. signaling NaN) numerically.
    return ("valid", price) if _price_supported(price) else ("invalid",)


def _wide_market(bid, ask):
    # Bounded inputs are exact at this precision, regardless of ambient context.
    with localcontext(Context(prec=40)):
        return ask - bid > max(Decimal("0.20"), (ask + bid) / 4)


def diagnose_quote_mark(legs, updates, *, mark, generation):
    """Check supplied four fixed legs; return no executable result, even on success.

    This is a synthetic/read-only boundary, not a provider-time decoder or freshness
    attestation. All four supplied legs must belong to one admitted-as-a-unit input
    generation. A visible but invalid latest quote cannot borrow an older quote.
    The caller must freeze an immutable, complete input generation and the mark.
    Candidate rows from after the mark are unavailable. Same-observation price
    conflicts are refused without authoritative source sequence; arrival order is
    not treated as revision authority. Identical repeats retain earliest availability.
    """
    legs = tuple(legs)
    if (len(legs) != 4 or any(not isinstance(leg, str) or not leg for leg in legs)
            or len(set(legs)) != 4 or not _aware(mark) or not isinstance(generation, str) or not generation):
        raise ValueError("four distinct contract IDs, one generation and an aware mark required")
    mark = mark.astimezone(UTC)
    newest = {}
    ambiguous = set()
    invalid_clock = set()
    for quote in updates:
        if quote.contract not in legs or quote.generation != generation:
            continue
        if not _aware(quote.available_at):
            invalid_clock.add(quote.contract)
            continue
        available = quote.available_at.astimezone(UTC)
        if available > mark:
            continue
        if not _aware(quote.observed_at) or quote.observed_at.astimezone(UTC) > available:
            invalid_clock.add(quote.contract)
            continue
        observed = quote.observed_at.astimezone(UTC)
        current = newest.get(quote.contract)
        if current is None or observed > current.observed_at.astimezone(UTC):
            newest[quote.contract] = quote
            ambiguous.discard(quote.contract)
        elif observed == current.observed_at.astimezone(UTC):
            if tuple(map(_price_key, (quote.bid, quote.ask))) != tuple(map(_price_key, (current.bid, current.ask))):
                ambiguous.add(quote.contract)
            if available < current.available_at.astimezone(UTC):
                newest[quote.contract] = quote

    reasons = {}
    for leg in legs:
        quote = newest.get(leg)
        if leg in invalid_clock:
            reasons[leg] = "unqualified_clock"
        elif quote is None:
            reasons[leg] = "missing_at_mark"
        elif leg in ambiguous:
            reasons[leg] = "conflicting_latest_update"
        elif mark - quote.observed_at.astimezone(UTC) > timedelta(seconds=60):
            reasons[leg] = "stale_quote"
        elif (not all(_price_supported(p) for p in (quote.bid, quote.ask))
                or quote.bid <= 0 or quote.ask < quote.bid):
            reasons[leg] = "invalid_market"
        elif _wide_market(quote.bid, quote.ask):
            reasons[leg] = "wide_spread"

    return {
        "status": "EXCLUDED_INPUT" if reasons else "FOUR_QUOTES_QUALIFIED",
        "execution": "NOT_ESTABLISHED",
        "reasons": reasons,
        "quotes": () if reasons else tuple(newest[leg] for leg in legs),
    }
