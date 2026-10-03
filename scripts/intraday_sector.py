"""Offline point-in-time participation diagnostics; no directional forecast."""
from datetime import datetime
import math


def _time(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timezone required")
    return result


def participation(packet, decision_at, max_age_seconds=60, max_skew_seconds=5):
    """Require complete PIT membership/weights and synchronized endpoint returns.

    Returns are fractions over one common interval. Weight sums must be one;
    missing constituent data never becomes zero or an equal-weight proxy.
    available_at is a trusted collector receipt, not a source device clock.
    """
    try:
        decision = _time(decision_at)
        weights = packet["weights"]
        if not packet["weights_source_id"] or not packet["universe_id"]:
            raise ValueError("missing universe/weight provenance")
        start, end = _time(packet["interval_start"]), _time(packet["interval_end"])
        effective = _time(packet["weights_effective_at"])
        available = _time(packet["weights_available_at"])
        if not start < end <= decision or effective > start or available > start:
            raise ValueError("weights/interval not point-in-time")
        if max_age_seconds < 0 or max_skew_seconds < 0:
            raise ValueError("invalid freshness limits")
        if not weights or any(isinstance(w, bool) or not math.isfinite(w) or w <= 0 for w in weights.values()):
            raise ValueError("invalid weights")
        if not math.isclose(math.fsum(weights.values()), 1.0, abs_tol=1e-9, rel_tol=0):
            raise ValueError("incomplete weight universe")
        rows = packet["constituents"]
        if set(rows) != set(weights):
            raise ValueError("incomplete constituent universe")
        contributions, returns, sectors, receipts, sources = [], [], {}, [], []
        for symbol, weight in weights.items():
            row = rows[symbol]
            if not row["source_id"]:
                raise ValueError("missing constituent provenance")
            source, receipt = _time(row["source_at"]), _time(row["available_at"])
            if not end <= source <= receipt <= decision:
                raise ValueError("invalid source/receipt lineage")
            if (decision-source).total_seconds() > max_age_seconds:
                raise ValueError("stale constituent")
            if _time(row["interval_start"]) != start or _time(row["interval_end"]) != end:
                raise ValueError("mismatched return intervals")
            value = row["return"]
            if isinstance(value, bool) or not math.isfinite(value) or value < -1:
                raise ValueError("invalid return")
            sector = row["sector"]
            if not isinstance(sector, str) or not sector.strip():
                raise ValueError("missing sector")
            contribution = weight * value
            contributions.append(contribution)
            returns.append(value)
            sectors[sector] = sectors.get(sector, 0) + contribution
            receipts.append(receipt)
            sources.append(source)
        if max((max(receipts)-min(receipts)).total_seconds(), (max(sources)-min(sources)).total_seconds()) > max_skew_seconds:
            raise ValueError("unsynchronized receipts")
        gross = math.fsum(abs(c) for c in contributions)
        result = {
            "status": "VALID_DIAGNOSTIC", "directional_edge": "UNVALIDATED",
            "weighted_return": math.fsum(contributions),
            "advancing_fraction": sum(r > 0 for r in returns)/len(returns),
            "declining_fraction": sum(r < 0 for r in returns)/len(returns),
            "advancing_weight": math.fsum(w for w, r in zip(weights.values(), returns) if r > 0),
            "contribution_hhi": math.fsum((abs(c)/gross)**2 for c in contributions) if gross else None,
            "largest_absolute_contribution_share": max(abs(c) for c in contributions)/gross if gross else None,
            "sector_contributions": sectors,
            "available_at": max(receipts).isoformat(),
            "interval_start": start.isoformat(), "interval_end": end.isoformat(),
            "universe_id": packet["universe_id"],
            "weights_source_id": packet["weights_source_id"],
        }
        if not all(math.isfinite(c) for c in contributions) or not math.isfinite(result["weighted_return"]):
            raise ValueError("numerical overflow")
        return result
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as error:
        return {"status": "UNAVAILABLE", "reason": str(error), "directional_edge": "UNVALIDATED"}
