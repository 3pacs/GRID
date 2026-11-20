"""Pure conditional-card display; no data reads, selection or promotion."""
import copy
import html
import json
import math

from analysis.desk_factory_report import factory_report_cards, _count, _identity

DIMENSIONS = (
    "year", "market_phase", "fed_balance_sheet", "vix_third", "spike_state",
    "sector", "group", "sector_phase", "sector_vs_market",
)
METRICS = ("gross_bp", "net_bp", "sd_bp", "t_gross")
NIGHT_PARTITIONS = ("year", "market_phase", "fed_balance_sheet", "vix_third", "spike_state")


def _finite(value):
    try:
        valid = type(value) in (int, float) and math.isfinite(value)
    except (OverflowError, RecursionError) as exc:
        raise ValueError("finite conditional metric required") from exc
    if not valid:
        raise ValueError("finite conditional metric required")
    return value


def _cut(value):
    if not isinstance(value, dict):
        raise ValueError("conditional cut object required")
    nights = _count(value.get("nights"))
    if nights < 30:
        if set(value) != {"nights"}:
            raise ValueError("small sample must retain producer's unscored shape")
    else:
        if set(value) != {"nights", *METRICS}:
            raise ValueError("complete conditional metrics required")
        for key in METRICS:
            if key == "t_gross" and value[key] is None:
                continue
            _finite(value[key])
        if value["sd_bp"] < 0:
            raise ValueError("negative dispersion")
    return copy.deepcopy(value)


def _metadata(value, depth=0):
    """Bounded JSON source metadata, not an interpreted claim."""
    if depth > 5:
        raise ValueError("metadata too deep")
    if value is None or type(value) is bool:
        return
    if type(value) in (int, float):
        _finite(value)
    elif isinstance(value, str):
        if len(value) > 2048:
            raise ValueError("metadata text too long")
    elif isinstance(value, dict):
        if len(value) > 100:
            raise ValueError("too many metadata fields")
        for key, child in value.items():
            _identity(key)
            _metadata(child, depth + 1)
    elif isinstance(value, list):
        if len(value) > 100:
            raise ValueError("too many metadata items")
        for child in value:
            _metadata(child, depth + 1)
    else:
        raise ValueError("JSON metadata required")


def conditional_factory_cards(raw: bytes, **adapter_args) -> dict:
    """Preserve descriptive development cuts, never rank the favorable cuts.

    Caller must supply the independently verified file digest and receipt clocks
    required by factory_report_cards. Byte integrity is not confirmation, chain
    verification, point-in-time sector validity or measured execution cost.
    """
    base = factory_report_cards(raw, **adapter_args)
    if base["source_state"] != "CROSS_SECTION_DEV_RANKING_ONLY":
        raise ValueError("cross-section development report required")
    rep = json.loads(raw)  # Already bounded, duplicate/constant checked by adapter.
    cards = rep.get("cards")
    if not isinstance(cards, dict) or len(cards) > 1000:
        raise ValueError("bounded conditional cards required")
    ranked = {row["trial_id"]: row for row in base["ranking"]}
    rows = []
    for trial_id, card in cards.items():
        _identity(trial_id)
        if trial_id not in ranked:
            raise ValueError("conditional card missing ranked trial")
        if not isinstance(card, dict) or set(card) - {"overall", "state_rows_matched", *DIMENSIONS}:
            raise ValueError("unknown conditional card fields")
        overall = _cut(card.get("overall"))
        if overall["nights"] != ranked[trial_id]["development_fires"]:
            raise ValueError("overall nights do not match ranked development count")
        matched = _count(card.get("state_rows_matched"))
        cuts = {}
        for dim in DIMENSIONS:
            if dim not in card:
                continue
            groups = card[dim]
            if not isinstance(groups, dict) or len(groups) > 1000:
                raise ValueError("bounded conditional groups required")
            cuts[dim] = {}
            for group, cut in groups.items():
                _identity(group)
                checked = _cut(cut)
                if checked["nights"] > overall["nights"]:
                    raise ValueError("stratum exceeds overall nights")
                cuts[dim][group] = checked
        partition_coverage = {}
        for dim in NIGHT_PARTITIONS:
            if dim not in cuts:
                partition_coverage[dim] = {"status": "not_supplied"}
                continue
            covered = sum(cut["nights"] for cut in cuts[dim].values())
            if covered > overall["nights"]:
                raise ValueError("night partition exceeds overall nights")
            partition_coverage[dim] = {
                "status": "reported_partition_counts",
                "classified_nights": covered,
                "unclassified_nights": overall["nights"] - covered,
                "total_nights": overall["nights"],
            }
        rows.append({
            "partition_coverage": partition_coverage,
            "trial_id": trial_id, "evidence": copy.deepcopy(ranked[trial_id]["evidence"]),
            "overall": overall, "state_rows_matched": matched, "cuts": cuts,
            "standing": "development_descriptions_only",
            "confirmation_status": "not_supplied",
            "forward_status": "not_supplied", "promotion_allowed": False,
        })
    metadata = {}
    for key in ("method", "coverage"):
        if key in rep:
            _metadata(rep[key])
            metadata[key] = copy.deepcopy(rep[key])
    return {
        "schema": "desk-conditional-cards-v1", "base": base,
        "cards": rows, "source_metadata": metadata,
        "cards_complete_for_ranking": len(cards) == len(ranked),
        "ranked_without_cards": [key for key in ranked if key not in cards],
        "interpretation": "retrospective descriptive cuts; not independent confirmation",
        "statistic_scope": "t_gross tests gross mean, not net mean",
        "partition_warning": "strata can overlap; do not sum nights across dimensions",
        "point_in_time_sector_validity": "not_verified",
        "execution_cost_measurement": "not_verified",
        "promotion_allowed": False,
    }


def _text(value):
    # Source labels must not turn into links, HTML, table delimiters or new rows.
    text = html.escape(str(value), quote=True)
    for char in ("\\", "|", "[", "]", "`"):
        text = text.replace(char, "\\" + char)
    return text.replace("\r", " ").replace("\n", " ")


def render_conditional_cards(raw: bytes, **adapter_args) -> str:
    """Render directly from validated source bytes; no mutable-card bypass."""
    result = conditional_factory_cards(raw, **adapter_args)
    lines = ["# Development conditional outcomes",
             "Retrospective descriptions, not confirmed signals. No promotion.",
             "t_gross is a gross-return statistic, not net-return evidence.",
             "Cuts may overlap; sample counts are not additive.",
             "Costs and point-in-time sector validity are not independently verified.",
             "Confirmation and forward records: not supplied.",
             "Report SHA256: " + result["base"]["report_sha256"],
             "Target unit: " + _text(result["base"]["target_unit"]),
             "Source method/coverage (unverified metadata): " +
             _text(json.dumps(result["source_metadata"], sort_keys=True))]
    for card in result["cards"]:
        lines += ["", "## " + _text(card["trial_id"]),
                  "Evidence: " + _text(card["evidence"]["label"]) +
                  "; quality=" + _text(card["evidence"]["quality"]),
                  "Reasons: " + _text(", ".join(card["evidence"]["reasons"]) or "none"),
                  "Night partition coverage (reported counts, not verified timestamps): " +
                  _text(json.dumps(card["partition_coverage"], sort_keys=True)),
                  "| Dimension | Stratum | Nights | Gross bp | Net bp | SD bp | t gross |",
                  "|---|---|---:|---:|---:|---:|---:|"]
        entries = [("overall", "all", card["overall"])]
        entries += [(dim, name, cut) for dim, groups in card["cuts"].items()
                    for name, cut in groups.items()]
        for dim, name, cut in entries:
            values = [dim, name, cut["nights"]] + [
                cut.get(key) if cut.get(key) is not None else "not reported"
                for key in METRICS]
            lines.append("| " + " | ".join(_text(x) for x in values) + " |")
    lines += ["", "Ranked trials without conditional cards: " +
              _text(", ".join(result["ranked_without_cards"]) or "none")]
    return "\n".join(lines) + "\n"
