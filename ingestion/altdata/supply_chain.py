"""
GRID Supply Chain Leading Indicators ingestion module.

Pulls shipping and freight rate data that leads economic turns
by 3-6 months:

1. Freightos Baltic Index (FBX) — global container shipping rates
   via the Freightos Terminal public API
2. Drewry World Container Index (WCI) — composite container freight
   benchmark, scraped from the public data page
3. ISM Manufacturing PMI supplier deliveries — from FRED (MANEMP
   already ingested, we add delivery-related series)

Series stored:
- shipping_rate_global (FBX composite)
- container_index (Drewry WCI)
- freight_baltic_dry (already in baltic_dry.py — we add complementary)
- supply_chain.fbx.{route} — per-route FBX indices
- supply_chain.ism_deliveries — ISM supplier delivery times
- supply_chain.ism_backlog — ISM backlog of orders

Source: Freightos, Drewry (public), FRED
Schedule: Weekly (shipping data updates weekly)
"""

from __future__ import annotations

import json
import math
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Configuration ────────────────────────────────────────────────────

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 1.0

# Freightos Baltic Index (FBX) — public API endpoint
_FREIGHTOS_URL: str = "https://terminal.freightos.com/api/v1/indexes"
_FREIGHTOS_HISTORICAL_URL: str = (
    "https://terminal.freightos.com/api/v1/indexes/history"
)
# Both Freightos URLs now 301 to a www.freightos.com sales page (checked from
# grid-svr 2026-11-07), and neither has ever written a row to raw_series. Each
# run made 6 historical calls, logged 6 "parse failed" warnings and marked the
# whole supply-chain pull partial. Drewry's container_index (USD/FEU, weekly)
# is the live container-freight series. Set False only for a working endpoint.
_FREIGHTOS_RETIRED: bool = True

# Drewry World Container Index — public page
_DREWRY_URL: str = "https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/world-container-index-assessed-by-drewry"
_DREWRY_WCI_MIN: float = 100.0
_DREWRY_WCI_MAX: float = 25_000.0

_DREWRY_ORDINAL_RE = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)\b", re.IGNORECASE)
_DREWRY_WEEKDAY_RE = re.compile(
    r"^(?:mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)[a-z]*,?\s+",
    re.IGNORECASE,
)
_DREWRY_DATE_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%d %b %Y",
    "%d %b %y",
    "%d %B %Y",
    "%d %B %y",
    "%b %d, %Y",
    "%B %d, %Y",
    "%b %d %Y",
    "%B %d %Y",
    "%d-%b-%Y",
    "%d-%b-%y",
    "%d/%m/%Y",
    "%Y/%m/%d",
)
_DREWRY_DATE_TOKEN = (
    r"(?:\d{4}-\d{2}-\d{2}"
    r"|\d{1,2}(?:st|nd|rd|th)?[\s-]+[A-Za-z]{3,9}[\s-]+\d{2,4}"
    r"|[A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})"
)
_DREWRY_LANE_RE = re.compile(
    r"\b(?:shanghai|rotterdam|los\s+angeles|new\s+york|genoa|"
    r"long\s+beach|ningbo|hamburg|antwerp|singapore|"
    r"transpacific|asia[-–—\s]+europe|east\s+coast|west\s+coast)\b"
    r"|\b(?:la|ny)\s*[-–—/]\s*(?:shanghai|rotterdam)\b"
    r"|\b(?:shanghai|rotterdam)\s*[-–—/]\s*(?:la|ny)\b",
    re.IGNORECASE,
)
_DREWRY_FOREIGN_INDEX_RE = re.compile(
    r"\b(?:scfi|fbx|freightos|baltic\s+dry|bdi|tanker|bulk|capesize|panamax)\b",
    re.IGNORECASE,
)
_DREWRY_COMPOSITE_WORD_RE = re.compile(
    r"(?:^|[^a-z])composite(?:[^a-z]|$)",
    re.IGNORECASE,
)
# Positive WCI attribution (Codex HOLD on #1384 42c3df0e): a generic
# "composite" is not the Drewry WCI.
_DREWRY_LABEL_UNITS_RE = re.compile(r"\s*\([^()]*\)\s*$")
_DREWRY_WCI_NAME_RE = re.compile(r"(?:world\s+container\s+index|\bwci\b)", re.IGNORECASE)
_DREWRY_WCI_COMPOSITE_LABEL_RE = re.compile(
    r"^(?:drewry\s+)?(?:"
    r"(?:world\s+container\s+index|wci)\s+composite(?:\s+index)?"
    r"|composite\s+(?:wci|world\s+container\s+index)(?:\s+index)?"
    r")$",
    re.IGNORECASE,
)
_DREWRY_BARE_COMPOSITE_RE = re.compile(r"^composite(?:\s+index)?$", re.IGNORECASE)
_DREWRY_TABLE_WCI_EXACT_RE = re.compile(
    r"^(?:drewry\s+)?(?:world\s+container\s+index|wci)(?:\s*\([^)]*\))?$",
    re.IGNORECASE,
)
_DREWRY_COLOCATED_DATE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"(?:assessed(?:\s+on)?|week\s+ending|as\s+of|"
        r"for(?:\s+the\s+week\s+(?:ending|of))?)\s*:?\s*"
        r"(?:(?:mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)[a-z]*,?\s*)?"
        rf"({_DREWRY_DATE_TOKEN})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:World\s+Container\s+Index|WCI)(?:\s+composite(?:\s+index)?)?"
        r"\s*(?:[-–—:,|]|\()\s*"
        r"(?:(?:mon|tue|tues|wed|thu|thur|thurs|fri|sat|sun)[a-z]*,?\s*)?"
        rf"({_DREWRY_DATE_TOKEN})",
        re.IGNORECASE,
    ),
)
_DREWRY_WCI_HEADING_DATE_RE = _DREWRY_COLOCATED_DATE_PATTERNS[1]
_DREWRY_PROSE_VALUE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"(?:(?:Drewry\s+)?World\s+Container\s+Index(?:\s+composite)?|"
        r"WCI\s+composite|composite\s+(?:WCI|World\s+Container\s+Index))"
        r"[^$]{0,160}\$\s*([\d,]+(?:\.\d+)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:(?:Drewry\s+)?World\s+Container\s+Index(?:\s+composite)?|"
        r"WCI\s+composite|composite\s+(?:WCI|World\s+Container\s+Index))"
        r"[^0-9]{0,80}([\d,]+(?:\.\d+)?)\s*(?:USD|per\s*40[- ]?ft|/FEU)",
        re.IGNORECASE,
    ),
)
_DREWRY_SENTENCE_SPLIT_RE = re.compile(r"(?<!\d)\.(?:\s+|$)|[;!?]\s+")
_DREWRY_USD_CELL_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d+)?)")
_DREWRY_JS_PROP_STR_RE = re.compile(
    r"(?:[\"']?(?:label|name|title|id|seriesName|caption|metric|index)[\"']?)"
    r"\s*:\s*([\"'])(.*?)\1",
    re.IGNORECASE,
)
_DREWRY_JS_TITLE_TEXT_RE = re.compile(
    r"[\"']?title[\"']?\s*:\s*\{[^{}]*[\"']?text[\"']?\s*:\s*([\"'])(.*?)\1",
    re.IGNORECASE | re.DOTALL,
)
_DREWRY_JS_DATES_RE = re.compile(
    r"(?:[\"']?(?:labels|categories|dates)[\"']?)\s*[=:]\s*\[([^\[\]]+)\]",
    re.IGNORECASE | re.DOTALL,
)
_DREWRY_JS_XAXIS_RE = re.compile(
    r"[\"']?xAxis[\"']?\s*:\s*\{([^{}]*)\}",
    re.IGNORECASE | re.DOTALL,
)
_DREWRY_JS_VALUES_RE = re.compile(
    r"(?:[\"']?(?:data|values|series)[\"']?)\s*[=:]\s*\[([^\[\]]+)\]",
    re.IGNORECASE | re.DOTALL,
)
_DREWRY_JS_DATA_ONLY_RE = re.compile(
    r"(?:[\"']?(?:data|values)[\"']?)\s*[=:]\s*\[([^\[\]]+)\]",
    re.IGNORECASE | re.DOTALL,
)
_DREWRY_JS_MULTISERIES_START_RE = re.compile(
    r"(?:[\"']?(?:datasets|series)[\"']?)\s*:\s*\[",
    re.IGNORECASE,
)


def _utc_today() -> date:
    """Return the current UTC calendar date (pull clock)."""
    return datetime.now(timezone.utc).date()


def _drewry_label_core(raw: str) -> str | None:
    """Whitespace-collapsed label without a trailing units parenthetical.

    None for an empty label or one naming a single lane or a foreign index.
    """
    label = re.sub(r"\s+", " ", (raw or "").strip())
    if not label:
        return None
    if _DREWRY_LANE_RE.search(label) or _DREWRY_FOREIGN_INDEX_RE.search(label):
        return None
    return _DREWRY_LABEL_UNITS_RE.sub("", label).strip()


def _drewry_names_wci(texts: Any) -> bool:
    """True if any context text (caption, heading, chart title) names the WCI.

    A context that also names a single lane or a foreign index proves nothing.
    """
    for text in texts:
        if not isinstance(text, str):
            continue
        if _DREWRY_LANE_RE.search(text) or _DREWRY_FOREIGN_INDEX_RE.search(text):
            continue
        if _DREWRY_WCI_NAME_RE.search(text):
            return True
    return False


def _drewry_scoped_heading_text(table: Any) -> str:
    """Text of the nearest heading before *table* that belongs to the table's own scope.

    The heading's innermost ``<section>``/``<article>`` must be exactly the
    table's innermost one (or both must have none). A heading in a sibling
    section, or in a child section of the table's article, never lends WCI
    context.
    """
    heading = table.find_previous(["h1", "h2", "h3", "h4"])
    if heading is None:
        return ""
    if heading.find_parent(["section", "article"]) is not table.find_parent(["section", "article"]):
        return ""
    return heading.get_text(" ", strip=True)


def _is_drewry_composite_table_label(raw: str, *, context_names_wci: bool = False) -> bool:
    """Return True only if a table header/row label positively attributes the WCI composite.

    The label must itself name the WCI ("World Container Index", "WCI",
    "WCI Composite", "Composite WCI", ...), or be exactly "Composite" /
    "Composite Index" inside a table whose own caption or heading names the
    WCI. Any other wording ("Composite Warehousing Cost") is refused, so a
    composite of something else on the same page is never read as the WCI.
    """
    core = _drewry_label_core(raw)
    if core is None:
        return False
    if _DREWRY_TABLE_WCI_EXACT_RE.match(core) or _DREWRY_WCI_COMPOSITE_LABEL_RE.match(core):
        return True
    return context_names_wci and bool(_DREWRY_BARE_COMPOSITE_RE.match(core))


def _is_drewry_composite_chart_label(raw: str, *, context_names_wci: bool = False) -> bool:
    """Return True only if a chart/series identifier positively attributes the WCI composite.

    The identifier must name both the WCI and the composite ("WCI Composite",
    "World Container Index composite", "Composite WCI"), or be exactly
    "Composite" / "Composite Index" in a chart whose own title or labels name
    the WCI. Single-lane, foreign-index and other composites are refused.
    """
    core = _drewry_label_core(raw)
    if core is None:
        return False
    if _DREWRY_WCI_COMPOSITE_LABEL_RE.match(core):
        return True
    return context_names_wci and bool(_DREWRY_BARE_COMPOSITE_RE.match(core))


_MAX_JS_OBJECT_DEPTH = 50  # chart configs nest a handful of levels deep


def _iter_js_objects(text: str) -> list[str]:
    """Extract balanced ``{ ... }`` blocks while respecting string literals.

    Blocks nested deeper than ``_MAX_JS_OBJECT_DEPTH`` are skipped, so a
    pathological page cannot make extraction quadratic in nesting depth.
    """
    objects: list[str] = []
    stack: list[int] = []
    in_quote: str | None = None
    escaped = False
    for idx, ch in enumerate(text):
        if in_quote is not None:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_quote:
                in_quote = None
            continue
        if ch in ("'", '"'):
            in_quote = ch
        elif ch == "{":
            stack.append(idx)
        elif ch == "}" and stack:
            start = stack.pop()
            if len(stack) < _MAX_JS_OBJECT_DEPTH:
                objects.append(text[start : idx + 1])
    return objects


def _extract_balanced_bracket(text: str, open_idx: int) -> str | None:
    """Return the balanced ``[ ... ]`` slice starting at ``open_idx``."""
    depth = 0
    in_quote: str | None = None
    escaped = False
    for idx in range(open_idx, len(text)):
        ch = text[idx]
        if in_quote is not None:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_quote:
                in_quote = None
            continue
        if ch in ("'", '"'):
            in_quote = ch
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return text[open_idx : idx + 1]
    return None


def _strip_nested_braces(obj_text: str) -> str:
    """Return the interior of ``{ ... }`` with nested ``{ ... }`` replaced by ``{}``."""
    if len(obj_text) < 2 or obj_text[0] != "{" or obj_text[-1] != "}":
        return obj_text
    out: list[str] = []
    depth = 0
    in_quote: str | None = None
    escaped = False
    for ch in obj_text[1:-1]:
        if in_quote is not None:
            if depth == 0:
                out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_quote:
                in_quote = None
            continue
        if ch in ("'", '"'):
            in_quote = ch
            if depth == 0:
                out.append(ch)
        elif ch == "{":
            if depth == 0:
                out.append("{}")
            depth += 1
        elif ch == "}":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    return "".join(out)


def _parse_js_date_list(raw_list: str) -> list[date] | None:
    tokens = [t.strip().strip("\"'") for t in raw_list.split(",") if t.strip()]
    if not tokens:
        return None
    dates: list[date] = []
    for tok in tokens:
        parsed = _parse_drewry_date(tok)
        if parsed is None:
            return None
        dates.append(parsed)
    return dates


def _parse_js_usd_list(raw_list: str) -> list[float] | None:
    tokens = [t.strip().strip("\"'") for t in raw_list.split(",") if t.strip()]
    if not tokens:
        return None
    vals: list[float] = []
    for tok in tokens:
        parsed = _parse_drewry_usd(tok)
        if parsed is None:
            return None
        vals.append(parsed)
    return vals


def _extract_drewry_chart_candidates(script_text: str) -> tuple[dict[date, float], bool]:
    """Extract WCI composite ``{date: value}`` pairs from an embedded chart script.

    Requires explicit composite proof directly on the matched chart/series
    object and refuses unlabeled or sibling-lane charts. Returns
    ``(candidates, saw_unproven_chart)``.
    """
    saw_unproven = False
    objects = _iter_js_objects(script_text)

    # First pass: multi-series chart objects (Chart.js datasets / Highcharts series)
    for obj in objects:
        direct = _strip_nested_braces(obj)
        dates_match = _DREWRY_JS_DATES_RE.search(direct)
        if dates_match is None:
            xaxis_match = _DREWRY_JS_XAXIS_RE.search(obj)
            if xaxis_match:
                dates_match = _DREWRY_JS_DATES_RE.search(xaxis_match.group(1))
        if dates_match is None:
            continue
        dates = _parse_js_date_list(dates_match.group(1))
        if not dates:
            continue

        # Check enclosing chart title/labels do not declare a single lane or foreign index
        outer_labels = [m.group(2) for m in _DREWRY_JS_PROP_STR_RE.finditer(direct)]
        title_text_match = _DREWRY_JS_TITLE_TEXT_RE.search(obj)
        if title_text_match:
            outer_labels.append(title_text_match.group(2))
        if any(
            _DREWRY_LANE_RE.search(lbl) or _DREWRY_FOREIGN_INDEX_RE.search(lbl)
            for lbl in outer_labels
        ):
            saw_unproven = True
            continue
        # The chart's own title/labels must name the WCI before a bare
        # "Composite" series inside it counts.
        chart_names_wci = _drewry_names_wci(outer_labels)

        ms_match = _DREWRY_JS_MULTISERIES_START_RE.search(obj)
        if ms_match is not None:
            bracket_slice = _extract_balanced_bracket(obj, ms_match.end() - 1)
            if bracket_slice and "{" in bracket_slice:
                for ds_obj in _iter_js_objects(bracket_slice):
                    ds_direct = _strip_nested_braces(ds_obj)
                    val_match = _DREWRY_JS_DATA_ONLY_RE.search(ds_direct)
                    if val_match is None:
                        continue
                    vals = _parse_js_usd_list(val_match.group(1))
                    if vals is None or len(vals) != len(dates):
                        continue
                    ds_labels = [
                        m.group(2) for m in _DREWRY_JS_PROP_STR_RE.finditer(ds_direct)
                    ]
                    if any(
                        _DREWRY_LANE_RE.search(lbl) or _DREWRY_FOREIGN_INDEX_RE.search(lbl)
                        for lbl in ds_labels
                    ):
                        saw_unproven = True
                        continue
                    if any(
                        _is_drewry_composite_chart_label(lbl, context_names_wci=chart_names_wci)
                        for lbl in ds_labels
                    ):
                        return dict(zip(dates, vals)), False
                    saw_unproven = True
                continue

        # Single-series chart object: direct numeric array + direct composite label
        val_match = _DREWRY_JS_VALUES_RE.search(direct)
        if val_match is None:
            continue
        vals = _parse_js_usd_list(val_match.group(1))
        if vals is None or len(vals) != len(dates):
            continue
        if any(
            _is_drewry_composite_chart_label(lbl, context_names_wci=chart_names_wci)
            for lbl in outer_labels
        ):
            return dict(zip(dates, vals)), False
        saw_unproven = True

    return {}, saw_unproven


def _parse_drewry_date(raw: str) -> date | None:
    """Parse a Drewry assessment date string; returns None if unparseable."""
    if not raw or not isinstance(raw, str):
        return None
    cleaned = raw.strip().replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = _DREWRY_WEEKDAY_RE.sub("", cleaned)
    cleaned = _DREWRY_ORDINAL_RE.sub(r"\1", cleaned)
    for fmt in _DREWRY_DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _parse_drewry_usd(raw: str) -> float | None:
    """Parse a Drewry USD/FEU cell and check plausible index bounds."""
    if not raw or not isinstance(raw, str):
        return None
    cleaned = raw.strip().replace("\xa0", " ")
    if not cleaned or "%" in cleaned or cleaned in {"-", "—", "N/A", "n/a"}:
        return None
    match = _DREWRY_USD_CELL_RE.search(cleaned)
    if match is None:
        return None
    try:
        val = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    if not math.isfinite(val) or not (_DREWRY_WCI_MIN <= val <= _DREWRY_WCI_MAX):
        return None
    return val


def _parse_drewry_wci_html(
    html: str,
    *,
    pull_date: date | None = None,
) -> list[dict[str, Any]]:
    """Parse Drewry WCI composite ``(date, value)`` observations from page HTML.

    Requires proof that the value and date belong to the WCI composite
    (structured composite table column/row, composite-labeled chart series
    with paired dates, or a visible WCI composite prose sentence with a
    co-located assessment date). Refuses sibling-lane charts/prose, generic
    unlabeled charts, generic page/chart dates, synthetic weekly offsets,
    and any ``obs_date`` later than ``pull_date`` (UTC clock; fail closed).
    """
    if not html or not isinstance(html, str):
        log.warning("Drewry WCI: empty HTML response — writing nothing (fail closed)")
        return []

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.warning("Drewry WCI: BeautifulSoup not installed — writing nothing (fail closed)")
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:  # noqa: BLE001
        log.warning("Drewry WCI: HTML parse failed ({e}) — writing nothing", e=str(exc))
        return []

    effective_pull_date = pull_date or _utc_today()
    candidates: dict[date, float] = {}
    saw_unproven_chart = False

    # 1. Structured HTML table (standard: rows=dates; or transposed: cols=dates)
    for table in soup.find_all("table"):
        caption_tag = table.find("caption")
        cap_text = caption_tag.get_text(" ", strip=True) if caption_tag is not None else ""
        if cap_text and (_DREWRY_LANE_RE.search(cap_text) or _DREWRY_FOREIGN_INDEX_RE.search(cap_text)):
            continue
        # The table's own caption, or the nearest heading before it, must name
        # the WCI before a bare "Composite" column or row counts.
        heading_text = _drewry_scoped_heading_text(table)
        table_names_wci = _drewry_names_wci([cap_text] if cap_text else [heading_text])

        raw_rows = [
            [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            for tr in table.find_all("tr")
        ]
        raw_rows = [r for r in raw_rows if len(r) >= 2]
        if len(raw_rows) < 2:
            continue

        headers = raw_rows[0]
        headers_lower = [h.lower() for h in headers]

        # Standard orientation: header names a Composite/WCI column, rows are dates
        comp_col = next(
            (
                i
                for i, h in enumerate(headers)
                if _is_drewry_composite_table_label(h, context_names_wci=table_names_wci)
            ),
            None,
        )
        if comp_col is not None:
            date_col = next(
                (
                    i
                    for i, h in enumerate(headers_lower)
                    if i != comp_col and ("date" in h or "week" in h)
                ),
                0 if comp_col != 0 else None,
            )
            if date_col is not None:
                for row in raw_rows[1:]:
                    if max(date_col, comp_col) >= len(row):
                        continue
                    obs_date = _parse_drewry_date(row[date_col])
                    val = _parse_drewry_usd(row[comp_col])
                    if obs_date is not None and val is not None:
                        candidates[obs_date] = val
            if candidates:
                break

        # Transposed orientation: header cells are assessment dates, Composite is a row
        date_cols = {
            i: d
            for i, h in enumerate(headers)
            if (d := _parse_drewry_date(h)) is not None
        }
        if date_cols:
            for row in raw_rows[1:]:
                if _is_drewry_composite_table_label(row[0], context_names_wci=table_names_wci):
                    for col_idx, obs_date in date_cols.items():
                        if col_idx < len(row):
                            val = _parse_drewry_usd(row[col_idx])
                            if val is not None:
                                candidates[obs_date] = val
                    break
            if candidates:
                break

    # 2. Embedded chart <script> with EXPLICIT paired dates AND WCI composite proof
    if not candidates:
        for script in soup.find_all("script"):
            script_text = script.string or script.get_text() or ""
            if not script_text:
                continue
            chart_pairs, unproven = _extract_drewry_chart_candidates(script_text)
            if chart_pairs:
                candidates.update(chart_pairs)
                break
            if unproven:
                saw_unproven_chart = True

    if saw_unproven_chart and not candidates:
        log.warning(
            "Drewry WCI: ignoring chart script without proven WCI composite "
            "label/identifier (possible sibling lane or generic chart)"
        )

    # 3. Visible prose text WITH co-located WCI composite label AND assessment date
    #    (never accepts generic <time> tags or decoupled page/chart update dates)
    if not candidates:
        for noise in soup.find_all(["script", "style", "noscript"]):
            noise.decompose()

        for block in soup.find_all(["p", "li", "dd", "div", "section", "article"]):
            if block.find(["p", "li", "dd", "section", "article", "table"]) is not None:
                continue
            block_text = block.get_text(" ", strip=True)
            if not block_text:
                continue

            # Optional WCI-labeled heading inside the same enclosing article/section
            scoped_heading_date: date | None = None
            container = block.find_parent(["article", "section"])
            if container is not None:
                for heading in container.find_all(["h1", "h2", "h3"]):
                    h_text = heading.get_text(" ", strip=True)
                    if _DREWRY_LANE_RE.search(h_text) or _DREWRY_FOREIGN_INDEX_RE.search(h_text):
                        continue
                    h_match = _DREWRY_WCI_HEADING_DATE_RE.search(h_text)
                    if h_match:
                        scoped_heading_date = _parse_drewry_date(h_match.group(1))
                        if scoped_heading_date is not None:
                            break

            for sentence in _DREWRY_SENTENCE_SPLIT_RE.split(block_text):
                sentence = sentence.strip()
                if not sentence:
                    continue
                if _DREWRY_LANE_RE.search(sentence) or _DREWRY_FOREIGN_INDEX_RE.search(sentence):
                    continue

                val: float | None = None
                for pattern in _DREWRY_PROSE_VALUE_PATTERNS:
                    val_match = pattern.search(sentence)
                    if val_match:
                        val = _parse_drewry_usd(val_match.group(1))
                        if val is not None:
                            break
                if val is None:
                    continue

                assessment_date: date | None = None
                for d_pattern in _DREWRY_COLOCATED_DATE_PATTERNS:
                    d_match = d_pattern.search(sentence)
                    if d_match:
                        assessment_date = _parse_drewry_date(d_match.group(1))
                        if assessment_date is not None:
                            break
                if assessment_date is None:
                    assessment_date = scoped_heading_date

                if assessment_date is not None:
                    candidates[assessment_date] = val
                    break
            if candidates:
                break

    data_points: list[dict[str, Any]] = []
    for obs_date in sorted(candidates):
        if obs_date > effective_pull_date:
            log.warning(
                "Drewry WCI: refusing future obs_date {d} > pull_date {p}",
                d=obs_date.isoformat(),
                p=effective_pull_date.isoformat(),
            )
            continue
        data_points.append({
            "date": obs_date.isoformat(),
            "value": candidates[obs_date],
        })

    if not data_points:
        log.warning(
            "Drewry WCI: could not prove WCI composite value and co-located "
            "assessment date from page — writing nothing (fail closed)"
        )

    return data_points

# FRED series for ISM manufacturing supply chain indicators
ISM_FRED_SERIES: dict[str, dict[str, str]] = {
    "supply_chain.durable_goods_orders": {
        "fred_id": "DGORDER",
        "description": "Manufacturers' New Orders: Durable Goods ($M)",
    },
    "supply_chain.mfg_new_orders": {
        "fred_id": "NEWORDER",
        "description": "Manufacturers' New Orders: All Manufacturing ($M)",
    },
    "supply_chain.business_inventories": {
        "fred_id": "BUSINV",
        "description": "Total Business Inventories ($M)",
    },
    "supply_chain.mfg_shipments": {
        "fred_id": "MNFCTRSMSA",
        "description": "Manufacturers' Shipments, Total ($M, SA)",
    },
    "supply_chain.industrial_production": {
        "fred_id": "INDPRO",
        "description": "Industrial Production Index (2017=100)",
    },
    "supply_chain.trade_balance": {
        "fred_id": "BOPGSTB",
        "description": "Trade Balance: Goods & Services ($M)",
    },
    "supply_chain.mfg_hours": {
        "fred_id": "AWHMAN",
        "description": "Avg Weekly Hours: Manufacturing (hours)",
    },
    "supply_chain.capex_orders": {
        "fred_id": "A36SNO",
        "description": "Nondefense Capital Goods New Orders excl Aircraft ($M)",
    },
}

# FBX route codes and descriptions
FBX_ROUTES: dict[str, str] = {
    "FBX": "Freightos Baltic Index — Global Composite",
    "FBX01": "China/East Asia to North America West Coast",
    "FBX02": "China/East Asia to North Europe",
    "FBX03": "China/East Asia to Mediterranean",
    "FBX11": "North America West Coast to China/East Asia",
    "FBX12": "North Europe to China/East Asia",
}


class SupplyChainPuller(BasePuller):
    """Pulls supply chain leading indicators into raw_series.

    Combines multiple data sources that collectively lead
    economic turns by 3-6 months:
    - Freightos Baltic Index (container shipping rates)
    - Drewry World Container Index
    - ISM Manufacturing delivery/backlog indicators from FRED

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Supply_Chain.
        fred_api_key: FRED API key for ISM series.
    """

    SOURCE_NAME: str = "Supply_Chain"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://terminal.freightos.com/",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 28,
    }

    def __init__(
        self,
        db_engine: Engine,
        fred_api_key: str = "",
        api_key: str = "",
    ) -> None:
        """Initialise the supply chain puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            fred_api_key: FRED API key for ISM series (optional —
                ISM pull is skipped if empty).
            api_key: Alias for fred_api_key (used by Hermes generic instantiation).
        """
        self.fred_api_key = fred_api_key or api_key
        super().__init__(db_engine)
        fred_status = "set" if self.fred_api_key else "missing"
        log.info(
            "SupplyChainPuller initialised — source_id={sid}, fred_key={fk}",
            sid=self.source_id,
            fk=fred_status,
        )

    # ------------------------------------------------------------------ #
    # Freightos Baltic Index (FBX)
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_freightos_current(self) -> list[dict[str, Any]]:
        """Fetch current FBX index values from Freightos Terminal.

        Returns:
            List of index data dicts with route, value, date.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }

        try:
            resp = requests.get(
                _FREIGHTOS_URL,
                headers=headers,
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("Freightos API request failed: {e}", e=str(exc))
            raise

        # JSON parse must happen OUTSIDE the requests.RequestException
        # handler — newer requests (>=2.27) surface JSONDecodeError as a
        # subclass of RequestException, so a non-JSON body would otherwise
        # be re-raised and burn 3 retries before logging an ERROR. The
        # Freightos endpoint occasionally returns an HTML splash/captcha
        # page when rate-limited; treat that as "no data this cycle".
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            log.warning(
                "Freightos response parse failed (likely HTML/empty body): {e}",
                e=str(exc),
            )
            return []

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("indexes", data.get("data", []))
        return []

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_freightos_historical(
        self,
        route: str = "FBX",
        start_date: str = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Fetch historical FBX data for a route.

        Parameters:
            route: FBX route code (e.g. 'FBX', 'FBX01').
            start_date: Start date for historical data.

        Returns:
            List of historical data points.
        """
        headers = {
            "User-Agent": "GRID-DataPuller/1.0",
            "Accept": "application/json",
        }

        try:
            resp = requests.get(
                _FREIGHTOS_HISTORICAL_URL,
                headers=headers,
                params={
                    "index": route,
                    "from": start_date,
                },
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning(
                "Freightos historical fetch failed for {r}: {e}",
                r=route,
                e=str(exc),
            )
            raise

        # JSON parse OUTSIDE the RequestException handler — newer requests
        # (>=2.27) surfaces JSONDecodeError as a RequestException subclass,
        # so a non-JSON body (HTML splash, rate-limit page, empty body)
        # would otherwise burn 3 retries before logging. Match the pattern
        # in _fetch_freightos_current (line 194).
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            log.warning(
                "Freightos historical parse failed for {r} (likely HTML/empty body): {e}",
                r=route,
                e=str(exc),
            )
            return []

        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get("history", data.get("data", []))
        return []

    def _pull_freightos(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull Freightos Baltic Index data.

        Attempts historical data first, falls back to current snapshot.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Dict with status and rows_inserted.
        """
        total_inserted = 0
        errors: list[str] = []

        # Try each route
        for route_code, route_desc in FBX_ROUTES.items():
            series_id = (
                "shipping_rate_global"
                if route_code == "FBX"
                else f"supply_chain.fbx.{route_code.lower()}"
            )

            try:
                # Try historical first
                historical = self._fetch_freightos_historical(
                    route=route_code,
                    start_date=start_date.isoformat(),
                )

                if historical:
                    inserted = self._store_freightos_data(
                        series_id, historical, start_date, route_code, route_desc,
                    )
                    total_inserted += inserted
                    log.info(
                        "Freightos {r}: {n} rows inserted",
                        r=route_code,
                        n=inserted,
                    )
                else:
                    log.debug(
                        "Freightos: no historical data for {r}",
                        r=route_code,
                    )

            except Exception as exc:
                err_msg = f"Freightos {route_code}: {str(exc)}"
                log.warning(err_msg)
                errors.append(err_msg)

            time.sleep(_RATE_LIMIT_DELAY)

        # Fallback: try current snapshot for the global composite
        if total_inserted == 0:
            try:
                current = self._fetch_freightos_current()
                if current:
                    inserted = self._store_freightos_snapshot(current, start_date)
                    total_inserted += inserted
            except Exception as exc:
                errors.append(f"Freightos current: {str(exc)}")

        return {
            "source": "freightos",
            "status": "SUCCESS" if total_inserted > 0 else "PARTIAL",
            "rows_inserted": total_inserted,
            "errors": errors,
        }

    def _store_freightos_data(
        self,
        series_id: str,
        data: list[dict[str, Any]],
        start_date: date,
        route_code: str,
        route_desc: str,
    ) -> int:
        """Store Freightos historical data points.

        Parameters:
            series_id: Target series ID.
            data: List of historical data points.
            start_date: Earliest date to store.
            route_code: FBX route code.
            route_desc: Route description.

        Returns:
            Number of rows inserted.
        """
        inserted = 0

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(series_id, conn)

            for point in data:
                # Parse date from various formats
                date_val = point.get("date") or point.get("timestamp") or point.get("t")
                value = point.get("value") or point.get("price") or point.get("v")

                if date_val is None or value is None:
                    continue

                try:
                    if isinstance(date_val, str):
                        obs_date = date.fromisoformat(date_val[:10])
                    elif isinstance(date_val, (int, float)):
                        obs_date = datetime.fromtimestamp(
                            date_val, tz=timezone.utc
                        ).date()
                    else:
                        continue
                except (ValueError, OSError):
                    continue

                if obs_date < start_date or obs_date in existing:
                    continue

                try:
                    val_float = float(value)
                except (ValueError, TypeError):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=val_float,
                    raw_payload={
                        "route": route_code,
                        "description": route_desc,
                        "source": "Freightos_FBX",
                        "unit": "USD/FEU",
                    },
                )
                inserted += 1

        return inserted

    def _store_freightos_snapshot(
        self,
        current_data: list[dict[str, Any]],
        start_date: date,
    ) -> int:
        """Store a current FBX snapshot.

        Parameters:
            current_data: List of current index values.
            start_date: Earliest date to store.

        Returns:
            Number of rows inserted.
        """
        inserted = 0
        today = date.today()

        if today < start_date:
            return 0

        with self.engine.begin() as conn:
            for item in current_data:
                index_code = item.get("code") or item.get("index", "")
                value = item.get("value") or item.get("price")

                if not index_code or value is None:
                    continue

                series_id = (
                    "shipping_rate_global"
                    if index_code.upper() == "FBX"
                    else f"supply_chain.fbx.{index_code.lower()}"
                )

                if self._row_exists(series_id, today, conn):
                    continue

                try:
                    val_float = float(value)
                except (ValueError, TypeError):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=today,
                    value=val_float,
                    raw_payload={
                        "route": index_code,
                        "source": "Freightos_FBX_snapshot",
                        "unit": "USD/FEU",
                    },
                )
                inserted += 1

        return inserted

    # ------------------------------------------------------------------ #
    # Drewry World Container Index
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(
            ConnectionError,
            TimeoutError,
            OSError,
            requests.RequestException,
        ),
    )
    def _fetch_drewry_wci(self) -> list[dict[str, Any]]:
        """Fetch Drewry World Container Index from the public page.

        Scrapes the Drewry WCI page for the latest index values.
        Falls back gracefully if page structure changes.

        Returns:
            List of data points with date and value.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        }

        resp = requests.get(
            _DREWRY_URL,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data_points = _parse_drewry_wci_html(resp.text, pull_date=_utc_today())

        log.info(
            "Drewry WCI: parsed {n} data points",
            n=len(data_points),
        )
        return data_points

    def _pull_drewry(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull Drewry World Container Index data.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Dict with status and rows_inserted.
        """
        try:
            data = self._fetch_drewry_wci()
        except Exception as exc:
            log.warning("Drewry WCI fetch failed: {e}", e=str(exc))
            return {
                "source": "drewry",
                "status": "FAILED",
                "rows_inserted": 0,
                "error": str(exc),
            }

        if not data:
            return {
                "source": "drewry",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["No data parsed from Drewry page"],
            }

        inserted = 0
        rejected_future = 0
        valid_points = 0
        pull_date = _utc_today()
        series_id = "container_index"

        with self.engine.begin() as conn:
            existing = self._get_existing_dates(series_id, conn)

            for point in data:
                try:
                    obs_date = date.fromisoformat(point["date"][:10])
                except (ValueError, KeyError, TypeError):
                    continue

                if obs_date > pull_date:
                    log.warning(
                        "Drewry WCI: refusing future obs_date {d} > pull_date {p}",
                        d=obs_date.isoformat(),
                        p=pull_date.isoformat(),
                    )
                    rejected_future += 1
                    continue

                valid_points += 1
                if obs_date < start_date or obs_date in existing:
                    continue

                try:
                    val = float(point["value"])
                except (ValueError, TypeError):
                    continue
                if not math.isfinite(val):
                    log.warning(
                        "Drewry WCI: refusing non-finite value {v!r} on {d}",
                        v=point.get("value"),
                        d=obs_date.isoformat(),
                    )
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=obs_date,
                    value=val,
                    raw_payload={
                        "source": "Drewry_WCI",
                        "description": "Drewry World Container Index",
                        "unit": "USD/FEU",
                    },
                )
                existing.add(obs_date)
                inserted += 1

        if rejected_future > 0 and valid_points == 0:
            return {
                "source": "drewry",
                "status": "PARTIAL",
                "rows_inserted": 0,
                "errors": ["All Drewry WCI dates were later than pull_date"],
            }

        log.info("Drewry WCI: {n} rows inserted", n=inserted)
        return {
            "source": "drewry",
            "status": "SUCCESS",
            "rows_inserted": inserted,
        }

    # ------------------------------------------------------------------ #
    # ISM Manufacturing from FRED
    # ------------------------------------------------------------------ #

    def _pull_ism_fred(
        self,
        start_date: date,
    ) -> dict[str, Any]:
        """Pull ISM supply chain indicators from FRED.

        Requires fred_api_key to be set. Skips gracefully if missing.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            Dict with status and rows_inserted.
        """
        if not self.fred_api_key:
            log.info(
                "SupplyChain: FRED API key not set — skipping ISM pull"
            )
            return {
                "source": "fred_ism",
                "status": "SKIPPED",
                "rows_inserted": 0,
                "errors": ["No FRED API key"],
            }

        try:
            from fedfred import FredAPI
        except ImportError:
            log.warning(
                "SupplyChain: fedfred not installed — skipping ISM pull"
            )
            return {
                "source": "fred_ism",
                "status": "SKIPPED",
                "rows_inserted": 0,
                "errors": ["fedfred not installed"],
            }

        fred = FredAPI(self.fred_api_key)
        total_inserted = 0
        errors: list[str] = []

        for series_id, meta in ISM_FRED_SERIES.items():
            fred_id = meta["fred_id"]

            try:
                try:
                    data: pd.DataFrame = fred.get_series_observations(
                        fred_id,
                        observation_start=str(start_date),
                    )
                except KeyError as exc:
                    # fedfred 3.0's to_pd_df indexes df['date'] even when FRED
                    # returns no observations, so an empty window (a monthly
                    # series asked for the last few weeks) raises KeyError('date').
                    # That is "no new data", not a failure (172 warnings/24h
                    # on 2026-11-07, and Supply_Chain marked PARTIAL every run).
                    if exc.args != ("date",):
                        raise
                    data = None

                if data is None or data.empty:
                    log.debug("FRED {fid}: no data returned", fid=fred_id)
                    continue

                # Normalise columns
                if "observation_date" in data.columns:
                    data = data.rename(columns={"observation_date": "date"})
                elif data.index.name == "date" or hasattr(data.index, "date"):
                    data = data.reset_index()

                data = data[data["value"].apply(
                    lambda v: v != "." and pd.notna(v)
                )].copy()
                data["value"] = pd.to_numeric(data["value"], errors="coerce")
                coerced_count = data["value"].isna().sum()
                if coerced_count > 0:
                    log.warning(
                        "Coerced {n} non-numeric values to NaN for {sid}",
                        n=int(coerced_count),
                        sid=series_id,
                    )
                data = data.dropna(subset=["value"])

                inserted = 0
                with self.engine.begin() as conn:
                    existing = self._get_existing_dates(series_id, conn)

                    for _, row in data.iterrows():
                        obs_date_val = (
                            row["date"].date()
                            if hasattr(row["date"], "date") and callable(row["date"].date)
                            else pd.Timestamp(row["date"]).date()
                        )
                        if obs_date_val < start_date or obs_date_val in existing:
                            continue

                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=obs_date_val,
                            value=float(row["value"]),
                            raw_payload={
                                "fred_series": fred_id,
                                "description": meta["description"],
                                "source": "FRED_ISM",
                            },
                        )
                        inserted += 1

                total_inserted += inserted
                log.info(
                    "ISM {sid} ({fid}): {n} rows inserted",
                    sid=series_id,
                    fid=fred_id,
                    n=inserted,
                )

            except Exception as exc:
                err_msg = f"ISM {fred_id}: {str(exc)}"
                log.warning(err_msg)
                errors.append(err_msg)

            time.sleep(0.25)  # FRED rate limit

        # PARTIAL only when a series actually failed: no new rows in the window
        # is the normal state for monthly data between releases.
        return {
            "source": "fred_ism",
            "status": "PARTIAL" if errors else "SUCCESS",
            "rows_inserted": total_inserted,
            "errors": errors,
        }

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def pull_all(
        self,
        start_date: str | date = "2020-01-01",
    ) -> list[dict[str, Any]]:
        """Pull all supply chain leading indicators.

        Never stops on a single-source failure -- logs and continues.

        Parameters:
            start_date: Earliest observation date.

        Returns:
            List of per-source result dicts.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)

        log.info(
            "Starting supply chain pull — Freightos + Drewry + ISM from {sd}",
            sd=start_date,
        )

        results: list[dict[str, Any]] = []

        # 1. Freightos Baltic Index (retired: see _FREIGHTOS_RETIRED)
        if _FREIGHTOS_RETIRED:
            log.debug("SupplyChain: Freightos retired (public API gone) — not called")
        else:
            try:
                fbx_result = self._pull_freightos(start_date)
                results.append(fbx_result)
            except Exception as exc:
                log.error("SupplyChain: Freightos pull failed: {e}", e=str(exc))
                results.append({
                    "source": "freightos",
                    "status": "FAILED",
                    "error": str(exc),
                })

        # 2. Drewry World Container Index
        try:
            drewry_result = self._pull_drewry(start_date)
            results.append(drewry_result)
        except Exception as exc:
            log.error("SupplyChain: Drewry pull failed: {e}", e=str(exc))
            results.append({
                "source": "drewry",
                "status": "FAILED",
                "error": str(exc),
            })

        # 3. ISM Manufacturing from FRED
        try:
            ism_result = self._pull_ism_fred(start_date)
            results.append(ism_result)
        except Exception as exc:
            log.error("SupplyChain: ISM FRED pull failed: {e}", e=str(exc))
            results.append({
                "source": "fred_ism",
                "status": "FAILED",
                "error": str(exc),
            })

        total_inserted = sum(r.get("rows_inserted", 0) for r in results)
        succeeded = sum(1 for r in results if r.get("status") == "SUCCESS")
        log.info(
            "Supply chain pull complete — {ok}/{total} sources succeeded, "
            "{n} total rows inserted",
            ok=succeeded,
            total=len(results),
            n=total_inserted,
        )

        return results


if __name__ == "__main__":
    from config import settings
    from db import get_engine

    fred_key = getattr(settings, "FRED_API_KEY", "")
    puller = SupplyChainPuller(
        db_engine=get_engine(),
        fred_api_key=fred_key,
    )
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(
            f"  {r.get('source', '?')}: {r.get('status')} — "
            f"{r.get('rows_inserted', 0)} rows"
        )
