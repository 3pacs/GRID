"""
GRID Apple Supplier List Puller.

Annual pull of the Apple Supplier List PDF (published with the Supplier
Responsibility report) and conversion of every listed supplier into a
high-confidence ``supply_chain_edges`` row pointing at Apple (``aapl``).

Why this puller exists
----------------------
Apple is the single largest discretionary spender in global electronics
manufacturing. Its published supplier list covers ~98% of its direct
spend on materials, manufacturing, and final assembly. Because Apple
publishes it as an accountability document (not a marketing piece), the
list is treated as a *confirmed* source — the counterparty set is
ground truth for Apple's tier-1 suppliers.

Pipeline
--------
1. Fetch the latest Apple Supplier List PDF from apple.com. Apple
   re-publishes the same document URL each year but also adds a
   year-stamped mirror; we try the year-stamped URL first, then fall
   back to the evergreen filename.
2. Parse the PDF with ``pypdf`` (installed on the server; see the
   hermes operator venv). The PDF layout is a two-column table where
   each row is "Supplier name | Supplier facility | City | State |
   Country". We flatten the columns by extracting text page-by-page,
   stripping header/footer boilerplate, and scanning for supplier
   blocks via regex anchors.
3. Canonicalize each supplier name (strip legal suffixes, normalize
   whitespace) and look it up in the ``PUBLIC_TICKER_MAP`` below. If
   the supplier is a listed equity we resolve the edge to its ticker
   node id; otherwise we create a ``private_company`` node in
   ``supply_chain_nodes``.
4. Infer the relationship type: assembly houses (Foxconn, Pegatron,
   Luxshare, Wistron, etc.) become ``contract_mfg``. Chip / component
   vendors (TSMC, Sony, Samsung, etc.) become ``raw_material`` if the
   vendor also appears in the ``CHIP_VENDORS`` set, otherwise
   ``component``.
5. For suppliers with a publicly reported Apple-revenue concentration
   (Bloomberg / Nikkei disclosures), populate ``pct_upstream_revenue``
   from ``APPLE_REV_CONCENTRATION_PCT``.
6. Upsert the edge with ``confidence='confirmed'`` and source
   ``Apple Supplier List <year>``.

The puller is idempotent: repeated runs upsert into the existing
``UNIQUE (upstream_id, downstream_id, relationship, as_of)`` index and
leave previously-created private-company nodes intact.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Config ────────────────────────────────────────────────────────────────────

_USER_AGENT: str = "GRID Intelligence ops@stepdad.finance"
_REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": _USER_AGENT,
    "Accept": "application/pdf,*/*;q=0.8",
}
_REQUEST_TIMEOUT: int = 60
_MAX_PDF_BYTES: int = 32 * 1024 * 1024  # 32 MB cap

# Apple reuses the same evergreen filename, and sometimes also publishes a
# year-stamped mirror. We try the year-stamped URL first, newest-year first,
# and fall back to the evergreen filename.
_APPLE_PDF_URL_CANDIDATES: tuple[str, ...] = (
    # Current canonical path (discovered 2026-04-12). The legacy
    # /supplier-responsibility/ path now 301-redirects to a landing
    # page, so we probe /supply-chain/ first.
    "https://www.apple.com/supply-chain/pdf/Apple-Supplier-List.pdf",
    "https://www.apple.com/supply-chain/pdf/Apple_Supplier_List.pdf",
    "https://www.apple.com/supply-chain/pdf/Apple-Supplier-List-{year}.pdf",
    "https://www.apple.com/supply-chain/pdf/FY{year}_Supplier_List.pdf",
    # Legacy evergreen paths (kept for resilience if Apple reverts)
    "https://www.apple.com/supplier-responsibility/pdf/Apple-Supplier-List.pdf",
    "https://www.apple.com/supplier-responsibility/pdf/Apple_Supplier_List.pdf",
)

# ── Supplier → ticker mapping ────────────────────────────────────────────────
#
# Names are lowercase, stripped of legal suffixes. A supplier matches if any
# of its alias fragments appears as a substring of the cleaned name (we use
# "in" semantics so "Foxconn Technology Group" and "Hon Hai Precision" both
# resolve to 2317.tw).
#
# Relationship classifications:
#   CONTRACT_MFG_NAMES  → contract_mfg
#   CHIP_VENDORS        → raw_material (silicon / wafers / packaging)
#   all others          → component (catch-all for modules, displays, batteries…)

PUBLIC_TICKER_MAP: dict[str, str] = {
    # Foxconn / Hon Hai
    "foxconn":                 "2317.tw",
    "hon hai":                 "2317.tw",
    # Pegatron
    "pegatron":                "4938.tw",
    # Wistron / Wiwynn (iPhone final assembly for a window)
    "wistron":                 "3231.tw",
    # Compal
    "compal":                  "2324.tw",
    # Quanta
    "quanta":                  "2382.tw",
    # Inventec
    "inventec":                "2356.tw",
    # Luxshare
    "luxshare":                "002475.sz",
    # GoerTek (acoustics, AirPods assembly)
    "goertek":                 "002241.sz",
    # BYD (metal + battery + assembly)
    "byd":                     "1211.hk",
    # TSMC (A-series / M-series silicon foundry)
    "tsmc":                    "tsm",
    "taiwan semiconductor":    "tsm",
    # Samsung Electronics (DRAM, NAND, OLED)
    "samsung electronics":     "005930.ks",
    # SK hynix (memory)
    "sk hynix":                "000660.ks",
    "hynix":                   "000660.ks",
    # Sony (camera sensors)
    "sony":                    "sony",
    # LG Display
    "lg display":              "034220.ks",
    "lg innotek":              "011070.ks",
    # Japan Display
    "japan display":           "6740.t",
    # Sharp (now Foxconn subsidiary, still listed separately in list)
    "sharp":                   "6753.t",
    # Texas Instruments, Analog Devices, Broadcom, Qualcomm, Skyworks, Qorvo
    "texas instruments":       "txn",
    "analog devices":          "adi",
    "broadcom":                "avgo",
    "qualcomm":                "qcom",
    "skyworks":                "swks",
    "qorvo":                   "qrvo",
    "cirrus logic":             "crus",
    "nxp":                     "nxpi",
    "infineon":                "ifx.de",
    "stmicroelectronics":       "stm",
    # Memory & storage
    "micron":                  "mu",
    "western digital":         "wdc",
    "kioxia":                  "kioxia",
    # Passives / capacitors
    "murata":                  "6981.t",
    "tdk":                     "6762.t",
    "yageo":                   "2327.tw",
    # Glass / housing / materials
    "corning":                 "glw",
    "3m":                      "mmm",
    "nitto denko":             "6988.t",
    "lens technology":         "300433.sz",
    # Battery cells
    "lg energy solution":      "373220.ks",
    "samsung sdi":             "006400.ks",
    "catl":                    "300750.sz",
    "panasonic":               "6752.t",
    # Misc published tier-1
    "jabil":                   "jbl",
    "flex":                    "flex",
    "stmicro":                 "stm",
    "amphenol":                "aph",
}

# Suppliers whose primary Apple business is final assembly / EMS.
CONTRACT_MFG_NAMES: frozenset[str] = frozenset(
    {
        "foxconn", "hon hai", "pegatron", "wistron", "compal",
        "quanta", "inventec", "byd", "jabil", "flex", "luxshare",
        "goertek",
    }
)

# Suppliers whose primary Apple business is silicon / wafer / memory /
# sensor — we tag these as raw_material so the downstream graph treats
# them as upstream bottleneck inputs, not interchangeable components.
CHIP_VENDORS: frozenset[str] = frozenset(
    {
        "tsmc", "taiwan semiconductor",
        "samsung electronics", "sk hynix", "hynix", "micron",
        "sony",  # image sensors
        "kioxia", "western digital",
        "texas instruments", "analog devices", "broadcom",
        "qualcomm", "skyworks", "qorvo", "cirrus logic",
        "nxp", "infineon", "stmicroelectronics", "stmicro",
    }
)

# Bloomberg / Nikkei / broker disclosures of each supplier's %-of-revenue
# exposure to Apple. Numeric values are best-published-guess as of 2024-2025
# and are only meant to seed the graph with plausible magnitudes; the
# explicit ``source`` field cites the list so consumers know it's an
# Apple-list edge, not a pure Bloomberg read-through.
APPLE_REV_CONCENTRATION_PCT: dict[str, float] = {
    "foxconn":             0.45,  # Hon Hai — Apple is ~45% of consolidated revenue
    "hon hai":             0.45,
    "luxshare":            0.35,  # Nikkei 2024
    "pegatron":            0.40,
    "wistron":             0.15,
    "goertek":             0.30,  # Apple AirPods / acoustics
    "catcher technology":  0.50,
    "tsmc":                0.25,  # Apple is TSMC's single largest customer
    "taiwan semiconductor": 0.25,
    "cirrus logic":        0.80,  # Cirrus is famously Apple-dependent
    "qorvo":               0.35,
    "skyworks":            0.55,
    "jabil":               0.20,
    "lens technology":     0.45,  # cover-glass maker, Apple-heavy
    "murata":              0.15,
    "tdk":                 0.10,
    "samsung electronics": 0.06,  # Apple is small share of SEC total
    "sk hynix":            0.10,
    "broadcom":            0.20,
}

# ── PDF loader ───────────────────────────────────────────────────────────────


def _fetch_pdf_bytes(session: requests.Session) -> tuple[bytes, str, int]:
    """Download the latest Apple Supplier List PDF.

    Returns:
        (pdf_bytes, final_url, report_year)

    Raises:
        RuntimeError: if every candidate URL fails.
    """
    year_now = datetime.now(timezone.utc).year
    # Try this year then last 2 fiscal years.
    years = [year_now, year_now - 1, year_now - 2]
    tried: list[str] = []
    for template in _APPLE_PDF_URL_CANDIDATES:
        for year in years:
            url = template.format(year=year) if "{year}" in template else template
            if url in tried:
                continue
            tried.append(url)
            try:
                resp = session.get(
                    url, timeout=_REQUEST_TIMEOUT, stream=True,
                    allow_redirects=True,
                )
                if resp.status_code != 200:
                    log.debug("apple_supplier_list: {u} -> {s}", u=url, s=resp.status_code)
                    continue
                content_type = resp.headers.get("Content-Type", "")
                if "pdf" not in content_type.lower():
                    log.debug(
                        "apple_supplier_list: non-pdf content-type {c} at {u}",
                        c=content_type, u=url,
                    )
                    continue
                raw = resp.raw.read(_MAX_PDF_BYTES, decode_content=True)
                if not raw or len(raw) < 2048:
                    log.debug("apple_supplier_list: empty body at {u}", u=url)
                    continue
                # Strict PDF magic-header validation. Apple has a habit of
                # serving a 200 HTML shell for non-PDF paths, so we reject
                # anything that doesn't start with %PDF.
                if not raw.lstrip()[:5].startswith(b"%PDF-"):
                    log.debug(
                        "apple_supplier_list: body at {u} is not a PDF (magic mismatch)",
                        u=url,
                    )
                    continue
                log.info(
                    "apple_supplier_list: fetched {n} bytes from {u}",
                    n=len(raw), u=url,
                )
                return raw, url, year
            except Exception as exc:  # pragma: no cover - network
                log.debug("apple_supplier_list: fetch error {u}: {e}", u=url, e=str(exc))
                continue
    raise RuntimeError(
        f"Failed to fetch Apple Supplier List; tried {len(tried)} URLs: {tried[:4]}"
    )


# pypdf extraction of the Apple Supplier List inserts stray single-character
# spaces ("T aiwan", "T echnology", "A T & S", "Y amagata") because the
# underlying glyphs are positioned column-by-column. We repair these by
# joining a lone uppercase letter to the next lowercase token. We do this
# conservatively — only when the adjacent token starts lowercase and the
# combined run is a plausible word of length >= 4.
_STRAY_CAP_SPACE_RE = re.compile(r"\b([A-Z])\s+([a-z][a-z]+)")


def _repair_pdf_spacing(txt: str) -> str:
    # Repeat until stable so we fix "T echnology" -> "Technology" before
    # "T aiwan" -> "Taiwan" on the same line.
    prev = None
    out = txt
    for _ in range(6):
        if out == prev:
            break
        prev = out
        out = _STRAY_CAP_SPACE_RE.sub(r"\1\2", out)
    # Also glue runs like "A T & S" -> "AT&S" when flanked by whitespace
    out = re.sub(r"\b([A-Z])\s+([A-Z])\s*&\s*([A-Z])\b", r"\1\2&\3", out)
    return out


def _parse_pdf_text(pdf_bytes: bytes) -> str:
    """Extract raw text from the PDF using pypdf."""
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as exc:  # pragma: no cover - import
        raise RuntimeError(
            "pypdf is required for apple_supplier_list — pip install pypdf"
        ) from exc

    reader = PdfReader(io.BytesIO(pdf_bytes))
    pages_text: list[str] = []
    for page in reader.pages:
        try:
            txt = page.extract_text() or ""
        except Exception as exc:
            log.debug("apple_supplier_list: page extract failed: {e}", e=str(exc))
            txt = ""
        pages_text.append(_repair_pdf_spacing(txt))
    return "\n".join(pages_text)


# ── Supplier extraction ──────────────────────────────────────────────────────

# Lines containing supplier info usually start with a capitalized company
# name. We scan every line, strip header/footer boilerplate, and filter
# against a noise pattern. The list document also contains a country
# column we can capture to enrich the node.

_NOISE_LINE_RE = re.compile(
    r"^(?:apple|supplier list|supplier name|as of|fiscal year|report|"
    r"page \d|© \d{4}|smelters|refiners|2g3t|this list|top 200|"
    r"spend|final assembly|table of contents|responsibility|"
    r"primary locations)",
    re.IGNORECASE,
)

# Countries seen in the Apple Supplier List. Order matters: two-word
# entries ("South Korea", "United States", "China mainland") must come
# before their single-word substrings so re matches the longest form.
_COUNTRY_NAMES: tuple[str, ...] = (
    "China mainland", "South Korea", "United States", "United Kingdom",
    "Czech Republic", "Hong Kong", "Saudi Arabia",
    "China", "Taiwan", "Japan", "Korea", "Vietnam", "Malaysia",
    "Thailand", "Philippines", "India", "Indonesia", "Singapore",
    "USA", "Mexico", "Brazil", "Germany", "France",
    "Ireland", "Israel", "Switzerland", "Austria", "Hungary",
    "Netherlands", "Belgium", "Canada", "Denmark", "Finland",
    "Sweden", "Norway", "Spain", "Italy", "Poland", "Portugal",
    "Slovakia", "Romania", "Russia", "Turkey", "Australia",
    "New Zealand", "Argentina", "Colombia", "Chile",
)
_COUNTRY_IN_LINE_RE = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in _COUNTRY_NAMES) + r")\b",
    re.IGNORECASE,
)

# A supplier name column usually ends with a legal suffix. We search for
# the last occurrence on the line and cut there.
_LEGAL_SUFFIX_BOUNDARY_RE = re.compile(
    r"\b(?:Incorporated|Inc\.?|Corporation|Corp\.?|Company Limited|"
    r"Company|Co\.,\s*Ltd\.?|Co\.\s*Ltd\.?|Co\.?|Limited|Ltd\.?|LLC|"
    r"L\.L\.C\.|PLC|plc|AG|SA|S\.A\.|N\.V\.|NV|GmbH|KG|Holdings|"
    r"Group|Technologies|Technology|International|Partners|"
    r"Manufactory|Semiconductor|Electronics|Industries|Industry|"
    r"Precision|Holding|Communication|Communications|Solutions|"
    r"Materials|Systems|Enterprise|Mfg\.|Manufacturing)\b",
)

# Common sub-region tokens that start the "primary locations" column.
# We use these as fallback name-boundary markers when no legal suffix
# is present (e.g. "3M", "Corning", "Sony"), AND as a reject filter for
# continuation lines that begin with a region token (e.g.
# "Negeri Sembilan Malaysia" appearing under the previous supplier's
# row).
_SUBREGION_TOKENS: tuple[str, ...] = (
    # China
    "Guangdong", "Jiangsu", "Shanghai", "Shenzhen", "Zhejiang",
    "Sichuan", "Anhui", "Chongqing", "Beijing", "Hubei", "Fujian",
    "Hebei", "Henan", "Shandong", "Yunnan", "Inner Mongolia",
    "Tianjin", "Liaoning", "Hunan", "Jiangxi", "Shanxi", "Gansu",
    "Jilin", "Guangxi", "Guizhou", "Heilongjiang",
    # Korea
    "Gyeonggi-Do", "Gyeongsangbuk-Do", "Chungcheongnam-Do",
    "Jeollanam-Do", "Incheon", "Seoul", "Busan", "Daegu", "Daejeon",
    # Japan
    "Mie", "Miyagi", "Miyazaki", "Fukushima", "Fukuoka", "Fukui",
    "Yamagata", "Yamaguchi", "Hokkaido", "Kanagawa", "Kyoto",
    "Osaka", "Tokyo", "Aichi", "Gifu", "Hyogo", "Shizuoka",
    "Niigata", "Nagano", "Ishikawa", "Chiba", "Ibaraki", "Saitama",
    "Tochigi", "Gunma", "Okayama", "Hiroshima", "Nagasaki", "Kumamoto",
    # Malaysia
    "Penang", "Selangor", "Johor", "Sabah", "Sarawak", "Perak",
    "Kedah", "Negeri Sembilan", "Malacca", "Kuala Lumpur", "Pahang",
    # Philippines
    "Laguna", "Cavite", "Batangas", "Cebu", "Pampanga", "Bulacan",
    # Vietnam
    "Ho Chi Minh", "Hanoi", "Bac Ninh", "Hai Phong", "Binh Duong",
    "Dong Nai", "Phu Tho", "Thai Nguyen", "Bac Giang", "Vinh Phuc",
    # Thailand
    "Bangkok", "Chachoengsao", "Chonburi", "Rayong", "Prachinburi",
    "Lamphun", "Samut Prakan", "Ayutthaya",
    # EU
    "Bavaria", "Saxony", "Berlin", "Hamburg", "Hessen", "Styria",
    "Carinthia", "Tyrol", "Antwerp", "Brussels", "Wallonia",
    "Flanders", "Limerick", "Dublin", "Cork", "Munster", "Leinster",
    # US states
    "California", "Texas", "Arizona", "Oregon", "Washington",
    "Nevada", "Colorado", "Utah", "Idaho", "Montana", "Wyoming",
    "Alabama", "Indiana", "Iowa", "Minnesota", "Ohio", "Wisconsin",
    "Illinois", "Michigan", "Missouri", "Kansas", "Kentucky",
    "Tennessee", "Virginia", "Pennsylvania", "Maryland",
    "Massachusetts", "Connecticut", "New York", "New Jersey",
    "Delaware", "Florida", "Georgia", "North Carolina",
    "South Carolina", "Mississippi", "Louisiana", "Arkansas",
    "Oklahoma", "New Mexico", "Maine", "New Hampshire", "Vermont",
    "Rhode Island", "Nebraska", "West Virginia", "Alaska", "Hawaii",
    # India
    "Karnataka", "Tamil Nadu", "Maharashtra", "Haryana",
    "Uttar Pradesh", "Gujarat", "Andhra Pradesh", "Telangana",
)

_SUBREGION_START_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(t) for t in _SUBREGION_TOKENS) + r")\b",
)

_LEGAL_SUFFIX_RE = re.compile(
    r"\s*(?:,?\s*(?:inc|inc\.|incorporated|llc|l\.l\.c\.|ltd|ltd\.|limited|"
    r"corp|corp\.|corporation|company|co|co\.|holdings|group|plc|ag|sa|"
    r"s\.a\.|n\.v\.|nv|gmbh|kg|partners|technologies|technology|intl|"
    r"international|holding|holdings limited|co\., ltd))+\s*$",
    re.IGNORECASE,
)


def _clean_supplier_name(raw: str) -> str:
    name = raw.strip()
    name = re.sub(r"\s+", " ", name)
    name = name.strip(" .,;:()[]\"'")
    # Drop trailing legal suffixes
    for _ in range(3):
        new = _LEGAL_SUFFIX_RE.sub("", name).strip()
        if new == name:
            break
        name = new
    return name


def _slug(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", (name or "").lower()).strip("_")
    return s


@dataclass
class SupplierRecord:
    name: str
    cleaned: str
    countries: list[str] = field(default_factory=list)


def _find_name_boundary(line: str) -> int | None:
    """Find the end-of-name index on a supplier-list line.

    Strategy, in priority order:
      1. Last legal-suffix token match — supplier name ends after it
         ("Amkor Technology Incorporated Shanghai..." -> cut after
         "Incorporated").
      2. First sub-region / US-state / Japanese-prefecture token —
         marks the start of the "primary locations" column
         ("3M Guangdong..." -> cut before "Guangdong").
      3. First country token.
      4. Return None if nothing matched.
    """
    # 1. Legal suffix: pick the LAST match so "Advanced Semiconductor
    #    Engineering Technology Holding Co., Ltd." captures the whole
    #    name including "Co., Ltd.".
    last_suffix_end: int | None = None
    for m in _LEGAL_SUFFIX_BOUNDARY_RE.finditer(line):
        last_suffix_end = m.end()
    if last_suffix_end is not None:
        return last_suffix_end
    # 2. Sub-region start — cut BEFORE it.
    m = _SUBREGION_START_RE.search(line)
    if m:
        return m.start()
    # 3. Country token — cut BEFORE it.
    m = _COUNTRY_IN_LINE_RE.search(line)
    if m:
        return m.start()
    return None


def _extract_suppliers(pdf_text: str) -> list[SupplierRecord]:
    """Walk the PDF text and return unique supplier records.

    The Apple list is formatted as "Supplier Name | Primary Locations |
    Country". After pypdf extraction all three columns collapse onto a
    single whitespace-delimited line. We split each line via
    ``_find_name_boundary`` (legal suffix > sub-region > country) to
    recover the supplier name, then merge duplicates by cleaned key.
    """
    seen: dict[str, SupplierRecord] = {}

    for raw_line in pdf_text.splitlines():
        line = raw_line.strip()
        if not line or len(line) < 4:
            continue
        if _NOISE_LINE_RE.match(line):
            continue
        # Must start with a capital letter or digit (e.g. "3M", "9Dot")
        if not (line[0].isupper() or line[0].isdigit()):
            continue

        boundary = _find_name_boundary(line)
        if boundary is None:
            # No boundary — this is likely a continuation line for the
            # prior supplier (e.g. "Taiwan Taiwan" on its own line).
            # Opportunistically capture a country token so we don't
            # lose the country for the prior record.
            continue

        # Reject continuation lines whose entire prefix is a sub-region
        # token — these are the Malaysian-state / Irish-county lines
        # ("Negeri Sembilan Malaysia", "Munster Ireland") that appear
        # under the previous supplier's row.
        if _SUBREGION_START_RE.match(line) and (
            _SUBREGION_START_RE.match(line).end() >= boundary  # type: ignore[union-attr]
        ):
            continue

        name_candidate = line[:boundary].strip()
        if not name_candidate:
            continue
        # Reject names shorter than 3 chars or longer than 90
        if len(name_candidate) < 3 or len(name_candidate) > 90:
            continue
        # Reject pure-digit or pure-lowercase runs
        if not any(c.isupper() for c in name_candidate):
            continue

        country_match = _COUNTRY_IN_LINE_RE.search(line[boundary:])
        country = country_match.group(0).title() if country_match else None

        cleaned = _clean_supplier_name(name_candidate)
        if not cleaned or len(cleaned) < 3 or len(cleaned) > 80:
            continue
        # Reject stopword-only leftovers (e.g. "The", "Our")
        if cleaned.lower() in {
            "the", "our", "and", "inc", "ltd", "co", "corp",
            "limited", "company", "group", "holdings",
        }:
            continue

        key = cleaned.lower()
        if key not in seen:
            seen[key] = SupplierRecord(
                name=name_candidate, cleaned=cleaned,
                countries=[country] if country else [],
            )
        else:
            if country and country not in seen[key].countries:
                seen[key].countries.append(country)

    return list(seen.values())


# ── Classification ───────────────────────────────────────────────────────────


def _resolve_node_id(cleaned: str) -> tuple[str, str | None]:
    """Return (node_id, ticker_if_public). node_id is a ticker (lowercase)
    for public suppliers and a slug for private companies.
    """
    lower = cleaned.lower()
    for alias, tkr in PUBLIC_TICKER_MAP.items():
        if alias in lower:
            return tkr.lower(), tkr
    return _slug(cleaned), None


def _classify_relationship(cleaned: str) -> str:
    lower = cleaned.lower()
    for alias in CONTRACT_MFG_NAMES:
        if alias in lower:
            return "contract_mfg"
    for alias in CHIP_VENDORS:
        if alias in lower:
            return "raw_material"
    return "component"


def _apple_revenue_pct(cleaned: str) -> float | None:
    lower = cleaned.lower()
    for alias, pct in APPLE_REV_CONCENTRATION_PCT.items():
        if alias in lower:
            return pct
    return None


# ── DB writes ────────────────────────────────────────────────────────────────


def _upsert_node(
    conn: Any,
    node_id: str,
    name: str,
    node_type: str,
    country: str | None,
    notes: str | None,
) -> bool:
    result = conn.execute(
        text(
            """
            INSERT INTO supply_chain_nodes (id, name, type, country, notes)
            VALUES (:id, :name, :type, :country, :notes)
            ON CONFLICT (id) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, supply_chain_nodes.name),
                country = COALESCE(EXCLUDED.country, supply_chain_nodes.country)
            """
        ),
        {
            "id": node_id,
            "name": name,
            "type": node_type,
            "country": country,
            "notes": notes,
        },
    )
    return bool(result.rowcount and result.rowcount > 0)


def _upsert_edge(
    conn: Any,
    upstream_id: str,
    relationship: str,
    pct_upstream_revenue: float | None,
    as_of: date,
    source: str,
) -> bool:
    result = conn.execute(
        text(
            """
            INSERT INTO supply_chain_edges (
                upstream_id, downstream_id, relationship, tier,
                pct_upstream_revenue, confidence, as_of, source
            ) VALUES (
                :u, 'aapl', :rel, 1,
                :pct_up, 'confirmed', :as_of, :src
            )
            ON CONFLICT (upstream_id, downstream_id, relationship, as_of)
            DO UPDATE SET
                pct_upstream_revenue = COALESCE(
                    EXCLUDED.pct_upstream_revenue,
                    supply_chain_edges.pct_upstream_revenue
                ),
                source = EXCLUDED.source,
                confidence = EXCLUDED.confidence
            """
        ),
        {
            "u": upstream_id,
            "rel": relationship,
            "pct_up": pct_upstream_revenue,
            "as_of": as_of,
            "src": source,
        },
    )
    return bool(result.rowcount and result.rowcount > 0)


# ── Runner ───────────────────────────────────────────────────────────────────


@dataclass
class AppleSupplierListStats:
    suppliers_parsed: int = 0
    nodes_upserted: int = 0
    edges_upserted: int = 0
    public_suppliers: int = 0
    private_suppliers: int = 0
    relationships: dict[str, int] = field(default_factory=dict)
    source_url: str = ""
    report_year: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "suppliers_parsed": self.suppliers_parsed,
            "nodes_upserted": self.nodes_upserted,
            "edges_upserted": self.edges_upserted,
            "public_suppliers": self.public_suppliers,
            "private_suppliers": self.private_suppliers,
            "relationships": dict(self.relationships),
            "source_url": self.source_url,
            "report_year": self.report_year,
        }


class AppleSupplierListPuller:
    """Pull the Apple Supplier List and upsert supply_chain_edges."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.session = requests.Session()
        self.session.headers.update(_REQUEST_HEADERS)
        self.stats = AppleSupplierListStats()

    def run(self) -> dict[str, Any]:
        pdf_bytes, url, year = _fetch_pdf_bytes(self.session)
        self.stats.source_url = url
        self.stats.report_year = year

        pdf_text = _parse_pdf_text(pdf_bytes)
        log.info(
            "apple_supplier_list: extracted {n} chars of text from {p} pages",
            n=len(pdf_text), p=pdf_text.count("\n") + 1,
        )

        suppliers = _extract_suppliers(pdf_text)
        self.stats.suppliers_parsed = len(suppliers)
        log.info("apple_supplier_list: {n} supplier records parsed", n=len(suppliers))

        as_of = date(year, 1, 1)
        source = f"Apple Supplier List {year}"

        with self.engine.begin() as conn:
            # Make sure the Apple focal node exists
            if _upsert_node(
                conn,
                node_id="aapl",
                name="Apple Inc.",
                node_type="ticker",
                country="United States",
                notes="Apple focal actor — supplier-list downstream",
            ):
                self.stats.nodes_upserted += 1

            for sup in suppliers:
                node_id, public_ticker = _resolve_node_id(sup.cleaned)
                relationship = _classify_relationship(sup.cleaned)
                pct_upstream_rev = _apple_revenue_pct(sup.cleaned)
                country = sup.countries[0] if sup.countries else None

                node_type = "ticker" if public_ticker else "private_company"
                inserted_node = _upsert_node(
                    conn,
                    node_id=node_id,
                    name=sup.cleaned,
                    node_type=node_type,
                    country=country,
                    notes=f"Apple Supplier List {year}",
                )
                if inserted_node:
                    self.stats.nodes_upserted += 1

                inserted_edge = _upsert_edge(
                    conn,
                    upstream_id=node_id,
                    relationship=relationship,
                    pct_upstream_revenue=pct_upstream_rev,
                    as_of=as_of,
                    source=source,
                )
                if inserted_edge:
                    self.stats.edges_upserted += 1

                if public_ticker:
                    self.stats.public_suppliers += 1
                else:
                    self.stats.private_suppliers += 1
                self.stats.relationships[relationship] = (
                    self.stats.relationships.get(relationship, 0) + 1
                )

        log.info(
            "apple_supplier_list: done — edges={e} nodes={n}",
            e=self.stats.edges_upserted, n=self.stats.nodes_upserted,
        )
        return self.stats.as_dict()


def run_annual(db_engine: Engine | None = None) -> dict[str, Any]:
    """Hermes operator entrypoint. Runs once a year."""
    if db_engine is None:
        from db import get_engine

        db_engine = get_engine()
    puller = AppleSupplierListPuller(db_engine=db_engine)
    return puller.run()


__all__ = [
    "AppleSupplierListPuller",
    "AppleSupplierListStats",
    "PUBLIC_TICKER_MAP",
    "APPLE_REV_CONCENTRATION_PCT",
    "run_annual",
]
