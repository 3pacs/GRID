"""The single verified 13F filer-CIK map (GD0 §1.3 / §6 item 4 remediation).

**Coordinator correction, 2026-09-28** (supersedes the first cut of this
remediation in PR #711's earlier revision): that revision retired the two
contradictory maps in ``ingestion/edgar.py`` and
``ingestion/altdata/institutional_flows.py`` by deriving both from
``ingestion/altdata/sec_13f_live.py``'s existing ``FILERS`` alone. That
removed the contradictions but also silently *shrank* the tracked universe
from ~50 (union of the three old lists) to 35 -- dropping real, still-active
managers (Vanguard, State Street, T. Rowe Price, Fidelity, Temasek, GIC,
Norges Bank, and others) that the owner never asked to stop tracking. The
owner's decision was "remove the contradictions," not "shrink coverage to
whichever of the three lists happened to be internally consistent."

This module fixes that by rebuilding the map from the **union** of the three
old lists (edgar.py's 50-entry ``TOP_HEDGE_FUND_CIKS``, institutional_flows
.py's 49-entry ``TOP_13F_FILERS``, and sec_13f_live.py's original 35-entry
``FILERS``) and checking **every** union CIK against SEC's own registrant
data before including it:

    * Fetched ``https://data.sec.gov/submissions/CIK##########.json`` for
      all 99 distinct CIKs in the union (UA ``"GRID Intelligence
      ops@stepdad.finance"``, ~6.7 req/s, under the 8 req/s ceiling).
    * A CIK is **kept** only when SEC's registrant name plausibly matches
      the manager label it was filed under in at least one of the three old
      lists (30 of 99 union CIKs verified this way on the first pass).
    * Every CIK that did **not** match -- including 5 that 404'd outright,
      i.e. the CIK doesn't exist at all -- was treated as "this manager is
      under a wrong CIK," and its correct CIK was located via SEC's own
      company-name search (``browse-edgar`` atom feed,
      ``https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany``) and
      re-verified against ``data.sec.gov/submissions``.
    * Two managers could not be matched to any SEC-registered filer after
      multiple search variants each (Balyasny Asset Management, GIC Private
      Limited) and are **dropped**, not guessed -- see
      ``DROPPED_FILERS`` below and ``verified_13f_filers_evidence.json``
      for the exact terms tried.

**A finding beyond the original audit's scope:** the pre-existing
``sec_13f_live.py`` map -- the one the owner had separately confirmed
correct -- itself had Baupost Group's and Lone Pine Capital's CIKs
**swapped** (CIK 1061165, labelled "Baupost Group," is actually Lone Pine
Capital LLC per SEC; CIK 1061768, labelled "Lone Pine Capital," is actually
Baupost Group LLC/MA). This map corrects that too.

Full evidence -- every CIK checked, the SEC name returned, and the fetch
date -- is in ``verified_13f_filers_evidence.json`` next to this module.

**BlackRock note:** CIK ``2012383`` here is BlackRock's **13F-filer** CIK
(confirmed live: SEC name "BlackRock, Inc."). This is a different
identifier space from BlackRock Inc.'s **issuer** CIK ``1364742`` (used for
BLK-the-stock's own SEC filings, and cited in
``intelligence/actor_identity.py``'s docstring for that reason) -- the two
were never actually in contradiction once the identifier spaces are kept
separate, per GD0's own framing. Fetched fresh 2026-09-28: CIK ``1364742``
resolves to "BlackRock Finance, Inc.", a related-but-distinct subsidiary,
not the same filer as ``2012383``.

``ingestion/edgar.py``, ``ingestion/altdata/institutional_flows.py``, and
``ingestion/altdata/sec_13f_live.py`` all derive their filer lookups from
``VERIFIED_FILERS`` in this module -- none of them keeps an independent
copy.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Filer:
    """Metadata for a tracked 13F filer.

    Attributes:
        key: Short slug used in CLI selection and logs.
        cik: SEC Central Index Key (unpadded string form).
        display_name: SEC's own registrant name for this CIK (from
            ``data.sec.gov/submissions``), used as the canonical
            ``institutional_holdings.holder_name`` -- not an approximation
            or a pre-verification label.
    """

    key: str
    cik: str
    display_name: str


# Verified 2026-09-28 against data.sec.gov/submissions + SEC company-name
# search. See the module docstring and verified_13f_filers_evidence.json for
# methodology and full evidence.
VERIFIED_FILERS: tuple[Filer, ...] = (
    Filer('berkshire_hathaway', '1067983', 'Berkshire Hathaway Inc'),
    Filer('bridgewater', '1350694', 'Bridgewater Associates, LP'),
    Filer('citadel', '1423053', 'Citadel Advisors LLC'),
    Filer('renaissance', '1037389', 'Renaissance Technologies LLC'),
    Filer('de_shaw', '1009207', 'D. E. Shaw & Co., Inc.'),
    Filer('millennium', '1273087', 'Millennium Management LLC'),
    Filer('point72', '1603466', 'Point72 Asset Management, L.P.'),
    Filer('tiger_global', '1167483', 'Tiger Global Management LLC'),
    Filer('aqr', '1167557', 'AQR Capital Management LLC'),
    Filer('viking_global', '1103804', 'Viking Global Investors LP'),
    Filer('elliott_management', '1791786', 'Elliott Investment Management L.P.'),
    Filer('third_point', '1040273', 'Third Point LLC'),
    Filer('baupost', '1061768', 'Baupost Group LLC/MA'),
    Filer('pershing_square', '1336528', 'Pershing Square Capital Management, L.P.'),
    Filer('coatue', '1135730', 'Coatue Management LLC'),
    Filer('lone_pine', '1061165', 'Lone Pine Capital LLC'),
    Filer('appaloosa', '1006438', 'Appaloosa Management LP'),
    Filer('greenlight', '1079114', 'Greenlight Capital Inc'),
    Filer('jana_partners', '1159159', 'Jana Partners LLC'),
    Filer('starboard_value', '1517137', 'Starboard Value LP'),
    Filer('soros_fund', '1029160', 'Soros Fund Management LLC'),
    Filer('sculptor_ochziff', '1403256', 'Sculptor Capital Management, Inc.'),
    Filer('valueact', '1418814', 'ValueAct Holdings, L.P.'),
    Filer('icahn', '921669', 'Icahn Carl C'),
    Filer('duquesne', '1536411', 'Duquesne Family Office LLC'),
    Filer('msd_capital', '1105497', 'MSD Capital, L.P.'),
    Filer('vanguard', '102909', 'Vanguard Group Inc'),
    Filer('state_street', '93751', 'State Street Corp'),
    Filer('t_rowe_price', '1897612', 'T. Rowe Price Investment Management, Inc.'),
    Filer('wellington', '902219', 'Wellington Management Group LLP'),
    Filer('blackrock', '2012383', 'BlackRock, Inc.'),
    Filer('trian', '1345471', 'Trian Fund Management, L.P.'),
    Filer('3g_capital', '1421669', '3G Capital Partners LP'),
    Filer('sequoia_capital', '1607841', 'SC US (TTGP), LTD.'),
    Filer('altimeter', '1541617', 'Altimeter Capital Management, LP'),
    Filer('baillie_gifford', '1088875', 'Baillie Gifford & Co'),
    Filer('capital_research_global', '1422848', 'Capital Research Global Investors'),
    Filer('geode_capital', '1214717', 'Geode Capital Management, LLC'),
    Filer('two_sigma', '1478735', 'Two Sigma Advisers, LP'),
    Filer('marshall_wace', '1318757', 'Marshall Wace, LLP'),
    Filer('canyon_capital', '1074034', 'Canyon Capital Advisors LLC'),
    Filer('glenview_capital', '1138995', 'Glenview Capital Management, LLC'),
    Filer('farallon', '909661', 'Farallon Capital Management, L.L.C.'),
    Filer('paulson', '1035674', 'Paulson & Co. Inc.'),
    Filer('maverick_capital', '934639', 'Maverick Capital Ltd'),
    Filer('discovery_capital', '1389507', 'Discovery Capital Management, LLC / CT'),
    Filer('dragoneer', '1602189', 'Dragoneer Investment Group, LLC'),
    Filer('matrix_capital', '1410830', 'Matrix Capital Management Company, LP'),
    Filer('anchorage_capital', '1300714', 'Anchorage Capital Group, L.L.C.'),
    Filer('york_capital', '1480532', 'York Capital Management Global Advisors, LLC'),
    Filer('cerberus', '1525907', 'Cerberus Capital Management, L.P.'),
    Filer('omega_advisors', '898202', 'Omega Advisors Inc.'),
    Filer('highfields_capital', '1079563', 'Highfields Capital Management LP'),
    Filer('senator_investment', '1443689', 'Senator Investment Group LP'),
    Filer('marcato_capital', '1541996', 'Marcato Capital Management LP'),
    Filer('eton_park', '1314588', 'Eton Park Capital Management, L.P.'),
    Filer('gmo', '1352662', 'Grantham, Mayo, Van Otterloo & Co. LLC'),
    Filer('winton_group', '1612063', 'Winton Group Ltd'),
    Filer('cadian_capital', '1423686', 'Cadian Capital Management, LP'),
    Filer('tudor_investment', '923093', 'Tudor Investment Corp Et Al'),
    Filer('whale_rock', '1387322', 'Whale Rock Capital Management LLC'),
    Filer('capital_group', '732812', 'Capital Group Companies, Inc.'),
    Filer('temasek', '1021944', 'Temasek Holdings (Private) Ltd'),
    Filer('norges_bank', '1374170', 'Norges Bank'),
    Filer('man_group', '1637460', 'Man Group plc'),
    Filer('exoduspoint', '1736225', 'ExodusPoint Capital Management, LP'),
    Filer('ares_management', '1259313', 'Ares Management LLC'),
    Filer('fidelity_fmr', '1145247', 'FMR Co Inc'),
    Filer('magnetar', '1352851', 'Magnetar Financial LLC'),
    Filer('kingdon_capital', '1000097', 'Kingdon Capital Management, L.L.C.'),
    Filer('king_street_capital', '1218199', 'King Street Capital Management, L.P.'),
    Filer('jpmorgan_investment_mgmt', '1363391', 'J.P. Morgan Investment Management Inc.'),
)

# Managers from the old union that could NOT be matched to any SEC-registered
# 13F filer after multiple search variants -- dropped per the coordinator's
# "find its correct CIK from SEC, or drop it with a note" instruction, never
# guessed. See verified_13f_filers_evidence.json for the exact terms tried.
DROPPED_FILERS: tuple[dict, ...] = (
    {
        "manager_label": "Balyasny Asset Management",
        "old_cik": "1534067",
        "old_sec_name_for_old_cik": "Haer Gary",
        "searched_terms": (
            "Balyasny Asset Management", "Balyasny Asset", "Balyasny",
            "Balyasny Asset Management LLC",
        ),
    },
    {
        "manager_label": "GIC Private Limited",
        "old_cik": "1599901",
        "old_sec_name_for_old_cik": "Avidity Biosciences, Inc.",
        "searched_terms": (
            "GIC Private Limited", "Government of Singapore",
            "Government of Singapore Investment", "GIC", "GIC (Ventures)",
            "GIC Pte",
        ),
    },
)


def filer_by_key(key: str) -> Filer | None:
    """Look up a filer by its short slug."""
    for f in VERIFIED_FILERS:
        if f.key == key:
            return f
    return None


def filer_by_cik(cik: str | int) -> Filer | None:
    """Look up a filer by CIK (unpadded or zero-padded, str or int)."""
    try:
        target = int(str(cik).strip())
    except (TypeError, ValueError):
        return None
    for f in VERIFIED_FILERS:
        if int(f.cik) == target:
            return f
    return None
