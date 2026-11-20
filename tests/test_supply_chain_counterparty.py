"""10-K counterparty names resolve to an existing node across legal forms."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ingestion.altdata import supply_chain_8k as s8
from ingestion.altdata import supply_chain_parser as scp

CATALOG = [
    ("wmt", "Walmart", "ticker"),
    ("mrk", "Merck & Co", "ticker"),
    ("mck", "McKesson", "ticker"),
    ("acme_widgets", "Acme Widgets LLC", "private_company"),
]


def parser(rows=CATALOG):
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = rows
    return scp.SupplyChain10KParser(db_engine=engine)


@pytest.mark.parametrize("raw, node", [
    ("Walmart Inc.", "wmt"),
    ("Walmart, Inc. and its affiliates", "wmt"),
    ("Wal-Mart Stores, Inc.", "wmt"),
    ("Merck & Co., Inc.", "mrk"),
    ("McKesson Corporation", "mck"),
    ("Acme Widgets, Inc.", "acme_widgets"),
])
def test_other_legal_forms_find_the_existing_node(raw, node):
    assert parser()._resolve_counterparty(raw) == (node, None)


def test_a_listed_node_beats_an_unlisted_one_with_the_same_name():
    p = parser(CATALOG + [("walmart_inc", "Walmart Inc", "private_company")])
    assert p._resolve_counterparty("Walmart Inc") == ("wmt", None)


def test_two_listed_nodes_with_one_key_resolve_neither():
    p = parser([("abc", "Alpha Inc", "ticker"), ("abcd", "Alpha Corp", "ticker")])
    nid, new = p._resolve_counterparty("Alpha Holdings")
    assert nid == "alpha_holdings" and new is not None and new.type == "private_company"


def test_an_unknown_company_still_becomes_a_new_private_node():
    nid, new = parser()._resolve_counterparty("Zenith Gearworks Inc.")
    assert nid == "zenith_gearworks_inc" and new.type == "private_company"


def test_the_8k_job_and_the_10k_parser_share_one_normaliser():
    assert s8.company_key is scp.company_key
    assert scp.company_key("The Boeing Company") == scp.company_key("BOEING CO") == "boeing"
