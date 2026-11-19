"""Apple Supplier List names map to the right company (whole-word aliases)."""
from __future__ import annotations

import pytest

from ingestion.altdata import apple_supplier_list as a


@pytest.mark.parametrize("name, node, relationship", [
    ("Micron", "mu", "raw_material"),
    ("Unimicron", "3037.tw", "component"),  # once Micron's symbol
    ("Flex", "flex", "contract_mfg"),
    ("Flexium Interconnect", "6269.tw", "component"),  # once Flex's symbol
    ("Hon Hai Precision Industry", "2317.tw", "contract_mfg"),
    ("SK hynix", "000660.ks", "raw_material"),
])
def test_look_alike_names_resolve_to_their_own_company(name, node, relationship):
    assert a._resolve_node_id(name)[0] == node
    assert a._classify_relationship(name) == relationship


def test_an_alias_inside_a_longer_word_does_not_match():
    assert not a._mentions("micron", "unimicron")
    assert not a._mentions("flex", "flexium interconnect")
    assert a._mentions("sk hynix", "sk hynix inc.")
    assert a._mentions("3m", "3m company")
