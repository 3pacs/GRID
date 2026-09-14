"""Every feature name ``physics/momentum.py`` reads must have a writer.

The news card on grid.stepdad.finance is fed by
``physics/momentum.py::NewsMomentumAnalyzer``, which looks its feature names
up in ``feature_registry`` by literal string. A name with a typo, or a name
left behind by a rename, does not raise: ``_resolve_feature_ids()`` returns
``{}`` and the analyzer degrades to "No price features available" or an
``available: false`` result. The card empties and nothing anywhere says why.

``PRICE_FEATURES = ["sp500_close"]`` was exactly that. Nothing in the
repository maps any raw series onto ``sp500_close`` — ``entity_map.py:27``
maps ``YF:^GSPC:close`` to ``sp500_full`` and says so in its own comment
("TYPO-FIX: sp500_close not in registry, sp500_full exists") — so the
sentiment/price cross-correlation had never once run.

What this guards is the half that can be checked without a database: a
feature name is only ever written to ``resolved_series`` by
``normalization/resolver.py``, which gets its target names from
``entity_map.py``. A name that is not a mapping target there has no writer,
whatever ``feature_registry`` happens to contain on any given host — so
membership in the mapping targets is a necessary condition, and it is a
static one.

The other half — that the name is also *registered* — is not statically
checkable, because ``feature_registry`` is live database state and some
targets (``sp500_full`` among them) were registered by hand rather than by
anything in this tree. That half is guarded on the migration side, by the
alembic revisions that INSERT these rows and by
``tests/test_migration_feature_registry_values.py``.
"""

from __future__ import annotations

import pytest

from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS
from physics.momentum import (
    GDELT_ACTOR_TONE_FEATURES,
    GDELT_TENSION_FEATURES,
    PRICE_FEATURES,
)


def _mapping_targets() -> set[str]:
    """Every feature name some raw series_id maps onto.

    Both dicts, because ``EntityMap.load_v2_mappings()`` merges
    ``NEW_MAPPINGS_V2`` into ``SEED_MAPPINGS`` at construction time — a
    module-global mutation that has not happened yet at import.
    """
    return set(SEED_MAPPINGS.values()) | set(NEW_MAPPINGS_V2.values())


@pytest.mark.unit
def test_mapping_targets_are_non_empty() -> None:
    """If this ever finds nothing, entity_map moved and the guard is blind."""
    assert len(_mapping_targets()) > 100


@pytest.mark.unit
@pytest.mark.parametrize(
    "feature_name",
    sorted(set(PRICE_FEATURES) | set(GDELT_ACTOR_TONE_FEATURES) | set(GDELT_TENSION_FEATURES)),
)
def test_momentum_feature_names_have_a_writer(feature_name: str) -> None:
    targets = _mapping_targets()
    assert feature_name in targets, (
        f"physics/momentum.py reads {feature_name!r}, but no series_id in "
        "normalization/entity_map.py maps onto it, so the resolver can never "
        "write a resolved_series row under that name. The analyzer will not "
        "raise -- it will silently report the corresponding section of the "
        "news card as unavailable. Either point the constant at the name "
        "entity_map actually targets, or add the mapping."
    )
