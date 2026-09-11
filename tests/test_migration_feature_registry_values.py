"""Migrations that INSERT into feature_registry must satisfy its CHECK constraints.

Migration 0063 shipped with normalization='NONE'. That is not in the allowed
set, so psql rejected the whole multi-row INSERT and registered nothing --
the migration merged, ran, and silently accomplished zero. Nothing caught it
before merge because the value is only validated by the database.

These constraints are stable schema, verified against griddb on 2026-09-11:

    feature_registry_normalization_check
        normalization IN (ZSCORE, MINMAX, RAW, RANK)
    feature_registry_missing_data_policy_check
        missing_data_policy IN (FORWARD_FILL, INTERPOLATE, NAN)
    feature_registry_family_check
        family IN (rates, credit, breadth, vol, fx, commodity, sentiment,
                   macro, earnings, crypto, equity, alternative, systemic,
                   trade, flows)
    chk_transformation_version_positive
        transformation_version >= 1

Parsing SQL with a regex is crude, but the alternative is a live database in
CI, and the failure this guards against is exactly the kind a unit test can
see: a literal that is not in a fixed set.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations"

NORMALIZATION = {"ZSCORE", "MINMAX", "RAW", "RANK"}
MISSING_DATA_POLICY = {"FORWARD_FILL", "INTERPOLATE", "NAN"}
FAMILY = {
    "rates", "credit", "breadth", "vol", "fx", "commodity", "sentiment",
    "macro", "earnings", "crypto", "equity", "alternative", "systemic",
    "trade", "flows",
}

# ('name', 'family', 'description', 'transformation',
#  version, lag, 'normalization', 'missing_data_policy', 'date', bool)
_ROW = re.compile(
    r"\(\s*'(?P<name>[^']*)'\s*,\s*'(?P<family>[^']*)'\s*,\s*'(?:[^']|'')*'\s*,"
    r"\s*'(?:[^']|'')*'\s*,\s*(?P<version>\d+)\s*,\s*-?\d+\s*,"
    r"\s*'(?P<normalization>[^']*)'\s*,\s*'(?P<missing>[^']*)'\s*,",
    re.MULTILINE,
)


def _files_inserting_into_feature_registry() -> list[Path]:
    out = []
    for p in sorted(MIGRATIONS.glob("*.sql")):
        text = p.read_text(errors="replace")
        if re.search(r"INSERT\s+INTO\s+feature_registry", text, re.IGNORECASE):
            out.append(p)
    return out


def test_at_least_one_migration_is_checked() -> None:
    """If this ever finds nothing, the regex has drifted and the suite is blind."""
    assert _files_inserting_into_feature_registry(), "no feature_registry inserts found"


@pytest.mark.parametrize(
    "path", _files_inserting_into_feature_registry(), ids=lambda p: p.name
)
def test_feature_registry_rows_satisfy_check_constraints(path: Path) -> None:
    rows = list(_ROW.finditer(path.read_text(errors="replace")))
    assert rows, f"{path.name} inserts into feature_registry but no row parsed"

    for m in rows:
        name = m.group("name")
        assert m.group("normalization") in NORMALIZATION, (
            f"{path.name}: {name} has normalization="
            f"{m.group('normalization')!r}; allowed {sorted(NORMALIZATION)}"
        )
        assert m.group("missing") in MISSING_DATA_POLICY, (
            f"{path.name}: {name} has missing_data_policy="
            f"{m.group('missing')!r}; allowed {sorted(MISSING_DATA_POLICY)}"
        )
        assert m.group("family") in FAMILY, (
            f"{path.name}: {name} has family={m.group('family')!r}; "
            f"allowed {sorted(FAMILY)}"
        )
        assert int(m.group("version")) >= 1, (
            f"{path.name}: {name} has transformation_version < 1"
        )


def test_the_guard_rejects_the_value_that_shipped() -> None:
    """The regression itself: normalization='NONE' must not pass."""
    bad = (
        "('x', 'fx', 'd', 'RAW', 1, 0, 'NONE', 'FORWARD_FILL', '2026-01-01', TRUE)"
    )
    m = _ROW.search(bad)
    assert m is not None, "guard regex no longer matches a standard row"
    assert m.group("normalization") not in NORMALIZATION
