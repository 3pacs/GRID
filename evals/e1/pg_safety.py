"""Where the E1 PostgreSQL gates may create and drop scratch schemas.

Never on a production cluster database. The fixture uses
``GRID_TEST_DB_URL`` only. There is no default URL, because a default of
``grid_user@localhost/grid`` would, on grid-svr, point at production. A
database whose name is a known production name, or starts with ``grid``
without ending in ``_test``, is refused outright.
"""

from __future__ import annotations

from sqlalchemy.engine import make_url

#: Production database names across the fleet (griddb on grid-svr, grid in
#: config.Settings' default, the Obsidian mirror, legacy names).
PRODUCTION_DB_NAMES = frozenset({"grid", "griddb", "grid_obsidian", "grid_v4", "gridprod", "postgres"})


class ProductionDatabaseRefused(RuntimeError):
    """The URL names a production database; E1 gates never write scratch schemas there."""


def check_scratch_target(url: str) -> str:
    """Return the database name if E1 may use it for scratch schemas, else raise."""
    name = (make_url(url).database or "").strip().lower()
    if not name:
        raise ProductionDatabaseRefused("GRID_TEST_DB_URL names no database")
    if name.endswith("_test"):
        return name
    if name in PRODUCTION_DB_NAMES or name.startswith("grid"):
        raise ProductionDatabaseRefused(
            f"refusing to create/drop E1 scratch schemas in database {name!r}: it is (or looks like) a "
            "production database. Point GRID_TEST_DB_URL at a disposable *_test database."
        )
    return name
