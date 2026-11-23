"""``feature_registry`` name -> id lookups for direct ``resolved_series`` readers.

A ``resolved_series`` read that finds its feature through
``JOIN feature_registry fr ON rs.feature_id = fr.id WHERE fr.name = :n``
hides the feature id from the planner until execution. PostgreSQL then plans
for an average feature (about 5.5k rows on griddb, 2026-11), so a
``ORDER BY rs.obs_date DESC LIMIT 1`` read walks every vintage of a large
feature and sorts it to keep one row: vix_spot has 375k rows,
ofr_financial_stress 1.03M, spy_full 225k. With the id bound as a value
(psycopg2 interpolates parameters on the client, so the server sees a
literal), the planner uses that feature's statistics and reads the newest
rows first from the ``(feature_id, obs_date)`` index. griddb, 2026-11-23:
120-970 ms per latest read before, under 1 ms after, the same rows.

Look the id up first with :func:`feature_id` (one name) or
:func:`feature_ids` (several), then filter ``rs.feature_id = :fid``. For a
read across several features, bind the ids as an array
(``unnest(CAST(:fids AS integer[])) AS f(id)``), take each feature's newest
row in a ``CROSS JOIN LATERAL`` subquery filtered on ``rs.feature_id = f.id``
with the read's own ``ORDER BY ... LIMIT``, then apply that same
``ORDER BY ... LIMIT`` to those rows (see the price read in
``api/routers/watchlist_core.py``). ``rs.feature_id = ANY(:fids)`` is not
enough there: PostgreSQL 14 cannot read a ScalarArrayOp index scan in
``obs_date`` order, so it still reads and sorts every row (71 ms for the one
id of spy_full; 0.7 ms with LATERAL).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import text

_ID_BY_NAME = text("SELECT id FROM feature_registry WHERE name = :name")
_IDS_BY_NAME = text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)")


def feature_id(conn: Any, name: str) -> int | None:
    """``feature_registry.id`` of *name* (names are unique), or None when unregistered."""
    if not name:
        return None
    row = conn.execute(_ID_BY_NAME, {"name": name}).fetchone()
    return int(row[0]) if row else None


def feature_ids(conn: Any, names: Iterable[str]) -> list[int]:
    """Ids of the registered *names*, in the order given, each once.

    Unregistered names are dropped, so an empty list means no feature
    matched: the name-join read it replaces would have returned no rows.
    """
    wanted = list(dict.fromkeys(n for n in names if n))
    if not wanted:
        return []
    rows = conn.execute(_IDS_BY_NAME, {"names": wanted}).fetchall()
    by_name = {r[1]: int(r[0]) for r in rows}
    return [by_name[n] for n in wanted if n in by_name]
