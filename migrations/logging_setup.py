"""Make alembic's own logging config take effect.

``import alembic`` installs a ``NullHandler`` on the ``alembic`` logger — the
usual library courtesy, so a library never prints unless asked. But that
handler counts as "found" in ``logging.Logger.callHandlers``, so Python's
last-resort stderr handler never fires, and **every** record from a migration
is discarded. Silently, with nothing in any config file saying so.

``migrations/env.py`` never called ``fileConfig``, which left
``alembic.ini``'s fully populated ``[loggers]``/``[handlers]``/
``[formatters]`` sections as dead text. Deploy 641 (2026-09-13) is the
evidence: ``snapshot_actor_index_20260912`` took its deferral branch and
emitted a warning naming two INVALID indexes and the exact SQL to clear them,
and not one character reached the deploy log. Neither did alembic's own
"Running upgrade x -> y".

That matters beyond tidiness. That revision is *designed* to skip work on a
large table and report it; the warning is the entire difference between a
documented deferral and the silent degradation #477/#479 spent two PRs
removing. A migration that decides not to act and cannot say so has become
the bug it was written to avoid.

This lives apart from ``env.py`` because ``env.py`` runs migrations at import
time — it needs a live alembic context and a database — so it cannot be
imported by a test. This module can.
"""

from __future__ import annotations

from logging.config import fileConfig


def configure_logging(config_file_name: str | None) -> bool:
    """Apply the logging config in ``config_file_name``; report whether it ran.

    ``disable_existing_loggers=False`` because ``config.py`` has already
    attached its loguru sinks by the time alembic gets here, and the
    ``fileConfig`` default of ``True`` would switch off every logger created
    before this point.
    """
    if config_file_name is None:
        return False
    fileConfig(config_file_name, disable_existing_loggers=False)
    return True
