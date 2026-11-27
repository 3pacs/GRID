"""Keep contagion trade-ticket rows out of regime reads of ``decision_journal`` (#959).

``trading/contagion_to_ticket.py::write_ticket_to_journal`` journals a trade ticket with
``inferred_state = CONTAGION_<shock>`` and ``contradiction_flags.flow_thesis = 'contagion_derived'`` (old rows
carry the tag too). It is a position record, not a regime state, but the regime readers selected "the latest
row" and served it as the regime.

The match is on the tag, never on the ``CONTAGION_`` state prefix: regime subtypes with that name are genuine
states. The tag is normalized like the other structured journal markers (outer whitespace, case) and the SQL is
null-safe for any JSONB shape.

This lives beside, not inside, ``store/journal_qualification.py``: that file's bytes are sealed by the Oct-25
native-compatibility workflow, so extending its predicate is a decision for its owner. Readers that use it
(thesis, sentiment, feedback, flow, dad, watchlist) are therefore NOT covered here.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import TextClause

from store.journal_qualification import _JOURNAL_MARKER_WHITESPACE

# A distinct bind name: it must not collide with the qualification guard's own ``journal_marker_whitespace``
# when both are applied to one statement.
CONTAGION_TICKET_JOURNAL_MARKER = (
    "LOWER(BTRIM(COALESCE(contradiction_flags ->> 'flow_thesis', ''), "
    ":contagion_marker_whitespace) COLLATE \"C\") = 'contagion_derived'"
)
NOT_CONTAGION_TICKET_JOURNAL_PREDICATE = "(NOT " + CONTAGION_TICKET_JOURNAL_MARKER + ")"


def excluding_contagion_tickets(statement):
    """``statement`` with contagion trade tickets excluded.

    A ``Select`` gets the predicate as an extra WHERE (compose before ``order_by``/``limit``). A ``text()`` clause
    must already contain a WHERE and the predicate text; only its whitespace parameter is bound here.
    """
    if isinstance(statement, Select):
        return statement.where(
            text(NOT_CONTAGION_TICKET_JOURNAL_PREDICATE).bindparams(
                contagion_marker_whitespace=_JOURNAL_MARKER_WHITESPACE
            )
        )
    if isinstance(statement, TextClause):
        return statement.bindparams(contagion_marker_whitespace=_JOURNAL_MARKER_WHITESPACE)
    raise TypeError("expected a SQLAlchemy Select or text() clause")
