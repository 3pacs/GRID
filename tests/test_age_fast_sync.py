"""Regression tests for scripts/age_fast_sync._esc dollar-quote injection guard.

The `sync()` body wraps its Cypher payload in a Postgres $$-quoted string:

    SELECT * FROM cypher('grid_graph', $$ MERGE (a:Actor {actor_id: '<val>'}) ... $$) AS (...)

A value containing '$$' would close the outer dollar-quote and let the
remainder execute as arbitrary Postgres SQL. `_esc` must reject any '$' in a
value to neutralize that surface, per docs/PUNCH-LIST-2026-05-13.md (Auditor
2026-07-09 — scripts/, P1 item at scripts/age_fast_sync.py:50).
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

# scripts.age_fast_sync imports `db.get_engine` at module scope, which pulls in
# psycopg2 — not required for exercising the pure-Python `_esc` helper. Stub it
# so this test module can run in environments without the DB driver installed.
sys.modules.setdefault("db", MagicMock(get_engine=MagicMock()))

from scripts.age_fast_sync import _esc


class TestEscBasics:
    def test_none_returns_empty_string(self):
        assert _esc(None) == ""

    def test_plain_ascii_passthrough(self):
        assert _esc("Alice") == "Alice"

    def test_numeric_input_coerced_to_str(self):
        assert _esc(42) == "42"


class TestEscCypherLiteralEscaping:
    def test_single_quote_escaped(self):
        assert _esc("O'Brien") == "O\\'Brien"

    def test_backslash_escaped(self):
        assert _esc("path\\to") == "path\\\\to"

    def test_backslash_then_quote_escaped_in_order(self):
        assert _esc("\\'") == "\\\\\\'"


class TestEscDollarQuoteInjectionGuard:
    """Any '$' character must fail loud — a stray '$$' would escape the outer
    Postgres dollar-quote used by `cypher('grid_graph', $$ ... $$)`."""

    @pytest.mark.parametrize(
        "payload",
        [
            "$$",
            "attacker$$; DROP TABLE actors; --",
            "leading$sign",
            "trailing$",
            "$",
        ],
    )
    def test_dollar_sign_raises(self, payload):
        with pytest.raises(ValueError, match=r"\$"):
            _esc(payload)

    def test_error_message_names_the_hazard(self):
        with pytest.raises(ValueError) as excinfo:
            _esc("bad$$value")
        assert "dollar-quoted" in str(excinfo.value)

    def test_safe_payloads_still_pass(self):
        # These look adversarial but have no '$' — must still round-trip.
        assert _esc("DROP TABLE actors --") == "DROP TABLE actors --"
        assert _esc("{malicious: 'cypher'}") == "{malicious: \\'cypher\\'}"
