"""Offline SQL protocol rows, not a PostgreSQL closure certification.

Captured-shape PG tests independently verify these runtime pins against the
server. Caller/cancellation tests need the new catalog SELECT response shape.
"""
from ingestion.options_publication import _FUNCTION_SOURCES


def reviewed_function_rows():
    return [(name, digest, False, "grid", None, None)
            for name, digest in _FUNCTION_SOURCES.items()]
