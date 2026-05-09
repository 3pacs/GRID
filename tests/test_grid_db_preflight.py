from scripts import grid_db_preflight as preflight


def test_assess_counts_flags_empty_audit_database() -> None:
    checks = preflight.assess_counts(
        {
            "oracle_predictions": 0,
            "resolved_series": 0,
            "signal_sources": 0,
            "feature_registry": 132,
        }
    )

    failed = {check.name: check.message for check in checks if not check.ok}

    assert "oracle_predictions" in failed
    assert "resolved_series" in failed
    assert "signal_sources" in failed
    assert "feature_registry" not in failed


def test_assess_counts_flags_missing_required_table() -> None:
    checks = preflight.assess_counts(
        {
            "oracle_predictions": 25_000,
            "resolved_series": None,
            "signal_sources": 5_000,
            "feature_registry": 500,
        }
    )

    resolved = next(check for check in checks if check.name == "resolved_series")

    assert not resolved.ok
    assert resolved.message == "missing table"


def test_assess_counts_allows_explicit_empty_mode() -> None:
    checks = preflight.assess_counts(
        {
            "oracle_predictions": 0,
            "resolved_series": 0,
            "signal_sources": 0,
            "feature_registry": 0,
        },
        allow_empty=True,
    )

    assert all(check.ok for check in checks)


def test_render_text_never_includes_password() -> None:
    text = preflight.render_text(
        {
            "host": "100.75.185.36",
            "port": 5432,
            "database": "griddb",
            "user": "grid",
        },
        [
            preflight.TableCheck(
                name="oracle_predictions",
                observed=25_000,
                min_count=10_000,
                ok=True,
                message="25000 rows",
            )
        ],
    )

    assert "grid@100.75.185.36:5432/griddb" in text
    assert "password" not in text.lower()
