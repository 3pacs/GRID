from pathlib import Path

from packaging.requirements import Requirement
from sqlalchemy import create_engine


REQUIREMENTS_PATH = Path(__file__).resolve().parents[1] / "requirements.txt"
REQUIREMENTS_API_PATH = Path(__file__).resolve().parents[1] / "requirements-api.txt"


def test_postgresql_engine_uses_installed_psycopg2_driver() -> None:
    # Engine construction imports the driver but does not open a connection.
    # SQLAlchemy 2.1 changed the default to psycopg v3, which GRID does not ship.
    requirement = Requirement(_requirement_line(REQUIREMENTS_PATH, "sqlalchemy"))
    assert "2.0.52" in requirement.specifier
    assert "2.1.0" not in requirement.specifier
    engine = create_engine("postgresql://localhost/grid_driver_contract")
    try:
        assert engine.dialect.driver == "psycopg2"
        assert engine.dialect.dbapi.__name__ == "psycopg2"
    finally:
        engine.dispose()


def _read_non_comment_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _requirement_line(path: Path, package: str) -> str:
    prefix = f"{package.lower()}"
    for line in _read_non_comment_lines(path):
        if line.lower().startswith(prefix):
            return line
    raise AssertionError(f"{package} missing from {path.name}")


def test_base_requirements_do_not_mix_edgar_and_patent_client() -> None:
    requirements = _read_non_comment_lines(REQUIREMENTS_PATH)

    has_edgartools = any(line.startswith("edgartools") for line in requirements)
    has_patent_client = any(line.startswith("patent-client") for line in requirements)

    assert has_edgartools, "Base requirements should include edgartools for SEC ingestion."
    assert not has_patent_client, (
        "Base requirements must not include patent-client while edgartools is present. "
        "These packages require incompatible hishel versions."
    )


def test_base_requirements_declare_openpyxl() -> None:
    # strategy/portfolio_workbook_plan.py imports openpyxl at module load (line
    # 17-18: Workbook, load_workbook, styles). The /api/v1/ten-year-portfolio
    # routes import that module on FastAPI startup, so a missing declaration
    # crashes a fresh deploy with ModuleNotFoundError before any request is
    # served. Also the implicit pandas .read_excel/.to_excel engine for .xlsx
    # in ingestion/altdata + ingestion/trade pullers.
    requirements = _read_non_comment_lines(REQUIREMENTS_PATH)
    assert any(line.startswith("openpyxl") for line in requirements), (
        "Base requirements must declare openpyxl — strategy.portfolio_workbook_plan "
        "imports it at module load and ingestion pullers use it as the pandas "
        "Excel engine."
    )

def test_fastapi_upper_bound_blocks_unreviewed_route_introspection_drift() -> None:
    # The original bound was <0.137.0, set when FastAPI 0.137 made
    # include_router() lazy: a sub-router now lands in `.routes` as a single
    # opaque `_IncludedRouter` with no `.path`, instead of its child routes
    # being flattened into the parent. Everything that enumerated
    # `app.routes` / `router.routes` to discover paths went blind, which is
    # what broke the actor-network and lever-ordering regression tests.
    #
    # That introspection has now been modernized onto the supported
    # `fastapi.routing.iter_route_contexts()` API (api/routers/system.py,
    # tests/test_api.py, tests/test_canvas_api.py), and 0.139 was verified to
    # be a routing no-op: /api/v1/intelligence/actor-network still resolves
    # exactly once and the double-prefixed path still 404s, static lever
    # routes still win over /levers/{domain}, the OpenAPI schema is unchanged,
    # and the /ws first-message token handshake still authenticates and
    # rejects as before.
    #
    # The ceiling stays one minor above the verified version on purpose: it is
    # a tested bound, not a guess, so 0.140 needs the same check re-run before
    # it is allowed in.
    for path in (REQUIREMENTS_PATH, REQUIREMENTS_API_PATH):
        fastapi_req = _requirement_line(path, "fastapi")

        assert "<0.140.0" in fastapi_req, (
            f"{path.name} must keep FastAPI below 0.140 — 0.139 is the "
            "highest release whose router/introspection behavior has actually "
            "been verified against GRID's facade routers and /ws auth."
        )
