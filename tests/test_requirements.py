from pathlib import Path


REQUIREMENTS_PATH = Path(__file__).resolve().parents[1] / "requirements.txt"


def _read_non_comment_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


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
