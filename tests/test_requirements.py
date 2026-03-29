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
