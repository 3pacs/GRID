from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_LIB = REPO_ROOT / "astrogrid_web" / "lib"
EXPECTED_MODULES = [
    "contract.js",
    "endpoints.js",
    "ephemeris.js",
    "hypotheses.js",
    "prophecy.js",
    "snapshot.js",
    "worldModel.js",
]


def test_astrogrid_web_lib_modules_are_real_files():
    missing = []
    symlinked = []

    for name in EXPECTED_MODULES:
        path = WEB_LIB / name
        if not path.exists():
            missing.append(name)
            continue
        if path.is_symlink():
            symlinked.append(name)

    assert not missing, f"Missing AstroGrid web runtime modules: {missing}"
    assert not symlinked, f"AstroGrid web runtime modules must be real files, not symlinks: {symlinked}"
