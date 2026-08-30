from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_dev_lock_includes_dev_and_build_inputs() -> None:
    root = _repo_root()
    lock = (root / "requirements-dev.lock").read_text()
    toolchain = (root / "requirements-dev-toolchain.txt").read_text()

    assert "pytest==" in lock
    assert "ruff==" in lock
    assert "mypy==" in lock
    assert "setuptools-scm==" in lock
    assert "setuptools==" in lock
    assert "pip-tools==7.5.2" in lock
    assert "pip==24.0" in lock
    assert "pip==24.0" in toolchain
    assert "pip-tools==7.5.2" in toolchain
    assert "#   -c requirements.lock" in lock


def test_makefile_dev_lock_targets_are_non_mutating_and_constrained() -> None:
    makefile = (_repo_root() / "Makefile").read_text()

    assert (
        "lock:\n\tpip-compile pyproject.toml --output-file requirements.lock --strip-extras"
        in makefile
    )
    assert "lock-dev:" in makefile
    assert "check-lock-dev:" in makefile
    assert "--extra dev" in makefile
    assert "--all-build-deps" in makefile
    assert "--constraint requirements.lock" in makefile
    assert "--constraint requirements-dev-toolchain.txt" in makefile
    assert "--no-header" in makefile
    assert "requirements-dev.lock" in makefile
    assert "mktemp" in makefile
    assert 'cp requirements-dev.lock "$$tmp"' in makefile
    assert 'diff -u requirements-dev.lock "$$tmp"' in makefile


def test_ci_installs_locked_dev_environment_without_dependency_resolution() -> None:
    ci = (_repo_root() / ".github" / "workflows" / "ci.yml").read_text()

    assert 'pip install -e ".[dev]"' not in ci
    assert ci.count("pip install -r requirements-dev.lock") == 4
    assert ci.count("pip install --no-deps --no-build-isolation -e .") == 4
    assert "make check-lock-dev" in ci
    assert (
        "hashFiles('pyproject.toml', 'requirements.lock', 'requirements-dev.lock', "
        "'requirements-dev-toolchain.txt')"
    ) in ci


def test_readme_bootstrap_uses_locked_dev_install() -> None:
    readme = (_repo_root() / "README.md").read_text()

    assert 'pip install -e ".[dev]"' not in readme
    assert "Python 3.12" in readme
    assert "`pip==24.0`" in readme
    assert "`pip-tools==7.5.2`" in readme
    assert "python -m pip install -r requirements-dev.lock" in readme
    assert "python -m pip install --no-deps --no-build-isolation -e ." in readme
