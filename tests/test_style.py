"""Verify Python style with pycodestyle, pydocstyle and pylint."""
import subprocess
import utils


def test_pycodestyle():
    """Run pycodestyle."""
    utils.assert_no_prohibited_terms("nopep8", "noqa", "pylint")
    subprocess.run(["pycodestyle", "mapreduce"], check=True)


def test_pydocstyle():
    """Run pydocstyle."""
    utils.assert_no_prohibited_terms("nopep8", "noqa", "pylint")
    subprocess.run(["pydocstyle", "mapreduce"], check=True)


def test_pylint():
    """Run pylint."""
    utils.assert_no_prohibited_terms("nopep8", "noqa", "pylint")
    subprocess.run([
        "pylint",
        "--rcfile", "pyproject.toml",
        "mapreduce",
    ], check=True)
