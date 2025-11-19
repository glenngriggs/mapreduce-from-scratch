"""Verify required files are present."""

import os


def test_check_submission_files():
    """Check for files and directories required by the spec."""
    assert os.path.exists("bin")
    assert os.path.exists("bin/mapreduce")
    assert os.path.exists("pyproject.toml")
    assert os.path.exists("mapreduce")
    assert os.path.exists("mapreduce/manager/__init__.py")
    assert os.path.exists("mapreduce/manager/__main__.py")
    assert os.path.exists("mapreduce/worker/__init__.py")
    assert os.path.exists("mapreduce/worker/__main__.py")
    assert os.path.exists("mapreduce/__init__.py")
    assert os.path.exists("mapreduce/submit.py")
    assert (
        os.path.exists("mapreduce/utils.py") or
        os.path.exists("mapreduce/utils")
    )
