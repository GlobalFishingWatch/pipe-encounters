"""Tests for the package version."""

import re

from pipe_encounters.utils.ver import __version__


def test_get_pipe_ver() -> None:
    """Checks that the package version starts with a major.minor.patch number."""
    match = re.match(r"[0-9]\.[0-9]\.[0-9]", __version__)
    assert match
