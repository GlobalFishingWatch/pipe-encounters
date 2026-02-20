from pipe_encounters.utils.ver import __version__
import re


def test_get_pipe_ver():
    match = re.match('[0-9]\.[0-9]\.[0-9]', __version__)
    assert match
