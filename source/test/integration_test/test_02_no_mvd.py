import pytest

COLUMNS = ["a", "b", "c"]

ROWS = [
    ("1", "1", "1"),
    ("1", "2", "2"),
    ("2", "1", "2"),
]

EXPECTED_FDS = set()

EXPECTED_MVDS = set()


@pytest.fixture(scope="module")
def results(discover):
    return discover(COLUMNS, ROWS)


def test_fd(results):
    fds, _mvds = results

    assert fds == EXPECTED_FDS


def test_mvd(results):
    _fds, mvds = results

    assert mvds == EXPECTED_MVDS
