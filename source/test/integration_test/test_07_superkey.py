import pytest

COLUMNS = ["id", "a", "b"]

ROWS = [
    ("1", "5", "7"),
    ("2", "5", "8"),
    ("3", "6", "7"),
    ("4", "6", "8"),
]

EXPECTED_FDS = {
    (("id",), "a"),
    (("id",), "b"),
}

EXPECTED_MVDS = {
    (("id",), ("a",)),
}


@pytest.fixture(scope="module")
def results(discover):
    return discover(COLUMNS, ROWS)


def test_fd(results):
    fds, _mvds = results

    assert fds == EXPECTED_FDS


def test_mvd(results):
    _fds, mvds = results

    assert mvds == EXPECTED_MVDS
