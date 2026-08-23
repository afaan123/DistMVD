import pytest

COLUMNS = ["a", "b", "c", "d"]

ROWS = [
    ("1", "1", "7", "7"),
    ("1", "1", "8", "8"),
    ("2", "2", "7", "7"),
    ("2", "2", "8", "8"),
]

EXPECTED_FDS = {
    (("a",), "b"),
    (("b",), "a"),
    (("c",), "d"),
    (("d",), "c"),
}

EXPECTED_MVDS = {
    (("a",), ("b",)),
    (("b",), ("a",)),
    (("c",), ("d",)),
    (("d",), ("c",)),
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
