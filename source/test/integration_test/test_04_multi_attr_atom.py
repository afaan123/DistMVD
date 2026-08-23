import pytest

COLUMNS = ["a", "b", "c", "d"]

ROWS = [
    ("1", "1", "1", "7"),
    ("1", "1", "1", "8"),
    ("1", "2", "2", "7"),
    ("1", "2", "2", "8"),
]

EXPECTED_FDS = {
    (("b",), "a"),
    (("b",), "c"),
    (("c",), "a"),
    (("c",), "b"),
    (("d",), "a"),
}

EXPECTED_MVDS = {
    (("a",), ("d",)),
    (("b",), ("a",)),
    (("b",), ("c",)),
    (("b",), ("d",)),
    (("c",), ("a",)),
    (("c",), ("b",)),
    (("c",), ("d",)),
    (("d",), ("a",)),
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
