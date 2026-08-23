import pytest

COLUMNS = ["a", "b", "c"]

ROWS = [
    ("1", "5", "7"),
    ("1", "5", "8"),
    ("2", "6", "9"),
]

EXPECTED_FDS = {
    (("a",), "b"),
    (("b",), "a"),
    (("c",), "a"),
    (("c",), "b"),
}

EXPECTED_MVDS = {
    (("a",), ("b",)),
    (("b",), ("a",)),
    (("c",), ("a",)),
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
