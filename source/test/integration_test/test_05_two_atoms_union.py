import pytest

COLUMNS = ["a", "b", "c"]

ROWS = [
    ("1", "1", "1"),
    ("1", "1", "2"),
    ("1", "2", "1"),
    ("1", "2", "2"),
    ("2", "5", "5"),
]

EXPECTED_FDS = {
    (("b",), "a"),
    (("c",), "a"),
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
