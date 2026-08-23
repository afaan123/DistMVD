import pytest

COLUMNS = ["x", "y", "z"]

ROWS = [
    ("1", "1", "1"),
    ("1", "1", "2"),
    ("1", "2", "1"),
    ("1", "2", "2"),
    ("2", "9", "9"),
]

EXPECTED_FDS = {
    (("y",), "x"),
    (("z",), "x"),
}

EXPECTED_MVDS = {
    (("x",), ("y",)),
    (("y",), ("x",)),
    (("z",), ("x",)),
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
