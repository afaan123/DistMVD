import pytest

COLUMNS = ["x", "y", "z"]

ROWS = [
    ("1", "1", "7"),
    ("1", "2", "7"),
    ("2", "1", "8"),
    ("2", "2", "8"),
    ("3", "1", "9"),
    ("3", "2", "9"),
]

EXPECTED_FDS = {
    (("x",), "z"),
    (("z",), "x"),
}

EXPECTED_MVDS = {
    (("x",), ("y",)),
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
