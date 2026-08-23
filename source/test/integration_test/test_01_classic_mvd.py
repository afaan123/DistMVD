import pytest

COLUMNS = ["course", "instructor", "book"]

ROWS = [
    ("CS", "A", "X"),
    ("CS", "A", "Y"),
    ("CS", "B", "X"),
    ("CS", "B", "Y"),
    ("EE", "C", "Z"),
]

EXPECTED_FDS = {
    (("instructor",), "course"),
    (("book",), "course"),
}

EXPECTED_MVDS = {
    (("course",), ("instructor",)),
    (("instructor",), ("course",)),
    (("book",), ("course",)),
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
