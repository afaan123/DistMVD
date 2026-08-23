import pytest

COLUMNS = ["A", "B", "C", "D", "E", "F"]

ROWS = [
    ("alpha", "red", "apple", "small", "cat", "high"),
    ("alpha", "red", "banana", "large", "dog", "low"),
    ("alpha", "blue", "apple", "small", "cat", "high"),
    ("alpha", "blue", "banana", "large", "dog", "low"),
    ("beta", "green", "orange", "medium", "bird", "medium"),
]

EXPECTED_FDS = {
    (("B",), "A"),
    (("C",), "A"),
    (("C",), "D"),
    (("C",), "E"),
    (("C",), "F"),
    (("D",), "A"),
    (("D",), "C"),
    (("D",), "E"),
    (("D",), "F"),
    (("E",), "A"),
    (("E",), "C"),
    (("E",), "D"),
    (("E",), "F"),
    (("F",), "A"),
    (("F",), "C"),
    (("F",), "D"),
    (("F",), "E"),
}

EXPECTED_MVDS = {
    (("A",), ("B",)),
    (("B",), ("A",)),
    (("C",), ("A",)),
    (("C",), ("B",)),
    (("C",), ("D",)),
    (("C",), ("E",)),
    (("C",), ("F",)),
    (("D",), ("A",)),
    (("D",), ("B",)),
    (("D",), ("C",)),
    (("D",), ("E",)),
    (("D",), ("F",)),
    (("E",), ("A",)),
    (("E",), ("B",)),
    (("E",), ("C",)),
    (("E",), ("D",)),
    (("E",), ("F",)),
    (("F",), ("A",)),
    (("F",), ("B",)),
    (("F",), ("C",)),
    (("F",), ("D",)),
    (("F",), ("E",)),
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
