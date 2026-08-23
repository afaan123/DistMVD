import pytest

SINGLE_ROW_COLUMNS = ["a", "b", "c"]
SINGLE_ROW_ROWS = [
    ("1", "2", "3"),
]

TWO_COLUMNS_COLUMNS = ["a", "b"]
TWO_COLUMNS_ROWS = [
    ("1", "5"),
    ("1", "6"),
    ("2", "7"),
]

SINGLE_COLUMN_COLUMNS = ["a"]
SINGLE_COLUMN_ROWS = [
    ("1",),
    ("2",),
]

ALL_DUPLICATES_COLUMNS = ["a", "b", "c"]
ALL_DUPLICATES_ROWS = [
    ("1", "1", "1"),
    ("1", "1", "1"),
    ("1", "1", "1"),
]

ALL_UNIQUE_COLUMNS = ["a", "b", "c"]
ALL_UNIQUE_ROWS = [
    ("1", "1", "1"),
    ("2", "2", "2"),
    ("3", "3", "3"),
]

CONSTANT_COLUMN_COLUMNS = ["a", "b", "c"]
CONSTANT_COLUMN_ROWS = [
    ("1", "5", "9"),
    ("1", "6", "9"),
    ("2", "7", "9"),
]

NULLS_COLUMNS = ["a", "b", "c"]
NULLS_ROWS = [
    ("1", "x", None),
    ("1", "x", "5"),
    ("2", "y", "6"),
]

CLASSIC_COLUMNS = ["a", "b", "c"]
CLASSIC_ROWS = [
    ("CS", "A", "X"),
    ("CS", "A", "Y"),
    ("CS", "B", "X"),
    ("CS", "B", "Y"),
    ("EE", "C", "Z"),
]

DUPLICATE_ROWS_COLUMNS = ["a", "b", "c"]
DUPLICATE_ROWS_ROWS = [
    ("CS", "A", "X"),
    ("CS", "A", "X"),
    ("CS", "A", "Y"),
    ("CS", "B", "X"),
    ("CS", "B", "Y"),
    ("EE", "C", "Z"),
]

EVERY_PAIRWISE_FD = {
    (("a",), "b"),
    (("a",), "c"),
    (("b",), "a"),
    (("b",), "c"),
    (("c",), "a"),
    (("c",), "b"),
}

DATASETS = {
    "single_row": (SINGLE_ROW_COLUMNS, SINGLE_ROW_ROWS),
    "two_columns": (TWO_COLUMNS_COLUMNS, TWO_COLUMNS_ROWS),
    "single_column": (SINGLE_COLUMN_COLUMNS, SINGLE_COLUMN_ROWS),
    "all_duplicates": (ALL_DUPLICATES_COLUMNS, ALL_DUPLICATES_ROWS),
    "all_unique": (ALL_UNIQUE_COLUMNS, ALL_UNIQUE_ROWS),
    "constant_column": (CONSTANT_COLUMN_COLUMNS, CONSTANT_COLUMN_ROWS),
    "nulls": (NULLS_COLUMNS, NULLS_ROWS),
    "classic": (CLASSIC_COLUMNS, CLASSIC_ROWS),
    "duplicate_rows": (DUPLICATE_ROWS_COLUMNS, DUPLICATE_ROWS_ROWS),
}


@pytest.fixture(scope="module")
def results(discover):
    return {
        name: discover(columns, rows)
        for name, (columns, rows) in DATASETS.items()
    }


def test_single_row_fds(results):
    fds, _mvds = results["single_row"]

    assert fds == {
        (("a",), "b"),
        (("a",), "c"),
        (("b",), "a"),
        (("b",), "c"),
        (("c",), "a"),
        (("c",), "b"),
    }


def test_single_row_mvds(results):
    _fds, mvds = results["single_row"]

    assert mvds == {
        (("a",), ("b",)),
        (("b",), ("a",)),
        (("c",), ("a",)),
    }


def test_two_columns_reports_no_fds(results):
    fds, _mvds = results["two_columns"]

    assert fds == set()


def test_two_columns_reports_no_mvds(results):
    _fds, mvds = results["two_columns"]

    assert mvds == set()


def test_single_column_reports_no_fds(results):
    fds, _mvds = results["single_column"]

    assert fds == set()


def test_single_column_reports_no_mvds(results):
    _fds, mvds = results["single_column"]

    assert mvds == set()


def test_all_duplicates_reports_every_pairwise_fd(results):
    fds, _mvds = results["all_duplicates"]

    assert fds == EVERY_PAIRWISE_FD


def test_all_unique_reports_every_pairwise_fd(results):
    fds, _mvds = results["all_unique"]

    assert fds == EVERY_PAIRWISE_FD


def test_constant_column_is_determined_by_every_other_column(results):
    fds, _mvds = results["constant_column"]

    assert fds == {
        (("a",), "c"),
        (("b",), "a"),
        (("b",), "c"),
    }


def test_constant_column_determines_nothing(results):
    _fds, mvds = results["constant_column"]

    assert mvds == {
        (("a",), ("b",)),
        (("b",), ("a",)),
    }


def test_nulls_are_treated_as_their_own_value(results):
    fds, _mvds = results["nulls"]

    assert fds == {
        (("a",), "b"),
        (("b",), "a"),
        (("c",), "a"),
        (("c",), "b"),
    }


def test_nulls_do_not_break_mvd_discovery(results):
    _fds, mvds = results["nulls"]

    assert mvds == {
        (("a",), ("b",)),
        (("b",), ("a",)),
        (("c",), ("a",)),
    }


def test_duplicate_rows_do_not_change_the_result(results):
    assert results["duplicate_rows"] == results["classic"]


@pytest.mark.parametrize("name", list(DATASETS))
def test_no_trivial_fd_is_reported(results, name):
    fds, _mvds = results[name]

    for lhs, rhs in fds:
        assert rhs not in lhs


@pytest.mark.parametrize("name", list(DATASETS))
def test_no_trivial_mvd_is_reported(results, name):
    columns, _rows = DATASETS[name]
    _fds, mvds = results[name]

    for lhs, rhs in mvds:
        assert not set(lhs) & set(rhs)
        assert set(columns) - set(lhs) - set(rhs)


@pytest.mark.parametrize("name", list(DATASETS))
def test_no_complement_pair_is_reported(results, name):
    columns, _rows = DATASETS[name]
    _fds, mvds = results[name]

    for lhs, rhs in mvds:
        z = tuple(sorted(set(columns) - set(lhs) - set(rhs)))
        assert (lhs, z) not in mvds
