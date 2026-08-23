import pytest

BROKEN_MVD_COLUMNS = ["course", "instructor", "book"]
BROKEN_MVD_ROWS = [
    ("CS", "A", "X"),
    ("CS", "B", "Y"),
    ("EE", "C", "Z"),
]

TRAP_COLUMNS = ["x", "y", "z"]
TRAP_ROWS = [
    ("1", "1", "1"),
    ("1", "1", "2"),
    ("1", "2", "1"),
    ("1", "2", "2"),
    ("2", "5", "5"),
    ("2", "6", "7"),
]

DATASETS = {
    "broken_mvd": (BROKEN_MVD_COLUMNS, BROKEN_MVD_ROWS),
    "trap": (TRAP_COLUMNS, TRAP_ROWS),
}


@pytest.fixture(scope="module")
def results(discover):
    return {
        name: discover(columns, rows)
        for name, (columns, rows) in DATASETS.items()
    }


def test_broken_mvd_does_not_report_a_course_dependency(results):
    fds, mvds = results["broken_mvd"]

    assert (("course",), ("instructor",)) not in mvds
    assert (("course",), ("book",)) not in mvds
    assert (("course",), "instructor") not in fds
    assert (("course",), "book") not in fds


def test_broken_mvd_reports_nothing_extra(results):
    fds, mvds = results["broken_mvd"]

    assert fds == {
        (("instructor",), "course"),
        (("instructor",), "book"),
        (("book",), "course"),
        (("book",), "instructor"),
    }
    assert mvds == {
        (("instructor",), ("course",)),
        (("book",), ("course",)),
    }


def test_trap_does_not_report_an_x_dependency(results):
    fds, mvds = results["trap"]

    assert (("x",), ("y",)) not in mvds
    assert (("x",), ("z",)) not in mvds
    assert (("x",), "y") not in fds
    assert (("x",), "z") not in fds


def test_trap_reports_nothing_extra(results):
    fds, mvds = results["trap"]

    assert fds == {
        (("y",), "x"),
        (("z",), "x"),
    }
    assert mvds == {
        (("y",), ("x",)),
        (("z",), ("x",)),
    }


@pytest.mark.parametrize("name", ["broken_mvd", "trap"])
def test_no_trivial_fd_is_reported(results, name):
    fds, _mvds = results[name]

    for lhs, rhs in fds:
        assert rhs not in lhs


@pytest.mark.parametrize("name", ["broken_mvd", "trap"])
def test_no_trivial_mvd_is_reported(results, name):
    columns, _rows = DATASETS[name]
    _fds, mvds = results[name]

    for lhs, rhs in mvds:
        assert not set(lhs) & set(rhs)
        assert set(columns) - set(lhs) - set(rhs)


@pytest.mark.parametrize("name", ["broken_mvd", "trap"])
def test_no_complement_pair_is_reported(results, name):
    columns, _rows = DATASETS[name]
    _fds, mvds = results[name]

    for lhs, rhs in mvds:
        z = tuple(sorted(set(columns) - set(lhs) - set(rhs)))
        assert (lhs, z) not in mvds


@pytest.mark.parametrize("name", ["broken_mvd", "trap"])
def test_no_redundant_lhs_is_reported(results, name):
    fds, mvds = results[name]

    for pairs in (fds, mvds):
        for lhs, rhs in pairs:
            for other_lhs, other_rhs in pairs:
                if rhs == other_rhs:
                    assert not set(lhs) < set(other_lhs)
