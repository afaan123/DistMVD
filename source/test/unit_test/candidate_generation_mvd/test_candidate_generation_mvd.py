import pytest

A, B, C, D = range(4)

LEVEL_1 = [
    frozenset({A}),
    frozenset({B}),
    frozenset({C}),
]

ALL_PRUNING = {
    "complement": True,
    "superset": True,
    "fd": True,
    "discovered_mvd": True,
}


def make_closure(num_attributes, lhs_list, fds=()):
    from closure import Closure

    closure = Closure(frozenset(range(num_attributes)))
    for lhs in lhs_list:
        closure.initialize_closure(lhs)
    closure.expand_closures(set(fds), lhs_list)
    return closure


def make_candidates(spark, num_attributes):
    from candidates import Candidates

    return Candidates(
        sc=spark.sparkContext,
        num_attributes=num_attributes,
        num_partitions=2,
        pruning=ALL_PRUNING,
        stats=None,
    )


@pytest.mark.parametrize(
    "num_attributes, lhs_list, expected",
    [
        (
            3,
            LEVEL_1,
            {
                (A,): [((B,), (C,))],
                (B,): [((A,), (C,))],
                (C,): [((A,), (B,))],
            },
        ),
        (
            4,
            [frozenset({A, B})],
            {(A, B): [((C,), (D,))]},
        ),
    ],
    ids=["classic", "complement_pair_level_2"],
)
def test_candidates(spark, num_attributes, lhs_list, expected):
    closure = make_closure(num_attributes, lhs_list)
    candidates = make_candidates(spark, num_attributes)

    to_validate, auto_validated = candidates.generate(lhs_list, closure, set())

    assert to_validate == expected
    assert auto_validated == []


@pytest.mark.parametrize(
    "fds",
    [
        [(frozenset({A}), B), (frozenset({A}), C)],
        [(frozenset({A}), B)],
    ],
    ids=["superkey", "fd_implied"],
)
def test_auto_validated(spark, fds):
    lhs_list = [frozenset({A})]
    closure = make_closure(3, lhs_list, fds)

    to_validate, auto_validated = make_candidates(spark, 3).generate(
        lhs_list, closure, set()
    )

    assert to_validate == {}
    assert auto_validated == [(frozenset({A}), frozenset({B}))]


def test_superset_lhs_is_pruned(spark):
    lhs_list = [frozenset({A, B})]
    closure = make_closure(4, lhs_list)

    to_validate, auto_validated = make_candidates(spark, 4).generate(
        lhs_list, closure, {(frozenset({A}), frozenset({C}))}
    )

    assert to_validate == {}
    assert auto_validated == []
