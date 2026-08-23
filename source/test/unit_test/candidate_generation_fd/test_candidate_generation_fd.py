import pytest

A, B, C = range(3)
ATTRIBUTES = 3

LEVEL_1 = [
    frozenset({A}),
    frozenset({B}),
    frozenset({C}),
]


def make_closure(fds=()):
    from closure import Closure

    closure = Closure(frozenset(range(ATTRIBUTES)))

    for lhs in LEVEL_1:
        closure.initialize_closure(lhs)

    closure.expand_closures(set(fds), LEVEL_1)
    return closure


def make_discovery():
    from fd_discovery import FDDiscovery

    return FDDiscovery(
        sc=None,
        packed_rdd=None,
        num_attributes=ATTRIBUTES,
        shuffle_partitions=2,
        num_partitions=2,
        attr_names=["course", "instructor", "book"],
        stats=None,
        packed=True,
        bits=2,
    )


@pytest.mark.parametrize(
    "fds, expected",
    [
        (
            {
                (frozenset({B}), A),
                (frozenset({C}), A),
            },
            [
                ((A,), [B, C]),
                ((B,), [C]),
                ((C,), [B]),
            ],
        ),
        (
            {
                (frozenset({A}), B),
                (frozenset({A}), C),
            },
            [
                ((B,), [A, C]),
                ((C,), [A, B]),
            ],
        ),
    ],
)
def test_candidates(fds, expected):
    closure = make_closure(fds)

    actual = make_discovery().generate_fd_candidates(LEVEL_1, closure)

    assert actual == expected


def test_trivial_rhs_never_offered():
    closure = make_closure()
    lhs = frozenset({A, B})

    actual = make_discovery().generate_fd_candidates([lhs], closure)

    assert actual == [((A, B), [C])]
