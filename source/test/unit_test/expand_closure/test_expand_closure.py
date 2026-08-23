A, B, C = range(3)
ATTRIBUTES = 3

LEVEL_1 = [frozenset({A}), frozenset({B}), frozenset({C})]

FD_AS_MVD_FDS = {
    (frozenset({A}), B),
    (frozenset({B}), A),
    (frozenset({C}), A),
    (frozenset({C}), B),
}


def make_closure(num_attributes, lhs_list, fds=()):
    from closure import Closure

    closure = Closure(frozenset(range(num_attributes)))
    for lhs in lhs_list:
        closure.initialize_closure(lhs)
    closure.expand_closures(set(fds), lhs_list)
    return closure


def test_expand():
    closure = make_closure(ATTRIBUTES, LEVEL_1, FD_AS_MVD_FDS)

    assert [closure.compute_closure(lhs) for lhs in LEVEL_1] == [
        frozenset({A, B}),
        frozenset({A, B}),
        frozenset({A, B, C}),
    ]
    assert [closure.check_superkey(lhs) for lhs in LEVEL_1] == [
        False,
        False,
        True,
    ]


def test_expand_is_transitive():
    c_determines_a_which_determines_b = {
        (frozenset({C}), A),
        (frozenset({A}), B),
    }

    closure = make_closure(
        ATTRIBUTES, LEVEL_1, c_determines_a_which_determines_b
    )

    assert closure.compute_closure(frozenset({C})) == frozenset({A, B, C})
