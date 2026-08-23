A, B, C, D = range(4)
ATTRIBUTES = 4

LEVEL_1 = [frozenset({A}), frozenset({B}), frozenset({C}), frozenset({D})]

MULTI_ATTR_FDS = {
    (frozenset({B}), A),
    (frozenset({B}), C),
    (frozenset({C}), A),
    (frozenset({C}), B),
    (frozenset({D}), A),
}

COMPLEMENT_PAIR_FDS = {
    (frozenset({A}), B),
    (frozenset({B}), A),
    (frozenset({C}), D),
    (frozenset({D}), C),
}


def make_closure(num_attributes, lhs_list, fds=()):
    from closure import Closure

    closure = Closure(frozenset(range(num_attributes)))
    for lhs in lhs_list:
        closure.initialize_closure(lhs)
    closure.expand_closures(set(fds), lhs_list)
    return closure


def test_inherit():
    closure = make_closure(ATTRIBUTES, LEVEL_1, MULTI_ATTR_FDS)
    level_2 = [frozenset({A, B}), frozenset({B, C}), frozenset({B, D})]

    closure.inherit_subset_closures(level_2)

    assert {lhs: closure.compute_closure(lhs) for lhs in level_2} == {
        frozenset({A, B}): frozenset({A, B, C}),
        frozenset({B, C}): frozenset({A, B, C}),
        frozenset({B, D}): frozenset({A, B, C, D}),
    }


def test_inherit_creates_superkey():
    closure = make_closure(ATTRIBUTES, LEVEL_1, COMPLEMENT_PAIR_FDS)

    closure.inherit_subset_closures([frozenset({A, C})])

    assert closure.check_superkey(frozenset({A, C})) == True
