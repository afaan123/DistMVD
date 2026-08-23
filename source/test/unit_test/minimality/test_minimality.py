import pytest

A, B, C, D = range(4)

COLUMNS_3 = ["a", "b", "c"]
COLUMNS_4 = ["a", "b", "c", "d"]
COLUMNS_5 = ["a", "b", "c", "d", "e"]


def cleanup(columns, discovered):
    from minimality import Minimality

    return set(
        Minimality(
            all_attributes=frozenset(range(len(columns))),
            attr_names=columns,
            stats=None,
        ).cleanup(discovered)
    )


@pytest.mark.parametrize(
    "columns, discovered, expected",
    [
        (
            COLUMNS_3,
            {
                (frozenset({A}), frozenset({B})),
                (frozenset({B}), frozenset({A})),
                (frozenset({C}), frozenset({A})),
            },
            {
                (("a",), ("b",)),
                (("b",), ("a",)),
                (("c",), ("a",)),
            },
        ),
        (
            COLUMNS_4,
            {
                (frozenset({A}), frozenset({B})),
                (frozenset({A}), frozenset({C, D})),
            },
            {(("a",), ("b",))},
        ),
        (
            COLUMNS_4,
            {
                (frozenset({A}), frozenset({B})),
                (frozenset({A, C}), frozenset({B})),
            },
            {(("a",), ("b",))},
        ),
        (
            COLUMNS_5,
            {
                (frozenset({A}), frozenset({B})),
                (frozenset({A}), frozenset({B, C})),
            },
            {(("a",), ("b",))},
        ),
        (
            COLUMNS_3,
            {
                (frozenset({A, B}), frozenset({B})),
                (frozenset({A}), frozenset({B, C})),
            },
            set(),
        ),
    ],
    ids=[
        "classic",
        "complement_collapses",
        "redundant_lhs",
        "redundant_rhs",
        "trivial",
    ],
)
def test_cleanup(columns, discovered, expected):
    assert cleanup(columns, discovered) == expected


def test_canonical_form_breaks_ties_on_the_smaller_indexes():
    from minimality import Minimality

    minimality = Minimality(
        all_attributes=frozenset(range(4)),
        attr_names=COLUMNS_4,
        stats=None,
    )

    assert minimality.canonical_form(
        frozenset({A, B}), frozenset({D})
    ) == frozenset({C})
