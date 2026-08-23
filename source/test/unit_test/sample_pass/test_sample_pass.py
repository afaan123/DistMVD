import pytest

A, B, C = range(3)

COLUMNS = ["a", "b", "c"]

CANDIDATES = {(A,): [((B,), (C,))]}

HOLDS = {(frozenset({A}), frozenset({B}))}

CLASSIC = (
    COLUMNS,
    [
        ("CS", "A", "X"),
        ("CS", "A", "Y"),
        ("CS", "B", "X"),
        ("CS", "B", "Y"),
        ("EE", "C", "Z"),
    ],
)

TRAP = (
    COLUMNS,
    [
        ("1", "1", "1"),
        ("1", "1", "2"),
        ("1", "2", "1"),
        ("1", "2", "2"),
        ("2", "5", "5"),
        ("2", "6", "7"),
    ],
)

NO_MVD = (
    COLUMNS,
    [
        ("1", "1", "1"),
        ("1", "2", "2"),
        ("2", "1", "2"),
    ],
)

SKEW = (
    COLUMNS,
    [
        ("1", "1", "1"),
        ("1", "1", "2"),
        ("1", "2", "1"),
        ("1", "2", "2"),
        ("2", "9", "9"),
    ],
)

SINGLE_ROW = (COLUMNS, [("1", "1", "1")])

NULLS = (
    COLUMNS,
    [
        ("1", "x", None),
        ("1", "x", "5"),
        ("2", "y", "6"),
    ],
)


def make_validation(spark, packed_rdd, bits, sampling_enabled, largest_k):
    from sampling import Sampling
    from validation import Validation

    sampling = Sampling(
        sc=spark.sparkContext,
        packed_rdd=packed_rdd,
        num_attributes=3,
        shuffle_partitions=2,
        sampling_enabled=sampling_enabled,
        largest_k=largest_k,
        stats=None,
        packed=True,
        mode="manual",
        bits=bits,
    )

    return Validation(
        sc=spark.sparkContext,
        packed_rdd=packed_rdd,
        num_attributes=3,
        shuffle_partitions=2,
        sampling=sampling,
        stats=None,
        packed=True,
        bits=bits,
    )


def discovered_mvds(spark, packed, dataset, sampling_enabled, largest_k):
    packed_rdd, bits = packed(*dataset)
    validation = make_validation(
        spark, packed_rdd, bits, sampling_enabled, largest_k
    )

    result = set()
    validation.validate_level(CANDIDATES, result)
    return result


@pytest.mark.parametrize("largest_k", [1, 2], ids=["k1", "k2"])
@pytest.mark.parametrize(
    "dataset, expected",
    [
        (CLASSIC, HOLDS),
        (SKEW, HOLDS),
        (SINGLE_ROW, HOLDS),
        (NULLS, HOLDS),
        (TRAP, set()),
        (NO_MVD, set()),
    ],
    ids=["classic", "skew", "single_row", "nulls", "trap", "no_mvd"],
)
def test_sampling_does_not_change_the_answer(
    spark, packed, dataset, expected, largest_k
):
    with_sampling = discovered_mvds(
        spark, packed, dataset, sampling_enabled=True, largest_k=largest_k
    )
    without_sampling = discovered_mvds(
        spark, packed, dataset, sampling_enabled=False, largest_k=largest_k
    )

    assert with_sampling == without_sampling == expected
