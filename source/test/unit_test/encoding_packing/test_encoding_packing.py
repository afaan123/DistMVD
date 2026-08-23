import pytest

CLASSIC = (
    ["course", "instructor", "book"],
    [
        ("CS", "A", "X"),
        ("CS", "A", "Y"),
        ("CS", "B", "X"),
        ("CS", "B", "Y"),
        ("EE", "C", "Z"),
    ],
)

NULLS = (["a", "b", "c"], [("1", "x", None), ("1", "x", "5"), ("2", "y", "6")])


def unpack(code, bits, num_columns):
    mask = (1 << bits) - 1
    return tuple(
        (code >> (column * bits)) & mask
        for column in reversed(range(num_columns))
    )


def encode_rows(rows, encoders):
    return [
        tuple(encoders[column][value] for column, value in enumerate(row))
        for row in rows
    ]


@pytest.mark.parametrize(
    "cardinality, expected_bits",
    [(0, 1), (1, 1), (3, 2), (4, 3), (5, 3), (255, 8)],
)
def test_bits_for_cardinality(cardinality, expected_bits):
    from packing import bits_for_cardinality

    assert bits_for_cardinality(cardinality) == expected_bits


def test_encoders_on_nulls(spark):
    from workers import build_value_encoders

    columns, rows = NULLS
    raw_rdd = spark.sparkContext.parallelize(rows, numSlices=2)

    assert build_value_encoders(raw_rdd, len(columns)) == [
        {"1": 1, "2": 2},
        {"x": 1, "y": 2},
        {"5": 1, "6": 2, None: 3},
    ]


def test_packed_codes_on_classic(packed):
    packed_rdd, bits = packed(*CLASSIC)

    assert bits == 2
    assert packed_rdd.collect() == [
        0b01_01_01,
        0b01_01_10,
        0b01_10_01,
        0b01_10_10,
        0b10_11_11,
    ]


def test_packing_is_lossless(spark, packed):
    from workers import build_value_encoders

    columns, rows = CLASSIC
    packed_rdd, bits = packed(columns, rows)
    raw_rdd = spark.sparkContext.parallelize(rows, numSlices=2)
    encoders = build_value_encoders(raw_rdd, len(columns))

    unpacked = [
        unpack(code, bits, len(columns)) for code in packed_rdd.collect()
    ]

    assert unpacked == encode_rows(rows, encoders)
