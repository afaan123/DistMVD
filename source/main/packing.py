def bits_for_cardinality(cardinality):
    n = int(cardinality) if cardinality else 0
    return max(1, n.bit_length())


def max_value_for_bits(bits):
    return (1 << bits) - 1


def create_shift_values(columns, total_columns, bits):
    shifts = []
    for col in columns:
        shift = (total_columns - 1 - col) * bits
        shifts.append(shift)
    return shifts


