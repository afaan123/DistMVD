from packing import create_shift_values
import sys

def prepare_column_indices(cols, num_cols, packed, bits):
    if packed:
        column_shifts = create_shift_values(list(cols), num_cols, bits)
        return column_shifts
    else:
        column_indices = []
        for column in cols:
            column_indices.append(int(column))
        return column_indices


def prepare_fd_shifts(fd_candidates, num_cols, bits, packed=True):
    result = []
    for lhs, rhs_list in fd_candidates:
        lhs_proj = prepare_column_indices(lhs, num_cols, packed, bits)
        rhs_proj_values = prepare_column_indices(rhs_list, num_cols, packed, bits)
        rhs_proj = []
        for rhs, proj in zip(rhs_list, rhs_proj_values):
            rhs_proj.append((rhs, proj))
        result.append((lhs, lhs_proj, rhs_proj))
    return result


def build_lhs_count_meta(lhs_keys, num_cols, bits, packed=True):
    meta = []
    for lhs_t in lhs_keys:
        proj = prepare_column_indices(lhs_t, num_cols, packed, bits)
        meta.append((lhs_t, proj))
    return meta


def build_validation_meta(candidates_by_lhs, num_cols, bits, packed=True):
    meta        = []
    lhs_id_list = []

    for lhs_idx, (lhs_id, candidates) in enumerate(candidates_by_lhs.items()):
        lhs_id_list.append(lhs_id)

        unique_subsets = set()
        for rhs_t, z_t in candidates:
            unique_subsets.add(rhs_t)
            unique_subsets.add(z_t)
            unique_subsets.add(tuple(sorted(set(rhs_t).union(set(z_t)))))
        ordered_subsets = sorted(unique_subsets, key=lambda s: (len(s), s))

        lhs_proj = prepare_column_indices(lhs_id, num_cols, packed, bits)

        subsets_with_proj = []
        for subset in ordered_subsets:
            proj = prepare_column_indices(subset, num_cols, packed, bits)
            subsets_with_proj.append((subset, proj))

        meta.append((lhs_idx, lhs_proj, subsets_with_proj))

    return meta, lhs_id_list