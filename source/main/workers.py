from collections import defaultdict
from pyspark import StorageLevel

def build_value_encoders(raw_rdd, num_attributes):
    def find_unique_values(rows):
        unique_values = []
        for column in range(num_attributes):
            unique_values.append(set())
        for row in rows:
            for column in range(num_attributes):
                unique_values[column].add(row[column])
        yield unique_values

    def merge_unique_values(left_partition, right_partition):
        for column in range(num_attributes):
            left_partition[column].update(right_partition[column])
        return left_partition

    unique_values_per_column = (
        raw_rdd
        # STAGE 1 (64 tasks for 64 partitions)
        # -> df.rdd.map(lambda row: tuple(row)) (main.py)
        # -> mapPartitions(find_unique_values) (workers.py)
        # -> Narrow transformations are pipelined into one stage (no shuffle)
        # -> Each task produces one list of unique-value sets for its partition
        .mapPartitions(find_unique_values)

        # ACTION
        # -> treeReduce(merge_unique_values, depth=3) starts the Spark job
        # -> Repeatedly merges the 64 partition summaries into one final result
        # -> merge_unique_values() called 63 times (64 -> 1)
        # -> Only 64 aggregated summaries are shuffled, not the original rows
        # -> depth=3 builds a balanced reduction tree to reduce communication
        .treeReduce(merge_unique_values, depth=3)
    )
    # Driver
    encoders = []
    for column_values in unique_values_per_column:
        sorted_values = sorted(column_values, key=str)
        value_encoder = {}
        next_id = 1
        for value in sorted_values:
            value_encoder[value] = next_id
            next_id += 1
        encoders.append(value_encoder)
    return encoders


def create_packed_rdd(sc, raw_rdd, encoders, num_attributes, bits):
    encoders_bc = sc.broadcast(encoders)
    def encode_and_pack_partition(rows):
        local_encoders = encoders_bc.value
        for values in rows:
            packed = 0
            for i in range(num_attributes):
                value = local_encoders[i].get(values[i], 0)
                shifted = packed << bits
                packed = shifted | value
            yield packed

    # STAGE (narrow transformation, no shuffle)
    # -> mapPartitions(encode_and_pack_partition): 64 tasks (= num_partitions)
    packed = raw_rdd.mapPartitions(encode_and_pack_partition)
    print("[ENCODE] packed_rdd storage: memory_and_disk")
    packed.persist(StorageLevel.MEMORY_AND_DISK)
    return packed


def create_encoded_rdd(sc, raw_rdd, encoders, num_attributes):
    encoders_bc = sc.broadcast(encoders)
    def encode_to_tuple_partition(rows):
        local_encoders = encoders_bc.value
        for values in rows:
            encoded_values = []
            for column in range(num_attributes):
                encoded_value = local_encoders[column].get(values[column], 0)
                encoded_values.append(encoded_value)
            yield tuple(encoded_values)

    # STAGE (narrow transformation, no shuffle)
    # -> mapPartitions(encode_to_tuple_partition): 64 tasks (= num_partitions)
    encoded = raw_rdd.mapPartitions(encode_to_tuple_partition)
    print("[ENCODE] encoded_tuple_rdd storage: memory_and_disk")
    encoded.persist(StorageLevel.MEMORY_AND_DISK)
    return encoded


def create_raw_rdd(raw_rdd):
    # raw_rdd is already an RDD of tuples (main.py: df.rdd.map(tuple)), so there is nothing to encode
    print("[ENCODE] raw_tuple_rdd storage: memory_and_disk (no encoding)")
    raw_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    return raw_rdd



_VIOLATED_HASH = -1
_FD_VIOLATED = "_FD_VIOLATED_SENTINEL"


def validate_partition_fds_packed(packed_rows, fd_meta_bc, bits):
    max_value = (1 << bits) - 1
    fd_meta  = fd_meta_bc.value
    observations = {}
    violated = set()


    for packed in packed_rows:
        for lhs_id, lhs_shifts, rhs_info in fd_meta:
            x_hash = 0
            # Xhash is lhs pair (A,B) -> (2,5) -> 21
            for shift in lhs_shifts:
                extracted_value = (packed >> shift) & max_value
                x_hash = x_hash << bits
                x_hash = x_hash | extracted_value

            for rhs_attr, rhs_shift in rhs_info:
                fd_key = (lhs_id, rhs_attr)
                if fd_key in violated:
                    continue

                y_value  = (packed >> rhs_shift) & max_value
                obs_key  = (lhs_id, rhs_attr, x_hash)

                if obs_key not in observations:
                    observations[obs_key] = y_value
                elif observations[obs_key] != _FD_VIOLATED and observations[obs_key] != y_value:
                    observations[obs_key] = _FD_VIOLATED
                    violated.add(fd_key)

    for obs_key, y_value in observations.items():
        if (obs_key[0], obs_key[1]) in violated:
            continue
        yield (obs_key, y_value)
    for lhs_id, rhs_attr in violated:
        yield ((lhs_id, rhs_attr, _VIOLATED_HASH), _FD_VIOLATED)


_FD_UNSET = "_FD_UNSET_SENTINEL"


def accumulate_fd_value(acc, y_value):
    if acc == _FD_UNSET:
        return y_value
    if acc == _FD_VIOLATED:
        return _FD_VIOLATED
    if y_value == _FD_VIOLATED:
        return _FD_VIOLATED
    if acc != y_value:
        return _FD_VIOLATED
    return acc


def merge_fd_values(a, b):
    if a == _FD_UNSET: return b
    if b == _FD_UNSET: return a
    if a == _FD_VIOLATED or b == _FD_VIOLATED:
        return _FD_VIOLATED
    if a != b:
        return _FD_VIOLATED
    return a



def validate_partition_fds_unpacked(packed_rows, fd_meta_bc, bits=None):
    fd_meta = fd_meta_bc.value
    observations = {}
    violated = set()

    for row in packed_rows:
        for lhs_id, lhs_cols, rhs_info in fd_meta:
            lhs_values = []
            for column in lhs_cols:
                lhs_values.append(row[column])
            x_hash = tuple(lhs_values)

            for rhs_attr, rhs_col in rhs_info:
                fd_key = (lhs_id, rhs_attr)
                if fd_key in violated:
                    continue
                y_value = row[rhs_col]
                obs_key = (lhs_id, rhs_attr, x_hash)
                if obs_key not in observations:
                    observations[obs_key] = y_value
                elif observations[obs_key] != _FD_VIOLATED and \
                        observations[obs_key] != y_value:
                    observations[obs_key] = _FD_VIOLATED
                    violated.add(fd_key)

    for obs_key, y_value in observations.items():
        if (obs_key[0], obs_key[1]) in violated:
            continue
        yield (obs_key, y_value)
    for lhs_id, rhs_attr in violated:
        yield ((lhs_id, rhs_attr, _VIOLATED_HASH), _FD_VIOLATED)




def group_count_emit(packed_rows, lhs_meta_bc, bits):
    max_value = (1 << bits) - 1
    lhs_meta = lhs_meta_bc.value
    local_counts = defaultdict(int)
    for packed in packed_rows:
        for lhs_id, lhs_shifts in lhs_meta:
            x_hash = 0
            for shift in lhs_shifts:
                value = (packed >> shift) & max_value
                x_hash = (x_hash << bits) | value
            local_counts[(lhs_id, x_hash)] += 1
    for key, count in local_counts.items():
        yield (key, count)

def group_count_emit_tuple(packed_rows, lhs_meta_bc, bits=None):
    lhs_meta = lhs_meta_bc.value
    local_counts = defaultdict(int)
    for row in packed_rows:
        for lhs_id, lhs_cols in lhs_meta:
            x_hash = tuple(row[c] for c in lhs_cols)
            local_counts[(lhs_id, x_hash)] += 1
    for key, count in local_counts.items():
        yield (key, count)



def validate_emit(packed_rows, meta_bc, sample_map_bc, bits):
    max_value         = (1 << bits) - 1
    meta              = meta_bc.value
    sample_map        = sample_map_bc.value if sample_map_bc is not None else None

    seen_per_group = defaultdict(set)

    for packed in packed_rows:
        for lhs_idx, lhs_shifts, subsets in meta:

            x_hash = 0
            for shift in lhs_shifts:
                x_hash = (x_hash << bits) | ((packed >> shift) & max_value)

            if sample_map is not None:
                allowed_set = sample_map[lhs_idx]
                if allowed_set is None or x_hash not in allowed_set:
                    continue

            for subset, subset_shifts in subsets:

                projection_hash = 0
                for shift in subset_shifts:
                    projection_hash = (projection_hash << bits) | \
                                      ((packed >> shift) & max_value)

                group_key = (lhs_idx, subset, x_hash)
                seen_set  = seen_per_group[group_key]
                if projection_hash in seen_set:
                    continue
                seen_set.add(projection_hash)

                yield (group_key, projection_hash)






def validate_emit_tuple(packed_rows, meta_bc, sample_map_bc, bits=None):
    meta              = meta_bc.value
    sample_map        = sample_map_bc.value if sample_map_bc is not None else None

    seen_per_group = defaultdict(set)

    for row in packed_rows:
        for lhs_idx, lhs_cols, subsets in meta:

            x_hash = tuple(row[c] for c in lhs_cols)

            if sample_map is not None:
                allowed_set = sample_map[lhs_idx]
                if allowed_set is None or x_hash not in allowed_set:
                    continue

            for subset, subset_cols in subsets:
                projection_hash = tuple(row[c] for c in subset_cols)

                group_key = (lhs_idx, subset, x_hash)
                seen_set = seen_per_group[group_key]
                if projection_hash in seen_set:
                    continue
                seen_set.add(projection_hash)

                yield (group_key, projection_hash)