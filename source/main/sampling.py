from collections import defaultdict
from workers import group_count_emit, group_count_emit_tuple
from meta import build_lhs_count_meta
import sys

class Sampling:
    def __init__(self, sc, packed_rdd, num_attributes, shuffle_partitions,
                 sampling_enabled, largest_k, stats=None, packed=True,
                 mode="manual", auto_coverage=0.5, auto_min_k=3, auto_max_k=50,
                 bits=None):
        self.sc = sc
        self.packed_rdd = packed_rdd
        self.num_attributes = num_attributes
        self.packed = packed
        self.bits = bits
        self.shuffle_partitions = shuffle_partitions
        self.sampling_enabled = sampling_enabled
        self.largest_k = largest_k
        self.stats = stats

        mode = mode if mode is not None else "manual"
        self.mode = mode.lower()
        if self.mode not in ("manual", "auto"):
            print(f"[SAMPLING] unknown mode '{mode}', falling back to 'manual'.")
            self.mode = "manual"
        self.auto_coverage = auto_coverage
        self.auto_min_k = auto_min_k
        self.auto_max_k = auto_max_k

    def plan(self, lhs_keys_to_sample):
        if not self.sampling_enabled or not lhs_keys_to_sample:
            return {}, {}

        group_sizes_per_lhs = self.compute_group_sizes(lhs_keys_to_sample)
        sample_map, chosen_sizes = self.select_samples(group_sizes_per_lhs)
        self.print_decisions(group_sizes_per_lhs, sample_map, chosen_sizes)
        self.record_stats(group_sizes_per_lhs, sample_map)

        return sample_map, dict(group_sizes_per_lhs)


    def compute_group_sizes(self, lhs_keys):
        lhs_meta = build_lhs_count_meta(lhs_keys, self.num_attributes, self.bits, packed=self.packed)
        lhs_meta_bc = self.sc.broadcast(lhs_meta)
        bits = self.bits
        shuffle_partitions = self.shuffle_partitions
        count_fn = group_count_emit if self.packed else group_count_emit_tuple
        counts = (self.packed_rdd
                  .mapPartitions(lambda rows: count_fn(rows, lhs_meta_bc, bits))
                  .reduceByKey(lambda a, b: a + b, numPartitions=shuffle_partitions)
                  .collect())

        lhs_meta_bc.unpersist(False)
        per_lhs = defaultdict(list)
        for (lhs_id, x_hash), size in counts:
            per_lhs[lhs_id].append((size, x_hash))
        return per_lhs

    def select_samples(self, groups_per_lhs):
        sample_map = {}
        chosen_sizes = []
        for lhs_id, groups in groups_per_lhs.items():
            sorted_groups = sorted(
                groups,
                key=lambda group: group[0],
                reverse=True
            )
            sample_count = self.determine_sample_count(groups)
            selected_groups = sorted_groups[:sample_count]
            sampled_hashes = set()
            for group_size, x_hash in selected_groups:
                sampled_hashes.add(x_hash)
                chosen_sizes.append(group_size)
            sample_map[lhs_id] = frozenset(sampled_hashes)
        return sample_map, chosen_sizes


    def determine_sample_count(self, sized_groups):
        if self.mode == "manual":
            return self.largest_k
        num_groups = len(sized_groups)
        if num_groups == 0:
            return 0
        group_sizes = []
        for group_size, x_hash in sized_groups:
            group_sizes.append(group_size)
        group_sizes.sort(reverse=True)
        total_tuples = sum(group_sizes)
        required_tuples = self.auto_coverage * total_tuples
        covered_tuples = 0
        groups_to_sample = 0
        for group_size in group_sizes:
            covered_tuples += group_size
            groups_to_sample += 1
            if covered_tuples >= required_tuples:
                break
        if groups_to_sample < self.auto_min_k:
            groups_to_sample = self.auto_min_k
        if groups_to_sample > self.auto_max_k:
            groups_to_sample = self.auto_max_k
        if self.auto_min_k <= groups_to_sample <= self.auto_max_k:
            groups_to_sample = groups_to_sample
        if groups_to_sample > num_groups:
            groups_to_sample = num_groups
        return groups_to_sample


    def print_decisions(self, per_lhs, sample_map, chosen_sizes):
        total_lhs   = len(per_lhs)
        num_sampled = len(sample_map)
        if self.mode == "manual":
            policy = f"manual, largest_k={self.largest_k}"
        else:
            policy = (f"auto, coverage={self.auto_coverage} "
                      f"k in [{self.auto_min_k}, {self.auto_max_k}]")
        print(f"    Sampling:")
        print(f"      {total_lhs} LHS  ({policy})")
        sampled_groups = sum(len(g) for g in sample_map.values())
        print(f"      sampled LHS: {num_sampled}   top groups picked: {sampled_groups:,}")
        if chosen_sizes:
            print(f"      group size: avg={sum(chosen_sizes) // len(chosen_sizes):,}  "
                  f"min={min(chosen_sizes):,}  max={max(chosen_sizes):,}  "
                  f"({len(chosen_sizes):,} groups)")

    def record_stats(self, per_lhs, sample_map):
        if not self.stats:
            return
        self.stats.record_stat("sampling_shuffle_jobs", 1)
        self.stats.record_stat("sampling_lhs_sampled", len(sample_map))
        self.stats.record_stat("sample_groups_scanned",
                               sum(len(groups) for groups in sample_map.values()))
        for sized_groups in per_lhs.values():
            self.stats.record_stat("total_groups", len(sized_groups))
            total_size = sum(size for size, _ in sized_groups)
            self.stats.record_stat("total_group_size", total_size)
            self.stats.update_max_stat("max_group_size", max(size for size, _ in sized_groups))