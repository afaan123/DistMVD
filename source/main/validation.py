import time
from collections import defaultdict
from pyspark.rdd import portable_hash
from workers import validate_emit, validate_emit_tuple
from meta import build_validation_meta


class Validation:
    def __init__(self, sc, packed_rdd, num_attributes, shuffle_partitions,
                 sampling, stats=None, packed=True, bits=None):
        self.sc = sc
        self.packed_rdd = packed_rdd
        self.num_attributes = num_attributes
        self.shuffle_partitions = shuffle_partitions
        self.sampling = sampling
        self.stats = stats
        self.packed = packed
        self.bits = bits

    def validate_level(self, candidates_by_lhs, discovered_mvds):
        sampling_start = time.time()
        if not candidates_by_lhs:
            return

        sample_map, group_sizes_per_lhs = self.sampling.plan(list(candidates_by_lhs.keys()))

        meta_for_worker, lhs_id_list = build_validation_meta(
            candidates_by_lhs, self.num_attributes, self.bits, packed=self.packed)

        if sample_map:
            sample_map_list = []
            for lhs_id in lhs_id_list:
                sampled_groups = sample_map.get(lhs_id)
                sample_map_list.append(sampled_groups)
        else:
            sample_map_list = None

        rejected = self.run_sample_pass(meta_for_worker, candidates_by_lhs,sample_map_list, lhs_id_list)

        if self.stats:
            self.stats.record_elapsed("sampling_time", sampling_start)

        validation_start = time.time()
        print(f"    Full validation:")
        self.run_full_pass(candidates_by_lhs,rejected, discovered_mvds)

        if self.stats:
            self.stats.record_elapsed("validation_time", validation_start)

    def run_sample_pass(self, meta_for_worker, candidates_by_lhs,sample_map_list, lhs_id_list):
        
        rejected = set()
        if sample_map_list is None:
            return rejected

        outcomes = self.run_validation_pass(meta_for_worker, candidates_by_lhs,sample_map_list=sample_map_list,
            lhs_id_list=lhs_id_list,label="SAMPLE")

        for (lhs_id, candidate_id), holds in outcomes.items():
            if not holds:
                rejected.add((lhs_id, candidate_id))

        print(f"      sample pass rejected {len(rejected)} candidates")
        if self.stats:
            self.stats.record_stat("rejected_by_sampling", len(rejected))
        return rejected

    def run_full_pass(self, candidates_by_lhs,rejected, discovered_mvds):

        survivors = defaultdict(list)
        survivor_count = 0
        for lhs_id, candidates in candidates_by_lhs.items():
            for candidate_id, (rhs_t, z_t) in enumerate(candidates):
                if (lhs_id, candidate_id) not in rejected:
                    survivors[lhs_id].append((rhs_t, z_t))
                    survivor_count += 1

        print(f"      survivors entering full pass: {survivor_count}")
        if not survivors:
            return

        meta_for_full, full_lhs_id_list = build_validation_meta(
            dict(survivors), self.num_attributes, self.bits, packed=self.packed)

        outcomes = self.run_validation_pass(
            meta_for_full, dict(survivors),
            sample_map_list=None,
            lhs_id_list=full_lhs_id_list,
            label="FULL")

        for lhs_id, candidates in survivors.items():
            for local_id, (rhs_t, _z_t) in enumerate(candidates):
                if outcomes.get((lhs_id, local_id), False):
                    discovered_mvds.add((frozenset(lhs_id), frozenset(rhs_t)))

    def run_validation_pass(self, meta_for_worker, candidates_by_lhs,sample_map_list, lhs_id_list, label):
        shuffle_partitions = self.shuffle_partitions
        packed = self.packed
        bits = self.bits
        emit_fn = validate_emit if packed else validate_emit_tuple

        meta_bc = self.sc.broadcast(meta_for_worker)

        if sample_map_list is not None:
            sample_bc = self.sc.broadcast(sample_map_list)
        else:
            sample_bc = None

        candidate_map = {}
        for lhs_id, candidates in candidates_by_lhs.items():
            candidate_map[lhs_id] = list(candidates)
        candidate_metadata = {
            "lhs_id_list": lhs_id_list,
            "candidates": candidate_map,
        }
        cand_meta_bc = self.sc.broadcast(candidate_metadata)

        emitted = self.packed_rdd.mapPartitions(
            lambda rows: emit_fn(rows, meta_bc, sample_bc, bits))


        def _xgroup_partition(key):
            lhs_idx, _subset, x_hash = key
            return portable_hash((lhs_idx, x_hash))

        def add_projection_hash(acc, proj_hash):
            acc.add(proj_hash)
            return acc

        def merge_hash_sets(acc_a, acc_b):
            acc_a.update(acc_b)
            return acc_a

        counts = (emitted
                  .aggregateByKey(set(), add_projection_hash, merge_hash_sets,
                                  numPartitions=shuffle_partitions,
                                  partitionFunc=_xgroup_partition)
                  .mapValues(len))

        def check_partition(count_items):
            from collections import defaultdict as _dd

            cm             = cand_meta_bc.value
            lhs_id_list_l  = cm["lhs_id_list"]
            all_candidates = cm["candidates"]

            per_group = _dd(dict)
            for (lhs_idx, subset, x_hash), count in count_items:
                per_group[(lhs_idx, x_hash)][subset] = count

            verdicts = {}

            for (lhs_idx, x_hash), subset_counts in per_group.items():
                lhs_id     = lhs_id_list_l[lhs_idx]
                candidates = all_candidates.get(lhs_id, [])

                for cand_id, (rhs_t, z_t) in enumerate(candidates):
                    key = (lhs_id, cand_id)

                    if verdicts.get(key) is False:
                        continue

                    union_t = tuple(sorted(set(rhs_t) | set(z_t)))

                    ny = subset_counts.get(rhs_t,   0)
                    nz = subset_counts.get(z_t,     0)
                    nu = subset_counts.get(union_t, 0)

                    if nu != ny * nz:
                        verdicts[key] = False
                    else:
                        if key not in verdicts:
                            verdicts[key] = True

            yield from verdicts.items()

        raw_verdicts = (counts
                        .mapPartitions(check_partition)
                        .reduceByKey(lambda a, b: a and b,
                                     numPartitions=shuffle_partitions))

        outcomes_raw = raw_verdicts.collect()

        meta_bc.unpersist(False)
        if sample_bc is not None:
            sample_bc.unpersist(False)
        cand_meta_bc.unpersist(False)

        if self.stats:
            self.stats.record_stat("mvd_shuffle_jobs", 2)

        outcomes = {}

        for (lhs_id, cand_id), holds in outcomes_raw:
            outcomes[(lhs_id, cand_id)] = holds

        for lhs_id, candidates in candidates_by_lhs.items():
            for cand_id in range(len(candidates)):
                key = (lhs_id, cand_id)
                if key not in outcomes:
                    outcomes[key] = True

        print(f"      {label.lower()} pass checked {len(outcomes)} (lhs, cand) outcomes")
        return outcomes

