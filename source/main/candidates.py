import time
from collections import defaultdict
from helper import generate_candidates_partition
import sys

class Candidates:
    def __init__(self, sc, num_attributes, num_partitions, pruning, stats=None):
        self.sc = sc
        self.num_attributes = num_attributes
        self.num_partitions = num_partitions
        self.stats = stats
        self.pruning = pruning
        self.all_attributes_tuple = tuple(range(num_attributes))

    def generate(self, lhs_list, closure, discovered_mvds):
        start_time = time.time()

        lhs_tuples = []
        for lhs in lhs_list:
            sorted_lhs = sorted(lhs)
            tuple_lhs = tuple(sorted_lhs)
            lhs_tuples.append(tuple_lhs)
        if not lhs_tuples:
            return {}, []

        gen_state_bc = self.build_state_broadcast(lhs_tuples, closure, discovered_mvds)

        # STAGE 1 (mapPartitions) -> generate_candidates_partition -> mapPartitions ->
        #  1 Job -> 1 Stage -> mapPartitions + persist(MEMORY_AND_DISK) 
        num_slices = min(self.num_partitions, max(1, len(lhs_tuples)))
        candidate_rdd = (self.sc.parallelize(lhs_tuples, numSlices=num_slices)
            .mapPartitions(lambda items: generate_candidates_partition(items, gen_state_bc)))

        candidates_to_validate = defaultdict(list)
        auto_validated = []
        counters = {"SKIP_COMPLEMENT": 0, "AUTO_SK": 0, "AUTO_FD": 0, "SKIP_SUPERSET": 0, "VALIDATE": 0}

        for kind, lhs_t, rhs_t, z_t in candidate_rdd.collect():
            counters[kind] += 1
            if kind == "AUTO_SK" or kind == "AUTO_FD":
                auto_validated.append((frozenset(lhs_t), frozenset(rhs_t)))
            elif kind == "VALIDATE":
                candidates_to_validate[lhs_t].append((rhs_t, z_t))
        gen_state_bc.unpersist(False)

        self.record_stats(counters, start_time)
        self.print_summary(counters, candidates_to_validate)

        return dict(candidates_to_validate), auto_validated

    def build_state_broadcast(self, lhs_tuples, closure, discovered_mvds):
        lhs_tuple_set = set(lhs_tuples)
        closure_map_t = closure.get_requested_closures(lhs_tuple_set)

        all_set = set(self.all_attributes_tuple)
        validated_mvd_lhs = defaultdict(set)

        if self.pruning["discovered_mvd"]:
            for lhs, rhs in discovered_mvds:
                rhs_t = tuple(sorted(rhs))
                lhs_t = tuple(sorted(lhs))
                validated_mvd_lhs[rhs_t].add(lhs_t)

                z_t = tuple(sorted(all_set - set(lhs_t) - set(rhs_t)))
                if z_t:
                    validated_mvd_lhs[z_t].add(lhs_t)

        return self.sc.broadcast({
            "all_attributes":    self.all_attributes_tuple,
            "closure_map":       closure_map_t,
            "validated_mvd_lhs": dict(validated_mvd_lhs),
            "pruning":           self.pruning,
        })

    def record_stats(self, counters, start_time):
        if not self.stats:
            return
        total = sum(counters.values())
        self.stats.record_stat("total_candidates_generated", total)
        self.stats.record_stat("skipped_complement", counters["SKIP_COMPLEMENT"])
        self.stats.record_stat("auto_validated_fd", counters["AUTO_FD"])
        self.stats.record_stat("auto_validated_superkey", counters["AUTO_SK"])
        self.stats.record_stat("pruned_superset", counters["SKIP_SUPERSET"])
        self.stats.record_elapsed("candidate_time", start_time)

    def print_summary(self, counters, candidates_to_validate):
        total = sum(counters.values())
        to_validate = sum(len(v) for v in candidates_to_validate.values())
        print(f"    generated={total}  skip-complement={counters['SKIP_COMPLEMENT']}  "
              f"auto-FD={counters['AUTO_FD']}  auto-SK={counters['AUTO_SK']}  "
              f"pruned-superset={counters['SKIP_SUPERSET']}")
        print(f"    {to_validate} need validation")