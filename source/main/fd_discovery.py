import time
from workers import validate_partition_fds_packed, validate_partition_fds_unpacked,accumulate_fd_value, merge_fd_values, _FD_UNSET, _FD_VIOLATED
from meta import prepare_fd_shifts
import sys

class FDDiscovery:
    def __init__(self, sc, packed_rdd, num_attributes, shuffle_partitions,
                 num_partitions, attr_names, stats=None, packed=True, bits=None,
                 superkey_threshold=0.80):
        self.sc = sc
        self.packed_rdd = packed_rdd
        self.num_attributes = num_attributes
        self.shuffle_partitions = shuffle_partitions
        self.num_partitions = num_partitions
        self.attr_names = attr_names
        self.stats = stats
        self.packed = packed
        self.bits = bits
        self.superkey_threshold = superkey_threshold
        self.all_attributes = frozenset(range(num_attributes))

    def discover_for_level(self, lhs_list, level, closure):
        start_time = time.time()

        fd_candidates = self.generate_fd_candidates(lhs_list, closure)
        if not fd_candidates:
            if self.stats:
                self.stats.record_elapsed("fd_time", start_time)
            print(f"[FD L{level}] no candidates")
            return set()

        fd_meta = prepare_fd_shifts(fd_candidates, self.num_attributes,self.bits, packed=self.packed)
        meta_bc = self.sc.broadcast(fd_meta)
        bits = self.bits
        shuffle_partitions = self.shuffle_partitions
        num_partitions     = self.num_partitions
        emit_fn = validate_partition_fds_packed if self.packed else validate_partition_fds_unpacked

        # Prior (already materialized in mvd.py __init__):
        # -> raw_rdd = df.rdd.map(tuple) (main.py, narrow, lazy — no job by itself)
        # -> create_*_rdd: mapPartitions + persist(MEMORY_AND_DISK) (workers.py, narrow, lazy)
        # -> packed_rdd.count() ACTION (mvd.py) — 1 job, 1 stage, 64 tasks, cache filled

        # STAGE 1 (narrow, no shuffle) -> packed_rdd.mapPartitions(emit_fn)
        # -> reads cached packed_rdd partitions (64 tasks) + meta_bc broadcast
        # -> each task yields ((lhs, rhs, x_hash), y) per partition -> No job yet
        # STAGE 2 (wide shuffle) -> aggregateByKey(accumulate, merge)
        # -> shuffle across shuffle_partitions (= 64) -> No job yet
        per_group = (self.packed_rdd
                     .mapPartitions(
                         lambda rows: emit_fn(rows, meta_bc, bits))
                     .aggregateByKey(
                         _FD_UNSET,
                         accumulate_fd_value,
                         merge_fd_values,
                         numPartitions=shuffle_partitions))
        
        # STAGE 3 (narrow, no shuffle) -> map: drop x_hash, value -> bool -> No job yet
        def get_fd_status(record):
            observation_key, observed_value = record
            lhs_id = observation_key[0]
            rhs_attr = observation_key[1]
            is_valid = observed_value != _FD_VIOLATED
            return (lhs_id, rhs_attr), is_valid

        per_fd_bool = per_group.map(get_fd_status)
        
        # STAGE 4 (wide shuffle) -> reduceByKey(and) -> filter(True) -> map frozenset -> No job yet
        def combine_fd_results(left_result, right_result):
            return left_result and right_result
        def keep_valid_fd(record):
            fd_key, is_valid = record
            return is_valid
        def format_fd(record):
            fd_key, is_valid = record
            lhs = fd_key[0]
            rhs = fd_key[1]
            return frozenset(lhs), rhs

        per_fd = (
            per_fd_bool
            .reduceByKey(
                combine_fd_results,
                numPartitions=max(1, num_partitions)
            )
            .filter(keep_valid_fd)
            .map(format_fd)
        )

        # ACTION -> collect() -> 1 Job -> 4 Stages -> FD set returned to driver
        discovered_fds = set(per_fd.collect())
        meta_bc.unpersist(False)

        self.print_discovered(discovered_fds, level)

        if self.stats:
            self.stats.record_stat("fd_shuffle_jobs", 2)
            self.stats.record_stat("total_fds_discovered", len(discovered_fds))
            self.stats.record_elapsed("fd_time", start_time)

        return discovered_fds

    def generate_fd_candidates(self, lhs_list, closure):
        candidates = []
        for lhs in lhs_list:
            if closure.check_superkey(lhs):
                continue
            closure_set = closure.compute_closure(lhs)
            rhs = []
            for attr in range(self.num_attributes):
                if attr not in lhs and attr not in closure_set:
                    rhs.append(attr)
            if len(rhs) > 0:
                candidates.append((tuple(sorted(lhs)), rhs))
        return candidates

    def should_run_fd_discovery(self, level, lhs_by_level, closure):
        if level == 1:
            return True
        previous_level = lhs_by_level[level - 1]
        superkeys = 0
        for lhs in previous_level:
            if closure.check_superkey(lhs):
                superkeys += 1
        ratio = superkeys / len(previous_level)
        if ratio < self.superkey_threshold:
            return True
        return False

    def _attr_name(self, i):
        if 0 <= i < len(self.attr_names):
            return self.attr_names[i]
        print(f"[DBG] attribute index {i} out of range: "
              f"num_attributes={self.num_attributes} "
              f"len(attr_names)={len(self.attr_names)}", file=sys.stderr)
        return f"attr{i}"

    def print_discovered(self, fd_set, level):
        print(f"    found {len(fd_set)} FDs")
        for lhs, rhs in sorted(fd_set, key=lambda fr: (sorted(fr[0]), fr[1])):
            lhs_names = [self._attr_name(i) for i in sorted(lhs)]
            print(f"      {tuple(lhs_names)} -> {self._attr_name(rhs)}")