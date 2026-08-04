import math
import time
from itertools import combinations
from pyspark import StorageLevel
from packing import bits_for_cardinality
from workers import create_packed_rdd, build_value_encoders, create_encoded_rdd, create_raw_rdd
from closure import Closure
from fd_discovery import FDDiscovery
from candidates import Candidates
from sampling import Sampling
from validation import Validation
from minimality import Minimality
import sys

class MVDDiscovery:
    def __init__(self, raw_rdd, attr_names,
                 stats=None, sampling_config=None,
                 fd_config=None, pruning_config=None,
                 num_partitions=None,shuffle_partitions=None, total_rows=None,
                 representation_config=None):

        self.sc = raw_rdd.context
        self.attr_names = list(attr_names)
        self.num_attributes = len(self.attr_names)
        self.all_attributes = frozenset(range(self.num_attributes))
        self.total_rows = total_rows
        self.stats = stats
        self.cardinalities = []

        self.encoding_enabled = representation_config["encoding"] if representation_config else True
        self.packing_enabled = representation_config["packing"] if representation_config else True
        if self.packing_enabled and not self.encoding_enabled:
            print("WARNING: packing requires encoding; encoding is off, so packing is disabled (raw representation).")
            self.packing_enabled = False
        self.packed = self.packing_enabled
        if self.packing_enabled:
            self.representation = "packed"
        elif self.encoding_enabled:
            self.representation = "encoded"
        else:
            self.representation = "raw"

        self.fd_enabled = fd_config["enabled"] if fd_config else True
        self.fd_superkey_threshold = fd_config["superkey_threshold"] if fd_config else 0.80
        self.sampling_enabled = sampling_config["enabled"] if sampling_config else True
        self.largest_k = sampling_config["largest_k"] if sampling_config else 10
        self.sampling_mode = sampling_config["mode"] if sampling_config else "manual"
        auto_cfg = sampling_config["auto"] if sampling_config else None
        self.sampling_auto_coverage = auto_cfg["coverage"] if auto_cfg else 0.3
        self.sampling_auto_min_k = auto_cfg["min_k"] if auto_cfg else 3
        self.sampling_auto_max_k = auto_cfg["max_k"] if auto_cfg else 20
        self.num_partitions = num_partitions if num_partitions else self.sc.defaultParallelism
        self.shuffle_partitions = shuffle_partitions if shuffle_partitions else self.num_partitions

        self.pruning = {
            "complement":     pruning_config["complement_pruning"]["enabled"] if pruning_config else True,
            "superset":       pruning_config["superset_pruning"]["enabled"] if pruning_config else True,
            "fd":             pruning_config["fd_pruning"]["enabled"] if pruning_config else True,
            "discovered_mvd": pruning_config["discovered_mvd_pruning"]["enabled"] if pruning_config else True,
        }

        print(f"representation={self.representation} (encoding={self.encoding_enabled}, packing={self.packing_enabled})")
        
        print("PRUNE")
        for key, value in self.pruning.items():
            print(f"  {key} = {value}")

        raw_rdd.persist(StorageLevel.MEMORY_AND_DISK)

        self.encoders = None
        if self.encoding_enabled:
            print("[ENCODE] Building column encoders...")
            encode_start = time.time()
            self.encoders = build_value_encoders(raw_rdd, self.num_attributes)
            cardinalities = []
            for encoder in self.encoders:
                cardinalities.append(len(encoder))
            self.cardinalities = cardinalities
            print(f"[ENCODE] Cardinalities: {cardinalities}")
            self.bits = bits_for_cardinality(max(cardinalities) if cardinalities else 0)
            if self.stats:
                self.stats.record_elapsed("encoding_time", encode_start)
        else:
            self.bits = bits_for_cardinality(self.total_rows)

        self.bits_per_column = self.bits
        self.packed_bits = self.bits * self.num_attributes
        
        build_start = time.time()
        if self.packing_enabled:
            self.packed_rdd = create_packed_rdd(self.sc, raw_rdd, self.encoders, self.num_attributes, self.bits)
        elif self.encoding_enabled:
            self.packed_rdd = create_encoded_rdd(self.sc, raw_rdd, self.encoders, self.num_attributes)
        else:
            self.packed_rdd = create_raw_rdd(raw_rdd)
        """ ACTION -> count() -> triggers previous mapPartitions stage (create_*_rdd) -> 1 Job -> 1 Stage -> mapPartitions + persist(MEMORY_AND_DISK) 
        -> 64 Tasks (= num_partitions) -> each task reads one raw_rdd partition, encodes/maps rows, writes cache blocks -> 
        partial counts summed on driver """
        self.packed_rdd.count()

        if self.stats:
            if self.packing_enabled:
                self.stats.record_elapsed("packing_time", build_start)
            elif self.encoding_enabled:
                self.stats.record_elapsed("encoding_time", build_start)
            else:
                self.stats.record_elapsed("raw_time", build_start)



        print(f"Representation ready ({self.representation}).")

        self.closure = Closure(self.all_attributes)

        self.fd_discovery = FDDiscovery(
            sc=self.sc, packed_rdd=self.packed_rdd,
            num_attributes=self.num_attributes,
            shuffle_partitions=self.shuffle_partitions,
            num_partitions=self.num_partitions,
            attr_names=self.attr_names, stats=self.stats,
            packed=self.packed, bits=self.bits,
            superkey_threshold=self.fd_superkey_threshold)

        self.candidates = Candidates(
            sc=self.sc, num_attributes=self.num_attributes,
            num_partitions=self.num_partitions, stats=self.stats,
            pruning=self.pruning)

        self.sampling = Sampling(
            sc=self.sc, packed_rdd=self.packed_rdd,
            num_attributes=self.num_attributes,
            shuffle_partitions=self.shuffle_partitions,
            sampling_enabled=self.sampling_enabled,
            largest_k=self.largest_k,
            stats=self.stats,
            packed=self.packed,
            mode=self.sampling_mode,
            auto_coverage=self.sampling_auto_coverage,
            auto_min_k=self.sampling_auto_min_k,
            auto_max_k=self.sampling_auto_max_k,
            bits=self.bits)

        self.validation = Validation(
            sc=self.sc, packed_rdd=self.packed_rdd,
            num_attributes=self.num_attributes,
            shuffle_partitions=self.shuffle_partitions,
            sampling=self.sampling,
            stats=self.stats,
            packed=self.packed, bits=self.bits)

        self.minimality = Minimality(
            all_attributes=self.all_attributes,
            attr_names=self.attr_names, stats=self.stats)

        self.functional_dependencies = set()
        self.discovered_mvds = set()

        if self.stats:
            self.stats.print_config(
                fd_enabled = self.fd_enabled,
                sampling_enabled = self.sampling_enabled,
                bits = self.bits,
                num_attributes = self.num_attributes,
                num_partitions = self.num_partitions,
                shuffle_partitions = self.shuffle_partitions,
                total_rows  = self.total_rows)

    def discovery(self):

        lhs_by_level={}
        for level in range(1,self.num_attributes-1):
            current_level=[]
            for c in combinations(range(self.num_attributes),level):
                current_level.append(frozenset(c))
            lhs_by_level[level]=current_level

        for level in range(1, self.num_attributes - 1):
            print(f"\n{'=' * 50}")
            print(f"  LEVEL {level}")
            print(f"{'=' * 50}")
            self.stats.record_level_stats(lhs_by_level[level])

            if level > 1:
                self.closure.inherit_subset_closures(lhs_by_level[level])

            if self.fd_enabled and self.fd_discovery.should_run_fd_discovery(level, lhs_by_level, self.closure):
                print(f"\nFD discovery")
                level_fds = self.fd_discovery.discover_for_level(lhs_by_level[level], level, self.closure)
                new_fds = level_fds - self.functional_dependencies
                self.functional_dependencies = self.functional_dependencies.union(level_fds)

                lhs_so_far = []
                for k in range(1, level + 1):
                    for lhs in lhs_by_level[k]:
                        lhs_so_far.append(lhs)

                closure_start = time.time()

                self.closure.expand_closures(new_fds, lhs_so_far)
                if self.stats:
                    self.stats.record_elapsed("closure_time", closure_start)
            else:
                if self.fd_enabled:
                    print(f"\nFD discovery")
                    print(f"    skipped (most level-{level-1} LHS are superkeys)")
                else:
                    for lhs in lhs_by_level[level]:
                        self.closure.initialize_closure(lhs)



            print(f"\nCandidate generation")
            candidates_to_validate, auto_validated = self.candidates.generate(lhs_by_level[level], self.closure, self.discovered_mvds)
            for lhs, rhs in auto_validated:
                self.discovered_mvds.add((lhs, rhs))

            mvds_before = len(self.discovered_mvds)

            if candidates_to_validate:
                print(f"\nMVD validation")
                self.validation.validate_level(candidates_to_validate, self.discovered_mvds)
            else:
                print(f"\nMVD validation")
                print(f"    skipped (no candidates need validation)")

            new_this_level = len(self.discovered_mvds) - mvds_before
            print(f"\nLEVEL {level} done - {new_this_level} new MVD"
                  f" (running total: {len(self.discovered_mvds)})")

        cleanup_start = time.time()
        result = self.minimality.cleanup(self.discovered_mvds)
        if self.stats:
            self.stats.record_elapsed("cleanup_time", cleanup_start)
        return result