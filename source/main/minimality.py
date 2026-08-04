from collections import defaultdict
from itertools import combinations


class Minimality:
    def __init__(self, all_attributes, attr_names, stats=None):
        self.all_attributes = all_attributes
        self.attr_names = attr_names
        self.stats = stats

    def cleanup(self, discovered_mvds):
        indexes = self.build_indexes(discovered_mvds)
        rejection_counters = [0, 0]

        final_mvds = set()
        seen_canonical = set()
        for lhs, rhs in discovered_mvds:
            canonical_rhs = self.canonical_form(lhs, rhs)
            key = (lhs, canonical_rhs)
            if key in seen_canonical:
                continue
            if self.is_minimal(lhs, canonical_rhs, indexes, rejection_counters):
                final_mvds.add(key)
                seen_canonical.add(key)

        if self.stats:
            self.stats.record_stat("rejected_lhs_minimal", rejection_counters[0])
            self.stats.record_stat("rejected_rhs_minimal", rejection_counters[1])
            current = self.stats.valid_mvds_found.value
            self.stats.valid_mvds_found.add(len(final_mvds) - current)

        return self.format_results(final_mvds)

    def build_indexes(self, discovered_mvds):
        by_rhs = defaultdict(set)
        by_lhs = defaultdict(set)
        z_sides_per_lhs = defaultdict(set)
        for lhs, rhs in discovered_mvds:
            by_rhs[rhs].add(lhs)
            by_lhs[lhs].add(rhs)
            z_sides_per_lhs[lhs].add(self.all_attributes - lhs - rhs)
        return {
            "by_rhs": by_rhs,
            "by_lhs": by_lhs,
            "z_sides_per_lhs": z_sides_per_lhs,
        }

    def canonical_form(self, lhs, rhs):
        z = self.all_attributes - lhs - rhs
        if len(rhs) < len(z) or (
                len(rhs) == len(z) and tuple(sorted(rhs)) <= tuple(sorted(z))):
            return rhs
        return z

    def is_minimal(self, lhs, rhs, indexes, counters):
        if rhs.issubset(lhs):
            return False
        z = self.all_attributes - lhs - rhs
        if not z:
            return False

        same_rhs_lhs = indexes["by_rhs"].get(rhs, ())
        for subset_size in range(1, len(lhs)):
            lhs_subsets = combinations(sorted(lhs), subset_size)
            for lhs_subset in lhs_subsets:
                lhs_subset = frozenset(lhs_subset)
                if lhs_subset in same_rhs_lhs:
                    counters[0] += 1
                    return False

        same_lhs_rhs = set(indexes["by_lhs"].get(lhs, ()))
        same_lhs_rhs.update(indexes["z_sides_per_lhs"].get(lhs, set()))
        for subset_size in range(1, len(rhs)):
            rhs_subsets = combinations(sorted(rhs), subset_size)
            for rhs_subset in rhs_subsets:
                rhs_subset = frozenset(rhs_subset)
                if rhs_subset in same_lhs_rhs:
                    counters[1] += 1
                    return False

        return True

    def format_results(self, pairs):
        return sorted([
            (tuple(self.attr_names[i] for i in sorted(lhs)),
             tuple(self.attr_names[i] for i in sorted(rhs)))
            for lhs, rhs in pairs
        ])