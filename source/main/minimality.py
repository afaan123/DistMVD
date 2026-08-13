from collections import defaultdict
from itertools import combinations


class Minimality:
    def __init__(self, all_attributes, attr_names, stats=None):
        self.all_attributes = all_attributes
        self.attr_names = attr_names
        self.stats = stats

    def cleanup(self, discovered_mvds):
        rejection_counters = [0, 0]

        basis = self.build_dependency_basis(discovered_mvds, rejection_counters)

        final_mvds = set()
        for lhs, blocks in basis.items():
            for rhs in blocks:
                if self.derivable_from_smaller(lhs, rhs, basis):
                    rejection_counters[0] += 1
                    continue
                final_mvds.add((lhs, rhs))

        if self.stats:
            self.stats.record_stat("rejected_lhs_minimal", rejection_counters[0])
            self.stats.record_stat("rejected_rhs_minimal", rejection_counters[1])
            current = self.stats.valid_mvds_found.value
            self.stats.valid_mvds_found.add(len(final_mvds) - current)

        return self.format_results(final_mvds)

    def build_dependency_basis(self, discovered_mvds, counters):
        # DEP(X): the finest partition of R\X whose blocks W all satisfy X ->> W.
        by_lhs = defaultdict(set)
        for lhs, rhs in discovered_mvds:
            if not rhs or (rhs & lhs):
                continue
            if not (self.all_attributes - lhs - rhs):
                continue
            by_lhs[lhs].add(rhs)

        basis = {}
        for lhs, rhs_sets in by_lhs.items():
            # Every valid RHS is a union of blocks, so the subset-minimal ones
            # are exactly the blocks.
            blocks = []
            for rhs in rhs_sets:
                if any(other < rhs for other in rhs_sets):
                    counters[1] += 1
                    continue
                blocks.append(rhs)

            # Candidate generation caps |RHS| at |R\X|//2, so at most one block
            # can be missing: whatever the found blocks do not cover.
            covered = frozenset().union(*blocks) if blocks else frozenset()
            leftover = self.all_attributes - lhs - covered
            if leftover:
                blocks.append(leftover)

            if len(blocks) >= 2:
                basis[lhs] = blocks
        return basis

    def derivable_from_smaller(self, lhs, rhs, basis):
        # X' ->> W' with X' subset of X implies X ->> W'\X, so a block that a
        # strictly smaller LHS already induces is not minimal.
        for subset_size in range(0, len(lhs)):
            for lhs_subset in combinations(sorted(lhs), subset_size):
                blocks = basis.get(frozenset(lhs_subset))
                if not blocks:
                    continue
                induced = set()
                for block in blocks:
                    trimmed = block - lhs
                    if trimmed:
                        induced.add(trimmed)
                if len(induced) >= 2 and rhs in induced:
                    return True
        return False

    def format_results(self, pairs):
        return sorted([
            (tuple(self.attr_names[i] for i in sorted(lhs)),
             tuple(self.attr_names[i] for i in sorted(rhs)))
            for lhs, rhs in pairs
        ])