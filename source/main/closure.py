import sys
class Closure:

    def __init__(self,all_attributes):
        self.all_attributes=all_attributes
        self.closures={}

    def compute_closure(self,lhs):
        if lhs in self.closures:
            return self.closures[lhs]
        return frozenset(lhs)

    def check_superkey(self,lhs):
        closure=self.compute_closure(lhs)
        return closure==self.all_attributes

    def initialize_closure(self,lhs):
        self.closures[lhs]=frozenset(lhs)

    def expand_closures(self,new_fds,all_lhs):
        if len(new_fds)==0:
            return
        for lhs in all_lhs:
            if lhs in self.closures:
                closure=set(self.closures[lhs])
            else:
                closure=set(lhs)
            updated=True
            while updated:
                updated=False
                for left,right in new_fds:
                    if right in closure:
                        continue
                    if left.issubset(closure):
                        closure.add(right)
                        updated=True
            self.closures[lhs]=frozenset(closure)

    def inherit_subset_closures(self,lhs_groups):
        for lhs in lhs_groups:
            merged=set(lhs)
            for attr in lhs:
                subset=set(lhs)
                subset.remove(attr)
                subset=frozenset(subset)
                if subset in self.closures:
                    merged = merged.union(self.closures[subset])
            current=self.closures.get(
                lhs,
                frozenset(lhs)
            )
            if merged!=current:
                self.closures[lhs]=frozenset(merged)

    def get_requested_closures(self, requested_lhs):
        requested_closures = {}
        for lhs in self.closures:
            lhs_tuple = tuple(sorted(lhs))
            if lhs_tuple not in requested_lhs:
                continue
            closure = self.closures[lhs]
            closure_tuple = tuple(sorted(closure))
            requested_closures[lhs_tuple] = closure_tuple
        return requested_closures
