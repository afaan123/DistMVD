# DistMVD — Distributed Discovery of Multivalued Dependencies for Fourth Normal Form Analysis

> **Use the `main` branch.** `main` holds the DistMVD implementation the thesis evaluates, together
> with the full documentation (setup, datasets, cluster run, configuration, algorithm).
>
> This branch, `metanome-mvd-equal`, is **not** an alternative implementation. It carries a few
> minimal changes whose only purpose is to make the reported dependencies identical to the
> centralised **Metanome** result.

## What differs from `main`

| File | Change |
| --- | --- |
| `minimality.py` | reports the **dependency basis** `DEP(X)` — the finest partition of `R \ X` whose blocks all satisfy `X ↠ W` — instead of canonicalising and filtering to minimal MVDs; a block is dropped only when a strictly smaller LHS already induces it |
| `mvd.py`, `main.py` | the lattice starts at `level = 0`, i.e. the empty LHS `X = {}` (its closure is itself, and it has no FDs to discover); `lhs_search_space` counts that level too |
| `candidates.py` | `SKIP_SUPERSET` candidates are counted as holding, since a validated smaller LHS implies them — needed for `DEP(X)` to be complete |


