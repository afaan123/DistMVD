# DistMVD — Distributed Discovery of Multivalued Dependencies for Fourth Normal Form Analysis

DistMVD discovers all **minimal, non-trivial multivalued dependencies (MVDs)** `X ↠ Y` in a
relational table, using PySpark (RDD API) so that discovery scales across a cluster.

`X ↠ Y` holds on `R(X, Y, Z)`, where `Z = R \ (X ∪ Y)`, iff in every group of tuples sharing an `X`
value the `(Y, Z)` combinations form the full cross product:

```
for every X-group g:   |π_YZ(g)|  ==  |π_Y(g)| · |π_Z(g)|
```


---

## Repository layout

```
DistMVD/
├── cluster_script/
│   └── run_cluster.sh         # launcher: syncs the cluster, runs every experiment in a config
├── dataset_script/            # one notebook per dataset: trims the raw file to the shape used
└── source/
    ├── main/
    │   ├── main.py            # entry point: config, Spark session, one experiment per run
    │   ├── mvd.py             # MVDDiscovery: representation + level-wise loop
    │   ├── workers.py         # executor-side kernels (encoding, packing, FD/MVD emit)
    │   ├── packing.py         # bit-width / bit-shift arithmetic for the packed representation
    │   ├── meta.py            # per-candidate projection metadata broadcast to executors
    │   ├── fd_discovery.py    # distributed FD discovery per level
    │   ├── closure.py         # attribute closures and superkey tests (driver-side)
    │   ├── candidates.py      # candidate generation + pruning
    │   ├── sampling.py        # largest-k group sampling plan
    │   ├── validation.py      # two-pass MVD validation (sample pass, then full pass)
    │   ├── minimality.py      # canonicalisation and minimality filtering
    │   ├── helper.py          # Spark session, partitions, candidate kernel, result CSV
    │   ├── stats.py           # accumulators, timings, Spark REST API metrics
    │   └── config/            # one YAML experiment file per dataset
    │       └── data/          # put the dataset CSVs here (not in the repository)
    └── test/                  # pytest suite + conftest.py (local Spark fixture)
```

---

## Requirements

Python 3.8+, PySpark 3.x, Java 8/11 (or whatever the Spark build requires).

```bash
pip install pyspark pyyaml requests
```

`requests` is only used for Spark's REST API metrics; without it the run still completes.

---

## Datasets

Input CSVs are **not** in the repository. Put every dataset file in

```
source/main/config/data/
```

creating that folder if it does not exist, and name each file exactly as its config's `input.path`
expects, e.g. `source/main/config/data/abalone_9c_4177r.csv`. Nine configs exist, named
`<dataset>_<cols>c_<rows>r`: `abalone_9c_4177r`, `ncvoter_11c_1400r`, `uniprot_13c_2000r`,
`tax_10c_4500r`, `cind_8c_45000r`, `lineitem_10c_80000r`, `partsupp_5c_800000r`,
`orders_7c_1500000r` (delimiter `;`) and `PDBX_6c_1500000r`.

On the cluster the files only have to be placed on the master — `run_cluster.sh` rsyncs
`source/main/config/data/` out to every worker before a run.

---

## Running

Experiments are launched from the Spark master with `cluster_script/run_cluster.sh`:

```bash
./cluster_script/run_cluster.sh <num_workers> <config_path>

# six remote workers + the master, over the abalone config
./cluster_script/run_cluster.sh 6 source/main/config/abalone_9c_4177r.yaml
```

`num_workers` counts the *remote* workers; the master acts as one more, and each worker contributes
4 cores — so `--total-executor-cores` is `(num_workers + 1) × 4`, with 8G per executor, 3G driver and
1G off-heap. The script performs the whole run:

1. ensures passwordless SSH to the master and the selected workers;
2. `git reset --hard` + `git pull origin server` on master and workers, and rsyncs
   `source/main/config/data/` out to them;
3. rewrites `$SPARK_HOME/conf/workers` and restarts the workers, so exactly the selected nodes are up;
4. zips `source/main/*.py` into `dependencies.zip` for `--py-files`;
5. runs **one `spark-submit` per experiment** in the config, deriving `spark.default.parallelism`,
   the shuffle partitions and `spark.sql.files.maxPartitionBytes` from that experiment's
   `spark.partitions` block and the actual core count;
6. drops the OS page cache on every active node before each experiment, so a warm cache cannot make
   later runs look artificially fast (needs passwordless sudo; warns and continues without it).

A failing experiment does not stop the sweep — it is listed at the end, and `main.py` appends a
`status=error` row with the same CSV schema, so no data point is lost. Results are written by the
**driver** with plain Python I/O; the script exports `MVD_RESULTS_DIR=$PROJECT_DIR/source/main`.

`SPARK_HOME`, `MASTER_IP`, `PROJECT_DIR` and the `ALL_WORKERS` list at the top of the script are
hard-coded for the thesis cluster and must be edited for another one.

---

## Tests

The suite runs on a local Spark session (`local[1]`) created once per pytest session by
`source/test/conftest.py`; no datasets and no cluster are needed.

```bash
python -m pytest source/test -q
```

113 tests: `unit_test/` (encoding/packing, closures, candidate generation, sampling, minimality),
`integration_test/` (11 hand-built relations through the full pipeline, compared against expected FD
and MVD sets), `edge_test/` (single row, single column, all-duplicate, all-unique, constant column,
NULLs) and `negative_test/` (near-miss relations, plus invariants that must hold for every result).
`conftest.py` pins `JAVA_HOME` via `/usr/libexec/java_home -v 17` (macOS); elsewhere set `JAVA_HOME`
yourself and drop that lookup.

---

## Configuration reference

Every config is a list under `experiments:`, one entry per experiment. The fields, all read at
runtime:

| Field | Meaning |
| --- | --- |
| `input` | `path` (relative to `source/main`), `delimiter`, `header`, `infer_schema` (`false` → all columns as strings) |
| `spark.max_partition_mb` | `spark.sql.files.maxPartitionBytes` |
| `spark.partitions` | `mode: auto \| manual`; manual uses `num_partitions` / `shuffle_partitions`, auto uses `auto.num_multiplier` / `auto.shuffle_multiplier` × total cores |
| `representation` | `encoding` (dictionary-encode columns to ids) and `packing` (bit-pack a row into one integer; requires encoding) |
| `fd` | `enabled`, and `superkey_threshold` — skip FD discovery at a level once that fraction of the previous level's LHS sets are superkeys |
| `sampling` | `enabled`, `mode: auto \| manual`, `largest_k` for manual, and `auto.coverage` / `auto.min_k` / `auto.max_k` |
| `pruning` | `fd_pruning`, `superset_pruning`, `discovered_mvd_pruning`, `complement_pruning` — each `{ enabled: … }`, independently toggleable for the ablation |
| `output` | `save_csv` and `output_path` (appended to; header written once) |
| `logging` | `print_mvds`, `print_stats`, `print_spark_metrics` (needs the Spark REST API) |

See `source/main/config/` for complete examples.

---

## How the algorithm works

**1. Load.** `main.py` reads the CSV, repartitions, persists (`MEMORY_AND_DISK`), materialises with
`count()`, and converts to a tuple RDD — the only form the rest of the pipeline uses.

**2. Representation** (`workers.py`, `packing.py`) — three interchangeable variants, the main
ablation axis: `packed` (encoding + packing, one `int` per row), `encoded` (tuple of ids), `raw`
(original strings). Encoders are built with `mapPartitions` → `treeReduce(depth=3)`, so only
per-partition summaries are shuffled, never the tuples; id `0` is reserved for unknown values. In the
packed layout every column has the same width `bits = bit_length(max cardinality)` and column `c` of
`n` sits at shift `(n - 1 - c) · bits`, making a projection a fixed shift-and-mask sequence with no
tuple allocation.

**3. Level-wise search** (`mvd.py`) — left-hand sides are enumerated by size, `level = 1 … n-2`
(beyond that no room is left for a non-trivial `Y` and `Z`). Each level runs four phases:

* **FD discovery** (`fd_discovery.py`) — one job with two shuffles decides all candidate FDs of the
  level at once: `mapPartitions` emits `((lhs, rhs, x_hash) → y)`, `aggregateByKey` collapses each
  group to one `y` or a violation sentinel, `reduceByKey(and)` folds group verdicts into FD verdicts.
* **Closure maintenance** (`closure.py`) — driver-side fixpoint over all LHS sets; each level first
  inherits the union of its subsets' closures. Closures give the superkey test and FD pruning.
* **Candidate generation** (`candidates.py`) — LHS sets are `parallelize`d and expanded on executors.
  For each LHS all `Y ⊂ R \ X` with `|Y| ≤ |R \ X| / 2` are classified:

  | Outcome | Rule |
  | --- | --- |
  | `SKIP_COMPLEMENT` | `X ↠ Y` ≡ `X ↠ Z`; keep the lexicographically smaller half |
  | `AUTO_SK` | `X` is a superkey ⇒ every `X ↠ Y` holds trivially |
  | `AUTO_FD` | `Y ⊆ X⁺` or `Z ⊆ X⁺` ⇒ the FD `X → Y` implies the MVD |
  | `SKIP_SUPERSET` | a smaller `X' ⊂ X` with the same `Y` (or `Z`) already yielded an MVD |
  | `VALIDATE` | must be checked against the data |

  Only `VALIDATE` reaches Spark. The four rules map one-to-one onto the `pruning` flags.
* **Validation** (`validation.py`, `sampling.py`) — a *sample pass* over the largest groups (a single
  violating group disproves an MVD, so a sampled failure is a sound rejection; passing is not
  acceptance), then a *full pass*: `validate_emit` produces the distinct projection hashes per
  `(lhs, subset, x_hash)`, `aggregateByKey` + `mapValues(len)` give `|π_Y|`, `|π_Z|`, `|π_YZ|`, and a
  custom partitioner on `(lhs_idx, x_hash)` keeps a group's three counts together so
  `n_YZ == n_Y · n_Z` is decided locally before a final `reduceByKey(and)`.

**4. Minimality** (`minimality.py`) — MVDs are canonicalised (of `Y` and `Z` the smaller is kept,
ties broken lexicographically) and dropped if a proper subset of the LHS carries the same RHS, or a
proper subset of the RHS already appears under the same LHS.
