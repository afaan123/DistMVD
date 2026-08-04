# DistMVD — Distributed Discovery of Multivalued Dependencies for Fourth Normal Form Analysis

DistMVD discovers all **minimal, non-trivial multivalued dependencies (MVDs)** `X ↠ Y` in a
relational table, using PySpark (RDD API) so that discovery scales across a cluster.

A multivalued dependency `X ↠ Y` holds on relation `R(X, Y, Z)`, where `Z = R \ (X ∪ Y)`, iff for
every group of tuples sharing the same `X` value, the set of `(Y, Z)` combinations in that group is
the full cross product of the distinct `Y` values and the distinct `Z` values:

```
for every X-group g:   |π_YZ(g)|  ==  |π_Y(g)| · |π_Z(g)|
```

This counting characterisation is exactly what the distributed validation stage checks, which is
what makes the check expressible as two Spark shuffles rather than a per-group join.

The implementation is a **level-wise (Apriori-style) lattice search over left-hand sides**, combined
with four pruning rules, functional-dependency-driven inference, and a sampling pre-filter that
rejects candidates cheaply before the full validation pass. Every run emits a wide CSV row of
runtime, pruning and Spark-level metrics, so the code doubles as the experiment harness for the
thesis evaluation (scalability and ablation studies).

---

## Repository layout

```
DistMVD/
├── README.md
├── .gitignore
└── source/
    ├── main/
    │   ├── main.py            # entry point: config loading, Spark session, one experiment per run
    │   ├── mvd.py             # MVDDiscovery: builds the representation, drives the level-wise loop
    │   ├── workers.py         # executor-side kernels (encoding, packing, FD/MVD emit functions)
    │   ├── packing.py         # bit-width and bit-shift arithmetic for the packed representation
    │   ├── meta.py            # per-candidate projection metadata broadcast to executors
    │   ├── fd_discovery.py    # distributed functional-dependency discovery per lattice level
    │   ├── closure.py         # attribute-closure maintenance and superkey tests (driver-side)
    │   ├── candidates.py      # candidate generation + pruning, parallelised over LHS sets
    │   ├── sampling.py        # largest-k group sampling plan (cheap rejection pre-pass)
    │   ├── validation.py      # two-pass MVD validation (sample pass, then full pass)
    │   ├── minimality.py      # canonicalisation and minimality filtering of the result set
    │   ├── helper.py          # Spark session, partition resolution, candidate kernel, result CSV
    │   ├── stats.py           # accumulators, timing breakdown, Spark REST API metrics
    │   └── config/            # one YAML experiment file per dataset
    └── test/                  # (empty — no automated tests in this submission)
```

Input CSVs are **not** included in the repository. Each config expects its dataset at
`source/main/config/data/<name>.csv` (see [Datasets](#datasets)).

---

## Requirements

| Component | Version used |
| --- | --- |
| Python | 3.8+ |
| Apache Spark / PySpark | 3.x |
| Java | 8 or 11 (whatever the Spark build requires) |

Python packages:

```bash
pip install pyspark pyyaml requests
```

`requests` is only used to read Spark's REST API (`/api/v1/applications/<id>/stages`) for the
per-stage metrics written into the results CSV. If it is missing, or the Spark UI is disabled,
metric collection is skipped and the run still completes.

---

## Datasets

Nine datasets are configured, named `<dataset>_<cols>c_<rows>r`:

| Config | Columns | Rows | Delimiter |
| --- | --- | --- | --- |
| `abalone_9c_4177r.yaml` | 9 | 4,177 | `,` |
| `ncvoter_11c_1400r.yaml` | 11 | 1,400 | `,` |
| `uniprot_13c_2000r.yaml` | 13 | 2,000 | `,` |
| `tax_10c_4500r.yaml` | 10 | 4,500 | `,` |
| `cind_8c_45000r.yaml` | 8 | 45,000 | `,` |
| `lineitem_10c_80000r.yaml` | 10 | 80,000 | `,` |
| `partsupp_5c_800000r.yaml` | 5 | 800,000 | `,` |
| `orders_7c_1500000r.yaml` | 7 | 1,500,000 | `;` |
| `PDBX_6c_1500000r.yaml` | 6 | 1,500,000 | `,` |

Place each CSV at the `input.path` given in its config, i.e. relative to `source/main`:

```
source/main/config/data/abalone_9c_4177r.csv
```

Because `input.path` is resolved against the **current working directory**, always launch from
`source/main` (or edit the path to an absolute one / an `hdfs://` URI).

---

## Running

Each invocation runs exactly **one** experiment from one config file. The experiment index is
1-based; loop over indices in a launcher script if a config ever holds several experiments.

```bash
cd source/main

spark-submit \
  --master local[*] \
  --py-files mvd.py,workers.py,packing.py,meta.py,fd_discovery.py,closure.py,\
candidates.py,sampling.py,validation.py,minimality.py,helper.py,stats.py \
  main.py config/abalone_9c_4177r.yaml 1
```

On a standalone / YARN cluster, replace `--master` and add the usual resource flags:

```bash
spark-submit \
  --master spark://<host>:7077 \
  --deploy-mode client \
  --executor-cores 4 --executor-memory 8G --total-executor-cores 16 \
  --py-files <as above> \
  main.py config/orders_7c_1500000r.yaml 1
```

Notes:

* `spark.cores.max` is read to size partitions in `auto` mode; if it is unset, Spark's
  `defaultParallelism` is used instead.
* The result CSV is written by the **driver** with plain Python file I/O, not by Spark — so it lands
  on the driver's local filesystem. Set `MVD_RESULTS_DIR` to control the base directory; otherwise
  relative `output_path` values resolve against `source/main`.
* On failure, the exception is logged **and** an `status=error` row with the same CSV schema is
  appended, so a sweep over many experiments never loses a data point.

---

## Configuration reference

Every config is a list under `experiments:`; the fields below are all read at runtime.

```yaml
experiments:
  - name: "abalone_9c_4177r"

    input:
      path: "config/data/abalone_9c_4177r.csv"   # relative to CWD (source/main)
      delimiter: ","
      header: true
      infer_schema: false                         # false → all columns read as strings

    spark:
      max_partition_mb: 128                       # spark.sql.files.maxPartitionBytes
      partitions:
        mode: auto                                # auto | manual
        num_partitions: 1                         # used when mode = manual
        shuffle_partitions: 1                     # used when mode = manual
        auto:
          num_multiplier: 2                       # num_partitions     = round(m × total_cores)
          shuffle_multiplier: 2                   # shuffle_partitions = round(m × total_cores)

    representation:
      encoding: true                              # dictionary-encode each column to integer ids
      packing: true                               # bit-pack the whole row into one integer
                                                  # packing requires encoding (else auto-disabled)

    fd:
      enabled: true                               # run FD discovery to drive closures + FD pruning
      superkey_threshold: 0.80                    # skip FD discovery at a level once ≥80% of the
                                                  # previous level's LHS sets are superkeys

    sampling:
      enabled: true
      mode: auto                                  # manual → always take largest_k groups
      largest_k: 10                               # k for manual mode
      auto:
        coverage: 0.3                             # take largest groups until 30% of tuples covered
        min_k: 3
        max_k: 20

    pruning:                                      # all four are independently toggleable (ablation)
      fd_pruning:             { enabled: true }
      superset_pruning:       { enabled: true }
      discovered_mvd_pruning: { enabled: true }
      complement_pruning:     { enabled: true }

    output:
      save_csv: true
      output_path: "results/abalone/abalone.csv"  # appended to; header written once

    logging:
      print_mvds: true
      print_stats: true
      print_spark_metrics: true                   # requires the Spark REST API to be reachable
```

---

## How the algorithm works

### 1. Load and repartition

`main.py` reads the CSV into a DataFrame, repartitions it to `num_partitions`, persists it
(`MEMORY_AND_DISK`) and forces materialisation with `count()`. The DataFrame is then converted to a
tuple RDD, which is the only form the rest of the pipeline uses.

### 2. Representation (`workers.py`, `packing.py`)

Three interchangeable representations exist, selected by `representation` — this is the main
ablation axis for the memory/runtime study:

| Setting | Row form | Built by |
| --- | --- | --- |
| `packed` (encoding + packing) | one Python `int` holding all columns | `create_packed_rdd` |
| `encoded` (encoding only) | tuple of integer ids | `create_encoded_rdd` |
| `raw` (neither) | the original tuple of strings | `create_raw_rdd` |

Dictionary encoders are built with `mapPartitions` → `treeReduce(depth=3)`: each partition emits one
set of distinct values per column, and only those per-partition summaries are merged — the tuples
themselves are never shuffled. Value ids start at 1, so `0` is reserved for values missing from an
encoder.

In the packed layout every column gets the *same* width,
`bits = bit_length(max column cardinality)`, and column `c` of `n` lives at shift
`(n - 1 - c) · bits`. A projection onto a set of columns is then a fixed sequence of shift-and-mask
operations followed by concatenation into a single integer key — no tuple allocation per projection,
which is what makes the emit kernels cheap.

### 3. Level-wise search (`mvd.py`)

The lattice of left-hand sides is enumerated by size, `level = 1 … n-2` (a level of `n-1` or more
leaves no room for a non-trivial `Y` and `Z`). Each level runs four phases:

**(a) FD discovery** (`fd_discovery.py`) — for every non-superkey LHS, all attributes outside its
current closure become FD candidates. One Spark job with two shuffles decides all of them at once:
`mapPartitions` emits `((lhs, rhs, x_hash) → y)` observations, `aggregateByKey` collapses each group
to a single `y` or a violation sentinel, and `reduceByKey(and)` folds per-group verdicts into a
per-FD verdict. The whole level is skipped once ≥ `superkey_threshold` of the previous level's LHS
sets are superkeys, since no new FDs can be found there.

**(b) Closure maintenance** (`closure.py`) — driver-side. New FDs are applied to a fixpoint over all
LHS sets seen so far, and each level first inherits the union of its subsets' closures. Closures
give the superkey test and drive FD-based pruning.

**(c) Candidate generation** (`candidates.py`, `helper.generate_candidates_partition`) — the LHS sets
of a level are `parallelize`d and expanded on executors. For each LHS, all `Y ⊂ R \ X` with
`|Y| ≤ |R \ X| / 2` are enumerated and classified into one of five outcomes:

| Outcome | Rule |
| --- | --- |
| `SKIP_COMPLEMENT` | `X ↠ Y` and `X ↠ Z` are equivalent; keep only the lexicographically smaller half |
| `AUTO_SK` | `X` is a superkey ⇒ every `X ↠ Y` holds trivially |
| `AUTO_FD` | `Y ⊆ X⁺` or `Z ⊆ X⁺` ⇒ the FD `X → Y` implies the MVD `X ↠ Y` |
| `SKIP_SUPERSET` | a strictly smaller `X' ⊂ X` with the same `Y` (or `Z`) already yielded an MVD |
| `VALIDATE` | none of the above — must be checked against the data |

Only the `VALIDATE` set reaches Spark. The four rules map one-to-one onto the four `pruning` flags,
so each can be switched off to measure its individual contribution.

**(d) Validation** (`validation.py`, `sampling.py`) — two passes over the persisted RDD:

* *Sample pass.* `sampling.py` counts group sizes per LHS in one shuffle and selects the largest
  groups (fixed `largest_k`, or as many as needed to cover `coverage` of the tuples, clamped to
  `[min_k, max_k]`). Validation then runs restricted to those groups. Because a single violating
  group disproves an MVD, a failure on sampled data is a **sound rejection**; passing is not
  acceptance, so survivors still go through the full pass.
* *Full pass.* Same kernel with no sample filter. `validate_emit` emits, per `(lhs, subset, x_hash)`,
  the distinct projection hashes; `aggregateByKey` into sets then `mapValues(len)` yields
  `|π_Y|`, `|π_Z|` and `|π_YZ|` per group. A custom partitioner keys on `(lhs_idx, x_hash)` so that
  the three counts belonging to one group land in the same partition and the
  `n_YZ == n_Y · n_Z` test is decided locally, with a final `reduceByKey(and)` across groups.

### 4. Minimality (`minimality.py`)

Discovered MVDs are canonicalised (of `Y` and `Z`, the smaller — tie broken lexicographically — is
kept, since the two describe the same dependency) and then filtered: an MVD is dropped if any proper
subset of its LHS carries the same RHS, or if any proper subset of its RHS already appears under the
same LHS. Results are returned as attribute-name pairs, sorted.

---

## Output

`print_mvds` lists the final dependencies:

```
========== MINIMAL NON-TRIVIAL MVDs ==========
  ('Sex',) ->> ('Length', 'Diameter')
  ...
[MASTER] Total MVDs: 12
```

`print_stats` prints the timing breakdown (read / encoding / packing / FD / closure / candidate /
sampling / validation / cleanup / other, each with a share of wall clock) plus skew indicators
(max and average group size).

With `save_csv: true` one row per run is appended to `output_path`. The schema is fixed in
`helper._RESULT_TEMPLATE` — successful and failed runs share it exactly, and if the header on disk
does not match, the old file is archived to `*.legacy-<timestamp>.csv` rather than being corrupted.
Columns cover:

* **identity** — `run_id`, `status`, `error`, `experiment_name`, `timestamp`, `dataset`, `rows`, `cols`
* **configuration** — `representation`, `encoding_enabled`, `packing_enabled`, `fd_enabled`,
  `sampling_enabled`, `largest_k`, the four `prune_*` flags, `executors`, `cores_per_executor`,
  `partitions`, `max_partition_mb`
* **representation cost** — `max_cardinality`, `avg_cardinality`, `bits_per_column`, `packed_bits`,
  `working_rdd_size_mb` (actual cached size of the working RDD, memory + disk)
* **search effort** — `levels_processed`, `lhs_sets_processed`, `lhs_search_space`,
  `candidates_generated`, `candidates_sent_to_validation`, and the per-rule counters
  (`skipped_complement`, `pruned_superset`, `auto_validated_fd`, `auto_validated_superkey`,
  `rejected_by_sampling`, `rejected_lhs_minimal`, `rejected_rhs_minimal`) with their percentages
* **results** — `total_mvds`, `fds_discovered`, `avg_lhs_size`, `avg_rhs_size`, `max_lhs_size`
* **timings** — `*_time` seconds and `*_pct` shares, `total_time`
* **Spark metrics** (REST API) — `stages_completed`, `shuffle_read_mb`, `shuffle_write_mb`,
  `input_read_mb`, `executor_run_time_sec`, `executor_cpu_time_sec`, `jvm_gc_time_sec`,
  `memory_spill_mb`, `disk_spill_mb`, `peak_execution_memory_mb`, task counts, and the
  `*_shuffle_jobs` counters

Since rows are appended, a whole sweep (all datasets × all ablation variants) accumulates into one
CSV per dataset, ready for analysis.

---

## Reproducing the experiments

**Scalability.** Keep the config fixed and vary `spark-submit` resources (`--total-executor-cores`,
number of executors) and/or the dataset size. `partitions.mode: auto` re-derives partition counts
from the cluster width, so the same YAML is valid at every scale; `executors`,
`cores_per_executor` and `partitions` are recorded in every row.

**Representation ablation.** Vary `representation` over the three valid combinations and compare
`working_rdd_size_mb`, `encoding_time` / `packing_time`, `validation_time` and the shuffle volumes:

| Variant | `encoding` | `packing` |
| --- | --- | --- |
| packed | `true` | `true` |
| encoded | `true` | `false` |
| raw | `false` | `false` |

(`encoding: false, packing: true` is not a fourth variant — packing needs encoded ids, so the code
warns and falls back to `raw`.)

**Pruning ablation.** Disable one `pruning.*` flag at a time and read off
`candidates_sent_to_validation`, `validation_time` and `total_time`. `total_mvds` must stay constant:
every rule is designed to be result-preserving, so a change in the count signals a bug rather than a
speed/accuracy trade-off.

**Sampling ablation.** Toggle `sampling.enabled`, or switch `mode` between `manual` (fixed
`largest_k`) and `auto` (coverage-driven), and compare `rejected_by_sampling` and `sampling_time`
against the reduction in `validation_time`.

**FD ablation.** Toggle `fd.enabled`, and vary `fd.superkey_threshold`, to separate the cost of FD
discovery from the candidates it removes via `auto_validated_fd` and `auto_validated_superkey`.

---

## Implementation notes and limitations

* All columns are read as strings by default (`infer_schema: false`); values are compared for
  equality only, so this is sufficient and avoids type-inference passes over the data.
* Encoding assumes each column's distinct values fit in driver memory — they are collected there to
  build the dictionaries. This holds for the evaluated datasets but bounds the applicable column
  cardinality.
* The packed representation uses a single, uniform bit width driven by the largest column
  cardinality, which trades a little space for branch-free shift arithmetic on every projection.
* Closure computation and minimality filtering are driver-side: they operate on the dependency sets,
  which are small compared to the data.
* NULL/empty handling follows CSV parsing — empty fields are ordinary values with their own id.
* `source/test/` is present but empty; correctness was checked against reference results rather than
  by an automated suite.
