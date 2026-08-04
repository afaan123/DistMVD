import os
import sys
import gc
import math
import time
import yaml
from pyspark import StorageLevel
from helper import (append_experiment_result, append_error_result,
                    cached_rdd_size_mb, build_spark_session,
                    cluster_total_cores, resolve_partitions)
from stats import Stats, print_spark_metrics
from mvd import MVDDiscovery


def run_experiment(config, spark):
    input_cfg     = config["input"]
    spark_cfg     = config["spark"]
    sampling_cfg  = config["sampling"]
    fd_cfg        = config["fd"]
    pruning_cfg   = config["pruning"]
    output_cfg    = config["output"]
    logging_cfg   = config["logging"]
    representation_cfg = config.get("representation", {})
    partition_cfg = spark_cfg["partitions"]

    sc = spark.sparkContext
    executors = max(1, len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1)

    total_cores = cluster_total_cores(sc)
    cores_per_executor = max(1, total_cores // executors)
    print(f"Executors={executors} Cores={total_cores} Cores Per Executor={cores_per_executor}")

    mode, num_partitions, shuffle_partitions = resolve_partitions(partition_cfg, total_cores)

    size_bytes = os.path.getsize(input_cfg["path"])
    total_data_mb = size_bytes / (1024 * 1024)

    print(f"Data={total_data_mb:.2f}MB  Partitions mode={mode} "
          f"Partitions={num_partitions} ShufflePartitions={shuffle_partitions}")

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))

    spark.conf.set("spark.sql.files.maxPartitionBytes",
                   str(spark_cfg["max_partition_mb"] * 1024 * 1024))

    stats = Stats(sc)
    stats.start_total_timer()

    read_start = time.time()
    # lazy
    df = spark.read.csv(
        input_cfg["path"],
        header = input_cfg["header"],
        inferSchema = input_cfg["infer_schema"],
        sep = input_cfg["delimiter"],
    )
    # lazy
    storage_level = StorageLevel.MEMORY_AND_DISK

    """ WIDE transformation -> repartition(64) -> Full shuffle -> persist is lazy -> No job """
    # lazy 
    df = df.repartition(num_partitions).persist(storage_level)

    attr_names = df.columns

    """ ACTION -> count() -> triggers repartition(64) shuffle -> 1 Job -> 2 Stages -> Stage 1: read CSV + shuffle-write -> tasks = #file splits (1 small single file) -> Stage 2: shuffle-read + count -> 64 Tasks (= num_partitions) -> partial counts summed on driver """
    total_rows = df.count()
    stats.record_elapsed("read_time", read_start)

    print(f" Rows={total_rows:,}  Cols={len(attr_names)} Partitions={num_partitions}")

    """ NARROW transformation -> Convert DataFrame rows to tuple-based RDD records -> No shuffle -> No job"""
    #lazy
    raw_rdd = df.rdd.map(lambda row: tuple(row))


    discovery = MVDDiscovery(
        raw_rdd  = raw_rdd,
        attr_names = attr_names,
        stats = stats,
        sampling_config = sampling_cfg,
        fd_config  = fd_cfg,
        pruning_config = pruning_cfg,
        num_partitions = num_partitions,
        shuffle_partitions   = shuffle_partitions,
        total_rows = total_rows,
        representation_config = representation_cfg,
    )

    minimal_mvds = discovery.discovery()
    stats.end_total_timer()

    spark_metrics = {}
    if logging_cfg.get("print_spark_metrics"):
        spark_metrics = print_spark_metrics(sc) or {}

    if output_cfg.get("save_csv"):
        ablation_variant = discovery.representation

        working_rdd_size_mb = cached_rdd_size_mb(discovery.packed_rdd)

        if minimal_mvds:
            lhs_sizes = [len(lhs) for lhs, _ in minimal_mvds]
            rhs_sizes = [len(rhs) for _, rhs in minimal_mvds]
            avg_lhs_size = round(sum(lhs_sizes) / len(lhs_sizes), 3)
            avg_rhs_size = round(sum(rhs_sizes) / len(rhs_sizes), 3)
            max_lhs_size = max(lhs_sizes)
        else:
            avg_lhs_size = avg_rhs_size = max_lhs_size = 0

        cards = discovery.cardinalities or []
        max_cardinality = max(cards) if cards else 0
        avg_cardinality = round(sum(cards) / len(cards), 2) if cards else 0

        n = len(attr_names)
        lhs_search_space = sum(math.comb(n, k) for k in range(1, max(1, n - 1)))

        append_experiment_result(
            output_path = output_cfg["output_path"],
            config = config,
            stats = stats,
            spark_metrics = spark_metrics,
            dataset_rows = total_rows,
            dataset_cols = len(attr_names),
            partitions = num_partitions,
            total_mvds = len(minimal_mvds),
            executors = executors,
            cores_per_executor = cores_per_executor,
            ablation_variant = ablation_variant,
            representation = discovery.representation,
            encoding_enabled = discovery.encoding_enabled,
            packing_enabled = discovery.packing_enabled,
            working_rdd_size_mb = working_rdd_size_mb,
            max_cardinality = max_cardinality,
            avg_cardinality = avg_cardinality,
            packed_bits = discovery.packed_bits,
            bits_per_column = discovery.bits_per_column,
            avg_lhs_size = avg_lhs_size,
            avg_rhs_size = avg_rhs_size,
            max_lhs_size = max_lhs_size,
            lhs_search_space = lhs_search_space,
        )

    if logging_cfg.get("print_mvds"):
        print("\n========== MINIMAL NON-TRIVIAL MVDs ==========")
        for lhs, rhs in minimal_mvds:
            print(f"  {lhs} ->> {rhs}")
        print(f"\n[MASTER] Total MVDs: {len(minimal_mvds)}")

    if logging_cfg.get("print_stats"):
        stats.print_stats()

    discovery.packed_rdd.unpersist()
    df.unpersist()
    spark.catalog.clearCache()


def main():
    if len(sys.argv) < 3:
        print("Usage: main.py <config.yaml> <experiment_index>")
        print("  experiment_index is 1-based; loop over experiments in the launcher.")
        sys.exit(1)

    config_path = sys.argv[1]
    idx = int(sys.argv[2])

    with open(config_path, "r") as f:
        root_config = yaml.safe_load(f)
    experiments = root_config["experiments"]

    if idx < 1 or idx > len(experiments):
        print(f"ERROR: experiment_index {idx} out of range 1..{len(experiments)}")
        sys.exit(1)

    exp = experiments[idx - 1]
    print("\n" + "=" * 80)
    print(f"EXPERIMENT {idx}: {exp['name']}")
    print("=" * 80)

    spark = build_spark_session()
    try:
        run_experiment(exp, spark)
    except Exception as e:
        import traceback
        traceback.print_exc()

        out_path = exp.get("output", {}).get("output_path")
        if out_path:
            try:
                append_error_result(out_path, exp, error=e)
                print(f"[MASTER] recorded error row for '{exp.get('name', '')}' -> {out_path}")
            except Exception as werr:
                print(f"[MASTER] failed to record error row: {werr}")
        raise
    finally:
        print("\n Stopping Spark...")
        spark.stop()
        gc.collect()


if __name__ == "__main__":
    main()