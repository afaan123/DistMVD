from itertools import combinations
from datetime import datetime
from urllib.parse import urlparse
from pyspark.sql import SparkSession
import csv
import os
import uuid
import sys


def cached_rdd_size_mb(rdd):
    try:
        rdd_id = rdd.id()
        for info in rdd.context._jsc.sc().getRDDStorageInfo():
            if info.id() == rdd_id:
                return round((info.memSize() + info.diskSize()) / (1024 * 1024), 4)
    except Exception:
        pass
    return 0.0


def build_spark_session():
    spark = SparkSession.builder.appName("MVD Discovery").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def cluster_total_cores(sc):
    cores_max = sc.getConf().get("spark.cores.max", None)
    if cores_max:
        return int(cores_max)
    return sc.defaultParallelism


def resolve_partitions(partition_cfg, total_cores):
    mode = partition_cfg["mode"] if "mode" in partition_cfg else "manual"
    if mode == "auto":
        auto_cfg = partition_cfg["auto"]
        num_partitions = max(1, round(auto_cfg["num_multiplier"] * total_cores))
        shuffle_partitions = max(1, round(auto_cfg["shuffle_multiplier"] * total_cores))
    else:
        num_partitions = partition_cfg["num_partitions"]
        shuffle_partitions = partition_cfg["shuffle_partitions"]
    return mode, num_partitions, shuffle_partitions

def generate_candidates_partition(lhs_items, gen_state_bc):
    state = gen_state_bc.value
    all_attrs = set(state["all_attributes"])
    closure_map = state["closure_map"]
    validated_mvd_lhs = state.get("validated_mvd_lhs", {})

    pruning = state["pruning"]
    prune_complement  = pruning["complement"]
    prune_fd          = pruning["fd"]
    prune_superset    = pruning["superset"]

    for lhs_t in lhs_items:
        lhs_set = set(lhs_t)
        closure_t = closure_map.get(lhs_t, lhs_t)
        closure_set = set(closure_t)
        is_superkey = prune_fd and (closure_set == all_attrs)
        remaining = sorted(all_attrs - lhs_set)
        remaining_size = len(remaining)
        max_rhs_size = remaining_size // 2

        for size in range(1, max_rhs_size + 1):
            for rhs_t in combinations(remaining, size):
                rhs_set = set(rhs_t)
                z_set = all_attrs - lhs_set - rhs_set
                z_t = tuple(sorted(z_set))

                # A ↠ BC and A ↠ DE are complements; keep only A ↠ BC.
                if prune_complement and size == len(z_t) and rhs_t > z_t:
                    yield ("SKIP_COMPLEMENT", lhs_t, rhs_t, z_t)
                    continue

                # Rule Already validated: A ↠ C
                # Current candidate: AB ↠ C
                # Since A ⊂ AB, prune AB ↠ C.
                pruned_superset = prune_superset and should_prune_by_smaller_lhs(
                    rhs_t, z_t, lhs_set, lhs_t, validated_mvd_lhs)

                if is_superkey:
                    if pruned_superset:
                        yield ("SKIP_SUPERSET", lhs_t, rhs_t, z_t)
                    else:
                        yield ("AUTO_SK", lhs_t, rhs_t, z_t)
                    continue
                
                # A -> BC implies A ↠ BC. 
                if prune_fd and (rhs_set.issubset(closure_set) or z_set.issubset(closure_set)):
                    if pruned_superset:
                        yield ("SKIP_SUPERSET", lhs_t, rhs_t, z_t)
                    else:
                        yield ("AUTO_FD", lhs_t, rhs_t, z_t)
                    continue

                if pruned_superset:
                    yield ("SKIP_SUPERSET", lhs_t, rhs_t, z_t)
                    continue

                yield ("VALIDATE", lhs_t, rhs_t, z_t)



def should_prune_by_smaller_lhs(rhs_t, z_t, lhs_set, lhs_t, validated_mvd_lhs):
    smaller_lhs_set = validated_mvd_lhs.get(rhs_t)
    if smaller_lhs_set is None:
        return False
    for prev_lhs in smaller_lhs_set:
        if (len(prev_lhs) < len(lhs_t) and set(prev_lhs).issubset(lhs_set)):
            return True
    return False



def _pruning_enabled(config, name):
    prune_cfg = config["pruning"] if "pruning" in config else None
    section = prune_cfg[name] if prune_cfg and name in prune_cfg else None
    return section["enabled"] if section else True


_RESULTS_BASE = os.environ.get("MVD_RESULTS_DIR") or os.path.dirname(os.path.abspath(__file__))


def _to_local_path(output_path):
    if output_path.startswith("file://"):
        path = urlparse(output_path).path
    else:
        path = output_path
    if not os.path.isabs(path):
        path = os.path.join(_RESULTS_BASE, path)
    return os.path.normpath(path)


def _open_csv_writer(output_path, fieldnames):
    local_path = _to_local_path(output_path)
    parent = os.path.dirname(local_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    write_header = True
    if os.path.exists(local_path):
        with open(local_path, "r", newline="") as existing:
            first_line = existing.readline().rstrip("\n\r")
        if first_line == ",".join(fieldnames):
            write_header = False
        else:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            root, ext = os.path.splitext(local_path)
            legacy = f"{root}.legacy-{stamp}{ext or '.csv'}"
            os.rename(local_path, legacy)
            print(f"[CSV] schema changed - archived old results to {legacy}")

    return open(local_path, "a", newline=""), write_header


# Single source of truth for the result-CSV schema. Every column lives here with
# a default, so success rows and error rows share the exact same header (no schema
# churn / archiving when the two are interleaved). Add new columns here AND in the
# success row below; error rows automatically pick up the default.
_RESULT_TEMPLATE = {
    "run_id": "",
    "status": "",
    "error": "",
    "experiment_name": "",
    "ablation_variant": "",
    "representation": "",
    "encoding_enabled": False,
    "packing_enabled": False,
    "dataset": "",
    "rows": 0,
    "cols": 0,
    "timestamp": "",
    "max_cardinality": 0,
    "avg_cardinality": 0,
    "packed_bits": 0,
    "bits_per_column": 0,
    "working_rdd_size_mb": 0.0,
    "executors": 0,
    "cores_per_executor": 0,
    "max_partition_mb": 0,
    "partitions": 0,
    "fd_enabled": False,
    "sampling_enabled": False,
    "largest_k": 0,
    "levels_processed": 0,
    "lhs_sets_processed": 0,
    "lhs_search_space": 0,
    "total_mvds": 0,
    "fds_discovered": 0,
    "valid_mvds_found": 0,
    "avg_lhs_size": 0,
    "avg_rhs_size": 0,
    "max_lhs_size": 0,
    "candidates_generated": 0,
    "candidates_sent_to_validation": 0,
    "auto_validated_fd": 0,
    "auto_validated_superkey": 0,
    "skipped_complement": 0,
    "pruned_superset": 0,
    "rejected_by_sampling": 0,
    "rejected_lhs_minimal": 0,
    "rejected_rhs_minimal": 0,
    "sampling_lhs_sampled": 0,
    "sample_groups_scanned": 0,
    "skip_complement_total_pct": 0,
    "superset_total_pct": 0,
    "auto_fd_total_pct": 0,
    "auto_sk_total_pct": 0,
    "sampling_total_pct": 0,
    "read_time": 0,
    "encoding_time": 0,
    "packing_time": 0,
    "raw_time": 0,
    "fd_time": 0,
    "closure_time": 0,
    "candidate_time": 0,
    "sampling_time": 0,
    "validation_time": 0,
    "cleanup_time": 0,
    "other_time": 0,
    "total_time": 0,
    "read_pct": 0,
    "encoding_pct": 0,
    "packing_pct": 0,
    "raw_pct": 0,
    "fd_pct": 0,
    "closure_pct": 0,
    "candidate_pct": 0,
    "sampling_pct": 0,
    "validation_pct": 0,
    "cleanup_pct": 0,
    "other_pct": 0,
    "prune_complement": False,
    "prune_superset": False,
    "prune_fd": False,
    "prune_discovered_mvd": False,
    "fd_shuffle_jobs": 0,
    "sampling_shuffle_jobs": 0,
    "mvd_shuffle_jobs": 0,
    "total_shuffle_jobs": 0,
    "max_group_size": 0,
    "total_groups": 0,
    "avg_group_size": 0,
    "stages_completed": 0,
    "shuffle_read_mb": 0,
    "shuffle_write_mb": 0,
    "input_read_mb": 0,
    "output_write_mb": 0,
    "executor_run_time_sec": 0,
    "executor_cpu_time_sec": 0,
    "executor_deserialize_sec": 0,
    "result_serialization_sec": 0,
    "jvm_gc_time_sec": 0,
    "memory_spill_mb": 0,
    "disk_spill_mb": 0,
    "peak_execution_memory_mb": 0,
    "num_tasks": 0,
    "num_failed_tasks": 0,
    "num_completed_tasks": 0,
}


def _write_result_row(output_path, row):
    # Force every row onto the canonical schema: fill missing columns with their
    # template default and drop anything unexpected, so the header never drifts.
    full = {**_RESULT_TEMPLATE, **row}
    fieldnames = list(_RESULT_TEMPLATE.keys())
    f, write_header = _open_csv_writer(output_path, fieldnames)
    try:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(full)
    finally:
        f.close()


def append_error_result(output_path, config, error,
                        executors=0, cores_per_executor=0, partitions=0):
    """Record a failed experiment as a row with status='error' and the message,
    keeping the same columns as a successful run (everything else defaulted)."""
    representation_cfg = config.get("representation", {})
    row = {
        "run_id": uuid.uuid4().hex[:12],
        "status": "error",
        "error": str(error).replace("\n", " ").replace("\r", " ")[:500],
        "experiment_name": config.get("name", ""),
        "dataset": config.get("input", {}).get("path", ""),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "encoding_enabled": representation_cfg.get("encoding", True),
        "packing_enabled": representation_cfg.get("packing", True),
        "executors": executors,
        "cores_per_executor": cores_per_executor,
        "partitions": partitions,
        "max_partition_mb": config.get("spark", {}).get("max_partition_mb", 0),
        "fd_enabled": config.get("fd", {}).get("enabled", False),
        "sampling_enabled": config.get("sampling", {}).get("enabled", False),
        "largest_k": config.get("sampling", {}).get("largest_k", 0),
        "prune_complement":     _pruning_enabled(config, "complement_pruning"),
        "prune_superset":       _pruning_enabled(config, "superset_pruning"),
        "prune_fd":             _pruning_enabled(config, "fd_pruning"),
        "prune_discovered_mvd": _pruning_enabled(config, "discovered_mvd_pruning"),
    }
    _write_result_row(output_path, row)


def append_experiment_result(output_path, config, stats, spark_metrics,
                              dataset_rows, dataset_cols, partitions,
                              total_mvds, executors, cores_per_executor,
                              ablation_variant="packed", representation="packed",
                              encoding_enabled=True, packing_enabled=True,
                              working_rdd_size_mb=0.0, max_cardinality=0,
                              avg_cardinality=0, packed_bits=0, bits_per_column=0,
                              avg_lhs_size=0, avg_rhs_size=0, max_lhs_size=0,
                              lhs_search_space=0):
    total_candidates = stats.total_candidates_generated.value
    skipped_complement = stats.skipped_complement.value
    auto_fd = stats.auto_validated_fd.value
    auto_sk = stats.auto_validated_superkey.value
    pruned_superset = stats.pruned_superset.value
    rejected_sampling = stats.rejected_by_sampling.value
    rejected_lhs_min = stats.rejected_lhs_minimal.value
    rejected_rhs_min = stats.rejected_rhs_minimal.value

    run_id = uuid.uuid4().hex[:12]
    experiment_name = config.get("name", "")
    levels_processed = stats.levels_processed.value
    lhs_sets_processed = stats.lhs_sets_processed.value
    sampling_lhs_sampled = stats.sampling_lhs_sampled.value
    sample_groups_scanned = stats.sample_groups_scanned.value

    total_auto = auto_fd + auto_sk
    sent_to_validation = total_candidates - skipped_complement - pruned_superset - total_auto

    candidate_denom = max(1, total_candidates)

    tb = stats.timing_breakdown()

    row = {
        "run_id": run_id,
        "status": "ok",
        "error": "",
        "experiment_name": experiment_name,
        "ablation_variant": ablation_variant,
        "representation": representation,
        "encoding_enabled": encoding_enabled,
        "packing_enabled": packing_enabled,
        "dataset": config["input"]["path"],
        "rows": dataset_rows,
        "cols": dataset_cols,
        "timestamp": datetime.now().isoformat(timespec="seconds"),

        "max_cardinality": max_cardinality,
        "avg_cardinality": avg_cardinality,
        "packed_bits": packed_bits,
        "bits_per_column": bits_per_column,
        "working_rdd_size_mb": working_rdd_size_mb,

        "executors": executors,
        "cores_per_executor": cores_per_executor,
        "max_partition_mb": config["spark"]["max_partition_mb"],
        "partitions": partitions,

        "fd_enabled": config.get("fd", {}).get("enabled", False),
        "sampling_enabled": config.get("sampling", {}).get("enabled", False),
        "largest_k": config.get("sampling", {}).get("largest_k", 0),

        "levels_processed": levels_processed,
        "lhs_sets_processed": lhs_sets_processed,
        "lhs_search_space": lhs_search_space,

        "total_mvds": total_mvds,
        "fds_discovered": stats.total_fds_discovered.value,
        "valid_mvds_found": stats.valid_mvds_found.value,
        "avg_lhs_size": avg_lhs_size,
        "avg_rhs_size": avg_rhs_size,
        "max_lhs_size": max_lhs_size,

        "candidates_generated": total_candidates,
        "candidates_sent_to_validation": sent_to_validation,
        "auto_validated_fd": auto_fd,
        "auto_validated_superkey": auto_sk,
        "skipped_complement": skipped_complement,
        "pruned_superset": pruned_superset,
        "rejected_by_sampling": rejected_sampling,
        "rejected_lhs_minimal": rejected_lhs_min,
        "rejected_rhs_minimal": rejected_rhs_min,

        "sampling_lhs_sampled": sampling_lhs_sampled,
        "sample_groups_scanned": sample_groups_scanned,

        "skip_complement_total_pct":  round((skipped_complement / candidate_denom) * 100, 2),
        "superset_total_pct": round((pruned_superset   / candidate_denom) * 100, 2),
        "auto_fd_total_pct":  round((auto_fd           / candidate_denom) * 100, 2),
        "auto_sk_total_pct":  round((auto_sk           / candidate_denom) * 100, 2),
        "sampling_total_pct": round((rejected_sampling / candidate_denom) * 100, 2),

        "read_time":       tb["read_time"],
        "encoding_time":   tb["encoding_time"],
        "packing_time":    tb["packing_time"],
        "raw_time":        tb["raw_time"],
        "fd_time":         tb["fd_time"],
        "closure_time":    tb["closure_time"],
        "candidate_time":  tb["candidate_time"],
        "sampling_time":   tb["sampling_time"],
        "validation_time": tb["validation_time"],
        "cleanup_time":    tb["cleanup_time"],
        "other_time":      tb["other_time"],
        "total_time":      tb["total_time"],

        "read_pct":       tb["read_pct"],
        "encoding_pct":   tb["encoding_pct"],
        "packing_pct":    tb["packing_pct"],
        "raw_pct":        tb["raw_pct"],
        "fd_pct":         tb["fd_pct"],
        "closure_pct":    tb["closure_pct"],
        "candidate_pct":  tb["candidate_pct"],
        "sampling_pct":   tb["sampling_pct"],
        "validation_pct": tb["validation_pct"],
        "cleanup_pct":    tb["cleanup_pct"],
        "other_pct":      tb["other_pct"],

        "prune_complement":     _pruning_enabled(config, "complement_pruning"),
        "prune_superset":       _pruning_enabled(config, "superset_pruning"),
        "prune_fd":             _pruning_enabled(config, "fd_pruning"),
        "prune_discovered_mvd": _pruning_enabled(config, "discovered_mvd_pruning"),

        "fd_shuffle_jobs": stats.fd_shuffle_jobs.value,
        "sampling_shuffle_jobs": stats.sampling_shuffle_jobs.value,
        "mvd_shuffle_jobs": stats.mvd_shuffle_jobs.value,
        "total_shuffle_jobs": (stats.fd_shuffle_jobs.value
                               + stats.sampling_shuffle_jobs.value
                               + stats.mvd_shuffle_jobs.value),

        "max_group_size": stats.max_group_size.value,
        "total_groups": stats.total_groups.value,
        "avg_group_size": round(
            stats.total_group_size.value / max(1, stats.total_groups.value), 4),

        "stages_completed":          spark_metrics.get("stages_completed", 0),
        "shuffle_read_mb":           spark_metrics.get("shuffle_read_mb", 0),
        "shuffle_write_mb":          spark_metrics.get("shuffle_write_mb", 0),
        "input_read_mb":             spark_metrics.get("input_read_mb", 0),
        "output_write_mb":           spark_metrics.get("output_write_mb", 0),
        "executor_run_time_sec":     spark_metrics.get("executor_run_time_sec", 0),
        "executor_cpu_time_sec":     spark_metrics.get("executor_cpu_time_sec", 0),
        "executor_deserialize_sec":  spark_metrics.get("executor_deserialize_sec", 0),
        "result_serialization_sec":  spark_metrics.get("result_serialization_sec", 0),
        "jvm_gc_time_sec":           spark_metrics.get("jvm_gc_time_sec", 0),
        "memory_spill_mb":           spark_metrics.get("memory_spill_mb", 0),
        "disk_spill_mb":             spark_metrics.get("disk_spill_mb", 0),
        "peak_execution_memory_mb":  spark_metrics.get("peak_execution_memory_mb", 0),
        "num_tasks":                 spark_metrics.get("num_tasks", 0),
        "num_failed_tasks":          spark_metrics.get("num_failed_tasks", 0),
        "num_completed_tasks":       spark_metrics.get("num_completed_tasks", 0),
    }

    _write_result_row(output_path, row)