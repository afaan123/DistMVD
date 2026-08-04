import time

class Stats:
    def __init__(self, sc):
        self.total_candidates_generated = sc.accumulator(0)
        self.skipped_complement = sc.accumulator(0)
        self.auto_validated_fd = sc.accumulator(0)
        self.auto_validated_superkey = sc.accumulator(0)
        self.pruned_superset = sc.accumulator(0)
        self.rejected_by_sampling = sc.accumulator(0)

        self.sampling_lhs_sampled = sc.accumulator(0)
        self.sample_groups_scanned = sc.accumulator(0)

        self.valid_mvds_found = sc.accumulator(0)
        self.rejected_lhs_minimal = sc.accumulator(0)
        self.rejected_rhs_minimal = sc.accumulator(0)

        self.total_fds_discovered = sc.accumulator(0)

        self.levels_processed = sc.accumulator(0)
        self.lhs_sets_processed = sc.accumulator(0)

        self.fd_shuffle_jobs = sc.accumulator(0)
        self.sampling_shuffle_jobs = sc.accumulator(0)
        self.mvd_shuffle_jobs = sc.accumulator(0)

        self.read_time = sc.accumulator(0.0)
        self.encoding_time = sc.accumulator(0.0)
        self.packing_time = sc.accumulator(0.0)
        self.raw_time = sc.accumulator(0.0)
        self.fd_time = sc.accumulator(0.0)
        self.closure_time = sc.accumulator(0.0)
        self.candidate_time = sc.accumulator(0.0)
        self.sampling_time = sc.accumulator(0.0)
        self.validation_time = sc.accumulator(0.0)
        self.cleanup_time = sc.accumulator(0.0)
        self.total_time = sc.accumulator(0.0)

        self.max_group_size = sc.accumulator(0)
        self.total_groups = sc.accumulator(0)
        self.total_group_size = sc.accumulator(0)

        self.wall_clock_start = None

    def start_total_timer(self):
        self.wall_clock_start = time.time()

    def end_total_timer(self):
        if self.wall_clock_start is not None:
            self.total_time.add(time.time() - self.wall_clock_start)

    def record_stat(self, name, value=1):
        if hasattr(self, name):
            getattr(self, name).add(value)

    def record_elapsed(self, name, start):
        if hasattr(self, name):
            getattr(self, name).add(time.time() - start)

    def update_max_stat(self, name, value):
        if hasattr(self, name):
            current = getattr(self, name).value
            if value > current:
                getattr(self, name).add(value - current)


    def record_level_stats(self, lhs_at_level):
        self.record_stat("levels_processed", 1)
        self.record_stat("lhs_sets_processed", len(lhs_at_level))
 
    def print_config(self, fd_enabled, sampling_enabled,
                     bits, num_attributes, num_partitions,
                     shuffle_partitions, total_rows):
        print(f"\n===== CONFIG =====\n"
              f"FD Enabled         : {fd_enabled}\n"
              f"Sampling Enabled   : {sampling_enabled}\n"
              f"BITS per column    : {bits}\n"
              f"Total packed bits  : {num_attributes * bits}\n"
              f"Num Partitions     : {num_partitions}\n"
              f"Shuffle Partitions : {shuffle_partitions}\n"
              f"Total Rows         : {total_rows}\n"
              f"Num Attributes     : {num_attributes}")

    def auto_validated_total(self):
        return self.auto_validated_fd.value + self.auto_validated_superkey.value

    def sent_to_exact_validation(self):
        return (self.total_candidates_generated.value
                - self.skipped_complement.value
                - self.pruned_superset.value
                - self.auto_validated_total())

    def avg_group_size(self):
        groups = self.total_groups.value
        return (self.total_group_size.value / groups) if groups > 0 else 0.0

    def validation_efficiency(self):
        eligible = self.total_candidates_generated.value - self.skipped_complement.value
        return (self.valid_mvds_found.value / eligible) if eligible > 0 else 0.0

    PHASE_TIMERS = [
        ("read",       "read_time"),
        ("encoding",   "encoding_time"),
        ("packing",    "packing_time"),
        ("raw",        "raw_time"),
        ("fd",         "fd_time"),
        ("closure",    "closure_time"),
        ("candidate",  "candidate_time"),
        ("sampling",   "sampling_time"),
        ("validation", "validation_time"),
        ("cleanup",    "cleanup_time"),
    ]

    def timing_breakdown(self):
        total = self.total_time.value
        denom = total if total > 0 else 1.0

        out = {}
        measured = 0.0
        for name, attr in self.PHASE_TIMERS:
            seconds = getattr(self, attr).value
            measured += seconds
            out[f"{name}_time"] = round(seconds, 4)
            out[f"{name}_pct"] = round((seconds / denom) * 100, 2)

        other = max(0.0, total - measured)
        out["other_time"] = round(other, 4)
        out["other_pct"] = round((other / denom) * 100, 2)
        out["total_time"] = round(total, 4)
        return out

    def print_stats(self):
        generated = self.total_candidates_generated.value
        skipped_complement = self.skipped_complement.value
        auto = self.auto_validated_total()
        sent = self.sent_to_exact_validation()
        sampling_rejected = self.rejected_by_sampling.value
        exact_checked = sent - sampling_rejected

        print("\n" + "=" * 60)
        print("DISCOVERY STATISTICS")
        print("=" * 60)

        print("\nFunctional Dependencies:")
        print(f"  FDs discovered: {self.total_fds_discovered.value:,}")

        print("\nLevel Processing:")
        print(f"  Levels processed:    {self.levels_processed.value:,}")
        print(f"  LHS sets processed:  {self.lhs_sets_processed.value:,}")

        print("\nCandidate Accounting:")
        print(f"  Generated:                  {generated:,}")
        print(f"  Skipped complement (|Y|==|Z|, Y>Z): {skipped_complement:,}")
        print(f"  Pruned superset (non-min):  {self.pruned_superset.value:,}")
        print(f"  Auto-validated (FD-implied):{self.auto_validated_fd.value:,}")
        print(f"  Auto-validated (superkey):  {self.auto_validated_superkey.value:,}")
        print(f"  Sent to exact validation:   {sent:,}")
        print(f"    Rejected by sampling:     {sampling_rejected:,}")
        print(f"    Run through full check:   {exact_checked:,}")

        print("\nSampling Decisions:")
        print(f"  LHS sampled (top-k groups): {self.sampling_lhs_sampled.value:,}")

        print("\nFinal Cleanup:")
        print(f"  Rejected (LHS not minimal): {self.rejected_lhs_minimal.value:,}")
        print(f"  Rejected (RHS not minimal): {self.rejected_rhs_minimal.value:,}")
        print(f"  Valid minimal MVDs:         {self.valid_mvds_found.value:,}")

        print("\nValidation Efficiency:")
        print(f"  valid / full-check candidates: {self.validation_efficiency():.4f}")

        print("\nShuffle Jobs:")
        total_shuffles = (self.fd_shuffle_jobs.value
                          + self.sampling_shuffle_jobs.value
                          + self.mvd_shuffle_jobs.value)
        print(f"  FD shuffles:             {self.fd_shuffle_jobs.value:,}")
        print(f"  Sampling shuffles:       {self.sampling_shuffle_jobs.value:,}")
        print(f"  MVD validation shuffles: {self.mvd_shuffle_jobs.value:,}")
        print(f"  Total shuffles:          {total_shuffles:,}")

        tb = self.timing_breakdown()
        total = tb["total_time"] or 1.0
        print("\nTimings (seconds | % of wall clock):")
        print(f"  CSV read:            {tb['read_time']:.2f}s  ({tb['read_pct']:.1f}%)")
        print(f"  Encoding:            {tb['encoding_time']:.2f}s  ({tb['encoding_pct']:.1f}%)")
        print(f"  Packing:             {tb['packing_time']:.2f}s  ({tb['packing_pct']:.1f}%)")
        print(f"  Raw build:           {tb['raw_time']:.2f}s  ({tb['raw_pct']:.1f}%)")
        print(f"  FD discovery:        {tb['fd_time']:.2f}s  ({tb['fd_pct']:.1f}%)")
        print(f"  Closure compute:     {tb['closure_time']:.2f}s  ({tb['closure_pct']:.1f}%)")
        print(f"  Candidate generate:  {tb['candidate_time']:.2f}s  ({tb['candidate_pct']:.1f}%)")
        print(f"  MVD sampling:        {tb['sampling_time']:.2f}s  ({tb['sampling_pct']:.1f}%)")
        print(f"  MVD validation:      {tb['validation_time']:.2f}s  ({tb['validation_pct']:.1f}%)")
        print(f"  Minimality cleanup:  {tb['cleanup_time']:.2f}s  ({tb['cleanup_pct']:.1f}%)")
        print(f"  Other/overhead:      {tb['other_time']:.2f}s  ({tb['other_pct']:.1f}%)")
        print(f"  Total (wall clock):  {tb['total_time']:.2f}s")

        print("\nData Skew Indicators:")
        print(f"  Max group size: {self.max_group_size.value:,}")
        print(f"  Avg group size: {self.avg_group_size():.2f}")

        print("=" * 60 + "\n")


def _fetch_stages(sc):
    import requests
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"
    response = requests.get(url, timeout=5)
    if response.status_code != 200:
        print(f"[STATS] Spark UI returned HTTP {response.status_code} - skipping metrics.")
        return None
    return response.json()


def print_spark_metrics(sc):
    try:
        import requests
        stages = _fetch_stages(sc)
        if stages is None:
            return

        bytes_to_mb = lambda b: b / (1024 * 1024)
        ms_to_sec = lambda ms: ms / 1000.0

        shuffle_read = sum(s.get("shuffleReadBytes", 0) for s in stages)
        shuffle_write = sum(s.get("shuffleWriteBytes", 0) for s in stages)
        input_bytes = sum(s.get("inputBytes", 0) for s in stages)
        output_bytes = sum(s.get("outputBytes", 0) for s in stages)
        executor_run_ms = sum(s.get("executorRunTime", 0) for s in stages)
        executor_cpu_raw = sum(s.get("executorCpuTime", 0) for s in stages)
        deserialize_ms = sum(s.get("executorDeserializeTime", 0) for s in stages)
        serialize_ms = sum(s.get("resultSerializationTime", 0) for s in stages)
        jvm_gc_ms = sum(s.get("jvmGcTime", 0) for s in stages)
        memory_spilled = sum(s.get("memoryBytesSpilled", 0) for s in stages)
        disk_spilled = sum(s.get("diskBytesSpilled", 0) for s in stages)
        peak_memory = max((s.get("peakExecutionMemory", 0) for s in stages), default=0)
        num_tasks = sum(s.get("numTasks", 0) for s in stages)
        num_failed = sum(s.get("numFailedTasks", 0) for s in stages)
        num_completed = sum(s.get("numCompleteTasks", 0) for s in stages)

        if executor_run_ms > 0 and executor_cpu_raw > executor_run_ms * 1000:
            executor_cpu_sec = executor_cpu_raw / 1_000_000_000.0
        else:
            executor_cpu_sec = executor_cpu_raw / 1000.0

        print("\nSpark Stage Metrics:")
        print(f"  Stages completed:           {len(stages)}")
        print(f"  Shuffle read:               {bytes_to_mb(shuffle_read):.2f} MB")
        print(f"  Shuffle write:              {bytes_to_mb(shuffle_write):.2f} MB")
        print(f"  Input read:                 {bytes_to_mb(input_bytes):.2f} MB")
        print(f"  Output write:               {bytes_to_mb(output_bytes):.2f} MB")
        print(f"  Executor run time:          {ms_to_sec(executor_run_ms):.2f} s")
        print(f"  Executor CPU time:          {executor_cpu_sec:.2f} s")
        print(f"  Executor deserialize time:  {ms_to_sec(deserialize_ms):.2f} s")
        print(f"  Result serialize time:      {ms_to_sec(serialize_ms):.2f} s")
        print(f"  JVM GC time:                {ms_to_sec(jvm_gc_ms):.2f} s")
        print(f"  Memory spill:               {bytes_to_mb(memory_spilled):.2f} MB")
        print(f"  Disk spill:                 {bytes_to_mb(disk_spilled):.2f} MB")
        print(f"  Peak execution memory:      {bytes_to_mb(peak_memory):.2f} MB")
        print(f"  Tasks (completed/failed):   {num_completed:,} / {num_failed:,}")

        return {
            "stages_completed":         len(stages),
            "shuffle_read_mb":          round(bytes_to_mb(shuffle_read), 4),
            "shuffle_write_mb":         round(bytes_to_mb(shuffle_write), 4),
            "input_read_mb":            round(bytes_to_mb(input_bytes), 4),
            "output_write_mb":          round(bytes_to_mb(output_bytes), 4),
            "executor_run_time_sec":    round(ms_to_sec(executor_run_ms), 4),
            "executor_cpu_time_sec":    round(executor_cpu_sec, 4),
            "executor_deserialize_sec": round(ms_to_sec(deserialize_ms), 4),
            "result_serialization_sec": round(ms_to_sec(serialize_ms), 4),
            "jvm_gc_time_sec":          round(ms_to_sec(jvm_gc_ms), 4),
            "memory_spill_mb":          round(bytes_to_mb(memory_spilled), 4),
            "disk_spill_mb":            round(bytes_to_mb(disk_spilled), 4),
            "peak_execution_memory_mb": round(bytes_to_mb(peak_memory), 4),
            "num_tasks":                num_tasks,
            "num_failed_tasks":         num_failed,
            "num_completed_tasks":      num_completed,
        }
    except requests.exceptions.Timeout:
        print("[STATS] Spark UI request timed out - skipping metrics.")
    except Exception as e:
        print(f"[STATS] Could not fetch Spark metrics: {e}")