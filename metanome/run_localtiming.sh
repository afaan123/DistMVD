#!/usr/bin/env bash
# Runs every experiment in the config in its own fresh JVM with ENFORCED limits:
#   CPU  : 16 logical CPUs   -> taskset affinity + -XX:ActiveProcessorCount
#   RAM  : 64 GB             -> cgroup MemoryMax (systemd-run --user), else ulimit -v
#   Disk : 64 GB             -> ulimit -f (per file) + watchdog on total output size
#   Heap : 32 GB             -> -Xmx32g (+ caps on metaspace / direct memory / code cache)
# Usage: bash metanome/run_localtiming.sh [config]   (default: metanome/experiments.conf)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIG_FILE="${1:-$SCRIPT_DIR/experiments.conf}"
CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"

CPU_CORES=16
RAM_GB=64
DISK_GB=64
HEAP_GB=32

RAM_BYTES=$((RAM_GB * 1024 * 1024 * 1024))
RAM_KB=$((RAM_GB * 1024 * 1024))
DISK_BYTES=$((DISK_GB * 1024 * 1024 * 1024))
DISK_BLOCKS_1K=$((DISK_GB * 1024 * 1024))   # bash 'ulimit -f' unit is 1024 bytes

die() { echo "ERROR: $*" >&2; exit 1; }
warn() { echo "WARNING: $*" >&2; }

[[ "$(uname -s)" == "Linux" ]] || die "This script enforces limits with Linux features; run it on the Linux server."
command -v taskset >/dev/null || die "taskset is required (util-linux)."
command -v python3 >/dev/null || die "python3 is required to select allowed CPUs."
command -v java    >/dev/null || die "java not found on PATH."
command -v javac   >/dev/null || die "javac not found on PATH."
[[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

# ---------------------------------------------------------------- CPU (16 cores)
CPU_SET="$(CPU_CORES=$CPU_CORES python3 - <<'PYCPU'
import os, sys
n = int(os.environ["CPU_CORES"])
allowed = sorted(os.sched_getaffinity(0))
if len(allowed) < n:
    sys.exit('ERROR: Need %d allowed logical CPUs; found %d.' % (n, len(allowed)))
if len(allowed) < n + 2:
    print('WARNING: Fewer than 2 additional allowed CPUs visible; verify host/scheduler headroom.', file=sys.stderr)
print(','.join(map(str, allowed[:n])))
PYCPU
)"

# ---------------------------------------------------------------- RAM (64 GB)
MEM_TOTAL_KB="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
if (( MEM_TOTAL_KB < RAM_KB )); then
  warn "Host has $((MEM_TOTAL_KB / 1024 / 1024)) GB RAM, less than the ${RAM_GB} GB budget."
fi

# Prefer a real cgroup memory limit (counts ALL process memory, not just heap).
# Probe it: start a scope with MemoryMax and read back the limit from inside it.
#
# Under nohup the run must survive logout. A systemd --user scope is killed at
# logout unless "lingering" is enabled, so only use cgroups when Linger=yes.
MEM_MODE="ulimit"
LINGER="$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null || echo no)"
if [[ "$LINGER" != "yes" ]]; then
  warn "systemd lingering is off for $(id -un); cgroup scopes would be killed at logout (nohup)."
  warn "Enable it with 'loginctl enable-linger $(id -un)' for the stronger cgroup RAM limit."
elif command -v systemd-run >/dev/null; then
  PROBE="$(systemd-run --user --scope --quiet \
      -p MemoryMax="$RAM_BYTES" -p MemorySwapMax=0 \
      bash -c 'cat "/sys/fs/cgroup$(head -1 /proc/self/cgroup | cut -d: -f3-)/memory.max"' \
      2>/dev/null || true)"
  if [[ "$PROBE" == "$RAM_BYTES" ]]; then
    MEM_MODE="cgroup"
  fi
fi
if [[ "$MEM_MODE" == "ulimit" ]]; then
  warn "cgroup memory limit unavailable (systemd-run --user); falling back to ulimit -v ${RAM_GB}G (virtual address space)."
fi

# ---------------------------------------------------------------- Disk (64 GB)
# Everything the run writes lives in these paths; their total size is capped.
TMP_DIR="$ROOT_DIR/metanome/tmp"
mkdir -p "$ROOT_DIR/build" "$ROOT_DIR/metanome/mvds" "$TMP_DIR"
DISK_PATHS=(
  "$ROOT_DIR/build"
  "$ROOT_DIR/metanome/mvds"
  "$TMP_DIR"
)
disk_used_bytes() {
  # experiment_log*.csv includes logs moved aside after a format change;
  # run_*.log is the nohup console output
  { du -sbc "${DISK_PATHS[@]}" "$ROOT_DIR"/metanome/experiment_log*.csv \
      "$ROOT_DIR"/metanome/run_*.log 2>/dev/null || true; } \
    | awk 'END {print $1 + 0}'
}
DISK_FREE="$(df -PB1 "$ROOT_DIR" | awk 'NR == 2 {print $4}')"
if (( DISK_FREE < DISK_BYTES )); then
  warn "Only $((DISK_FREE / 1024 / 1024 / 1024)) GB free on the output filesystem, less than the ${DISK_GB} GB budget."
fi

# ---------------------------------------------------------------- Java (32 GB heap)
JAVA_OPTS=(
  -Xms8g
  -Xmx${HEAP_GB}g
  -XX:MaxMetaspaceSize=1g
  -XX:MaxDirectMemorySize=1g
  -XX:ReservedCodeCacheSize=256m
  -XX:ActiveProcessorCount=$CPU_CORES
  -XX:MaxRAM=${RAM_GB}g
  -XX:+UseG1GC
  -XX:+ExitOnOutOfMemoryError
  -Djava.io.tmpdir="$TMP_DIR"
)
export MALLOC_ARENA_MAX=4   # keeps glibc from reserving huge virtual arenas (matters for ulimit -v)

export DISTMVD_CPU_CORES=$CPU_CORES
export DISTMVD_RAM_GB=$RAM_GB
export DISTMVD_DISK_GB=$DISK_GB
export DISTMVD_JAVA_HEAP_GB=$HEAP_GB
export DISTMVD_CPU_SET="$CPU_SET"
export DISTMVD_MEMORY_ENFORCEMENT="$MEM_MODE"
export DISTMVD_DISK_ENFORCEMENT="ulimit-f+watchdog"
export DISTMVD_ENFORCED=1

# ---------------------------------------------------------------- Build
cd "$ROOT_DIR"
javac -d build \
  metanome/LocalTiming.java \
  metanome/MvdFinder.java \
  metanome/ColumnBasedMvdFinder.java \
  metanome/MvDAlgorithmConfig.java \
  metanome/MvD.java \
  metanome/Relation.java \
  metanome/Tuple.java \
  metanome/StringTuple.java \
  metanome/PositionListIndex.java

printf 'Config         : %s\n' "$CONFIG_FILE"
printf 'CPU (enforced) : %d logical CPUs, affinity %s\n' "$CPU_CORES" "$CPU_SET"
printf 'RAM (enforced) : %d GB via %s\n' "$RAM_GB" "$MEM_MODE"
printf 'Disk (enforced): %d GB total output (ulimit -f + watchdog), currently %d MB used\n' \
  "$DISK_GB" "$(( $(disk_used_bytes) / 1024 / 1024 ))"
printf 'Heap (enforced): -Xmx%dg\n' "$HEAP_GB"

# ---------------------------------------------------------------- Experiments
EXP_COUNT="$(awk '
  /^[[:space:]]*[#!]/ {next}
  /^[[:space:]]*exp\.count[[:space:]]*[=:]/ {
    sub(/^[^=:]*[=:][[:space:]]*/, "", $0)
    sub(/[[:space:]]*[#!].*$/, "", $0)
    gsub(/[[:space:]]/, "", $0)
    count = $0
  }
  END {print (count == "" ? "1" : count)}
' "$CONFIG_FILE")"
[[ "$EXP_COUNT" =~ ^[1-9][0-9]*$ ]] || die "exp.count must be a positive integer, got: $EXP_COUNT"

run_one() {
  local i="$1"
  local cmd=(taskset -c "$CPU_SET" java "${JAVA_OPTS[@]}"
             -cp build de.metanome.algorithms.mvddet.LocalTiming "$CONFIG_FILE" "$i")
  (
    ulimit -f "$DISK_BLOCKS_1K"
    if [[ "$MEM_MODE" == "cgroup" ]]; then
      exec systemd-run --user --scope --quiet \
        -p MemoryMax="$RAM_BYTES" -p MemorySwapMax=0 "${cmd[@]}"
    else
      ulimit -v "$RAM_KB"
      exec "${cmd[@]}"
    fi
  ) &
  local pid=$!

  # Disk watchdog: kill the JVM if total output exceeds the budget.
  while kill -0 "$pid" 2>/dev/null; do
    if (( $(disk_used_bytes) > DISK_BYTES )); then
      echo "ERROR: disk budget of ${DISK_GB} GB exceeded; killing experiment $i." >&2
      kill -TERM "$pid" 2>/dev/null || true
      sleep 5
      kill -KILL "$pid" 2>/dev/null || true
      break
    fi
    sleep 1
  done

  local rc=0
  wait "$pid" || rc=$?
  return "$rc"
}

FAILED=()
for ((i = 1; i <= EXP_COUNT; i++)); do
  if (( i > 1 )); then
    printf '\nPrevious JVM exited. Waiting 30 seconds before experiment %d...\n' "$i"
    sleep 30
  fi
  printf '\n===== Experiment %d/%d: starting fresh JVM =====\n' "$i" "$EXP_COUNT"
  rc=0
  run_one "$i" || rc=$?
  if (( rc == 0 )); then
    printf '===== Experiment %d/%d: finished; JVM terminated =====\n' "$i" "$EXP_COUNT"
  else
    printf '===== Experiment %d/%d: FAILED (exit %d; 137 = killed, e.g. RAM/disk limit) =====\n' \
      "$i" "$EXP_COUNT" "$rc" >&2
    FAILED+=("$i")
  fi
done

if (( ${#FAILED[@]} > 0 )); then
  printf '\nCompleted with failures in experiment(s): %s\n' "${FAILED[*]}" >&2
  exit 1
fi
printf '\nAll %d experiments completed.\n' "$EXP_COUNT"
