#!/bin/bash
set -e
SPARK_HOME="/home/node/spark"
MASTER_IP="192.168.2.172"
MASTER="spark://$MASTER_IP:7077"
PROJECT_DIR="/home/node/mvd_project/Distributed_Mutlivalued_thesis"
export MVD_RESULTS_DIR="$PROJECT_DIR/source/main"
ALL_WORKERS=(
192.168.2.23
192.168.2.183
192.168.2.184
192.168.2.156
192.168.2.159
192.168.2.135
)
# 192.168.2.252
NUM_WORKERS=$1
CONFIG_FILE=$2
if [ -z "$NUM_WORKERS" ] || [ -z "$CONFIG_FILE" ]; then
    echo "Usage: ./run_cluster.sh <num_workers> <config_path>"
    echo "  e.g. ./run_cluster.sh 7 source/main/config/config_synthetic_1m_5c.yaml"
    exit 1
fi
if [ ! -f "$PROJECT_DIR/$CONFIG_FILE" ]; then
    echo "ERROR: config file not found: $PROJECT_DIR/$CONFIG_FILE"
    exit 1
fi
NUM_EXPERIMENTS=$(python3 -c "
import yaml
with open('$PROJECT_DIR/$CONFIG_FILE') as f:
    cfg = yaml.safe_load(f)
print(len(cfg['experiments']))
")
TOTAL_WORKERS=$((NUM_WORKERS + 1))
EXECUTOR_CORES=4
TOTAL_CORES=$((TOTAL_WORKERS * EXECUTOR_CORES))
if [ "$NUM_WORKERS" -eq 1 ]; then
    DRIVER_MEM="3G"
    EXECUTOR_MEM="8G"
    OFFHEAP_SIZE="1g"
else
    DRIVER_MEM="3G"
    EXECUTOR_MEM="8G"
    OFFHEAP_SIZE="1g"
fi
# Drop the OS page cache on every active node so disk-read time doesn't drift
# between runs (a warm cache makes later experiments read the CSV from RAM and
# look artificially fast). Active nodes = whatever is in conf/workers (master +
# remote workers). Best-effort: needs passwordless sudo; warns instead of failing.
drop_page_cache() {
    echo "Dropping OS page cache on active nodes..."
    while read -r HOST; do
        [ -z "$HOST" ] && continue
        if ssh -o BatchMode=yes -o ConnectTimeout=5 "node@$HOST" \
            "sync && echo 3 | sudo -n tee /proc/sys/vm/drop_caches > /dev/null" 2>/dev/null; then
            echo "  $HOST: cache dropped"
        else
            echo "  $HOST: WARN could not drop cache (need passwordless sudo?)"
        fi
    done < "$SPARK_HOME/conf/workers"
}
echo "======================================"
echo "CLUSTER PLAN"
echo "  Remote workers:         $NUM_WORKERS"
echo "  + master as worker:     1 ($MASTER_IP)"
echo "  Total workers:          $TOTAL_WORKERS"
echo "  Cores per worker:       $EXECUTOR_CORES"
echo "  Total cores:            $TOTAL_CORES"
echo "  Driver memory:          $DRIVER_MEM"
echo "  Executor memory:        $EXECUTOR_MEM"
echo "  Off-heap size:          $OFFHEAP_SIZE"
echo "  Config file:            $CONFIG_FILE"
echo "  Experiments:            $NUM_EXPERIMENTS (separate spark-submit each)"
echo "======================================"
echo "======================================"
echo "ENSURING PASSWORDLESS SSH"
echo "======================================"
# Make sure a key exists
if [ ! -f "$HOME/.ssh/id_rsa" ]; then
    echo "No SSH key found, generating one..."
    ssh-keygen -t rsa -N "" -f "$HOME/.ssh/id_rsa"
fi
# Authorize the master's own key for SSH-to-self (needed because the master
# is also a worker, and start-workers.sh SSHes into every worker incl. itself)
mkdir -p "$HOME/.ssh"
touch "$HOME/.ssh/authorized_keys"
chmod 700 "$HOME/.ssh"
chmod 600 "$HOME/.ssh/authorized_keys"
if ! grep -qF "$(cat "$HOME/.ssh/id_rsa.pub")" "$HOME/.ssh/authorized_keys"; then
    cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
    echo "Authorized own key for SSH-to-self"
fi
# Ensure passwordless SSH to master (self) and all active workers.
# BatchMode test avoids hanging; ssh-copy-id only runs (and may prompt once) if needed.
for HOST in "$MASTER_IP" "${ALL_WORKERS[@]:0:$NUM_WORKERS}"; do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
        "node@$HOST" "true" 2>/dev/null; then
        echo "  $HOST: passwordless SSH OK"
    else
        echo "  $HOST: setting up passwordless SSH (may prompt for password once)"
        ssh-copy-id -o StrictHostKeyChecking=accept-new "node@$HOST"
    fi
done
echo "======================================"
echo "UPDATING MASTER REPOSITORY"
echo "======================================"
cd $PROJECT_DIR
git reset --hard
git pull origin server
echo "======================================"
echo "UPDATING WORKER REPOSITORIES"
echo "======================================"
for ((i=0; i<NUM_WORKERS; i++)); do
    WORKER=${ALL_WORKERS[$i]}
    echo "Updating worker: $WORKER"
    ssh node@$WORKER "cd $PROJECT_DIR && git reset --hard && git pull origin server"
done
echo "======================================"
echo "SYNCING DATASETS TO WORKERS"
echo "======================================"
for ((i=0; i<NUM_WORKERS; i++)); do
    WORKER=${ALL_WORKERS[$i]}
    echo "Syncing data -> $WORKER"
    rsync -a --delete "$PROJECT_DIR/source/main/config/data/" \
        "node@$WORKER:$PROJECT_DIR/source/main/config/data/"
done
echo "======================================"
echo "CONFIGURING $TOTAL_WORKERS WORKERS"
echo "======================================"
> $SPARK_HOME/conf/workers
echo "$MASTER_IP" >> $SPARK_HOME/conf/workers
for ((i=0; i<NUM_WORKERS; i++)); do
    echo "${ALL_WORKERS[$i]}" >> $SPARK_HOME/conf/workers
done
echo "ACTIVE WORKERS:"
cat $SPARK_HOME/conf/workers
echo "======================================"
echo "RESTARTING WORKERS"
echo "======================================"
$SPARK_HOME/sbin/stop-workers.sh || true
sleep 3
$SPARK_HOME/sbin/start-workers.sh
sleep 10
echo "======================================"
echo "CREATING PYTHON DEPENDENCY ZIP"
echo "======================================"
cd $PROJECT_DIR/source/main
zip -r -FS dependencies.zip *.py > /dev/null
echo "======================================"
echo "RUNNING DISTRIBUTED MVD DISCOVERY"
echo "  Config: $CONFIG_FILE"
echo "  Experiments: $NUM_EXPERIMENTS (one fresh spark-submit each)"
echo "======================================"
FAILED_EXPERIMENTS=""
for ((EXP_IDX=1; EXP_IDX<=NUM_EXPERIMENTS; EXP_IDX++)); do
    IDX0=$((EXP_IDX - 1))
    read MAX_PART_MB NUM_PARTITIONS SHUFFLE_PARTITIONS <<< $(python3 -c "
import yaml
with open('$PROJECT_DIR/$CONFIG_FILE') as f:
    cfg = yaml.safe_load(f)
spark = cfg['experiments'][$IDX0]['spark']
parts = spark['partitions']
cores = $TOTAL_CORES
mode = parts['mode'] if 'mode' in parts else 'manual'
if mode == 'auto':
    auto = parts['auto']
    num = max(1, round(auto['num_multiplier'] * cores))
    shuf = max(1, round(auto['shuffle_multiplier'] * cores))
else:
    num = parts['num_partitions']
    shuf = parts['shuffle_partitions']
print(spark['max_partition_mb'], num, shuf)
")
    MAX_PART_BYTES=$((MAX_PART_MB * 1024 * 1024))

    echo "======================================"
    echo "EXPERIMENT $EXP_IDX / $NUM_EXPERIMENTS"
    echo "  max_partition_mb: $MAX_PART_MB ($MAX_PART_BYTES bytes)  num_partitions: $NUM_PARTITIONS  shuffle_partitions: $SHUFFLE_PARTITIONS"
    echo "======================================"

    drop_page_cache

    if $SPARK_HOME/bin/spark-submit \
        --master $MASTER \
        --deploy-mode client \
        --total-executor-cores $TOTAL_CORES \
        --executor-cores $EXECUTOR_CORES \
        --executor-memory $EXECUTOR_MEM \
        --driver-memory $DRIVER_MEM \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=$OFFHEAP_SIZE \
        --conf spark.default.parallelism=$NUM_PARTITIONS \
        --conf spark.sql.adaptive.coalescePartitions.enabled=false \
        --conf spark.sql.files.maxPartitionBytes=$MAX_PART_BYTES \
        --conf spark.locality.wait=0s \
        --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=30s \
        --conf spark.rdd.compress=true \
        --py-files dependencies.zip \
        main.py \
        "$PROJECT_DIR/$CONFIG_FILE" \
        $EXP_IDX; then
        echo "EXPERIMENT $EXP_IDX OK"
    else
        echo "EXPERIMENT $EXP_IDX FAILED (continuing with the rest)"
        FAILED_EXPERIMENTS="$FAILED_EXPERIMENTS $EXP_IDX"
    fi
done
echo "======================================"
if [ -n "$FAILED_EXPERIMENTS" ]; then
    echo "FINISHED with FAILURES in experiments:$FAILED_EXPERIMENTS"
else
    echo "ALL $NUM_EXPERIMENTS EXPERIMENTS FINISHED"
fi
echo "======================================"
