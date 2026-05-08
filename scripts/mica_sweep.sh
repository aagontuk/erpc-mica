#!/usr/bin/env bash
# mica_sweep.sh — Rate sweep benchmark for mica_server.
#
# Starts the server locally (process 0), then sweeps target_pps from
# --start-pps to --end-pps in steps of --inc-pps, followed by an
# unlimited run (target_pps=0).  Prints a table showing where rx
# throughput stops tracking tx (the server bottleneck knee point).
#
# Usage:
#   ./scripts/mica_sweep.sh [OPTIONS]
#
# Sweep options:
#   --start-pps N    First target rate in pps  (default: 500000)
#   --end-pps   N    Last  target rate in pps  (default: 6000000)
#   --inc-pps   N    Step  between rates in pps (default: 500000)
#
# Workload options:
#   --num-keys  N    Key space size; rounded up to next power of 2
#                    (default: 1048576)
#   --ycsb  a|b|c    YCSB workload mix:
#                      a = 50% GET / 50% SET
#                      b = 95% GET /  5% SET  (default)
#                      c = 100% GET
#   --skew  zipf|uniform
#                    Key access distribution:
#                      zipf    = Zipf theta=0.99  (default)
#                      uniform = uniform random
#
# Fixed parameters (edit variables below to change):
#   CLIENT_NODE        SSH target for client process  (default: node-1)
#   NUM_SERVER_THREADS Server RPC threads             (default: 1)
#   NUM_CLIENT_THREADS Client threads per process     (default: 8)
#   BATCH_SEND         RPCs enqueued per poll turn    (default: 3)
#   WARMUP_MS          Warmup window in ms            (default: 2000)
#   TEST_MS            Measurement window in ms       (default: 5000)
#   NUMA_NODE          NUMA node for both sides       (default: 1)
#   NUMA_PORTS         NIC port IDs on that node      (default: 3)

set -euo pipefail

# ---- Locate repo root -------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$REPO_ROOT/build/mica_server"

# ---- Fixed parameters (edit here) ------------------------------------------
CLIENT_NODE="node-1"
NUM_SERVER_THREADS=1
NUM_CLIENT_THREADS=8
BATCH_SEND=3
WARMUP_MS=2000
TEST_MS=5000
NUMA_NODE=1
NUMA_PORTS=3

# ---- Defaults for user options ----------------------------------------------
START_PPS=500000
END_PPS=6000000
INC_PPS=500000
NUM_KEYS=1048576
YCSB="b"
SKEW="zipf"

# ---- Argument parsing -------------------------------------------------------
usage() {
    grep '^#' "$0" | sed -n '/^# Usage:/,/^# Fixed/{ /^# Fixed/d; s/^# \{0,3\}//; p }'
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --start-pps) START_PPS="$2"; shift 2 ;;
        --end-pps)   END_PPS="$2";   shift 2 ;;
        --inc-pps)   INC_PPS="$2";   shift 2 ;;
        --num-keys)  NUM_KEYS="$2";  shift 2 ;;
        --ycsb)      YCSB="${2,,}";  shift 2 ;;
        --skew)      SKEW="${2,,}";  shift 2 ;;
        -h|--help)   usage ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

# ---- Validate ---------------------------------------------------------------
[[ "$YCSB" =~ ^[abc]$          ]] || { echo "Error: --ycsb must be a, b, or c"           >&2; exit 1; }
[[ "$SKEW" =~ ^(zipf|uniform)$ ]] || { echo "Error: --skew must be zipf or uniform"       >&2; exit 1; }
(( START_PPS > 0 ))                || { echo "Error: --start-pps must be > 0"             >&2; exit 1; }
(( END_PPS >= START_PPS ))         || { echo "Error: --end-pps must be >= --start-pps"    >&2; exit 1; }
(( INC_PPS > 0 ))                  || { echo "Error: --inc-pps must be > 0"               >&2; exit 1; }
[[ -x "$BINARY" ]]                 || { echo "Error: binary not found: $BINARY"           >&2; exit 1; }

# ---- Round up to next power of 2 --------------------------------------------
next_pow2() {
    local n=$1 p=1
    while (( p < n )); do p=$(( p * 2 )); done
    echo $p
}

ORIG_KEYS=$NUM_KEYS
NUM_KEYS=$(next_pow2 "$NUM_KEYS")
(( NUM_KEYS == ORIG_KEYS )) || \
    echo "Note: --num-keys $ORIG_KEYS rounded up to $NUM_KEYS (next power of 2)"

# ---- Derive binary flags ----------------------------------------------------
WORKLOAD="${YCSB^^}"
ZIPF_THETA=$([ "$SKEW" = "zipf" ] && echo "0.99" || echo "0")

# ---- Cleanup ----------------------------------------------------------------
SERVER_PID=""
SERVER_LOG=""

cleanup() {
    # Disown the server before killing so bash does not print "Killed" to the terminal.
    [[ -n "$SERVER_PID" ]] && disown "$SERVER_PID" 2>/dev/null || true
    [[ -n "$SERVER_PID" ]] && sudo kill -9 "$SERVER_PID" 2>/dev/null || true
    sudo pkill -9 mica_server 2>/dev/null || true
    ssh "$CLIENT_NODE" "sudo pkill -9 mica_server 2>/dev/null; true" 2>/dev/null || true
    sudo rm -f /dev/hugepages/rtemap_*
    ssh "$CLIENT_NODE" "sudo rm -f /dev/hugepages/rtemap_* 2>/dev/null; true" 2>/dev/null || true
    [[ -n "$SERVER_LOG" ]] && rm -f "$SERVER_LOG" || true
}
trap cleanup EXIT INT TERM

echo "Cleaning up stale processes..."
cleanup
sleep 1

# ---- Start server -----------------------------------------------------------
SERVER_LOG=$(mktemp /tmp/mica_server_XXXXXX.log)

echo "Starting server  (num_keys=$NUM_KEYS, threads=$NUM_SERVER_THREADS)..."
sudo "$BINARY" \
    --process_id 0 --num_processes 2 \
    --num_server_threads "$NUM_SERVER_THREADS" --num_client_threads 0 \
    --num_keys "$NUM_KEYS" \
    --numa_node "$NUMA_NODE" --numa_1_ports "$NUMA_PORTS" \
    > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

echo -n "Waiting for server init"
for (( i=1; i<=90; i++ )); do
    if grep -q "DpdkTransport created" "$SERVER_LOG" 2>/dev/null; then
        echo "  [${i}s]"; break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo ""; echo "Error: server exited unexpectedly:" >&2
        cat "$SERVER_LOG" >&2; exit 1
    fi
    sleep 1; echo -n "."
done
grep -q "DpdkTransport created" "$SERVER_LOG" 2>/dev/null || {
    echo ""; echo "Error: server did not become ready within 90s" >&2
    cat "$SERVER_LOG" >&2; exit 1
}

# ---- Build sweep target list ------------------------------------------------
SWEEP_TARGETS=()
for (( pps=START_PPS; pps<=END_PPS; pps+=INC_PPS )); do
    SWEEP_TARGETS+=("$pps")
done
SWEEP_TARGETS+=(0)   # always end with unlimited run

# ---- Print run header -------------------------------------------------------
echo ""
echo "┌─────────────────────────────────────────────────────────────────────┐"
printf "│  %-69s│\n" "MICA server rate sweep"
printf "│  %-69s│\n" "num_keys=$NUM_KEYS  ycsb=$WORKLOAD  skew=$SKEW (theta=$ZIPF_THETA)"
printf "│  %-69s│\n" "server_threads=$NUM_SERVER_THREADS  client_threads=$NUM_CLIENT_THREADS  batch_send=$BATCH_SEND  warmup=${WARMUP_MS}ms  test=${TEST_MS}ms"
printf "│  %-69s│\n" "sweep: ${START_PPS} → ${END_PPS} pps  step=${INC_PPS}  + unlimited"
echo "├──────────────┬────────────┬────────────┬────────────┬───────────────┤"
printf "│ %-12s │ %-10s │ %-10s │ %-10s │ %-13s │\n" \
    "target(Mpps)" "tx(Mpps)" "rx(Mpps)" "mean(µs)" "p99(µs)"
echo "├──────────────┼────────────┼────────────┼────────────┼───────────────┤"

# ---- Run sweep --------------------------------------------------------------
for TARGET in "${SWEEP_TARGETS[@]}"; do
    if (( TARGET == 0 )); then
        LABEL="unlimited"
    else
        LABEL=$(awk "BEGIN{printf \"%.2f\", $TARGET/1e6}")
    fi

    RAW=$(ssh "$CLIENT_NODE" \
        "cd '$REPO_ROOT' && sudo '$BINARY' \
            --process_id 1 --num_processes 2 \
            --num_server_threads $NUM_SERVER_THREADS \
            --num_client_threads $NUM_CLIENT_THREADS \
            --num_keys $NUM_KEYS \
            --target_pps $TARGET \
            --batch_send $BATCH_SEND \
            --test_ms $TEST_MS --warmup_ms $WARMUP_MS \
            --workload $WORKLOAD --zipf_theta $ZIPF_THETA \
            --numa_node $NUMA_NODE --numa_1_ports $NUMA_PORTS \
            2>/dev/null" 2>/dev/null) || true

    AGG_LINE=$(echo "$RAW" | grep "^Sent:"  || true)
    LAT_LINE=$(echo "$RAW" | grep "^mean="  || true)

    if [[ -z "$AGG_LINE" ]]; then
        printf "│ %-12s │ %-10s │ %-10s │ %-10s │ %-13s │\n" \
            "$LABEL" "FAILED" "FAILED" "-" "-"
        continue
    fi

    TX=$(   echo "$AGG_LINE" | grep -oP 'Sent: \d+ \(\K[0-9.]+')
    RX=$(   echo "$AGG_LINE" | grep -oP 'Received: \d+ \(\K[0-9.]+')
    MEAN=$( echo "$LAT_LINE" | grep -oP 'mean=\K[0-9.]+')
    P99=$(  echo "$LAT_LINE" | grep -oP 'p99=\K[0-9.]+')

    # Flag rows where the server is saturated.  With pending-table backpressure
    # tx naturally slows to match rx, so tx-rx gap stays small.  Instead detect
    # saturation via a sudden RTT inflation: mark the row if mean latency has
    # jumped to more than 10x the first (lowest-load) baseline.
    FLAG=""
    if [[ -n "$MEAN" ]]; then
        [[ -z "${BASELINE_MEAN:-}" ]] && BASELINE_MEAN="$MEAN"
        if awk "BEGIN{exit !($MEAN > $BASELINE_MEAN * 10)}"; then
            FLAG=" ◄"
        fi
    fi

    printf "│ %-12s │ %-10s │ %-10s │ %-10s │ %-13s │\n" \
        "$LABEL" \
        "${TX:-n/a}" \
        "${RX:-n/a}${FLAG}" \
        "${MEAN:-n/a}" \
        "${P99:-n/a}"
done

echo "└──────────────┴────────────┴────────────┴────────────┴───────────────┘"
echo ""
echo "◄ = mean RTT jumped >10x above baseline; server is saturated at this point."
