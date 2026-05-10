#!/usr/bin/env bash
# mica_sweep.sh — Client-thread sweep to find mica_server max throughput.
#
# Starts the server locally (process 0), then sweeps --num-client-threads
# from 1 up to --max-client-threads, printing throughput and latency for
# each point.  Stops early when aggregate Mrps plateaus (< 5% gain over
# the previous point), which indicates the server is saturated.
#
# Add --grid to run the full combination matrix of server threads (1..N),
# YCSB workloads (A/B/C), and skew (zipf/uniform), saving peak results to
# a CSV file.
#
# Usage:
#   ./scripts/mica_sweep.sh [OPTIONS]
#
# Options:
#   --num-server-threads N   Server RPC threads (default: 1; grid: max threads to sweep)
#   --max-client-threads N   Sweep client threads 1..N (default: 16)
#   --num-keys N             Key space size; rounded up to next power of 2
#                            (default: 1048576)
#   --ycsb a|b|c             YCSB workload mix (single-config mode only):
#                              a = 50% GET / 50% SET
#                              b = 95% GET /  5% SET  (default)
#                              c = 100% GET
#   --skew zipf|uniform      Key access distribution (single-config mode only,
#                            default: zipf)
#   --test-ms N              Test duration per sweep point in ms (default: 5000)
#   --grid                   Grid mode: sweep all combos of server threads,
#                            ycsb, and skew; save peak results to CSV
#   --csv-out FILE           CSV output file for grid mode
#                            (default: mica_results_<timestamp>.csv)
#
# Fixed parameters (edit variables below to change):
#   CLIENT_NODE   SSH target for client process  (default: node-1)
#   NUMA_NODE     NUMA node for both sides       (default: 1)
#   NUMA_PORTS    NIC port IDs on that node      (default: 3)

set -euo pipefail

# ---- Locate repo root -------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$REPO_ROOT/build/mica_server"

# ---- Fixed parameters (edit here) ------------------------------------------
CLIENT_NODE="node-1"
NUMA_NODE=1
NUMA_PORTS=3

# ---- Defaults for user options ----------------------------------------------
NUM_SERVER_THREADS=1
MAX_CLIENT_THREADS=16
NUM_KEYS=1048576
YCSB="b"
SKEW="zipf"
TEST_MS=5000
GRID=0
CSV_OUT=""

# ---- Argument parsing -------------------------------------------------------
usage() {
    grep '^#' "$0" | sed -n '/^# Usage:/,/^# Fixed/{ /^# Fixed/d; s/^# \{0,3\}//; p }'
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --num-server-threads) NUM_SERVER_THREADS="$2"; shift 2 ;;
        --max-client-threads) MAX_CLIENT_THREADS="$2"; shift 2 ;;
        --num-keys)           NUM_KEYS="$2";           shift 2 ;;
        --ycsb)               YCSB="${2,,}";           shift 2 ;;
        --skew)               SKEW="${2,,}";           shift 2 ;;
        --test-ms)            TEST_MS="$2";            shift 2 ;;
        --grid)               GRID=1;                  shift ;;
        --csv-out)            CSV_OUT="$2";            shift 2 ;;
        -h|--help)   usage ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

# ---- Validate ---------------------------------------------------------------
[[ "$YCSB" =~ ^[abc]$          ]] || { echo "Error: --ycsb must be a, b, or c"      >&2; exit 1; }
[[ "$SKEW" =~ ^(zipf|uniform)$ ]] || { echo "Error: --skew must be zipf or uniform" >&2; exit 1; }
[[ -x "$BINARY" ]]                 || { echo "Error: binary not found: $BINARY"      >&2; exit 1; }

if [[ "$GRID" == "1" && -z "$CSV_OUT" ]]; then
    CSV_OUT="mica_results_$(date +%Y%m%d_%H%M%S).csv"
fi

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

# ---- Derive binary flags (single-config mode) --------------------------------
WORKLOAD="${YCSB^^}"
ZIPF_THETA=$([ "$SKEW" = "zipf" ] && echo "0.99" || echo "0")

# ---- Server state -----------------------------------------------------------
SERVER_PID=""
SERVER_LOG=""

# ---- Server lifecycle functions ---------------------------------------------
start_server() {
    local nthreads=$1
    SERVER_LOG=$(mktemp /tmp/mica_server_XXXXXX.log)
    echo "Starting server (num_keys=$NUM_KEYS, threads=$nthreads)..."
    cd "$REPO_ROOT"
    sudo "$BINARY" \
        --process_id 0 --num_processes 2 \
        --num_server_threads "$nthreads" --num_client_threads 0 \
        --num_keys "$NUM_KEYS" \
        --numa_node "$NUMA_NODE" --numa_1_ports "$NUMA_PORTS" \
        > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!

    echo -n "Waiting for server init"
    local needed=$nthreads
    for (( i=1; i<=90; i++ )); do
        local found
        found=$(grep -c "DpdkTransport created" "$SERVER_LOG" 2>/dev/null || true)
        if (( found >= needed )); then
            echo "  [${i}s]"; break
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo ""; echo "Error: server exited unexpectedly:" >&2
            cat "$SERVER_LOG" >&2; exit 1
        fi
        sleep 1; echo -n "."
    done
    local found
    found=$(grep -c "DpdkTransport created" "$SERVER_LOG" 2>/dev/null || true)
    (( found >= needed )) || {
        echo ""; echo "Error: server did not become ready within 90s" >&2
        cat "$SERVER_LOG" >&2; exit 1
    }
}

stop_server() {
    [[ -n "$SERVER_PID" ]] && disown "$SERVER_PID" 2>/dev/null || true
    [[ -n "$SERVER_PID" ]] && sudo kill -9 "$SERVER_PID" 2>/dev/null || true
    sudo pkill -9 mica_server 2>/dev/null || true
    sudo rm -f /dev/hugepages/rtemap_*
    [[ -n "$SERVER_LOG" ]] && rm -f "$SERVER_LOG" || true
    SERVER_PID=""
    SERVER_LOG=""
}

# ---- Cleanup ----------------------------------------------------------------
cleanup() {
    stop_server
    ssh "$CLIENT_NODE" "sudo pkill -9 mica_server 2>/dev/null; true" 2>/dev/null || true
    ssh "$CLIENT_NODE" "sudo rm -f /dev/hugepages/rtemap_* 2>/dev/null; true" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Cleaning up stale processes..."
cleanup
sleep 1

# ---- Run one client point ---------------------------------------------------
# Outputs: AGG_MRPS, AVG_P50, AVG_P99  (sets variables in caller's scope)
# Uses: WORKLOAD, ZIPF_THETA, NUM_SERVER_THREADS, NUM_KEYS, TEST_MS (globals)
run_point() {
    local nthreads=$1
    local raw

    raw=$(ssh "$CLIENT_NODE" \
        "cd '$REPO_ROOT' && sudo '$BINARY' \
            --process_id 1 --num_processes 2 \
            --num_server_threads $NUM_SERVER_THREADS \
            --num_client_threads $nthreads \
            --num_keys $NUM_KEYS \
            --test_ms $TEST_MS \
            --workload $WORKLOAD --zipf_theta $ZIPF_THETA \
            --numa_node $NUMA_NODE --numa_1_ports $NUMA_PORTS \
            2>/dev/null") || true

    local stat_lines
    stat_lines=$(echo "$raw" | grep "^Thread " | tail -n "$nthreads")

    if [[ -z "$stat_lines" ]]; then
        AGG_MRPS="0"; AVG_P50="0"; AVG_P99="0"
        return 1
    fi

    local sum_mrps=0 sum_p50=0 sum_p99=0 n=0
    while IFS= read -r line; do
        local mrps p50 p99
        mrps=$(echo "$line" | grep -oP '[\d.]+ Mrps' | grep -oP '[\d.]+')
        p50=$( echo "$line" | grep -oP 'p50=\K[\d.]+')
        p99=$( echo "$line" | grep -oP 'p99=\K[\d.]+')
        sum_mrps=$(awk "BEGIN{printf \"%.3f\", $sum_mrps + ${mrps:-0}}")
        sum_p50=$( awk "BEGIN{printf \"%.2f\",  $sum_p50  + ${p50:-0}}")
        sum_p99=$( awk "BEGIN{printf \"%.2f\",  $sum_p99  + ${p99:-0}}")
        (( n++ )) || true
    done <<< "$stat_lines"

    AGG_MRPS="$sum_mrps"
    AVG_P50=$(awk "BEGIN{printf \"%.2f\", $sum_p50 / $n}")
    AVG_P99=$(awk "BEGIN{printf \"%.2f\", $sum_p99 / $n}")
}

# =============================================================================
# GRID MODE
# =============================================================================
if [[ "$GRID" == "1" ]]; then
    echo ""
    echo "Grid sweep: server threads 1..${NUM_SERVER_THREADS}, ycsb a/b/c, skew zipf/uniform"
    echo "Max client threads per point: ${MAX_CLIENT_THREADS}  test_ms=${TEST_MS}"
    echo "Output CSV: ${CSV_OUT}"
    echo ""

    # Write CSV header
    echo "threads,ycsb,skew,throughput_mrps,p50_us,p99_us" > "$CSV_OUT"

    for (( sthreads=1; sthreads<=NUM_SERVER_THREADS; sthreads++ )); do
        NUM_SERVER_THREADS_SAVED=$NUM_SERVER_THREADS
        NUM_SERVER_THREADS=$sthreads   # run_point reads this global

        start_server "$sthreads"

        for ycsb_iter in a b c; do
            for skew_iter in zipf uniform; do
                WORKLOAD="${ycsb_iter^^}"
                ZIPF_THETA=$([ "$skew_iter" = "zipf" ] && echo "0.99" || echo "0")

                echo "  [threads=$sthreads ycsb=$ycsb_iter skew=$skew_iter] sweeping 1..${MAX_CLIENT_THREADS} client threads..."

                PEAK_MRPS=0; PEAK_P50=0; PEAK_P99=0
                AGG_MRPS=0; AVG_P50=0; AVG_P99=0

                for (( t=1; t<=MAX_CLIENT_THREADS; t++ )); do
                    if run_point "$t"; then
                        if awk "BEGIN{exit !($AGG_MRPS > $PEAK_MRPS)}"; then
                            PEAK_MRPS="$AGG_MRPS"
                            PEAK_P50="$AVG_P50"
                            PEAK_P99="$AVG_P99"
                        fi
                        printf "    client_threads=%-3d  %.3f Mrps  p50=%s µs  p99=%s µs\n" \
                            "$t" "$AGG_MRPS" "$AVG_P50" "$AVG_P99"
                    else
                        printf "    client_threads=%-3d  FAILED\n" "$t"
                    fi
                    sleep 1
                done

                echo "  → peak: ${PEAK_MRPS} Mrps  p50=${PEAK_P50} µs  p99=${PEAK_P99} µs"
                echo "${sthreads},${ycsb_iter},${skew_iter},${PEAK_MRPS},${PEAK_P50},${PEAK_P99}" >> "$CSV_OUT"
            done
        done

        NUM_SERVER_THREADS=$NUM_SERVER_THREADS_SAVED
        stop_server
        sleep 1
    done

    echo ""
    echo "Grid sweep complete. Results written to: ${CSV_OUT}"
    echo ""
    cat "$CSV_OUT"
    exit 0
fi

# =============================================================================
# SINGLE-CONFIG MODE (original behaviour)
# =============================================================================
start_server "$NUM_SERVER_THREADS"

# ---- Print header -----------------------------------------------------------
echo ""
echo "Sweeping client threads 1..${MAX_CLIENT_THREADS}  (server_threads=${NUM_SERVER_THREADS}, workload=${WORKLOAD}, skew=${SKEW})"
echo ""
echo "┌─────────────────┬────────────┬────────────┬────────────┐"
printf "│ %-15s │ %-10s │ %-10s │ %-10s │\n" "client threads" "Mrps" "p50 (µs)" "p99 (µs)"
echo "├─────────────────┼────────────┼────────────┼────────────┤"

# ---- Sweep ------------------------------------------------------------------
PREV_MRPS=0
PEAK_MRPS=0
PEAK_THREADS=0

for (( t=1; t<=MAX_CLIENT_THREADS; t++ )); do
    AGG_MRPS=0; AVG_P50=0; AVG_P99=0
    if ! run_point "$t"; then
        printf "│ %-15s │ %-10s │ %-10s │ %-10s │\n" "$t" "FAILED" "-" "-"
        continue
    fi

    # Detect plateau: gain < 5% of previous point (skip check at t=1)
    FLAG=""
    if (( t > 1 )); then
        GAIN=$(awk "BEGIN{printf \"%.4f\", ($AGG_MRPS - $PREV_MRPS) / ($PREV_MRPS + 0.0001)}")
        if awk "BEGIN{exit !($GAIN < 0.05)}"; then
            FLAG=" ●"  # server saturated
        fi
    fi

    printf "│ %-15s │ %-10s │ %-10s │ %-10s │\n" \
        "$t" "${AGG_MRPS}${FLAG}" "$AVG_P50" "$AVG_P99"

    # Track peak
    if awk "BEGIN{exit !($AGG_MRPS > $PEAK_MRPS)}"; then
        PEAK_MRPS="$AGG_MRPS"
        PEAK_THREADS="$t"
    fi

    PREV_MRPS="$AGG_MRPS"

    # Stop once saturated
    if [[ -n "$FLAG" ]]; then
        break
    fi

    # Brief pause between points so the server drains its queues
    sleep 1
done

echo "└─────────────────┴────────────┴────────────┴────────────┘"
echo ""
printf "Peak: %.3f Mrps at %d client thread(s)  ●= throughput gain <5%% (server saturated)\n" \
    "$PEAK_MRPS" "$PEAK_THREADS"
printf "Config: num_keys=%s  ycsb=%s  skew=%s  server_threads=%s  test=%sms\n" \
    "$NUM_KEYS" "$WORKLOAD" "$SKEW" "$NUM_SERVER_THREADS" "$TEST_MS"
