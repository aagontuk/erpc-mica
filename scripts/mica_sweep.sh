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
#   --num-keys N             Key space size (default: 64000000)
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
#   --hugepages N            2MB hugepages to allocate on NUMA_NODE before
#                            starting (default: 3072 = 6 GB)
#
# Fixed parameters (edit variables below to change):
#   CLIENT_NODE         SSH target for client process   (default: node-1)
#   NUMA_NODE           NUMA node for both sides         (default: 1)
#   SERVER_NUMA_PORTS   NIC port ID(s) on the server     (default: 2)
#   CLIENT_NUMA_PORTS   NIC port ID(s) on the client     (default: 2)

set -euo pipefail

# ---- Locate repo root -------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$REPO_ROOT/build/mica_server"

# ---- Fixed parameters (edit here) ------------------------------------------
CLIENT_NODE="node1"
NUMA_NODE=0
SERVER_NUMA_PORTS=0
CLIENT_NUMA_PORTS=1

# ---- Defaults for user options ----------------------------------------------
NUM_SERVER_THREADS=8
MAX_CLIENT_THREADS=16
NUM_KEYS=1048576
YCSB="b"
SKEW="zipf"
TEST_MS=10000
GRID=0
CSV_OUT=""
HUGEPAGES=4096

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
        --hugepages)          HUGEPAGES="$2";          shift 2 ;;
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
        --numa_node "$NUMA_NODE" "--numa_${NUMA_NODE}_ports" "$SERVER_NUMA_PORTS" \
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
        # Use sudo kill -0 so the check works when the server runs as root.
        if ! sudo kill -0 "$SERVER_PID" 2>/dev/null; then
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
    if [[ -n "$SERVER_PID" ]]; then
        disown "$SERVER_PID" 2>/dev/null || true
        # Send SIGTERM first so DPDK/mlx5 can close the NIC cleanly.
        sudo kill -TERM "$SERVER_PID" 2>/dev/null || true
        for (( _ti=0; _ti<50; _ti++ )); do
            sudo kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 0.1
        done
        # Fall back to SIGKILL if still alive after 5 s.
        sudo kill -9 "$SERVER_PID" 2>/dev/null || true
    fi
    sudo pkill -9 mica_server 2>/dev/null || true
    # Wait up to 10 s for processes to fully exit (SIGKILL delivery is async)
    for (( _si=0; _si<100; _si++ )); do
        sudo pgrep mica_server >/dev/null 2>&1 || break
        sleep 0.1
    done
    sudo rm -rf /run/dpdk/rte/
    sudo rm -f /dev/hugepages/rtemap_*
    # Wait for the Nexus SM UDP port (31850) to be released before returning,
    # so the next start_server doesn't get "bind: Address already in use".
    for (( _ui=0; _ui<60; _ui++ )); do
        ss -uln 2>/dev/null | grep -qF ':31850 ' || break
        sleep 0.5
    done
    # Wait up to 30 s for huge pages to return to the pool.  DPDK uses ~768 2MB
    # pages per run (-m 1024 + HugeAlloc 512 MB); with only 3072 total, the 5th
    # start starves if prior pages are not reclaimed before we proceed.
    local _hp_total _hp_need
    _hp_total=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 3072)
    _hp_need=$(( _hp_total / 4 ))   # need at least 25% free for next run
    for (( _hi=0; _hi<300; _hi++ )); do
        local _hp_free
        _hp_free=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages 2>/dev/null || echo "$_hp_total")
        (( _hp_free >= _hp_need )) && break
        sleep 0.1
    done
    [[ -n "$SERVER_LOG" ]] && rm -f "$SERVER_LOG" || true
    SERVER_PID=""
    SERVER_LOG=""
}

# ---- Client-side DPDK cleanup (run between client invocations) ---------------
cleanup_client() {
    ssh "$CLIENT_NODE" \
        "sudo pkill -9 mica_server 2>/dev/null; \
         sudo rm -rf /run/dpdk/rte/ 2>/dev/null; \
         sudo rm -f /dev/hugepages/rtemap_* 2>/dev/null; \
         true" 2>/dev/null || true
}

# ---- Cleanup ----------------------------------------------------------------
cleanup() {
    stop_server
    cleanup_client
}
trap cleanup EXIT INT TERM

# ---- Hugepage setup ---------------------------------------------------------
echo "Allocating $HUGEPAGES x 2MB hugepages on NUMA node $NUMA_NODE (server: local)..."
sudo sh -c "echo $HUGEPAGES > /sys/devices/system/node/node${NUMA_NODE}/hugepages/hugepages-2048kB/nr_hugepages"
actual=$(cat /sys/devices/system/node/node${NUMA_NODE}/hugepages/hugepages-2048kB/nr_hugepages)
(( actual >= HUGEPAGES )) || { echo "Error: only $actual hugepages allocated (need $HUGEPAGES)" >&2; exit 1; }

echo "Allocating $HUGEPAGES x 2MB hugepages on NUMA node $NUMA_NODE (client: $CLIENT_NODE)..."
client_actual=$(ssh "$CLIENT_NODE" \
    "sudo sh -c 'echo $HUGEPAGES > /sys/devices/system/node/node${NUMA_NODE}/hugepages/hugepages-2048kB/nr_hugepages'; \
     cat /sys/devices/system/node/node${NUMA_NODE}/hugepages/hugepages-2048kB/nr_hugepages")
(( client_actual >= HUGEPAGES )) || { echo "Error: only $client_actual hugepages allocated on $CLIENT_NODE (need $HUGEPAGES)" >&2; exit 1; }

echo "Cleaning up stale processes..."
cleanup
sleep 1

# ---- Run one client point ---------------------------------------------------
# Outputs: AGG_MRPS, WALL_MRPS, AVG_LAT, AVG_P50, AVG_P99, LAT_SAMPLES,
#          TOTAL_RX, TOTAL_GET_OK, TOTAL_GET_MISS, TOTAL_SET_OK, TOTAL_SET_FAIL
#          (sets variables in caller's scope)
# Uses: WORKLOAD, ZIPF_THETA, NUM_SERVER_THREADS, NUM_KEYS, TEST_MS (globals)
extract_field() {
    local line=$1 pattern=$2 default=${3:-0} out
    out=$(grep -oP "$pattern" <<< "$line" | head -n 1 || true)
    echo "${out:-$default}"
}

reset_point_results() {
    AGG_MRPS=0
    WALL_MRPS=0
    AVG_LAT=0
    AVG_P50=0
    AVG_P99=0
    LAT_SAMPLES=0
    TOTAL_RX=0
    TOTAL_GET_OK=0
    TOTAL_GET_MISS=0
    TOTAL_SET_OK=0
    TOTAL_SET_FAIL=0
}

run_point() {
    local nthreads=$1
    local raw

    reset_point_results

    raw=$(ssh "$CLIENT_NODE" \
        "cd '$REPO_ROOT' && sudo '$BINARY' \
            --process_id 1 --num_processes 2 \
            --num_server_threads $NUM_SERVER_THREADS \
            --num_client_threads $nthreads \
            --num_keys $NUM_KEYS \
            --test_ms $TEST_MS \
            --workload $WORKLOAD --zipf_theta $ZIPF_THETA \
            --numa_node $NUMA_NODE --numa_${NUMA_NODE}_ports $CLIENT_NUMA_PORTS \
            2>/dev/null") || true

    local agg_line lat_line totals_line
    agg_line=$(printf "%s\n" "$raw" | grep "^Client aggregate throughput:" | tail -n 1 || true)
    lat_line=$(printf "%s\n" "$raw" | grep "^Client aggregate latency:" | tail -n 1 || true)
    totals_line=$(printf "%s\n" "$raw" | grep "^Client aggregate totals:" | tail -n 1 || true)

    if [[ -n "$agg_line" ]]; then
        AGG_MRPS=$(extract_field "$agg_line" '^Client aggregate throughput: \K[\d.]+')
        WALL_MRPS=$(extract_field "$agg_line" ', \K[\d.]+(?= Mrps over)')

        if [[ -n "$lat_line" ]]; then
            AVG_LAT=$(extract_field "$lat_line" 'avg=\K[\d.]+')
            AVG_P50=$(extract_field "$lat_line" 'p50=\K[\d.]+')
            AVG_P99=$(extract_field "$lat_line" 'p99=\K[\d.]+')
            LAT_SAMPLES=$(extract_field "$lat_line" 'samples=\K[0-9]+')
        fi

        if [[ -n "$totals_line" ]]; then
            TOTAL_RX=$(extract_field "$totals_line" 'rx=\K[0-9]+')
            TOTAL_GET_OK=$(extract_field "$totals_line" 'GET ok=\K[0-9]+')
            TOTAL_GET_MISS=$(extract_field "$totals_line" 'GET ok=[0-9]+ miss=\K[0-9]+')
            TOTAL_SET_OK=$(extract_field "$totals_line" 'SET ok=\K[0-9]+')
            TOTAL_SET_FAIL=$(extract_field "$totals_line" 'SET ok=[0-9]+ fail=\K[0-9]+')
        fi

        return 0
    fi

    local stat_lines
    stat_lines=$(printf "%s\n" "$raw" | grep "^Thread " | tail -n "$nthreads")

    if [[ -z "$stat_lines" ]]; then
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
    WALL_MRPS="$sum_mrps"
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

    # Write CSV header. Each row is the peak client-thread point for one
    # server-thread/workload/skew configuration.
    echo "server_threads,ycsb,skew,peak_client_threads,throughput_mrps_sum,throughput_mrps_wall,avg_us,p50_us,p99_us,latency_samples,total_rx,get_ok,get_miss,set_ok,set_fail" > "$CSV_OUT"

    for (( sthreads=1; sthreads<=NUM_SERVER_THREADS; sthreads++ )); do
        NUM_SERVER_THREADS_SAVED=$NUM_SERVER_THREADS
        NUM_SERVER_THREADS=$sthreads   # run_point reads this global

        for ycsb_iter in a b c; do
            for skew_iter in zipf uniform; do
                WORKLOAD="${ycsb_iter^^}"
                ZIPF_THETA=$([ "$skew_iter" = "zipf" ] && echo "0.99" || echo "0")

                echo "  [threads=$sthreads ycsb=$ycsb_iter skew=$skew_iter] sweeping 1..${MAX_CLIENT_THREADS} client threads..."

                PEAK_THREADS=0
                PEAK_MRPS=0; PEAK_WALL_MRPS=0
                PEAK_AVG_LAT=0; PEAK_P50=0; PEAK_P99=0; PEAK_LAT_SAMPLES=0
                PEAK_TOTAL_RX=0; PEAK_GET_OK=0; PEAK_GET_MISS=0
                PEAK_SET_OK=0; PEAK_SET_FAIL=0
                reset_point_results

                # Start the server once for this (sthreads, ycsb, skew) combo.
                # The client properly disconnects sessions after each run, so
                # the same server instance handles all client-thread counts.
                # This avoids repeated DPDK NIC teardown/reinit which leaves
                # stale state under forced kill.
                start_server "$sthreads"

                for (( t=1; t<=MAX_CLIENT_THREADS; t++ )); do
                    # Clean up client-side DPDK state from the previous run
                    # so the next client starts as a fresh DPDK primary.
                    if (( t > 1 )); then
                        cleanup_client
                        sleep 1
                    fi
                    if run_point "$t"; then
                        if awk "BEGIN{exit !($AGG_MRPS > $PEAK_MRPS)}"; then
                            PEAK_THREADS="$t"
                            PEAK_MRPS="$AGG_MRPS"
                            PEAK_WALL_MRPS="$WALL_MRPS"
                            PEAK_AVG_LAT="$AVG_LAT"
                            PEAK_P50="$AVG_P50"
                            PEAK_P99="$AVG_P99"
                            PEAK_LAT_SAMPLES="$LAT_SAMPLES"
                            PEAK_TOTAL_RX="$TOTAL_RX"
                            PEAK_GET_OK="$TOTAL_GET_OK"
                            PEAK_GET_MISS="$TOTAL_GET_MISS"
                            PEAK_SET_OK="$TOTAL_SET_OK"
                            PEAK_SET_FAIL="$TOTAL_SET_FAIL"
                        fi
                        printf "    client_threads=%-3d  %.3f Mrps  p50=%s us  p99=%s us  rx=%s\n" \
                            "$t" "$AGG_MRPS" "$AVG_P50" "$AVG_P99" "$TOTAL_RX"
                    else
                        printf "    client_threads=%-3d  FAILED\n" "$t"
                    fi
                done

                stop_server

                echo "  -> peak: ${PEAK_MRPS} Mrps at ${PEAK_THREADS} client threads  p50=${PEAK_P50} us  p99=${PEAK_P99} us"
                echo "${sthreads},${ycsb_iter},${skew_iter},${PEAK_THREADS},${PEAK_MRPS},${PEAK_WALL_MRPS},${PEAK_AVG_LAT},${PEAK_P50},${PEAK_P99},${PEAK_LAT_SAMPLES},${PEAK_TOTAL_RX},${PEAK_GET_OK},${PEAK_GET_MISS},${PEAK_SET_OK},${PEAK_SET_FAIL}" >> "$CSV_OUT"
            done
        done

        NUM_SERVER_THREADS=$NUM_SERVER_THREADS_SAVED
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
    start_server "$NUM_SERVER_THREADS"
    if ! run_point "$t"; then
        stop_server
        printf "│ %-15s │ %-10s │ %-10s │ %-10s │\n" "$t" "FAILED" "-" "-"
        continue
    fi
    stop_server

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

    sleep 1
done

echo "└─────────────────┴────────────┴────────────┴────────────┘"
echo ""
printf "Peak: %.3f Mrps at %d client thread(s)  ●= throughput gain <5%% (server saturated)\n" \
    "$PEAK_MRPS" "$PEAK_THREADS"
printf "Config: num_keys=%s  ycsb=%s  skew=%s  server_threads=%s  test=%sms\n" \
    "$NUM_KEYS" "$WORKLOAD" "$SKEW" "$NUM_SERVER_THREADS" "$TEST_MS"
