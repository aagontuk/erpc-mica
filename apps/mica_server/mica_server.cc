#include <gflags/gflags.h>
#include <signal.h>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <thread>
#include <vector>

#include "../apps_common.h"
#include "mica_server.h"
#include "rpc.h"
#include "util/autorun_helpers.h"
#include "util/numautils.h"

// ---- gflags ----

DEFINE_uint64(num_server_threads, 1,    "Server threads");
DEFINE_uint64(num_client_threads, 1,    "Client threads per process");
DEFINE_uint64(num_keys,        1000000, "Keys pre-loaded in table");
DEFINE_uint64(target_pps,      1000000, "Total target send rate (pps); 0 = unlimited");
DEFINE_uint64(warmup_ms,          2000, "Warmup duration in ms (stats ignored)");
DEFINE_string(workload,            "B", "YCSB workload: A (50/50) B (95/5) C (100/0)");
DEFINE_double(zipf_theta,         0.99, "Zipf skew; 0 = uniform");

// ---- Globals ----

volatile sig_atomic_t ctrl_c_pressed = 0;
void ctrl_c_handler(int) { ctrl_c_pressed = 1; }

// ---- Zipf sampler (Hormann & Derflinger 1996) ----

struct zipf_params {
    uint64_t n;
    double   theta;
    double   alpha;
    double   zeta_n;
    double   eta;
};

static double zeta_sum(uint64_t n, double theta) {
    double z = 0.0;
    for (uint64_t i = 1; i <= n; i++)
        z += 1.0 / pow(static_cast<double>(i), theta);
    return z;
}

static void zipf_init(zipf_params *p, uint64_t n, double theta) {
    p->n      = n;
    p->theta  = theta;
    p->alpha  = 1.0 / (1.0 - theta);
    p->zeta_n = zeta_sum(n, theta);
    double zeta2 = 1.0 + pow(0.5, theta);
    p->eta = (1.0 - pow(2.0 / static_cast<double>(n), 1.0 - theta))
           / (1.0 - zeta2 / p->zeta_n);
}

// r must be a uniform random in [0, UINT64_MAX]; returns key in [1, n].
static uint64_t zipf_sample(zipf_params *p, uint64_t r) {
    double   u  = static_cast<double>(r) / static_cast<double>(UINT64_MAX);
    double   uz = u * p->zeta_n;
    if (uz < 1.0) return 1;
    if (uz < 1.0 + pow(0.5, p->theta)) return 2;
    uint64_t k = 1 + static_cast<uint64_t>(
        static_cast<double>(p->n) * pow(p->eta * u - p->eta + 1.0, p->alpha));
    return k < 1 ? 1 : (k > p->n ? p->n : k);
}

static zipf_params g_zipf;

// ---- Per-thread result record (written at end of client_func, read by main) ----

struct ThreadResult {
    uint64_t tx_measured = 0;   // sends during measurement window
    uint64_t rx_measured = 0;   // completions during measurement window
    uint64_t gets_ok = 0, gets_miss = 0;
    uint64_t sets_ok = 0, sets_fail = 0, sets_dedup = 0;
    uint64_t unresolved = 0;
    double   measure_s  = 0.0;  // length of measurement window in seconds
    double   freq_ghz   = 0.0;
    uint64_t *rtt_samples = nullptr;  // ownership transferred from ClientContext
    size_t    rtt_count   = 0;
};
static ThreadResult g_results[64];  // indexed by thread_id

// ---- Workload helper ----

static uint8_t workload_from_flag() {
    if (FLAGS_workload == "A") return 0;
    if (FLAGS_workload == "B") return 1;
    return 2;  // "C" or anything else → 100% GET
}

// ============================================================
// Server side
// ============================================================

static constexpr size_t kMaxBatch = 16;
static constexpr size_t kEvLoopMs = 1000;

class ServerContext : public BasicAppContext {
public:
    size_t           thread_id;
    MicaTable       *table = nullptr;
    erpc::HugeAlloc *alloc = nullptr;

    // Per-thread nseq dedup: ring buffer keyed by (nseq & (kDedupSize-1)).
    // Stores last nseq that occupied each slot; duplicate if stored value == nseq.
    static constexpr size_t kDedupSize = 1 << 20;  // must be power of 2
    uint64_t *dedup_ring = nullptr;  // heap-allocated; 8 MB would overflow thread stack

    // Batch state
    size_t           batch_sz = 0;
    erpc::ReqHandle *req_handle_arr[kMaxBatch];
    bool             is_set_arr[kMaxBatch];
    MicaKey          key_arr[kMaxBatch];
    uint64_t         val_arr[kMaxBatch];
    uint64_t         keyhash_arr[kMaxBatch];

    struct {
        size_t gets_ok = 0, gets_miss = 0;
        size_t sets_ok = 0, sets_fail = 0, sets_dedup = 0;
    } stats;
};

static void drain_batch(ServerContext *c) {
    for (size_t i = 0; i < c->batch_sz; i++) {
        erpc::ReqHandle *rh   = c->req_handle_arr[i];
        erpc::MsgBuffer &resp = rh->pre_resp_msgbuf_;
        c->rpc_->resize_msg_buffer(&resp, sizeof(ht_rpc_resp_t));
        auto *r = reinterpret_cast<ht_rpc_resp_t *>(resp.buf_);

        if (c->is_set_arr[i]) {
            MicaResult res = c->table->set(
                c->keyhash_arr[i], c->key_arr[i],
                reinterpret_cast<const char *>(&c->val_arr[i]));
            r->status = (res == MicaResult::kSuccess) ? HT_SUCCESS : HT_ERR_FULL;
            r->value  = 0;
            if (res == MicaResult::kSuccess) c->stats.sets_ok++;
            else                              c->stats.sets_fail++;
        } else {
            MicaResult res = c->table->get(
                c->keyhash_arr[i], c->key_arr[i],
                reinterpret_cast<char *>(&r->value));
            r->status = (res == MicaResult::kSuccess) ? HT_SUCCESS
                                                       : HT_ERR_KEY_NOT_FOUND;
            if (res == MicaResult::kSuccess) c->stats.gets_ok++;
            else                              c->stats.gets_miss++;
        }

        c->rpc_->enqueue_response(rh, &resp);
    }
    c->batch_sz = 0;
}

static void ht_get_handler(erpc::ReqHandle *req_handle, void *_ctx) {
    auto *c = static_cast<ServerContext *>(_ctx);
    const auto *req = reinterpret_cast<const ht_rpc_req_t *>(
        req_handle->get_req_msgbuf()->buf_);

    MicaKey  mk = make_key(req->key);
    uint64_t kh = mica::util::hash(&mk, sizeof(MicaKey));

    const size_t bi       = c->batch_sz;
    c->req_handle_arr[bi] = req_handle;
    c->is_set_arr[bi]     = false;
    c->key_arr[bi]        = mk;
    c->keyhash_arr[bi]    = kh;
    c->table->prefetch_table(kh);

    c->batch_sz++;
    if (c->batch_sz == kMaxBatch) drain_batch(c);
}

static void ht_set_handler(erpc::ReqHandle *req_handle, void *_ctx) {
    auto *c = static_cast<ServerContext *>(_ctx);
    const auto *req = reinterpret_cast<const ht_rpc_req_t *>(
        req_handle->get_req_msgbuf()->buf_);

    // Dedup check
    size_t slot = req->nseq & (ServerContext::kDedupSize - 1);
    if (c->dedup_ring[slot] == req->nseq) {
        erpc::MsgBuffer &resp = req_handle->pre_resp_msgbuf_;
        c->rpc_->resize_msg_buffer(&resp, sizeof(ht_rpc_resp_t));
        auto *r   = reinterpret_cast<ht_rpc_resp_t *>(resp.buf_);
        r->status = HT_ERR_DUPLICATE;
        r->value  = 0;
        c->rpc_->enqueue_response(req_handle, &resp);
        c->stats.sets_dedup++;
        return;
    }
    c->dedup_ring[slot] = req->nseq;

    MicaKey  mk = make_key(req->key);
    uint64_t kh = mica::util::hash(&mk, sizeof(MicaKey));

    const size_t bi       = c->batch_sz;
    c->req_handle_arr[bi] = req_handle;
    c->is_set_arr[bi]     = true;
    c->key_arr[bi]        = mk;
    c->val_arr[bi]        = req->value;
    c->keyhash_arr[bi]    = kh;
    c->table->prefetch_table(kh);

    c->batch_sz++;
    if (c->batch_sz == kMaxBatch) drain_batch(c);
}

static void populate_table(ServerContext &c) {
    const size_t N = static_cast<size_t>(FLAGS_num_keys);
    for (size_t i = 1; i <= N; i++) {
        MicaKey  mk = make_key(i);
        uint64_t kh = mica::util::hash(&mk, sizeof(MicaKey));
        uint64_t v  = i + 1;  // value = key + 1 (matches client)
        MicaResult res = c.table->set(kh, mk, reinterpret_cast<const char *>(&v));
        if (res != MicaResult::kSuccess) {
            printf("thread %zu: populate stopped at key %zu\n", c.thread_id, i);
            break;
        }
    }
}

static void server_func(erpc::Nexus *nexus, size_t tid) {
    ServerContext c;
    c.thread_id = tid;
    c.dedup_ring = new uint64_t[ServerContext::kDedupSize];
    memset(c.dedup_ring, 0xff, ServerContext::kDedupSize * sizeof(uint64_t));

    // MB() is from common.h (via rpc.h); initial_size is in bytes.
    c.alloc = new erpc::HugeAlloc(MB(512), FLAGS_numa_node, nullptr, nullptr);
    auto cfg = mica::util::Config::load_file("apps/mica_server/mica_server.json");
    c.table  = new MicaTable(cfg.get("table"), kValSize, c.alloc);

    populate_table(c);

    std::vector<size_t> ports = flags_get_numa_ports(FLAGS_numa_node);
    erpc::Rpc<erpc::CTransport> rpc(nexus, &c, tid, basic_sm_handler,
                                    ports.at(0));
    c.rpc_ = &rpc;

    while (!ctrl_c_pressed) {
        const size_t before = c.batch_sz;
        rpc.run_event_loop_once();
        if (c.batch_sz == before && c.batch_sz > 0) drain_batch(&c);
    }

    printf("Server thread %zu: gets_ok=%zu miss=%zu | "
           "sets_ok=%zu fail=%zu dedup=%zu\n",
           tid, c.stats.gets_ok, c.stats.gets_miss,
           c.stats.sets_ok, c.stats.sets_fail, c.stats.sets_dedup);

    delete c.table;
    delete c.alloc;
    delete[] c.dedup_ring;
}

// ============================================================
// Client side
// ============================================================

static constexpr uint32_t kMaxPending = 1 << 10;  // 1024; must be power of 2

struct PendingSlot {
    uint64_t        nseq;
    uint64_t        orig_send_tsc;
    bool            valid;
    bool            is_set;
    erpc::MsgBuffer req_buf;
    erpc::MsgBuffer resp_buf;
};

class ClientContext : public BasicAppContext {
public:
    size_t   thread_id;
    uint8_t  ycsb_workload;  // 0=A(50/50), 1=B(95/5), 2=C(100/0)
    bool     use_zipf;
    uint64_t key_mask;       // FLAGS_num_keys - 1 (num_keys must be power of 2)
    uint64_t rng_state;

    // Per-thread sequence counter.  Upper 16 bits = thread_id; lower 48 bits
    // are a monotone counter.  Globally unique with no atomic op on the hot path.
    // nseq = (thread_id << 48) | (thread_nseq++ & 0x0000FFFFFFFFFFFFull)
    uint64_t thread_nseq = 1;

    uint64_t target_pps;
    uint64_t packet_delay_cycle;
    uint64_t deadline;

    PendingSlot pending[kMaxPending];
    uint32_t    in_flight = 0;

    uint64_t warmup_end_tsc;
    uint64_t measure_start_tsc = 0;  // TSC when measurement window opens
    uint64_t measure_end_tsc   = 0;  // TSC when measurement window closes

    static constexpr size_t kMaxSamples = 1 << 20;
    uint64_t *rtt_samples = nullptr;
    size_t    rtt_count   = 0;

    struct {
        uint64_t tx = 0, rx = 0, unique_rx = 0;
        uint64_t tx_measured = 0;  // sends  in measurement window (post-warmup)
        uint64_t rx_measured = 0;  // recvs  in measurement window (post-warmup)
        uint64_t gets_ok = 0, gets_miss = 0;
        uint64_t sets_ok = 0, sets_fail = 0, sets_dedup = 0;
        uint64_t unresolved = 0;
    } stats;
};

static inline uint64_t xorshift64(uint64_t &state) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return state;
}

static inline uint64_t next_key(ClientContext &c) {
    uint64_t r = xorshift64(c.rng_state);
    if (c.use_zipf) return zipf_sample(&g_zipf, r);
    return (r & c.key_mask) + 1;  // keys are 1-based in the table
}

static inline bool next_is_set(ClientContext &c) {
    static const uint8_t read_pct[3] = {50, 95, 100};
    return (xorshift64(c.rng_state) % 100) >= read_pct[c.ycsb_workload];
}

void kv_cont_func(void *_ctx, void *_tag);  // forward declaration

static void kv_send_req(ClientContext &c) {
    // No atomic: thread_id in upper 16 bits guarantees global uniqueness.
    uint64_t nseq = (static_cast<uint64_t>(c.thread_id) << 48)
                  | (c.thread_nseq++ & 0x0000FFFFFFFFFFFFull);
    uint32_t     idx  = static_cast<uint32_t>(nseq & (kMaxPending - 1));
    PendingSlot &slot = c.pending[idx];

    slot.nseq          = nseq;
    slot.orig_send_tsc = erpc::rdtsc();
    slot.valid         = true;
    slot.is_set        = next_is_set(c);

    auto *req  = reinterpret_cast<ht_rpc_req_t *>(slot.req_buf.buf_);
    req->key   = next_key(c);
    req->value = req->key + 1;
    req->nseq  = nseq;
    req->cmd   = slot.is_set ? 1 : 0;

    uint8_t req_type = slot.is_set ? kReqTypeHtSet : kReqTypeHtGet;

    // Encode nseq (not idx) in the tag so kv_cont_func can detect stale slots.
    c.rpc_->enqueue_request(c.fast_get_rand_session_num(), req_type,
                            &slot.req_buf, &slot.resp_buf,
                            kv_cont_func,
                            reinterpret_cast<void *>(static_cast<uintptr_t>(nseq)));
    c.in_flight++;
    c.stats.tx++;
}

void kv_cont_func(void *_ctx, void *_tag) {
    auto    *c    = static_cast<ClientContext *>(_ctx);
    uint64_t nseq = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(_tag));
    uint32_t idx  = static_cast<uint32_t>(nseq & (kMaxPending - 1));
    PendingSlot &slot = c->pending[idx];

    c->stats.rx++;

    if (!slot.valid || slot.nseq != nseq) {
        c->in_flight--;
        return;
    }

    slot.valid = false;
    c->in_flight--;
    c->stats.unique_rx++;

    const auto *resp =
        reinterpret_cast<const ht_rpc_resp_t *>(slot.resp_buf.buf_);

    if (slot.is_set) {
        if      (resp->status == HT_SUCCESS)       c->stats.sets_ok++;
        else if (resp->status == HT_ERR_DUPLICATE) c->stats.sets_dedup++;
        else                                        c->stats.sets_fail++;
    } else {
        if (resp->status == HT_SUCCESS) c->stats.gets_ok++;
        else                            c->stats.gets_miss++;
    }

    uint64_t now = erpc::rdtsc();
    if (now >= c->warmup_end_tsc) {
        c->stats.rx_measured++;
        if (c->rtt_count < ClientContext::kMaxSamples)
            c->rtt_samples[c->rtt_count++] = now - slot.orig_send_tsc;
    }
}

static void create_sessions(ClientContext &c) {
    std::string server_uri = erpc::get_uri_for_process(0);
    for (size_t i = 0; i < FLAGS_num_server_threads; i++) {
        int sn = c.rpc_->create_session(server_uri, i);
        erpc::rt_assert(sn >= 0, "create_session failed");
        c.session_num_vec_.push_back(sn);
    }
    while (c.num_sm_resps_ != FLAGS_num_server_threads)
        c.rpc_->run_event_loop(kEvLoopMs);
}

static void report_stats(ClientContext &c, double measure_s) {
    double freq_ghz = c.rpc_->get_freq_ghz();
    double tx_mpps  = (measure_s > 0) ? c.stats.tx_measured / measure_s / 1e6 : 0.0;
    double rx_mpps  = (measure_s > 0) ? c.stats.rx_measured / measure_s / 1e6 : 0.0;

    printf("Thread %zu: sent=%lu (%.2f Mpps)  rx=%lu (%.2f Mpps)  unresolved=%lu\n",
           c.thread_id, c.stats.tx_measured, tx_mpps,
           c.stats.rx_measured, rx_mpps, c.stats.unresolved);

    if (c.rtt_count == 0) {
        printf("Thread %zu: no latency samples (warmup too long?)\n", c.thread_id);
    } else {
        std::sort(c.rtt_samples, c.rtt_samples + c.rtt_count);
        double mean_us = 0;
        for (size_t i = 0; i < c.rtt_count; i++)
            mean_us += erpc::to_usec(c.rtt_samples[i], freq_ghz);
        mean_us /= static_cast<double>(c.rtt_count);
        double p50_us = erpc::to_usec(c.rtt_samples[c.rtt_count * 50 / 100], freq_ghz);
        double p99_us = erpc::to_usec(c.rtt_samples[c.rtt_count * 99 / 100], freq_ghz);
        printf("Thread %zu: mean=%.2f us  p50=%.2f us  p99=%.2f us\n",
               c.thread_id, mean_us, p50_us, p99_us);
    }

    printf("Thread %zu: GET ok=%lu miss=%lu | SET ok=%lu fail=%lu dedup=%lu\n",
           c.thread_id, c.stats.gets_ok, c.stats.gets_miss,
           c.stats.sets_ok, c.stats.sets_fail, c.stats.sets_dedup);
}

static void print_aggregate(size_t nthreads) {
    uint64_t tx_measured = 0, rx_measured = 0;
    uint64_t gets_ok = 0, gets_miss = 0;
    uint64_t sets_ok = 0, sets_fail = 0, sets_dedup = 0;
    uint64_t unresolved = 0;
    double   measure_s = 0.0;
    double   freq_ghz  = 0.0;
    size_t   total_rtt = 0;

    for (size_t i = 0; i < nthreads; i++) {
        tx_measured += g_results[i].tx_measured;
        rx_measured += g_results[i].rx_measured;
        gets_ok     += g_results[i].gets_ok;
        gets_miss   += g_results[i].gets_miss;
        sets_ok     += g_results[i].sets_ok;
        sets_fail   += g_results[i].sets_fail;
        sets_dedup  += g_results[i].sets_dedup;
        unresolved  += g_results[i].unresolved;
        if (g_results[i].measure_s > measure_s) measure_s = g_results[i].measure_s;
        if (g_results[i].freq_ghz  > 0)         freq_ghz  = g_results[i].freq_ghz;
        total_rtt += g_results[i].rtt_count;
    }

    double tx_mpps = (measure_s > 0) ? tx_measured / measure_s / 1e6 : 0.0;
    double rx_mpps = (measure_s > 0) ? rx_measured / measure_s / 1e6 : 0.0;

    printf("\n=== Aggregate (%zu threads) ===\n", nthreads);
    printf("Sent: %lu (%.2f Mpps)  Received: %lu (%.2f Mpps)  Unresolved: %lu\n",
           tx_measured, tx_mpps, rx_measured, rx_mpps, unresolved);

    if (total_rtt > 0 && freq_ghz > 0) {
        uint64_t *merged = new uint64_t[total_rtt];
        size_t off = 0;
        for (size_t i = 0; i < nthreads; i++) {
            memcpy(merged + off, g_results[i].rtt_samples,
                   g_results[i].rtt_count * sizeof(uint64_t));
            off += g_results[i].rtt_count;
        }
        std::sort(merged, merged + total_rtt);

        double mean_us = 0;
        for (size_t i = 0; i < total_rtt; i++)
            mean_us += erpc::to_usec(merged[i], freq_ghz);
        mean_us /= static_cast<double>(total_rtt);
        double p50_us = erpc::to_usec(merged[total_rtt * 50 / 100], freq_ghz);
        double p99_us = erpc::to_usec(merged[total_rtt * 99 / 100], freq_ghz);
        printf("mean=%.2f us  p50=%.2f us  p99=%.2f us  (%zu samples)\n",
               mean_us, p50_us, p99_us, total_rtt);
        delete[] merged;
    }

    printf("GET ok=%lu miss=%lu | SET ok=%lu fail=%lu dedup=%lu\n",
           gets_ok, gets_miss, sets_ok, sets_fail, sets_dedup);

    for (size_t i = 0; i < nthreads; i++) {
        delete[] g_results[i].rtt_samples;
        g_results[i].rtt_samples = nullptr;
    }
}

static void client_func(erpc::Nexus *nexus, size_t tid) {
    ClientContext c;
    c.thread_id     = tid;
    c.rng_state     = 0xdeadbeef ^ (tid * 1000003ULL);
    c.use_zipf      = FLAGS_zipf_theta > 0.0;
    c.key_mask      = FLAGS_num_keys - 1;
    c.ycsb_workload = workload_from_flag();
    c.target_pps    = FLAGS_target_pps / FLAGS_num_client_threads;

    if (c.use_zipf && tid == 0)
        zipf_init(&g_zipf, FLAGS_num_keys, FLAGS_zipf_theta);

    c.rtt_samples = new uint64_t[ClientContext::kMaxSamples];

    std::vector<size_t> ports = flags_get_numa_ports(FLAGS_numa_node);
    erpc::Rpc<erpc::CTransport> rpc(nexus, &c, tid, basic_sm_handler,
                                    ports.at(0));
    rpc.retry_connect_on_invalid_rpc_id_ = true;
    c.rpc_ = &rpc;

    for (uint32_t i = 0; i < kMaxPending; i++) {
        c.pending[i].req_buf  = rpc.alloc_msg_buffer_or_die(sizeof(ht_rpc_req_t));
        c.pending[i].resp_buf = rpc.alloc_msg_buffer_or_die(sizeof(ht_rpc_resp_t));
        c.pending[i].valid    = false;
    }

    create_sessions(c);

    printf("Process %zu thread %zu: connected, starting.\n",
           FLAGS_process_id, tid);

    const double freq_ghz = rpc.get_freq_ghz();
    // cycles/packet = freq_hz / pps = (freq_ghz * 1e9) / pps
    c.packet_delay_cycle = (c.target_pps > 0)
        ? static_cast<uint64_t>(freq_ghz * 1e9 / static_cast<double>(c.target_pps))
        : 0;

    uint64_t start_tsc = erpc::rdtsc();
    c.warmup_end_tsc   = start_tsc + erpc::ms_to_cycles(FLAGS_warmup_ms, freq_ghz);
    uint64_t end_tsc   = (FLAGS_test_ms > 0)
        ? start_tsc + erpc::ms_to_cycles(FLAGS_test_ms, freq_ghz)
        : UINT64_MAX;

    c.deadline = start_tsc;
    bool measuring = false;

    while (!ctrl_c_pressed && erpc::rdtsc() < end_tsc) {
        uint64_t now = erpc::rdtsc();

        if (!measuring && now >= c.warmup_end_tsc) {
            measuring = true;
            c.measure_start_tsc = now;
        }

        if ((c.packet_delay_cycle == 0 || now >= c.deadline)
                && c.in_flight < kMaxPending) {
            kv_send_req(c);
            c.deadline += c.packet_delay_cycle;
            if (measuring) c.stats.tx_measured++;
        }

        rpc.run_event_loop_once();
    }
    c.measure_end_tsc = erpc::rdtsc();

    for (uint32_t i = 0; i < kMaxPending; i++) {
        if (c.pending[i].valid) {
            c.pending[i].valid = false;
            c.stats.unresolved++;
        }
    }

    double measure_s = (c.measure_start_tsc > 0)
        ? static_cast<double>(c.measure_end_tsc - c.measure_start_tsc)
          / (freq_ghz * 1e9)
        : 0.0;

    report_stats(c, measure_s);

    // Transfer ownership of RTT samples to g_results for aggregate summary.
    ThreadResult &res  = g_results[tid];
    res.tx_measured    = c.stats.tx_measured;
    res.rx_measured    = c.stats.rx_measured;
    res.gets_ok        = c.stats.gets_ok;
    res.gets_miss      = c.stats.gets_miss;
    res.sets_ok        = c.stats.sets_ok;
    res.sets_fail      = c.stats.sets_fail;
    res.sets_dedup     = c.stats.sets_dedup;
    res.unresolved     = c.stats.unresolved;
    res.measure_s      = measure_s;
    res.freq_ghz       = freq_ghz;
    res.rtt_samples    = c.rtt_samples;   // main() will free via print_aggregate
    res.rtt_count      = c.rtt_count;
    c.rtt_samples      = nullptr;         // prevent double-free
}

// ============================================================
// main
// ============================================================

int main(int argc, char **argv) {
    signal(SIGINT, ctrl_c_handler);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    fprintf(stderr, "[debug] flags parsed, process_id=%zu\n", FLAGS_process_id);

    std::string uri = erpc::get_uri_for_process(FLAGS_process_id);
    fprintf(stderr, "[debug] uri=%s\n", uri.c_str());

    erpc::Nexus nexus(uri, FLAGS_numa_node, 0);
    fprintf(stderr, "[debug] nexus created\n");
    nexus.register_req_func(kReqTypeHtGet, ht_get_handler);
    nexus.register_req_func(kReqTypeHtSet, ht_set_handler);

    size_t nthreads = (FLAGS_process_id == 0) ? FLAGS_num_server_threads
                                              : FLAGS_num_client_threads;

    std::vector<std::thread> threads(nthreads);
    for (size_t i = 0; i < nthreads; i++) {
        threads[i] = std::thread(
            (FLAGS_process_id == 0) ? server_func : client_func, &nexus, i);
        erpc::bind_to_core(threads[i], FLAGS_numa_node, i);
    }
    for (auto &t : threads) t.join();

    if (FLAGS_process_id != 0 && nthreads > 1)
        print_aggregate(nthreads);

    return 0;
}
