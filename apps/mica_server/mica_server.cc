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
#include "util/latency.h"
#include "util/numautils.h"
#include "util/timer.h"

// ---- gflags ----

DEFINE_uint64(num_server_threads, 1,    "Server threads");
DEFINE_uint64(num_client_threads, 1,    "Client threads per process");
DEFINE_uint64(num_keys,        1048576, "Keys pre-loaded in table; must be a power of 2");
DEFINE_string(workload,            "B", "YCSB workload: A (50/50) B (95/5) C (100/0)");
DEFINE_double(zipf_theta,         0.99, "Zipf skew; 0 = uniform");

// ---- Latency measurement (same knobs as small_rpc_tput) ----

static constexpr bool   kAppMeasureLatency = true;
static constexpr double kAppLatFac         = 3.0;

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

    size_t           batch_sz = 0;
    uint64_t         batch_first_tsc = 0;
    erpc::ReqHandle *req_handle_arr[kMaxBatch];
    bool             is_set_arr[kMaxBatch];
    MicaKey          key_arr[kMaxBatch];
    uint64_t         val_arr[kMaxBatch];
    uint64_t         keyhash_arr[kMaxBatch];

    struct {
        size_t gets_ok = 0, gets_miss = 0;
        size_t sets_ok = 0, sets_fail = 0;
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
    if (bi == 0) c->batch_first_tsc = erpc::rdtsc();
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

    MicaKey  mk = make_key(req->key);
    uint64_t kh = mica::util::hash(&mk, sizeof(MicaKey));

    const size_t bi       = c->batch_sz;
    if (bi == 0) c->batch_first_tsc = erpc::rdtsc();
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
        uint64_t v  = i + 1;
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

    c.alloc = new erpc::HugeAlloc(MB(512), FLAGS_numa_node, nullptr, nullptr);
    auto cfg = mica::util::Config::load_file("apps/mica_server/mica_server.json");
    c.table  = new MicaTable(cfg.get("table"), kValSize, c.alloc);

    populate_table(c);

    std::vector<size_t> ports = flags_get_numa_ports(FLAGS_numa_node);
    erpc::Rpc<erpc::CTransport> rpc(nexus, &c, tid, basic_sm_handler,
                                    ports.at(0));
    c.rpc_ = &rpc;

    const double freq_ghz = rpc.get_freq_ghz();
    const uint64_t kBatchMaxCycles =
        static_cast<uint64_t>(5.0 * freq_ghz * 1000.0);  // 5 µs

    while (!ctrl_c_pressed) {
        rpc.run_event_loop_once();
        if (c.batch_sz > 0 &&
            erpc::rdtsc() - c.batch_first_tsc >= kBatchMaxCycles)
            drain_batch(&c);
    }

    printf("Server thread %zu: gets_ok=%zu miss=%zu | sets_ok=%zu fail=%zu\n",
           tid, c.stats.gets_ok, c.stats.gets_miss,
           c.stats.sets_ok, c.stats.sets_fail);

    delete c.table;
    delete c.alloc;
}

// ============================================================
// Client side
// ============================================================

// Each slot maps directly to one in-flight RPC; slot index is the eRPC tag.
// Stale completions cannot occur: eRPC at-most-once guarantees one callback
// per enqueue_request call, and we only reissue a slot from its own callback.
//
// 16 in-flight slots per server thread: keeps each server thread's pipeline
// full regardless of how many server threads are in use.
static constexpr size_t kPendingPerServerThread = 16;

struct PendingSlot {
    bool            is_set;
    size_t          req_tsc;  // send timestamp for latency (when kAppMeasureLatency)
    erpc::MsgBuffer req_buf;
    erpc::MsgBuffer resp_buf;
};

class ClientContext : public BasicAppContext {
public:
    size_t   thread_id;
    uint8_t  ycsb_workload;
    bool     use_zipf;
    uint64_t key_mask;
    uint64_t rng_state;
    bool     draining = false;  // set true to stop reissuing and let slots drain

    erpc::Latency    latency;
    erpc::ChronoTimer tput_timer;

    std::vector<PendingSlot> pending;  // sized to kPendingPerServerThread * num_server_threads

    struct {
        size_t rx_tot  = 0;
        size_t gets_ok = 0, gets_miss = 0;
        size_t sets_ok = 0, sets_fail = 0;
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

static uint8_t workload_from_flag() {
    if (FLAGS_workload == "A") return 0;
    if (FLAGS_workload == "B") return 1;
    return 2;  // "C" or anything else → 100% GET
}

void kv_cont_func(void *_ctx, void *_tag);  // forward declaration

static void kv_send_req(ClientContext &c, size_t slot_idx) {
    PendingSlot &slot = c.pending[slot_idx];
    slot.is_set = next_is_set(c);

    auto *req  = reinterpret_cast<ht_rpc_req_t *>(slot.req_buf.buf_);
    req->key   = next_key(c);
    req->value = req->key + 1;
    req->cmd   = slot.is_set ? 1 : 0;

    if (kAppMeasureLatency) slot.req_tsc = erpc::rdtsc();

    uint8_t req_type = slot.is_set ? kReqTypeHtSet : kReqTypeHtGet;
    c.rpc_->enqueue_request(c.fast_get_rand_session_num(), req_type,
                            &slot.req_buf, &slot.resp_buf,
                            kv_cont_func,
                            reinterpret_cast<void *>(slot_idx));
}

void kv_cont_func(void *_ctx, void *_tag) {
    auto   *c        = static_cast<ClientContext *>(_ctx);
    size_t  slot_idx = reinterpret_cast<size_t>(_tag);
    PendingSlot &slot = c->pending[slot_idx];

    if (kAppMeasureLatency) {
        double lat_us = erpc::to_usec(erpc::rdtsc() - slot.req_tsc,
                                      c->rpc_->get_freq_ghz());
        c->latency.update(static_cast<size_t>(lat_us * kAppLatFac));
    }

    const auto *resp =
        reinterpret_cast<const ht_rpc_resp_t *>(slot.resp_buf.buf_);

    if (slot.is_set) {
        if (resp->status == HT_SUCCESS) c->stats.sets_ok++;
        else                            c->stats.sets_fail++;
    } else {
        if (resp->status == HT_SUCCESS) c->stats.gets_ok++;
        else                            c->stats.gets_miss++;
    }

    c->stats.rx_tot++;

    if (!c->draining)
        kv_send_req(*c, slot_idx);  // reissue immediately — same pattern as small_rpc_tput
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

static void print_stats(ClientContext &c) {
    double seconds   = c.tput_timer.get_sec();
    double tput_mrps = c.stats.rx_tot / (seconds * 1e6);

    printf("Thread %zu: %.3f Mrps, re_tx=%zu. "
           "GET ok=%zu miss=%zu | SET ok=%zu fail=%zu. "
           "Lat: p50=%.2f us p99=%.2f us\n",
           c.thread_id, tput_mrps,
           c.rpc_->pkt_loss_stats_.num_re_tx_,
           c.stats.gets_ok, c.stats.gets_miss,
           c.stats.sets_ok, c.stats.sets_fail,
           kAppMeasureLatency ? c.latency.perc(0.50) / kAppLatFac : 0.0,
           kAppMeasureLatency ? c.latency.perc(0.99) / kAppLatFac : 0.0);

    c.stats = {};
    c.rpc_->pkt_loss_stats_.num_re_tx_ = 0;
    c.latency.reset();
    c.tput_timer.reset();
}

static void client_func(erpc::Nexus *nexus, size_t tid) {
    ClientContext c;
    c.thread_id     = tid;
    c.rng_state     = 0xdeadbeef ^ (tid * 1000003ULL);
    c.use_zipf      = FLAGS_zipf_theta > 0.0;
    c.key_mask      = FLAGS_num_keys - 1;
    c.ycsb_workload = workload_from_flag();

    erpc::rt_assert((FLAGS_num_keys & (FLAGS_num_keys - 1)) == 0,
                    "--num_keys must be a power of 2 for uniform key distribution");

    std::vector<size_t> ports = flags_get_numa_ports(FLAGS_numa_node);
    erpc::Rpc<erpc::CTransport> rpc(nexus, &c, tid, basic_sm_handler,
                                    ports.at(0));
    rpc.retry_connect_on_invalid_rpc_id_ = true;
    c.rpc_ = &rpc;

    const size_t max_pending = kPendingPerServerThread * FLAGS_num_server_threads;
    c.pending.resize(max_pending);
    for (size_t i = 0; i < max_pending; i++) {
        c.pending[i].req_buf  = rpc.alloc_msg_buffer_or_die(sizeof(ht_rpc_req_t));
        c.pending[i].resp_buf = rpc.alloc_msg_buffer_or_die(sizeof(ht_rpc_resp_t));
    }

    create_sessions(c);

    printf("Process %zu thread %zu: connected, starting (%zu slots across %zu server threads).\n",
           FLAGS_process_id, tid, max_pending, FLAGS_num_server_threads);

    // Fill all slots upfront — same pattern as small_rpc_tput's initial send_reqs loop.
    c.tput_timer.reset();
    for (size_t i = 0; i < max_pending; i++) kv_send_req(c, i);

    for (size_t i = 0; i < FLAGS_test_ms; i += kEvLoopMs) {
        rpc.run_event_loop(kEvLoopMs);
        if (ctrl_c_pressed == 1) break;
        print_stats(c);
    }

    // Stop reissuing and drain all in-flight RPCs so sessions become idle.
    c.draining = true;
    rpc.run_event_loop(kEvLoopMs * 2);

    // Disconnect every session so the server reclaims its ring entries.
    for (int sn : c.session_num_vec_) rpc.destroy_session(sn);
    const size_t expected_sm_resps = 2 * FLAGS_num_server_threads;  // connect + disconnect
    for (size_t ms = 0; ms < 5000 && c.num_sm_resps_ < expected_sm_resps; ms += kEvLoopMs)
        rpc.run_event_loop(kEvLoopMs);
}

// ============================================================
// main
// ============================================================

int main(int argc, char **argv) {
    signal(SIGINT,  ctrl_c_handler);
    signal(SIGTERM, ctrl_c_handler);
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

    if (FLAGS_process_id != 0 && FLAGS_zipf_theta > 0.0)
        zipf_init(&g_zipf, FLAGS_num_keys, FLAGS_zipf_theta);

    std::vector<std::thread> threads(nthreads);
    for (size_t i = 0; i < nthreads; i++) {
        threads[i] = std::thread(
            (FLAGS_process_id == 0) ? server_func : client_func, &nexus, i);
        erpc::bind_to_core(threads[i], FLAGS_numa_node, i);
    }
    for (auto &t : threads) t.join();

    return 0;
}
