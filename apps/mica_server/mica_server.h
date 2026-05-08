#pragma once

#include <stdint.h>
#include "mica/table/fixedtable.h"
#include "mica/util/hash.h"

// ---- Wire types ----

struct ht_rpc_req_t {
    uint8_t  cmd;    // 0=GET, 1=SET
    uint64_t key;
    uint64_t value;  // only meaningful for SET
    uint64_t nseq;   // client sequence number for dedup
} __attribute__((packed));

struct ht_rpc_resp_t {
    uint8_t  status;
    uint64_t value;  // populated for successful GET
} __attribute__((packed));

// ---- RPC type IDs ----

static constexpr uint8_t kReqTypeHtGet = 1;
static constexpr uint8_t kReqTypeHtSet = 2;

// ---- Response status codes ----

// mica::table::Result is not used on the wire; it is mapped here at the server.
enum HtStatus : uint8_t {
    HT_SUCCESS           = 0,
    HT_ERR_KEY_NOT_FOUND = 1,
    HT_ERR_FULL          = 2,
    HT_ERR_DUPLICATE     = 3,
};

// ---- MICA table configuration ----

struct MicaServerTableConfig {
    static constexpr size_t kBucketCap    = 7;
    static constexpr bool   kConcurrent   = false;
    static constexpr bool   kVerbose      = false;
    static constexpr bool   kCollectStats = true;
    static constexpr size_t kKeySize      = 8;  // uint64_t key
};

using MicaTable  = mica::table::FixedTable<MicaServerTableConfig>;
using MicaKey    = MicaTable::ft_key_t;  // 1 qword with kKeySize=8; access via qword[0]
using MicaResult = mica::table::Result;

static constexpr size_t kValSize = 8;  // uint64_t value

static inline MicaKey make_key(uint64_t k) {
    MicaKey mk;
    mk.qword[0] = k;
    return mk;
}
