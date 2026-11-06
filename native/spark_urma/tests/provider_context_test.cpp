#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#define private public
#include "urma_transport.h"
#undef private
#include <urma_api.h>
#include "provider_context.h"
#include <cstdio>
using namespace spark_urma;

static int jetty_gets, jetty_puts, seg_gets, seg_puts, jetty_imports, seg_imports;
static bool fail_export;
static urma_target_jetty_t target{};
static urma_target_seg_t target_seg{};
static urma_target_seg_t local_seg{};
static urma_jfc_t completion_queue{};
static uint8_t local_memory[8192], remote_memory[8192];
static std::deque<urma_cr_t> completions;
static void check(bool ok, const char* why) { if (!ok) throw std::runtime_error(why); }
template<class F> static void rejects(F f) {
    bool rejected = false; try { f(); } catch (const std::runtime_error&) { rejected = true; }
    check(rejected, "malformed context was accepted");
}
template<class T> static T* allocate_context() {
    auto* result = static_cast<T*>(std::calloc(1, sizeof(T)+32));
    if (!result) throw std::bad_alloc();
    result->ext.flag.bs.enable = 1; result->ext.length = 32;
    std::memset(result->ext.buf, 0xA5, 32);
    return result;
}
extern "C" urma_status_t __wrap_urma_get_rjetty(urma_jetty_t*, urma_rjetty_t** out, uint32_t* size) {
    ++jetty_gets; *out = allocate_context<urma_rjetty_t>(); *size = sizeof(urma_rjetty_t)+32;
    (*out)->jetty_id.eid.raw[0] = 9; (*out)->jetty_id.uasid = 3; (*out)->jetty_id.id = 27;
    (*out)->trans_mode = URMA_TM_RM; (*out)->type = URMA_JETTY;
    // Deliberately leave TP zero, as June core does.
    return fail_export ? URMA_FAIL : URMA_SUCCESS;
}
extern "C" void __wrap_urma_put_rjetty(urma_rjetty_t* p) { ++jetty_puts; std::free(p); }
extern "C" urma_status_t __wrap_urma_get_seg_ctx(urma_target_seg_t*, urma_seg_t** out, uint32_t* size) {
    ++seg_gets; *out = allocate_context<urma_seg_t>(); *size = sizeof(urma_seg_t)+32;
    (*out)->ubva.eid.raw[0] = 9; (*out)->ubva.uasid = 3; (*out)->ubva.va = 0x10000;
    (*out)->len = 8192; (*out)->token_id = 7;
    (*out)->attr.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    return fail_export ? URMA_FAIL : URMA_SUCCESS;
}
extern "C" void __wrap_urma_put_seg_ctx(urma_seg_t* p) { ++seg_puts; std::free(p); }
extern "C" urma_target_jetty_t* __wrap_urma_import_jetty(urma_context_t*, urma_rjetty_t* p, urma_token_t*) {
    ++jetty_imports;
    check(p->tp_type == URMA_CTP && p->trans_mode == URMA_TM_RM, "mode/TP lost on context import");
    check(p->ext.length == 32 && static_cast<unsigned char>(p->ext.buf[31]) == 0xA5, "Jetty extension lost");
    return &target;
}
extern "C" urma_target_seg_t* __wrap_urma_import_seg(
    urma_context_t*, urma_seg_t* p, urma_token_t*, uint64_t addr, urma_import_seg_flag_t flags) {
    ++seg_imports;
    check(addr == 0 && flags.bs.mapping == URMA_SEG_NOMAP, "NOMAP import changed");
    check(p->ext.length == 32 && static_cast<unsigned char>(p->ext.buf[31]) == 0xA5, "segment extension lost");
    return &target_seg;
}
extern "C" urma_status_t __wrap_urma_post_jetty_send_wr(urma_jetty_t*, urma_jfs_wr_t* wr, urma_jfs_wr_t**) {
    check(wr->tjetty == &target && wr->flag.bs.complete_enable, "wrong imported Jetty/completion flag");
    const bool write = wr->opcode == URMA_OPC_WRITE;
    check(write || wr->opcode == URMA_OPC_READ, "unexpected opcode");
    auto& local = write ? wr->rw.src.sge[0] : wr->rw.dst.sge[0];
    auto& remote = write ? wr->rw.dst.sge[0] : wr->rw.src.sge[0];
    check(local.tseg == &local_seg && remote.tseg == &target_seg && remote.addr >= 0x10000 &&
          remote.addr - 0x10000 + remote.len <= sizeof(remote_memory), "incorrect SGE/MR association");
    auto* address = reinterpret_cast<void*>(local.addr);
    auto* peer = remote_memory + (remote.addr - 0x10000);
    if (write) std::memcpy(peer, address, local.len); else std::memcpy(address, peer, local.len);
    urma_cr_t cr{}; cr.user_ctx = wr->user_ctx; cr.status = URMA_CR_SUCCESS;
    completions.push_back(cr); return URMA_SUCCESS;
}
extern "C" int __wrap_urma_poll_jfc(urma_jfc_t*, int, urma_cr_t* cr) {
    if (completions.empty()) return 0;
    *cr = completions.front(); completions.pop_front(); return 1;
}
int main() {
    try {
        UrmaTransport t;
        struct Reset { UrmaTransport& t; ~Reset() {
            t.remote_jetty_ = nullptr; t.registered_regions_.clear(); t.imported_regions_.clear();
            t.state_ = TransportState::CLOSED;
        }} reset{t};
        t.options_.device_name = "bonding_dev_0"; t.options_.transport_mode = URMA_TM_RM;
        t.options_.tp_type = URMA_CTP; t.state_ = TransportState::INITIALIZED;
        t.jfc_ = &completion_queue;
        t.local_max_raw_operation_bytes_ = 4096;
        auto& registration = t.registered_regions_[1]; registration.registered = true;
        registration.segment = &local_seg; registration.region.handle = 1;
        registration.region.address = local_memory; registration.region.length = sizeof(local_memory);
        auto jetty = t.export_jetty_context();
        auto segment = t.export_segment_context(1);
        check(jetty_gets == jetty_puts && seg_gets == seg_puts, "exported SDK allocations leaked");
        EndpointDescriptor ep{}; ep.eid[0] = 9; ep.uasid = 3; ep.jetty_id = 27;
        ep.transport_mode = URMA_TM_RM; ep.tp_type = URMA_CTP; ep.max_chunk_bytes = 4096;
        for (size_t at : {size_t(0),size_t(4),size_t(8),size_t(12),size_t(16),size_t(20),size_t(24)}) {
            auto bad = jetty; bad[at] ^= 1; rejects([&] { t.connect(ep, bad); });
        }
        auto truncated = jetty; truncated.pop_back(); rejects([&] { t.connect(ep, truncated); });
        auto no_ext = jetty;
        auto* raw = reinterpret_cast<urma_rjetty_t*>(no_ext.data()+CONTEXT_HEADER);
        raw->ext.flag.bs.enable = 0; rejects([&] { t.connect(ep, no_ext); });
        rejects([&] { t.connect(ep); });
        auto wrong_identity = ep; wrong_identity.jetty_id++;
        rejects([&] { t.connect(wrong_identity, jetty); });
        check(jetty_imports == 0, "invalid context reached provider");
        t.connect(ep, jetty); check(jetty_imports == 1, "full Jetty context not imported");
        rejects([&] { t.connect(ep, truncated); });
        check(t.state_ == TransportState::CONNECTED && t.remote_jetty_ == &target,
              "invalid prospective peer destroyed the existing connection");
        RemoteRegion remote{}; remote.has_segment = true; remote.segment_eid[0] = 9;
        remote.segment_uasid = 3; remote.segment_address = remote.remote_address = 0x10000;
        remote.segment_length = remote.length = 8192; remote.token = 7; remote.generation = 1;
        // Use the actual exported attribute encoding, not a guessed bit layout.
        auto exported = unpack_context<urma_seg_t>(segment, 2);
        remote.segment_attributes = exported->attr.value; remote.provider_context = segment;
        auto mismatched = remote; mismatched.segment_uasid++;
        rejects([&] { t.find_or_import_remote_locked(mismatched); });
        auto missing = remote; missing.provider_context.clear();
        rejects([&] { t.find_or_import_remote_locked(missing); });
        t.find_or_import_remote_locked(remote); t.find_or_import_remote_locked(remote);
        check(seg_imports == 1, "full MR context not imported/cached");
        for (size_t i = 0; i < 4096; ++i) local_memory[i] = static_cast<uint8_t>(i * 31);
        auto write = t.write(remote, local_memory, 4096);
        check(t.wait(write, 1000).status == 0, "WRITE did not complete");
        std::memset(local_memory, 0, 4096);
        auto read = t.read(remote, local_memory, 4096);
        check(t.wait(read, 1000).status == 0, "READ did not complete");
        for (size_t i = 0; i < 4096; ++i)
            check(local_memory[i] == static_cast<uint8_t>(i * 31), "READ/WRITE payload mismatch");
        fail_export = true;
        rejects([&] { t.export_jetty_context(); }); rejects([&] { t.export_segment_context(1); });
        check(jetty_gets == jetty_puts && seg_gets == seg_puts, "failed export leaked SDK allocation");
        t.options_.device_name = "udmac0d1e2";
        check(t.export_jetty_context().empty() && t.export_segment_context(1).empty(), "physical path unexpectedly exported bonding data");
        puts("PROVIDER_CONTEXT_PASS export/import, full extensions, RM/CTP, WRITE/READ=4096 bytes, ABI/length/identity rejection, cache, SDK put, physical bypass (wrapped provider; NOT hardware UB)");
        return 0;
    } catch (const std::exception& e) { std::fprintf(stderr, "%s\n", e.what()); return 1; }
}
