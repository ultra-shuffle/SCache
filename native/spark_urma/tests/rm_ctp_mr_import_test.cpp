// Inspect actual transport import arguments through test-only linker wrappers.
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
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <stdexcept>
using namespace spark_urma;
static urma_seg_t seen{};
static urma_import_seg_flag_t flags{};
static uint64_t map_address;
static unsigned imports, unimports;
static bool fail_import, fail_unimport;
static urma_target_seg_t slots[16]{};
extern "C" urma_target_seg_t* __wrap_urma_import_seg(
    urma_context_t*, urma_seg_t* seg, urma_token_t*, uint64_t address, urma_import_seg_flag_t flag) {
    seen = *seg; flags = flag; map_address = address;
    ++imports;
    if (fail_import) errno = EACCES;
    return fail_import ? nullptr : &slots[imports % 16];
}
extern "C" urma_status_t __wrap_urma_unimport_seg(urma_target_seg_t*) {
    ++unimports;
    return fail_unimport ? static_cast<urma_status_t>(-1) : URMA_SUCCESS;
}
static void check(bool ok, const char* why) { if (!ok) throw std::runtime_error(why); }
template<class F> static void rejects(F f) {
    bool rejected = false;
    try { f(); } catch (const std::runtime_error&) { rejected = true; }
    check(rejected, "expected rejection");
}
int main() {
    UrmaTransport t;
    struct Reset { UrmaTransport& t; ~Reset() {
        t.imported_regions_.clear(); t.state_ = TransportState::CLOSED;
    }} reset{t};
    try {
        t.remote_ep_.segment_uasid = 999;
        t.remote_ep_.segment_eid[0] = 99;
        RemoteRegion r{};
        r.has_segment = true; r.segment_address = 0x80000; r.segment_length = 8192;
        r.remote_address = 0x80100; r.length = 128; r.token = 0xffffffff; r.generation = 7;
        r.segment_eid[0] = 17; r.segment_uasid = 42; r.segment_attributes = 6;
        auto* first = t.find_or_import_remote_locked(r).segment;
        check(imports == 1 && seen.ubva.va == r.segment_address && seen.len == r.segment_length,
              "import used block window rather than full MR");
        check(seen.ubva.uasid == 42 && std::memcmp(&seen.ubva.eid, r.segment_eid, 16) == 0,
              "import used endpoint identity instead of current MR");
        check(seen.attr.value == 6 && seen.token_id == 0xffffffff && map_address == 0,
              "attributes/token/map address lost");
        check(flags.bs.mapping == URMA_SEG_NOMAP &&
              flags.bs.access == (URMA_ACCESS_READ | URMA_ACCESS_WRITE), "wrong import flags");
        r.remote_address += 256;
        check(t.find_or_import_remote_locked(r).segment == first && imports == 1,
              "same MR was imported once per block");
        r.segment_eid[0] = 18;
        check(t.find_or_import_remote_locked(r).segment != first && imports == 2,
              "distinct EID conflated in cache");
        ++r.generation;
        t.find_or_import_remote_locked(r);
        check(imports == 3, "new registration generation reused stale import");
        RemoteRegion invalid = r; invalid.remote_address = r.segment_address + r.segment_length;
        rejects([&] { t.find_or_import_remote_locked(invalid); });
        invalid = r; invalid.has_segment = false;
        rejects([&] { t.find_or_import_remote_locked(invalid); });
        invalid = r; invalid.segment_address = UINT64_MAX - 1;
        rejects([&] { t.find_or_import_remote_locked(invalid); });
        check(imports == 3, "invalid MR reached provider");
        fail_import = true; ++r.generation;
        t.options_.device_name = "bonding-diagnostic";
        t.options_.transport_mode = URMA_TM_RM;
        t.options_.tp_type = URMA_CTP;
        t.options_.eid_index = 3; t.options_.priority = 7;
        bool diagnostic = false;
        try { t.find_or_import_remote_locked(r); }
        catch (const std::runtime_error& error) {
            const std::string message = error.what();
            diagnostic = message.find("urma_import_seg failed device=bonding-diagnostic") != std::string::npos &&
                message.find("mode=RM tp=CTP eid_index=3 priority=7") != std::string::npos &&
                message.find("errno=" + std::to_string(EACCES)) != std::string::npos &&
                message.find("segment_length=8192") != std::string::npos;
        }
        check(diagnostic, "MR import failure lost provider errno or selected endpoint context");
        check(t.imported_regions_.size() == 3 && t.metrics_.active_imports.load() == 3,
              "failed import entered cache");
        fail_import = false; fail_unimport = true;
        rejects([&] { t.release_cached_remote_imports(); });
        check(t.imported_regions_.size() == 3, "failed unimport discarded ownership");
        fail_unimport = false;
        t.release_cached_remote_imports();
        check(t.imported_regions_.empty() && t.metrics_.active_imports.load() == 0 && unimports == 4,
              "unimport retry failed to release exactly three imports");
    } catch (const std::exception& e) { std::fprintf(stderr, "%s\n", e.what()); return 1; }
    std::puts("RM_CTP_MR_IMPORT_PASS (wrapped provider; no hardware)");
}
