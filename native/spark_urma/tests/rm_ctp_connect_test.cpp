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
#include <stdexcept>
using namespace spark_urma;
static urma_rjetty_t seen{};
static urma_target_jetty_t target{};
static int imports, binds, unbinds, unimports;
static bool fail_unimport;
static bool fail_import;
static int import_errno;
extern "C" urma_target_jetty_t* __wrap_urma_import_jetty(
    urma_context_t*, urma_rjetty_t* r, urma_token_t*) {
    seen = *r; ++imports;
    if (fail_import) { if (import_errno) errno = import_errno; return nullptr; }
    return &target;
}
extern "C" urma_status_t __wrap_urma_bind_jetty(urma_jetty_t*, urma_target_jetty_t*) {
    ++binds; return URMA_SUCCESS;
}
extern "C" urma_status_t __wrap_urma_unbind_jetty(urma_jetty_t*) { ++unbinds; return URMA_SUCCESS; }
extern "C" urma_status_t __wrap_urma_unimport_jetty(urma_target_jetty_t*) {
    ++unimports; return fail_unimport ? static_cast<urma_status_t>(-1) : URMA_SUCCESS;
}
static void check(bool ok, const char* why) { if (!ok) throw std::runtime_error(why); }
template<class F> static void rejects(F action) {
    bool rejected = false; try { action(); } catch (const std::runtime_error&) { rejected = true; }
    check(rejected, "expected connection rejection");
}
static void run(bool rm) {
    imports = binds = unbinds = unimports = 0; fail_unimport = false;
    UrmaTransport t;
    struct Reset { UrmaTransport& t; ~Reset() {
        t.remote_jetty_ = nullptr; t.requests_.clear(); t.state_ = TransportState::CLOSED;
    }} reset{t};
    t.state_ = TransportState::INITIALIZED;
    t.options_.transport_mode = rm ? URMA_TM_RM : URMA_TM_RC;
    t.options_.tp_type = rm ? URMA_CTP : URMA_RTP;
    t.options_.device_name = "test-device";
    t.options_.eid_index = 7; t.options_.priority = rm ? 6 : 0;
    t.local_max_raw_operation_bytes_ = 65536;
    EndpointDescriptor ep{};
    ep.protocol_version = PROTOCOL_VERSION; ep.max_chunk_bytes = 8192;
    ep.transport_mode = t.options_.transport_mode; ep.tp_type = t.options_.tp_type;
    ep.uasid = 19; ep.jetty_id = 27; ep.eid[0] = 31;
    fail_import = true;
    for (int reported : {EINVAL, 0}) {
        import_errno = reported; errno = EBUSY;
        std::string message;
        try { t.connect(ep); } catch (const std::runtime_error& e) { message = e.what(); }
        check(message.find("urma_import_jetty failed device=test-device") != std::string::npos,
              "missing provider operation/device diagnostic");
        check(message.find("eid_index=7") != std::string::npos, "missing EID diagnostic");
        check(message.find("errno=" + std::to_string(reported)) != std::string::npos,
              "stale errno or lost provider errno");
        check(t.state_ == TransportState::INITIALIZED && t.remote_jetty_ == nullptr,
              "failed import changed connection state");
    }
    fail_import = false; imports = 0;
    t.connect(ep);
    check(seen.type == URMA_JETTY && seen.trans_mode == ep.transport_mode && seen.tp_type == ep.tp_type,
          "wrong remote Jetty type/mode/TP");
    check(seen.flag.bs.order_type == URMA_DEF_ORDER,
          "remote import must keep the same official default ordering as local queues");
    check(seen.jetty_id.uasid == 19 && seen.jetty_id.id == 27, "peer identity lost");
    check(t.effective_max_raw_operation_bytes_ == 8192, "wrong negotiated transfer limit");
    check(binds == (rm ? 0 : 1), "RM bound an RC peer or RC omitted bind");
    auto wrong = ep; wrong.tp_type ^= 1;
    rejects([&] { t.connect(wrong); });
    check(imports == 1 && unimports == 0 && t.state_ == TransportState::CONNECTED,
          "bad endpoint destroyed usable peer");
    UrmaTransport::RequestRecord building{};
    building.request_id = 1; building.is_aggregate = true; building.aggregate_building = true;
    t.requests_.emplace(1, building);
    rejects([&] { t.connect(ep); });
    check(unimports == 0, "reconnected between child submissions");
    t.requests_.clear();
    t.connect(ep);
    check(imports == 2 && unimports == 1 && unbinds == (rm ? 0 : 1), "wrong reconnect lifecycle");
    fail_unimport = true;
    rejects([&] { t.connect(ep); });
    check(t.state_ == TransportState::CLOSING && t.remote_jetty_ == &target,
          "cleanup failure discarded ownership or allowed data path");
}
int main() {
    try { run(true); run(false); }
    catch (const std::exception& e) { std::fprintf(stderr, "%s\n", e.what()); return 1; }
    std::puts("RM_CTP_CONNECT_PASS (wrapped provider; no hardware)");
}
