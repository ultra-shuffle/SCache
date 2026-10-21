// Provider failures must retain the original errno across cleanup calls.
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
#include <cerrno>
#include <cstdio>
#include <stdexcept>

using namespace spark_urma;
static urma_token_id_t token{};
static bool allocation_failure, cleanup_failure;
extern "C" urma_token_id_t* __wrap_urma_alloc_token_id(urma_context_t*) {
    if (allocation_failure) { errno = ENOMEM; return nullptr; }
    return &token;
}
extern "C" urma_target_seg_t* __wrap_urma_register_seg(urma_context_t*, urma_seg_cfg_t*) {
    errno = EACCES;
    return nullptr;
}
extern "C" urma_status_t __wrap_urma_free_token_id(urma_token_id_t*) {
    errno = EBUSY; // Must not replace the MR registration error.
    return cleanup_failure ? static_cast<urma_status_t>(-1) : URMA_SUCCESS;
}
static void check(bool ok, const char* reason) {
    if (!ok) throw std::runtime_error(reason);
}
int main() {
    UrmaTransport t;
    struct Reset { UrmaTransport& t; ~Reset() {
        t.registered_regions_.clear(); t.state_ = TransportState::CLOSED;
    }} reset{t};
    t.options_.device_name = "udma-diagnostic";
    t.options_.transport_mode = URMA_TM_RM; t.options_.tp_type = URMA_CTP;
    t.options_.eid_index = 1; t.options_.priority = 6;
    try {
        for (unsigned scenario = 0; scenario < 3; ++scenario) {
            allocation_failure = scenario == 0;
            cleanup_failure = scenario == 2;
            t.state_ = TransportState::INITIALIZED;
            bool failed = false;
            try { t.register_region(reinterpret_cast<void*>(0x10000), 4096); }
            catch (const std::runtime_error& error) {
                failed = true;
                const std::string message = error.what();
                check(message.find("device=udma-diagnostic mode=RM tp=CTP eid_index=1 priority=6")
                      != std::string::npos, "registration failure lost endpoint context");
                check(message.find("errno=" + std::to_string(allocation_failure ? ENOMEM : EACCES))
                      != std::string::npos, "cleanup overwrote the primary provider errno");
                if (!allocation_failure)
                    check(message.find("length=4096") != std::string::npos, "missing MR length");
                if (cleanup_failure)
                    check(message.find("token cleanup also failed") != std::string::npos,
                          "cleanup failure was hidden");
            }
            check(failed, "failed provider registration was accepted");
            check(t.registered_regions_.size() == (cleanup_failure ? 1u : 0u),
                  "failed token cleanup lost ownership or successful cleanup leaked ownership");
            if (cleanup_failure)
                check(t.state_ == TransportState::CLOSING, "failed cleanup remained reusable");
        }
    } catch (const std::exception& error) {
        std::fprintf(stderr, "%s\n", error.what()); return 1;
    }
    std::puts("REGISTRATION_DIAGNOSTICS_PASS (wrapped provider; no hardware)");
}
