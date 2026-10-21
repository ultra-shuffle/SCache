// Real transport code with wrapped provider calls. No hardware or fake DMA.
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
#include <stdexcept>
#include <cstdlib>
#include <new>

static bool fail_cpp_allocations = false;
[[gnu::noinline]] void* operator new(std::size_t length) {
    if (fail_cpp_allocations) throw std::bad_alloc();
    if (void* memory = std::malloc(length == 0 ? 1 : length)) return memory;
    throw std::bad_alloc();
}
[[gnu::noinline]] void operator delete(void* memory) noexcept { std::free(memory); }
[[gnu::noinline]] void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

using namespace spark_urma;
static int scenario, posts;
static uint64_t submitted;
static bool complete;
static std::vector<uint64_t> finite_queue;
static unsigned queue_overflows;
static bool finite_poll_enabled = true;
static bool finite_poll_failure = false;
static bool finite_completion_failure = false;
static bool finite_reverse_completions = false;
extern "C" urma_status_t __wrap_urma_post_jetty_send_wr(
    urma_jetty_t*, urma_jfs_wr_t* wr, urma_jfs_wr_t** bad) {
    ++posts;
    if (scenario == 6) {
        if (finite_queue.size() >= 2) {
            ++queue_overflows;
            *bad = wr;
            return URMA_ENOMEM; // fixed official UDMA SQ-full result
        }
        finite_queue.push_back(wr->user_ctx);
        return URMA_SUCCESS;
    }
    if (scenario == 5) {
        if (posts == 1) { submitted = wr->user_ctx; return URMA_SUCCESS; }
        *bad = wr;
        fail_cpp_allocations = true;
        return URMA_FAIL;
    }
    submitted = wr->user_ctx;
    if (scenario >= 3) {
        if (posts == 1) { *bad = wr; return URMA_EAGAIN; }
        // Scenario 3 deliberately does not write bad_wr on the second
        // failure: the transport must reset the previous retry's pointer.
        return scenario == 4 ? URMA_SUCCESS : URMA_EAGAIN;
    }
    *bad = scenario == 0 ? wr : nullptr;
    return scenario == 2 ? URMA_SUCCESS : URMA_EAGAIN;
}
extern "C" int __wrap_urma_poll_jfc(urma_jfc_t*, int, urma_cr_t* cr) {
    if (scenario == 6) {
        if (finite_poll_failure) return -1;
        if (!finite_poll_enabled || finite_queue.empty()) return 0;
        *cr = {};
        cr->user_ctx = finite_reverse_completions ? finite_queue.back() : finite_queue.front();
        if (finite_reverse_completions) finite_queue.pop_back();
        else finite_queue.erase(finite_queue.begin());
        cr->status = finite_completion_failure ? URMA_CR_WR_FLUSH_ERR : URMA_CR_SUCCESS;
        finite_completion_failure = false;
        return 1;
    }
    if (!complete) return 0;
    complete = false;
    *cr = {};
    cr->user_ctx = submitted;
    cr->status = URMA_CR_SUCCESS;
    return 1;
}
static void check(bool yes, const char* message) {
    if (!yes) throw std::runtime_error(message);
}
static void run(int mode, int operation) {
    scenario = mode; posts = 0; submitted = 0; complete = false;
    UrmaTransport t;
    // RAII clears test-only sentinel handles even if an assertion throws.
    struct Reset {
        UrmaTransport& t;
        ~Reset() {
            t.requests_.clear(); t.registered_regions_.clear(); t.imported_regions_.clear();
            t.jfc_ = nullptr; t.jetty_ = nullptr; t.remote_jetty_ = nullptr;
            t.state_ = TransportState::CLOSED;
        }
    } reset{t};
    char buffer[64]{};
    UrmaTransport::LocalRegionState local{};
    local.region.handle = 1; local.region.address = buffer; local.region.length = sizeof(buffer);
    local.segment = reinterpret_cast<urma_target_seg_t*>(0x10);
    t.registered_regions_.emplace(1, local);
    RemoteRegion remote{};
    remote.remote_address = 0x10040; remote.length = 64; remote.token = 7; remote.generation = 1;
    remote.has_segment = true; remote.segment_address = 0x10000; remote.segment_length = 4096;
    t.imported_regions_.push_back({remote, reinterpret_cast<urma_target_seg_t*>(0x20), 0, 0});
    t.state_ = TransportState::CONNECTED; t.effective_max_raw_operation_bytes_ = 4096;
    t.options_.request_timeout_ms = mode >= 3 ? 100 : 1; t.options_.drain_timeout_ms = 1;
    t.jfc_ = reinterpret_cast<urma_jfc_t*>(0x30);
    t.jetty_ = reinterpret_cast<urma_jetty_t*>(0x40);
    // Keep a separate accepted request in flight. An ambiguous submission
    // must never consume its metrics slot when reporting its own API failure.
    const bool ambiguous = mode == 1 || mode == 3;
    if (ambiguous) t.metrics_.record_read(999, 1);
    auto submit = [&]() {
        if (operation == 2) return t.send(buffer, sizeof(buffer));
        if (mode == 0) return operation == 0 ? t.read(remote, buffer, sizeof(buffer)) :
            t.write(remote, buffer, sizeof(buffer));
        return operation == 0 ? t.readBlock(remote, buffer, sizeof(buffer)) :
            t.writeBlock(remote, buffer, sizeof(buffer));
    };
    if (mode == 0) {
        bool rejected = false;
        try { submit(); }
        catch (const std::runtime_error& e) {
            rejected = std::string(e.what()).find("post failed") != std::string::npos;
        }
        check(rejected && t.requests_.empty(), "explicit unaccepted WR must fail without ownership");
        check(t.registered_regions_.at(1).inflight == 0, "rejected WR pinned local MR");
    } else {
        const auto id = submit();
        check(posts == (mode >= 3 ? 2 : 1),
              "retry count wrong or ambiguous WR was resubmitted");
        check(t.registered_regions_.at(1).inflight == 1, "provider ownership missing");
        if (ambiguous) {
            check(t.metrics_.submitted_requests.load() == 2 &&
                  t.metrics_.failed_requests.load() == 1 &&
                  t.metrics_.inflight_requests.load() == 1,
                  "ambiguous post corrupted another request's metrics");
            bool protected_failure = false;
            try { t.wait(id, 1); }
            catch (const std::runtime_error& e) {
                protected_failure = std::string(e.what()).find("ERR_DMA_UNRESOLVED") != std::string::npos;
            }
            check(protected_failure, "ambiguous post returned release permission");
        }
        complete = true;
        const auto result = t.wait(id, 10);
        check(!ambiguous ? result.status == 0 : result.status != 0,
              "late successful CQ must not erase ambiguous API failure");
        check(t.registered_regions_.at(1).inflight == 0 && t.imported_regions_[0].inflight == 0,
              "late CQ did not release provider references");
    }
}
static void run_aggregate_finalization_oom(bool write) {
    scenario = 5; posts = 0; submitted = 0; complete = false;
    UrmaTransport t;
    struct Reset {
        UrmaTransport& t;
        ~Reset() {
            fail_cpp_allocations = false;
            t.requests_.clear(); t.registered_regions_.clear(); t.imported_regions_.clear();
            t.jfc_ = nullptr; t.jetty_ = nullptr; t.remote_jetty_ = nullptr;
            t.state_ = TransportState::CLOSED;
        }
    } reset{t};
    char buffer[8192]{};
    UrmaTransport::LocalRegionState local{};
    local.region.handle = 1; local.region.address = buffer; local.region.length = sizeof(buffer);
    local.segment = reinterpret_cast<urma_target_seg_t*>(0x10);
    t.registered_regions_.emplace(1, local);
    RemoteRegion remote{};
    remote.remote_address = 0x10000; remote.length = sizeof(buffer);
    remote.token = 7; remote.generation = 1; remote.has_segment = true;
    remote.segment_address = remote.remote_address; remote.segment_length = remote.length;
    t.imported_regions_.push_back({remote, reinterpret_cast<urma_target_seg_t*>(0x20), 0, 0});
    t.state_ = TransportState::CONNECTED; t.effective_max_raw_operation_bytes_ = 4096;
    t.options_.request_timeout_ms = 1; t.options_.drain_timeout_ms = 1;
    t.jfc_ = reinterpret_cast<urma_jfc_t*>(0x30);
    t.jetty_ = reinterpret_cast<urma_jetty_t*>(0x40);
    // Force the next terminal-cache insertion to need a deque block. This
    // deterministically exercises allocation failure while reporting failure
    // AFTER child 1 was accepted and child 2 was definitively rejected.
    for (int i = 0; i < 63; ++i) t.terminal_order_.push_back(10000 + i);
    uint64_t id = 0;
    try {
        id = write ? t.writeBlock(remote, buffer, sizeof(buffer)) :
            t.readBlock(remote, buffer, sizeof(buffer));
    } catch (...) {
        fail_cpp_allocations = false;
        std::fprintf(stderr, "AGGREGATE_FINALIZATION_EXCEPTION_ESCAPED provider_owned=%llu\n",
            static_cast<unsigned long long>(t.metrics_.provider_outstanding_requests.load()));
        throw;
    }
    fail_cpp_allocations = false;
    check(id != 0 && posts == 2, "partial post did not return its tracked parent");
    check(t.metrics_.provider_outstanding_requests.load() == 1 &&
        t.registered_regions_.at(1).inflight == 1, "OOM lost accepted child's memory ownership");
    bool retained = false;
    try { t.wait(id, 1); }
    catch (const std::exception& error) {
        retained = std::string(error.what()).find("ERR_DMA_UNRESOLVED") != std::string::npos;
    }
    check(retained, "OOM reporting returned ordinary failure before child drain");
    complete = true;
    check(t.wait(id, 10).status != 0, "late CQ erased the logical block submission failure");
    check(t.registered_regions_.at(1).inflight == 0 && t.imported_regions_[0].inflight == 0,
          "accepted child did not release its references after the late CQ");
}
static void run_finite_queue(int operation, bool stalled = false, bool poll_error = false,
                             bool completion_error = false, bool reverse_completions = false) {
    scenario = 6; posts = 0; queue_overflows = 0; finite_queue.clear();
    finite_poll_enabled = !stalled;
    finite_poll_failure = poll_error;
    finite_completion_failure = completion_error;
    finite_reverse_completions = reverse_completions;
    UrmaTransport t;
    struct Reset {
        UrmaTransport& t;
        ~Reset() {
            t.requests_.clear(); t.registered_regions_.clear(); t.imported_regions_.clear();
            t.jfc_ = nullptr; t.jetty_ = nullptr; t.remote_jetty_ = nullptr;
            t.state_ = TransportState::CLOSED;
        }
    } reset{t};
    char buffer[16384]{};
    UrmaTransport::LocalRegionState local{};
    local.region.handle = 1; local.region.address = buffer; local.region.length = sizeof(buffer);
    local.segment = reinterpret_cast<urma_target_seg_t*>(0x10);
    t.registered_regions_.emplace(1, local);
    RemoteRegion remote{};
    remote.remote_address = remote.segment_address = 0x10000;
    remote.length = remote.segment_length = sizeof(buffer);
    remote.token = 7; remote.generation = 1; remote.has_segment = true;
    t.imported_regions_.push_back({remote, reinterpret_cast<urma_target_seg_t*>(0x20), 0, 0});
    t.state_ = TransportState::CONNECTED;
    t.effective_max_raw_operation_bytes_ = 4096;
    t.options_.queue_depth = 2; t.options_.request_timeout_ms = stalled ? 1 : 100;
    t.options_.drain_timeout_ms = 1;
    t.jfc_ = reinterpret_cast<urma_jfc_t*>(0x30);
    t.jetty_ = reinterpret_cast<urma_jetty_t*>(0x40);
    std::vector<uint64_t> ids;
    if (operation == 2) {
        for (int i = 0; i < 4; ++i) ids.push_back(t.send(buffer + i * 4096, 4096));
    } else {
        ids.push_back(operation == 0 ? t.readBlock(remote, buffer, sizeof(buffer)) :
            t.writeBlock(remote, buffer, sizeof(buffer)));
    }
    if (stalled) {
        check(ids.size() == 1 && posts == 2 && queue_overflows == 0,
              "capacity deadline submitted another WR or lost tracked block ID");
        bool retained = false;
        try { t.wait(ids[0], 1); } catch (const std::runtime_error& e) {
            retained = std::string(e.what()).find("ERR_DMA_UNRESOLVED") != std::string::npos;
        }
        check(retained && t.registered_regions_.at(1).inflight == 2 &&
              t.imported_regions_[0].inflight == 2,
              "capacity timeout prematurely released accepted children's MR references");
        finite_poll_enabled = true;
        finite_poll_failure = false;
        check(t.wait(ids[0], 100).status != 0, "late CQ erased the capacity timeout failure");
    } else {
        bool observed_failure = false;
        for (auto id : ids)
            observed_failure = (t.wait(id, 100).status != 0) || observed_failure;
        check(observed_failure == completion_error,
              "capacity polling lost a failed CQE or rejected a normal block");
    }
    check(posts == (stalled ? 2 : 4) && queue_overflows == 0 && finite_queue.empty(),
          "configured SQ depth was exceeded instead of polling existing completions");
    check(t.metrics_.provider_outstanding_requests.load() == 0 &&
          t.registered_regions_.at(1).inflight == 0 && t.imported_regions_[0].inflight == 0,
          "queue-capacity polling lost provider references");
}

int main() {
    try {
        for (int op = 0; op < 3; ++op) for (int i = 0; i < 5; ++i) run(i, op);
        run_aggregate_finalization_oom(false);
        run_aggregate_finalization_oom(true);
        for (int op = 0; op < 3; ++op) run_finite_queue(op);
        run_finite_queue(0, true);
        run_finite_queue(1, true);
        run_finite_queue(0, true, true);
        run_finite_queue(1, true, true);
        for (int op = 0; op < 3; ++op) run_finite_queue(op, false, false, true);
        for (int op = 0; op < 3; ++op) {
            run_finite_queue(op, false, false, false, true);
            run_finite_queue(op, false, false, true, true);
        }
    }
    catch (const std::exception& e) { std::fprintf(stderr, "%s\n", e.what()); return 1; }
    std::puts("RM_CTP_POST_OWNERSHIP_PASS cases=33 (wrapped provider; no hardware)");
}
