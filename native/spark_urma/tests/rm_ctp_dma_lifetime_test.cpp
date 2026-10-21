// Regression: an API failure must not imply release permission while a child
// remains provider-owned. Test-only state injection; no hardware is exercised.
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
#include <cstdio>

using namespace spark_urma;

int main() {
    UrmaTransport transport;
    transport.options_.drain_timeout_ms = 2;
    UrmaTransport::RequestRecord parent{};
    parent.request_id = 1;
    parent.is_aggregate = true;
    parent.bytes = 8192;
    parent.state = RequestState::POSTED;
    parent.children = {2};
    UrmaTransport::RequestRecord child{};
    child.request_id = 2;
    child.bytes = 4096;
    child.state = RequestState::POSTED;
    child.provider_outstanding = true;
    child.aggregate_parent = 1;
    transport.requests_.emplace(1, parent);
    transport.requests_.emplace(2, child);
    transport.metrics_.record_provider_post(2);
    transport.finalize_locked(transport.requests_.at(1), RequestState::COMPLETED_FAILED,
                             static_cast<int>(ErrorCode::ERR_COMPLETION), 0,
                             "test-only second child submission failure");
    bool protected_failure = false;
    try {
        const auto result = transport.wait(1, 1);
        std::printf("API_RESULT status=%d provider_outstanding=%llu\n", result.status,
                    static_cast<unsigned long long>(transport.metrics_.provider_outstanding_requests.load()));
    } catch (const std::exception& e) {
        std::printf("API_EXCEPTION %s\n", e.what());
        protected_failure = std::string(e.what()).find("ERR_DMA_UNRESOLVED") != std::string::npos;
    }
    // Release injected ownership before destructing; no provider operation was posted.
    transport.provider_drained_locked(transport.requests_.at(2));
    transport.finalize_locked(transport.requests_.at(2), RequestState::COMPLETED_FAILED,
                             static_cast<int>(ErrorCode::ERR_COMPLETION), 0, "test cleanup");
    std::puts(protected_failure ? "DMA_LIFETIME_REGRESSION_PASS" :
              "DMA_LIFETIME_REGRESSION_FAIL: ordinary failure returned while DMA ownership remains");
    return protected_failure ? 0 : 1;
}
