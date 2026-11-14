// Offline state injection: no device, provider submission or hardware timing.
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
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
#include <stdexcept>

using namespace spark_urma;
static void check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
static void completed_child(UrmaTransport& t, uint64_t parent_id, uint64_t child_id,
                            bool success = true) {
    UrmaTransport::RequestRecord parent{};
    parent.request_id = parent_id; parent.is_aggregate = true;
    parent.bytes = 4096; parent.state = RequestState::POSTED; parent.children = {child_id};
    UrmaTransport::RequestRecord child{};
    child.request_id = child_id; child.bytes = 4096; child.aggregate_parent = parent_id;
    child.state = success ? RequestState::COMPLETED_SUCCESS : RequestState::COMPLETED_FAILED;
    child.completed_bytes = success ? 4096 : 0;
    child.status = success ? 0 : static_cast<int>(ErrorCode::ERR_COMPLETION);
    t.requests_.emplace(parent_id, parent); t.requests_.emplace(child_id, child);
}
int main(int argc, char** argv) {
    try {
        UrmaTransport t;
        constexpr size_t history = 65536;
        for (size_t i = 0; i < history; ++i) {
            UrmaTransport::RequestRecord old{};
            old.request_id = i + 100; old.is_aggregate = true;
            old.state = RequestState::COMPLETED_SUCCESS;
            t.requests_.emplace(old.request_id, old);
        }
        if (argc == 2 && std::string(argv[1]) == "--benchmark") {
            const auto start = std::chrono::steady_clock::now();
            constexpr int repetitions = 2000;
            for (int i = 0; i < repetitions; ++i) {
                completed_child(t, 1, 2);
                check(t.wait(1, 1000).status == 0, "benchmark completion failed");
                t.requests_.erase(1); t.requests_.erase(2);
            }
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - start).count();
            std::printf("OFFLINE_WAIT_HISTORY_BENCH history=%zu repetitions=%d elapsed_ns=%lld NOT_HARDWARE\n",
                        history, repetitions, static_cast<long long>(ns));
            return 0;
        }
        completed_child(t, 1, 2); completed_child(t, 3, 4);
        check(t.wait(1, 1000).bytes == 4096, "target did not complete");
        check(t.requests_.at(3).state == RequestState::POSTED,
              "waiting for one aggregate traversed/refreshed another aggregate");
        check(t.wait(3, 1000).bytes == 4096, "independent waiter did not complete");
        completed_child(t, 5, 6, false);
        check(t.wait(5, 1000).status != 0, "child failure lost");
        completed_child(t, 7, 8); completed_child(t, 9, 10);
        t.close();
        check(t.requests_.at(7).state == RequestState::COMPLETED_SUCCESS &&
              t.requests_.at(9).state == RequestState::COMPLETED_SUCCESS,
              "close no longer refreshes all remaining aggregates");
        std::puts("TARGETED_COMPLETION_HISTORY_INDEPENDENCE_FAILURE_AND_GLOBAL_CLOSE_PASS");
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what()); return 1;
    }
}
