// Offline native range contract: CPU VA must not be confused with remote UBVA.
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
#include <stdexcept>

using namespace spark_urma;
int main() {
    UrmaTransport transport;
    UrmaTransport::LocalRegionState local;
    local.region.handle = 1;
    local.region.address = reinterpret_cast<void*>(0x10000);
    local.region.remote_address = 0x90000;
    local.region.length = 4096;
    transport.registered_regions_.emplace(1, local);
    bool ok = true;
    try {
        ok = transport.find_local_region_locked(reinterpret_cast<void*>(0x10040), 64).region.handle == 1;
        try {
            transport.find_local_region_locked(reinterpret_cast<void*>(0x90040), 64);
            ok = false;
        } catch (const std::runtime_error&) {}
        try {
            transport.find_local_region_locked(reinterpret_cast<void*>(0x10fff), 2);
            ok = false;
        } catch (const std::runtime_error&) {}
    } catch (...) { ok = false; }
    // No real registration or provider calls were made.
    transport.registered_regions_.clear();
    std::puts(ok ? "LOCAL_VA_CONTRACT_PASS" : "LOCAL_VA_CONTRACT_FAIL");
    return ok ? 0 : 1;
}
