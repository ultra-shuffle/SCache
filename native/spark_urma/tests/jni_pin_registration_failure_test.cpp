// SPDX-License-Identifier: Apache-2.0
// Exercise the JNI boundary without a device: registration on a NEW transport
// must fail after the global pin is created, then release that pin.
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <new>

// Armed only after the wrapped wait has thrown. Turn off at the first JNI
// callback so the fake JVM's own std::string storage is outside the injection.
static bool fail_next_cpp_allocation = false;
[[gnu::noinline]] void* operator new(std::size_t length) {
    if (fail_next_cpp_allocation) {
        fail_next_cpp_allocation = false;
        throw std::bad_alloc();
    }
    if (void* memory = std::malloc(length == 0 ? 1 : length)) return memory;
    throw std::bad_alloc();
}
[[gnu::noinline]] void operator delete(void* memory) noexcept { std::free(memory); }
[[gnu::noinline]] void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

#include "../src/spark_urma_jni.cpp"

namespace {
int pins_created = 0;
int pins_deleted = 0;
int java_throws = 0;
bool fail_metrics_array = false;
bool fail_endpoint_array = false;
bool pending_vm_exception = false;
int endpoint_writes = 0;
std::string last_exception_class;
int wait_scenario = 0;
uint8_t direct_bytes[8] = {};
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

jlongArray JNICALL new_long_array(JNIEnv*, jsize) {
    if (fail_metrics_array) return nullptr;
    return reinterpret_cast<jlongArray>(direct_bytes);
}
void JNICALL set_long_array_region(JNIEnv*, jlongArray, jsize, jsize, const jlong*) {}
jbyteArray JNICALL new_byte_array(JNIEnv*, jsize) {
    return fail_endpoint_array ? nullptr : reinterpret_cast<jbyteArray>(direct_bytes);
}
void JNICALL set_byte_array_region(JNIEnv*, jbyteArray, jsize, jsize, const jbyte*) {
    ++endpoint_writes;
}
jboolean JNICALL exception_check(JNIEnv*) { return pending_vm_exception ? JNI_TRUE : JNI_FALSE; }
jobject JNICALL new_global_ref(JNIEnv*, jobject value) {
    ++pins_created;
    return value;
}
void JNICALL delete_global_ref(JNIEnv*, jobject) { ++pins_deleted; }
void* JNICALL direct_address(JNIEnv*, jobject) { return direct_bytes; }
jlong JNICALL direct_capacity(JNIEnv*, jobject) { return sizeof(direct_bytes); }
jclass JNICALL find_class(JNIEnv*, const char* name) {
    fail_next_cpp_allocation = false;
    last_exception_class = name;
    return reinterpret_cast<jclass>(direct_bytes);
}
jint JNICALL throw_new(JNIEnv*, jclass, const char*) {
    ++java_throws;
    return 0;
}
}

// Wrap the actual C++ wait boundary, not the Java exception mapper. An
// unexpected allocation/bookkeeping exception is not a terminal CQ result.
extern "C" CompletionResult __wrap__ZN10spark_urma13UrmaTransport4waitEmj(
    UrmaTransport*, uint64_t id, uint32_t) {
    switch (wait_scenario) {
        case 0: throw std::bad_alloc();
        case 1: throw std::runtime_error("unexpected internal wait bookkeeping failure");
        case 2: throw std::runtime_error("ERR_DMA_UNRESOLVED: injected unresolved provider work");
        case 3: throw std::runtime_error("ERR_UNKNOWN_REQUEST: injected unknown request");
        case 4: return {id, static_cast<int>(ErrorCode::ERR_TIMEOUT), 0, "drained timeout"};
        case 6: fail_next_cpp_allocation = true; throw std::bad_alloc();
        case 7: {
            std::runtime_error failure("unexpected wait exception with allocation unavailable during JNI mapping");
            fail_next_cpp_allocation = true;
            throw failure;
        }
        case 8: throw 42; // Unexpected non-standard C++ exception must not cross JNI.
        default: return {id, static_cast<int>(ErrorCode::ERR_COMPLETION), 0, "drained CQ failure"};
    }
}

int main() {
    JNINativeInterface_ functions = {};
    functions.NewLongArray = new_long_array;
    functions.SetLongArrayRegion = set_long_array_region;
    functions.NewByteArray = new_byte_array;
    functions.SetByteArrayRegion = set_byte_array_region;
    functions.ExceptionCheck = exception_check;
    functions.NewGlobalRef = new_global_ref;
    functions.DeleteGlobalRef = delete_global_ref;
    functions.GetDirectBufferAddress = direct_address;
    functions.GetDirectBufferCapacity = direct_capacity;
    functions.FindClass = find_class;
    functions.ThrowNew = throw_new;
    JNIEnv_ env = {};
    env.functions = &functions;

    NativeHandle owner;
    auto handle = reinterpret_cast<jlong>(&owner);
    auto buffer = reinterpret_cast<jobject>(direct_bytes);
    for (int i = 0; i < 1000; ++i) {
        auto descriptor = Java_org_scache_network_ub_UrmaNative_nativeRegisterBuffer(
            &env, nullptr, handle, buffer);
        require(descriptor == nullptr, "failed registration returned a descriptor");
        require(owner.pins.empty(), "failed registration retained a pin record");
    }
    require(pins_created == 1000, "expected registration failure pin was not created");
    require(pins_deleted == 1000, "failed registration leaked global references");
    require(java_throws == 1000, "registration failures were not reported to Java");

    fail_endpoint_array = true;
    require(Java_org_scache_network_ub_UrmaNative_nativeGetLocalEndpoint(&env, nullptr, handle) == nullptr,
            "endpoint allocation failure returned an endpoint");
    require(endpoint_writes == 0, "endpoint allocation failure wrote into a null Java array");
    fail_endpoint_array = false;

    // If metrics cannot be materialized after close, JNI intentionally keeps
    // the owner alive so Java can retry without a dangling native handle.
    auto* retry_owner = new NativeHandle();
    auto retry_handle = reinterpret_cast<jlong>(retry_owner);
    fail_metrics_array = true;
    require(Java_org_scache_network_ub_UrmaNative_nativeClose(&env, nullptr, retry_handle) == nullptr,
            "close unexpectedly returned metrics after allocation failure");
    fail_metrics_array = false;
    require(Java_org_scache_network_ub_UrmaNative_nativeClose(&env, nullptr, retry_handle) != nullptr,
            "close retry did not release the retained owner");

    for (wait_scenario = 0; wait_scenario < 6; ++wait_scenario) {
        last_exception_class.clear();
        Java_org_scache_network_ub_UrmaNative_nativeWait(&env, nullptr, handle, 1, 1);
        const char* expected = wait_scenario <= 2 ?
            "org/scache/network/ub/UrmaDmaUnresolvedException" : wait_scenario == 3 ?
            "org/scache/network/ub/UrmaProtocolException" : wait_scenario == 4 ?
            "org/scache/network/ub/UrmaTimeoutException" :
            "org/scache/network/ub/UrmaCompletionException";
        require(last_exception_class == expected,
                "unexpected native wait failure must not authorize ordinary completion cleanup");
    }
    for (wait_scenario = 6; wait_scenario <= 8; ++wait_scenario) {
        last_exception_class.clear();
        try {
            Java_org_scache_network_ub_UrmaNative_nativeWait(&env, nullptr, handle, 1, 1);
        } catch (...) {
            fail_next_cpp_allocation = false;
            throw std::runtime_error("C++ exception escaped nativeWait while mapping an unresolved failure");
        }
        require(last_exception_class == "org/scache/network/ub/UrmaDmaUnresolvedException",
                "allocation/non-standard wait exception must retain unresolved-DMA classification");
    }
    wait_scenario = 0;
    pending_vm_exception = true;
    const int previous_throws = java_throws;
    last_exception_class.clear();
    Java_org_scache_network_ub_UrmaNative_nativeWait(&env, nullptr, handle, 1, 1);
    require(java_throws == previous_throws && last_exception_class.empty(),
            "an existing JVM exception must not be replaced while reporting a wait failure");
    pending_vm_exception = false;
    const char* reasons[] = {
        "ERR_BOUNDS: injected", "ERR_INVALID_ARG: injected", "ERR_REGISTRATION: injected",
        "ERR_OWNER: injected", "ERR_CONNECTION: injected", "ERR_COMPLETION: injected",
        "std::bad_alloc"};
    for (std::size_t i = 0; i < sizeof(reasons) / sizeof(reasons[0]); ++i) {
        fail_next_cpp_allocation = true;
        throw_mapped(&env, reasons[i]);
        const char* expected = i < 4 ? "org/scache/network/ub/UrmaProtocolException" :
            i == 4 ? "org/scache/network/ub/UrmaConnectionException" :
            i == 5 ? "org/scache/network/ub/UrmaCompletionException" :
            "org/scache/network/ub/UrmaNativeException";
        require(last_exception_class == expected, "allocation-free exception mapping changed classification");
    }
    std::puts("JNI_EXCEPTION_BOUNDARY_PASS wait_cases=10 mapped_prefixes=7 (no provider initialized)");
}
