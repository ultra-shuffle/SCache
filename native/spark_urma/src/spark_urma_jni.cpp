// SPDX-License-Identifier: Apache-2.0
// SparkUrma: JNI bridge between Java (org.scache.network.ub.*) and
// the C++ URMA Transport (urma_transport.h).
//
// Java classes: org.scache.network.ub.UrmaNative (native methods)
//              org.scache.network.ub.UrmaTransport (Java wrapper)

#include <jni.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <memory>
#include <new>

#include "urma_transport.h"
#include "urma_types.h"

using namespace spark_urma;

struct NativeHandle {
    UrmaTransport transport;
    struct Pin { jobject buffer; uint64_t region; };
    std::vector<Pin> pins;
};

static NativeHandle* get_handle(jlong handle) {
    if (handle == 0) throw std::runtime_error("Null native handle");
    return reinterpret_cast<NativeHandle*>(handle);
}

// ---- Helper: get transport pointer from Java long handle ----
static UrmaTransport* get_transport(jlong handle) {
    if (handle == 0) {
        throw std::runtime_error("Null native handle");
    }
    return &get_handle(handle)->transport;
}

// ---- Throw Java exception with message ----
static void throw_exception_without_cpp_allocation(
    JNIEnv* env, const char* className, const char* msg) {
    if (env->ExceptionCheck()) return;
    jclass cls = env->FindClass(className);
    if (cls) {
        env->ThrowNew(cls, msg);
    }
}

static void throw_exception(JNIEnv* env, const char* className, const std::string& msg) {
    throw_exception_without_cpp_allocation(env, className, msg.c_str());
}

static void throw_exception(JNIEnv* env, const char* className, const char* msg) {
    throw_exception_without_cpp_allocation(env, className, msg);
}

static void throw_mapped(JNIEnv* env, const char* message) {
    if (std::strncmp(message, "ERR_BOUNDS", 10) == 0 ||
        std::strncmp(message, "ERR_INVALID_ARG", 15) == 0 ||
        std::strncmp(message, "ERR_REGISTRATION", 16) == 0 ||
        std::strncmp(message, "ERR_OWNER", 9) == 0) {
        throw_exception(env, "org/scache/network/ub/UrmaProtocolException", message);
    } else if (std::strncmp(message, "ERR_CONNECTION", 14) == 0) {
        throw_exception(env, "org/scache/network/ub/UrmaConnectionException", message);
    } else if (std::strncmp(message, "ERR_COMPLETION", 14) == 0) {
        throw_exception(env, "org/scache/network/ub/UrmaCompletionException", message);
    } else {
        throw_exception(env, "org/scache/network/ub/UrmaNativeException", message);
    }
}

#define THROW_URMA(env, msg) throw_mapped(env, msg)
#define THROW_INIT(env, msg) throw_exception(env, "org/scache/network/ub/UrmaInitializationException", msg)
#define THROW_CONN(env, msg) throw_exception(env, "org/scache/network/ub/UrmaConnectionException", msg)
#define THROW_TIMEOUT(env, msg) throw_exception(env, "org/scache/network/ub/UrmaTimeoutException", msg)
#define THROW_COMPLETION(env, msg) throw_exception(env, "org/scache/network/ub/UrmaCompletionException", msg)
#define THROW_PROTOCOL(env, msg) throw_exception(env, "org/scache/network/ub/UrmaProtocolException", msg)

static void validate_direct_range(JNIEnv* env, jobject buffer, jint offset, jint length,
                                  const char* label, void** address) {
    if (buffer == nullptr) throw std::runtime_error(std::string(label) + " buffer is null");
    if (offset < 0 || length < 0) throw std::runtime_error(std::string(label) + " offset/length is negative");
    void* base = env->GetDirectBufferAddress(buffer);
    jlong capacity = env->GetDirectBufferCapacity(buffer);
    if (base == nullptr || capacity < 0) throw std::runtime_error(std::string(label) + " must be DirectByteBuffer");
    if (static_cast<jlong>(offset) > capacity || static_cast<jlong>(length) > capacity - offset)
        throw std::runtime_error(std::string(label) + " offset/length exceeds capacity");
    *address = static_cast<uint8_t*>(base) + offset;
}

static void decode_segment(JNIEnv* env, jbyteArray blob, RemoteRegion& remote) {
    if (blob == nullptr || env->GetArrayLength(blob) != 56)
        throw std::runtime_error("ERR_INVALID_ARG: complete 56-byte MR identity is required");
    uint8_t data[56];
    env->GetByteArrayRegion(blob, 0, 56, reinterpret_cast<jbyte*>(data));
    if (env->ExceptionCheck()) throw std::runtime_error("ERR_INVALID_ARG: MR identity array access failed");
    const auto number = [&data](size_t start, size_t bytes) {
        uint64_t value = 0;
        for (size_t i = 0; i < bytes; ++i) value = (value << 8) | data[start + i];
        return value;
    };
    std::memcpy(remote.segment_eid, data, 16);
    remote.segment_uasid = static_cast<uint32_t>(number(16, 4));
    remote.segment_attributes = static_cast<uint32_t>(number(20, 4));
    remote.segment_address = number(24, 8);
    remote.segment_length = number(32, 8);
    if (number(40, 4) != remote.token || number(44, 4) != 0 || number(48, 8) != remote.generation)
        throw std::runtime_error("ERR_INVALID_ARG: block/MR identity mismatch");
    remote.has_segment = true;
}

static jlongArray metrics_array(JNIEnv* env, const TransportMetrics& m) {
    jlong values[32] = {
        static_cast<jlong>(m.submitted_requests.load()), static_cast<jlong>(m.completed_requests.load()),
        static_cast<jlong>(m.failed_requests.load()), static_cast<jlong>(m.timed_out_requests.load()),
        static_cast<jlong>(m.write_bytes.load()), static_cast<jlong>(m.read_bytes.load()),
        static_cast<jlong>(m.send_bytes.load()), static_cast<jlong>(m.checksum_errors.load()),
        static_cast<jlong>(m.chunked_blocks.load()), static_cast<jlong>(m.chunks.load()),
        static_cast<jlong>(m.inflight_requests.load()), static_cast<jlong>(m.submitted_bytes.load()),
        static_cast<jlong>(m.completed_bytes.load()), static_cast<jlong>(m.late_completions.load()),
        static_cast<jlong>(m.unknown_waits.load()), static_cast<jlong>(m.write_requests.load()),
        static_cast<jlong>(m.read_requests.load()), static_cast<jlong>(m.send_requests.load()),
        static_cast<jlong>(m.logical_blocks.load()), static_cast<jlong>(m.registered_regions.load()),
        static_cast<jlong>(m.unregistered_regions.load()), static_cast<jlong>(m.active_regions.load()),
        static_cast<jlong>(m.imported_regions.load()), static_cast<jlong>(m.unimported_regions.load()),
        static_cast<jlong>(m.active_imports.load()), static_cast<jlong>(m.init_calls.load()),
        static_cast<jlong>(m.uninit_calls.load()), static_cast<jlong>(m.active_transports.load()),
        static_cast<jlong>(m.close_errors.load()), static_cast<jlong>(m.tcp_payload_bytes.load()),
        static_cast<jlong>(m.provider_outstanding_requests.load()),
        static_cast<jlong>(m.provider_drained_requests.load())};
    jlongArray result = env->NewLongArray(32);
    if (result != nullptr) env->SetLongArrayRegion(result, 0, 32, values);
    return result;
}

// ---- JNI method implementations ----

extern "C" {

JNIEXPORT jint JNICALL
Java_org_scache_network_ub_UrmaNative_nativeApiVersion(JNIEnv*, jclass) { return 3; }

// Versioned symbols prevent accidental calls into a library with the old JNI signature.
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeInitV2(
    JNIEnv* env, jclass clazz, jstring deviceName,
    jint queueDepth, jint maxChunkBytes, jboolean strict,
    jint mode, jint tpType, jint eidIndex, jint priority,
    jint requestTimeoutMs, jint drainTimeoutMs)
{
    (void)clazz;
    try {
        if (deviceName == nullptr || queueDepth <= 0 || maxChunkBytes <= 0 ||
            eidIndex < 0 || priority < 0 || priority > 15 ||
            requestTimeoutMs <= 0 || drainTimeoutMs <= 0 ||
            !((mode == 1 && tpType == 1) || (mode == 2 && tpType == 0))) {
            throw std::runtime_error("invalid nativeInit arguments");
        }
        const char* dev = env->GetStringUTFChars(deviceName, nullptr);
        if (dev == nullptr) return 0;
        TransportOptions opts;
        opts.device_name = dev;
        opts.wire_role.clear();
        opts.transport_mode = static_cast<uint32_t>(mode);
        opts.tp_type = static_cast<uint32_t>(tpType);
        opts.eid_index = static_cast<uint32_t>(eidIndex);
        opts.priority = static_cast<uint32_t>(priority);
        opts.request_timeout_ms = static_cast<uint32_t>(requestTimeoutMs);
        opts.drain_timeout_ms = static_cast<uint32_t>(drainTimeoutMs);
        opts.queue_depth = static_cast<uint32_t>(queueDepth);
        opts.max_chunk_bytes = static_cast<uint32_t>(maxChunkBytes);
        opts.strict_mode = (strict == JNI_TRUE);
        env->ReleaseStringUTFChars(deviceName, dev);

        std::unique_ptr<NativeHandle> transport(new NativeHandle());
        transport->transport.init(opts);
        return reinterpret_cast<jlong>(transport.release());
    } catch (const std::exception& e) {
        THROW_INIT(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeConnect(long handle, byte[] eid, int uasid, int jettyId,
//                           long segAddr, long segLen, int segToken)
JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeConnectV2(
    JNIEnv* env, jclass clazz, jlong handle,
    jbyteArray eid, jint uasid, jint jettyId, jint remoteMaxChunkBytes,
    jint segmentUasid, jlong segmentGeneration,
    jlong segAddr, jlong segLen, jint segToken,
    jint version, jint mode, jint tpType, jbyteArray segmentEid)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        EndpointDescriptor remote;
        remote.protocol_version = static_cast<uint32_t>(version);
        remote.transport_mode = static_cast<uint32_t>(mode);
        remote.tp_type = static_cast<uint32_t>(tpType);
        remote.uasid = static_cast<uint32_t>(uasid);
        remote.jetty_id = static_cast<uint32_t>(jettyId);
        remote.segment_address = static_cast<uint64_t>(segAddr);
        remote.segment_length = static_cast<uint64_t>(segLen);
        remote.segment_uasid = static_cast<uint32_t>(segmentUasid);
        remote.segment_token = static_cast<uint32_t>(segToken);
        remote.segment_generation = static_cast<uint64_t>(segmentGeneration);

        if (eid == nullptr || env->GetArrayLength(eid) != 16 ||
            segmentEid == nullptr || env->GetArrayLength(segmentEid) != 16 ||
            remoteMaxChunkBytes <= 0 || segLen < 0)
            throw std::runtime_error("endpoint EID must be exactly 16 bytes and segment length non-negative");
        remote.max_chunk_bytes = static_cast<uint32_t>(remoteMaxChunkBytes);
        env->GetByteArrayRegion(eid, 0, 16, reinterpret_cast<jbyte*>(remote.eid));
        if (env->ExceptionCheck()) return;
        env->GetByteArrayRegion(segmentEid, 0, 16, reinterpret_cast<jbyte*>(remote.segment_eid));
        if (env->ExceptionCheck()) return;

        transport->connect(remote);
    } catch (const std::exception& e) {
        THROW_CONN(env, e.what());
    }
}

// UrmaNative.nativeGetLocalEndpoint(long handle) -> byte[] (serialized EndpointDescriptor)
JNIEXPORT jbyteArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeGetLocalEndpoint(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        const auto& ep = transport->local_endpoint();

        EndpointDescriptor net_ep = ep;
        net_ep.to_network();
        jbyteArray result = env->NewByteArray(static_cast<jsize>(sizeof(net_ep)));
        if (result == nullptr) return nullptr;
        env->SetByteArrayRegion(result, 0, static_cast<jsize>(sizeof(net_ep)),
                                reinterpret_cast<const jbyte*>(&net_ep));
        if (env->ExceptionCheck()) return nullptr;
        return result;
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

// UrmaNative.nativeClose(long handle)
JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeClose(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        transport->close();
        jlongArray result = metrics_array(env, transport->metrics());
        // If Java allocation fails, retain a closed handle for a safe retry;
        // the Java wrapper still owns its numeric handle until this returns.
        if (result == nullptr || env->ExceptionCheck()) return nullptr;
        auto* owner = get_handle(handle);
        for (const auto& pin : owner->pins) {
            if (pin.buffer != nullptr) env->DeleteGlobalRef(pin.buffer);
        }
        delete owner;
        return result;
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

// UrmaNative.nativeRegisterBuffer(long handle, ByteBuffer directBuffer) ->
// [handle, remoteAddress, length, token, providerGeneration]
JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeRegisterBuffer(
    JNIEnv* env, jclass clazz, jlong handle, jobject buffer)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        if (buffer == nullptr) throw std::runtime_error("buffer is null");
        void* addr = env->GetDirectBufferAddress(buffer);
        jlong cap = env->GetDirectBufferCapacity(buffer);

        if (!addr || cap <= 0) throw std::runtime_error("must use non-empty ByteBuffer.allocateDirect()");

        jlongArray result = env->NewLongArray(10);
        if (result == nullptr) return nullptr;
        auto* owner = get_handle(handle);
        jobject pin = env->NewGlobalRef(buffer);
        if (pin == nullptr) return nullptr;
        try {
            owner->pins.push_back({pin, 0});
        } catch (...) {
            env->DeleteGlobalRef(pin); // no registration has been attempted
            throw;
        }
        const size_t pin_index = owner->pins.size() - 1;

        RegisteredRegion region;
        try {
            region = transport->register_region(addr, static_cast<size_t>(cap));
        } catch (...) {
            // No MR was returned, so the buffer must not remain pinned merely
            // because registration failed (for example, on MR exhaustion).
            env->DeleteGlobalRef(pin);
            owner->pins.pop_back();
            throw;
        }
        owner->pins[pin_index].region = region.handle;
        uint64_t eid_high = 0, eid_low = 0;
        for (size_t i = 0; i < 8; ++i) {
            eid_high = (eid_high << 8) | region.segment_eid[i];
            eid_low = (eid_low << 8) | region.segment_eid[i + 8];
        }
        jlong values[10] = {static_cast<jlong>(region.handle),
                           static_cast<jlong>(region.remote_address),
                           static_cast<jlong>(region.length),
                           static_cast<jlong>(region.token),
                           static_cast<jlong>(region.generation),
                           static_cast<jlong>(eid_high), static_cast<jlong>(eid_low),
                           static_cast<jlong>(region.segment_uasid),
                           static_cast<jlong>(region.segment_attributes),
                           static_cast<jlong>(region.segment_length)};
        env->SetLongArrayRegion(result, 0, 10, values);
        if (env->ExceptionCheck()) {
            // Java did not receive the descriptor. Roll the MR back, but keep
            // its global pin if the provider refuses to unregister it.
            try {
                transport->unregister_region(region.handle);
                env->DeleteGlobalRef(pin);
                owner->pins.pop_back();
            } catch (...) {
                // Preserve the pending JVM exception and the live MR's pin.
            }
            return nullptr;
        }
        return result;
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

// UrmaNative.nativeUnregisterBuffer(long handle, long regionHandle)
JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeUnregisterBuffer(
    JNIEnv* env, jclass clazz, jlong handle, jlong regionHandle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        transport->unregister_region(static_cast<uint64_t>(regionHandle));
        for (auto& pin : get_handle(handle)->pins) {
            if (pin.region == static_cast<uint64_t>(regionHandle) && pin.buffer != nullptr) {
                env->DeleteGlobalRef(pin.buffer);
                pin.buffer = nullptr;
            }
        }
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
    }
}

// UrmaNative.nativeReleaseCachedRemoteImports(long handle)
JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeReleaseCachedRemoteImports(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        transport->release_cached_remote_imports();
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
    }
}

// UrmaNative.nativeWrite(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                         long remoteGeneration,
//                         ByteBuffer source, int offset, int length) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeWriteMR(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject source, jint offset, jint length, jbyteArray segmentBytes)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, source, offset, length, "source", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint32_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);
        decode_segment(env, segmentBytes, remote);

        uint64_t req_id = transport->write(remote, addr, static_cast<size_t>(length));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeRead(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                        long remoteGeneration,
//                        ByteBuffer destination, int offset, int length) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeReadMR(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject destination, jint offset, jint length, jbyteArray segmentBytes)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, destination, offset, length, "destination", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint32_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);
        decode_segment(env, segmentBytes, remote);

        uint64_t req_id = transport->read(remote, addr, static_cast<size_t>(length));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeSend(long handle, ByteBuffer message, int offset, int length) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeSend(
    JNIEnv* env, jclass clazz, jlong handle,
    jobject message, jint offset, jint length)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, message, offset, length, "message", &addr);

        uint64_t req_id = transport->send(addr, static_cast<size_t>(length));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeWait(long handle, long requestId, int timeoutMs) -> int (0=success, -1=timeout, <0=error)
JNIEXPORT jint JNICALL
Java_org_scache_network_ub_UrmaNative_nativeWait(
    JNIEnv* env, jclass clazz, jlong handle, jlong requestId, jint timeoutMs)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        auto result = transport->wait(static_cast<uint64_t>(requestId),
                                       static_cast<uint32_t>(timeoutMs));
        if (result.status == static_cast<int>(ErrorCode::ERR_TIMEOUT)) {
            THROW_TIMEOUT(env, result.message); return result.status;
        }
        if (result.status != 0) {
            THROW_COMPLETION(env, result.message.empty() ? "URMA request failed" : result.message);
            return result.status;
        }
        return 0;
    } catch (const std::bad_alloc&) {
        // Do not allocate std::string while handling an allocation failure.
        // A pending JVM OOME is also preserved if FindClass cannot succeed.
        throw_exception_without_cpp_allocation(env,
            "org/scache/network/ub/UrmaDmaUnresolvedException",
            "ERR_DMA_UNRESOLVED: native wait allocation failed without terminal provider evidence");
        return -1;
    } catch (const std::exception& e) {
        try {
            const std::string message(e.what());
            if (message.rfind("ERR_DMA_UNRESOLVED", 0) == 0) {
                throw_exception(env, "org/scache/network/ub/UrmaDmaUnresolvedException", message);
            } else if (message.rfind("ERR_UNKNOWN_REQUEST", 0) == 0 ||
                message.rfind("ERR_INVALID_ARG", 0) == 0) {
                THROW_PROTOCOL(env, message);
            } else {
                // Only the returned CompletionResult above proves that accepted
                // WRs have drained. Unexpected wait bookkeeping/allocation errors
                // must not masquerade as a terminal CQ failure and release slices.
                throw_exception(env, "org/scache/network/ub/UrmaDmaUnresolvedException",
                    "ERR_DMA_UNRESOLVED: native wait failed without terminal provider evidence: " + message);
            }
        } catch (const std::bad_alloc&) {
            std::fprintf(stderr, "SPARK_URMA_WAIT_EXCEPTION_MAPPING_OOM detail=%s\n", e.what());
            throw_exception_without_cpp_allocation(env,
                "org/scache/network/ub/UrmaDmaUnresolvedException",
                "ERR_DMA_UNRESOLVED: allocation failed while reporting a native wait exception");
        }
        return -1;
    } catch (...) {
        throw_exception_without_cpp_allocation(env,
            "org/scache/network/ub/UrmaDmaUnresolvedException",
            "ERR_DMA_UNRESOLVED: non-standard native wait exception without terminal provider evidence");
        return -1;
    }
}

// UrmaNative.nativeWriteChunked(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                                long remoteGeneration,
//                                ByteBuffer source, int offset, int totalLength) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeWriteChunkedMR(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject source, jint offset, jint totalLength, jbyteArray segmentBytes)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, source, offset, totalLength, "source", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint32_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);
        decode_segment(env, segmentBytes, remote);

        uint64_t req_id = transport->write_chunked(remote, addr,
                                                     static_cast<size_t>(totalLength));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeReadChunked(long handle, long remoteAddr, long remoteLen, int remoteToken,
//                               long remoteGeneration,
//                               ByteBuffer dest, int offset, int totalLength) -> long requestId
JNIEXPORT jlong JNICALL
Java_org_scache_network_ub_UrmaNative_nativeReadChunkedMR(
    JNIEnv* env, jclass clazz, jlong handle,
    jlong remoteAddr, jlong remoteLen, jint remoteToken, jlong remoteGeneration,
    jobject dest, jint offset, jint totalLength, jbyteArray segmentBytes)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);

        void* addr = nullptr;
        validate_direct_range(env, dest, offset, totalLength, "destination", &addr);

        RemoteRegion remote;
        remote.remote_address = static_cast<uint64_t>(remoteAddr);
        remote.length = static_cast<uint64_t>(remoteLen);
        remote.token = static_cast<uint32_t>(remoteToken);
        remote.generation = static_cast<uint64_t>(remoteGeneration);
        decode_segment(env, segmentBytes, remote);

        uint64_t req_id = transport->read_chunked(remote, addr,
                                                    static_cast<size_t>(totalLength));
        return static_cast<jlong>(req_id);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return 0;
    }
}

// UrmaNative.nativeGetMetrics(long handle) -> long[]
JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeGetMetrics(
    JNIEnv* env, jclass clazz, jlong handle)
{
    (void)clazz;
    try {
        auto* transport = get_transport(handle);
        const auto& m = transport->metrics();

        return metrics_array(env, m);
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrmaNative_nativeShutdownProcessRuntime(
    JNIEnv* env, jclass clazz)
{
    (void)clazz;
    try {
        UrmaTransport::shutdown_process_runtime();
    } catch (const std::exception& e) {
        THROW_URMA(env, e.what());
    }
}

JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrmaNative_nativeGetProcessRuntimeMetrics(
    JNIEnv* env, jclass clazz)
{
    (void)clazz;
    const ProcessRuntimeMetrics metrics = UrmaTransport::process_runtime_metrics();
    jlong values[5] = {
        static_cast<jlong>(metrics.generation),
        static_cast<jlong>(metrics.init_calls),
        static_cast<jlong>(metrics.uninit_calls),
        static_cast<jlong>(metrics.active_transports),
        metrics.initialized ? 1 : 0};
    jlongArray result = env->NewLongArray(5);
    if (result != nullptr) env->SetLongArrayRegion(result, 0, 5, values);
    return result;
}

}  // extern "C"
