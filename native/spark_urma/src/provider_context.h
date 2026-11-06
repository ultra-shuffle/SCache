// SPDX-License-Identifier: Apache-2.0
// Opaque paired-SDK data, not a portable redefinition of provider structures.
#pragma once
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <vector>

namespace spark_urma {
constexpr size_t CONTEXT_LIMIT = 32768;
constexpr size_t CONTEXT_HEADER = 88;
inline void context_u32(std::vector<uint8_t>& out, uint32_t n) {
    for (int i = 3; i >= 0; --i) out.push_back(static_cast<uint8_t>(n >> (i * 8)));
}
inline uint32_t context_u32(const uint8_t* p) {
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | p[3];
}
inline uint32_t context_endian() {
    const uint32_t n = 1;
    return *reinterpret_cast<const uint8_t*>(&n) == 1 ? 1u : 2u;
}
inline std::vector<uint8_t> pack_context(const void* data, uint32_t bytes, uint32_t kind, uint32_t base) {
    if (!data || bytes < base || bytes > CONTEXT_LIMIT - CONTEXT_HEADER)
        throw std::runtime_error("ERR_PROTOCOL: provider context length out of range");
    std::vector<uint8_t> out;
    out.reserve(CONTEXT_HEADER + bytes);
    for (uint32_t n : {0x55524358u, 1u, kind, base, bytes, context_endian()}) context_u32(out, n);
    const char* fingerprint = SPARK_URMA_SDK_FINGERPRINT;
    out.insert(out.end(), fingerprint, fingerprint + 64);
    const auto* p = static_cast<const uint8_t*>(data);
    out.insert(out.end(), p, p + bytes);
    return out;
}
template<class T> using ContextPtr = std::unique_ptr<T, decltype(&std::free)>;
template<class T> ContextPtr<T> unpack_context(const std::vector<uint8_t>& blob, uint32_t kind) {
    if (blob.size() < CONTEXT_HEADER || blob.size() > CONTEXT_LIMIT ||
        context_u32(blob.data()) != 0x55524358u || context_u32(blob.data()+4) != 1 ||
        context_u32(blob.data()+8) != kind || context_u32(blob.data()+12) != sizeof(T) ||
        context_u32(blob.data()+16) != blob.size()-CONTEXT_HEADER ||
        context_u32(blob.data()+20) != context_endian() ||
        std::memcmp(blob.data()+24, SPARK_URMA_SDK_FINGERPRINT, 64) != 0 ||
        blob.size()-CONTEXT_HEADER < sizeof(T))
        throw std::runtime_error("ERR_PROTOCOL: incompatible/truncated provider context or SDK ABI");
    const size_t length = blob.size()-CONTEXT_HEADER;
    ContextPtr<T> result(static_cast<T*>(std::malloc(length)), &std::free);
    if (!result) throw std::bad_alloc();
    std::memcpy(result.get(), blob.data()+CONTEXT_HEADER, length);
#if SPARK_URMA_JUNE_CONTEXT_API
    if (!result->ext.flag.bs.enable || result->ext.length == 0 ||
        result->ext.length != length-sizeof(T))
        throw std::runtime_error("ERR_PROTOCOL: missing or malformed bonding context extension");
#endif
    return result;
}
} // namespace spark_urma
