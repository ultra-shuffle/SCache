// SPDX-License-Identifier: Apache-2.0
#include "urma_transport.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <thread>

extern "C" {
#include "urma_api.h"
#include "urma_types.h"
#if SPARK_URMA_BOND_CONTEXT_API
#include "urma_ubagg.h"
#endif
}

namespace spark_urma {
namespace {
std::mutex g_process_mutex;
bool g_transport_active = false;
bool g_runtime_initialized = false;
bool g_runtime_shutdown = false;
uint64_t g_transport_epoch = 0;
uint64_t g_runtime_init_calls = 0;
uint64_t g_runtime_uninit_calls = 0;


std::string join_errors(const std::vector<std::string>& errors) {
    std::ostringstream out;
    for (size_t i = 0; i < errors.size(); ++i) {
        if (i != 0) out << "; ";
        out << errors[i];
    }
    return out.str();
}

std::string provider_failure(const char* operation, const TransportOptions& options, int saved_errno) {
    std::ostringstream out;
    out << operation << " failed device=" << options.device_name
        << " mode=" << (options.transport_mode == URMA_TM_RM ? "RM" : "RC")
        << " tp=" << (options.tp_type == URMA_CTP ? "CTP" : "RTP")
        << " eid_index=" << options.eid_index << " priority=" << options.priority
        << " queue_depth=" << options.queue_depth << " errno=" << saved_errno;
    if (saved_errno != 0) out << " (" << std::strerror(saved_errno) << ")";
    else out << " (provider did not set errno; inspect provider/kernel logs)";
    return out.str();
}
}  // namespace

#define LOCK() std::lock_guard<std::mutex> _lk(owner_mutex_)

UrmaTransport::UrmaTransport() = default;

UrmaTransport::~UrmaTransport() noexcept {
    try {
        if (state_ != TransportState::CLOSED) close();
    } catch (...) {
        // Destructors cannot report errors. Explicit close is the tested contract.
    }
}

bool UrmaTransport::range_contains(uint64_t base, uint64_t length,
                                   uint64_t address, uint64_t bytes) {
    if (address < base || bytes > length) return false;
    return address - base <= length - bytes;
}

void UrmaTransport::init(const TransportOptions& options) {
    LOCK();
    if (state_ != TransportState::NEW) throw std::runtime_error("ERR_INIT: already initialized");
    if (options.device_name.empty() || options.queue_depth == 0 ||
        options.max_chunk_bytes == 0) {
        throw std::runtime_error("ERR_INVALID_ARG: invalid device/queue/maxChunkBytes");
    }

    if (!((options.transport_mode == URMA_TM_RM && options.tp_type == URMA_CTP) ||
          (options.transport_mode == URMA_TM_RC && options.tp_type == URMA_RTP)) ||
        options.priority > 15 || options.request_timeout_ms == 0 || options.drain_timeout_ms == 0) {
        throw std::runtime_error("ERR_INVALID_ARG: require RM/CTP or RC/RTP, valid priority and timeouts");
    }

    options_ = options;
    owner_tid_ = std::this_thread::get_id();
    try {
        {
            std::lock_guard<std::mutex> process_lock(g_process_mutex);
            if (g_transport_active) {
                throw std::runtime_error("ERR_BUSY: one active UrmaTransport is allowed per process");
            }
            if (g_runtime_shutdown) {
                throw std::runtime_error("ERR_CLOSED: process URMA runtime was shut down");
            }
            if (!g_runtime_initialized) {
                urma_init_attr_t init_attr = {};
                if (urma_init(&init_attr) != URMA_SUCCESS) {
                    throw std::runtime_error("urma_init failed");
                }
                g_runtime_initialized = true;
                ++g_runtime_init_calls;
                metrics_.init_calls.fetch_add(1);
            }
            g_transport_active = true;
            owns_runtime_slot_ = true;
            owner_epoch_ = ++g_transport_epoch;
        }
        metrics_.active_transports.fetch_add(1);

        urma_device_t* device = urma_get_device_by_name(
            const_cast<char*>(options_.device_name.c_str()));
        if (device == nullptr) throw std::runtime_error("device not found: " + options_.device_name);
        // Follow the frozen official urma_sample.c device naming convention.
        // multi_path is an UBAGG queue flag, not a synonym for CTP; physical
        // UDMA RM/CTP must leave it clear. Capability bits below still gate TP.
        const bool bonding_device = std::strncmp(device->name, "bonding", 7) == 0;
        const bool multi_path = bonding_device && options_.transport_mode == URMA_TM_RM;
        urma_device_attr_t device_attr = {};
        if (urma_query_device(device, &device_attr) != URMA_SUCCESS) {
            throw std::runtime_error("urma_query_device failed");
        }
        const auto& cap = device_attr.dev_cap;
        std::fprintf(stderr, "SPARK_URMA_PATH device=%s bonding_name=%u multi_path=%u "
            "reported_rm_multi_path=%u\n", options_.device_name.c_str(),
            static_cast<unsigned>(bonding_device), static_cast<unsigned>(multi_path),
            static_cast<unsigned>(cap.tp_feature.bs.rm_multi_path));
        std::fprintf(stderr,
            "SPARK_URMA_MODE device=%s mode=%u tp=%u eid_index=%u priority=%u "
            "mode_mask=%u rc_rtp=%u rm_ctp=%u\n",
            options_.device_name.c_str(), options_.transport_mode, options_.tp_type,
            options_.eid_index, options_.priority, static_cast<unsigned>(cap.trans_mode),
            static_cast<unsigned>(cap.rc_tp_cap.bs.rtp), static_cast<unsigned>(cap.rm_tp_cap.bs.ctp));
        const bool supported = options_.transport_mode == URMA_TM_RM ?
            cap.rm_tp_cap.bs.ctp : cap.rc_tp_cap.bs.rtp;
        if (!(cap.trans_mode & options_.transport_mode) || !supported) {
            throw std::runtime_error("ERR_CAPABILITY: selected device does not advertise requested mode/TP combination");
        }
        // One JFC receives completions from both the JFS and shared JFR. UMDK's Jetty
        // contract requires capacity for their combined depths, even if this application
        // normally issues only one-sided READs.
        if (options_.queue_depth > cap.max_jfc_depth / 2 ||
            options_.queue_depth > cap.max_jfs_depth || options_.queue_depth > cap.max_jfr_depth) {
            throw std::runtime_error("ERR_CAPABILITY: JFC needs twice the configured JFS/JFR depth");
        }
        // The official perftest reserves one additional SGE for a bonding
        // device's internal path metadata, even when each application WR has
        // only one payload SGE. Physical UDMA keeps the one-SGE configuration.
        const uint32_t queue_sge = bonding_device ? 2u : 1u;
        if (cap.max_jfs_sge < queue_sge || cap.max_jfs_rsge < 1 ||
            cap.max_jfr_sge < queue_sge)
            throw std::runtime_error("ERR_CAPABILITY: provider lacks required queue SGE capacity");
        // Protocol 2 negotiates ONE raw limit for the generic SEND/READ/WRITE
        // API. The device exposes distinct limits, but dropping max_msg here
        // would lose the peer's SEND bound. SCache uses one-sided operations;
        // lifting this conservative cap needs operation-specific negotiation.
        const uint64_t provider_limit = std::min<uint64_t>(
            device_attr.dev_cap.max_msg_size,
            std::min<uint64_t>(device_attr.dev_cap.max_read_size,
                               device_attr.dev_cap.max_write_size));
        if (provider_limit == 0) {
            throw std::runtime_error(
                "provider advertised a zero READ/WRITE/message limit");
        }
        provider_max_raw_operation_bytes_ = static_cast<uint32_t>(
            std::min<uint64_t>(
                provider_limit, std::numeric_limits<uint32_t>::max()));
        local_max_raw_operation_bytes_ =
            std::min<uint32_t>(
                options_.max_chunk_bytes, provider_max_raw_operation_bytes_);
        effective_max_raw_operation_bytes_ =
            local_max_raw_operation_bytes_;
        if (local_max_raw_operation_bytes_ == 0) {
            throw std::runtime_error(
                "negotiated local raw operation limit is zero");
        }
        std::fprintf(stderr,
            "SPARK_URMA_CAPABILITY configuredMax=%u providerMax=%u "
            "localMax=%u sendMessageMax=%llu readMax=%u writeMax=%u portCount=%u wireMtuEnum=%d\n",
            options_.max_chunk_bytes, provider_max_raw_operation_bytes_,
            local_max_raw_operation_bytes_,
            static_cast<unsigned long long>(cap.max_msg_size),
            static_cast<unsigned>(cap.max_read_size), static_cast<unsigned>(cap.max_write_size),
            static_cast<unsigned>(device_attr.port_cnt),
            device_attr.port_cnt == 0 ? -1 :
                static_cast<int>(device_attr.port_attr[0].active_mtu));
        uint32_t eid_count = 0;
        urma_eid_info_t* eids = urma_get_eid_list(device, &eid_count);
        bool eid_found = false;
        if (eids != nullptr) {
            for (uint32_t i = 0; i < eid_count; ++i) {
                if (eids[i].eid_index == options_.eid_index) eid_found = true;
            }
            urma_free_eid_list(eids);
        }
        if (!eid_found) throw std::runtime_error("ERR_CAPABILITY: configured EID index is unavailable");
        errno = 0;
        ctx_ = urma_create_context(device, options_.eid_index);
        if (ctx_ == nullptr) throw std::runtime_error(provider_failure("urma_create_context", options_, errno));

        if (multi_path) {
#if SPARK_URMA_BOND_CONTEXT_API
            // Match official B081/June-24 perftest's default aggregate context.
            // This must precede every JFC/JFR/Jetty creation: a context in use
            // cannot safely change its physical member selection. The paired
            // SDK enum/structure define this command, never a guessed opcode.
            bondp_set_bonding_mode_in_t mode = {};
            mode.bonding_mode = BONDP_BONDING_MODE_STANDALONE;
            mode.bonding_level = BONDP_BONDING_LEVEL_IODIE;
            urma_user_ctl_in_t input = {};
            input.addr = reinterpret_cast<uint64_t>(&mode);
            input.len = sizeof(mode);
            input.opcode = BONDP_USER_CTL_SET_BONDING_MODE;
            urma_user_ctl_out_t output = {};
            errno = 0;
            const auto status = urma_user_ctl(ctx_, &input, &output);
            if (status != URMA_SUCCESS) {
                const int saved_errno = errno;
                throw std::runtime_error(provider_failure(
                    "urma_user_ctl(standalone,IODIE)", options_, saved_errno) +
                    " status=" + std::to_string(static_cast<int>(status)));
            }
            std::fprintf(stderr, "SPARK_URMA_BOND_CONTEXT mode=standalone level=iodie "
                "source=paired-sdk-user-ctl\n");
#else
            // Legacy SDKs need no new private ABI invented by this project.
            // They remain on their provider default, explicitly marked unknown
            // rather than falsely claiming parity with a different perftest.
            std::fprintf(stderr, "SPARK_URMA_BOND_CONTEXT mode=provider-default "
                "level=unverified reason=paired-sdk-explicit-api-unavailable\n");
#endif
        }

        urma_jfc_cfg_t jfc_cfg = {};
        jfc_cfg.depth = options_.queue_depth * 2;
        errno = 0;
        jfc_ = urma_create_jfc(ctx_, &jfc_cfg);
        if (jfc_ == nullptr) throw std::runtime_error(provider_failure("urma_create_jfc", options_, errno));

        // Hardware uses the Jetty data path, matching the imported endpoint type.
        urma_jfs_cfg_t jfs_cfg = {};
        jfs_cfg.depth = options_.queue_depth;
        jfs_cfg.trans_mode = static_cast<urma_transport_mode_t>(options_.transport_mode);
        jfs_cfg.jfc = jfc_;
        jfs_cfg.priority = static_cast<uint8_t>(options_.priority);
        jfs_cfg.max_sge = queue_sge;
        jfs_cfg.max_rsge = 1;
        // Match the UMDK 25.12 sample/perftest retry defaults rather than
        // relying on zero-initialized provider-specific queue behavior.
        jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        jfs_cfg.flag.bs.multi_path = multi_path;

        urma_jfr_cfg_t jfr_cfg = {};
        jfr_cfg.depth = options_.queue_depth;
        jfr_cfg.max_sge = queue_sge;
        jfr_cfg.trans_mode = jfs_cfg.trans_mode;
        jfr_cfg.jfc = jfc_;
        jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg.token_value.token = 0xDEADBEEF;
        errno = 0;
        jfr_ = urma_create_jfr(ctx_, &jfr_cfg);
        if (jfr_ == nullptr) throw std::runtime_error(provider_failure("urma_create_jfr", options_, errno));

        urma_jetty_cfg_t jetty_cfg = {};
        jetty_cfg.jfs_cfg = jfs_cfg;
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.shared.jfr = jfr_;
        jetty_cfg.shared.jfc = jfc_;
        errno = 0;
        jetty_ = urma_create_jetty(ctx_, &jetty_cfg);
        if (jetty_ == nullptr) throw std::runtime_error(provider_failure("urma_create_jetty", options_, errno));

        local_ep_.protocol_version = PROTOCOL_VERSION;
        local_ep_.uasid = jetty_->jetty_id.uasid;
        local_ep_.jetty_id = jetty_->jetty_id.id;
        local_ep_.max_chunk_bytes = local_max_raw_operation_bytes_;
        local_ep_.transport_mode = options_.transport_mode;
        local_ep_.tp_type = options_.tp_type;
        std::memcpy(local_ep_.eid, &ctx_->eid, sizeof(local_ep_.eid));
        std::fprintf(stderr, "SPARK_URMA_INIT eid=%02x%02x%02x%02x... jetty_uasid=%u jetty_id=%u ctx_eid_index=%u\n",
                     local_ep_.eid[0], local_ep_.eid[1], local_ep_.eid[2], local_ep_.eid[3],
                     local_ep_.uasid, local_ep_.jetty_id, ctx_->eid_index);
        state_ = TransportState::INITIALIZED;
    } catch (const std::exception& original) {
        std::vector<std::string> errors;
        cleanup_locked(errors);
        throw std::runtime_error(std::string("ERR_INIT: ") + original.what() +
                                 (errors.empty() ? "" : "; cleanup: " + join_errors(errors)));
    }
}

void UrmaTransport::shutdown_process_runtime() {
    std::lock_guard<std::mutex> process_lock(g_process_mutex);
    if (g_transport_active) {
        throw std::runtime_error("ERR_BUSY: cannot shutdown process runtime with an active transport");
    }
    if (!g_runtime_initialized) return;
    if (urma_uninit() != URMA_SUCCESS) {
        throw std::runtime_error("ERR_CLOSED: urma_uninit failed");
    }
    g_runtime_initialized = false;
    g_runtime_shutdown = true;
    ++g_runtime_uninit_calls;
}

ProcessRuntimeMetrics UrmaTransport::process_runtime_metrics() {
    std::lock_guard<std::mutex> process_lock(g_process_mutex);
    ProcessRuntimeMetrics metrics;
    metrics.generation = g_transport_epoch;
    metrics.init_calls = g_runtime_init_calls;
    metrics.uninit_calls = g_runtime_uninit_calls;
    metrics.active_transports = g_transport_active ? 1 : 0;
    metrics.initialized = g_runtime_initialized;
    return metrics;
}

void UrmaTransport::connect(const EndpointDescriptor& remote) {
    LOCK();
    if (remote.protocol_version != PROTOCOL_VERSION || remote.max_chunk_bytes == 0 ||
        remote.transport_mode != options_.transport_mode || remote.tp_type != options_.tp_type) {
        throw std::runtime_error("ERR_PROTOCOL: incompatible endpoint version/mode/TP/limit");
    }
    if (state_ == TransportState::CONNECTED) {
        for (const auto& item : requests_) {
            if (provider_owned_locked(item.second))
                throw std::runtime_error("ERR_BUSY: cannot reconnect while a request is building or provider-owned");
        }
        if (metrics_.inflight_requests.load() != 0 ||
            metrics_.provider_outstanding_requests.load() != 0) {
            throw std::runtime_error("ERR_BUSY: cannot reconnect with provider requests in flight");
        }
        for (auto& imported : imported_regions_) {
            if (imported.segment != nullptr && urma_unimport_seg(imported.segment) != URMA_SUCCESS) {
                state_ = TransportState::CLOSING;
                throw std::runtime_error("ERR_CONNECTION: failed to unimport old remote segment");
            }
            if (imported.segment != nullptr) {
                metrics_.unimported_regions.fetch_add(1);
                metrics_.active_imports.fetch_sub(1);
                imported.segment = nullptr;
            }
        }
        imported_regions_.clear();
        if (remote_jetty_ != nullptr) {
            if (remote_bound_ && urma_unbind_jetty(jetty_) != URMA_SUCCESS) {
                state_ = TransportState::CLOSING;
                throw std::runtime_error("ERR_CONNECTION: failed to unbind old peer");
            }
            remote_bound_ = false;
            if (urma_unimport_jetty(remote_jetty_) != URMA_SUCCESS) {
                state_ = TransportState::CLOSING;
                throw std::runtime_error("ERR_CONNECTION: failed to unimport old peer");
            }
            remote_jetty_ = nullptr;
        }
        state_ = TransportState::INITIALIZED;
    } else if (state_ != TransportState::INITIALIZED) {
        throw std::runtime_error("ERR_CONNECTION: transport is not initialized");
    }
    if (remote.protocol_version != PROTOCOL_VERSION || remote.max_chunk_bytes == 0) {
        throw std::runtime_error("ERR_PROTOCOL: incompatible endpoint descriptor");
    }
    remote_ep_ = remote;
    effective_max_raw_operation_bytes_ =
        std::min<uint32_t>(
            local_max_raw_operation_bytes_, remote.max_chunk_bytes);
    if (effective_max_raw_operation_bytes_ == 0) {
        throw std::runtime_error(
            "ERR_PROTOCOL: negotiated raw operation limit is zero");
    }
    std::fprintf(stderr,
        "SPARK_URMA_NEGOTIATED localMax=%u peerMax=%u effectiveMax=%u\n",
        local_max_raw_operation_bytes_, remote.max_chunk_bytes,
        effective_max_raw_operation_bytes_);
    std::fprintf(stderr, "SPARK_URMA_CONNECT peer_jetty=%u peer_uasid=%u mode=%u tp=%u\n",
        remote.jetty_id, remote.uasid, remote.transport_mode, remote.tp_type);
    urma_rjetty_t remote_jetty = {};
    std::memcpy(&remote_jetty.jetty_id.eid, remote.eid, sizeof(remote.eid));
    remote_jetty.jetty_id.uasid = remote.uasid;
    remote_jetty.jetty_id.id = remote.jetty_id;
    remote_jetty.trans_mode = static_cast<urma_transport_mode_t>(options_.transport_mode);
    remote_jetty.tp_type = static_cast<urma_tp_type_t>(options_.tp_type);
    remote_jetty.type = URMA_JETTY;
    urma_token_t token = {.token = 0xDEADBEEF};
    errno = 0;
    remote_jetty_ = urma_import_jetty(ctx_, &remote_jetty, &token);
    if (remote_jetty_ == nullptr) throw std::runtime_error(
        "ERR_CONNECTION: " + provider_failure("urma_import_jetty", options_, errno));
    if (options_.transport_mode == URMA_TM_RC && urma_bind_jetty(jetty_, remote_jetty_) != URMA_SUCCESS) {
        if (urma_unimport_jetty(remote_jetty_) != URMA_SUCCESS) {
            state_ = TransportState::CLOSING;
            throw std::runtime_error("ERR_CONNECTION: bind failed and imported peer cleanup failed; close required");
        }
        remote_jetty_ = nullptr;
        throw std::runtime_error("ERR_CONNECTION: urma_bind_jetty failed");
    }
    remote_bound_ = options_.transport_mode == URMA_TM_RC;
    state_ = TransportState::CONNECTED;
}

RegisteredRegion UrmaTransport::register_region(void* address, size_t length) {
    LOCK();
    if (state_ != TransportState::INITIALIZED && state_ != TransportState::CONNECTED) {
        throw std::runtime_error("ERR_CLOSED: registration requires an open transport");
    }
    if (address == nullptr || length == 0) {
        throw std::runtime_error("ERR_INVALID_ARG: registration address/length is invalid");
    }
    const uint64_t base = reinterpret_cast<uint64_t>(address);
    if (base > std::numeric_limits<uint64_t>::max() - length) {
        throw std::runtime_error("ERR_BOUNDS: registration range overflow");
    }
    for (const auto& item : registered_regions_) {
        const RegisteredRegion& existing = item.second.region;
        const uint64_t existing_base = reinterpret_cast<uint64_t>(existing.address);
        // Both intervals use CPU VA, not provider UBVA. Subtraction avoids
        // overflowing an end address and also detects partial intersections.
        const bool overlap = base >= existing_base ?
            base - existing_base < existing.length : existing_base - base < length;
        if (overlap) {
            throw std::runtime_error("ERR_REGISTRATION: overlapping registration");
        }
    }

    const uint64_t region_id = next_region_handle_++;
    LocalRegionState initial;
    initial.registered = false;
    initial.region.handle = region_id;
    initial.region.address = address;
    initial.region.length = length;
    auto& stored = registered_regions_.emplace(region_id, initial).first->second;
    errno = 0;
    urma_token_id_t* token_id = urma_alloc_token_id(ctx_);
    if (token_id == nullptr) {
        const int saved_errno = errno;
        registered_regions_.erase(region_id);
        throw std::runtime_error("ERR_REGISTRATION: " +
            provider_failure("urma_alloc_token_id", options_, saved_errno));
    }
    stored.token_id = token_id;
    urma_seg_cfg_t cfg = {};
    cfg.va = base;
    cfg.len = length;
    cfg.token_id = token_id;
    cfg.token_value.token = 0xDEADBEEF;
    cfg.flag.bs.token_id_valid = URMA_TOKEN_ID_VALID;
    cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    errno = 0;
    urma_target_seg_t* segment = urma_register_seg(ctx_, &cfg);
    if (segment == nullptr) {
        // Cleanup is a second provider call and may overwrite errno. Preserve
        // the registration error, especially for permissions/memlock failures.
        const std::string reason = provider_failure("urma_register_seg", options_, errno) +
            " length=" + std::to_string(length);
        if (urma_free_token_id(token_id) == URMA_SUCCESS) {
            registered_regions_.erase(region_id);
        } else {
            state_ = TransportState::CLOSING;
            throw std::runtime_error("ERR_REGISTRATION: " + reason +
                "; token cleanup also failed; close required");
        }
        throw std::runtime_error("ERR_REGISTRATION: " + reason);
    }
    stored.segment = segment;
    stored.registered = true;
    metrics_.registered_regions.fetch_add(1);
    metrics_.active_regions.fetch_add(1);

    RegisteredRegion region;
    region.handle = region_id;
    region.address = address;
    region.length = length;
    region.remote_address = segment->seg.ubva.va;
    region.token = segment->seg.token_id;
    std::memcpy(region.segment_eid, &segment->seg.ubva.eid, sizeof(region.segment_eid));
    region.segment_uasid = segment->seg.ubva.uasid;
    region.segment_attributes = segment->seg.attr.value;
    region.segment_length = segment->seg.len;
    // Software registration identity for cache invalidation, not a provider
    // cookie or a substitute for the peer's block generation/lease checks.
    region.generation = region.handle;
    local_ep_.segment_uasid = segment->seg.ubva.uasid;
    std::memcpy(local_ep_.segment_eid, &segment->seg.ubva.eid, sizeof(local_ep_.segment_eid));
    local_ep_.segment_generation = region.generation;
    region.owner_epoch = owner_epoch_;
    stored.region = region;

    // Endpoint identity and region identity are logically separate. These fields are only
    // the diagnostic default region; each operation imports its supplied RemoteRegion.
    local_ep_.segment_address = region.remote_address;
    local_ep_.segment_length = region.length;
    local_ep_.segment_token = static_cast<uint32_t>(region.token);
    return region;
}

RegisteredRegion UrmaTransport::registered_region(uint64_t handle) const {
    LOCK();
    auto it = registered_regions_.find(handle);
    if (it == registered_regions_.end() || !it->second.registered) {
        throw std::runtime_error("ERR_REGISTRATION: unknown or unregistered handle");
    }
    return it->second.region;
}

void UrmaTransport::unregister_region(uint64_t handle) {
    LOCK();
    auto it = registered_regions_.find(handle);
    if (it == registered_regions_.end()) {
        throw std::runtime_error("ERR_REGISTRATION: unknown or already unregistered handle");
    }
    if (it->second.inflight != 0) {
        throw std::runtime_error("ERR_BUSY: region has in-flight provider requests");
    }
    if (it->second.segment != nullptr) {
        if (urma_unregister_seg(it->second.segment) != URMA_SUCCESS) {
            throw std::runtime_error("ERR_REGISTRATION: urma_unregister_seg failed");
        }
        it->second.segment = nullptr;
        it->second.registered = false;
        metrics_.unregistered_regions.fetch_add(1);
        metrics_.active_regions.fetch_sub(1);
    }
    if (it->second.token_id != nullptr && urma_free_token_id(it->second.token_id) != URMA_SUCCESS) {
        throw std::runtime_error("ERR_REGISTRATION: urma_free_token_id failed");
    }
    it->second.registered = false;
    it->second.segment = nullptr;
    it->second.token_id = nullptr;
    registered_regions_.erase(it);
}

void UrmaTransport::release_cached_remote_imports() {
    LOCK();
    // A provider CQE can arrive after the API-level request has become terminal.
    // Drain those late completions before unimporting peer segments; otherwise cleanup
    // races the provider and returns ERR_BUSY even though transport inflight is zero.
    const auto drain_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::min<uint32_t>(std::max<uint32_t>(1, options_.request_timeout_ms), 5000));
    while (metrics_.provider_outstanding_requests.load() != 0 &&
           std::chrono::steady_clock::now() < drain_deadline) {
        poll_once_locked();
        if (metrics_.provider_outstanding_requests.load() != 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    }
    if (metrics_.provider_outstanding_requests.load() != 0) {
        throw std::runtime_error(
            "ERR_BUSY: provider requests are outstanding while releasing remote imports");
    }
    for (const auto& imported : imported_regions_) {
        if (imported.inflight != 0) {
            throw std::runtime_error(
                "ERR_BUSY: remote import has in-flight provider references");
        }
    }
    for (auto it = imported_regions_.begin(); it != imported_regions_.end();) {
        if (it->segment != nullptr && urma_unimport_seg(it->segment) != URMA_SUCCESS) {
            throw std::runtime_error(
                "ERR_CONNECTION: failed to release cached remote segment import");
        }
        if (it->segment != nullptr) {
            metrics_.unimported_regions.fetch_add(1);
            metrics_.active_imports.fetch_sub(1);
        }
        it = imported_regions_.erase(it);
    }
}

UrmaTransport::LocalRegionState& UrmaTransport::find_local_region_locked(
    const void* address, size_t length) {
    const uint64_t addr = reinterpret_cast<uint64_t>(address);
    for (auto& item : registered_regions_) {
        auto& state = item.second;
        if (state.registered && range_contains(reinterpret_cast<uint64_t>(state.region.address),
                                               state.region.length, addr, length)) {
            return state;
        }
    }
    throw std::runtime_error("ERR_REGISTRATION: local range is not registered by this transport");
}

UrmaTransport::ImportedRegionState& UrmaTransport::find_or_import_remote_locked(
    const RemoteRegion& remote) {
    if (!remote.has_segment || remote.segment_address == 0 || remote.segment_length == 0 ||
        remote.segment_length > UINT64_MAX - remote.segment_address ||
        remote.remote_address < remote.segment_address || remote.length == 0 ||
        remote.remote_address - remote.segment_address > remote.segment_length ||
        remote.length > remote.segment_length - (remote.remote_address - remote.segment_address) ||
        remote.token > UINT32_MAX || remote.generation == 0) {
        throw std::runtime_error("ERR_BOUNDS: invalid remote region descriptor");
    }
    for (auto& imported : imported_regions_) {
        if (imported.region.segment_address == remote.segment_address &&
            imported.region.segment_length == remote.segment_length &&
            imported.region.segment_uasid == remote.segment_uasid &&
            imported.region.segment_attributes == remote.segment_attributes &&
            std::memcmp(imported.region.segment_eid, remote.segment_eid, 16) == 0 &&
            imported.region.token == remote.token &&
            imported.region.generation == remote.generation) {
            imported.last_used = ++import_clock_;
            return imported;
        }
    }
    if (imported_regions_.size() >= MAX_REMOTE_IMPORTS) {
        auto victim = imported_regions_.end();
        for (auto it = imported_regions_.begin(); it != imported_regions_.end(); ++it) {
            if (it->inflight == 0 &&
                (victim == imported_regions_.end() || it->last_used < victim->last_used)) {
                victim = it;
            }
        }
        if (victim == imported_regions_.end()) {
            throw std::runtime_error("ERR_BUSY: remote import cache is full with in-flight regions");
        }
        if (urma_unimport_seg(victim->segment) != URMA_SUCCESS) {
            throw std::runtime_error("ERR_CONNECTION: failed to evict remote segment import");
        }
        metrics_.unimported_regions.fetch_add(1);
        metrics_.active_imports.fetch_sub(1);
        imported_regions_.erase(victim);
    }
    urma_seg_t segment = {};
    std::memcpy(&segment.ubva.eid, remote.segment_eid, sizeof(remote.segment_eid));
    segment.ubva.uasid = remote.segment_uasid;
    segment.ubva.va = remote.segment_address;
    segment.len = remote.segment_length;
    segment.attr.value = remote.segment_attributes;
    segment.token_id = static_cast<uint32_t>(remote.token);
    urma_token_t token = {.token = 0xDEADBEEF};
    urma_import_seg_flag_t import_flags = {};
    import_flags.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE;
    import_flags.bs.mapping = URMA_SEG_NOMAP;
    // Public API argument four is a mapping address, never a generation.
    // Allocate cache storage before acquiring a provider resource.
    imported_regions_.reserve(imported_regions_.size() + 1);
    errno = 0;
    urma_target_seg_t* imported = urma_import_seg(
        ctx_, &segment, &token, 0, import_flags);
    const int import_errno = errno;
    std::fprintf(stderr, "SPARK_URMA_IMPORT remote_seg_uasid=%u remote_eid=%02x%02x%02x%02x... addr=%#llx token=%llu seggen=%llu\n",
                 remote.segment_uasid, remote.segment_eid[0], remote.segment_eid[1], remote.segment_eid[2], remote.segment_eid[3],
                 (unsigned long long)remote.remote_address, (unsigned long long)remote.token, (unsigned long long)remote.generation);
    if (imported == nullptr) throw std::runtime_error("ERR_CONNECTION: " +
        provider_failure("urma_import_seg", options_, import_errno) +
        " segment_length=" + std::to_string(remote.segment_length) +
        " segment_uasid=" + std::to_string(remote.segment_uasid));
    imported_regions_.push_back({remote, imported, 0, ++import_clock_});
    metrics_.imported_regions.fetch_add(1);
    metrics_.active_imports.fetch_add(1);
    return imported_regions_.back();
}

void UrmaTransport::wait_for_post_capacity_locked(
    std::chrono::steady_clock::time_point deadline) {
    if (options_.queue_depth == 0)
        throw std::runtime_error("ERR_INVALID_ARG: zero submission queue depth");
    // Follow official perftest: submitted - completed must stay below the
    // configured JFS depth. UDMA reports a full SQ as ENOMEM, not EAGAIN;
    // prevent normal overfill instead of retrying arbitrary allocation errors.
    while (metrics_.provider_outstanding_requests.load() >= options_.queue_depth) {
        poll_once_locked();
        if (metrics_.provider_outstanding_requests.load() < options_.queue_depth) return;
        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error("ERR_TIMEOUT: submission capacity deadline exceeded queue_depth=" +
                std::to_string(options_.queue_depth) + " provider_outstanding=" +
                std::to_string(metrics_.provider_outstanding_requests.load()));
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

uint64_t UrmaTransport::post_rw_locked(bool write_op, const RemoteRegion& remote,
                                       void* local, size_t length, size_t remote_offset) {
    if (state_ != TransportState::CONNECTED) throw std::runtime_error("ERR_CONNECTION: not connected");
    trim_terminal_cache_locked();
    if (length == 0 ||
        length > effective_max_raw_operation_bytes_ ||
        length > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("ERR_INVALID_ARG: raw request length must be 1..maxChunkBytes");
    }
    if (remote_offset > remote.length || length > remote.length - remote_offset) {
        throw std::runtime_error("ERR_BOUNDS: remote offset/length exceeds descriptor");
    }
    if (remote.remote_address > UINT64_MAX - remote_offset) {
        throw std::runtime_error("ERR_BOUNDS: remote address overflow");
    }
    LocalRegionState& local_region = find_local_region_locked(local, length);
    ImportedRegionState& imported = find_or_import_remote_locked(remote);
    const auto post_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::max<uint32_t>(1, options_.request_timeout_ms));
    wait_for_post_capacity_locked(post_deadline);
    const uint64_t request_id = next_request_id();

    // Allocate bookkeeping before handing the WR to the provider. Allocation
    // failure after successful submission would lose ownership of a live DMA.
    RequestRecord pending{};
    pending.request_id = request_id;
    pending.is_write = write_op;
    pending.is_read = !write_op;
    pending.bytes = length;
    pending.state = RequestState::POSTED;
    pending.local_region_handle = local_region.region.handle;
    pending.remote_segment = imported.segment;
    pending.message = "provider rejected submission without identifying the unaccepted WR";
    requests_.emplace(request_id, pending);

    urma_sge_t local_sge = {};
    local_sge.addr = reinterpret_cast<uint64_t>(local);
    local_sge.len = static_cast<uint32_t>(length);
    local_sge.tseg = local_region.segment;
    urma_sge_t remote_sge = {};
    remote_sge.addr = remote.remote_address + remote_offset;
    remote_sge.len = static_cast<uint32_t>(length);
    remote_sge.tseg = imported.segment;
    urma_sg_t source = {.sge = write_op ? &local_sge : &remote_sge, .num_sge = 1};
    urma_sg_t destination = {.sge = write_op ? &remote_sge : &local_sge, .num_sge = 1};
    urma_jfs_wr_t wr = {};
    wr.flag.bs.complete_enable = 1;
    wr.opcode = write_op ? URMA_OPC_WRITE : URMA_OPC_READ;
    wr.tjetty = remote_jetty_;
    wr.user_ctx = request_id;
    wr.rw.src = source;
    wr.rw.dst = destination;
    urma_jfs_wr_t* bad = nullptr;
    urma_status_t post_status = URMA_EAGAIN;
    for (uint64_t spins = 0; ; ++spins) {
        bad = nullptr;
        post_status = urma_post_jetty_send_wr(jetty_, &wr, &bad);
        if (post_status == URMA_SUCCESS) break;
        // Retry only if the provider explicitly identifies this sole WR as
        // unaccepted. A missing/foreign bad_wr leaves ownership ambiguous.
        if (post_status != URMA_EAGAIN || bad != &wr) break;
        // Q8 exposes a finite provider ring. Drain completions while the
        // logical block fan-out is being posted.  A fixed spin count made a
        // transiently full ring surface as a false READ/WRITE post failure
        // under the normal Spark fetch concurrency; use the transport
        // request deadline and yield periodically instead.
        try {
            poll_once_locked();
        } catch (...) {
            requests_.erase(request_id); // this WR has not been accepted
            throw;
        }
        if (std::chrono::steady_clock::now() >= post_deadline) break;
        if ((spins & 0x3ffu) == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        } else {
            std::this_thread::yield();
        }
    }
    if (post_status != URMA_SUCCESS) {
        if (bad != &wr) {
            auto& unresolved = requests_.at(request_id);
            unresolved.provider_outstanding = true;
            unresolved.state = RequestState::CANCELLED_PROVIDER_OUTSTANDING;
            unresolved.status = static_cast<int>(ErrorCode::ERR_COMPLETION);
            ++local_region.inflight;
            ++imported.inflight;
            metrics_.record_provider_post(request_id);
            // The provider did not identify this WR as unaccepted. Account it as
            // a submitted attempt before recording its API failure; otherwise
            // record_completion can decrement an unrelated in-flight request.
            if (write_op) metrics_.record_write(request_id, length);
            else metrics_.record_read(request_id, length);
            metrics_.record_completion(request_id, false);
            state_ = TransportState::CLOSING;
            // Return the tracked child so block callers attach it before wait.
            // Never report an ordinary submission failure and free its memory.
            return request_id;
        }
        requests_.erase(request_id);
        std::fprintf(stderr, "SPARK_URMA_POST_FAIL op=%s status=%d bad=%p opcode=%d flag=%#x\n",
                     write_op ? "WRITE" : "READ", static_cast<int>(post_status),
                     static_cast<void*>(bad), wr.opcode, wr.flag.value);
        throw std::runtime_error(write_op ? "ERR_COMPLETION: WRITE post failed"
                                          : "ERR_COMPLETION: READ post failed");
    }

    auto& request = requests_.at(request_id);
    request.message.clear();
    request.provider_outstanding = true;
    ++local_region.inflight;
    ++imported.inflight;
    metrics_.record_provider_post(request_id);
    if (write_op) metrics_.record_write(request_id, length);
    else metrics_.record_read(request_id, length);
    return request_id;
}

uint64_t UrmaTransport::write(const RemoteRegion& remote, const void* source,
                              size_t length, size_t remote_offset) {
    LOCK();
    return post_rw_locked(true, remote, const_cast<void*>(source), length, remote_offset);
}

uint64_t UrmaTransport::read(const RemoteRegion& remote, void* destination,
                             size_t length, size_t remote_offset) {
    LOCK();
    return post_rw_locked(false, remote, destination, length, remote_offset);
}

uint64_t UrmaTransport::send(const void* message, size_t length) {
    LOCK();
    if (state_ != TransportState::CONNECTED) throw std::runtime_error("ERR_CONNECTION: not connected");
    if (length == 0 ||
        length > effective_max_raw_operation_bytes_ ||
        length > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("ERR_INVALID_ARG: SEND length must be 1..maxChunkBytes");
    }
    LocalRegionState& local_region = find_local_region_locked(message, length);
    const auto post_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::max<uint32_t>(1, options_.request_timeout_ms));
    wait_for_post_capacity_locked(post_deadline);
    const uint64_t request_id = next_request_id();
    RequestRecord pending{};
    pending.request_id = request_id;
    pending.is_send = true;
    pending.bytes = length;
    pending.state = RequestState::POSTED;
    pending.local_region_handle = local_region.region.handle;
    pending.message = "provider rejected SEND without identifying the unaccepted WR";
    requests_.emplace(request_id, pending);
    urma_sge_t sge = {};
    sge.addr = reinterpret_cast<uint64_t>(const_cast<void*>(message));
    sge.len = static_cast<uint32_t>(length);
    sge.tseg = local_region.segment;
    urma_jfs_wr_t wr = {};
    wr.opcode = URMA_OPC_SEND;
    wr.flag.bs.complete_enable = 1;
    wr.tjetty = remote_jetty_;
    wr.user_ctx = request_id;
    wr.send.src.sge = &sge;
    wr.send.src.num_sge = 1;
    urma_jfs_wr_t* bad = nullptr;
    urma_status_t post_status = URMA_EAGAIN;
    for (uint64_t spins = 0; ; ++spins) {
        bad = nullptr;
        post_status = urma_post_jetty_send_wr(jetty_, &wr, &bad);
        if (post_status == URMA_SUCCESS) break;
        if (post_status != URMA_EAGAIN || bad != &wr) break;
        try { poll_once_locked(); }
        catch (...) { requests_.erase(request_id); throw; }
        if (std::chrono::steady_clock::now() >= post_deadline) break;
        if ((spins & 0x3ffu) == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        } else {
            std::this_thread::yield();
        }
    }
    if (post_status != URMA_SUCCESS) {
        if (bad != &wr) {
            auto& unresolved = requests_.at(request_id);
            unresolved.provider_outstanding = true;
            unresolved.state = RequestState::CANCELLED_PROVIDER_OUTSTANDING;
            unresolved.status = static_cast<int>(ErrorCode::ERR_COMPLETION);
            ++local_region.inflight;
            metrics_.record_provider_post(request_id);
            metrics_.record_send(request_id, length);
            metrics_.record_completion(request_id, false);
            state_ = TransportState::CLOSING;
            return request_id;
        }
        requests_.erase(request_id);
        std::fprintf(stderr, "SPARK_URMA_POST_FAIL op=SEND status=%d bad=%p opcode=%d flag=%#x\n",
                     static_cast<int>(post_status), static_cast<void*>(bad), wr.opcode, wr.flag.value);
        throw std::runtime_error("ERR_COMPLETION: SEND post failed");
    }
    auto& request = requests_.at(request_id);
    request.message.clear();
    request.provider_outstanding = true;
    ++local_region.inflight;
    metrics_.record_provider_post(request_id);
    metrics_.record_send(request_id, length);
    return request_id;
}

uint64_t UrmaTransport::writeBlock(const RemoteRegion& remote, const void* source,
                                   size_t total_length) {
    const uint64_t aggregate_id = next_request_id();
    {
        LOCK();
        if (state_ != TransportState::CONNECTED || effective_max_raw_operation_bytes_ == 0)
            throw std::runtime_error("ERR_CONNECTION: block write requires connected transport");
        RequestRecord aggregate;
        aggregate.request_id = aggregate_id;
        aggregate.is_aggregate = true;
        aggregate.aggregate_building = true;
        aggregate.bytes = total_length;
        aggregate.state = RequestState::POSTED;
        aggregate.children.reserve(total_length / effective_max_raw_operation_bytes_ +
            (total_length % effective_max_raw_operation_bytes_ != 0));
        requests_.emplace(aggregate_id, std::move(aggregate));
        metrics_.logical_blocks.fetch_add(1);
        if (total_length == 0) {
            auto& stored = requests_.at(aggregate_id);
            stored.aggregate_building = false;
            finalize_locked(stored, RequestState::COMPLETED_SUCCESS, 0, 0, "zero-byte logical block");
            return aggregate_id;
        }
    }
    const auto* bytes = static_cast<const uint8_t*>(source);
    try {
        const size_t raw_limit = effective_max_raw_operation_bytes_;
        for (size_t offset = 0; offset < total_length; offset += raw_limit) {
            const size_t chunk =
                std::min<size_t>(raw_limit, total_length - offset);
            LOCK();
            const uint64_t child = post_rw_locked(true, remote,
                const_cast<uint8_t*>(bytes + offset), chunk, offset);
            requests_.at(aggregate_id).children.push_back(child);
            auto child_it = requests_.find(child);
            if (child_it != requests_.end()) child_it->second.aggregate_parent = aggregate_id;
            metrics_.chunks.fetch_add(1);
        }
        metrics_.chunked_blocks.fetch_add(1);
        LOCK();
        requests_.at(aggregate_id).aggregate_building = false;
    } catch (const std::exception& error) {
        LOCK();
        auto& aggregate = requests_.at(aggregate_id);
        fail_aggregate_locked(aggregate, error.what());
    }
    return aggregate_id;
}

uint64_t UrmaTransport::readBlock(const RemoteRegion& remote, void* destination,
                                  size_t total_length) {
    const uint64_t aggregate_id = next_request_id();
    {
        LOCK();
        if (state_ != TransportState::CONNECTED || effective_max_raw_operation_bytes_ == 0)
            throw std::runtime_error("ERR_CONNECTION: block read requires connected transport");
        RequestRecord aggregate;
        aggregate.request_id = aggregate_id;
        aggregate.is_aggregate = true;
        aggregate.aggregate_building = true;
        aggregate.bytes = total_length;
        aggregate.state = RequestState::POSTED;
        aggregate.children.reserve(total_length / effective_max_raw_operation_bytes_ +
            (total_length % effective_max_raw_operation_bytes_ != 0));
        requests_.emplace(aggregate_id, std::move(aggregate));
        metrics_.logical_blocks.fetch_add(1);
        if (total_length == 0) {
            auto& stored = requests_.at(aggregate_id);
            stored.aggregate_building = false;
            finalize_locked(stored, RequestState::COMPLETED_SUCCESS, 0, 0, "zero-byte logical block");
            return aggregate_id;
        }
    }
    auto* bytes = static_cast<uint8_t*>(destination);
    try {
        const size_t raw_limit = effective_max_raw_operation_bytes_;
        for (size_t offset = 0; offset < total_length; offset += raw_limit) {
            const size_t chunk =
                std::min<size_t>(raw_limit, total_length - offset);
            LOCK();
            const uint64_t child = post_rw_locked(false, remote, bytes + offset, chunk, offset);
            requests_.at(aggregate_id).children.push_back(child);
            auto child_it = requests_.find(child);
            if (child_it != requests_.end()) child_it->second.aggregate_parent = aggregate_id;
            metrics_.chunks.fetch_add(1);
        }
        metrics_.chunked_blocks.fetch_add(1);
        LOCK();
        requests_.at(aggregate_id).aggregate_building = false;
    } catch (const std::exception& error) {
        LOCK();
        auto& aggregate = requests_.at(aggregate_id);
        fail_aggregate_locked(aggregate, error.what());
    }
    return aggregate_id;
}

uint64_t UrmaTransport::write_chunked(const RemoteRegion& remote, const void* source,
                                      size_t total_length) {
    return writeBlock(remote, source, total_length);
}

uint64_t UrmaTransport::read_chunked(const RemoteRegion& remote, void* destination,
                                     size_t total_length) {
    return readBlock(remote, destination, total_length);
}

void UrmaTransport::fail_aggregate_locked(RequestRecord& request, const char* message) noexcept {
    request.aggregate_building = false;
    try {
        finalize_locked(request, RequestState::COMPLETED_FAILED,
                        static_cast<int>(ErrorCode::ERR_COMPLETION), 0, message);
    } catch (...) {
        // Error-message/cache allocation is not a DMA fence. Keep the tracked
        // parent ID and all accepted children available to wait()/close(), even
        // if the terminal-cache insertion itself cannot be recorded.
        if (request.state == RequestState::POSTED || request.state == RequestState::CREATED) {
            request.state = RequestState::COMPLETED_FAILED;
            request.status = static_cast<int>(ErrorCode::ERR_COMPLETION);
            request.completed_bytes = 0;
        }
        request.message.clear();
        std::fprintf(stderr,
            "SPARK_URMA_AGGREGATE_FAILURE_BOOKKEEPING_ERROR request=%llu reason=%s "
            "retained_child_ownership=true\n",
            static_cast<unsigned long long>(request.request_id), message);
    }
}

void UrmaTransport::finalize_locked(RequestRecord& request, RequestState state, int status,
                                    size_t completed_bytes, const std::string& message) {
    if (request.state != RequestState::POSTED && request.state != RequestState::CREATED) return;
    request.state = state;
    request.status = status;
    request.completed_bytes = completed_bytes;
    request.message = message;
    // Terminal API status does not release aggregate ownership of child
    // records: they are needed to prove that every accepted WR has drained.
    if (!request.is_aggregate) {
        if (state == RequestState::COMPLETED_SUCCESS) {
            metrics_.record_completion(request.request_id, true);
            metrics_.completed_bytes.fetch_add(completed_bytes);
        }
        else if (state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING ||
                 state == RequestState::TIMED_OUT_DRAINED) {
            metrics_.record_timeout(request.request_id);
        }
        else metrics_.record_completion(request.request_id, false);
        if (!request.provider_outstanding && !request.local_ref_released) {
            auto region = registered_regions_.find(request.local_region_handle);
            if (region != registered_regions_.end() && region->second.inflight != 0) --region->second.inflight;
            request.local_ref_released = true;
        }
    }
    terminal_order_.push_back(request.request_id);
}

void UrmaTransport::provider_drained_locked(RequestRecord& request) {
    if (!request.provider_outstanding) return;
    request.provider_outstanding = false;
    metrics_.record_provider_drained(request.request_id);
    if (!request.local_ref_released) {
        auto region = registered_regions_.find(request.local_region_handle);
        if (region != registered_regions_.end() && region->second.inflight != 0) {
            --region->second.inflight;
        }
        request.local_ref_released = true;
    }
    if (request.remote_segment != nullptr) {
        for (auto& imported : imported_regions_) {
            if (imported.segment == request.remote_segment) {
                if (imported.inflight != 0) --imported.inflight;
                break;
            }
        }
        request.remote_segment = nullptr;
    }
    if (request.state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING) {
        request.state = RequestState::TIMED_OUT_DRAINED;
    } else if (request.state == RequestState::CANCELLED_PROVIDER_OUTSTANDING) {
        request.state = RequestState::CANCELLED_DRAINED;
    }
}

void UrmaTransport::poll_once_locked() {
    urma_cr_t completion = {};
    const int count = urma_poll_jfc(jfc_, 1, &completion);
    if (count < 0) throw std::runtime_error("ERR_COMPLETION: urma_poll_jfc failed");
    if (count == 0) return;
    auto it = requests_.find(completion.user_ctx);
    if (it == requests_.end() || it->second.is_aggregate) {
        metrics_.late_completions.fetch_add(1);
        return;
    }
    RequestRecord& request = it->second;
    if (request.state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING ||
        request.state == RequestState::CANCELLED_PROVIDER_OUTSTANDING) {
        metrics_.late_completions.fetch_add(1);
        provider_drained_locked(request);
        return;
    }
    if (request.state != RequestState::POSTED) {
        metrics_.late_completions.fetch_add(1);
        return;
    }
    const bool success = completion.status == URMA_CR_SUCCESS;
    provider_drained_locked(request);
    finalize_locked(request,
                    success ? RequestState::COMPLETED_SUCCESS : RequestState::COMPLETED_FAILED,
                    success ? 0 : static_cast<int>(ErrorCode::ERR_COMPLETION),
                    success ? request.bytes : 0,
                    success ? "" : "provider completion failure status=" + std::to_string(completion.status));
}

void UrmaTransport::refresh_aggregates_locked() {
    std::vector<uint64_t> aggregate_ids;
    aggregate_ids.reserve(requests_.size());
    for (const auto& item : requests_) {
        if (item.second.is_aggregate && item.second.state == RequestState::POSTED) {
            aggregate_ids.push_back(item.first);
        }
    }
    for (uint64_t aggregate_id : aggregate_ids) {
        auto aggregate_it = requests_.find(aggregate_id);
        if (aggregate_it == requests_.end()) continue;
        RequestRecord& aggregate = aggregate_it->second;
        if (!aggregate.is_aggregate || aggregate.state != RequestState::POSTED ||
            aggregate.aggregate_building) continue;
        bool all_success = !aggregate.children.empty();
        size_t completed = 0;
        for (uint64_t child_id : aggregate.children) {
            auto child_it = requests_.find(child_id);
            if (child_it == requests_.end()) {
                finalize_locked(aggregate, RequestState::COMPLETED_FAILED,
                                static_cast<int>(ErrorCode::ERR_INTERNAL), completed,
                                "aggregate child was evicted unexpectedly");
                break;
            }
            const RequestRecord& child = child_it->second;
            if (child.state == RequestState::COMPLETED_FAILED ||
                child.state == RequestState::TIMED_OUT_PROVIDER_OUTSTANDING ||
                child.state == RequestState::TIMED_OUT_DRAINED ||
                child.state == RequestState::CANCELLED_PROVIDER_OUTSTANDING ||
                child.state == RequestState::CANCELLED_DRAINED) {
                finalize_locked(aggregate, RequestState::COMPLETED_FAILED, child.status,
                                completed, "child " + std::to_string(child_id) + ": " + child.message);
                break;
            }
            if (child.state != RequestState::COMPLETED_SUCCESS) all_success = false;
            else completed += child.completed_bytes;
        }
        if (aggregate.state == RequestState::POSTED && all_success) {
            if (completed == aggregate.bytes) {
                finalize_locked(aggregate, RequestState::COMPLETED_SUCCESS, 0, completed, "");
            } else {
                finalize_locked(aggregate, RequestState::COMPLETED_FAILED,
                                static_cast<int>(ErrorCode::ERR_INTERNAL), completed,
                                "aggregate completed byte count mismatch");
            }
        }
    }
}

CompletionResult UrmaTransport::result_locked(const RequestRecord& request) const {
    return CompletionResult{request.request_id, request.status,
                            request.completed_bytes, request.message};
}

void UrmaTransport::trim_terminal_cache_locked() {
    size_t examined = 0;
    while (terminal_order_.size() > MAX_TERMINAL_RESULTS && examined < terminal_order_.size()) {
        uint64_t id = terminal_order_.front();
        terminal_order_.pop_front();
        auto it = requests_.find(id);
        if (it != requests_.end() && it->second.state != RequestState::POSTED &&
            !provider_owned_locked(it->second) && it->second.aggregate_parent == 0) {
            for (uint64_t child_id : it->second.children) {
                auto child = requests_.find(child_id);
                if (child != requests_.end()) child->second.aggregate_parent = 0;
            }
            requests_.erase(it);
        } else {
            terminal_order_.push_back(id);
            ++examined;
        }
    }
}

bool UrmaTransport::provider_owned_locked(const RequestRecord& request) const {
    if (request.provider_outstanding || request.aggregate_building) return true;
    for (uint64_t id : request.children) {
        auto child = requests_.find(id);
        // Missing bookkeeping is not evidence of completion.
        if (child == requests_.end() || child->second.provider_outstanding) return true;
    }
    return false;
}

CompletionResult UrmaTransport::wait(uint64_t request_id, uint32_t timeout_ms) {
    if (request_id == 0 || timeout_ms == 0) {
        throw std::runtime_error("ERR_INVALID_ARG: request id and timeout must be positive");
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    auto drain_deadline = std::chrono::steady_clock::time_point::max();
    while (true) {
        {
            LOCK();
            auto it = requests_.find(request_id);
            if (it == requests_.end()) {
                metrics_.unknown_waits.fetch_add(1);
                throw std::runtime_error("ERR_UNKNOWN_REQUEST: request id is not known");
            }
            refresh_aggregates_locked();
            it = requests_.find(request_id);
            const auto now = std::chrono::steady_clock::now();
            if (it->second.state == RequestState::POSTED && now >= deadline) {
                const RequestState timeout_state = provider_owned_locked(it->second)
                    ? RequestState::TIMED_OUT_PROVIDER_OUTSTANDING
                    : RequestState::TIMED_OUT_DRAINED;
                finalize_locked(it->second, timeout_state,
                                static_cast<int>(ErrorCode::ERR_TIMEOUT), 0,
                                "request deadline exceeded");
            }
            const bool terminal = it->second.state != RequestState::POSTED &&
                                  it->second.state != RequestState::CREATED;
            if (terminal) {
                if (!provider_owned_locked(it->second)) return result_locked(it->second);
                if (drain_deadline == std::chrono::steady_clock::time_point::max()) {
                    drain_deadline = now + std::chrono::milliseconds(options_.drain_timeout_ms);
                }
                if (now >= drain_deadline) {
                    state_ = TransportState::CLOSING;
                    throw std::runtime_error("ERR_DMA_UNRESOLVED: request=" + std::to_string(request_id) +
                        " accepted WRs remain provider-owned; retain local memory and peer lease");
                }
            }
            try {
                if (jfc_ != nullptr) poll_once_locked();
            } catch (const std::exception& error) {
                if (provider_owned_locked(it->second)) {
                    state_ = TransportState::CLOSING;
                    throw std::runtime_error(std::string("ERR_DMA_UNRESOLVED: CQ polling failed: ") + error.what());
                }
                throw;
            }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

void UrmaTransport::cleanup_locked(std::vector<std::string>& errors) {
    if (metrics_.provider_outstanding_requests.load() != 0) {
        errors.push_back("provider requests are still outstanding");
        return;
    }
    for (const auto& item : registered_regions_) {
        if (item.second.inflight != 0) {
            errors.push_back("registered region still has provider references: " +
                             std::to_string(item.first));
        }
    }
    if (!errors.empty()) return;

    // A failed destroy retains its handle and all dependent resources. A
    // later close retries only resources that were not successfully released.
    const auto destroy = [&errors](auto*& handle, auto release, const char* name) {
        if (handle != nullptr && release(handle) != URMA_SUCCESS) {
            errors.push_back(name);
            return false;
        }
        handle = nullptr;
        return true;
    };

    for (auto& imported : imported_regions_) {
        if (imported.segment != nullptr) {
            if (!destroy(imported.segment, urma_unimport_seg, "urma_unimport_seg failed")) return;
            metrics_.unimported_regions.fetch_add(1);
            metrics_.active_imports.fetch_sub(1);
        }
    }
    imported_regions_.clear();
    if (remote_jetty_ != nullptr) {
        if (remote_bound_) {
            if (urma_unbind_jetty(jetty_) != URMA_SUCCESS) {
                errors.push_back("urma_unbind_jetty failed");
                return;
            }
            remote_bound_ = false;
        }
        if (!destroy(remote_jetty_, urma_unimport_jetty, "urma_unimport_jetty failed")) return;
    }
    for (auto& item : registered_regions_) {
        if (item.second.segment != nullptr) {
            if (!destroy(item.second.segment, urma_unregister_seg, "urma_unregister_seg failed")) return;
            item.second.registered = false;
            metrics_.unregistered_regions.fetch_add(1);
            metrics_.active_regions.fetch_sub(1);
        }
        if (!destroy(item.second.token_id, urma_free_token_id, "urma_free_token_id failed")) return;
    }
    registered_regions_.clear();
    if (local_seg_ != nullptr) {
        if (!destroy(local_seg_, urma_unregister_seg, "internal unregister failed")) return;
    }
    std::free(local_buffer_);
    local_buffer_ = nullptr;
    if (!destroy(jetty_, urma_delete_jetty, "urma_delete_jetty failed")) return;
    if (!destroy(jfs_, urma_delete_jfs, "urma_delete_jfs failed")) return;
    if (!destroy(jfr_, urma_delete_jfr, "urma_delete_jfr failed")) return;
    if (!destroy(jfc_, urma_delete_jfc, "urma_delete_jfc failed")) return;
    if (!destroy(ctx_, urma_delete_context, "urma_delete_context failed")) return;
    if (owns_runtime_slot_) {
        std::lock_guard<std::mutex> process_lock(g_process_mutex);
        g_transport_active = false;
        owns_runtime_slot_ = false;
        if (metrics_.active_transports.load() != 0) metrics_.active_transports.fetch_sub(1);
    }
}

void UrmaTransport::close() {
    LOCK();
    if (state_ == TransportState::CLOSED) return;
    state_ = TransportState::CLOSING;
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(std::min<uint32_t>(options_.request_timeout_ms, 1000));
    while (metrics_.provider_outstanding_requests.load() != 0 &&
           std::chrono::steady_clock::now() < deadline) {
        poll_once_locked();
        refresh_aggregates_locked();
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    if (metrics_.provider_outstanding_requests.load() != 0) {
        for (auto& item : requests_) {
            if (!item.second.is_aggregate && item.second.state == RequestState::POSTED) {
                finalize_locked(item.second, RequestState::CANCELLED_PROVIDER_OUTSTANDING,
                            static_cast<int>(ErrorCode::ERR_CLOSED), 0,
                            "cancelled by close deadline");
            }
        }
        refresh_aggregates_locked();
        metrics_.close_errors.fetch_add(1);
        throw std::runtime_error("ERR_BUSY: close drain deadline exceeded with " +
                                 std::to_string(metrics_.provider_outstanding_requests.load()) +
                                 " provider request(s) outstanding");
    }
    refresh_aggregates_locked();
    std::vector<std::string> errors;
    cleanup_locked(errors);
    if (!errors.empty()) {
        // Retained provider handles still belong to this object. A later
        // close must retry cleanup, not report success and let JNI drop pins.
        metrics_.close_errors.fetch_add(1);
        throw std::runtime_error("ERR_CLOSED: " + join_errors(errors));
    }
    state_ = TransportState::CLOSED;
}

uint64_t UrmaTransport::next_request_id() {
    return request_seq_.fetch_add(1, std::memory_order_relaxed);
}

void TransportMetrics::record_write(uint64_t, size_t bytes) {
    submitted_requests.fetch_add(1); inflight_requests.fetch_add(1);
    submitted_bytes.fetch_add(bytes); write_requests.fetch_add(1); write_bytes.fetch_add(bytes);
}
void TransportMetrics::record_read(uint64_t, size_t bytes) {
    submitted_requests.fetch_add(1); inflight_requests.fetch_add(1);
    submitted_bytes.fetch_add(bytes); read_requests.fetch_add(1); read_bytes.fetch_add(bytes);
}
void TransportMetrics::record_send(uint64_t, size_t bytes) {
    submitted_requests.fetch_add(1); inflight_requests.fetch_add(1);
    submitted_bytes.fetch_add(bytes); send_requests.fetch_add(1); send_bytes.fetch_add(bytes);
}
void TransportMetrics::record_completion(uint64_t, bool success) {
    if (success) completed_requests.fetch_add(1); else failed_requests.fetch_add(1);
    if (inflight_requests.load() != 0) inflight_requests.fetch_sub(1);
}
void TransportMetrics::record_timeout(uint64_t) {
    timed_out_requests.fetch_add(1);
    if (inflight_requests.load() != 0) inflight_requests.fetch_sub(1);
}
void TransportMetrics::record_provider_post(uint64_t) {
    provider_outstanding_requests.fetch_add(1);
}
void TransportMetrics::record_provider_drained(uint64_t) {
    provider_drained_requests.fetch_add(1);
    if (provider_outstanding_requests.load() != 0) {
        provider_outstanding_requests.fetch_sub(1);
    }
}
void TransportMetrics::record_chunked_block(uint64_t count) {
    logical_blocks.fetch_add(1); chunked_blocks.fetch_add(1); chunks.fetch_add(count);
}

}  // namespace spark_urma
