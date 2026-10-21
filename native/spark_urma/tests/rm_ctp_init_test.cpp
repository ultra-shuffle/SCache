#include "urma_transport.h"
#include <urma_api.h>
#if SPARK_URMA_BOND_CONTEXT_API
#include <urma_ubagg.h>
#endif
#include <cstdio>
#include <cstring>
#include <stdexcept>
using namespace spark_urma;
static urma_device_t device{};
static urma_context_t context{};
static urma_jfc_t jfc{};
static urma_jfr_t jfr{};
static urma_jetty_t jetty{};
static urma_eid_info_t eid{};
static urma_jfs_cfg_t seen_jfs{};
static urma_jfr_cfg_t seen_jfr{};
static uint32_t seen_jfc_depth;
static unsigned created_context, deleted_context, deleted_jfc, deleted_jfr, deleted_jetty;
static bool unsupported, fail_jetty;
static bool insufficient_cq;
static bool fail_delete_jfr;
static bool fail_delete_jetty, fail_delete_jfc, fail_delete_context;
static unsigned context_controls, queue_creations;
static bool fail_context_control;
extern "C" urma_status_t __wrap_urma_user_ctl(urma_context_t* ctx,
        urma_user_ctl_in_t* input, urma_user_ctl_out_t*) {
    ++context_controls;
    if (queue_creations != 0)
        throw std::runtime_error("bond context changed after queue creation");
#if SPARK_URMA_BOND_CONTEXT_API
    if (ctx != &context || input->opcode != BONDP_USER_CTL_SET_BONDING_MODE ||
        input->len != sizeof(bondp_set_bonding_mode_in_t) || input->addr == 0)
        throw std::runtime_error("wrong paired SDK bonding context command");
    const auto& cfg = *reinterpret_cast<const bondp_set_bonding_mode_in_t*>(input->addr);
    if (cfg.bonding_mode != BONDP_BONDING_MODE_STANDALONE ||
        cfg.bonding_level != BONDP_BONDING_LEVEL_IODIE)
        throw std::runtime_error("bond context differs from official perftest default");
#else
    (void)ctx; (void)input;
    throw std::runtime_error("legacy SDK unexpectedly used a private context command");
#endif
    return fail_context_control ? URMA_EINVAL : URMA_SUCCESS;
}
extern "C" urma_status_t __wrap_urma_init(urma_init_attr_t*) { return URMA_SUCCESS; }
extern "C" urma_status_t __wrap_urma_uninit() { return URMA_SUCCESS; }
extern "C" urma_device_t* __wrap_urma_get_device_by_name(char* name) {
    std::snprintf(device.name, sizeof(device.name), "%s", name); return &device;
}
extern "C" urma_status_t __wrap_urma_query_device(urma_device_t*, urma_device_attr_t* attr) {
    *attr = {};
    auto& c = attr->dev_cap;
    c.trans_mode = URMA_TM_RM;
    c.rm_tp_cap.bs.ctp = !unsupported;
    c.max_jfc_depth = insufficient_cq ? 63 : 64;
    c.max_jfs_depth = c.max_jfr_depth = 64;
    c.max_jfs_sge = c.max_jfr_sge = 2;
    c.max_jfs_rsge = 1;
    // The official API exposes distinct SEND-message and one-sided limits.
    c.max_msg_size = 4096;
    c.max_read_size = 65536;
    c.max_write_size = 32768;
    return URMA_SUCCESS;
}
extern "C" urma_eid_info_t* __wrap_urma_get_eid_list(urma_device_t*, uint32_t* count) {
    *count = 1; eid.eid_index = 7; return &eid;
}
extern "C" void __wrap_urma_free_eid_list(urma_eid_info_t*) {}
extern "C" urma_context_t* __wrap_urma_create_context(urma_device_t*, uint32_t index) {
    ++created_context; queue_creations = 0; context.eid_index = index; return &context;
}
extern "C" urma_jfc_t* __wrap_urma_create_jfc(urma_context_t*, urma_jfc_cfg_t* cfg) {
    ++queue_creations;
    seen_jfc_depth = cfg->depth;
    return &jfc;
}
extern "C" urma_jfr_t* __wrap_urma_create_jfr(urma_context_t*, urma_jfr_cfg_t* cfg) {
    seen_jfr = *cfg; return &jfr;
}
extern "C" urma_jetty_t* __wrap_urma_create_jetty(urma_context_t*, urma_jetty_cfg_t* cfg) {
    seen_jfs = cfg->jfs_cfg;
    if (!cfg->flag.bs.share_jfr || cfg->shared.jfr != &jfr) return nullptr;
    jetty.jetty_id.id = 123; jetty.jetty_id.uasid = 42;
    return fail_jetty ? nullptr : &jetty;
}
extern "C" urma_status_t __wrap_urma_delete_context(urma_context_t*) {
    ++deleted_context; return fail_delete_context ? static_cast<urma_status_t>(-1) : URMA_SUCCESS;
}
extern "C" urma_status_t __wrap_urma_delete_jfc(urma_jfc_t*) {
    ++deleted_jfc; return fail_delete_jfc ? static_cast<urma_status_t>(-1) : URMA_SUCCESS;
}
extern "C" urma_status_t __wrap_urma_delete_jfr(urma_jfr_t*) {
    ++deleted_jfr; return fail_delete_jfr ? static_cast<urma_status_t>(-1) : URMA_SUCCESS;
}
extern "C" urma_status_t __wrap_urma_delete_jetty(urma_jetty_t*) {
    ++deleted_jetty; return fail_delete_jetty ? static_cast<urma_status_t>(-1) : URMA_SUCCESS;
}
static void check(bool ok, const char* why) { if (!ok) throw std::runtime_error(why); }
int main() {
    try {
        TransportOptions options{};
        options.device_name = "bonding-test"; options.queue_depth = 32; options.max_chunk_bytes = 65536;
        options.transport_mode = URMA_TM_RM; options.tp_type = URMA_CTP;
        options.eid_index = 7; options.priority = 6;
        options.request_timeout_ms = 100; options.drain_timeout_ms = 2;
        { UrmaTransport t; t.init(options);
          check(t.local_endpoint().max_chunk_bytes == 4096,
                "shared SEND/READ/WRITE endpoint limit lost its message bound");
          check(context.eid_index == 7, "EID selection lost");
          check(seen_jfs.trans_mode == URMA_TM_RM && seen_jfr.trans_mode == URMA_TM_RM,
                "RC leaked into RM queues");
          check(seen_jfs.flag.bs.order_type == URMA_DEF_ORDER &&
                seen_jfr.flag.bs.order_type == URMA_DEF_ORDER,
                "unexpected explicit ordering replaced the official RM default");
          check(seen_jfc_depth == seen_jfs.depth + seen_jfr.depth,
                "shared JFC cannot hold both send and receive completions");
          check(seen_jfs.priority == 6 && seen_jfs.flag.bs.multi_path, "CTP queue selection lost");
          check(seen_jfs.max_sge == 2 && seen_jfs.max_rsge == 1 && seen_jfr.max_sge == 2,
                "bonding queue did not reserve its extra SGE");
          check(seen_jfs.rnr_retry == URMA_TYPICAL_RNR_RETRY &&
                seen_jfs.err_timeout == URMA_TYPICAL_ERR_TIMEOUT &&
                seen_jfr.min_rnr_timer == URMA_TYPICAL_MIN_RNR_TIMER,
                "UMDK queue retry/timer defaults were not applied");
          t.close(); }
        check(deleted_context == 1 && deleted_jfc == 1 && deleted_jfr == 1 && deleted_jetty == 1,
              "normal resource cleanup incomplete");
        check(context_controls == SPARK_URMA_BOND_CONTEXT_API,
              "explicit context command missing or used with unsupported SDK");
        insufficient_cq = true;
        bool rejected_cq = false;
        { UrmaTransport t; try { t.init(options); } catch (const std::runtime_error&) { rejected_cq = true; } }
        check(rejected_cq, "insufficient shared JFC depth was accepted");
        insufficient_cq = false;
        options.device_name = "udmac0d1e2";
        const auto physical_controls = context_controls;
        { UrmaTransport t; t.init(options);
          check(seen_jfs.trans_mode == URMA_TM_RM && seen_jfs.priority == 6,
                "physical device lost RM/CTP configuration");
          check(!seen_jfs.flag.bs.multi_path, "UBAGG multi_path leaked into physical UDMA");
          check(seen_jfs.max_sge == 1 && seen_jfr.max_sge == 1,
                "bonding SGE capacity leaked into physical UDMA");
          t.close(); }
        check(context_controls == physical_controls,
              "aggregate context command leaked into physical UDMA");
        options.device_name = "bonding-test";
        for (int scenario = 0; scenario < 3; ++scenario) {
            unsupported = scenario == 0; fail_jetty = scenario == 2;
            options.eid_index = scenario == 1 ? 999 : 7;
            bool rejected = false;
            { UrmaTransport t; try { t.init(options); } catch (const std::runtime_error&) { rejected = true; } }
            check(rejected, "invalid capability/EID or create failure accepted");
            check(UrmaTransport::process_runtime_metrics().active_transports == 0, "runtime slot leaked");
        }
        check(created_context == 3 && deleted_context == 3 && deleted_jfc == 3 && deleted_jfr == 3 && deleted_jetty == 2,
              "partial initialization cleanup invalid");
        unsupported = fail_jetty = false;
        options.eid_index = 7;
#if SPARK_URMA_BOND_CONTEXT_API
        // Refused context selection must stop before any queue/MR allocation,
        // release the new context, and never silently use the PORT default.
        const auto old_contexts = deleted_context;
        fail_context_control = true;
        bool rejected_context = false;
        { UrmaTransport t;
          try { t.init(options); } catch (const std::runtime_error& e) {
              rejected_context = std::strstr(e.what(), "standalone,IODIE") != nullptr;
          } }
        fail_context_control = false;
        check(rejected_context && queue_creations == 0 && deleted_context == old_contexts + 1,
              "failed explicit context selection allocated queues or leaked context");
        check(UrmaTransport::process_runtime_metrics().active_transports == 0,
              "failed context selection leaked process runtime slot");
#endif
        { UrmaTransport t; t.init(options);
          fail_delete_jfr = true;
          bool failed = false;
          try { t.close(); } catch (const std::runtime_error&) { failed = true; }
          check(failed && t.state() == TransportState::CLOSING,
                "failed provider cleanup falsely marked transport CLOSED");
          const auto contexts = deleted_context;
          fail_delete_jfr = false;
          t.close();
          check(t.state() == TransportState::CLOSED && deleted_context == contexts + 1,
                "close retry skipped retained provider resources");
          check(UrmaTransport::process_runtime_metrics().active_transports == 0,
                "close retry leaked process runtime slot"); }
        // Successive failures in one close sequence: already destroyed
        // children must not be destroyed again when a parent retry fails.
        { UrmaTransport t; t.init(options);
          bool* failures[] = {&fail_delete_jetty, &fail_delete_jfr,
                              &fail_delete_jfc, &fail_delete_context};
          const auto jets = deleted_jetty, frs = deleted_jfr;
          const auto fcs = deleted_jfc, contexts = deleted_context;
          for (bool* failure : failures) {
              *failure = true;
              bool rejected = false;
              try { t.close(); } catch (const std::runtime_error&) { rejected = true; }
              *failure = false;
              check(rejected && t.state() == TransportState::CLOSING,
                    "staged close failure lost retry state");
              check(UrmaTransport::process_runtime_metrics().active_transports == 1,
                    "runtime slot released before context cleanup");
          }
          t.close();
          check(deleted_jetty == jets + 2 && deleted_jfr == frs + 2 &&
                deleted_jfc == fcs + 2 && deleted_context == contexts + 2,
                "close retry repeated a successful destroy or skipped a failed one");
          t.close();
          check(deleted_context == contexts + 2, "successful close is not idempotent");
          check(UrmaTransport::process_runtime_metrics().active_transports == 0,
                "staged close retry leaked runtime slot"); }
        UrmaTransport::shutdown_process_runtime();
    } catch (const std::exception& e) { std::fprintf(stderr, "%s\n", e.what()); return 1; }
    std::puts("RM_CTP_INIT_PASS (wrapped provider; no hardware)");
}
