// SPDX-License-Identifier: Apache-2.0
// Observe the real transport's API arguments/order with a wrapped provider.
// The payload copy below is test code, not hardware DMA or a UB simulator.
#include "urma_transport.h"
#include <urma_api.h>
#if SPARK_URMA_BOND_CONTEXT_API
#include <urma_ubagg.h>
#endif
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <string>
#include <vector>
using namespace spark_urma;
namespace {
std::vector<std::string> trace;
std::deque<urma_cr_t> completions;
urma_device_t device{}; urma_context_t context{}; urma_eid_info_t eid{};
urma_jfc_t jfc{}; urma_jfr_t jfr{}; urma_jetty_t jetty{};
urma_token_id_t token_id{};
urma_target_seg_t local_segment{}, imported_segment{};
urma_target_jetty_t imported_jetty{};
alignas(4096) unsigned char local_data[4096], peer_data[4096];
bool bonding=false;
void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
void called(const char* name){trace.emplace_back(name);}
void token(urma_token_t* value){check(value&&value->token==0xDEADBEEF,"token value changed between APIs");}
template<class T> T* extended(){
    auto* p=static_cast<T*>(std::calloc(1,sizeof(T)+32));
    if(!p)throw std::bad_alloc();
    p->ext.flag.bs.enable=1;p->ext.length=32;std::memset(p->ext.buf,0xA5,32);return p;
}
void before(const char* a,const char* b){
    auto x=std::find(trace.begin(),trace.end(),a),y=std::find(trace.begin(),trace.end(),b);
    check(x!=trace.end()&&y!=trace.end()&&x<y,"URMA dependency order violated");
}
}
extern "C" urma_status_t __wrap_urma_init(urma_init_attr_t*){called("init");return URMA_SUCCESS;}
extern "C" urma_status_t __wrap_urma_uninit(){called("uninit");return URMA_SUCCESS;}
extern "C" urma_device_t* __wrap_urma_get_device_by_name(char* name){
    called("get_device_by_name");std::snprintf(device.name,sizeof(device.name),"%s",name);return &device;
}
extern "C" urma_status_t __wrap_urma_query_device(urma_device_t*,urma_device_attr_t* out){
    called("query_device");*out={};auto& c=out->dev_cap;
    c.trans_mode=URMA_TM_RM;c.rm_tp_cap.bs.ctp=1;c.tp_feature.bs.rm_multi_path=1;
    c.max_jfc_depth=c.max_jfs_depth=c.max_jfr_depth=64;
    c.max_jfs_sge=c.max_jfr_sge=2;c.max_jfs_rsge=1;
    c.max_msg_size=c.max_read_size=c.max_write_size=4096;
    c.priority_info[6].tp_type.bs.ctp=1;return URMA_SUCCESS;
}
extern "C" urma_eid_info_t* __wrap_urma_get_eid_list(urma_device_t*,uint32_t* count){
    called("get_eid_list");*count=1;eid.eid_index=7;return &eid;
}
extern "C" void __wrap_urma_free_eid_list(urma_eid_info_t*){called("free_eid_list");}
extern "C" urma_context_t* __wrap_urma_create_context(urma_device_t* d,uint32_t index){
    called("create_context");check(index==7,"EID index was not passed to create_context");
    context.dev=d;context.eid_index=index;context.eid.raw[0]=9;context.uasid=17;return &context;
}
extern "C" urma_status_t __wrap_urma_user_ctl(urma_context_t*,urma_user_ctl_in_t* in,urma_user_ctl_out_t*){
    called("user_ctl");check(bonding,"bonding user_ctl reached physical UDMA");
#if SPARK_URMA_BOND_CONTEXT_API
    check(in->opcode==BONDP_USER_CTL_SET_BONDING_MODE,"incorrect SDK opcode");
    check(in->len==sizeof(bondp_set_bonding_mode_in_t),"incorrect SDK command size");
    const auto* mode=reinterpret_cast<const bondp_set_bonding_mode_in_t*>(in->addr);
    check(mode->bonding_mode==BONDP_BONDING_MODE_STANDALONE&&mode->bonding_level==BONDP_BONDING_LEVEL_IODIE,
          "bond context differs from selected perftest profile");
#else
    (void)in;throw std::runtime_error("unavailable SDK private command was used");
#endif
    return URMA_SUCCESS;
}
extern "C" urma_jfc_t* __wrap_urma_create_jfc(urma_context_t*,urma_jfc_cfg_t* cfg){
    called("create_jfc");check(cfg->depth==16&&cfg->jfce==nullptr,"unexpected polling JFC configuration");return &jfc;
}
extern "C" urma_jfr_t* __wrap_urma_create_jfr(urma_context_t*,urma_jfr_cfg_t* cfg){
    called("create_jfr");check(cfg->trans_mode==URMA_TM_RM&&cfg->jfc==&jfc,"JFR mode/JFC mismatch");
    check(cfg->depth==8&&cfg->max_sge==(bonding?2u:1u),"JFR depth/SGE mismatch");
    check(cfg->flag.bs.order_type==URMA_DEF_ORDER&&cfg->min_rnr_timer==URMA_TYPICAL_MIN_RNR_TIMER,"JFR defaults mismatch");
    token(&cfg->token_value);return &jfr;
}
extern "C" urma_jetty_t* __wrap_urma_create_jetty(urma_context_t*,urma_jetty_cfg_t* cfg){
    called("create_jetty");auto& s=cfg->jfs_cfg;
    check(cfg->flag.bs.share_jfr&&cfg->shared.jfr==&jfr&&cfg->shared.jfc==&jfc,"shared JFR/JFC mismatch");
    check(s.trans_mode==URMA_TM_RM&&s.priority==6&&s.jfc==&jfc,"JFS mode/priority/JFC mismatch");
    check(s.depth==8&&s.max_sge==(bonding?2u:1u)&&s.max_rsge==1,"JFS depth/SGE mismatch");
    check(s.flag.bs.multi_path==bonding&&s.flag.bs.order_type==URMA_DEF_ORDER,"multipath/order mismatch");
    check(s.rnr_retry==URMA_TYPICAL_RNR_RETRY&&s.err_timeout==URMA_TYPICAL_ERR_TIMEOUT,"JFS retry mismatch");
    jetty={};jetty.jetty_id.eid=context.eid;jetty.jetty_id.uasid=17;jetty.jetty_id.id=77;return &jetty;
}
extern "C" urma_token_id_t* __wrap_urma_alloc_token_id(urma_context_t*){called("alloc_token_id");return &token_id;}
extern "C" urma_target_seg_t* __wrap_urma_register_seg(urma_context_t*,urma_seg_cfg_t* cfg){
    called("register_seg");check(cfg->va==reinterpret_cast<uint64_t>(local_data)&&cfg->len==4096,"registration CPU VA/length mismatch");
    check(cfg->token_id==&token_id&&cfg->flag.bs.token_id_valid==URMA_TOKEN_ID_VALID,"explicit token id ownership mismatch");
    check(cfg->flag.bs.access==(URMA_ACCESS_READ|URMA_ACCESS_WRITE)&&cfg->flag.bs.token_policy==URMA_TOKEN_NONE,"registration access mismatch");
    token(&cfg->token_value);local_segment={};local_segment.seg.ubva.eid=context.eid;
    local_segment.seg.ubva.uasid=29;local_segment.seg.ubva.va=0x420000;
    local_segment.seg.len=4096;local_segment.seg.token_id=7;
    local_segment.seg.attr.bs.access=URMA_ACCESS_READ|URMA_ACCESS_WRITE;return &local_segment;
}
extern "C" urma_status_t __wrap_urma_get_rjetty(urma_jetty_t*,urma_rjetty_t** out,uint32_t* length){
    called("get_rjetty");check(bonding,"physical path exported bonding context");*out=extended<urma_rjetty_t>();
    (*out)->jetty_id=jetty.jetty_id;(*out)->trans_mode=URMA_TM_RM;(*out)->type=URMA_JETTY;
    *length=sizeof(urma_rjetty_t)+32;return URMA_SUCCESS;
}
extern "C" void __wrap_urma_put_rjetty(urma_rjetty_t* p){called("put_rjetty");std::free(p);}
extern "C" urma_status_t __wrap_urma_get_seg_ctx(urma_target_seg_t*,urma_seg_t** out,uint32_t* length){
    called("get_seg_ctx");check(bonding,"physical path exported bonding segment context");*out=extended<urma_seg_t>();
    **out=local_segment.seg;(*out)->ext.flag.bs.enable=1;(*out)->ext.length=32;
    *length=sizeof(urma_seg_t)+32;return URMA_SUCCESS;
}
extern "C" void __wrap_urma_put_seg_ctx(urma_seg_t* p){called("put_seg_ctx");std::free(p);}
extern "C" urma_target_jetty_t* __wrap_urma_import_jetty(urma_context_t*,urma_rjetty_t* p,urma_token_t* t){
    called("import_jetty");token(t);check(p->trans_mode==URMA_TM_RM&&p->tp_type==URMA_CTP&&p->type==URMA_JETTY,"RM/CTP Jetty import mismatch");
    check(p->jetty_id.id==77&&p->jetty_id.uasid==17,"Jetty identity mismatch");
    check(p->ext.flag.bs.enable==bonding,"Jetty context form mismatch");
    if(bonding)check(p->ext.length==32&&static_cast<unsigned char>(p->ext.buf[31])==0xA5,"Jetty extension lost");
    return &imported_jetty;
}
extern "C" urma_status_t __wrap_urma_bind_jetty(urma_jetty_t*,urma_target_jetty_t*){throw std::runtime_error("RM must not call RC bind");}
extern "C" urma_status_t __wrap_urma_unbind_jetty(urma_jetty_t*){throw std::runtime_error("RM must not call RC unbind");}
extern "C" urma_target_seg_t* __wrap_urma_import_seg(urma_context_t*,urma_seg_t* p,urma_token_t* t,uint64_t addr,urma_import_seg_flag_t flags){
    called("import_seg");token(t);check(addr==0&&flags.bs.mapping==URMA_SEG_NOMAP,"NOMAP mapping argument mismatch");
    check(flags.bs.access==(URMA_ACCESS_READ|URMA_ACCESS_WRITE),"import permissions mismatch");
    check(p->ubva.va==0x420000&&p->ubva.uasid==29&&p->len==4096&&p->token_id==7,"MR identity replaced by Jetty/EID-index identity");
    check(p->ext.flag.bs.enable==bonding,"segment context form mismatch");
    if(bonding)check(p->ext.length==32&&static_cast<unsigned char>(p->ext.buf[31])==0xA5,"segment extension lost");
    return &imported_segment;
}
extern "C" urma_status_t __wrap_urma_post_jetty_send_wr(urma_jetty_t*,urma_jfs_wr_t* wr,urma_jfs_wr_t**){
    const bool write=wr->opcode==URMA_OPC_WRITE;called(write?"post_WRITE":"post_READ");
    check(write||wr->opcode==URMA_OPC_READ,"non READ/WRITE payload operation");
    check(wr->flag.bs.complete_enable&&!wr->flag.bs.inline_flag&&wr->tjetty==&imported_jetty,"WR flags/Jetty mismatch");
    check(wr->rw.src.num_sge==1&&wr->rw.dst.num_sge==1,"payload SGE count mismatch");
    const auto& l=(write?wr->rw.src:wr->rw.dst).sge[0];const auto& r=(write?wr->rw.dst:wr->rw.src).sge[0];
    check(l.addr==reinterpret_cast<uint64_t>(local_data)&&l.tseg==&local_segment,"local SGE must use CPU VA and registered segment");
    check(r.addr==0x420000&&r.tseg==&imported_segment&&l.len==1024&&r.len==1024,"remote SGE address/segment/length mismatch");
    if(write)std::memcpy(peer_data,local_data,1024);else std::memcpy(local_data,peer_data,1024);
    urma_cr_t cr{};cr.user_ctx=wr->user_ctx;cr.status=URMA_CR_SUCCESS;completions.push_back(cr);return URMA_SUCCESS;
}
extern "C" int __wrap_urma_poll_jfc(urma_jfc_t* q,int,urma_cr_t* cr){
    called("poll_jfc");check(q==&jfc,"poll used different JFC");if(completions.empty())return 0;
    *cr=completions.front();completions.pop_front();return 1;
}
#define RELEASE(api,type) extern "C" urma_status_t __wrap_urma_##api(type*){called(#api);return URMA_SUCCESS;}
RELEASE(unimport_seg,urma_target_seg_t)
RELEASE(unimport_jetty,urma_target_jetty_t)
RELEASE(unregister_seg,urma_target_seg_t)
RELEASE(free_token_id,urma_token_id_t)
RELEASE(delete_jetty,urma_jetty_t)
RELEASE(delete_jfr,urma_jfr_t)
RELEASE(delete_jfc,urma_jfc_t)
RELEASE(delete_context,urma_context_t)
static void run(bool aggregate){
    bonding=aggregate;trace.clear();UrmaTransport t;TransportOptions options{};
    options.device_name=bonding?"bonding_dev_0":"udmac0d1e2";
    options.transport_mode=URMA_TM_RM;options.tp_type=URMA_CTP;options.priority=6;options.eid_index=7;
    options.queue_depth=8;options.max_chunk_bytes=4096;options.request_timeout_ms=100;options.drain_timeout_ms=100;
    t.init(options);auto registered=t.register_region(local_data,sizeof(local_data));
    auto jetty_context=t.export_jetty_context(),seg_context=t.export_segment_context(registered.handle);
    t.connect(t.local_endpoint(),jetty_context);
    RemoteRegion remote{};remote.has_segment=true;remote.remote_address=remote.segment_address=registered.remote_address;
    remote.length=remote.segment_length=registered.segment_length;remote.token=registered.token;remote.generation=registered.generation;
    remote.segment_uasid=registered.segment_uasid;remote.segment_attributes=registered.segment_attributes;
    std::memcpy(remote.segment_eid,registered.segment_eid,16);remote.provider_context=seg_context;
    for(size_t i=0;i<1024;i++)local_data[i]=static_cast<unsigned char>(i*31);
    auto w=t.write(remote,local_data,1024);check(t.wait(w,100).status==0,"WRITE completion failed");
    std::memset(local_data,0,1024);auto r=t.read(remote,local_data,1024);check(t.wait(r,100).status==0,"READ completion failed");
    for(size_t i=0;i<1024;i++)check(local_data[i]==static_cast<unsigned char>(i*31),"payload mismatch");
    t.close();
    before("query_device","create_context");before("create_context","create_jfc");before("create_jfc","create_jfr");
    before("create_jfr","create_jetty");before("alloc_token_id","register_seg");before("register_seg","import_seg");
    before("import_jetty","post_WRITE");before("import_seg","post_WRITE");before("post_WRITE","poll_jfc");
    before("post_READ","unimport_seg");before("unimport_seg","unregister_seg");before("unregister_seg","free_token_id");
    before("delete_jetty","delete_jfr");before("delete_jfr","delete_jfc");before("delete_jfc","delete_context");
    if(bonding){before("get_rjetty","put_rjetty");before("get_seg_ctx","put_seg_ctx");
#if SPARK_URMA_BOND_CONTEXT_API
        before("create_context","user_ctl");before("user_ctl","create_jfc");
#endif
    }
    std::printf("OFFICIAL_API_TRACE device=%s",options.device_name.c_str());
    for(const auto& name:trace)std::printf(" -> %s",name.c_str());
    std::puts("");
}
int main(){try{run(false);run(true);UrmaTransport::shutdown_process_runtime();}
    catch(const std::exception& e){std::fprintf(stderr,"%s\n",e.what());return 1;}
    std::puts("OFFICIAL_API_ALIGNMENT_PASS physical+bonding RM/CTP; WR fields/order/cleanup; wrapped provider; NOT hardware UB");}
