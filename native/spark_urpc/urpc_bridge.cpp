// SPDX-License-Identifier: Apache-2.0
// Experimental official URPC framework bridge. All application URPC verbs run
// on one I/O thread; registered RPC staging is distinct from the SCache arena.
#include <arpa/inet.h>
#include <endian.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
extern "C" {
#include "urpc_framework_api.h"
#include "urpc_framework_errno.h"
#include "urma_api.h"
}
namespace scache_urpc {
static constexpr size_t SLOT=8192, SLOTS=4096, CHUNK=4096;
static uint8_t* pool=nullptr;
static uint64_t pool_mh=0;
static std::vector<size_t> free_slots;
static std::mutex pool_mutex;
static void check(bool ok,const char* op) { if(!ok) throw std::runtime_error(op); }
static int raw_get(urpc_sge_t* s,uint64_t n,urpc_allocator_option_t*) {
    std::lock_guard<std::mutex> lock(pool_mutex);
    if(n>SLOT || free_slots.empty()) return -1;
    size_t i=free_slots.back(); free_slots.pop_back();
    *s={reinterpret_cast<uint64_t>(pool+i*SLOT),static_cast<uint32_t>(n),0,pool_mh};
    return 0;
}
static int raw_put(urpc_sge_t* s,urpc_allocator_option_t*) {
    if(!s || !s->addr) return 0;
    std::lock_guard<std::mutex> lock(pool_mutex);
    const uint64_t base=reinterpret_cast<uint64_t>(pool);
    if(s->addr<base || s->addr-base>=SLOT*SLOTS || (s->addr-base)%SLOT) return -1;
    free_slots.push_back((s->addr-base)/SLOT); s->addr=0; return 0;
}
static int sges_get(urpc_sge_t** s,uint32_t n,urpc_allocator_option_t*) {
    *s=static_cast<urpc_sge_t*>(calloc(n,sizeof(**s))); return *s ? 0 : -1;
}
static int sges_put(urpc_sge_t* s,urpc_allocator_option_t*) { free(s); return 0; }
static int buf_get(urpc_sge_t** s,uint32_t* n,uint64_t size,urpc_allocator_option_t* o) {
    *n=1;
    if(sges_get(s,1,o)) return -1;
    if(raw_get(*s,size,o)) {free(*s);*s=nullptr;return -1;} return 0;
}
static int buf_put(urpc_sge_t* s,uint32_t n,urpc_allocator_option_t* o) {
    for(uint32_t i=0;i<n;i++) { raw_put(s+i,o); }
    free(s); return 0;
}
static urpc_allocator_t allocator={buf_get,buf_put,raw_get,raw_put,sges_get,sges_put};
static void unused_handler(urpc_sge_t*,uint32_t,void*,urpc_sge_t**,uint32_t*) {}
struct Endpoint { uint32_t qid; uint64_t function; uint16_t port; };
struct Region { uint64_t address,length,generation,cookie; };
// Fixed-width endian-neutral request. Arena grants never become raw unchecked
// arbitrary-address reads: the responder checks the full registered identity.
struct Wire {
    uint64_t magic,operation,address,generation,cookie,offset,length;
};
static constexpr uint64_t MAGIC=0x5343555250433031ULL;
class Engine {
    std::thread io;
    std::mutex mutex;
    std::deque<std::function<void()>> commands;
    std::atomic<bool> stopping{false};
    std::exception_ptr fatal;
    uint64_t qh=0, client_qh=0, fid=0, peer_fid=0, seq=0;
    uint64_t remote_qh=0, responses_outstanding=0;
    uint32_t remote_qid=0;
    uint32_t chid=URPC_U32_FAIL;
    urpc_host_info_t peer{};
    bool attached=false, ready_rpc=false;
    std::atomic<bool> closed{false};
    std::map<uint64_t,Region> regions;
    struct Response { bool done=false; int status=0; std::vector<uint8_t> bytes; };
    Response* waiting=nullptr;
    void post_receive(uint64_t handle) {
        urpc_sge_t* s=nullptr; uint32_t n=0;
        if(buf_get(&s,&n,SLOT,nullptr)!=0) {
            stopping=true;throw std::runtime_error("receive staging exhausted");
        }
        if(urpc_queue_rx_post(handle,s,n)!=0) {
            stopping=true;
            throw std::runtime_error("receive repost failed; staging retained");
        }
    }
    void setup(const std::string& eid,const std::string& ip,uint16_t port,const std::string& dir) {
        urpc_config_t cfg{};
        cfg.role=URPC_ROLE_SERVER_CLIENT;
        cfg.feature=URPC_FEATURE_DISABLE_TOKEN_POLICY;
        cfg.trans_info_num=1; cfg.trans_info[0].trans_mode=URPC_TRANS_MODE_UB;
        cfg.trans_info[0].assign_mode=DEV_ASSIGN_MODE_EID;
        check(urma_str_to_eid(const_cast<char*>(eid.c_str()),
            reinterpret_cast<urma_eid_t*>(&cfg.trans_info[0].ub.eid))==0,"EID parse");
        cfg.unix_domain_file_path=const_cast<char*>(dir.c_str());
        check(urpc_init(&cfg)==0,"urpc_init");
        pool=static_cast<uint8_t*>(aligned_alloc(4096,SLOT*SLOTS));
        check(pool!=nullptr,"RPC pool allocation");
        pool_mh=urpc_mem_seg_register(reinterpret_cast<uint64_t>(pool),SLOT*SLOTS);
        check(pool_mh!=0 && pool_mh!=URPC_U64_FAIL,"urpc_mem_seg_register");
        for(size_t i=0;i<SLOTS;i++) free_slots.push_back(i);
        check(urpc_allocator_register(&allocator)==0,"urpc_allocator_register");
        urpc_qcfg_create_t q{};
        q.create_flag=QCREATE_FLAG_RX_BUF_SIZE|QCREATE_FLAG_RX_DEPTH|QCREATE_FLAG_TX_DEPTH;
        q.rx_buf_size=SLOT; q.rx_depth=64; q.tx_depth=64;
        qh=urpc_queue_create(QUEUE_TRANS_MODE_JETTY,&q);
        check(qh!=0 && qh!=URPC_U64_FAIL,"urpc_queue_create");
        client_qh=urpc_queue_create(QUEUE_TRANS_MODE_JETTY,&q);
        check(client_qh!=0 && client_qh!=URPC_U64_FAIL,"create client queue");
        for (uint64_t handle : {qh,client_qh}) {
            for (unsigned i=0;i<64;i++) {
                urpc_sge_t* s=nullptr; uint32_t n=0;
                check(buf_get(&s,&n,SLOT,nullptr)==0,"initial receive allocation");
                check(urpc_queue_rx_post(handle,s,n)==0,"initial receive post");
            }
        }
        urpc_handler_info_t fn{};
        fn.type=URPC_HANDLER_SYNC; fn.sync_handler=unused_handler;
        snprintf(fn.name,sizeof(fn.name),"scache_granted_region_rw");
        check(urpc_func_register(&fn,&fid)==0,"urpc_func_register");
        urpc_control_plane_config_t cp{};
        cp.server.server_type=SERVER_TYPE_IPV4; cp.server.ipv4.port=port;
        snprintf(cp.server.ipv4.ip_addr,sizeof(cp.server.ipv4.ip_addr),"%s",ip.c_str());
        check(urpc_server_start(&cp)==0,"urpc_server_start");
        ready_rpc=true;
        fprintf(stderr,"SCACHE_URPC_ENGINE_READY port=%u function=%llu staging=%zu rawChunk=%zu\n",
            port,(unsigned long long)fid,SLOT*SLOTS,CHUNK);
    }
    void receive(urpc_poll_msg_t& m) {
        urpc_sge_t* rsp=nullptr; uint32_t nr=0;
        const uint32_t rh=urpc_hdr_size_get(URPC_REQ,0), sh=urpc_hdr_size_get(URPC_RSP,0);
        int status=-2; uint64_t size=0,op=0; Region* region=nullptr; uint64_t offset=0;
        if(m.req_recved.func_id==fid && m.req_recved.args_sge_num==1 &&
           m.req_recved.arg_valid_total_size>=rh+sizeof(Wire)) {
            Wire w; memcpy(&w,reinterpret_cast<void*>(m.req_recved.args[0].addr+rh),sizeof(w));
            op=be64toh(w.operation); size=be64toh(w.length); offset=be64toh(w.offset);
            auto it=regions.find(be64toh(w.address));
            if(be64toh(w.magic)==MAGIC && (op==1||op==2) && size<=CHUNK &&
               it!=regions.end() && it->second.generation==be64toh(w.generation) &&
               it->second.cookie==be64toh(w.cookie) && offset<=it->second.length &&
               size<=it->second.length-offset &&
               (op!=2 || m.req_recved.arg_valid_total_size>=rh+sizeof(Wire)+size)) {
                status=0; region=&it->second;
            }
        }
        check(buf_get(&rsp,&nr,sh+8+(status==0&&op==1?size:0),nullptr)==0,"response allocation");
        uint64_t code=htobe64(static_cast<uint64_t>(status));
        memcpy(reinterpret_cast<void*>(rsp[0].addr+sh),&code,8);
        if(status==0) {
            uint8_t* a=reinterpret_cast<uint8_t*>(region->address+offset);
            if(op==1) memcpy(reinterpret_cast<void*>(rsp[0].addr+sh+8),a,size);
            else memcpy(a,reinterpret_cast<void*>(m.req_recved.args[0].addr+rh+sizeof(Wire)),size);
        }
        urpc_return_wr_t wr{}; wr.rsps=rsp; wr.rsps_sge_num=nr;
        urpc_return_option_t option{};
        if(urpc_func_return(qh,m.req_recved.req_ctx,&wr,&option)!=0) {
            // Submission ownership is ambiguous: retain staging, fail closed.
            throw std::runtime_error("urpc_func_return failed; staging quarantined");
        }
        ++responses_outstanding;
        buf_put(m.req_recved.args,m.req_recved.args_sge_num,nullptr);
    }
    void poll_one(uint64_t handle,uint32_t channel) {
        urpc_poll_option_t option{}; option.urpc_qh=handle;
        urpc_poll_msg_t msgs[8]{};
        int n=urpc_func_poll(channel,&option,msgs,8);
        check(n>=0,"urpc_func_poll");
        for(int i=0;i<n;i++) {
            auto& m=msgs[i];
            switch(m.event) {
            case POLL_EVENT_REQ_RECVED: receive(m); post_receive(handle); break;
            case POLL_EVENT_RSP_SENDED:
                buf_put(m.rsp_sended.rsps,m.rsp_sended.rsps_sge_num,nullptr);
                check(responses_outstanding>0,"untracked response completion");
                --responses_outstanding; break;
            case POLL_EVENT_REQ_RSPED: {
                check(waiting!=nullptr,"unexpected RPC response");
                auto& r=m.req_rsped; uint32_t h=urpc_hdr_size_get(URPC_RSP,0);
                check(r.rsps && r.rsps_sge_num==1 && r.rsp_valid_total_size>=h+8,"invalid RPC response");
                uint64_t status; memcpy(&status,reinterpret_cast<void*>(r.rsps[0].addr+h),8);
                waiting->status=static_cast<int64_t>(be64toh(status));
                uint8_t* b=reinterpret_cast<uint8_t*>(r.rsps[0].addr+h+8);
                waiting->bytes.assign(b,b+r.rsp_valid_total_size-h-8);
                if(r.args) buf_put(r.args,r.args_sge_num,nullptr);
                buf_put(r.rsps,r.rsps_sge_num,nullptr);
                waiting->done=true; post_receive(handle); break;
            }
            case POLL_EVENT_REQ_SENDED: buf_put(m.req_sended.args,m.req_sended.args_sge_num,nullptr); break;
            default: throw std::runtime_error("unexpected URPC event="+std::to_string(m.event));
            }
        }
    }
    void poll() {
        poll_one(qh,URPC_U32_FAIL);
        poll_one(client_qh,chid);
    }
public:
    Endpoint endpoint{};
    Engine(std::string eid,std::string ip,uint16_t port,std::string dir) {
        std::promise<void> init; auto started=init.get_future();
        io=std::thread([this,eid,ip,port,dir,&init] {
            try {
                setup(eid,ip,port,dir);
                urpc_qcfg_get_t q{}; check(urpc_queue_cfg_get(qh,&q)==0,"urpc_queue_cfg_get");
                endpoint={q.qid,fid,port}; init.set_value();
                while(!stopping) {
                    std::function<void()> command;
                    { std::lock_guard<std::mutex> lock(mutex);
                      if(!commands.empty()){command=std::move(commands.front());commands.pop_front();} }
                    if(command) command();
                    if (!stopping) poll();
                    std::this_thread::sleep_for(std::chrono::microseconds(50));
                }
            } catch(...) {
                fatal=std::current_exception();
                try {std::rethrow_exception(fatal);}
                catch(const std::exception& e){fprintf(stderr,"SCACHE_URPC_IO_FATAL %s\n",e.what());}
                catch(...){fprintf(stderr,"SCACHE_URPC_IO_FATAL unknown\n");}
                try {init.set_exception(fatal);} catch(...) {}
                stopping=true;
            }
        });
        try {started.get();} catch(...) {if(io.joinable())io.join();throw;}
    }
    ~Engine(){
        stopping=true;if(io.joinable())io.join();
        // Explicit close is required. Failed close deliberately retains staging.
    }
    bool healthy() const {return !stopping && !closed;}
    bool is_closed() const {return closed;}
    template<class F> auto execute(F f)->decltype(f()) {
        using Result=decltype(f());
        auto task=std::make_shared<std::packaged_task<Result()>>(std::move(f));
        auto result=task->get_future();
        { std::lock_guard<std::mutex> lock(mutex);
          if(stopping) {if(fatal)std::rethrow_exception(fatal);throw std::runtime_error("engine stopped");}
          commands.emplace_back([task]{(*task)();}); }
        if(result.wait_for(std::chrono::seconds(40))!=std::future_status::ready)
            throw std::runtime_error("URPC I/O thread deadline; retain engine and arena");
        return result.get();
    }
    void connect(std::string ip,Endpoint remote) { execute([=] {
        check(!attached,"only one peer per experimental engine");
        chid=urpc_channel_create(); check(chid!=URPC_U32_FAIL,"urpc_channel_create");
        peer.host_type=HOST_TYPE_IPV4; peer.ipv4.port=remote.port;
        snprintf(peer.ipv4.ip_addr,sizeof(peer.ipv4.ip_addr),"%s",ip.c_str());
        urpc_channel_connect_option_t o{};
        o.flag=URPC_CHANNEL_CONN_FLAG_TIMEOUT; o.timeout=15000;
        check(urpc_channel_server_attach(chid,&peer,&o)==0,"urpc_channel_server_attach");
        urpc_channel_queue_attr_t a{}; a.type=CHANNEL_QUEUE_TYPE_LOCAL;
        check(urpc_channel_queue_add(chid,client_qh,a,&o)==0,"add local queue");
        a.type=CHANNEL_QUEUE_TYPE_REMOTE;
        check(urpc_channel_queue_add(chid,remote.qid,a,&o)==0,"add remote queue");
        urpc_channel_qinfos_t qs{};
        check(urpc_channel_queue_query(chid,&qs)==0 && qs.r_qnum>0,"query channel queues");
        remote_qh=qs.r_qinfo[0].urpc_qh;
        remote_qid=remote.qid;
        check(urpc_channel_queue_pair(chid,client_qh,qs.r_qinfo[0].urpc_qh,&o)==0,"pair queues");
        peer_fid=remote.function; attached=true;
    });}
    Region grant(void* address,uint64_t n) { return execute([=] {
        const uint64_t a=reinterpret_cast<uint64_t>(address);
        check(a!=0 && n>0 && n<=UINT64_MAX-a,"invalid arena range");
        check(regions.find(a)==regions.end(),"duplicate arena grant");
        for (const auto& item:regions) {
            const auto& r=item.second;
            check(a>=r.address ? a-r.address>=r.length : r.address-a>=n,"overlapping arena grant");
        }
        uint64_t g=++seq;
        Region r{a,n,g,static_cast<uint32_t>(g^0x73636163u)}; regions.emplace(a,r); return r;
    });}
    void revoke(uint64_t address) {execute([=]{check(regions.erase(address)==1,"unknown arena");});}
    void validate_local(void* address,uint64_t n) {
        execute([=] {
            uint64_t a=reinterpret_cast<uint64_t>(address);
            for(const auto& item:regions) {
                const auto& r=item.second;
                if(a>=r.address && a-r.address<=r.length && n<=r.length-(a-r.address))return;
            }
            throw std::runtime_error("local range is not granted to this URPC transport");
        });
    }
    std::vector<uint8_t> request(Region r,uint64_t offset,size_t n,const uint8_t* write_data=nullptr) {
        std::vector<uint8_t> write_copy;
        if(write_data) write_copy.assign(write_data,write_data+n);
        return execute([=] {
            check(attached && !waiting && n>0 && n<=CHUNK,"invalid/not-connected RPC request");
            const uint32_t h=urpc_hdr_size_get(URPC_REQ,0);
            urpc_sge_t* args=nullptr; uint32_t num=0;
            bool write=!write_copy.empty();
            check(buf_get(&args,&num,h+sizeof(Wire)+(write?n:0),nullptr)==0,"request allocation");
            Wire w{htobe64(MAGIC),htobe64(write?2:1),htobe64(r.address),htobe64(r.generation),
                htobe64(r.cookie),htobe64(offset),htobe64(n)};
            memcpy(reinterpret_cast<void*>(args[0].addr+h),&w,sizeof(w));
            if(write)memcpy(reinterpret_cast<void*>(args[0].addr+h+sizeof(w)),write_copy.data(),n);
            urpc_call_wr_t wr{}; wr.func_id=peer_fid; wr.args=args; wr.args_num=num;
            urpc_call_option_t o{}; o.option_flag=FUNC_CALL_FLAG_CALL_MODE; o.call_mode=0;
            Response response; waiting=&response;
            struct RequestGuard {
                Engine* engine;
                Response* response;
                ~RequestGuard() {
                    engine->waiting=nullptr;
                    if(!response->done) engine->stopping=true;
                }
            } guard{this,&response};
            uint64_t id=urpc_func_call(chid,&wr,&o);
            if(id==URPC_U64_FAIL) {waiting=nullptr;throw std::runtime_error("urpc_func_call failed; staging retained");}
            const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(15);
            while(!response.done) {
                poll();
                if(std::chrono::steady_clock::now()>deadline) {
                    waiting=nullptr; stopping=true;
                    throw std::runtime_error("RPC timeout; engine quarantined");
                }
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            waiting=nullptr;
            check(response.status==0,"remote rejected arena identity/bounds");
            check(response.bytes.size()==(write?0:n),"RPC payload length mismatch");
            return response.bytes;
        });
    }
    void close() {
        if(closed)return;
        execute([this] {
            check(regions.empty(),"revoke all arena grants before close");
            check(waiting==nullptr,"RPC still outstanding at close");
            auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(10);
            while(responses_outstanding) {
                poll(); check(std::chrono::steady_clock::now()<deadline,"response drain deadline");
            }
            urpc_channel_connect_option_t o{};
            o.flag=URPC_CHANNEL_CONN_FLAG_TIMEOUT; o.timeout=5000;
            if(attached) {
                check(urpc_channel_queue_unpair(chid,client_qh,remote_qh,&o)==0,"unpair at close");
                urpc_channel_queue_attr_t a{}; a.type=CHANNEL_QUEUE_TYPE_REMOTE;
                check(urpc_channel_queue_rm(chid,remote_qid,a,&o)==0,"remove remote at close");
                a.type=CHANNEL_QUEUE_TYPE_LOCAL;
                check(urpc_channel_queue_rm(chid,client_qh,a,&o)==0,"remove local at close");
                check(urpc_channel_server_detach(chid,&peer,&o)==0,"detach at close");
                check(urpc_channel_destroy(chid)==0,"destroy channel");
                chid=URPC_U32_FAIL; attached=false;
            }
            for(uint64_t handle : {client_qh,qh}) {
            check(urpc_queue_modify(handle,QUEUE_STATUS_ERR)==0,"set queue error for flush");
            bool flush_done=false;
            while(!flush_done) {
                urpc_poll_option_t option{};option.urpc_qh=handle;
                urpc_poll_msg_t msg{};
                int n=urpc_func_poll(URPC_U32_FAIL,&option,&msg,1);
                check(n>=0,"close poll");
                if(n>0) {
                    if(msg.event==POLL_EVENT_REQ_ERR) {
                        if(msg.req_err.args)buf_put(msg.req_err.args,msg.req_err.args_sge_num,nullptr);
                        flush_done=msg.req_err.err_code==URPC_ERR_CR_WR_FLUSH_ERR_DONE;
                    } else if(msg.event==POLL_EVENT_ERR) {
                        if(msg.event_err.args)buf_put(msg.event_err.args,msg.event_err.args_sge_num,nullptr);
                    } else check(false,"unexpected close event");
                }
                check(std::chrono::steady_clock::now()<deadline,"queue flush deadline");
            }
            int result;
            while((result=urpc_queue_destroy(handle))!=0) {
                check(std::chrono::steady_clock::now()<deadline,"peer still owns queue at close");
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            }
            qh=client_qh=0;
            check(urpc_func_unregister(fid)==0,"unregister function");
            {
                std::lock_guard<std::mutex> lock(pool_mutex);
                check(free_slots.size()==SLOTS,"RPC staging buffers still owned at close");
            }
            check(urpc_mem_seg_unregister(pool_mh)==0,"unregister staging");
            check(urpc_allocator_unregister()==0,"unregister allocator");
            urpc_uninit();
            free(pool);pool=nullptr;pool_mh=0;free_slots.clear();
            closed=true;stopping=true;
            fprintf(stderr,"SCACHE_URPC_CLOSE_PASS staging_live=0 arena_grants=0\n");
        });
        if(io.joinable())io.join();
    }
};
}
#ifdef SCACHE_URPC_SMOKE_MAIN
#include <fstream>
#include <iostream>
int main(int argc,char** argv) {
    try {
        if(argc!=6)throw std::runtime_error("usage: smoke server|client EID PORT SOCKET_DIR PEER_INFO");
        bool server=std::string(argv[1])=="server";
        scache_urpc::Engine engine(argv[2],"127.0.0.1",static_cast<uint16_t>(std::stoi(argv[3])),argv[4]);
        std::vector<uint8_t> arena(65536);
        for(size_t i=0;i<arena.size();i++)arena[i]=static_cast<uint8_t>((i*31+7)&255);
        auto region=engine.grant(arena.data(),arena.size());
        if(server) {
            std::ofstream info(argv[5]);
            info<<engine.endpoint.qid<<" "<<engine.endpoint.function<<" "<<engine.endpoint.port<<" "
                <<region.address<<" "<<region.length<<" "<<region.generation<<" "<<region.cookie<<std::endl;
            info.close();
            std::this_thread::sleep_for(std::chrono::seconds(5));
        } else {
            scache_urpc::Endpoint ep{};
            std::ifstream info(argv[5]);
            info>>ep.qid>>ep.function>>ep.port>>region.address>>region.length>>region.generation>>region.cookie;
            scache_urpc::check(info.good()||info.eof(),"peer descriptor read");
            engine.connect("127.0.0.1",ep);
            auto bytes=engine.request(region,123,4096);
            for(size_t i=0;i<bytes.size();i++) scache_urpc::check(bytes[i]==arena[123+i],"READ integrity");
            std::vector<uint8_t> payload(4096,0x5a);
            engine.request(region,8192,payload.size(),payload.data());
            auto readback=engine.request(region,8192,payload.size());
            scache_urpc::check(readback==payload,"WRITE integrity");
            std::cout<<"SCACHE_OFFICIAL_URPC_READ_WRITE_PASS bytes=12288"<<std::endl;
        }
        engine.revoke(reinterpret_cast<uint64_t>(arena.data()));
        engine.close();
        return 0;
    } catch(const std::exception& e) {fprintf(stderr,"URPC_BRIDGE_ERROR %s\n",e.what());return 1;}
}
#endif
