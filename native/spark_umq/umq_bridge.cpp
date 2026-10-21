// SPDX-License-Identifier: Apache-2.0
// Official June-24 UMQ UB/RM/CTP backend; bounded request/response over messages.
// Application arenas are software-authorized, not exposed as one-sided DMA MR.
#include <endian.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>
extern "C" {
#include "umq_api.h"
}
namespace scache_umq {
static constexpr size_t CHUNK=3840;
static constexpr uint64_t MAGIC=0x5343554d51303031ULL;
static void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
struct Region{uint64_t address,length,generation,cookie;};
struct Wire{uint64_t magic,kind,seq,address,length,generation,cookie,offset,bytes,status;};
class Engine {
    std::thread io;
    std::mutex mutex;
    std::deque<std::function<void()>> commands;
    std::atomic<bool> stopping{false},good{true},closed{false};
    uint64_t qh=0,sequence=0,generation=0;
    bool initialized=false,bound=false;
    std::map<uint64_t,Region> regions;
    bool waiting=false,response_ready=false;
    uint64_t wanted_seq=0,response_status=0;
    std::vector<uint8_t> response;
    void initialize(const std::string& device,unsigned eid,unsigned priority){
        umq_log_config_t logging{};logging.log_flag=UMQ_LOG_FLAG_FUNC|UMQ_LOG_FLAG_LEVEL;logging.level=UMQ_LOG_LEVEL_DEBUG;
        logging.func=[](int,char* message){fprintf(stderr,"[official-umq] %s",message);};
        umq_log_config_set(&logging);
        umq_init_cfg_t cfg{};
        cfg.buf_mode=UMQ_BUF_SPLIT;cfg.feature=UMQ_FEATURE_API_BASE;
        cfg.io_lock_free=true;cfg.trans_info_num=1;
        cfg.trans_info[0].trans_mode=UMQ_TRANS_MODE_UB;
        auto& dev=cfg.trans_info[0].dev_info;dev.assign_mode=UMQ_DEV_ASSIGN_MODE_DEV;
        check(device.size()<sizeof(dev.dev.dev_name),"device name too long");
        strcpy(dev.dev.dev_name,device.c_str());dev.dev.eid_idx=eid;
        cfg.block_cfg.small_block_size=BLOCK_SIZE_4K;
        cfg.buf_pool_cfg.umq_buf_pool_max_size=2ULL*1024*1024*1024;
        cfg.buf_pool_cfg.expansion_block_count=512;
        cfg.buf_pool_cfg.tls_qbuf_pool_depth=128;
        cfg.buf_pool_cfg.tls_expand_qbuf_pool_depth=64;
        check(umq_init(&cfg)==0,"official June-24 umq_init failed");initialized=true;
        umq_create_option_t option{};option.trans_mode=UMQ_TRANS_MODE_UB;option.dev_info=dev;
        strcpy(option.name,"scache-rm-ctp");
        option.create_flag=UMQ_CREATE_FLAG_RX_BUF_SIZE|UMQ_CREATE_FLAG_TX_BUF_SIZE|
            UMQ_CREATE_FLAG_RX_DEPTH|UMQ_CREATE_FLAG_TX_DEPTH|UMQ_CREATE_FLAG_TP_MODE|
            UMQ_CREATE_FLAG_TP_TYPE|UMQ_CREATE_FLAG_PRIORITY;
        option.rx_buf_size=4096;option.tx_buf_size=4096;option.rx_depth=64;option.tx_depth=64;
        option.tp_mode=UMQ_TM_RM;option.tp_type=UMQ_TP_TYPE_CTP;option.priority=priority;
        qh=umq_create(&option);check(qh!=0,"official umq_create RM/CTP failed");
        bind_info.resize(1024*1024);
        uint32_t n=umq_bind_info_get(qh,bind_info.data(),bind_info.size());
        check(n>0&&n<=bind_info.size(),"official umq_bind_info_get failed");bind_info.resize(n);
        fprintf(stderr,"SCACHE_UMQ_ENGINE_READY mode=RM tp=CTP device=%s priority=%u bind_bytes=%u raw=%zu\n",
            device.c_str(),priority,n,CHUNK);
    }
    void send(const Wire& w,const uint8_t* payload,size_t n){
        check(bound,"UMQ is not bound");
        const size_t bytes=sizeof(w)+n;
        check(bytes<=3968,"UMQ message exceeds 4KiB pool payload");
        umq_buf_t* buf=umq_buf_alloc(bytes,1,qh,nullptr);
        check(buf!=nullptr,"umq_buf_alloc failed");
        memcpy(buf->buf_data,&w,sizeof(w));if(n)memcpy(buf->buf_data+sizeof(w),payload,n);
        buf->io_direction=UMQ_IO_TX;buf->data_size=bytes;buf->total_data_size=bytes;
        umq_buf_t* bad=nullptr;
        if(umq_enqueue(qh,buf,&bad)!=0){
            if(bad==buf)umq_buf_free(bad);else good=false;
            throw std::runtime_error("official umq_enqueue failed; no fallback");
        }
    }
    void receive(const uint8_t* data,size_t n){
        check(n>=sizeof(Wire),"short UMQ wire header");
        Wire w;memcpy(&w,data,sizeof(w));
        check(be64toh(w.magic)==MAGIC,"UMQ wire magic mismatch");
        uint64_t kind=be64toh(w.kind),seq=be64toh(w.seq),bytes=be64toh(w.bytes);
        check(bytes<=CHUNK,"UMQ wire payload too large");
        if(kind==3){
            check(waiting&&seq==wanted_seq,"unexpected UMQ response identity");
            response_status=be64toh(w.status);
            check(n==sizeof(Wire)+bytes,"UMQ response length mismatch");
            response.assign(data+sizeof(Wire),data+n);response_ready=true;return;
        }
        check(kind==1||kind==2,"unknown UMQ request kind");
        auto it=regions.find(be64toh(w.address));uint64_t off=be64toh(w.offset);
        bool valid=it!=regions.end()&&it->second.length==be64toh(w.length)&&
            it->second.generation==be64toh(w.generation)&&it->second.cookie==be64toh(w.cookie)&&
            off<=it->second.length&&bytes<=it->second.length-off&&bytes>0&&
            n==sizeof(Wire)+(kind==2?bytes:0);
        Wire reply{};reply.magic=htobe64(MAGIC);reply.kind=htobe64(3);reply.seq=w.seq;
        reply.status=htobe64(valid?0:1);reply.bytes=htobe64(valid&&kind==1?bytes:0);
        const uint8_t* source=nullptr;
        if(valid){
            auto* ptr=reinterpret_cast<uint8_t*>(it->second.address+off);
            if(kind==2)memcpy(ptr,data+sizeof(Wire),bytes);else source=ptr;
        }
        send(reply,source,valid&&kind==1?bytes:0);
    }
    void poll(){
        if(!bound)return;
        for(unsigned i=0;i<8;++i){
            umq_buf_t* b=umq_dequeue(qh);if(!b)return;
            // June-24 umq_ub_dequeue_with_poll_rx merges multiple received
            // messages into qbuf_next. A next pointer alone is not fragmentation.
            while(b){
                umq_buf_t* next=b->qbuf_next;b->qbuf_next=nullptr;
                try {
                    check(b->status==0,"UMQ receive completion error");
                    check(b->total_data_size==b->data_size,
                        "unexpected fragmented UMQ message");
                    receive(reinterpret_cast<uint8_t*>(b->buf_data),b->data_size);
                }catch(...){
                    umq_buf_free(b);if(next)umq_buf_free(next);good=false;throw;
                }
                umq_buf_free(b);b=next;
            }
        }
    }
    void execute(std::function<void()> fn){
        check(good.load()&&!closed.load(),"UMQ runtime unhealthy or closed");
        auto task=std::make_shared<std::packaged_task<void()>>(std::move(fn));auto f=task->get_future();
        {std::lock_guard<std::mutex> lock(mutex);commands.emplace_back([task]{(*task)();});}
        f.get();
    }
public:
    std::vector<uint8_t> bind_info;
    Engine(const std::string& device,unsigned eid,unsigned priority){
        auto ready=std::make_shared<std::promise<void>>();auto f=ready->get_future();
        io=std::thread([this,device,eid,priority,ready]{
            try{initialize(device,eid,priority);ready->set_value();}
            catch(...){good=false;ready->set_exception(std::current_exception());return;}
            while(!stopping.load()){
                std::function<void()> job;
                {std::lock_guard<std::mutex> lock(mutex);if(!commands.empty()){job=std::move(commands.front());commands.pop_front();}}
                if(job)job();
                try{poll();}catch(const std::exception& e){good=false;fprintf(stderr,"SCACHE_UMQ_FATAL %s\n",e.what());}
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        });
        try{f.get();}catch(...){stopping=true;if(io.joinable())io.join();throw;}
    }
    ~Engine(){if(io.joinable()){stopping=true;io.join();}}
    bool healthy()const{return good.load();}bool is_closed()const{return closed.load();}
    void connect(const std::vector<uint8_t>& remote){execute([&]{
        check(!bound,"UMQ supports one fixed peer");
        check(umq_bind(qh,const_cast<uint8_t*>(remote.data()),remote.size())==0,"official umq_bind RM/CTP failed");
        bound=true;fprintf(stderr,"SCACHE_UMQ_CONNECTED mode=RM tp=CTP\n");
    });}
    Region grant(void* ptr,uint64_t n){Region result{};execute([&]{
        uint64_t base=reinterpret_cast<uint64_t>(ptr);check(ptr&&n&&base<=UINT64_MAX-n,"invalid arena");
        for(const auto& x:regions){uint64_t a=x.second.address,l=x.second.length;
            check(base>=a?base-a>=l:a-base>=n,"overlapping arena grant");}
        result={base,n,++generation,(generation*2654435761ULL)&0xffffffffULL};
        regions.emplace(base,result);
    });return result;}
    void revoke(uint64_t address){execute([&]{check(regions.erase(address)==1,"unknown arena grant");});}
    void validate_local(const void* p,uint64_t n){execute([&]{
        uint64_t address=reinterpret_cast<uint64_t>(p);bool found=false;
        for(const auto& x:regions)if(address>=x.first&&n<=x.second.length&&address-x.first<=x.second.length-n)found=true;
        check(found,"local window is not authorized");
    });}
    std::vector<uint8_t> request(const Region& r,uint64_t off,size_t n,const void* source){
        std::vector<uint8_t> out;execute([&]{
            check(bound&&n>0&&n<=CHUNK&&off<=r.length&&n<=r.length-off,"invalid UMQ request window");
            check(!waiting,"one UMQ request may be outstanding");
            Wire w{};w.magic=htobe64(MAGIC);w.kind=htobe64(source?2:1);w.seq=htobe64(++sequence);
            w.address=htobe64(r.address);w.length=htobe64(r.length);w.generation=htobe64(r.generation);
            w.cookie=htobe64(r.cookie);w.offset=htobe64(off);w.bytes=htobe64(n);
            wanted_seq=sequence;waiting=true;response_ready=false;
            try{
                send(w,static_cast<const uint8_t*>(source),source?n:0);
                auto end=std::chrono::steady_clock::now()+std::chrono::seconds(15);
                while(!response_ready){poll();if(std::chrono::steady_clock::now()>end){good=false;throw std::runtime_error("UMQ response timeout; retain pool");}
                    std::this_thread::sleep_for(std::chrono::microseconds(10));}
                waiting=false;check(response_status==0,"remote UMQ arena generation/cookie/window rejected");
                check(response.size()==(source?0:n),"UMQ response byte count mismatch");out=response;
            }catch(...){waiting=false;throw;}
        });return out;
    }
    void close(){
        if(closed)return;
        execute([&]{
            check(regions.empty()&&!waiting,"revoke arenas and finish requests before UMQ close");
            // Base UMQ owns TX/RX reclamation during unbind/destroy.
            if(bound){check(umq_unbind(qh)==0,"umq_unbind failed");bound=false;}
            check(umq_destroy(qh)==0,"umq_destroy failed");qh=0;
            if(initialized){umq_uninit();initialized=false;}
            closed=true;
            fprintf(stderr,"SCACHE_UMQ_CLOSE_PASS arena_grants=0 mode=RM tp=CTP\n");
        });stopping=true;if(io.joinable())io.join();
    }
};
} // namespace scache_umq
