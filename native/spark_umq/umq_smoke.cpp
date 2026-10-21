// SPDX-License-Identifier: Apache-2.0
#include "umq_bridge.cpp"
#include <fstream>
#include <filesystem>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
using namespace scache_umq;
static void wait_file(const std::string& file){
    auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(45);
    while(!std::filesystem::exists(file)){
        check(std::chrono::steady_clock::now()<deadline,"peer metadata deadline");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
static void touch(const std::string& file){std::ofstream f(file);f<<"ready\n";}
int main(int argc,char** argv){
    signal(SIGSEGV,[](int){void* frames[40];int n=backtrace(frames,40);backtrace_symbols_fd(frames,n,2);_exit(139);});
    try{
        check(argc==3,"usage: umq-smoke A|B exchange-directory");
        bool a=strcmp(argv[1],"A")==0;std::string dir=argv[2];
        std::string mine=dir+(a?"/A":"/B"),peer=dir+(a?"/B":"/A");
        Engine engine(a?"openurma0":"openurma1",0,6);
        std::vector<uint8_t> arena(1024*1024);
        for(size_t i=0;i<arena.size();++i)arena[i]=static_cast<uint8_t>(i*31+7);
        auto region=engine.grant(arena.data(),arena.size());
        {
            std::ofstream f(mine+".partial",std::ios::binary);
            uint32_t n=engine.bind_info.size();f.write(reinterpret_cast<char*>(&n),sizeof(n));
            f.write(reinterpret_cast<const char*>(engine.bind_info.data()),n);
            f.write(reinterpret_cast<char*>(&region),sizeof(region));
        }
        std::filesystem::rename(mine+".partial",mine+".info");
        wait_file(peer+".info");std::ifstream f(peer+".info",std::ios::binary);
        uint32_t n=0;f.read(reinterpret_cast<char*>(&n),sizeof(n));check(n>0&&n<1024*1024,"bind info size");
        std::vector<uint8_t> info(n);f.read(reinterpret_cast<char*>(info.data()),n);
        Region remote{};f.read(reinterpret_cast<char*>(&remote),sizeof(remote));check(f.good(),"metadata read");
        engine.connect(info);touch(mine+".bound");wait_file(peer+".bound");
        if(!a){
            size_t total=70000;std::vector<uint8_t> expected(total);
            for(size_t i=0;i<total;++i)expected[i]=static_cast<uint8_t>(i^0x5a);
            for(size_t off=0;off<total;off+=CHUNK){
                size_t len=std::min(CHUNK,total-off);
                auto read=engine.request(remote,off,len,nullptr);
                for(size_t j=0;j<len;++j)check(read[j]==static_cast<uint8_t>((off+j)*31+7),"READ mismatch");
                engine.request(remote,off,len,expected.data()+off);
            }
            for(size_t off=0;off<total;off+=CHUNK){
                size_t len=std::min(CHUNK,total-off);auto read=engine.request(remote,off,len,nullptr);
                check(memcmp(read.data(),expected.data()+off,len)==0,"WRITE readback mismatch");
            }
            fprintf(stderr,"OFFICIAL_UMQ_RM_CTP_READ_WRITE_PASS bytes=%zu\n",total*3);
            touch(dir+"/data.done");
        }else wait_file(dir+"/data.done");
        engine.revoke(region.address);touch(mine+".closing");wait_file(peer+".closing");engine.close();
        return 0;
    }catch(const std::exception& e){fprintf(stderr,"UMQ_NATIVE_FAIL %s errno=%d\n",e.what(),errno);return 1;}
}
