// SPDX-License-Identifier: Apache-2.0
#include "urpc_bridge.cpp"
#include <jni.h>
namespace {
std::mutex api_mutex;
scache_urpc::Engine* runtime=nullptr;
bool ever_started=false;
void raise(JNIEnv* env,const std::exception& e) {
    const char* cls=runtime && !runtime->healthy() && !runtime->is_closed()
        ? "org/scache/network/ub/UrmaDmaUnresolvedException" : "org/scache/network/ub/UrmaException";
    jclass type=env->FindClass(cls);
    if(type)env->ThrowNew(type,e.what());
}
std::string utf(JNIEnv* env,jstring s) {
    scache_urpc::check(s!=nullptr,"null string");
    const char* p=env->GetStringUTFChars(s,nullptr);
    scache_urpc::check(p!=nullptr,"UTF conversion");
    std::string out(p);env->ReleaseStringUTFChars(s,p);return out;
}
scache_urpc::Engine& engine() {
    scache_urpc::check(runtime!=nullptr,"URPC runtime is not initialized");return *runtime;
}
jlongArray longs(JNIEnv* env,std::initializer_list<jlong> data) {
    jlongArray a=env->NewLongArray(static_cast<jsize>(data.size()));
    if(a)env->SetLongArrayRegion(a,0,static_cast<jsize>(data.size()),data.begin());
    return a;
}
}
extern "C" JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrpcNative_start(JNIEnv* env,jclass,jstring eid,jstring host,jint port,jstring dir) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {
        scache_urpc::check(!ever_started,"One official URPC runtime is allowed per process");
        auto e=utf(env,eid),h=utf(env,host),d=utf(env,dir);
        scache_urpc::check(port>0 && port<=65535,"invalid URPC port");
        ever_started=true;
        runtime=new scache_urpc::Engine(e,h,static_cast<uint16_t>(port),d);
        auto ep=runtime->endpoint;
        return longs(env,{ep.qid,static_cast<jlong>(ep.function),ep.port});
    } catch(const std::exception& e) {raise(env,e);return nullptr;}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrpcNative_connect(JNIEnv* env,jclass,jstring host,jint port,jint qid,jlong function) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {engine().connect(utf(env,host),{static_cast<uint32_t>(qid),
        static_cast<uint64_t>(function),static_cast<uint16_t>(port)});}
    catch(const std::exception& e){raise(env,e);}
}
extern "C" JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UrpcNative_grant(JNIEnv* env,jclass,jobject buffer) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {
        void* ptr=env->GetDirectBufferAddress(buffer);
        jlong n=env->GetDirectBufferCapacity(buffer);
        scache_urpc::check(ptr && n>0,"arena must be a nonempty direct buffer");
        auto r=engine().grant(ptr,static_cast<uint64_t>(n));
        return longs(env,{static_cast<jlong>(r.address),static_cast<jlong>(r.length),
            static_cast<jlong>(r.generation),static_cast<jlong>(r.cookie)});
    }catch(const std::exception& e){raise(env,e);return nullptr;}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrpcNative_revoke(JNIEnv* env,jclass,jlong address) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try{engine().revoke(static_cast<uint64_t>(address));}catch(const std::exception& e){raise(env,e);}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrpcNative_transfer(JNIEnv* env,jclass,jlong address,jlong size,jlong generation,
    jint cookie,jlong remote_offset,jobject buffer,jint local_offset,jint length,jboolean write) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {
        auto* ptr=static_cast<uint8_t*>(env->GetDirectBufferAddress(buffer));
        jlong capacity=env->GetDirectBufferCapacity(buffer);
        scache_urpc::check(ptr && local_offset>=0 && length>0 && length<=4096 &&
            local_offset<=capacity-length && remote_offset>=0 && size>0 &&
            remote_offset<=size && length<=size-remote_offset,"invalid JNI window");
        ptr+=local_offset;
        engine().validate_local(ptr,static_cast<uint64_t>(length));
        scache_urpc::Region r{static_cast<uint64_t>(address),static_cast<uint64_t>(size),
            static_cast<uint64_t>(generation),static_cast<uint32_t>(cookie)};
        auto result=engine().request(r,static_cast<uint64_t>(remote_offset),
            static_cast<size_t>(length),write?ptr:nullptr);
        if(!write)memcpy(ptr,result.data(),static_cast<size_t>(length));
    }catch(const std::exception& e){raise(env,e);}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UrpcNative_close(JNIEnv* env,jclass) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {engine().close();delete runtime;runtime=nullptr;}
    catch(const std::exception& e){raise(env,e);}
}
