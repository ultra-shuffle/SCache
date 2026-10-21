// SPDX-License-Identifier: Apache-2.0
#include "umq_bridge.cpp"
#include <jni.h>
#include <dlfcn.h>
namespace {
std::mutex api_mutex;
// JVM native libraries are normally RTLD_LOCAL. The paired June-24 UMQ
// plugin resolves common utility symbols from libumq; promote that exact
// already loaded dependency, keeping it alive for the process runtime.
void expose_umq_plugin_symbols() {
    static void* handle = nullptr;
    if (handle) return;
    handle = dlopen("libumq.so", RTLD_NOW | RTLD_GLOBAL | RTLD_NOLOAD);
    if (!handle) throw std::runtime_error(
        std::string("Cannot promote paired libumq for its plugin: ") + dlerror());
}
scache_umq::Engine* runtime=nullptr;
bool ever_started=false;
void raise(JNIEnv* env,const std::exception& e) {
    const char* cls=runtime && !runtime->healthy() && !runtime->is_closed()
        ? "org/scache/network/ub/UrmaDmaUnresolvedException" : "org/scache/network/ub/UrmaException";
    jclass type=env->FindClass(cls);
    if(type)env->ThrowNew(type,e.what());
}
std::string utf(JNIEnv* env,jstring s) {
    scache_umq::check(s!=nullptr,"null string");
    const char* p=env->GetStringUTFChars(s,nullptr);
    scache_umq::check(p!=nullptr,"UTF conversion");
    std::string out(p);env->ReleaseStringUTFChars(s,p);return out;
}
scache_umq::Engine& engine() {
    scache_umq::check(runtime!=nullptr,"UMQ runtime is not initialized");return *runtime;
}
jlongArray longs(JNIEnv* env,std::initializer_list<jlong> data) {
    jlongArray a=env->NewLongArray(static_cast<jsize>(data.size()));
    if(a)env->SetLongArrayRegion(a,0,static_cast<jsize>(data.size()),data.begin());
    return a;
}
}
extern "C" JNIEXPORT jint JNICALL
Java_org_scache_network_ub_UmqNative_maxRawBytes(JNIEnv*,jclass){return scache_umq::CHUNK;}
extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_scache_network_ub_UmqNative_start(JNIEnv* env,jclass,jstring device,jint eid,jint priority) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {
        scache_umq::check(!ever_started,"One official UMQ runtime is allowed per process");
        scache_umq::check(eid>=0 && priority>=0 && priority<=15,"invalid UMQ EID/priority");
        expose_umq_plugin_symbols();
        ever_started=true;
        runtime=new scache_umq::Engine(utf(env,device),eid,priority);
        const auto& b=runtime->bind_info;
        jbyteArray out=env->NewByteArray(b.size());
        if(out)env->SetByteArrayRegion(out,0,b.size(),reinterpret_cast<const jbyte*>(b.data()));
        return out;
    } catch(const std::exception& e) {raise(env,e);return nullptr;}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UmqNative_connect(JNIEnv* env,jclass,jbyteArray input) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {
        scache_umq::check(input!=nullptr,"null UMQ bind info");
        jsize n=env->GetArrayLength(input);scache_umq::check(n>0 && n<=1048576,"UMQ bind info length");
        std::vector<uint8_t> data(n);env->GetByteArrayRegion(input,0,n,reinterpret_cast<jbyte*>(data.data()));
        if(env->ExceptionCheck())return;
        engine().connect(data);
    } catch(const std::exception& e){raise(env,e);}
}
extern "C" JNIEXPORT jlongArray JNICALL
Java_org_scache_network_ub_UmqNative_grant(JNIEnv* env,jclass,jobject buffer) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {
        void* ptr=env->GetDirectBufferAddress(buffer);
        jlong n=env->GetDirectBufferCapacity(buffer);
        scache_umq::check(ptr && n>0,"arena must be a nonempty direct buffer");
        auto r=engine().grant(ptr,static_cast<uint64_t>(n));
        return longs(env,{static_cast<jlong>(r.address),static_cast<jlong>(r.length),
            static_cast<jlong>(r.generation),static_cast<jlong>(r.cookie)});
    }catch(const std::exception& e){raise(env,e);return nullptr;}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UmqNative_revoke(JNIEnv* env,jclass,jlong address) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try{engine().revoke(static_cast<uint64_t>(address));}catch(const std::exception& e){raise(env,e);}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UmqNative_transfer(JNIEnv* env,jclass,jlong address,jlong size,jlong generation,
    jint cookie,jlong remote_offset,jobject buffer,jint local_offset,jint length,jboolean write) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {
        auto* ptr=static_cast<uint8_t*>(env->GetDirectBufferAddress(buffer));
        jlong capacity=env->GetDirectBufferCapacity(buffer);
        scache_umq::check(ptr && local_offset>=0 && length>0 && length<=static_cast<jint>(scache_umq::CHUNK) &&
            local_offset<=capacity-length && remote_offset>=0 && size>0 &&
            remote_offset<=size && length<=size-remote_offset,"invalid JNI window");
        ptr+=local_offset;
        engine().validate_local(ptr,static_cast<uint64_t>(length));
        scache_umq::Region r{static_cast<uint64_t>(address),static_cast<uint64_t>(size),
            static_cast<uint64_t>(generation),static_cast<uint32_t>(cookie)};
        auto result=engine().request(r,static_cast<uint64_t>(remote_offset),
            static_cast<size_t>(length),write?ptr:nullptr);
        if(!write)memcpy(ptr,result.data(),static_cast<size_t>(length));
    }catch(const std::exception& e){raise(env,e);}
}
extern "C" JNIEXPORT void JNICALL
Java_org_scache_network_ub_UmqNative_close(JNIEnv* env,jclass) {
    std::lock_guard<std::mutex> lock(api_mutex);
    try {engine().close();delete runtime;runtime=nullptr;}
    catch(const std::exception& e){raise(env,e);}
}
