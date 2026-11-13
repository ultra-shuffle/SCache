// Test-only JNI stand-in. Never packaged in a production runtime or called a UB test.
#include <jni.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
namespace {
std::mutex mutex;
std::condition_variable done;
std::atomic<bool> started{false}, released{false}, wait_error{false}, close_error{false};
std::atomic<int> active{0}, close_count{0}, early_close{0};
}
extern "C" {
JNIEXPORT void JNICALL Java_org_scache_network_ub_UrmaWaitConcurrencyTest_reset(JNIEnv*, jclass) {
    started=false; released=false; wait_error=false; close_error=false;
    active=0; close_count=0; early_close=0;
}
JNIEXPORT jboolean JNICALL Java_org_scache_network_ub_UrmaWaitConcurrencyTest_entered(JNIEnv*, jclass) { return started; }
JNIEXPORT void JNICALL Java_org_scache_network_ub_UrmaWaitConcurrencyTest_release(JNIEnv*, jclass) {
    { std::lock_guard<std::mutex> guard(mutex); released=true; }
    done.notify_all();
}
JNIEXPORT void JNICALL Java_org_scache_network_ub_UrmaWaitConcurrencyTest_failWait(JNIEnv*, jclass) { wait_error=true; }
JNIEXPORT void JNICALL Java_org_scache_network_ub_UrmaWaitConcurrencyTest_failClose(JNIEnv*, jclass) { close_error=true; }
JNIEXPORT jint JNICALL Java_org_scache_network_ub_UrmaWaitConcurrencyTest_closes(JNIEnv*, jclass) { return close_count; }
JNIEXPORT jint JNICALL Java_org_scache_network_ub_UrmaWaitConcurrencyTest_earlyCloses(JNIEnv*, jclass) { return early_close; }
JNIEXPORT jint JNICALL Java_org_scache_network_ub_UrmaNative_nativeWait(JNIEnv* env, jclass, jlong, jlong, jint) {
    active++; started=true;
    { std::unique_lock<std::mutex> guard(mutex); done.wait(guard, [] { return released.load(); }); }
    active--;
    if (wait_error) {
        env->ThrowNew(env->FindClass("org/scache/network/ub/UrmaCompletionException"), "controlled native completion failure");
        return -2;
    }
    return 0;
}
JNIEXPORT jlong JNICALL Java_org_scache_network_ub_UrmaNative_nativeSend(JNIEnv*, jclass, jlong, jobject, jint, jint) { return 42; }
JNIEXPORT jlongArray JNICALL Java_org_scache_network_ub_UrmaNative_nativeClose(JNIEnv* env, jclass, jlong) {
    close_count++;
    if (active.load()!=0) early_close++;
    if (close_error.exchange(false)) return nullptr;
    return env->NewLongArray(32);
}
}
