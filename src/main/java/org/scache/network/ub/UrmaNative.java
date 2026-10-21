package org.scache.network.ub;

import java.nio.ByteBuffer;

/**
 * JNI bridge to libSparkUrma.so — wraps the C++ UrmaTransport.
 *
 * All native methods take a `long handle` (native pointer to UrmaTransport*).
 * The Java UrmaTransport class owns the handle lifecycle.
 *
 * Thread-safety: native methods are synchronized on the transport handle;
 * the C++ layer uses mutexes for internal state.
 */
public final class UrmaNative {

    private static volatile boolean loaded = false;
    private static volatile Throwable loadError = null;

    private UrmaNative() {}

    /** Load libSparkUrma.so. Call once before any native method. */
    public static synchronized void ensureLoaded() throws UnsatisfiedLinkError {
        if (loaded) return;
        try {
            System.loadLibrary("SparkUrma");
            if (nativeApiVersion() != 3) throw new UnsatisfiedLinkError("SCache requires libSparkUrma JNI API 3");
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            loadError = e;
            throw e;
        }
    }

    public static boolean isLoaded() { return loaded; }

    // ---- Lifecycle ----
    static native int nativeApiVersion();
    static native long nativeInitV2(String deviceName, int queueDepth, int maxChunkBytes, boolean strict,
                                   int mode, int tpType, int eidIndex, int priority, int requestTimeoutMs, int drainTimeoutMs);
    static native void nativeConnectV2(long handle, byte[] eid, int uasid, int jettyId,
                                     int remoteMaxChunkBytes, int segmentUasid,
                                     long segmentGeneration, long segAddr,
                                     long segLen, int segToken, int version, int mode, int tpType, byte[] segmentEid);
    static native byte[] nativeGetLocalEndpoint(long handle);
    static native long[] nativeClose(long handle);
    static native void nativeShutdownProcessRuntime();
    static native long[] nativeGetProcessRuntimeMetrics();

    // ---- Buffer management ----
    static native long[] nativeRegisterBuffer(long handle, ByteBuffer directBuffer);
    static native void nativeUnregisterBuffer(long handle, long regionHandle);
    static native void nativeReleaseCachedRemoteImports(long handle);

    // ---- Data operations ----
    static native long nativeWriteMR(long handle, long remoteAddr, long remoteLen, int remoteToken,
                                    long remoteGeneration,
                                    ByteBuffer source, int offset, int length, byte[] segment);
    static native long nativeReadMR(long handle, long remoteAddr, long remoteLen, int remoteToken,
                                   long remoteGeneration,
                                   ByteBuffer destination, int offset, int length, byte[] segment);
    static native long nativeSend(long handle, ByteBuffer message, int offset, int length);

    // ---- Chunked operations ----
    static native long nativeWriteChunkedMR(long handle, long remoteAddr, long remoteLen,
                                           int remoteToken, long remoteGeneration,
                                           ByteBuffer source, int offset, int totalLength, byte[] segment);
    static native long nativeReadChunkedMR(long handle, long remoteAddr, long remoteLen,
                                          int remoteToken, long remoteGeneration,
                                          ByteBuffer dest, int offset, int totalLength, byte[] segment);

    // ---- Completion ----
    static native int nativeWait(long handle, long requestId, int timeoutMs);

    // ---- Metrics ----
    static native long[] nativeGetMetrics(long handle);
}
