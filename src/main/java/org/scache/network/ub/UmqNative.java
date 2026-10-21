package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Experimental JNI entry points into the official June-24 UMQ library, not UrmaNative. */
final class UmqNative {
    private UmqNative() {}
    static {
        String path = System.getProperty("scache.umq.library", System.getenv("SCACHE_UMQ_LIBRARY"));
        if (path == null || !Paths.get(path).isAbsolute() || !Files.isRegularFile(Paths.get(path))) {
            throw new UnsatisfiedLinkError("Set SCACHE_UMQ_LIBRARY to the absolute libScacheUmq.so path");
        }
        System.load(path);
    }
    static native int maxRawBytes();
    static native byte[] start(String device, int eidIndex, int priority);
    static native void connect(byte[] bindInfo);
    static native long[] grant(ByteBuffer memory);
    static native void revoke(long address);
    static native void transfer(long address, long regionLength, long generation, int cookie,
                                long regionOffset, ByteBuffer local, int localOffset,
                                int length, boolean write);
    static native void close();
}
