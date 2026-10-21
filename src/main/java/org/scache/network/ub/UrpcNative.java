package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Experimental JNI entry points into the official URPC framework, not UrmaNative. */
final class UrpcNative {
    private UrpcNative() {}
    static {
        String path = System.getProperty("scache.urpc.library", System.getenv("SCACHE_URPC_LIBRARY"));
        if (path == null || !Paths.get(path).isAbsolute() || !Files.isRegularFile(Paths.get(path))) {
            throw new UnsatisfiedLinkError("Set SCACHE_URPC_LIBRARY to the absolute libScacheUrpc.so path");
        }
        System.load(path);
    }
    static native long[] start(String eid, String host, int port, String socketDirectory);
    static native void connect(String host, int port, int qid, long function);
    static native long[] grant(ByteBuffer memory);
    static native void revoke(long address);
    static native void transfer(long address, long regionLength, long generation, int cookie,
                                long regionOffset, ByteBuffer local, int localOffset,
                                int length, boolean write);
    static native void close();
}
