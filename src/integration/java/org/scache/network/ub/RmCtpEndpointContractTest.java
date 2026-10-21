package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Offline protocol/configuration contract; does not load JNI or exercise hardware. */
public final class RmCtpEndpointContractTest {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
    private static void rejects(Runnable action) {
        try { action.run(); }
        catch (IllegalArgumentException | UrmaProtocolException expected) { return; }
        throw new AssertionError("invalid input was accepted");
    }
    public static void main(String[] args) {
        UrmaConnectionConfig config = new UrmaConnectionConfig("rm-ctp", 7, 6, 30000, 5000);
        check(config.transportMode == 1 && config.tpType == 1, "CLI mode confused with API mode");
        rejects(() -> new UrmaConnectionConfig("rc-ctp", 0, 6, 1, 1));
        rejects(() -> new UrmaConnectionConfig("rm-ctp", -1, 6, 1, 1));
        rejects(() -> new UrmaConnectionConfig("rm-ctp", 0, 16, 1, 1));
        rejects(() -> new UrmaConnectionConfig("rm-ctp", 0, 6, 0, 1));
        EndpointDescriptor ep = new EndpointDescriptor();
        ep.transportMode = config.transportMode;
        ep.tpType = config.tpType;
        ep.eid[0] = 11;
        ep.segmentEid[0] = 22;
        ep.segmentUasid = 9;
        ep.segmentAddress = 0x123456789L;
        ep.segmentLength = 8192;
        ep.segmentToken = 0xfedcba98;
        ep.segmentGeneration = 42;
        byte[] wire = ep.toBytes();
        check(wire.length == 88, "wire size");
        check(ByteBuffer.wrap(wire).getInt(12) == 1, "TP field offset/byte order");
        check(wire[72] == 22, "segment EID offset");
        EndpointDescriptor decoded = EndpointDescriptor.fromBytes(wire);
        check(Arrays.equals(wire, decoded.toBytes()), "roundtrip");
        check(decoded.segmentUasid == 9 && decoded.segmentUasid != config.eidIndex,
            "provider segment UASID confused with local EID index");
        RemoteBuffer unsignedToken = new RemoteBuffer(4096, 64, 0xffffffff, 1);
        check(RealUbTransport.window(unsignedToken, 0, 64).token == -1, "unsigned token bits lost");
        Object owner = new Object();
        UbRegisteredBuffer registered = new UbRegisteredBuffer(owner, owner, 1,
            ByteBuffer.allocateDirect(64), 4096, 64, 0x80000000);
        check(Integer.toUnsignedLong(registered.token) == 0x80000000L, "registered token sign rejected");
        byte[] mrEid = new byte[16]; mrEid[0] = 17;
        SegmentDescriptor mr = new SegmentDescriptor(mrEid, 42, 6, 0x10000, 4096, -1, 7);
        mrEid[0] = 99;
        check(mr.eid()[0] == 17, "caller mutated MR identity");
        check(mr.equals(SegmentDescriptor.fromBytes(mr.toBytes())), "MR wire roundtrip");
        UbRegisteredBuffer withMr = new UbRegisteredBuffer(owner, owner, 7, 7,
            ByteBuffer.allocateDirect(4096), 0x10000, 4096, -1, mr);
        check(withMr.descriptor().segment == mr, "registered descriptor dropped MR identity");
        SegmentDescriptor other = new SegmentDescriptor(new byte[16], 43, 6, 0x20000, 4096, -1, 8);
        check(!mr.equals(other), "two arena identities conflated");
        RemoteBuffer block = new RemoteBuffer(0x10080, 128, -1, 7, mr);
        check(RealUbTransport.window(block, 64, 64).segment == mr, "window replaced full MR identity");
        mr.checkWindow(0x10fff, 1);
        rejects(() -> mr.checkWindow(0x10fff, 2));
        rejects(() -> new RemoteBuffer(0x10080, 128, -1, 8, mr));
        byte[] reserved = mr.toBytes(); reserved[44] = 1;
        rejects(() -> SegmentDescriptor.fromBytes(reserved));
        check(decoded.eid[0] != decoded.segmentEid[0], "endpoint/segment identity conflated");
        rejects(() -> EndpointDescriptor.fromBytes(Arrays.copyOf(wire, 72)));
        byte[] oldVersion = wire.clone();
        ByteBuffer.wrap(oldVersion).putInt(1);
        rejects(() -> EndpointDescriptor.fromBytes(oldVersion));
        byte[] badMode = wire.clone();
        ByteBuffer.wrap(badMode).putInt(56, 2);
        rejects(() -> EndpointDescriptor.fromBytes(badMode));
        for (java.lang.reflect.Method method : UrmaTransport.class.getDeclaredMethods()) {
            int modifiers = method.getModifiers();
            if (java.lang.reflect.Modifier.isPublic(modifiers) &&
                !java.lang.reflect.Modifier.isStatic(modifiers) &&
                !method.getName().equals("isClosed") && !method.getName().equals("deviceName")) {
                check(java.lang.reflect.Modifier.isSynchronized(modifiers),
                    "unprotected native handle method: " + method.getName());
            }
        }
        System.out.println("RM_CTP_ENDPOINT_CONTRACT_PASS (offline; no hardware)");
    }
}
