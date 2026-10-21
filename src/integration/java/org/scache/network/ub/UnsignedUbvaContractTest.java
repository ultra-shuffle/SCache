package org.scache.network.ub;

import java.nio.ByteBuffer;

/** Offline bounds checks for unsigned provider addresses carried in Java longs. */
public final class UnsignedUbvaContractTest {
    private static void expectReject(Runnable operation) {
        try {
            operation.run();
        } catch (UrmaProtocolException | UrmaException expected) {
            return;
        }
        throw new AssertionError("invalid unsigned address range was accepted");
    }

    public static void main(String[] args) {
        long base = Long.MIN_VALUE + 0x1000L;
        SegmentDescriptor segment = new SegmentDescriptor(new byte[16], 1, 0,
            base, 4096, 7, 1);
        RemoteBuffer full = new RemoteBuffer(base, 4096, 7, 1, segment);
        EndpointDescriptor endpoint = new EndpointDescriptor();
        endpoint.transportMode = 1;
        endpoint.tpType = 1;
        endpoint.segmentAddress = base;
        endpoint.segmentLength = 4096;
        if (EndpointDescriptor.fromBytes(endpoint.toBytes()).segmentAddress != base) {
            throw new AssertionError("endpoint high-bit UBVA changed");
        }
        RemoteBuffer window = RealUbTransport.window(full, 128, 512);
        if (window.remoteAddress != base + 128 || window.length != 4096 - 128) {
            throw new AssertionError("high-bit UBVA window changed");
        }
        UbRegisteredBuffer registered = new UbRegisteredBuffer(new Object(), null, 1, 1,
            ByteBuffer.allocateDirect(4096), base, 4096, 7, segment);
        if (registered.descriptor().remoteAddress != base) {
            throw new AssertionError("registered high-bit UBVA changed");
        }
        expectReject(() -> segment.checkWindow(base - 1, 1));
        expectReject(() -> segment.checkWindow(base + 4095, 2));
        expectReject(() -> new SegmentDescriptor(new byte[16], 1, 0, -8L, 16, 7, 1));
        System.out.println("UNSIGNED_UBVA_CONTRACT_PASS");
    }
}
