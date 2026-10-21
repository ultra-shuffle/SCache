package org.scache.network.ub;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

/** Two-JVM functional test: only endpoint metadata is exchanged through files. */
public final class UrpcTransportProbe {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
    public static void main(String[] args) throws Exception {
        boolean server = "server".equals(args[0]);
        Path file = Paths.get(args[1]);
        Path done = Paths.get(args[1] + ".done");
        UbTransport t = UbTransportFactory.open("urpc", server ? "openurma0" : "openurma1",
            64, 4096, true, UrmaConnectionConfig.legacyRc());
        ByteBuffer arena = ByteBuffer.allocateDirect(65536);
        for (int i = 0; i < arena.capacity(); i++) arena.put(i, (byte) (i * 31 + 7));
        UbRegisteredBuffer registration = t.registerBuffer(arena);
        if (server) {
            Path temp = Paths.get(file + ".partial");
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(temp))) {
                byte[] ep = t.localEndpoint();
                out.writeInt(ep.length); out.write(ep);
                out.write(registration.segment.toBytes());
            }
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!Files.exists(done)) {
                check(System.nanoTime() < deadline, "client did not finish");
                Thread.sleep(50);
            }
        } else {
            byte[] ep, seg = new byte[SegmentDescriptor.WIRE_BYTES];
            try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
                ep = new byte[in.readInt()]; in.readFully(ep); in.readFully(seg);
            }
            t.connect(ep); t.connect(ep); // identical peer is idempotent
            SegmentDescriptor s = SegmentDescriptor.fromBytes(seg);
            RemoteBuffer remote = new RemoteBuffer(s.address, s.length, s.token, s.generation, s);
            long id = t.read(remote, 123, arena, 11, 4096);
            check(t.waitFor(id, 1000) == 0, "read completion");
            for (int i = 0; i < 4096; i++)
                check(arena.get(11 + i) == (byte) ((123 + i) * 31 + 7), "read content " + i);
            for (int i = 0; i < 12500; i++) arena.put(32768 + i, (byte) (i ^ 0x5a));
            id = t.write(remote, 8192, arena, 32768, 12500);
            check(t.waitFor(id, 1000) == 0, "write completion");
            id = t.read(remote, 8192, arena, 1024, 12500);
            check(t.waitFor(id, 1000) == 0, "readback completion");
            for (int i = 0; i < 12500; i++)
                check(arena.get(1024 + i) == (byte) (i ^ 0x5a), "write/readback " + i);
            try { t.read(remote, 65530, arena, 0, 100); throw new AssertionError("bounds accepted"); }
            catch (IllegalArgumentException expected) {}
            SegmentDescriptor stale = new SegmentDescriptor(s.eid(), s.uasid, s.attributes,
                s.address, s.length, s.token, s.generation + 1);
            try {
                t.read(new RemoteBuffer(stale.address, stale.length, stale.token, stale.generation, stale),
                    0, arena, 0, 1);
                throw new AssertionError("stale identity accepted");
            } catch (UrmaDmaUnresolvedException unsafe) { throw unsafe; }
            catch (UrmaException expected) {}
            id = t.read(remote, 0, arena, 0, 1);
            check(t.waitFor(id, 1000) == 0, "transport unusable after safe rejection");
            UbTransportMetrics m = t.metrics();
            check(m.terminalReconciled(), "metrics not reconciled");
            check(m.backendName.equals("official-urpc"), "wrong backend");
            check(m.backendCounters.get("urpc.rpcCompleted") > 0, "no official RPC completions");
            System.out.println("URPC_JAVA_DATA_PASS calls=" + m.backendCounters.get("urpc.rpcCalls") +
                " completed=" + m.backendCounters.get("urpc.rpcCompleted") +
                " failed=" + m.backendCounters.get("urpc.rpcFailed") + " bytes=" + m.completedBytes);
        }
        t.unregisterBuffer(registration);
        t.close();
        check(t.isClosed(), "transport not closed");
        if (!server) Files.write(done, new byte[]{1});
        System.out.println("URPC_JAVA_CLOSE_PASS role=" + args[0]);
    }
}
