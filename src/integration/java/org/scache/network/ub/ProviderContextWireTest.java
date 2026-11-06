package org.scache.network.ub;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;

/** Two logical peers: tests control-wire preservation, not a hardware/provider test. */
public final class ProviderContextWireTest {
    private static void check(boolean ok, String why) { if (!ok) throw new AssertionError(why); }
    private static void rejects(Runnable action) {
        try { action.run(); } catch (UrmaProtocolException expected) { return; }
        throw new AssertionError("malformed descriptor accepted");
    }
    private static byte[] exchange(DataInputStream in, DataOutputStream out, byte[] local) throws IOException {
        out.writeInt(local.length); out.write(local); out.flush();
        int n = in.readInt();
        if (n < 0 || n > 65536) throw new IOException("invalid peer length");
        byte[] peer = new byte[n]; in.readFully(peer); return peer;
    }
    public static void main(String[] args) throws Exception {
        byte[] context = new byte[4096];
        for (int i = 0; i < context.length; i++) context[i] = (byte)(i * 31);
        EndpointDescriptor ep = new EndpointDescriptor(); ep.transportMode = 1; ep.tpType = 1;
        ep.providerContext = context.clone();
        SegmentDescriptor seg = new SegmentDescriptor(new byte[16], 17, 6, 0x10000, 8192, -1, 9, context);
        context[0] ^= 1;
        check(seg.providerContext()[0] != context[0], "segment context is mutable by caller");
        byte[] returned = seg.providerContext(); returned[1] ^= 1;
        check(returned[1] != seg.providerContext()[1], "segment context accessor is mutable");
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<?> peer = executor.submit(() -> {
                    try (Socket socket = listener.accept()) {
                        socket.setSoTimeout(10000);
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        check(Arrays.equals(ep.toBytes(), EndpointDescriptor.fromBytes(exchange(in, out, ep.toBytes())).toBytes()), "peer endpoint extension lost");
                        check(seg.equals(SegmentDescriptor.fromBytes(exchange(in, out, seg.toBytes()))), "peer MR extension lost");
                    } catch (IOException e) { throw new UncheckedIOException(e); }
                });
                try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), listener.getLocalPort())) {
                    socket.setSoTimeout(10000);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    check(Arrays.equals(ep.providerContext, EndpointDescriptor.fromBytes(exchange(in, out, ep.toBytes())).providerContext), "Jetty context mismatch");
                    check(seg.equals(SegmentDescriptor.fromBytes(exchange(in, out, seg.toBytes()))), "MR context mismatch");
                }
                peer.get(15, TimeUnit.SECONDS);
            } finally { executor.shutdownNow(); }
        }
        byte[] mr = seg.toBytes();
        rejects(() -> SegmentDescriptor.fromBytes(Arrays.copyOf(mr, mr.length-1)));
        rejects(() -> SegmentDescriptor.fromBytes(Arrays.copyOf(mr, mr.length+1)));
        byte[] bad = mr.clone(); ByteBuffer.wrap(bad).putInt(56, Integer.MAX_VALUE);
        rejects(() -> SegmentDescriptor.fromBytes(bad));
        rejects(() -> EndpointDescriptor.fromBytes(new byte[88]));
        byte[] old = ep.toBytes(); ByteBuffer.wrap(old).putInt(2);
        rejects(() -> EndpointDescriptor.fromBytes(old));
        System.out.println("PROVIDER_CONTEXT_WIRE_PASS peers=2 transport=loopback-control opaque_bytes=4096 mode=RM/CTP old_protocol=rejected");
    }
}
