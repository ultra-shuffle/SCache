package org.scache.network.ub;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Experimental official URPC READ/WRITE RPC backend. This is NOT one-sided DMA:
 * the responder copies an authorized arena window through registered staging.
 * The pinned official framework uses RC; no implicit RM/CTP claim or fallback.
 */
public final class UrpcUbTransport implements UbTransport {
    private static final int MAGIC = 0x55525043, VERSION = 1, RAW = 32768;
    private final Map<Long, UbRegisteredBuffer> registrations = new HashMap<>();
    private final LinkedHashMap<Long, Integer> results = new LinkedHashMap<>();
    private final byte[] identity = new byte[16], endpoint;
    private byte[] peerIdentity, connected;
    private long nextRequest = 1, submitted, completed, failed, submittedBytes, completedBytes, failedBytes;
    private long rpcAttempted, rpcCompleted, rpcFailed, readBytes, writeBytes, copies;
    private boolean closed;
    private final int queueDepth;
    private int rawLimit;

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException("Missing " + name);
        return value.trim();
    }
    public static UrpcUbTransport open(int queueDepth, int maxChunkBytes, UrmaConnectionConfig config) {
        if (!"rc-rtp".equals(config.mode) || config.eidIndex != 0 || config.priority != 0)
            throw new IllegalArgumentException(
                "Pinned experimental URPC framework requires explicit rc-rtp, EID index 0 and priority 0; not hardware RM/CTP");
        return new UrpcUbTransport(queueDepth, maxChunkBytes);
    }
    private UrpcUbTransport(int queueDepth, int maxChunkBytes) {
        if (queueDepth < 1) throw new IllegalArgumentException("queueDepth must be positive");
        this.queueDepth = 1; // One blocking logical operation; native RX/TX rings are separate.
        if (maxChunkBytes < 1) throw new IllegalArgumentException("maxChunkBytes must be positive");
        this.rawLimit = Math.min(RAW, maxChunkBytes);
        UUID uuid = UUID.randomUUID();
        ByteBuffer.wrap(identity).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        String host = requiredEnv("SCACHE_URPC_HOST");
        int port = Integer.parseInt(requiredEnv("SCACHE_URPC_PORT"));
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid URPC port");
        long[] info = UrpcNative.start(requiredEnv("SCACHE_URPC_EID"), host, port,
            requiredEnv("SCACHE_URPC_SOCKET_DIR"));
        if (info.length != 4 || info[3] != RAW)
            throw new IllegalStateException("URPC JAR/JNI raw limit mismatch; deploy the paired artifacts");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC); out.writeInt(VERSION); out.write(identity);
            out.writeUTF(host); out.writeInt(port); out.writeInt((int) info[0]);
            out.writeLong(info[1]); out.writeInt(rawLimit); out.flush();
            endpoint = bytes.toByteArray();
        } catch (IOException impossible) { throw new AssertionError(impossible); }
        System.err.println("SCACHE_TRANSPORT_SELECTED backend=official-urpc path=framework-RC stagingCopy=true fallback=false");
    }
    private void ready() { if (closed) throw new IllegalStateException("URPC transport is closed"); }
    @Override public synchronized UbTransportCapabilities capabilities() {
        return new UbTransportCapabilities("official-urpc", UbTransportCapabilities.BackendClass.OFFICIAL_URPC,
            true, true, Integer.MAX_VALUE, rawLimit, queueDepth, 1, true, false);
    }
    @Override public synchronized byte[] localEndpoint() { ready(); return endpoint.clone(); }
    @Override public synchronized void connect(byte[] bytes) {
        ready();
        if (bytes == null) throw new IllegalArgumentException("null endpoint");
        if (connected != null) {
            if (Arrays.equals(connected, bytes)) return;
            throw new IllegalStateException("Experimental URPC backend supports one fixed peer per process");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != MAGIC || in.readInt() != VERSION) throw new IOException("Not a URPC endpoint");
            byte[] peer = new byte[16]; in.readFully(peer);
            String host = in.readUTF(); int port = in.readInt(), qid = in.readInt();
            long function = in.readLong(); int raw = in.readInt();
            if (in.available() != 0 || port < 1 || port > 65535 || raw < 1 || raw > RAW || qid <= 0 ||
                    Arrays.equals(peer, identity)) throw new IOException("Invalid URPC peer");
            UrpcNative.connect(host, port, qid, function);
            rawLimit = Math.min(rawLimit, raw); peerIdentity = peer; connected = bytes.clone();
        } catch (IOException e) { throw new IllegalArgumentException("Invalid URPC endpoint", e); }
    }
    @Override public synchronized UbRegisteredBuffer registerBuffer(ByteBuffer buffer) {
        ready();
        if (buffer == null || !buffer.isDirect() || buffer.isReadOnly() || buffer.capacity() < 1)
            throw new IllegalArgumentException("URPC grant needs a nonempty direct buffer");
        long[] r = UrpcNative.grant(buffer);
        try {
            SegmentDescriptor segment = new SegmentDescriptor(identity, 0, MAGIC, r[0], r[1], (int) r[3], r[2]);
            UbRegisteredBuffer registered = new UbRegisteredBuffer(this, null, r[2], r[2], buffer,
                r[0], r[1], (int) r[3], segment);
            registrations.put(registered.handle, registered);
            return registered;
        } catch (RuntimeException | Error error) {
            registrations.remove(r[2]);
            try { UrpcNative.revoke(r[0]); }
            catch (RuntimeException | Error cleanup) { error.addSuppressed(cleanup); }
            throw error;
        }
    }
    @Override public synchronized void unregisterBuffer(UbRegisteredBuffer buffer) {
        ready(); buffer.requireOwner(this);
        if (!buffer.isRegistered() || registrations.get(buffer.handle) != buffer)
            throw new IllegalArgumentException("Unknown/revoked URPC arena");
        UrpcNative.revoke(buffer.remoteAddress);
        registrations.remove(buffer.handle); buffer.markUnregistered();
    }
    private long transfer(RemoteBuffer remote, long remoteOffset, ByteBuffer local,
                          int localOffset, int length, boolean write) {
        ready();
        if (connected == null) throw new IllegalStateException("URPC peer is not connected");
        if (remote == null || remote.segment == null || local == null || !local.isDirect() ||
            (!write && local.isReadOnly()) ||
            localOffset < 0 || length < 0 || localOffset > local.capacity() - length ||
            remoteOffset < 0 || remoteOffset > remote.length || length > remote.length - remoteOffset)
            throw new IllegalArgumentException("Invalid URPC buffer/window");
        SegmentDescriptor s = remote.segment;
        s.checkWindow(remote.remoteAddress, remote.length);
        if (!Arrays.equals(s.eid(), peerIdentity) || s.attributes != MAGIC ||
            s.token != remote.token || s.generation != remote.generation)
            throw new IllegalArgumentException("URPC region identity mismatch");
        long baseOffset = remote.remoteAddress - s.address + remoteOffset;
        long id = nextRequest++; submitted++; submittedBytes += length;
        try {
            for (int offset = 0; offset < length;) {
                int chunk = Math.min(rawLimit, length - offset);
                rpcAttempted++;
                try {
                    UrpcNative.transfer(s.address, s.length, s.generation, s.token,
                        baseOffset + offset, local, localOffset + offset, chunk, write);
                    rpcCompleted++;
                } catch (RuntimeException | Error error) {
                    rpcFailed++; throw error;
                }
                offset += chunk;
                copies += chunk;
                if (write) writeBytes += chunk; else readBytes += chunk;
            }
            completed++; completedBytes += length; results.put(id, 0);
        } catch (RuntimeException | Error error) {
            failed++; failedBytes += length; throw error;
        }
        while (results.size() > 4096) results.remove(results.keySet().iterator().next());
        return id;
    }
    @Override public synchronized long read(RemoteBuffer r, long o, ByteBuffer b, int l, int n) {
        return transfer(r, o, b, l, n, false);
    }
    @Override public synchronized long write(RemoteBuffer r, long o, ByteBuffer b, int l, int n) {
        return transfer(r, o, b, l, n, true);
    }
    @Override public synchronized int waitFor(long id, int timeoutMs) {
        if (timeoutMs <= 0 || !results.containsKey(id)) throw new IllegalArgumentException("Unknown URPC request");
        return results.get(id);
    }
    @Override public synchronized UbTransportMetrics metrics() {
        return UbTransportMetrics.builder("official-urpc")
            .operations(submitted, completed, failed, 0, 0)
            .bytes(submittedBytes, completedBytes, failedBytes, 0)
            .registeredRegions(registrations.size())
            .backendCounter("urpc.rpcCalls", rpcAttempted)
            .backendCounter("urpc.rpcCompleted", rpcCompleted)
            .backendCounter("urpc.rpcFailed", rpcFailed)
            .backendCounter("urpc.readBytes", readBytes)
            .backendCounter("urpc.writeBytes", writeBytes)
            .backendCounter("urpc.stagingCopyBytes", copies)
            .backendCounter("urpc.frameworkNative", 1)
            .backendCounter("urpc.hardwareVerified", 0)
            .backendCounter("submittedRequests", rpcAttempted)
            .backendCounter("completedRequests", rpcCompleted)
            .backendCounter("failedRequests", rpcFailed)
            .backendCounter("providerDrainedRequests", rpcCompleted)
            .backendCounter("activeTransports", closed ? 0 : 1)
            .build();
    }
    @Override public synchronized boolean isClosed() { return closed; }
    @Override public synchronized void close() {
        if (closed) return;
        if (!registrations.isEmpty()) throw new IllegalStateException("Revoke URPC arenas before close");
        UrpcNative.close(); closed = true;
    }
}
