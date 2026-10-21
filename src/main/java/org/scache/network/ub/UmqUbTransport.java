package org.scache.network.ub;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Experimental official UMQ READ/WRITE RPC backend. This is NOT one-sided DMA:
 * the responder copies an authorized arena window through registered staging.
 * Uses official June-24 UMQ UB/RM/CTP; no RC or TCP data fallback.
 */
public final class UmqUbTransport implements UbTransport {
    private static final int MAGIC = 0x554d5131, VERSION = 1, RAW = 3840;
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
    public static UmqUbTransport open(String device, int queueDepth, int maxChunkBytes, UrmaConnectionConfig config) {
        if (!"rm-ctp".equals(config.mode) || config.eidIndex < 0 || config.priority < 0 || config.priority > 15)
            throw new IllegalArgumentException("Official June-24 UMQ backend requires explicit RM/CTP and valid EID/priority");
        return new UmqUbTransport(device, queueDepth, maxChunkBytes, config);
    }
    private UmqUbTransport(String device, int queueDepth, int maxChunkBytes, UrmaConnectionConfig config) {
        if (queueDepth < 1 || maxChunkBytes < 1 || device == null || device.isEmpty())
            throw new IllegalArgumentException("Invalid UMQ device/depth/chunk");
        this.queueDepth = 1;
        this.rawLimit = Math.min(RAW, maxChunkBytes);
        if (UmqNative.maxRawBytes() != RAW) throw new IllegalStateException("UMQ JAR/JNI raw limit mismatch");
        UUID uuid = UUID.randomUUID();
        ByteBuffer.wrap(identity).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        byte[] bind = UmqNative.start(device, config.eidIndex, config.priority);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC); out.writeInt(VERSION); out.write(identity);
            out.writeInt(rawLimit); out.writeInt(bind.length); out.write(bind); out.flush();
            endpoint = bytes.toByteArray();
        } catch (IOException impossible) { throw new AssertionError(impossible); }
        System.err.println("SCACHE_TRANSPORT_SELECTED backend=official-umq path=UMQ-RM-CTP stagingCopy=true fallback=false");
    }
    private void ready() { if (closed) throw new IllegalStateException("UMQ transport is closed"); }
    @Override public synchronized UbTransportCapabilities capabilities() {
        return new UbTransportCapabilities("official-umq", UbTransportCapabilities.BackendClass.OFFICIAL_UMQ,
            true, true, Integer.MAX_VALUE, rawLimit, queueDepth, 1, true, false);
    }
    @Override public synchronized byte[] localEndpoint() { ready(); return endpoint.clone(); }
    @Override public synchronized void connect(byte[] bytes) {
        ready();
        if (bytes == null) throw new IllegalArgumentException("null endpoint");
        if (connected != null) {
            if (Arrays.equals(connected, bytes)) return;
            throw new IllegalStateException("Experimental UMQ backend supports one fixed peer per process");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != MAGIC || in.readInt() != VERSION) throw new IOException("Not a UMQ endpoint");
            byte[] peer = new byte[16]; in.readFully(peer);
            int raw = in.readInt(), length = in.readInt();
            if (raw < 1 || raw > RAW || length < 1 || length > 1048576 || length != in.available() ||
                Arrays.equals(peer, identity)) throw new IOException("Invalid UMQ peer");
            byte[] bind = new byte[length]; in.readFully(bind);
            UmqNative.connect(bind);
            rawLimit = Math.min(rawLimit, raw); peerIdentity = peer; connected = bytes.clone();
        } catch (IOException e) { throw new IllegalArgumentException("Invalid UMQ endpoint", e); }
    }
    @Override public synchronized UbRegisteredBuffer registerBuffer(ByteBuffer buffer) {
        ready();
        if (buffer == null || !buffer.isDirect() || buffer.isReadOnly() || buffer.capacity() < 1)
            throw new IllegalArgumentException("UMQ grant needs a nonempty direct buffer");
        long[] r = UmqNative.grant(buffer);
        try {
            SegmentDescriptor segment = new SegmentDescriptor(identity, 0, MAGIC, r[0], r[1], (int) r[3], r[2]);
            UbRegisteredBuffer registered = new UbRegisteredBuffer(this, null, r[2], r[2], buffer,
                r[0], r[1], (int) r[3], segment);
            registrations.put(registered.handle, registered);
            return registered;
        } catch (RuntimeException | Error error) {
            registrations.remove(r[2]);
            try { UmqNative.revoke(r[0]); }
            catch (RuntimeException | Error cleanup) { error.addSuppressed(cleanup); }
            throw error;
        }
    }
    @Override public synchronized void unregisterBuffer(UbRegisteredBuffer buffer) {
        ready(); buffer.requireOwner(this);
        if (!buffer.isRegistered() || registrations.get(buffer.handle) != buffer)
            throw new IllegalArgumentException("Unknown/revoked UMQ arena");
        UmqNative.revoke(buffer.remoteAddress);
        registrations.remove(buffer.handle); buffer.markUnregistered();
    }
    private long transfer(RemoteBuffer remote, long remoteOffset, ByteBuffer local,
                          int localOffset, int length, boolean write) {
        ready();
        if (connected == null) throw new IllegalStateException("UMQ peer is not connected");
        if (remote == null || remote.segment == null || local == null || !local.isDirect() ||
            (!write && local.isReadOnly()) ||
            localOffset < 0 || length < 0 || localOffset > local.capacity() - length ||
            remoteOffset < 0 || remoteOffset > remote.length || length > remote.length - remoteOffset)
            throw new IllegalArgumentException("Invalid UMQ buffer/window");
        SegmentDescriptor s = remote.segment;
        s.checkWindow(remote.remoteAddress, remote.length);
        if (!Arrays.equals(s.eid(), peerIdentity) || s.attributes != MAGIC ||
            s.token != remote.token || s.generation != remote.generation)
            throw new IllegalArgumentException("UMQ region identity mismatch");
        long baseOffset = remote.remoteAddress - s.address + remoteOffset;
        long id = nextRequest++; submitted++; submittedBytes += length;
        try {
            for (int offset = 0; offset < length;) {
                int chunk = Math.min(rawLimit, length - offset);
                rpcAttempted++;
                try {
                    UmqNative.transfer(s.address, s.length, s.generation, s.token,
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
        if (timeoutMs <= 0 || !results.containsKey(id)) throw new IllegalArgumentException("Unknown UMQ request");
        return results.get(id);
    }
    @Override public synchronized UbTransportMetrics metrics() {
        return UbTransportMetrics.builder("official-umq")
            .operations(submitted, completed, failed, 0, 0)
            .bytes(submittedBytes, completedBytes, failedBytes, 0)
            .registeredRegions(registrations.size())
            .backendCounter("umq.rpcCalls", rpcAttempted)
            .backendCounter("umq.rpcCompleted", rpcCompleted)
            .backendCounter("umq.rpcFailed", rpcFailed)
            .backendCounter("umq.readBytes", readBytes)
            .backendCounter("umq.writeBytes", writeBytes)
            .backendCounter("umq.stagingCopyBytes", copies)
            .backendCounter("umq.libraryNative", 1)
            .backendCounter("umq.hardwareVerified", 0)
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
        if (!registrations.isEmpty()) throw new IllegalStateException("Revoke UMQ arenas before close");
        UmqNative.close(); closed = true;
    }
}
