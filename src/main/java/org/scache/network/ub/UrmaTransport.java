package org.scache.network.ub;

import java.nio.ByteBuffer;

/**
 * Java wrapper around the native C++ UrmaTransport.
 *
 * Validation Rules:
 * - Only DirectByteBuffer (ByteBuffer.allocateDirect) is accepted.
 * - HeapByteBuffer and byte[] MUST be rejected explicitly.
 * - All offset/length combinations are bounds-checked.
 * - Double close() is safe.
 */
public final class UrmaTransport implements AutoCloseable {

    private long nativeHandle;
    private volatile boolean closed = false;

    private String deviceName;
    private int localMaxChunkBytes;
    private int maxChunkBytes;
    private long ownerEpoch;
    private TransportMetrics closeMetrics;

    private UrmaTransport() {}

    /**
     * Create and initialize a URMA transport.
     *
     * @param deviceName   URMA device name (e.g. "openurma0")
     * @param queueDepth   Completion queue depth
     * @param maxChunkBytes Configured maximum bytes per raw URMA operation. The
     *                      native transport clamps this to the queried provider capability.
     * @param strict       If true, reject unsupported capabilities instead of falling back
     */
    public static UrmaTransport open(String deviceName, int queueDepth,
                                      int maxChunkBytes, boolean strict)
            throws UrmaInitializationException {
        return open(deviceName, queueDepth, maxChunkBytes, strict, UrmaConnectionConfig.legacyRc());
    }

    public static UrmaTransport open(String deviceName, int queueDepth,
                                      int maxChunkBytes, boolean strict,
                                      String wireRole)
            throws UrmaInitializationException {
        // Compatibility overload for existing Java callers, not a hardware setting.
        if (!"listen".equals(wireRole) && !"connect".equals(wireRole)) {
            throw new UrmaInitializationException("wireRole must be listen or connect");
        }
        return open(deviceName, queueDepth, maxChunkBytes, strict, UrmaConnectionConfig.legacyRc());
    }

    public static UrmaTransport open(String deviceName, int queueDepth,
                                      int maxChunkBytes, boolean strict,
                                      UrmaConnectionConfig config)
            throws UrmaInitializationException {
        if (config == null) throw new UrmaInitializationException("connection config is required");
        UrmaNative.ensureLoaded();

        if (deviceName == null || deviceName.isBlank()) {
            throw new UrmaInitializationException("deviceName must not be null or blank");
        }
        if (queueDepth <= 0) {
            throw new UrmaInitializationException("queueDepth must be > 0, got " + queueDepth);
        }
        if (maxChunkBytes <= 0) {
            throw new UrmaInitializationException("maxChunkBytes must be > 0, got " + maxChunkBytes);
        }
        UrmaTransport t = new UrmaTransport();
        t.deviceName = deviceName;
        t.ownerEpoch = System.nanoTime();
        t.nativeHandle = UrmaNative.nativeInitV2(deviceName, queueDepth, maxChunkBytes, strict,
            config.transportMode, config.tpType, config.eidIndex, config.priority,
            config.requestTimeoutMs, config.drainTimeoutMs);
        try {
            EndpointDescriptor local = EndpointDescriptor.fromBytes(
                UrmaNative.nativeGetLocalEndpoint(t.nativeHandle));
            t.maxChunkBytes = local.maxChunkBytes;
            t.localMaxChunkBytes = local.maxChunkBytes;
            return t;
        } catch (RuntimeException | Error failure) {
            // A failure after nativeInitV2 still owns a context. Do not leave
            // it (and its provider objects) behind just because open failed.
            try { t.close(); }
            catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Connect to a remote endpoint. */
    public synchronized void connect(EndpointDescriptor remote) throws UrmaConnectionException {
        checkNotClosed();
        if (remote == null) throw new UrmaConnectionException("remote endpoint must not be null");
        remote.validate();
        UrmaNative.nativeConnectV2(nativeHandle, remote.eid, remote.uasid, remote.jettyId,
                                  remote.maxChunkBytes, remote.segmentUasid,
                                  remote.segmentGeneration,
                                  remote.segmentAddress, remote.segmentLength, remote.segmentToken,
                                  remote.protocolVersion, remote.transportMode, remote.tpType,
                                  remote.segmentEid);
        maxChunkBytes = negotiatedRawLimit(localMaxChunkBytes, remote.maxChunkBytes);
    }

    static int negotiatedRawLimit(int localMax, int peerMax) {
        if (localMax <= 0 || peerMax <= 0)
            throw new IllegalArgumentException("positive local and peer raw operation limits required");
        return Math.min(localMax, peerMax);
    }

    /** Get this transport's local endpoint for exchange via control channel. */
    public synchronized EndpointDescriptor getLocalEndpoint() {
        checkNotClosed();
        byte[] raw = UrmaNative.nativeGetLocalEndpoint(nativeHandle);
        return EndpointDescriptor.fromBytes(raw);
    }

    // ---- Buffer Registration ----

    /**
     * Register a DirectByteBuffer as a URMA segment.
     * @throws UrmaException if buffer is not direct
     */
    public synchronized RegisteredBuffer registerBuffer(ByteBuffer buffer) {
        checkNotClosed();
        checkDirect(buffer);
        long[] descriptor = UrmaNative.nativeRegisterBuffer(nativeHandle, buffer);
        try {
            if (descriptor == null || descriptor.length != 10) {
                throw new UrmaException("native registration returned an invalid descriptor");
            }
            byte[] eid = ByteBuffer.allocate(16).putLong(descriptor[5]).putLong(descriptor[6]).array();
            SegmentDescriptor segment = new SegmentDescriptor(eid, (int) descriptor[7], (int) descriptor[8],
                descriptor[1], descriptor[9], (int) descriptor[3], descriptor[4]);
            return new RegisteredBuffer(this, descriptor[0], buffer,
                descriptor[1], descriptor[2], descriptor[3], descriptor[4], segment);
        } catch (RuntimeException | Error failure) {
            // JNI already owns a live MR when it returns a handle. Do not lose
            // that handle if Java-side descriptor validation/allocation fails.
            if (descriptor != null && descriptor.length > 0 && descriptor[0] > 0) {
                try {
                    UrmaNative.nativeUnregisterBuffer(nativeHandle, descriptor[0]);
                } catch (RuntimeException | Error cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    public synchronized void unregisterBuffer(RegisteredBuffer buffer) {
        checkNotClosed();
        if (buffer == null) throw new UrmaException("registered buffer must not be null");
        buffer.checkOwner(this);
        if (!buffer.registered) throw new UrmaException("buffer is already unregistered");
        UrmaNative.nativeUnregisterBuffer(nativeHandle, buffer.handle);
        buffer.registered = false;
    }

    /**
     * Release cached peer MR imports once application-level reads have drained.
     * The transport remains connected and may import descriptors again later.
     */
    public synchronized void releaseCachedRemoteImports() {
        checkNotClosed();
        UrmaNative.nativeReleaseCachedRemoteImports(nativeHandle);
    }

    // ---- Write ----

    /**
     * Write data to a remote URMA segment.
     * @return request ID for wait()
     */
    public synchronized long write(RemoteBuffer remote, ByteBuffer source, int offset, int length) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(source);
        checkBounds(source, offset, length);

        if (closed) throw new UrmaException("Transport closed");
        return UrmaNative.nativeWriteMR(nativeHandle,
            remote.remoteAddress, remote.length, remote.token, remote.generation,
            source, offset, length, remote.segment.toBytes());
    }

    // ---- Read ----

    /**
     * Read data from a remote URMA segment.
     * @return request ID for wait()
     */
    public synchronized long read(RemoteBuffer remote, ByteBuffer dest, int offset, int length) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(dest);
        checkBounds(dest, offset, length);

        return UrmaNative.nativeReadMR(nativeHandle,
            remote.remoteAddress, remote.length, remote.token, remote.generation,
            dest, offset, length, remote.segment.toBytes());
    }

    // ---- Send ----

    /**
     * Send a message via URMA SEND.
     * @return request ID for wait()
     */
    public synchronized long send(ByteBuffer message, int offset, int length) {
        checkNotClosed();
        checkDirect(message);
        checkBounds(message, offset, length);

        return UrmaNative.nativeSend(nativeHandle, message, offset, length);
    }

    // ---- Chunked operations ----

    /**
     * Write a large block split using the negotiated provider capability.
     */
    public synchronized long writeChunked(RemoteBuffer remote, ByteBuffer source, int offset, int totalLength) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(source);
        checkBounds(source, offset, totalLength);

        return UrmaNative.nativeWriteChunkedMR(nativeHandle,
            remote.remoteAddress, remote.length, remote.token, remote.generation,
            source, offset, totalLength, remote.segment.toBytes());
    }

    /**
     * Read a large block split into chunks.
     */
    public synchronized long readChunked(RemoteBuffer remote, ByteBuffer dest, int offset, int totalLength) {
        checkNotClosed();
        checkRemote(remote);
        checkDirect(dest);
        checkBounds(dest, offset, totalLength);

        return UrmaNative.nativeReadChunkedMR(nativeHandle,
            remote.remoteAddress, remote.length, remote.token, remote.generation,
            dest, offset, totalLength, remote.segment.toBytes());
    }

    // ---- Completion ----

    /**
     * Wait for a specific request to complete.
     * @param requestId the request ID returned by write/read/send
     * @param timeoutMs timeout in milliseconds
     * @return 0 on success
     * @throws UrmaTimeoutException after a timeout whose accepted WRs have drained
     * @throws UrmaCompletionException on a drained failed completion
     * @throws UrmaDmaUnresolvedException if provider ownership could not be drained;
     *         registered memory and the remote lease must remain pinned
     */
    public synchronized int waitFor(long requestId, int timeoutMs) {
        checkNotClosed();
        if (requestId <= 0) throw new UrmaException("requestId must be > 0");
        if (timeoutMs <= 0) throw new UrmaException("timeoutMs must be > 0");
        return UrmaNative.nativeWait(nativeHandle, requestId, timeoutMs);
    }

    // ---- Metrics ----

    public synchronized TransportMetrics getMetrics() {
        checkNotClosed();
        long[] raw = UrmaNative.nativeGetMetrics(nativeHandle);
        return new TransportMetrics(raw);
    }

    // ---- Cleanup ----

    @Override
    public synchronized void close() {
        if (closed) return;
        if (nativeHandle != 0) {
            long[] raw = UrmaNative.nativeClose(nativeHandle);
            // Native code intentionally retains the owner if it cannot return
            // the metrics array. Keep this handle for a later close retry.
            if (raw == null) throw new UrmaException("native close returned no metrics; retry required");
            nativeHandle = 0;
            closed = true;
            closeMetrics = new TransportMetrics(raw);
        }
        closed = true;
    }

    public boolean isClosed() { return closed; }
    public synchronized TransportMetrics closeMetrics() {
        if (!closed || closeMetrics == null) throw new UrmaException("transport has not closed successfully");
        return closeMetrics;
    }
    public String deviceName() { return deviceName; }
    public synchronized int maxChunkBytes() { return maxChunkBytes; }

    static ProcessRuntimeMetrics processRuntimeMetrics() {
        UrmaNative.ensureLoaded();
        return new ProcessRuntimeMetrics(UrmaNative.nativeGetProcessRuntimeMetrics());
    }

    static void shutdownProcessRuntime() {
        UrmaNative.ensureLoaded();
        UrmaNative.nativeShutdownProcessRuntime();
    }

    // ---- Internal validation ----

    private void checkNotClosed() {
        if (closed) throw new UrmaException("Transport is closed");
        if (nativeHandle == 0) throw new UrmaException("Invalid native handle");
    }

    static void checkDirect(ByteBuffer buf) {
        if (buf == null) {
            throw new UrmaException("ByteBuffer must not be null");
        }
        if (!buf.isDirect()) {
            throw new UrmaException(
                "Only DirectByteBuffer is supported. Use ByteBuffer.allocateDirect(). " +
                "HeapByteBuffer and byte[] are rejected because they can be moved by GC.");
        }
    }

    static void checkBounds(ByteBuffer buf, int offset, int length) {
        if (offset < 0) {
            throw new UrmaException("offset must be >= 0, got " + offset);
        }
        if (length < 0) {
            throw new UrmaException("length must be >= 0, got " + length);
        }
        if (offset > buf.capacity() || length > buf.capacity() - offset) {
            throw new UrmaException(
                "offset(" + offset + ") + length(" + length +
                ") exceeds capacity(" + buf.capacity() + ")");
        }
    }

    private void checkRemote(RemoteBuffer remote) {
        if (remote == null) throw new UrmaException("remote buffer must not be null");
        if (remote.segment == null) throw new UrmaException("remote MR identity is required");
        remote.segment.checkWindow(remote.remoteAddress, remote.length);
        if (remote.remoteAddress == 0 || remote.length <= 0) {
            throw new UrmaException("invalid remote region descriptor");
        }
    }

    // ---- Inner types ----

    public static class RegisteredBuffer {
        private final UrmaTransport owner;
        public final long handle;
        public final ByteBuffer buffer;
        public final long remoteAddress;
        public final long length;
        public final long token;
        public final long generation;
        public final SegmentDescriptor segment;
        private volatile boolean registered = true;
        RegisteredBuffer(UrmaTransport owner, long handle, ByteBuffer buffer,
                         long remoteAddress, long length, long token, long generation, SegmentDescriptor segment) {
            this.owner = owner;
            this.handle = handle;
            this.buffer = buffer;
            this.remoteAddress = remoteAddress;
            this.length = length;
            this.token = token;
            this.generation = generation;
            this.segment = segment;
        }
        void checkOwner(UrmaTransport candidate) {
            if (owner != candidate) throw new UrmaException("registered buffer belongs to another transport");
        }
        public boolean isRegistered() { return registered; }
        public RemoteBuffer descriptor() {
            if (!registered) throw new UrmaException("buffer is unregistered");
            return new RemoteBuffer(remoteAddress, length, (int) token, generation, segment);
        }
    }
}
