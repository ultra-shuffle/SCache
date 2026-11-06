package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Immutable registered-MR identity. A block window is NOT a different MR. */
public final class SegmentDescriptor {
    public static final int WIRE_BYTES = 56;
    private final byte[] eid;
    private final byte[] providerContext;
    public static final int MAX_PROVIDER_CONTEXT = 32768;
    public final int uasid, attributes, token;
    public final long address, length, generation;

    public SegmentDescriptor(byte[] eid, int uasid, int attributes, long address,
                             long length, int token, long generation) {
        this(eid, uasid, attributes, address, length, token, generation, new byte[0]);
    }

    public SegmentDescriptor(byte[] eid, int uasid, int attributes, long address,
                             long length, int token, long generation, byte[] providerContext) {
        if (eid == null || eid.length != 16) throw new UrmaProtocolException("MR EID must be 16 bytes");
        // A provider UBVA is an unsigned 64-bit address carried losslessly in a
        // Java long; a negative long is not necessarily an invalid address.
        if (address == 0 || length <= 0 ||
                Long.compareUnsigned(address, -1L - length) > 0 || generation <= 0)
            throw new UrmaProtocolException("invalid or unsupported MR address/length/generation");
        this.eid = eid.clone();
        this.uasid = uasid;
        this.attributes = attributes;
        this.address = address;
        this.length = length;
        this.token = token;
        this.generation = generation;
        if (providerContext == null || providerContext.length > MAX_PROVIDER_CONTEXT)
            throw new UrmaProtocolException("invalid provider context length");
        this.providerContext = providerContext.clone();
    }

    public byte[] eid() { return eid.clone(); }
    public byte[] providerContext() { return providerContext.clone(); }

    public void checkWindow(long start, long bytes) {
        if (bytes < 0 || bytes > length ||
                Long.compareUnsigned(start, address) < 0 ||
                Long.compareUnsigned(start - address, length - bytes) > 0)
            throw new UrmaProtocolException("block range exceeds registered MR");
    }

    public byte[] toBytes() {
        return ByteBuffer.allocate(WIRE_BYTES + 4 + providerContext.length).order(ByteOrder.BIG_ENDIAN)
            .put(eid).putInt(uasid).putInt(attributes).putLong(address).putLong(length)
            .putInt(token).putInt(0).putLong(generation)
            .putInt(providerContext.length).put(providerContext).array();
    }

    public static SegmentDescriptor fromBytes(byte[] raw) {
        if (raw == null || raw.length < WIRE_BYTES + 4 || raw.length > WIRE_BYTES + 4 + MAX_PROVIDER_CONTEXT)
            throw new UrmaProtocolException("invalid MR descriptor length");
        ByteBuffer b = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        byte[] eid = new byte[16]; b.get(eid);
        int uasid = b.getInt(), attributes = b.getInt();
        long address = b.getLong(), length = b.getLong();
        int token = b.getInt();
        if (b.getInt() != 0) throw new UrmaProtocolException("nonzero MR descriptor reserved field");
        long generation = b.getLong();
        int lengthBytes = b.getInt();
        if (lengthBytes < 0 || lengthBytes != b.remaining()) throw new UrmaProtocolException("invalid provider context length");
        byte[] context = new byte[lengthBytes]; b.get(context);
        return new SegmentDescriptor(eid, uasid, attributes, address, length, token, generation, context);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof SegmentDescriptor)) return false;
        return Arrays.equals(toBytes(), ((SegmentDescriptor) other).toBytes());
    }
    @Override public int hashCode() { return Arrays.hashCode(toBytes()); }
}
