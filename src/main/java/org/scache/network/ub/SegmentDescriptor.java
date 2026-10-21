package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Immutable registered-MR identity. A block window is NOT a different MR. */
public final class SegmentDescriptor {
    public static final int WIRE_BYTES = 56;
    private final byte[] eid;
    public final int uasid, attributes, token;
    public final long address, length, generation;

    public SegmentDescriptor(byte[] eid, int uasid, int attributes, long address,
                             long length, int token, long generation) {
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
    }

    public byte[] eid() { return eid.clone(); }

    public void checkWindow(long start, long bytes) {
        if (bytes < 0 || bytes > length ||
                Long.compareUnsigned(start, address) < 0 ||
                Long.compareUnsigned(start - address, length - bytes) > 0)
            throw new UrmaProtocolException("block range exceeds registered MR");
    }

    public byte[] toBytes() {
        return ByteBuffer.allocate(WIRE_BYTES).order(ByteOrder.BIG_ENDIAN)
            .put(eid).putInt(uasid).putInt(attributes).putLong(address).putLong(length)
            .putInt(token).putInt(0).putLong(generation).array();
    }

    public static SegmentDescriptor fromBytes(byte[] raw) {
        if (raw == null || raw.length != WIRE_BYTES) throw new UrmaProtocolException("invalid MR descriptor length");
        ByteBuffer b = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        byte[] eid = new byte[16]; b.get(eid);
        int uasid = b.getInt(), attributes = b.getInt();
        long address = b.getLong(), length = b.getLong();
        int token = b.getInt();
        if (b.getInt() != 0) throw new UrmaProtocolException("nonzero MR descriptor reserved field");
        return new SegmentDescriptor(eid, uasid, attributes, address, length, token, b.getLong());
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof SegmentDescriptor)) return false;
        return Arrays.equals(toBytes(), ((SegmentDescriptor) other).toBytes());
    }
    @Override public int hashCode() { return Arrays.hashCode(toBytes()); }
}
