package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Endpoint descriptor exchanged via TCP control channel before URMA data transfer.
 * Protocol 2: packed network byte order, 88 bytes; incompatible with protocol 1.
 */
public class EndpointDescriptor {
    public int protocolVersion = 2;
    public int uasid;
    public int jettyId;
    public int tpType;
    public byte[] eid = new byte[16];
    public long segmentAddress;
    public long segmentLength;
    public int segmentToken;
    public long segmentGeneration;
    public int segmentUasid;
    public int maxChunkBytes = 10 * 1024 * 1024;
    public int transportMode;
    public byte[] segmentEid = new byte[16];

    private static final int SIZE = 88;

    public byte[] toBytes() {
        validate();
        ByteBuffer bb = ByteBuffer.allocate(SIZE).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(protocolVersion);
        bb.putInt(uasid);
        bb.putInt(jettyId);
        bb.putInt(tpType);
        bb.put(eid);
        bb.putLong(segmentAddress);
        bb.putLong(segmentLength);
        bb.putInt(segmentToken);
        bb.putInt(maxChunkBytes);
        bb.putInt(transportMode);
        bb.putInt(segmentUasid); // provider segment UBVA UASID; not the local EID index
        bb.putLong(segmentGeneration);
        bb.put(segmentEid);
        return bb.array();
    }

    public static EndpointDescriptor fromBytes(byte[] raw) {
        if (raw == null || raw.length != SIZE) {
            throw new UrmaProtocolException("endpoint descriptor must be exactly " + SIZE + " bytes");
        }
        EndpointDescriptor ep = new EndpointDescriptor();
        ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
        ep.protocolVersion = bb.getInt();
        ep.uasid = bb.getInt();
        ep.jettyId = bb.getInt();
        ep.tpType = bb.getInt();
        bb.get(ep.eid);
        ep.segmentAddress = bb.getLong();
        ep.segmentLength = bb.getLong();
        ep.segmentToken = bb.getInt();
        ep.maxChunkBytes = bb.getInt();
        ep.transportMode = bb.getInt();
        ep.segmentUasid = bb.getInt();
        ep.segmentGeneration = bb.getLong();
        bb.get(ep.segmentEid);
        ep.validate();
        return ep;
    }

    public void validate() {
        if (protocolVersion != 2) throw new UrmaProtocolException("unsupported endpoint protocol " + protocolVersion);
        if (eid == null || eid.length != 16) throw new UrmaProtocolException("EID must be exactly 16 bytes");
        if (segmentEid == null || segmentEid.length != 16) throw new UrmaProtocolException("segment EID must be exactly 16 bytes");
        if (!((transportMode == 1 && tpType == 1) || (transportMode == 2 && tpType == 0)))
            throw new UrmaProtocolException("unsupported endpoint mode/TP combination");
        // UBVA is unsigned; a high-bit address is carried as a negative Java long.
        if (segmentLength < 0) throw new UrmaProtocolException("negative segment length");
        if (maxChunkBytes <= 0)
            throw new UrmaProtocolException("invalid provider maxChunkBytes " + maxChunkBytes);
    }

    @Override
    public String toString() {
        return String.format("Endpoint{uasid=%d, jetty=%d, segAddr=0x%x, segLen=%d, chunkBytes=%d}",
            uasid, jettyId, segmentAddress, segmentLength, maxChunkBytes);
    }
}
