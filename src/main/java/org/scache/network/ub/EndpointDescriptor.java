package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Endpoint descriptor exchanged via TCP control channel before URMA data transfer.
 * Protocol 3: 88-byte network-order identity, context length, opaque SDK context.
 * Both peers must use protocol 3; older descriptors are rejected.
 */
public class EndpointDescriptor {
    public int protocolVersion = 3;
    public byte[] providerContext = new byte[0];
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
        ByteBuffer bb = ByteBuffer.allocate(SIZE + 4 + providerContext.length).order(ByteOrder.BIG_ENDIAN);
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
        bb.putInt(providerContext.length).put(providerContext);
        return bb.array();
    }

    public static EndpointDescriptor fromBytes(byte[] raw) {
        if (raw == null || raw.length < SIZE + 4 || raw.length > SIZE + 4 + SegmentDescriptor.MAX_PROVIDER_CONTEXT) {
            throw new UrmaProtocolException("invalid endpoint descriptor length");
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
        int contextLength = bb.getInt();
        if (contextLength < 0 || contextLength != bb.remaining()) throw new UrmaProtocolException("invalid Jetty context length");
        ep.providerContext = new byte[contextLength]; bb.get(ep.providerContext);
        ep.validate();
        return ep;
    }

    public void validate() {
        if (protocolVersion != 3) throw new UrmaProtocolException("unsupported endpoint protocol " + protocolVersion);
        if (providerContext == null || providerContext.length > SegmentDescriptor.MAX_PROVIDER_CONTEXT)
            throw new UrmaProtocolException("invalid Jetty context length");
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
