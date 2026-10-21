package org.scache.network.ub;
public class RemoteBuffer {
    public final long remoteAddress;
    public final long length;
    public final int token;
    /** Software registration identity used to distinguish cached remote MRs. */
    public final long generation;
    public final SegmentDescriptor segment;

    public RemoteBuffer(long remoteAddress, long length, int token) {
        this(remoteAddress, length, token, 0L);
    }

    public RemoteBuffer(long remoteAddress, long length, int token, long generation) {
        this(remoteAddress, length, token, generation, null);
    }

    public RemoteBuffer(long remoteAddress, long length, int token, long generation, SegmentDescriptor segment) {
        if (segment != null) {
            segment.checkWindow(remoteAddress, length);
            if (token != segment.token || generation != segment.generation)
                throw new UrmaProtocolException("block and MR identity mismatch");
        }
        this.remoteAddress = remoteAddress;
        this.length = length;
        this.token = token;
        this.generation = generation;
        this.segment = segment;
    }
}
