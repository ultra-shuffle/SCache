package org.scache.network.ub;

/** Java-only negotiation regression; no JNI library or hardware is loaded. */
public final class NegotiatedCapabilitiesTest {
    private static void check(boolean ok, String reason) {
        if (!ok) throw new AssertionError(reason);
    }
    public static void main(String[] args) throws Exception {
        check(UrmaTransport.negotiatedRawLimit(65536, 4096) == 4096, "peer limit ignored");
        check(UrmaTransport.negotiatedRawLimit(65536, 32768) == 32768,
            "reconnect should use the original local limit, not the previous peer limit");
        boolean rejected = false;
        try { UrmaTransport.negotiatedRawLimit(0, 4096); }
        catch (IllegalArgumentException expected) { rejected = true; }
        check(rejected, "invalid negotiation limit accepted");

        java.lang.reflect.Constructor<UrmaTransport> nativeCtor =
            UrmaTransport.class.getDeclaredConstructor();
        nativeCtor.setAccessible(true);
        UrmaTransport delegate = nativeCtor.newInstance();
        java.lang.reflect.Field limit = UrmaTransport.class.getDeclaredField("maxChunkBytes");
        limit.setAccessible(true);
        limit.setInt(delegate, 65536);
        java.lang.reflect.Constructor<RealUbTransport> adapterCtor =
            RealUbTransport.class.getDeclaredConstructor(UrmaTransport.class, int.class);
        adapterCtor.setAccessible(true);
        RealUbTransport adapter = adapterCtor.newInstance(delegate, 64);
        UbTransportCapabilities opening = adapter.capabilities();
        check(opening.maxRawOperationBytes == 65536, "opening capability wrong");
        check(opening == adapter.capabilities(), "unchanged capability allocated again");
        limit.setInt(delegate, 4096); // Simulate the delegate's completed peer negotiation.
        check(adapter.capabilities().maxRawOperationBytes == 4096,
            "adapter cached the pre-connect raw operation limit");
        limit.setInt(delegate, 32768);
        check(adapter.capabilities().maxRawOperationBytes == 32768,
            "adapter did not refresh after a later peer negotiation");
        check(adapter.capabilities().maxQueueDepth == 64, "queue depth changed on negotiation");
        System.out.println("NEGOTIATED_CAPABILITIES_PASS (Java-only; no hardware)");
    }
}
