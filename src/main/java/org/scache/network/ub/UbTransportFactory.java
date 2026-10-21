package org.scache.network.ub;

import java.util.Locale;

/** Explicit native-backend selection below UBBlockTransferService; no implicit fallback. */
public final class UbTransportFactory {
    private UbTransportFactory() {}

    public static String normalizeBackend(String backend) {
        if (backend == null || backend.trim().isEmpty()) return "real";
        return backend.trim().toLowerCase(Locale.ROOT);
    }

    public static UbTransport open(String backend, String deviceName, int queueDepth,
                                   int maxChunkBytes, boolean strict, String wireRole) {
        String selected = normalizeBackend(backend);
        if ("urpc".equals(selected))
            return UrpcUbTransport.open(queueDepth, maxChunkBytes, UrmaConnectionConfig.legacyRc());
        if (!"real".equals(selected)) {
            throw new IllegalArgumentException(
                "Unsupported production scache.ub.transport=" + selected + "; use real");
        }
        return RealUbTransport.open(deviceName, queueDepth, maxChunkBytes, strict, wireRole);
    }

    public static UbTransport open(String backend, String deviceName, int queueDepth,
                                   int maxChunkBytes, boolean strict, UrmaConnectionConfig config) {
        String selected = normalizeBackend(backend);
        if ("urpc".equals(selected)) return UrpcUbTransport.open(queueDepth, maxChunkBytes, config);
        if (!"real".equals(selected)) {
            throw new IllegalArgumentException("Unsupported production scache.ub.transport=" + selected);
        }
        return RealUbTransport.open(deviceName, queueDepth, maxChunkBytes, strict, config);
    }
}
