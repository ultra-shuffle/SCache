package org.scache.network.ub;

/** Explicit hardware transport selection. No silent RC/RM fallback. */
public final class UrmaConnectionConfig {
    public final String mode;
    public final int transportMode;
    public final int tpType;
    public final int eidIndex;
    public final int priority;
    public final int requestTimeoutMs;
    public final int drainTimeoutMs;

    public UrmaConnectionConfig(String mode, int eidIndex, int priority,
                                int requestTimeoutMs, int drainTimeoutMs) {
        if (!"rc-rtp".equals(mode) && !"rm-ctp".equals(mode)) {
            throw new IllegalArgumentException("scache.ub.mode must be rc-rtp or rm-ctp");
        }
        if (eidIndex < 0 || priority < 0 || priority > 15 || requestTimeoutMs <= 0
                || drainTimeoutMs <= 0) {
            throw new IllegalArgumentException("explicit nonnegative EID, priority 0..15 and positive request/drain timeouts required");
        }
        this.mode = mode;
        // Public UMDK enum values are bit masks, NOT perftest -p arguments.
        this.transportMode = "rm-ctp".equals(mode) ? 1 : 2;
        this.tpType = "rm-ctp".equals(mode) ? 1 : 0;
        this.eidIndex = eidIndex;
        this.priority = priority;
        this.requestTimeoutMs = requestTimeoutMs;
        this.drainTimeoutMs = drainTimeoutMs;
    }

    public static UrmaConnectionConfig legacyRc() {
        return new UrmaConnectionConfig("rc-rtp", 0, 0, 30000, 5000);
    }
}
