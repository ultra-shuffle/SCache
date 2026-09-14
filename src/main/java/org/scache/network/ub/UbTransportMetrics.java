package org.scache.network.ub;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable, versioned metrics snapshot for the real URMA transport. */
public final class UbTransportMetrics {
    public static final int SCHEMA_VERSION = 1;

    public final int schemaVersion;
    public final String backendName;
    public final long submittedOperations, completedOperations, failedOperations, timedOutOperations;
    public final long submittedBytes, completedBytes, failedBytes;
    public final long inflightOperations, inflightBytes, registeredRegions;
    public final Map<String, Long> backendCounters;

    private UbTransportMetrics(Builder b) {
        schemaVersion = b.schemaVersion;
        backendName = b.backendName;
        submittedOperations = b.submittedOperations;
        completedOperations = b.completedOperations;
        failedOperations = b.failedOperations;
        timedOutOperations = b.timedOutOperations;
        submittedBytes = b.submittedBytes;
        completedBytes = b.completedBytes;
        failedBytes = b.failedBytes;
        inflightOperations = b.inflightOperations;
        inflightBytes = b.inflightBytes;
        registeredRegions = b.registeredRegions;
        backendCounters = Collections.unmodifiableMap(new LinkedHashMap<>(b.backendCounters));
        validate();
    }

    private void validate() {
        if (schemaVersion != SCHEMA_VERSION) throw new IllegalArgumentException("unsupported metrics schema");
        if (backendName == null || backendName.trim().isEmpty()) throw new IllegalArgumentException("backendName is empty");
        long[] values = {submittedOperations, completedOperations, failedOperations, timedOutOperations,
            submittedBytes, completedBytes, failedBytes, inflightOperations, inflightBytes,
            registeredRegions};
        for (long value : values) if (value < 0) throw new IllegalArgumentException("metrics must be non-negative");
        if (timedOutOperations > failedOperations) throw new IllegalArgumentException("timeouts must be included in failures");
        for (Map.Entry<String, Long> entry : backendCounters.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isEmpty() || entry.getValue() == null || entry.getValue() < 0) {
                throw new IllegalArgumentException("invalid backend counter");
            }
        }
    }

    /** Valid only for a quiescent snapshot; timeouts are already included in failedOperations. */
    public boolean terminalReconciled() {
        return submittedOperations == completedOperations + failedOperations + inflightOperations
            && submittedBytes == completedBytes + failedBytes + inflightBytes;
    }

    public static Builder builder(String backendName) { return new Builder(backendName); }

    public static final class Builder {
        private int schemaVersion = SCHEMA_VERSION;
        private final String backendName;
        private long submittedOperations, completedOperations, failedOperations, timedOutOperations;
        private long submittedBytes, completedBytes, failedBytes;
        private long inflightOperations, inflightBytes, registeredRegions;
        private final Map<String, Long> backendCounters = new LinkedHashMap<>();

        private Builder(String backendName) { this.backendName = backendName; }
        public Builder operations(long submitted, long completed, long failed, long timedOut, long inflight) {
            submittedOperations=submitted; completedOperations=completed; failedOperations=failed;
            timedOutOperations=timedOut; inflightOperations=inflight; return this;
        }
        public Builder bytes(long submitted, long completed, long failed, long inflight) {
            submittedBytes=submitted; completedBytes=completed; failedBytes=failed; inflightBytes=inflight; return this;
        }
        public Builder registeredRegions(long value) { registeredRegions=value; return this; }
        public Builder backendCounter(String name, long value) { backendCounters.put(name, value); return this; }
        public UbTransportMetrics build() { return new UbTransportMetrics(this); }
    }
}
