package org.scache.network.ub;

/** Exception accounting only: no provider, JNI or memory reclamation. */
public final class RmCtpLogicalMetricsTest {
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }

    public static void main(String[] args) {
        RealUbTransport.LogicalMetrics metrics = new RealUbTransport.LogicalMetrics();
        TransportMetrics nativeMetrics = new TransportMetrics(new long[32]);
        RuntimeException[] failures = {
            new UrmaTimeoutException("drained timeout"),
            new UrmaCompletionException("drained failed completion"),
            new UrmaDmaUnresolvedException("provider still owns destination"),
            new UrmaProtocolException("unknown request")
        };
        for (int i = 0; i < failures.length; i++) {
            long id = i + 1;
            RuntimeException failure = failures[i];
            metrics.recordSubmitted(id, 64);
            try {
                metrics.await(id, () -> { throw failure; });
                throw new AssertionError("exception swallowed");
            } catch (RuntimeException actual) {
                if (failure instanceof UrmaProtocolException) {
                    check(actual instanceof UrmaDmaUnresolvedException && actual.getCause() == failure,
                        "unknown wait exception did not retain DMA ownership");
                } else {
                    check(actual == failure, "exception identity changed");
                }
            }
            UbTransportMetrics snapshot = metrics.snapshot(nativeMetrics);
            check(snapshot.failedOperations == Math.min(i + 1, 2), "terminal failure not counted");
            check(snapshot.inflightOperations == Math.max(0, i - 1), "unresolved request lost");
            check(snapshot.timedOutOperations == 1, "timeout accounting changed");
            check(snapshot.terminalReconciled(), "bytes/operations do not reconcile");
        }
        // Repeated observation of a terminal request must not double-count it.
        try {
            metrics.await(1, () -> { throw failures[0]; });
        } catch (UrmaTimeoutException expected) { }
        check(metrics.snapshot(nativeMetrics).failedOperations == 2, "terminal counted twice");
        metrics.recordSubmitted(5, 128);
        check(metrics.await(5, () -> 0) == 0, "success changed");
        check(metrics.snapshot(nativeMetrics).completedBytes == 128, "success bytes lost");
        check(metrics.snapshot(nativeMetrics).inflightBytes == 128, "unresolved bytes lost");
        // Native has already returned an ID at this point. An accounting
        // exception must quarantine the request, not look like a pre-post failure.
        try {
            RealUbTransport.recordSubmittedAfterPost(metrics, 3, 64);
            throw new AssertionError("duplicate post-accounting failure was ignored");
        } catch (UrmaDmaUnresolvedException expected) {
            check(expected.getCause() instanceof IllegalStateException,
                "post-accounting failure lost its cause");
            check(metrics.snapshot(nativeMetrics).inflightBytes == 128,
                "post-accounting failure silently returned DMA ownership");
        }
        metrics.recordClosed();
        check(metrics.snapshot(nativeMetrics).inflightOperations == 0, "close did not reconcile");
        check(metrics.snapshot(nativeMetrics).terminalReconciled(), "close accounting mismatch");
        System.out.println("RM_CTP_LOGICAL_METRICS_PASS (offline; no hardware)");
    }
}
