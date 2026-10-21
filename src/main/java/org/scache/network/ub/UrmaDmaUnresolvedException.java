package org.scache.network.ub;

/** Failure is terminal for the caller, but memory ownership has NOT returned. */
public final class UrmaDmaUnresolvedException extends RuntimeException {
    public UrmaDmaUnresolvedException(String message) { super(message); }
    public UrmaDmaUnresolvedException(String message, Throwable cause) { super(message, cause); }
}
