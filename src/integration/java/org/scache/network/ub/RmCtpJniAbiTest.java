package org.scache.network.ub;

/** Loads the actual JNI binary, but rejects inputs before touching a device. */
public final class RmCtpJniAbiTest {
    public static void main(String[] args) {
        UrmaNative.ensureLoaded();
        if (UrmaNative.nativeApiVersion() != 3) throw new AssertionError("wrong JNI ABI");
        boolean invalidModeRejected = false;
        try {
            UrmaNative.nativeInitV2("not-a-device", 8, 4096, true, 2, 1, 0, 6, 10, 10);
        } catch (UrmaInitializationException e) {
            invalidModeRejected = e.getMessage().contains("invalid nativeInit arguments");
        }
        if (!invalidModeRejected) throw new AssertionError("invalid mode reached provider initialization");
        boolean invalidHandleRejected = false;
        try {
            UrmaNative.nativeConnectV2(0, new byte[16], 0, 1, 4096, 0, 1,
                4096, 64, -1, 2, 1, 1, new byte[16]);
        } catch (UrmaConnectionException e) {
            invalidHandleRejected = e.getMessage().contains("Null native handle");
        }
        if (!invalidHandleRejected) throw new AssertionError("invalid native handle accepted");
        int handleMethods = 0;
        for (java.lang.reflect.Method method : UrmaNative.class.getDeclaredMethods()) {
            Class<?>[] types = method.getParameterTypes();
            if (!java.lang.reflect.Modifier.isNative(method.getModifiers()) ||
                    types.length == 0 || types[0] != long.class) continue;
            Object[] values = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                if (types[i] == long.class) values[i] = 0L;
                else if (types[i] == int.class) values[i] = 1;
                else if (types[i] == byte[].class) values[i] = new byte[56];
                else if (types[i] == java.nio.ByteBuffer.class)
                    values[i] = java.nio.ByteBuffer.allocateDirect(64);
                else throw new AssertionError("unhandled JNI argument " + types[i]);
            }
            try {
                method.invoke(null, values);
                throw new AssertionError("null handle accepted by " + method.getName());
            } catch (java.lang.reflect.InvocationTargetException invocation) {
                Throwable cause = invocation.getCause();
                if (!(cause instanceof RuntimeException) || cause.getMessage() == null ||
                        !cause.getMessage().contains("Null native handle")) {
                    throw new AssertionError("JNI symbol/boundary failure " + method.getName(), cause);
                }
            } catch (IllegalAccessException inaccessible) {
                throw new AssertionError(inaccessible);
            }
            handleMethods++;
        }
        if (handleMethods < 10) throw new AssertionError("incomplete JNI boundary coverage");
        System.out.println("RM_CTP_JNI_HANDLE_BOUNDARIES_PASS methods=" + handleMethods);
        System.out.println("RM_CTP_JNI_ABI_PASS (actual JNI library; no device initialization)");
    }
}
