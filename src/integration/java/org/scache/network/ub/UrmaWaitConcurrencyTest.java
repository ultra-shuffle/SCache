package org.scache.network.ub;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

/** Exercises the production Java wrapper with a deliberately blocked test JNI completion.
 * Native data correctness is covered separately by the real two-node workloads.
 */
public final class UrmaWaitConcurrencyTest {
    private static native void reset();
    private static native boolean entered();
    private static native void release();
    private static native void failWait();
    private static native void failClose();
    private static native int closes();
    private static native int earlyCloses();

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static UrmaTransport fixture() throws Exception {
        Constructor<UrmaTransport> c = UrmaTransport.class.getDeclaredConstructor();
        c.setAccessible(true);
        UrmaTransport t = c.newInstance();
        Field f = UrmaTransport.class.getDeclaredField("nativeHandle");
        f.setAccessible(true); f.setLong(t, 1L);
        return t;
    }
    private static void awaitEntry() throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!entered() && System.nanoTime() < end) Thread.sleep(1);
        require(entered(), "native wait did not start");
    }
    public static void main(String[] args) throws Exception {
        System.load(args[0]);
        boolean blockingControl = args.length > 1 && args[1].equals("blocking");
        ExecutorService workers = Executors.newFixedThreadPool(3);
        try {
            reset(); UrmaTransport t = fixture();
            Future<Integer> wait = workers.submit(() -> t.waitFor(1, 10000));
            awaitEntry();
            CountDownLatch sendEntered = new CountDownLatch(1);
            Future<Long> sent = workers.submit(() -> {
                sendEntered.countDown(); return t.send(ByteBuffer.allocateDirect(8), 0, 8);
            });
            require(sendEntered.await(5, TimeUnit.SECONDS), "submission thread not scheduled");
            if (blockingControl) {
                try { sent.get(200, TimeUnit.MILLISECONDS); throw new AssertionError("old lock was not reproduced"); }
                catch (TimeoutException expected) { }
                release(); require(wait.get(5, TimeUnit.SECONDS) == 0, "wait failed");
                require(sent.get(5, TimeUnit.SECONDS) == 42L, "submission failed");
                t.close();
                System.out.println("OLD_WRAPPER_NATIVE_WAIT_BLOCKS_SUBMISSION_CONFIRMED");
                return;
            }
            require(sent.get(2, TimeUnit.SECONDS) == 42L, "submit did not overlap wait");
            require(!wait.isDone(), "controlled wait unexpectedly finished");
            CountDownLatch closeEntered = new CountDownLatch(1);
            Future<?> close = workers.submit(() -> { closeEntered.countDown(); t.close(); });
            require(closeEntered.await(5, TimeUnit.SECONDS), "close thread not scheduled");
            Thread.sleep(100);
            require(!close.isDone() && closes() == 0, "close freed a live waiter handle");
            release(); require(wait.get(5, TimeUnit.SECONDS) == 0, "wait failed");
            close.get(5, TimeUnit.SECONDS);
            require(t.isClosed() && closes() == 1 && earlyCloses() == 0, "close lifetime guard failed");
            System.out.println("REAL_WRAPPER_SUBMISSION_OVERLAPS_WAIT_AND_CLOSE_RETAINS_HANDLE_PASS");

            reset(); UrmaTransport failed = fixture(); failWait();
            Future<Integer> error = workers.submit(() -> failed.waitFor(2, 10000));
            awaitEntry(); release();
            try { error.get(5, TimeUnit.SECONDS); throw new AssertionError("injected failure was swallowed"); }
            catch (ExecutionException expected) {
                require(expected.getCause() instanceof UrmaCompletionException, "wrong wait failure");
            }
            failClose();
            try { failed.close(); throw new AssertionError("failed close was swallowed"); }
            catch (UrmaException expected) { require(!failed.isClosed(), "failed close lost its handle"); }
            failed.close(); failed.close();
            require(failed.isClosed() && closes() == 2 && earlyCloses() == 0, "close retry/idempotence failed");
            System.out.println("WAIT_ERROR_UNLOCK_AND_FAILED_CLOSE_RETRY_PASS");
        } finally { release(); workers.shutdownNow(); require(workers.awaitTermination(5, TimeUnit.SECONDS), "threads leaked"); }
    }
}
