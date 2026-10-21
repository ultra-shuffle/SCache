package org.scache.deploy

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.Success
import java.util.concurrent.{ConcurrentHashMap, Semaphore}

import org.scache.deploy.DeployMessages.{IpcBlock, IpcFile, IpcPoolSlice}
import org.scache.storage.{BlockId, ScacheBlockId}

/** Hardware-independent regression test for all-or-nothing pool batch ownership. */
object UbPoolBatchSafetyTest {
  private implicit val ec: ExecutionContext = ExecutionContext.global

  def main(args: Array[String]): Unit = {
    val noSlots = new Semaphore(0)
    val interrupted = Promise[(Throwable, Boolean)]()
    val waiter = new Thread(() => {
      try ScacheClient.acquireUbPoolSlot(noSlots)
      catch { case e: Throwable => interrupted.success((e, Thread.currentThread().isInterrupted)) }
    }, "ub-pool-interrupt-test")
    waiter.setDaemon(true)
    waiter.start()
    waiter.interrupt()
    val (interruption, flagPreserved) = scala.concurrent.Await.result(
      interrupted.future, scala.concurrent.duration.Duration(5, "seconds"))
    waiter.join(1000)
    assert(interruption.isInstanceOf[IllegalStateException] && flagPreserved,
      "interrupted slot acquisition must finish as a nonfatal failure and preserve the flag")

    val first = ScacheBlockId("batch", 1, 1, 1, 1)
    val second = ScacheBlockId("batch", 1, 1, 1, 2)
    val ids: Seq[BlockId] = Seq(first, second)
    val pool = Some(IpcBlock(4096, IpcPoolSlice("/pool", 0L, 4096)))

    val flights = new ConcurrentHashMap[String, Future[Option[IpcBlock]]]()
    val completed = Promise[Option[IpcBlock]]()
    val observedRemoved = Promise[Boolean]()
    flights.put("one", completed.future)
    completed.future.onComplete(_ => observedRemoved.success(!flights.containsKey("one")))(
      ExecutionContext.parasitic)
    ScacheClient.finishUbPoolFetch(flights, "one", completed, Success(pool))
    assert(Await.result(observedRemoved.future, Duration.Inf),
      "completed single-flight result escaped before its stale cache entry was removed")

    val old = Promise[Option[IpcBlock]]()
    val replacement = Promise[Option[IpcBlock]]()
    flights.put("one", old.future)
    assert(flights.replace("one", old.future, replacement.future))
    ScacheClient.finishUbPoolFetch(flights, "one", old, Success(pool))
    assert(flights.get("one") eq replacement.future,
      "old completion removed a newer single-flight entry")
    flights.remove("one", replacement.future)

    val released = ArrayBuffer.empty[BlockId]
    val pending = Promise[Option[IpcBlock]]()
    val started = Promise[Unit]()
    val failure = new IllegalStateException("fetch failed")
    val worker = Future {
      try {
        ScacheClient.awaitUbPoolBatch(ids,
          id => if (id == first) Future.successful(pool)
            else { started.success(()); pending.future },
          id => { released += id; () })
        throw new AssertionError("failed fetch was accepted")
      } catch {
        case e: IllegalStateException if e eq failure => ()
      }
    }
    scala.concurrent.Await.result(started.future, scala.concurrent.duration.Duration.Inf)
    assert(!worker.isCompleted, "batch returned before every submitted fetch settled")
    pending.failure(failure)
    scala.concurrent.Await.result(worker, scala.concurrent.duration.Duration.Inf)
    assert(released.toSeq == Seq(first), s"pool reservation leak: $released")

    released.clear()
    val asyncInterruption = new InterruptedException("fetch Future failed")
    val asyncResult = Future {
      try {
        ScacheClient.awaitUbPoolBatch(ids,
          id => if (id == first) Future.successful(pool)
            else Future.failed(asyncInterruption),
          id => { released += id; () })
        throw new AssertionError("failed Future interruption was accepted")
      } catch {
        case e: Throwable if (e eq asyncInterruption) ||
            Option(e.getCause).contains(asyncInterruption) => ()
      }
    }
    Await.result(asyncResult, scala.concurrent.duration.Duration(5, "seconds"))
    assert(released.toSeq == Seq(first),
      s"failed Future interruption leaked pool reservation: $released")

    released.clear()
    val interruptedBatchPending = Promise[Option[IpcBlock]]()
    val interruptedBatchStarted = Promise[Unit]()
    val interruptedBatchDone = Promise[(Throwable, Boolean)]()
    val interruptedBatchWaiter = new Thread(() => {
      try {
        ScacheClient.awaitUbPoolBatch(ids,
          id => if (id == first) Future.successful(pool)
            else { interruptedBatchStarted.success(()); interruptedBatchPending.future },
          id => { released += id; () })
        interruptedBatchDone.success((new AssertionError("interrupted batch was accepted"), false))
      } catch {
        case e: Throwable =>
          interruptedBatchDone.success((e, Thread.currentThread().isInterrupted))
      }
    }, "ub-pool-batch-interrupt-test")
    interruptedBatchWaiter.setDaemon(true)
    interruptedBatchWaiter.start()
    Await.result(interruptedBatchStarted.future, Duration.Inf)
    interruptedBatchWaiter.interrupt()
    Thread.sleep(50)
    assert(!interruptedBatchDone.isCompleted,
      "interrupted batch returned while another fetch still owned its pool reservation")
    interruptedBatchPending.success(None)
    val (batchInterruption, batchFlagPreserved) = Await.result(
      interruptedBatchDone.future, scala.concurrent.duration.Duration(5, "seconds"))
    interruptedBatchWaiter.join(1000)
    assert(batchInterruption.isInstanceOf[IllegalStateException] && batchFlagPreserved,
      "interrupted batch must fail after settlement and preserve its interrupt flag")
    assert(released.toSeq == Seq(first), s"interrupted batch leaked pool reservation: $released")

    released.clear()
    val synchronousInterruptPending = Promise[Option[IpcBlock]]()
    val synchronousInterruptStarted = Promise[Unit]()
    val synchronousInterruptDone = Promise[(Throwable, Boolean)]()
    val synchronousInterruptWaiter = new Thread(() => {
      try {
        ScacheClient.awaitUbPoolBatch(ids,
          id => if (id == first) {
            synchronousInterruptStarted.success(())
            synchronousInterruptPending.future
          } else throw new InterruptedException("interrupted while starting fetch"),
          id => { released += id; () })
        synchronousInterruptDone.success((new AssertionError("sync interruption was accepted"), false))
      } catch {
        case e: Throwable =>
          synchronousInterruptDone.success((e, Thread.currentThread().isInterrupted))
      }
    }, "ub-pool-sync-interrupt-test")
    synchronousInterruptWaiter.setDaemon(true)
    synchronousInterruptWaiter.start()
    Await.result(synchronousInterruptStarted.future, Duration.Inf)
    Thread.sleep(50)
    assert(!synchronousInterruptDone.isCompleted,
      "synchronous interruption abandoned an earlier in-flight fetch")
    synchronousInterruptPending.success(pool)
    val (synchronousInterruption, synchronousFlagPreserved) = Await.result(
      synchronousInterruptDone.future, scala.concurrent.duration.Duration(5, "seconds"))
    synchronousInterruptWaiter.join(1000)
    assert(synchronousInterruption.isInstanceOf[IllegalStateException] && synchronousFlagPreserved,
      "synchronous interruption must fail after settlement and preserve the interrupt flag")
    assert(released.toSeq == Seq(first),
      s"synchronous interruption leaked earlier pool reservation: $released")

    released.clear()
    try {
      ScacheClient.awaitUbPoolBatch(ids,
        id => if (id == first) Future.successful(pool)
          else throw new IllegalStateException("start failed"),
        id => { released += id; () })
      throw new AssertionError("synchronous failure was accepted")
    } catch {
      case e: IllegalStateException if e.getMessage == "start failed" => ()
    }
    assert(released.toSeq == Seq(first), s"synchronous failure leaked pool: $released")

    released.clear()
    try {
      ScacheClient.awaitUbPoolBatch(ids,
        id => if (id == first) Future.successful(Some(IpcBlock(4096, IpcFile("/fallback"))))
          else Future.failed(new IllegalStateException("second failed")),
        id => { released += id; () })
      throw new AssertionError("failed batch was accepted")
    } catch {
      case e: IllegalStateException if e.getMessage == "second failed" => ()
    }
    assert(released.isEmpty, "file IPC does not own a registered pool reservation")

    released.clear()
    val result = ScacheClient.awaitUbPoolBatch(ids,
      id => if (id == first) Future.successful(pool)
        else Future.successful(Some(IpcBlock(4096, IpcFile("/fallback")))),
      id => { released += id; () })
    assert(result.size == 2 && released.isEmpty,
      "successful batch ownership must pass to the Spark caller")
    println("UB_POOL_BATCH_SAFETY_PASS")
  }
}
