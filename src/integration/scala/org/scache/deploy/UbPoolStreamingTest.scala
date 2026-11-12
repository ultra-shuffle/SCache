package org.scache.deploy

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure, Success, Try}
import org.scache.storage.{BlockId, ScacheBlockId}
import org.scache.deploy.DeployMessages._

object UbPoolStreamingTest {
  private implicit val ec: ExecutionContext = ExecutionContext.parasitic
  def main(args: Array[String]): Unit = {
    val ids: Seq[BlockId] = (0 until 3).map(i => ScacheBlockId("stream", 1, 1, 1, i))
    val pending = ids.map(_ => Promise[Option[IpcBlock]]())
    val delivered = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
    val outcomes = new java.util.concurrent.ConcurrentHashMap[Int, Try[Option[IpcBlock]]]()
    ScacheClient.streamUbPoolBatch(ids, id => pending(ids.indexOf(id)).future,
      (index, result) => { delivered.add(index); outcomes.put(index, result); () })
    assert(delivered.isEmpty)
    val slice = Some(IpcBlock(4096, IpcPoolSlice("/pool", 0, 4096)))
    pending(1).success(slice)
    assert(delivered.size() == 1 && delivered.peek() == 1)
    assert(!pending(0).isCompleted && !pending(2).isCompleted)
    pending(0).failure(new IllegalStateException("injected failure"))
    assert(outcomes.get(0).isFailure && outcomes.get(1) == Success(slice))
    pending(2).success(slice)
    assert(delivered.size() == 3 && outcomes.get(2) == Success(slice))
    println("STREAM_EARLY_OUT_OF_ORDER_AND_PARTIAL_FAILURE_PASS")
    val called = new CountDownLatch(3)
    ScacheClient.streamUbPoolBatch(ids, _ => throw new IllegalArgumentException("start"),
      (_, outcome) => { assert(outcome.isFailure); called.countDown() })
    assert(called.await(1, TimeUnit.SECONDS))
    println("STREAM_SYNCHRONOUS_START_FAILURE_ALL_CHILDREN_REPORTED_PASS")
  }
}
