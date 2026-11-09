package org.scache.deploy

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/** A SF100-sized batch must queue, rather than create one parked OS thread per block. */
object UbPoolFetchExecutorTest {
  private implicit val ec: ExecutionContext = ExecutionContext.global
  def main(args: Array[String]): Unit = {
    val parallelism = 4
    val workers = ScacheClient.newUbPoolFetchExecutionContext(parallelism)
    val entered = new CountDownLatch(parallelism)
    val proceed = new CountDownLatch(1)
    val names = ConcurrentHashMap.newKeySet[String]()
    val active = new AtomicInteger()
    val peak = new AtomicInteger()
    try {
      val requests = (0 until 5000).map { id =>
        Future {
          names.add(Thread.currentThread().getName)
          val count = active.incrementAndGet()
          peak.accumulateAndGet(count, (left: Int, right: Int) => math.max(left, right))
          entered.countDown()
          try {
            assert(proceed.await(10, TimeUnit.SECONDS))
            id
          } finally active.decrementAndGet()
        }(workers)
      }
      assert(entered.await(5, TimeUnit.SECONDS), "fixed workers did not start")
      assert(active.get() == parallelism && names.size() == parallelism,
        s"queued batch expanded native threads: active=${active.get()} threads=${names.size()}")
      assert(requests.count(_.isCompleted) == 0, "blocked work completed unexpectedly")
      proceed.countDown()
      val results = Await.result(Future.sequence(requests), 30.seconds)
      assert(results == (0 until 5000), "batch result order/content changed")
      assert(peak.get() <= parallelism && names.size() <= parallelism)
      assert(active.get() == 0)
      println(s"UB_POOL_FETCH_EXECUTOR_PASS requests=${results.size} threads=${names.size()} peak=${peak.get()}")
    } finally {
      proceed.countDown()
      workers.shutdown()
      assert(workers.awaitTermination(10, TimeUnit.SECONDS))
    }
  }
}
