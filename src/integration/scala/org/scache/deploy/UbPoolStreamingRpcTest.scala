package org.scache.deploy

import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.scache.deploy.DeployMessages._
import org.scache.rpc._
import org.scache.storage.{BlockManagerId, ScacheBlockId}
import org.scache.util.ScacheConf

/** Real loopback RPC and production Daemon receiver; no simulated URMA/driver needed. */
object UbPoolStreamingRpcTest {
  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val home = Files.createTempDirectory(Paths.get(args(0)), "stream-rpc-").toString
    val configDir = Files.createDirectories(Paths.get(home, "conf"))
    Files.writeString(configDir.resolve("log4j.properties"),
      "log4j.rootLogger=ERROR, console\n" +
      "log4j.appender.console=org.apache.log4j.ConsoleAppender\n" +
      "log4j.appender.console.layout=org.apache.log4j.PatternLayout\n" +
      "log4j.appender.console.layout.ConversionPattern=%p %m%n\n")
    val conf = new ScacheConf(home)
    conf.set("scache.rpc.askTimeout", "2s")
    val env = RpcEnv.create("stream-test-client", "127.0.0.1", 0, conf, false)
    val submitted = new LinkedBlockingQueue[RpcEndpointRef]()
    env.setupEndpoint("ScacheClient", new ThreadSafeRpcEndpoint {
      override val rpcEnv: RpcEnv = env
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case ClientHello(_, _) => context.reply(ClientIdentity(1, "test", 0,
          "127.0.0.1", env.address.port, BlockManagerId("0", "127.0.0.1", env.address.port),
          "ub", true, true, false))
        case GetBlocksIpcStream(_, replyTo) => submitted.add(replyTo); context.reply(true)
      }
    })
    val daemon = new Daemon(home, "stream-test", DaemonEndpointConfig(
      bindHost=Some("127.0.0.1"), clientHost=Some("127.0.0.1"),
      clientPort=Some(env.address.port)))
    val ids = (0 until 2).map(i => ScacheBlockId("test", 1, 1, 1, i).toString)
    def callback(count: AtomicInteger, first: CountDownLatch, last: CountDownLatch) =
      new java.util.function.BiConsumer[java.lang.Integer, AnyRef] {
        override def accept(i: java.lang.Integer, outcome: AnyRef): Unit = {
          count.incrementAndGet()
          if (i.intValue() == 0) first.countDown() else last.countDown()
        }
      }
    try {
      val count = new AtomicInteger(); val first = new CountDownLatch(1); val last = new CountDownLatch(1)
      val work = Future(daemon.getBlocksIpcStreaming(ids, callback(count, first, last)))
      val ref = submitted.poll(5, TimeUnit.SECONDS); assert(ref != null)
      ref.send(IpcBlockReady(1, None, Some("injected child failure")))
      ref.send(IpcBlockReady(1, None, Some("duplicate completion")))
      assert(last.await(1, TimeUnit.SECONDS) && !work.isCompleted)
      ref.send(IpcBlockReady(0, Some(IpcBlock(1, IpcPoolSlice("/test", 0, 1))), None))
      Await.result(work, 5.seconds)
      assert(count.get() == 2 && first.getCount == 0)
      println("STREAM_REAL_RPC_EARLY_DELIVERY_OUT_OF_ORDER_DUPLICATE_GUARD_PASS")

      val lateCount = new AtomicInteger(); val early = new CountDownLatch(1); val late = new CountDownLatch(1)
      val timeout = Future(daemon.getBlocksIpcStreaming(ids, callback(lateCount, early, late)))
      val lateRef = submitted.poll(5, TimeUnit.SECONDS); assert(lateRef != null)
      lateRef.send(IpcBlockReady(0, None, None))
      assert(early.await(1, TimeUnit.SECONDS))
      assert(Await.ready(timeout, 5.seconds).value.get.isFailure)
      lateRef.send(IpcBlockReady(1, None, None))
      assert(late.await(1, TimeUnit.SECONDS) && lateCount.get() == 2)
      println("STREAM_REAL_RPC_TIMEOUT_RETAINS_LATE_RESULT_DELIVERY_PASS")
    } finally {
      daemon.stop(); env.shutdown(); env.awaitTermination()
    }
  }
}
