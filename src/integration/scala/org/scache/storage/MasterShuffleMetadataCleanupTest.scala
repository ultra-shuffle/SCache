package org.scache.storage

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.scache.MapOutputTrackerMaster
import org.scache.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.scache.storage.BlockManagerMessages._
import org.scache.util.ScacheConf

/** Exercises the real master RPC endpoint, including both metadata indexes. */
object MasterShuffleMetadataCleanupTest {
  def main(args: Array[String]): Unit = {
    val home = Files.createTempDirectory("scache-master-cleanup-test-")
    Files.createDirectories(home.resolve("conf"))
    Files.writeString(home.resolve("conf/scache.conf"), "")
    Files.writeString(home.resolve("conf/log4j.properties"), "log4j.rootLogger=OFF\n")
    val conf = new ScacheConf(home.toString)
    conf.set("scache.ub.pool.enabled", "true", slient = true)
    val env = RpcEnv.create("master-cleanup-test", "127.0.0.1", 0, conf)
    val removals = new AtomicInteger()
    try {
      val tracker = new MapOutputTrackerMaster(conf, isLocal = true)
      tracker.hostnameToClientId = mutable.HashMap("127.0.0.1" -> 0)
      val slave = env.setupEndpoint("slave", new ThreadSafeRpcEndpoint {
        override val rpcEnv: RpcEnv = env
        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case _: RemoveShuffle => removals.incrementAndGet(); context.reply(0)
          case _: RemoveApplication => removals.incrementAndGet(); context.reply(0)
        }
      })
      val master = env.setupEndpoint("master",
        new BlockManagerMasterEndpoint(env, true, tracker, conf))
      val owners = Seq(BlockManagerId("0", "127.0.0.1", 10001),
        BlockManagerId("1", "127.0.0.1", 10002))
      owners.foreach { id =>
        val cap = BlockManagerCapability(1, "test-epoch-" + id.executorId,
          id.executorId, id, "ub", true, true, false, id.host, id.port)
        assert(master.askWithRetry[Boolean](RegisterBlockManager(id, 1L << 30, slave, cap)))
      }
      def add(app: String, job: Int, shuffle: Int, count: Int): Seq[ScacheBlockId] = {
        assert(tracker.registerShuffle(app, job, shuffle, 1, count))
        val blocks = (0 until count).map(i => ScacheBlockId(app, job, shuffle, 0, i))
        blocks.foreach { block => owners.foreach { id =>
          assert(master.askWithRetry[Boolean](
            UpdateBlockInfo(id, block, StorageLevel.MEMORY_ONLY, 128L, 0L)))
        }}
        blocks
      }
      def locations(block: BlockId): Seq[BlockManagerId] =
        master.askWithRetry[Seq[BlockManagerId]](GetLocations(block))
      def indexed: Set[BlockId] = Await.result(
        master.askWithRetry[Future[Seq[BlockId]]](GetMatchingBlockIds(_ => true, false)),
        30.seconds).toSet
      val selected = add("app-a", 1, 7, 3)
      val otherJob = add("app-a", 2, 7, 2)
      val otherShuffle = add("app-a", 1, 8, 2)
      val otherApp = add("app-b", 1, 7, 2)
      assert(locations(selected.head).size == 2)
      master.askWithRetry[Seq[Int]](RemoveShuffle(7, "app-a", 1))
      selected.foreach(b => assert(locations(b).isEmpty, "stale master locations: " + b))
      assert(indexed == (otherJob ++ otherShuffle ++ otherApp).toSet,
        "stale per-node BlockStatus or unrelated namespace removed")
      master.askWithRetry[Seq[Int]](RemoveShuffle(7, "app-a", 1))
      master.askWithRetry[Seq[Int]](RemoveShuffle(7, "app-a", -1))
      otherJob.foreach(b => assert(locations(b).isEmpty))
      assert(indexed == (otherShuffle ++ otherApp).toSet)
      master.askWithRetry[Seq[Int]](RemoveApplication("app-a"))
      assert(indexed == otherApp.toSet)
      otherShuffle.foreach(b => assert(locations(b).isEmpty))
      assert(locations(otherApp.head).size == 2)
      master.askWithRetry[Seq[Int]](RemoveShuffle(7))
      assert(indexed.isEmpty && locations(otherApp.head).isEmpty)
      // Repeated query-like rounds must return both indexes to empty.
      for (round <- 0 until 20) {
        val blocks = add("rounds", 0, 100 + round, 1000)
        assert(indexed.size == 1000)
        if (round % 2 == 0) master.askWithRetry[Seq[Int]](RemoveShuffle(100 + round, "rounds", 0))
        else master.askWithRetry[Seq[Int]](RemoveApplication("rounds"))
        assert(indexed.isEmpty, "metadata accumulates across rounds " + round)
        assert(locations(blocks.head).isEmpty && locations(blocks.last).isEmpty)
      }
      assert(removals.get() >= 50)
      println("MASTER_SHUFFLE_METADATA_CLEANUP_PASS rounds=20 blocks=20000 replicas=2")
    } finally {
      env.shutdown()
      env.awaitTermination()
    }
  }
}
